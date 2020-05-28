// Copyright 2018-2020, Wayfair GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::dflt;
use crate::errors::Result;
use crate::onramp::prelude::*;

//NOTE: This is required for StreamHander's stream
use futures::StreamExt;
use halfbrown::HashMap;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::error::KafkaResult;
use rdkafka::Message;
use serde_yaml::Value;
use std::time::Duration;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// kafka group ID to register with
    pub group_id: String,
    /// List of topics to subscribe to
    pub topics: Vec<String>,
    /// List of bootstrap brokers
    pub brokers: Vec<String>,
    /// If sync is set to true the kafka onramp will wait for an event
    /// to be fully acknowledged before fetching the next one. Defaults
    /// to `false`. Do not use in combination with batching offramps!
    #[serde(default = "dflt::d_false")]
    pub sync: bool,
    /// Optional rdkafka configuration
    ///
    /// Default settings:
    /// * `client.id` - `"tremor-<hostname>-<thread id>"`
    /// * `bootstrap.servers` - `brokers` from the config concatinated by `,`
    /// * `enable.partition.eof` - `"false"`
    /// * `session.timeout.ms` - `"6000"`
    /// * `enable.auto.commit` - `"true"`
    /// * `auto.commit.interval.ms"` - `"5000"`
    /// * `enable.auto.offset.store` - `"true"`
    pub rdkafka_options: Option<HashMap<String, String>>,
}

impl ConfigImpl for Config {}

pub struct Kafka {
    pub config: Config,
}

impl onramp::Impl for Kafka {
    fn from_config(_id: &str, config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self { config }))
        } else {
            Err("Missing config for blaster onramp".into())
        }
    }
}

// A simple context to customize the consumer behavior and print a log line every time
// offsets are committed
pub struct LoggingConsumerContext;

impl ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &rdkafka::TopicPartitionList) {
        match result {
            Ok(_) => info!("Offsets committed successfully"),
            Err(e) => warn!("Error while committing offsets: {}", e),
        };
    }
}

pub type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;

#[allow(clippy::too_many_lines, clippy::cognitive_complexity)]
async fn onramp_loop(
    rx: Receiver<onramp::Msg>,
    config: &Config,
    mut preprocessors: Preprocessors,
    mut codec: Box<dyn Codec>,
    mut metrics_reporter: RampReporter,
) -> Result<()> {
    let context = LoggingConsumerContext;
    let mut client_config = ClientConfig::new();
    let mut pipelines: Vec<(TremorURL, pipeline::Addr)> = Vec::new();
    let tid = task::current().id();
    info!("Starting kafka onramp");
    let client_config = client_config
        .set("group.id", &config.group_id)
        .set("client.id", &format!("tremor-{}-{:?}", hostname(), tid))
        .set("bootstrap.servers", &config.brokers.join(","))
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        // Commit automatically every 5 seconds.
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        // but only commit the offsets explicitly stored via `consumer.store_offset`.
        .set("enable.auto.offset.store", "true")
        .set_log_level(RDKafkaLogLevel::Debug);

    let client_config = if let Some(options) = config.rdkafka_options.clone() {
        options
            .iter()
            .fold(client_config, |c: &mut ClientConfig, (k, v)| c.set(k, v))
    } else {
        client_config
    };

    let client_config = client_config.to_owned();
    let consumer: LoggingConsumer = client_config.create_with_context(context)?;

    let topics: Vec<&str> = config
        .topics
        .iter()
        .map(std::string::String::as_str)
        .collect();

    let first_broker: Vec<&str> = config.brokers[0].split(':').collect();
    let mut origin_uri = tremor_pipeline::EventOriginUri {
        scheme: "tremor-kafka".to_string(),
        // picking the first host for these
        host: first_broker[0].to_string(),
        port: match first_broker.get(1) {
            Some(n) => Some(n.parse()?),
            None => None,
        },
        path: vec![],
    };

    let mut stream = consumer.start();

    info!("[kafka] subscribing to: {:?}", topics);
    // This is terribly ugly, thank you rdkafka!
    // We need to do this because:
    // - subscribing to a topic that does not exist will brick the whole consumer
    // - subscribing to a topic that does not exist will claim to succeed
    // - getting the metadata of a topic that does not exist will claim to succeed
    // - The only indication of it missing is in the metadata, in the topics list
    //   in the errors ...
    //
    // This is terrible :/
    let mut id = 0;
    let mut good_topics = Vec::new();
    for topic in topics {
        match consumer.fetch_metadata(Some(topic), Duration::from_secs(1)) {
            Ok(m) => {
                let errors: Vec<_> = m
                    .topics()
                    .iter()
                    .map(rdkafka::metadata::MetadataTopic::error)
                    .collect();
                match errors.as_slice() {
                    [None] => good_topics.push(topic),
                    [Some(e)] => error!(
                        "Kafka error for topic '{}': {:?}. Not subscribing!",
                        topic, e
                    ),
                    _ => error!(
                        "Unknown kafka error for topic '{}'. Not subscribing!",
                        topic
                    ),
                }
            }
            Err(e) => error!("Kafka error for topic '{}': {}. Not subscribing!", topic, e),
        };
    }

    match consumer.subscribe(&good_topics) {
        Ok(()) => info!("Subscribed to topics: {:?}", good_topics),
        Err(e) => error!("Kafka error for topics '{:?}': {}", good_topics, e),
    };

    // We do this twice so we don't consume a message from kafka and then wait
    // as this could lead to timeouts
    loop {
        match task::block_on(handle_pipelines(
            false,
            &rx,
            &mut pipelines,
            &mut metrics_reporter,
        ))? {
            PipeHandlerResult::Retry => continue,
            PipeHandlerResult::Terminate => return Ok(()),
            _ => break, // fixme .unwrap()
        }
    }
    while let Some(m) = stream.next().await {
        loop {
            match task::block_on(handle_pipelines(
                false,
                &rx,
                &mut pipelines,
                &mut metrics_reporter,
            ))? {
                PipeHandlerResult::Retry => continue,
                PipeHandlerResult::Terminate => return Ok(()),
                _ => break, // fixme .unwrap()
            }
        }
        if let Ok(m) = m {
            if let Some(data) = m.payload_view::<[u8]>() {
                if let Ok(data) = data {
                    id += 1;
                    let mut ingest_ns = nanotime();
                    origin_uri.path = vec![
                        m.topic().to_string(),
                        m.partition().to_string(),
                        m.offset().to_string(),
                    ];
                    send_event(
                        &pipelines,
                        &mut preprocessors,
                        &mut codec,
                        &mut metrics_reporter,
                        &mut ingest_ns,
                        &origin_uri,
                        id,
                        data.to_vec(),
                    );
                } else {
                    error!("failed to fetch data from kafka")
                }
            } else {
                error!("Failed to fetch kafka message.");
            }
        }
    }
    Ok(())
}

#[async_trait::async_trait]
impl Onramp for Kafka {
    async fn start(
        &mut self,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let (tx, rx) = channel(1);
        let config = self.config.clone();
        let codec = codec::lookup(&codec)?;
        let preprocessors = make_preprocessors(&preprocessors)?;
        task::Builder::new()
            .name(format!("onramp-kafka-{}", "???"))
            .spawn(async move {
                if let Err(e) =
                    onramp_loop(rx, &config, preprocessors, codec, metrics_reporter).await
                {
                    error!("[Onramp] Error: {}", e)
                }
            })?;
        Ok(tx)
    }
    fn default_codec(&self) -> &str {
        "json"
    }
}
