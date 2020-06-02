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
use rdkafka::consumer::stream_consumer::{self, StreamConsumer};
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
    id: String,
}

pub struct KafkaInt {
    config: Config,
    onramp_id: String,
    stream: Option<rentals::MessageStream>,
    origin_uri: EventOriginUri,
}

impl std::fmt::Debug for KafkaInt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Kafka")
    }
}

rental! {
    pub mod rentals {
        use super::{LoggingConsumerContext, LoggingConsumer};
        use rdkafka::consumer::stream_consumer;

        #[rental(covariant)]
        pub struct MessageStream {
            consumer: Box<LoggingConsumer>,
            stream: stream_consumer::MessageStream<'consumer, LoggingConsumerContext>,
        }
    }
}
#[allow(dead_code)]
impl rentals::MessageStream {
    #[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr, clippy::mut_from_ref)]
    unsafe fn mut_suffix(
        &self,
    ) -> &mut stream_consumer::MessageStream<'static, LoggingConsumerContext> {
        std::mem::transmute(self.suffix())
    }
}

impl KafkaInt {
    fn from_config(config: &Config, id: &str) -> Self {
        let origin_uri = tremor_pipeline::EventOriginUri {
            scheme: "tremor-kafka".to_string(),
            // picking the first host for these
            host: "not-connected".to_string(),
            port: None,
            path: vec![],
        };

        Self {
            config: config.clone(),
            onramp_id: id.to_string(),
            stream: None,
            origin_uri,
        }
    }
}

impl onramp::Impl for Kafka {
    fn from_config(id: &str, config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self {
                config,
                id: id.to_string(),
            }))
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

#[async_trait::async_trait()]
impl Source for KafkaInt {
    async fn read(&mut self) -> Result<SourceReply> {
        if let Some(stream) = self
            .stream
            .as_mut()
            .map(|stream| unsafe { stream.mut_suffix() })
        {
            if let Some(Ok(m)) = stream.next().await {
                if let Some(Ok(data)) = m.payload_view::<[u8]>() {
                    let mut origin_uri = self.origin_uri.clone();
                    origin_uri.path = vec![
                        m.topic().to_string(),
                        m.partition().to_string(),
                        m.offset().to_string(),
                    ];
                    Ok(SourceReply::Data {
                        origin_uri: origin_uri,
                        data: data.to_vec(),
                        stream: 0,
                    })
                } else {
                    error!("Failed to fetch kafka message.");
                    Ok(SourceReply::Empty(100))
                }
            } else {
                Ok(SourceReply::Empty(100))
            }
        } else {
            Ok(SourceReply::StateChange(SourceState::Disconnected))
        }
    }
    async fn init(&mut self) -> Result<SourceState> {
        let context = LoggingConsumerContext;
        let mut client_config = ClientConfig::new();
        let tid = task::current().id();

        // Get the correct origin url
        let first_broker: Vec<&str> = if let Some(broker) = self.config.brokers.first() {
            broker.split(':').collect()
        } else {
            return Err(format!("No brokers provided for Kafka onramp {}", self.onramp_id).into());
        };
        self.origin_uri = tremor_pipeline::EventOriginUri {
            scheme: "tremor-kafka".to_string(),
            // picking the first host for these
            host: first_broker[0].to_string(),
            port: match first_broker.get(1) {
                Some(n) => Some(n.parse()?),
                None => None,
            },
            path: vec![],
        };

        info!("Starting kafka onramp {}", self.onramp_id);
        // Setting up the configuration with default and then overwriting
        // them with custom settings.
        let client_config = client_config
            .set("group.id", &self.config.group_id)
            .set(
                "client.id",
                &format!("tremor-{}-{}-{:?}", hostname(), self.onramp_id, tid),
            )
            .set("bootstrap.servers", &self.config.brokers.join(","))
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            // Commit automatically every 5 seconds.
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "5000")
            // but only commit the offsets explicitly stored via `consumer.store_offset`.
            .set("enable.auto.offset.store", "true")
            .set_log_level(RDKafkaLogLevel::Debug);

        let client_config = if let Some(options) = self.config.rdkafka_options.clone() {
            options
                .iter()
                .fold(client_config, |c: &mut ClientConfig, (k, v)| c.set(k, v))
        } else {
            client_config
        };

        let client_config = client_config.to_owned();

        // Set up the the consumer
        let consumer: LoggingConsumer = client_config.create_with_context(context)?;

        // Handle topics
        let topics: Vec<&str> = self
            .config
            .topics
            .iter()
            .map(std::string::String::as_str)
            .collect();
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

        let stream = rentals::MessageStream::new(Box::new(consumer), |consumer| consumer.start());
        self.stream = Some(stream);

        Ok(SourceState::Connected)
    }
}

#[async_trait::async_trait]
impl Onramp for Kafka {
    async fn start(
        &mut self,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let source = KafkaInt::from_config(&self.config, &self.id);
        let (manager, tx) =
            SourceManager::new(source, preprocessors, codec, metrics_reporter).await?;
        thread::Builder::new()
            .name(format!("on-kafka-{}", self.id))
            .spawn(move || task::block_on(manager.run()))?;
        Ok(tx)
    }
    fn default_codec(&self) -> &str {
        "json"
    }
}
