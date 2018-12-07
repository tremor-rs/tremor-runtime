// Copyright 2018, Wayfair GmbH
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

//! # Kafka Onramp
//!
//! The `kafka` onramp allows receiving events from a kafka queue.
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
//!

use dflt;
use errors::*;
use futures::prelude::*;
use futures::sync::mpsc::channel;
use hostname::get_hostname;
use onramp::{EnterReturn, Onramp as OnrampT, PipelineOnramp};
use pipeline::prelude::*;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::error::KafkaResult;
use rdkafka::Message;
use rdkafka_sys;
use serde_yaml;
use std::collections::HashMap;
use std::thread;
use utils;

pub struct Onramp {
    config: Config,
}
// A simple context to customize the consumer behavior and print a log line every time
// offsets are committed
pub struct LoggingConsumerContext;

impl ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
    fn commit_callback(
        &self,
        result: KafkaResult<()>,
        _offsets: *mut rdkafka_sys::RDKafkaTopicPartitionList,
    ) {
        match result {
            Ok(_) => info!("Offsets committed successfully"),
            Err(e) => warn!("Error while committing offsets: {}", e),
        };
    }
}

#[derive(Clone, Deserialize)]
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
// Define a new type for convenience
pub type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;
impl Onramp {
    pub fn new(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        Ok(Self { config })
    }
}

impl OnrampT for Onramp {
    fn enter_loop(&mut self, pipelines: PipelineOnramp) -> EnterReturn {
        let hostname = match get_hostname() {
            Some(h) => h,
            None => "tremor-host.local".to_string(),
        };
        let config = self.config.clone();
        let pipelines = pipelines.clone();
        let hostname = hostname.clone();
        let tid = 0; //TODO: get a good thread id
        let len = pipelines.len();
        thread::spawn(move || {
            let context = LoggingConsumerContext;
            let mut consumer = ClientConfig::new();
            let i = 0;
            let consumer = consumer
                .set("group.id", &config.group_id)
                .set("client.id", &format!("tremor-{}-{}", hostname, tid))
                .set("bootstrap.servers", &config.brokers.join(","))
                .set("enable.partition.eof", "false")
                .set("session.timeout.ms", "6000")
                // Commit automatically every 5 seconds.
                .set("enable.auto.commit", "true")
                .set("auto.commit.interval.ms", "5000")
                // but only commit the offsets explicitly stored via `consumer.store_offset`.
                .set("enable.auto.offset.store", "true")
                .set_log_level(RDKafkaLogLevel::Debug);

            let consumer: LoggingConsumer = if let Some(options) = config.rdkafka_options {
                options
                    .iter()
                    .fold(consumer, |c: &mut ClientConfig, (k, v)| c.set(k, v))
                    .create_with_context(context)
                    .expect("Consumer creation failed")
            } else {
                consumer
                    .create_with_context(context)
                    .expect("Consumer creation failed")
            };
            let topics: Vec<&str> = config.topics.iter().map(|topic| topic.as_str()).collect();
            consumer
                .subscribe(&topics)
                .expect("Can't subscribe to specified topic");
            for message in consumer.start().wait() {
                match message {
                    Err(_e) => {
                        warn!("Onramp error");
                    }
                    Ok(Err(_m)) => {
                        warn!("Onramp error");
                    }
                    Ok(Ok(m)) => {
                        // Send a copy to the message to every output topic in parallel, and wait for the
                        // delivery report to be received.
                        if let Some(Ok(p)) = m.payload_view::<[u8]>() {
                            let mut vars = HashMap::new();
                            if let Some(key) = m.key_view::<str>() {
                                vars.insert("key".to_string(), key.unwrap().into());
                            };
                            let i = i + 1 % len;

                            if config.sync {
                                // In sync mode we create a channel to wait for a reply
                                // until we get this reply we will block!
                                let (tx, rx) = channel(0);
                                // TODO: How do we track success on finished events?
                                let msg = OnData {
                                    reply_channel: Some(tx),
                                    data: EventValue::Raw(p.to_vec()),
                                    vars,
                                    ingest_ns: utils::nanotime(),
                                };

                                pipelines[i].do_send(msg);
                                for _r in rx.wait() {}
                            } else {
                                // In async mode we just eat through the data without waiting
                                // for any replies from the pipeline
                                let msg = OnData {
                                    reply_channel: None,
                                    data: EventValue::Raw(p.to_vec()),
                                    vars,
                                    ingest_ns: utils::nanotime(),
                                };

                                pipelines[i].do_send(msg);
                            }
                        }
                    }
                }
            }
        })
    }
}
