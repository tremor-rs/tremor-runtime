// Copyright 2020-2021, The Tremor Team
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
#![cfg(not(tarpaulin_include))]

use crate::errors::Result;
use crate::source::prelude::*;

//NOTE: This is required for StreamHandlers stream
use halfbrown::HashMap;
use log::Level::Debug;
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::{client::ClientContext, util::Timeout};
use rdkafka::{config::ClientConfig, TopicPartitionList};
use rdkafka::{
    consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext},
    error::KafkaError,
};
use rdkafka::{Message, Offset};
use std::collections::BTreeMap;
use std::collections::HashMap as StdMap;
use std::mem::{self};
use std::time::Duration;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// kafka group ID to register with
    pub group_id: String,
    /// List of topics to subscribe to
    pub topics: Vec<String>,
    /// List of bootstrap brokers
    pub brokers: Vec<String>,

    /// This config determines the behaviour of this source
    /// if `enable.auto.commit` is set to false in `rdkafka_options`:
    ///
    /// if set to true this source will reset the consumer offset to a
    /// failed message, so it will effectively retry those messages.
    ///
    /// If set to `false` this source will only commit the consumer offset
    /// if the message has been successfully acknowledged.
    ///
    /// This might lead to events being sent multiple times.
    /// This should not be used when you expect persistent errors (e.g. if the message content is malformed and will lead to repeated errors)
    #[serde(default = "default_retry_failed_events")]
    pub retry_failed_events: bool,

    /// poll interval to use for polling the kafka consumer in milliseconds.
    /// default is 100 ms.
    /// This is the duration to wait before polling again if no message is currently available in the queue.
    #[serde(default = "default_poll_interval")]
    pub poll_interval: u64,
    /// Optional rdkafka configuration
    ///
    /// Default settings:
    /// * `client.id` - `"tremor-<hostname>-<thread id>"`
    /// * `bootstrap.servers` - `brokers` from the config concatenated by `,`
    /// * `enable.partition.eof` - `"false"`
    /// * `session.timeout.ms` - `"6000"`
    /// * `enable.auto.commit` - `"true"`
    /// * `auto.commit.interval.ms"` - `"5000"`
    /// * `enable.auto.offset.store` - `"true"`
    pub rdkafka_options: Option<HashMap<String, String>>,
}

/// defaults to `true` to keep backwards compatibility
fn default_retry_failed_events() -> bool {
    true
}

/// 100 milliseconds
fn default_poll_interval() -> u64 {
    100
}

impl ConfigImpl for Config {}

pub struct Kafka {
    pub config: Config,
    onramp_id: TremorUrl,
}

#[derive(Debug)]
struct MsgOffset {
    topic: String,
    partition: i32,
    offset: Offset,
}
impl<'consumer> From<BorrowedMessage<'consumer>> for MsgOffset {
    fn from(m: BorrowedMessage) -> Self {
        Self {
            topic: m.topic().into(),
            partition: m.partition(),
            offset: Offset::Offset(m.offset()),
        }
    }
}

pub struct Int {
    uid: u64,
    config: Config,
    onramp_id: TremorUrl,
    consumer: Option<LoggingConsumer>,
    kafka_poll_timeout: Timeout,
    origin_uri: EventOriginUri,
    auto_commit: bool,
    messages: BTreeMap<u64, MsgOffset>,
}

impl std::fmt::Debug for Int {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Kafka")
    }
}

impl Int {
    /// get a map aggregating the highest offsets for each topic and partition
    /// for which we have messages stored up to and including the id of this message
    fn get_topic_map_for_id(&mut self, id: u64) -> StdMap<(String, i32), Offset> {
        let mut split = self.messages.split_off(&(id + 1));
        mem::swap(&mut split, &mut self.messages);
        // split now contains all messages up to and including `id`
        let mut tm = StdMap::with_capacity(split.len());
        for (
            _,
            MsgOffset {
                topic,
                partition,
                offset,
            },
        ) in split
        {
            if let Offset::Offset(msg_offset) = offset {
                // we need to commit the message offset + 1, dont ask
                // The `KafkaConsumer` javadocs say:
                //
                // Note: The committed offset should always be the offset of the next message that your application will read. Thus, when calling commitSync(offsets) you should add one to the offset of the last message processed.
                //
                // See: https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
                //
                let commit_offset = Offset::Offset(msg_offset + 1);
                let this_offset = tm.entry((topic, partition)).or_insert(commit_offset);
                if let (Offset::Offset(old), Offset::Offset(new)) = (this_offset, commit_offset) {
                    *old = (*old).max(new)
                }
            }
        }
        tm
    }
    fn from_config(uid: u64, onramp_id: TremorUrl, config: &Config) -> Self {
        let origin_uri = EventOriginUri {
            uid,
            scheme: "tremor-kafka".to_string(),
            host: "not-connected".to_string(),
            port: None,
            path: vec![],
        };

        let auto_commit = config
            .rdkafka_options
            .as_ref()
            .and_then(|m| m.get("enable.auto.commit"))
            .map_or(true, |v| v == "true");

        Self {
            uid,
            config: config.clone(),
            onramp_id,
            consumer: None,
            kafka_poll_timeout: Timeout::After(Duration::new(0, 0)), // ensure non-blocking calls by using a zero duration here
            origin_uri,
            auto_commit,
            messages: BTreeMap::new(),
        }
    }
}

impl onramp::Impl for Kafka {
    fn from_config(id: &TremorUrl, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self {
                config,
                onramp_id: id.clone(),
            }))
        } else {
            Err(format!("[Source::{}] Missing config for kafka onramp.", id).into())
        }
    }
}

// A simple context to customize the consumer behavior and print a log line every time
// offsets are committed
pub struct LoggingConsumerContext {
    onramp_id: TremorUrl,
}

impl ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
    fn commit_callback(&self, result: KafkaResult<()>, offsets: &rdkafka::TopicPartitionList) {
        match result {
            Ok(_) => {
                if offsets.count() > 0 {
                    info!(
                        "[Source::{}] Offsets committed successfully",
                        self.onramp_id
                    );
                    if log_enabled!(Debug) {
                        let offset_strings: Vec<String> = offsets
                            .elements()
                            .iter()
                            .map(|elem| {
                                format!(
                                    "[Topic: {}, Partition: {}, Offset: {:?}]",
                                    elem.topic(),
                                    elem.partition(),
                                    elem.offset()
                                )
                            })
                            .collect();
                        debug!(
                            "[Source::{}] Offsets: {}",
                            self.onramp_id,
                            offset_strings.join(" ")
                        );
                    }
                }
            }
            // this is actually not an error - we just didnt have any offset to commit
            Err(KafkaError::ConsumerCommit(rdkafka_sys::RDKafkaError::NoOffset)) => {}
            Err(e) => warn!(
                "[Source::{}] Error while committing offsets: {}",
                self.onramp_id, e
            ),
        };
    }
}
pub type LoggingConsumer = BaseConsumer<LoggingConsumerContext>;

/// ensure a zero poll timeout to have a non-blocking call

#[async_trait::async_trait()]
impl Source for Int {
    fn is_transactional(&self) -> bool {
        !self.auto_commit
    }
    fn id(&self) -> &TremorUrl {
        &self.onramp_id
    }
    async fn pull_event(&mut self, id: u64) -> Result<SourceReply> {
        if let Some(stream) = self.consumer.as_mut() {
            if let Some(Ok(m)) = stream.poll(self.kafka_poll_timeout) {
                debug!(
                    "[Source::{}] EventId: {} Offset: {}",
                    self.onramp_id,
                    id,
                    m.offset()
                );
                if let Some(Ok(data)) = m.payload_view::<[u8]>() {
                    let mut origin_uri = self.origin_uri.clone();
                    origin_uri.path = vec![
                        m.topic().to_string(),
                        m.partition().to_string(),
                        m.offset().to_string(),
                    ];
                    let data = data.to_vec();
                    let mut kafka_meta_data = Value::object_with_capacity(1);
                    let mut meta_key = None;
                    if let Some(key) = m.key() {
                        meta_key = Some(key);
                    }
                    let mut meta_headers = None;
                    if let Some(headers) = m.headers() {
                        let mut key_val = Value::object_with_capacity(headers.count());
                        for i in 0..headers.count() {
                            if let Some(header) = headers.get(i) {
                                let key = String::from(header.0);
                                let val = Value::Bytes(Vec::from(header.1).into());
                                key_val.insert(key, val)?;
                            }
                        }
                        meta_headers = Some(key_val);
                    }
                    let mut meta_data = Value::object_with_capacity(2);
                    if let Some(meta_key) = meta_key {
                        meta_data.insert("key", Value::Bytes(Vec::from(meta_key).into()))?;
                    }
                    if let Some(meta_headers) = meta_headers {
                        meta_data.insert("headers", meta_headers)?;
                    }
                    kafka_meta_data.insert("kafka", meta_data)?;

                    if !self.auto_commit {
                        self.messages.insert(id, MsgOffset::from(m));
                    }
                    Ok(SourceReply::Data {
                        origin_uri,
                        data,
                        meta: Some(kafka_meta_data),
                        codec_override: None,
                        stream: 0,
                    })
                } else {
                    error!(
                        "[Source::{}] Failed to fetch kafka message.",
                        self.onramp_id
                    );
                    Ok(SourceReply::Empty(self.config.poll_interval))
                }
            } else {
                Ok(SourceReply::Empty(self.config.poll_interval))
            }
        } else {
            Ok(SourceReply::StateChange(SourceState::Disconnected))
        }
    }

    #[allow(clippy::clippy::too_many_lines)]
    async fn init(&mut self) -> Result<SourceState> {
        let context = LoggingConsumerContext {
            onramp_id: self.onramp_id.clone(),
        };
        let mut client_config = ClientConfig::new();
        let tid = task::current().id();

        // Get the correct origin url
        let first_broker: Vec<&str> = if let Some(broker) = self.config.brokers.first() {
            broker.split(':').collect()
        } else {
            return Err(format!("[Source::{}] No brokers provided.", self.onramp_id).into());
        };
        // picking the first host for these
        let (host, port) = match first_broker.as_slice() {
            [host] => ((*host).to_string(), None),
            [host, port] => ((*host).to_string(), Some(port.parse()?)),
            _ => {
                return Err(format!(
                    "[Source::{}] Invalid broker config: {}",
                    self.onramp_id,
                    first_broker.join(":")
                )
                .into())
            }
        };
        self.origin_uri = EventOriginUri {
            uid: self.uid,
            scheme: "tremor-kafka".to_string(),
            host,
            port,
            path: vec![],
        };

        info!("[Source::{}] Starting kafka onramp", self.onramp_id);
        // Setting up the configuration with default and then overwriting
        // them with custom settings.
        //
        // ENABLE LIBRDKAFKA DEBUGGING:
        // - set librdkafka logger to debug in logger.yaml
        // - configure: debug: "all" for this onramp
        client_config
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
            .set("enable.auto.offset.store", "true");

        self.config
            .rdkafka_options
            .iter()
            .flat_map(halfbrown::HashMap::iter)
            .for_each(|(k, v)| {
                client_config.set(k, v);
            });

        debug!(
            "[Source::{}] Consuming from Kafka with config: {:?}",
            self.onramp_id, &client_config
        );

        // Set up the the consumer
        let consumer: LoggingConsumer = client_config.create_with_context(context)?;

        // Handle topics
        let topics: Vec<&str> = self
            .config
            .topics
            .iter()
            .map(std::string::String::as_str)
            .collect();
        info!("[Source::{}] Subscribing to: {:?}", self.onramp_id, topics);

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
                            "[Source::{}] Kafka error for topic '{}': {:?}. Not subscribing!",
                            self.onramp_id, topic, e
                        ),
                        _ => error!(
                            "[Source::{}] Unknown kafka error for topic '{}'. Not subscribing!",
                            self.onramp_id, topic
                        ),
                    }
                }
                Err(e) => error!(
                    "[Source::{}] Kafka error for topic '{}': {}. Not subscribing!",
                    self.onramp_id, topic, e
                ),
            };
        }

        // bail out if there is no topic left to subscribe to
        if good_topics.len() < self.config.topics.len() {
            return Err(format!(
                "[Source::{}] Unable to subscribe to all configured topics: {}",
                self.onramp_id,
                self.config.topics.join(", ")
            )
            .into());
        }

        match consumer.subscribe(&good_topics) {
            Ok(()) => info!(
                "[Source::{}] Subscribed to topics: {:?}",
                self.onramp_id, good_topics
            ),
            Err(e) => {
                error!(
                    "[Source::{}] Kafka error for topics '{:?}': {}",
                    self.onramp_id, good_topics, e
                );
                return Err(e.into());
            }
        };

        self.consumer = Some(consumer);

        Ok(SourceState::Connected)
    }
    fn trigger_breaker(&mut self) {}
    fn restore_breaker(&mut self) {}

    // If we fail a message we seek back to this failed
    // message to replay data from here.
    //
    // This might seek over multiple topics but since we internally only keep
    // track of a singular stream this is OK.
    //
    // If this is undesirable, multiple onramps with an onramp per topic
    // should be used.
    fn fail(&mut self, id: u64) {
        trace!("[Source::{}] Fail {}", self.onramp_id, id);
        if !self.auto_commit && self.config.retry_failed_events {
            let tm = self.get_topic_map_for_id(id);
            if let Some(consumer) = self.consumer.as_mut() {
                for ((topic, partition), offset) in tm {
                    if let Err(e) =
                        consumer.seek(&topic, partition, offset, self.kafka_poll_timeout)
                    {
                        error!("[Source::{}] failed to seek message: {}", self.onramp_id, e)
                    }
                }
            }
        }
    }
    fn ack(&mut self, id: u64) {
        trace!("[Source::{}] Ack {}", self.onramp_id, id);
        if !self.auto_commit {
            let tm = self.get_topic_map_for_id(id);
            if !tm.is_empty() {
                if let Some(consumer) = self.consumer.as_mut() {
                    let topic_partition_list = TopicPartitionList::from_topic_map(&tm);
                    if let Err(e) = consumer.commit(&topic_partition_list, CommitMode::Async) {
                        error!(
                            "[Source::{}] failed to commit message: {}",
                            self.onramp_id, e
                        )
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Onramp for Kafka {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        let source = Int::from_config(config.onramp_uid, self.onramp_id.clone(), &self.config);
        SourceManager::start(source, config).await
    }
    fn default_codec(&self) -> &str {
        "json"
    }
}
