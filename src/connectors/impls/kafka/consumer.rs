// Copyright 2021, The Tremor Team
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

use std::sync::Arc;
use std::time::Duration;

use super::SmolRuntime;
use crate::connectors::prelude::*;
use async_std::channel::{bounded, Receiver, Sender, TryRecvError};
use async_std::stream::StreamExt;
use async_std::task::{self, JoinHandle};
use halfbrown::HashMap;
use indexmap::IndexMap;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::{ClientContext, Offset, TopicPartitionList};
use rdkafka_sys::RDKafkaErrorCode;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// consumer group id to register with
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
    /// This should not be used when persistent errors are expected (e.g. if the message content is malformed and will lead to repeated errors)
    #[serde(default = "Default::default")]
    pub retry_failed_events: bool,
    /// Optional rdkafka configuration
    pub rdkafka_options: Option<HashMap<String, String>>,
}

impl ConfigImpl for Config {}

#[derive(Default, Debug)]
pub(crate) struct Builder {}

impl Builder {
    fn verify_brokers(id: &str, brokers: &Vec<String>) -> Result<(String, Option<u16>)> {
        let mut first_broker: Option<(String, Option<u16>)> = None;
        for broker in brokers {
            match broker.split(':').collect::<Vec<_>>().as_slice() {
                [host] => {
                    first_broker.get_or_insert_with(|| ((*host).to_string(), None));
                }
                [host, port] => {
                    let port: u16 = port.parse().map_err(|_| {
                        Error::from(ErrorKind::InvalidConfiguration(
                            id.to_string(),
                            format!("Invalid broker: {}:{}", host, port),
                        ))
                    })?;
                    first_broker.get_or_insert_with(|| ((*host).to_string(), Some(port)));
                }
                b => {
                    return Err(ErrorKind::InvalidConfiguration(
                        id.to_string(),
                        format!("Invalid broker: {}", b.join(":")),
                    )
                    .into())
                }
            }
        }
        first_broker.ok_or_else(|| {
            ErrorKind::InvalidConfiguration(id.to_string(), "Missing brokers.".to_string()).into()
        })
    }
}
#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "kafka_consumer".into()
    }

    async fn from_config(
        &self,
        alias: &str,
        config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(raw_config) = config {
            let config = Config::new(raw_config)?;
            // returns the first broker if all are valid
            let (host, port) = Self::verify_brokers(alias, &config.brokers)?;
            let origin_uri = EventOriginUri {
                scheme: "tremor-kafka".to_string(),
                host,
                port,
                path: vec![],
            };

            let tid = task::current().id();
            let mut client_config = ClientConfig::new();
            let client_id = format!("tremor-{}-{}-{:?}", hostname(), alias, tid);
            client_config
                .set("group.id", config.group_id.clone())
                .set("client.id", &client_id)
                .set("bootstrap.servers", &config.brokers.join(","))
                .set("enable.partition.eof", "false")
                .set("session.timeout.ms", "6000")
                .set("enable.auto.commit", "true")
                .set("auto.commit.interval.ms", "5000")
                .set("enable.auto.offset.store", "true");
            config
                .rdkafka_options
                .iter()
                .flat_map(halfbrown::HashMap::iter)
                .for_each(|(k, v)| {
                    client_config.set(k, v);
                });

            debug!(
                "[Connector::{}] Kafka Consumer Config: {:?}",
                alias, &client_config
            );

            Ok(Box::new(KafkaConsumerConnector {
                config,
                client_config,
                origin_uri,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(alias.to_string()).into())
        }
    }
}

struct TremorConsumerContext {
    ctx: SourceContext,
}

impl ClientContext for TremorConsumerContext {
    fn stats(&self, statistics: rdkafka::Statistics) {
        // FIXME: expose as metrics to the source
        info!("{} Client stats: {:?}", &self.ctx, statistics);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        error!("{} Kafka Error {}: {}", &self.ctx, error, reason);
        // check for errors that manifest a non-working consumer, so we bail out
        if let KafkaError::Subscription(_)
            | KafkaError::ClientConfig(_, _, _, _)
            | KafkaError::ClientCreation(_)
            // TODO: what else?
            | KafkaError::Global(RDKafkaErrorCode::UnknownTopicOrPartition | RDKafkaErrorCode::UnknownTopic) =
            error
        {
            let notifier = self.ctx.notifier().clone();
            let alias = self.ctx.alias.clone();
            async_std::task::spawn(async move {
                if let Err(e) = notifier.notify().await {
                    error!(
                        "[Source::{}] Error notifying the connector of a failure: {}",
                        &alias, e
                    );
                }
            });
        }
    }
}

impl ConsumerContext for TremorConsumerContext {
    fn post_rebalance<'a>(&self, _rebalance: &rdkafka::consumer::Rebalance<'a>) {}

    fn commit_callback(
        &self,
        _result: rdkafka::error::KafkaResult<()>,
        _offsets: &rdkafka::TopicPartitionList,
    ) {
    }
}

impl From<&SourceContext> for TremorConsumerContext {
    fn from(source_ctx: &SourceContext) -> Self {
        Self {
            ctx: source_ctx.clone(),
        }
    }
}

type TremorConsumer = StreamConsumer<TremorConsumerContext, SmolRuntime>;

struct KafkaConsumerConnector {
    config: Config,
    client_config: ClientConfig,
    origin_uri: EventOriginUri,
}

#[async_trait::async_trait()]
impl Connector for KafkaConsumerConnector {
    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = KafkaConsumerSource::new(
            self.config.clone(),
            self.client_config.clone(),
            self.origin_uri.clone(),
        );
        builder.spawn(source, source_context).map(Some)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Optional("json")
    }
}

fn kafka_meta<'a>(msg: &BorrowedMessage<'a>) -> Value<'static> {
    let headers = msg.headers().map(|headers| {
        let mut headers_meta = Value::object_with_capacity(headers.count());
        for i in 0..headers.count() {
            if let Some(header) = headers.get(i) {
                let key = String::from(header.0);
                let val = Value::Bytes(Vec::from(header.1).into());
                headers_meta.try_insert(key, val);
            }
        }
        headers_meta
    });
    literal!({
        "kafka": {
            "key": msg.key().map(|s| Value::Bytes(Vec::from(s).into())),
            "headers": headers,
            "topic": msg.topic().to_string(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "timestamp": msg.timestamp().to_millis().map(|ms| ms * 1_000_000), // convert to nanos
        }
    })
}

struct KafkaConsumerSource {
    client_config: ClientConfig,
    origin_uri: EventOriginUri,
    topics: Vec<String>,
    topic_resolver: TopicResolver,
    transactional: bool,
    retry_failed_events: bool,
    seek_timeout: Duration,
    source_tx: Sender<(SourceReply, Option<u64>)>,
    source_rx: Receiver<(SourceReply, Option<u64>)>,
    consumer: Option<Arc<TremorConsumer>>,
    consumer_task: Option<JoinHandle<Result<()>>>,
}

impl KafkaConsumerSource {
    const DEFAULT_SEEK_TIMEOUT: Duration = Duration::from_millis(500);

    fn new(config: Config, client_config: ClientConfig, origin_uri: EventOriginUri) -> Self {
        let Config {
            topics,
            retry_failed_events,
            ..
        } = config;
        let topic_resolver = TopicResolver::new(topics.clone());
        let auto_commit = client_config
            .get("enable.auto.commit")
            .map_or(true, |v| v == "true");
        let seek_timeout = client_config
            // this will put the default from kafka if not present
            .create_native_config()
            .and_then(|c| c.get("max.poll.interval.ms"))
            .map(|poll_interval| poll_interval.parse().unwrap_or(500_u64) / 2)
            .map(Duration::from_millis)
            .unwrap_or(Self::DEFAULT_SEEK_TIMEOUT);

        let (source_tx, source_rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));
        Self {
            client_config,
            origin_uri,
            topics,
            topic_resolver,
            transactional: !auto_commit,
            retry_failed_events,
            seek_timeout,
            source_tx,
            source_rx,
            consumer: None,
            consumer_task: None,
        }
    }
}

#[async_trait::async_trait()]
impl Source for KafkaConsumerSource {
    async fn connect(&mut self, ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        if let Some(consumer_task) = self.consumer_task.take() {
            consumer_task.cancel().await;
        }
        let (version_n, version_s) = rdkafka::util::get_rdkafka_version();
        info!(
            "{} Connecting using rdkafka -1x{:08x}, {}",
            &ctx, version_n, version_s
        );
        let consumer_context = TremorConsumerContext::from(ctx);
        let consumer: TremorConsumer = self.client_config.create_with_context(consumer_context)?;

        let topics: Vec<&str> = self
            .topics
            .iter()
            .map(std::string::String::as_str)
            .collect();
        info!("{} Subscribing to: {:?}", &ctx, topics);

        match consumer.subscribe(&topics) {
            Ok(()) => info!("{} Subscription initiated...", &ctx),
            Err(e) => {
                error!("{} Error subscribing: {}", ctx, e);
                return Err(e.into());
            }
        };
        let arc_consumer = Arc::new(consumer);
        let task_consumer = arc_consumer.clone();
        self.consumer = Some(arc_consumer);

        let source_tx = self.source_tx.clone();
        let source_ctx = ctx.clone();
        let consumer_origin_uri = self.origin_uri.clone();
        let topic_resolver = self.topic_resolver.clone();
        let handle = task::spawn(async move {
            let mut stream = task_consumer.stream();
            loop {
                match stream.next().await {
                    Some(Ok(kafka_msg)) => {
                        // handle kafka msg
                        let (stream_id, pull_id) =
                            topic_resolver.resolve_stream_and_pull_ids(&kafka_msg);
                        let mut origin_uri = consumer_origin_uri.clone();
                        origin_uri.path = vec![
                            kafka_msg.topic().to_string(),
                            kafka_msg.partition().to_string(),
                            kafka_msg.offset().to_string(),
                        ];
                        let data: Vec<u8> = kafka_msg
                            .payload()
                            .map(|slice| slice.to_vec())
                            .unwrap_or_default();

                        let meta = kafka_meta(&kafka_msg);
                        let reply = SourceReply::Data {
                            origin_uri,
                            data,
                            meta: Some(meta),
                            stream: stream_id,
                            port: Some(OUT),
                        };
                        source_tx.send((reply, Some(pull_id))).await?;
                    }
                    Some(Err(e)) => {
                        // handle kafka error
                        match e {
                            // Those we consider fatal
                            KafkaError::MessageConsumption(
                                e @ (RDKafkaErrorCode::UnknownTopicOrPartition
                                | RDKafkaErrorCode::TopicAuthorizationFailed
                                | RDKafkaErrorCode::UnknownTopic),
                            ) => {
                                error!("{} Subscription failed: {}.", &source_ctx, e);
                                // Initiate reconnect (if configured)
                                source_ctx.notifier().notify().await?;
                                break;
                            }
                            err => {
                                // TODO: gather some more fatal errors that require a reconnect
                                debug!("{} Error consuming from kafka: {}", &source_ctx, err);
                            }
                        }
                    }
                    None => {
                        // handle kafka being done
                        // this shouldn't happen
                        warn!(
                            "{} Consumer is done consuming. Initiating reconnect...",
                            &source_ctx
                        );
                        source_ctx.notifier().notify().await?;
                        break;
                    }
                }
            }
            Ok(())
        });
        self.consumer_task = Some(handle);

        Ok(true)
    }

    async fn pull_data(&mut self, pull_id: &mut u64, ctx: &SourceContext) -> Result<SourceReply> {
        match self.source_rx.try_recv() {
            Ok((reply, custom_pull_id)) => {
                if let Some(custom_pull_id) = custom_pull_id {
                    *pull_id = custom_pull_id;
                }
                Ok(reply)
            }
            Err(TryRecvError::Empty) => Ok(SourceReply::Empty(DEFAULT_POLL_INTERVAL)),
            Err(TryRecvError::Closed) => {
                error!("{} Consumer unavailable. Initiating Reconnect...", &ctx);
                ctx.notifier().notify().await?;
                return Err("Consumer unavailable.".into());
            }
        }
    }

    async fn ack(&mut self, stream_id: u64, pull_id: u64, ctx: &SourceContext) -> Result<()> {
        if self.transactional {
            if let Some(consumer) = self.consumer.as_ref() {
                let _ = consumer.client();
                if let Some((topic, partition, offset)) =
                    self.topic_resolver.resolve_topic(stream_id, pull_id)
                {
                    let mut tpl: TopicPartitionList = TopicPartitionList::with_capacity(1);
                    tpl.add_partition_offset(topic, partition, offset)?;
                    consumer.commit(&tpl, CommitMode::Async)?;
                } else {
                    error!("{} Could not ack event with stream={}, pull_id={}. Unable to detect topic from internal state.", &ctx, stream_id, pull_id);
                }
            }
        }
        Ok(())
    }

    async fn fail(&mut self, stream_id: u64, pull_id: u64, ctx: &SourceContext) -> Result<()> {
        if self.transactional && self.retry_failed_events {
            if let Some(consumer) = self.consumer.as_ref() {
                if let Some((topic, partition, offset)) =
                    self.topic_resolver.resolve_topic(stream_id, pull_id)
                {
                    consumer.seek(topic, partition, offset, self.seek_timeout)?;
                } else {
                    error!("{} Could not seek back to failed event with stream={}, pull_id={}. Unable to detect topic from internal state.", &ctx, stream_id, pull_id);
                }
            }
        }
        Ok(())
    }

    async fn on_cb_close(&mut self, _ctx: &SourceContext) -> Result<()> {
        if let Some(consumer) = self.consumer.as_ref() {
            consumer
                .assignment()
                .and_then(|partitions| consumer.pause(&partitions))?;
        }
        Ok(())
    }
    async fn on_cb_open(&mut self, _ctx: &SourceContext) -> Result<()> {
        if let Some(consumer) = self.consumer.as_ref() {
            consumer
                .assignment()
                .and_then(|partitions| consumer.resume(&partitions))?;
        }
        Ok(())
    }

    async fn on_pause(&mut self, _ctx: &SourceContext) -> Result<()> {
        if let Some(consumer) = self.consumer.as_ref() {
            consumer
                .assignment()
                .and_then(|partitions| consumer.pause(&partitions))?;
        }
        Ok(())
    }

    async fn on_resume(&mut self, _ctx: &SourceContext) -> Result<()> {
        if let Some(consumer) = self.consumer.as_ref() {
            consumer
                .assignment()
                .and_then(|partitions| consumer.resume(&partitions))?;
        }
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &SourceContext) -> Result<()> {
        // stop the consumer task
        if let Some(consumer_task) = self.consumer_task.take() {
            consumer_task.cancel().await;
        }
        // clear out the consumer
        self.consumer = None;
        Ok(())
    }

    fn is_transactional(&self) -> bool {
        self.transactional
    }

    fn asynchronous(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct TopicResolver(IndexMap<String, u64>);
impl TopicResolver {
    fn new(mut topics: Vec<String>) -> Self {
        // we sort topics alphabetically, to ensure reproducability in case of different orderings in configs
        // and then put them into an index map, to be able to get the id from the topic name and the topic name from the insertion index, which should be equal to the id
        topics.sort();
        Self(
            topics
                .into_iter()
                .enumerate()
                .map(|(num, topic)| (topic, num as u64))
                .collect(),
        )
    }

    /// Resolve topic, partition and message offset for the given `stream_id` and `pull_id`
    fn resolve_topic(&self, stream_id: u64, pull_id: u64) -> Option<(&str, i32, Offset)> {
        let topic_id = stream_id >> 32;
        let partition = (stream_id & 0xfffffffe) as i32;
        // insertion order should be the same as the actual indices,
        // so the index lookup can be misused as a reverse lookup
        self.0.get_index(topic_id as usize).map(|(topic, idx)| {
            // sanity check that the indices are set up correctly
            debug_assert_eq!(
                *idx, topic_id,
                "topic_id and retrieved idx are not equal in topic_indices map"
            );
            (topic.as_str(), partition, Offset::Offset(pull_id as i64))
        })
    }

    /// Resolve stream_id and pull_id for the given kafka_msg
    ///
    /// With the help of the topic index we use the 32 bit partition id
    /// and the topic index to form a stream id to identify the partition of a topic.
    ///
    /// The pull id only needs to be unique per stream
    /// with a stream being a partition, we can simply use the offset.
    fn resolve_stream_and_pull_ids(&self, kafka_msg: &BorrowedMessage<'_>) -> (u64, u64) {
        let partition = kafka_msg.partition() as u64;
        let topic = kafka_msg.topic();
        let offset = kafka_msg.offset();
        let stream_id = self.0.get(topic).copied().unwrap_or_default() & (partition << 32);
        (stream_id, offset as u64)
    }
}
