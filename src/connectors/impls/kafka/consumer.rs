// Copyright 2022, The Tremor Team
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

use async_std::sync::Arc;
use beef::Cow;
use std::time::Duration;

use super::SmolRuntime;
use crate::connectors::impls::kafka::{is_failed_connect_error, KAFKA_CONNECT_TIMEOUT};
use crate::connectors::prelude::*;
use crate::connectors::utils::metrics::make_metrics_payload;
use async_broadcast::{broadcast, Receiver as BroadcastReceiver, Sender as BroadcastSender};
use async_std::channel::{bounded, Receiver, Sender, TryRecvError};
use async_std::prelude::{FutureExt, StreamExt};
use async_std::task::{self, JoinHandle};
use halfbrown::HashMap;
use indexmap::IndexMap;
use log::Level::Debug;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::{ClientContext, Offset, TopicPartitionList};
use rdkafka_sys::RDKafkaErrorCode;

const KAFKA_CONSUMER_META_KEY: &'static str = "kafka_consumer";

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

#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "kafka_consumer".into()
    }

    async fn from_config(
        &self,
        alias: &str,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn Connector>> {
        let metrics_interval_s = config.metrics_interval_s;
        if let Some(raw_config) = &config.config {
            let config = Config::new(raw_config)?;
            // returns the first broker if all are valid
            let (host, port) = super::verify_brokers(alias, &config.brokers)?;
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

            if let Some(metrics_interval_s) = metrics_interval_s {
                // enable stats collection
                client_config.set(
                    "statistics.interval.ms",
                    format!("{}", metrics_interval_s * 1000),
                );
            }
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

#[derive(Debug, Clone)]
struct KafkaStats {}

struct TremorConsumerContext {
    ctx: SourceContext,
    connect_tx: Sender<Result<bool>>,
    metrics_tx: BroadcastSender<EventPayload>,
}

impl ClientContext for TremorConsumerContext {
    fn stats(&self, stats: rdkafka::Statistics) {
        // expose as metrics to the source

        if stats.client_type.eq("consumer") {
            let timestamp = stats.time as u64 * 1_000_000_000;

            // consumer stats
            let mut fields = HashMap::with_capacity(4);
            fields.insert(Cow::const_str("rx_msgs"), Value::from(stats.rxmsgs));
            fields.insert(
                Cow::const_str("rx_msg_bytes"),
                Value::from(stats.rxmsg_bytes),
            );
            if let Some(cg) = stats.cgrp {
                fields.insert(
                    Cow::const_str("partitions_assigned"),
                    Value::from(cg.assignment_size),
                );
            }
            let mut consumer_lag = 0_i64;
            for (_name, topic) in &stats.topics {
                for (_index, partition) in &topic.partitions {
                    if partition.consumer_lag >= 0 {
                        consumer_lag += partition.consumer_lag;
                    }
                }
            }
            fields.insert(Cow::const_str("consumer_lag"), Value::from(consumer_lag));
            let mut tags = HashMap::with_capacity(1);
            tags.insert(
                Cow::const_str("connector"),
                Value::from(self.ctx.alias.clone()),
            );
            let metrics_payload =
                make_metrics_payload("kafka_consumer_stats", fields, tags, timestamp);

            if let Err(e) = self.metrics_tx.try_broadcast(metrics_payload) {
                warn!("{} Error sending kafka statistics: {}", &self.ctx, e);
            }
        }
    }

    fn error(&self, error: KafkaError, reason: &str) {
        error!("{} Kafka Error {}: {}", &self.ctx, error, reason);
        // check for errors that manifest a non-working consumer, so we bail out
        if matches!(&error, KafkaError::Subscription(_)) || is_failed_connect_error(&error) {
            if !self.connect_tx.is_closed() {
                // we are in the connect phase - channel is still open, so notify the connect method of an error
                if let Err(e) = self.connect_tx.try_send(Err(error.into())) {
                    error!(
                        "{} Error notifying the connect method of a failure: {e}",
                        self.ctx
                    );
                } else {
                    self.connect_tx.close();
                }
            } else {
                // issue a reconnect upon fatal errors
                let notifier = self.ctx.notifier().clone();
                task::spawn(async move {
                    notifier.notify().await?;
                    Ok::<(), Error>(())
                });
            }
        }
    }
}

impl ConsumerContext for TremorConsumerContext {
    fn post_rebalance<'a>(&self, rebalance: &rdkafka::consumer::Rebalance<'a>) {
        match rebalance {
            Rebalance::Assign(tpl) => {
                let offset_strings: Vec<String> = tpl
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
                // if we got something assigned, this is a good indicator that we are connected
                if !offset_strings.is_empty() {
                    let _ = self.connect_tx.try_send(Ok(true));
                }
                info!(
                    "{} Rebalance Assigned: {}",
                    &self.ctx,
                    offset_strings.join(" ")
                );
            }
            Rebalance::Revoke(tpl) => {
                let offset_strings: Vec<String> = tpl
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
                info!(
                    "{} Rebalance Revoked: {}",
                    &self.ctx,
                    offset_strings.join(" ")
                );
            }
            Rebalance::Error(err_info) => {
                warn!("{} Post Rebalance error {}", &self.ctx, err_info);
            }
        }
    }

    fn commit_callback(
        &self,
        result: rdkafka::error::KafkaResult<()>,
        offsets: &rdkafka::TopicPartitionList,
    ) {
        match result {
            Ok(_) => {
                if offsets.count() > 0 {
                    debug!("{} Offsets committed successfully", &self.ctx);
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
                        debug!("{} Offsets: {}", &self.ctx, offset_strings.join(" "));
                    }
                }
            }
            // this is actually not an error - we just didnt have any offset to commit
            Err(KafkaError::ConsumerCommit(rdkafka_sys::RDKafkaErrorCode::NoOffset)) => {}
            Err(e) => warn!("{} Error while committing offsets: {}", &self.ctx, e),
        };
    }
}

impl TremorConsumerContext {
    fn new(
        source_ctx: &SourceContext,
        connect_tx: Sender<Result<bool>>,
        metrics_tx: BroadcastSender<EventPayload>,
    ) -> Self {
        Self {
            ctx: source_ctx.clone(),
            connect_tx,
            metrics_tx,
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
        CodecReq::Required
    }
}

fn kafka_meta<'a>(msg: &BorrowedMessage<'a>) -> Value<'static> {
    let headers = msg.headers().map(|headers| {
        let mut headers_meta = Value::object_with_capacity(headers.count());
        for i in 0..headers.count() {
            if let Some(header) = headers.get(i) {
                let key = String::from(header.0);
                let val = Value::Bytes(header.1.to_vec().into());
                headers_meta.try_insert(key, val);
            }
        }
        headers_meta
    });
    literal!({
        KAFKA_CONSUMER_META_KEY: {
            "key": msg.key().map(|s| Value::Bytes(s.to_vec().into())),
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
    metrics_rx: Option<BroadcastReceiver<EventPayload>>,
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
        // we only ever want to report on the latest metrics and discard old ones
        // if no messages arrive, no metrics will be reported, so be it.
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
            metrics_rx: None,
        }
    }
}

#[async_trait::async_trait()]
impl Source for KafkaConsumerSource {
    async fn connect(&mut self, ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        if let Some(consumer_task) = self.consumer_task.take() {
            if let Some(consumer) = self.consumer.take() {
                consumer.unsubscribe();
                drop(consumer);
            }
            consumer_task.cancel().await;
        }
        let (version_n, version_s) = rdkafka::util::get_rdkafka_version();
        info!(
            "{} Connecting using rdkafka 0x{:08x}, {}",
            &ctx, version_n, version_s
        );
        let (connect_result_tx, connect_result_rx) = bounded(1);
        let (mut metrics_tx, metrics_rx) = broadcast(1);
        metrics_tx.set_overflow(true);
        self.metrics_rx = Some(metrics_rx);
        let consumer_context =
            TremorConsumerContext::new(ctx, connect_result_tx.clone(), metrics_tx);
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
            info!("{} Consumer started.", &source_ctx);
            let mut stream = task_consumer.stream();
            let mut connect_result_channel = Some(connect_result_tx);

            loop {
                match stream.next().await {
                    Some(Ok(kafka_msg)) => {
                        //debug!("{source_ctx} Received kafka msg: {kafka_msg:?}");
                        if let Some(tx) = connect_result_channel.take() {
                            let _ = tx.try_send(Ok(true));
                        }
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

                                if let Some(tx) = connect_result_channel.take() {
                                    let _ = tx.try_send(Err("Subscription failed".into()));
                                } else {
                                    // Initiate reconnect (if configured)
                                    source_ctx.notifier().notify().await?;
                                }
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
                        if let Some(tx) = connect_result_channel.take() {
                            let _ = tx.try_send(Err("Consumer done".into()));
                        } else {
                            source_ctx.notifier().notify().await?;
                        }
                        break;
                    }
                }
            }
            Ok(())
        });
        self.consumer_task = Some(handle);

        let res = connect_result_rx
            .recv()
            .timeout(KAFKA_CONNECT_TIMEOUT)
            .await;
        match res {
            Err(_timeout) => {
                // all good, we didn't receive an error, so let's assume we are fine
                Ok(true)
            }
            Ok(Err(e)) => {
                // receive error - bail out
                Err(e.into())
            }
            Ok(Ok(connected)) => connected,
        }
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
                    debug!(
                        "{} Failing: [topic={}, partition={} offset={:?}]",
                        &ctx, topic, partition, offset
                    );
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

    async fn on_stop(&mut self, ctx: &SourceContext) -> Result<()> {
        // clear out the consumer
        if let Some(consumer) = self.consumer.take() {
            consumer.unsubscribe();
            drop(consumer);
        }
        // stop the consumer task
        if let Some(consumer_task) = self.consumer_task.take() {
            consumer_task.cancel().await;
            info!("{} Consumer stopped.", &ctx);
        }
        Ok(())
    }

    fn is_transactional(&self) -> bool {
        self.transactional
    }

    fn asynchronous(&self) -> bool {
        true
    }

    fn metrics(&mut self, _timestamp: u64, _ctx: &SourceContext) -> Vec<EventPayload> {
        if let Some(metrics_rx) = self.metrics_rx.as_mut() {
            let mut vec = Vec::with_capacity(metrics_rx.len());
            while let Ok(payload) = metrics_rx.try_recv() {
                vec.push(payload);
            }
            vec
        } else {
            vec![]
        }
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
        let partition = (stream_id >> 32) as i32;
        let topic_id = stream_id & 0xffffffff;

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
        let partition = kafka_msg.partition();
        let topic = kafka_msg.topic();
        let offset = kafka_msg.offset();
        self.resolve_stream_and_pull_ids_inner(topic, partition, offset)
    }

    fn resolve_stream_and_pull_ids_inner(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> (u64, u64) {
        let topic_idx = self.0.get(topic).copied().unwrap_or_default();
        let stream_id = topic_idx | ((partition as u64) << 32);
        (stream_id, offset as u64)
    }
}

#[cfg(test)]
mod test {

    use super::{Offset, TopicResolver};
    use proptest::prelude::*;

    fn topics_and_index() -> BoxedStrategy<(Vec<String>, usize)> {
        proptest::collection::hash_set(proptest::string::string_regex(".+").unwrap(), 1..100_usize)
            .prop_flat_map(|topics| {
                let len = topics.len();
                (Just(topics.into_iter().collect::<Vec<_>>()), 0..len)
            })
            .boxed()
    }
    proptest! {
        #[test]
        fn topic_resolver_prop(
            (topics, topic_idx) in topics_and_index(),
            partition in 0..i32::MAX,
            offset in 0..i64::MAX
        ) {
            let topic = topics.get(topic_idx).unwrap().to_string();
            let resolver = TopicResolver::new(topics);

            let (stream_id, pull_id) = resolver.resolve_stream_and_pull_ids_inner(topic.as_str(), partition, offset);
            let res = resolver.resolve_topic(stream_id, pull_id);
            assert!(res.is_some());
            let (resolved_topic, resolved_partition, resolved_offset) = res.unwrap();
            assert_eq!(topic.as_str(), resolved_topic);
            assert_eq!(partition, resolved_partition);
            assert_eq!(Offset::Offset(offset), resolved_offset);
        }
    }
}
