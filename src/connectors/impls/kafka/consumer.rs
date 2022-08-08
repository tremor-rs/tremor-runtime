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
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tremor_common::time::nanotime;
use tremor_value::value::StaticValue;

use crate::connectors::impls::kafka::{
    SmolRuntime, TremorRDKafkaContext, KAFKA_CONNECT_TIMEOUT, NO_ERROR,
};
use crate::connectors::prelude::*;
use async_broadcast::{broadcast, Receiver as BroadcastReceiver};
use async_std::channel::{bounded, Receiver, Sender};
use async_std::prelude::{FutureExt, StreamExt};
use async_std::task::{self, JoinHandle};
use halfbrown::HashMap;
use indexmap::IndexMap;
use log::Level::Debug;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::{Offset, TopicPartitionList};
use rdkafka_sys::RDKafkaErrorCode;

const KAFKA_CONSUMER_META_KEY: &str = "kafka_consumer";

#[derive(Deserialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
enum Mode {
    #[default]
    /// - enable.auto.commit: true,
    /// - enable.auto.offset.store: true,
    /// - auto.commit.interval.ms: 5000 # 5s
    Performance,
    /// - enable.auto.commit: true
    /// - enable.auto.offset.store: false
    /// - auto.commit.interval.ms: # take the configured value
    /// - retry_failed_events: true
    Transactional {
        #[serde(default = "default_commit_interval")]
        commit_interval: u64,
    }, // enable.auto.commit: true, enable.auto.offset.store: false, retry_failed_events: true
    Custom {
        /// Optional rdkafka configuration
        ///
        /// Can be overwritten by tremor for settings required for `auto_commit` and/or `retry_failed_events`.
        rdkafka_options: HashMap<String, StaticValue>,

        /// This config determines what to do upon events reported as failed.
        /// It only applies if `enable.auto.commit: false` or `enable.auto.commit: false, enable.auto.offset.store: true`
        ///
        /// if set to `true` this source will reset the consumer offset to a
        /// failed message both locally and on the broker, so it will effectively retry those messages.
        /// DANGER: this might lead to a lot of traffic to the group-coordinator
        ///
        /// If set to `false` this source will do nothing on failed messages, thus only commit the consumer offset
        /// of acknowledged messages. Failed messages will be ignored.
        ///
        /// This might lead to events being sent multiple times, if no new messages are acknowledged.
        /// DANGER: This should not be used when persistent errors are expected (e.g. if the message content is malformed and will lead to repeated errors)
        #[serde(default = "default_false")]
        retry_failed_events: bool,
    },
}

impl Mode {
    /// returns true if the given value is either `true` or `"true"`
    fn is_true_value(v: &StaticValue) -> bool {
        let v = v.value();
        v.as_bool() == Some(true) || v.as_str() == Some("true")
    }

    /// extract an int from the given value
    fn get_int_value(v: &StaticValue) -> Result<i64> {
        let v = v.value();
        let res = if let Some(int) = v.as_i64() {
            int
        } else if let Some(str_int) = v.as_str() {
            str_int.parse::<i64>()?
        } else {
            return Err("not an int value".into());
        };
        Ok(res)
    }

    fn retries_failed_events(&self) -> bool {
        match self {
            Mode::Custom {
                retry_failed_events,
                ..
            } => *retry_failed_events,
            Mode::Transactional { .. } => true,
            Mode::Performance => false,
        }
    }

    fn is_transactional(&self) -> bool {
        self.stores_offsets() || self.commits_offsets()
    }

    /// returns `true` if the current mode is configured to store offsets locally
    fn stores_offsets(&self) -> bool {
        match self {
            Mode::Transactional { commit_interval } => *commit_interval > 0,
            Mode::Custom {
                rdkafka_options, ..
            } => {
                rdkafka_options
                    .get("enable.auto.commit")
                    .map(Mode::is_true_value)
                    .unwrap_or_default()
                    && rdkafka_options
                        .get("enable.auto.offset.store")
                        .map(Mode::is_true_value)
                        .unwrap_or_default()
                    && rdkafka_options
                        .get("auto.commit.interval.ms")
                        .map(Mode::get_int_value)
                        .and_then(Result::ok)
                        != Some(0)
            }
            Mode::Performance => false,
        }
    }

    /// returns `true` if the current mode is configured to commit offsets for acked messages immediately to the group coordinator
    fn commits_offsets(&self) -> bool {
        match self {
            Mode::Transactional { commit_interval } => *commit_interval == 0,
            Mode::Custom {
                rdkafka_options, ..
            } => !rdkafka_options
                .get("enable.auto.commit")
                .map(Mode::is_true_value)
                .unwrap_or_default(),
            Mode::Performance => false,
        }
    }

    fn to_config(&self) -> Result<ClientConfig> {
        let mut client_config = ClientConfig::new();
        match self {
            Mode::Performance => {
                client_config
                    .set("enable.auto.commit", "true")
                    .set("enable.auto.offset.store", "true")
                    .set("auto.commit.interval.ms", "5000");
            }
            Mode::Transactional { commit_interval } => {
                let interval_ms = format!("{}", Duration::from_nanos(*commit_interval).as_millis());
                client_config
                    .set("enable.auto.commit", "true")
                    .set("enable.auto.offset.store", "false");
                if *commit_interval > 0 {
                    client_config.set("auto.commit.interval.ms", interval_ms.as_str());
                }
            }
            Mode::Custom {
                rdkafka_options,
                retry_failed_events,
            } => {
                // avoid combination like as it contradicts itself
                //      enable.auto.commit = true
                //      enable.auto.offset.store = true
                //      retry_failed_events = true
                //
                if rdkafka_options
                    .get("enable.auto.commit")
                    .map(Mode::is_true_value)
                    .unwrap_or_default()
                    && rdkafka_options
                        .get("enable.auto.offset.store")
                        .map(Mode::is_true_value)
                        .unwrap_or_default()
                    && *retry_failed_events
                {
                    return Err("Cannot enable `retry_failed_events` and `enable.auto.commit` and `enable.auto.offset.store` at the same time.".into());
                }
                for (k, v) in rdkafka_options {
                    client_config.set(k, v.to_string());
                }
            }
        }
        Ok(client_config)
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// consumer group id to register with
    group_id: String,
    /// List of topics to subscribe to
    topics: Vec<String>,
    /// List of bootstrap brokers
    brokers: Vec<String>,
    /// Mode of operation for this consumer
    ///
    /// Possible values:
    /// - `performance`: automatically commits offsets every 5s. This pre-canned setting is tuned for maximum performance and throughput at the cost of possibl message loss.
    /// - `transactional`: only stores the message offset if the event has been handled successfully,
    ///        reset the offset to any failed message, this will possibly replay already handled messages.
    ///        The default `commit_interval` (in nanoseconds) is equivalent to `5 seconds`, it can be configured.
    ///        If set to `0`, every message will be committed immediately, this will lead to lots of traffic towards the group-coordinator, use with care!
    ///        DANGER: This will possible replay failed messages infinitely with persistent errors (e.g. wrong message format),
    ///                but it offers at-least-once processing guarantee from end-to-end.
    ///
    ///   Example:
    ///
    ///   ```
    ///     "transactional": {
    ///         "commit_interval": nanos::from_seconds(5)    
    ///     }
    ///   ```
    ///
    /// - `custom`: Configure the connector yourself as you like, by providing your own set of `rdkafka_options`. You should know what you are doing when using this.
    ///
    ///   Example: ```
    ///   "custom": {
    ///     "rdkafka_options": {
    ///         "enable.auto.commit": false
    ///     },
    ///     "retry_failed_events": true
    ///   }
    ///   ```
    mode: Mode,
}

impl ConfigImpl for Config {}

fn default_commit_interval() -> u64 {
    5_000_000_000 // 5 seconds, the default from librdkafka
}

#[derive(Default, Debug)]
pub(crate) struct Builder {}

#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "kafka_consumer".into()
    }

    async fn build_cfg(
        &self,
        alias: &Alias,
        config: &ConnectorConfig,
        raw_config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let metrics_interval_s = config.metrics_interval_s;
        let config = Config::new(raw_config)?;
        // returns the first broker if all are valid
        let (host, port) = super::verify_brokers(alias, &config.brokers)?;
        let origin_uri = EventOriginUri {
            scheme: "tremor-kafka".to_string(),
            host,
            port,
            path: vec![],
        };

        let client_id = format!("tremor-{}-{}", hostname(), alias);
        let mut client_config = config.mode.to_config().map_err(|e| {
            Error::from(ErrorKind::InvalidConfiguration(
                alias.to_string(),
                e.to_string(),
            ))
        })?;

        // we do overwrite the rdkafka options to ensure a sane config
        set_client_config(&mut client_config, "group.id", &config.group_id)?;
        set_client_config(&mut client_config, "client.id", &client_id)?;
        set_client_config(
            &mut client_config,
            "bootstrap.servers",
            config.brokers.join(","),
        )?;

        if let Some(metrics_interval_s) = metrics_interval_s {
            // enable stats collection
            set_client_config(
                &mut client_config,
                "statistics.interval.ms",
                format!("{}", metrics_interval_s * 1000),
            )?;
        }

        info!("[Connector::{alias}] Kafka Consumer Config: {client_config:?}",);

        Ok(Box::new(KafkaConsumerConnector {
            config,
            client_config,
            origin_uri,
        }))
    }
}

fn set_client_config<V: Into<String>>(
    client_config: &mut ClientConfig,
    key: &'static str,
    value: V,
) -> Result<()> {
    if client_config.get(key).is_some() {
        return Err(format!("Provided rdkafka_option that will be overwritten: {key}").into());
    }
    client_config.set(key, value);
    Ok(())
}

impl ConsumerContext for TremorRDKafkaContext<SourceContext> {
    fn post_rebalance<'a>(&self, rebalance: &rdkafka::consumer::Rebalance<'a>) {
        // store the last timestamp
        self.last_rebalance_ts.store(nanotime(), Ordering::Release);
        match rebalance {
            Rebalance::Assign(tpl) => {
                let partitions: Vec<String> = tpl
                    .elements()
                    .iter()
                    .map(|elem| {
                        format!("[Topic: {}, Partition: {}]", elem.topic(), elem.partition(),)
                    })
                    .collect();
                // if we got something assigned, this is a good indicator that we are connected
                if !partitions.is_empty() {
                    if let Err(e) = self.connect_tx.try_send(NO_ERROR)
                    // we seem to be connected, indicate success
                    {
                        // we can safely ignore errors here as they will happen after the first connector
                        // as we only have &self here, we cannot switch out the connector
                        trace!("{} Error sending to connect channel: {e}", &self.ctx);
                    };
                }
                info!(
                    "{} Partitions Assigned: {}",
                    &self.ctx,
                    partitions.join(" ")
                );
            }
            Rebalance::Revoke(tpl) => {
                let partitions: Vec<String> = tpl
                    .elements()
                    .iter()
                    .map(|elem| {
                        format!("[Topic: {}, Partition: {}]", elem.topic(), elem.partition(),)
                    })
                    .collect();
                info!("{} Partitions Revoked: {}", &self.ctx, partitions.join(" "));
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
            Err(KafkaError::ConsumerCommit(rdkafka_sys::RDKafkaErrorCode::UnknownMemberId)) => {
                warn!(
                    "{} UnknownMemberId error during commit. Reconnecting...",
                    &self.ctx
                );
                self.on_connection_lost();
            }
            Err(e) => warn!("{} Error while committing offsets: {}", &self.ctx, e),
        };
    }
}
type TremorConsumerContext = TremorRDKafkaContext<SourceContext>;
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
    // map from stream_id to offset
    offsets: Option<HashMap<u64, i64>>,
    stores_offsets: bool,
    retry_failed_events: bool,
    seek_timeout: Duration,
    source_tx: Sender<(SourceReply, Option<u64>)>,
    source_rx: Receiver<(SourceReply, Option<u64>)>,
    consumer: Option<Arc<TremorConsumer>>,
    consumer_task: Option<JoinHandle<()>>,
    metrics_rx: Option<BroadcastReceiver<EventPayload>>,
    last_rebalance_ts: Arc<AtomicU64>,
    cached_assignment: Option<(TopicPartitionList, u64)>,
}

impl KafkaConsumerSource {
    const DEFAULT_SEEK_TIMEOUT: Duration = Duration::from_millis(500);

    fn new(config: Config, client_config: ClientConfig, origin_uri: EventOriginUri) -> Self {
        let Config { topics, mode, .. } = config;
        let topic_resolver = TopicResolver::new(topics.clone());
        let seek_timeout = client_config
            // this will put the default from kafka if not present
            .create_native_config()
            .and_then(|c| c.get("max.poll.interval.ms"))
            .map(|poll_interval| poll_interval.parse().unwrap_or(500_u64) / 2)
            .map(Duration::from_millis)
            .unwrap_or(Self::DEFAULT_SEEK_TIMEOUT);

        let (source_tx, source_rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));
        let offsets = if mode.is_transactional() {
            Some(HashMap::new())
        } else {
            None
        };
        Self {
            client_config,
            origin_uri,
            topics,
            topic_resolver,
            offsets,
            stores_offsets: mode.stores_offsets(),
            retry_failed_events: mode.retries_failed_events(),
            seek_timeout,
            source_tx,
            source_rx,
            consumer: None,
            consumer_task: None,
            metrics_rx: None,
            last_rebalance_ts: Arc::new(AtomicU64::new(0)),
            cached_assignment: None,
        }
    }

    /// gets the current assignment from the cache or fetches it from the group coordinator
    /// if we witnessed a rebalance in between
    ///
    /// This tries to avoid excess assignment calls to the group coordinator
    fn get_assignment(&mut self) -> KafkaResult<TopicPartitionList> {
        let KafkaConsumerSource {
            cached_assignment,
            consumer,
            ..
        } = self;
        if let Some(consumer) = consumer {
            let last_rebalance_ts = self.last_rebalance_ts.load(Ordering::Acquire);
            match cached_assignment {
                Some((tpl, created)) if last_rebalance_ts <= *created => {
                    KafkaResult::Ok(tpl.clone())
                }
                ca => {
                    // if we witnessed a new rebalance, we gotta fetch the new assignment
                    let new_assignment = consumer.assignment()?;
                    *ca = Some((new_assignment.clone(), last_rebalance_ts));
                    KafkaResult::Ok(new_assignment)
                }
            }
        } else {
            // highly unlikely
            KafkaResult::Err(KafkaError::ClientCreation(
                "No client available".to_string(),
            ))
        }
    }
}

#[async_trait::async_trait()]
impl Source for KafkaConsumerSource {
    async fn connect(&mut self, ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        if let Some(consumer_task) = self.consumer_task.take() {
            self.cached_assignment.take(); // clear out references to the consumer
                                           // drop the consumer
            if let Some(consumer) = self.consumer.take() {
                drop(consumer);
            }
            // terminate the consumer task
            consumer_task.cancel().await;
        }

        let (version_n, version_s) = rdkafka::util::get_rdkafka_version();
        info!(
            "{} Connecting using rdkafka 0x{:08x}, {}",
            &ctx, version_n, version_s
        );
        let (connect_result_tx, connect_result_rx) = bounded(1);

        // we only ever want to report on the latest metrics and discard old ones
        // if no messages arrive, no metrics will be reported, so be it.
        let (mut metrics_tx, metrics_rx) = broadcast(1);
        metrics_tx.set_overflow(true);
        self.metrics_rx = Some(metrics_rx);
        let consumer_context = TremorConsumerContext::consumer(
            ctx.clone(),
            connect_result_tx.clone(),
            metrics_tx,
            self.last_rebalance_ts.clone(),
        );
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

        let handle = task::spawn(consumer_task(
            task_consumer,
            self.topic_resolver.clone(),
            self.origin_uri.clone(),
            connect_result_tx,
            self.source_tx.clone(),
            ctx.clone(),
        ));
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
            Ok(Ok(KafkaError::Global(RDKafkaErrorCode::NoError))) => Ok(true), // connected
            Ok(Ok(err)) => Err(err.into()),                                    // any other error
        }
    }

    async fn pull_data(&mut self, pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        let (reply, custom_pull_id) = self.source_rx.recv().await?;
        if let Some(custom_pull_id) = custom_pull_id {
            *pull_id = custom_pull_id;
        }
        Ok(reply)
    }

    async fn ack(&mut self, stream_id: u64, pull_id: u64, ctx: &SourceContext) -> Result<()> {
        if let Some(offsets) = self.offsets.as_mut() {
            if let Some(consumer) = self.consumer.as_ref() {
                if let Some((topic, partition, offset)) =
                    self.topic_resolver.resolve_topic(stream_id, pull_id)
                {
                    // we need to keep track of the maximum offset per partition
                    // in order to not commit earlier offsets
                    if let Some(raw_offset) = offset.to_raw() {
                        // store offset if not yet in map or if raw_offset exceeds the stored_offset for that partition
                        if offsets
                            .get(&stream_id)
                            .filter(|stored_offset| raw_offset < **stored_offset)
                            .is_none()
                        {
                            offsets.insert(stream_id, raw_offset); // keep track of the maximum offset
                            if self.stores_offsets {
                                // we deliberately do not send a commit to the group-coordinator by calling `consumer.commit`
                                // but instead use the client-local memory store of committed offsets
                                // store the new maximum offset for this partition
                                debug!("{ctx} Storing offset {topic} {partition}: {raw_offset}");
                                consumer.store_offset(topic, partition, raw_offset)?;
                            } else {
                                // commit directly to the group coordinator
                                let mut tpl = TopicPartitionList::with_capacity(1);
                                // we need to commit the message offset + 1 - this is the offset we are going to continue from afterwards
                                let offset = Offset::Offset(raw_offset + 1);
                                debug!("{ctx} Committing offset {topic} {partition}: {offset:?}");
                                tpl.add_partition_offset(topic, partition, offset)?;
                                consumer.commit(&tpl, CommitMode::Async)?;
                            }
                        } else {
                            debug!(
                                "{ctx} Not committing {topic} {partition}: {offset:?}: {offsets:?}"
                            );
                        }
                    } else {
                        debug!("{ctx} Unable to store/commit offset={offset:?} for event with stream={stream_id}, pull_id={pull_id}");
                    }
                } else {
                    error!("{ctx} Could not ack event with stream={stream_id}, pull_id={pull_id}. Unable to detect topic from internal state.");
                }
            }
        }
        Ok(())
    }

    async fn fail(&mut self, stream_id: u64, pull_id: u64, ctx: &SourceContext) -> Result<()> {
        // how can we make sure we do not conflict with the store_offset handling in `ack`?
        if let KafkaConsumerSource {
            retry_failed_events: true,
            offsets: Some(offsets),
            ..
        } = self
        {
            if let Some(consumer) = self.consumer.as_ref() {
                if let Some((topic, partition, offset)) =
                    self.topic_resolver.resolve_topic(stream_id, pull_id)
                {
                    debug!(
                        "{} Failing: [topic={}, partition={} offset={:?}]",
                        &ctx, topic, partition, offset
                    );

                    // reset the committed offset to the broker/group-coordinator, so we can pick up there upon the next restart/reconnect
                    // this operation is expensive but necessary to ensure transactional mode
                    let mut tpl = TopicPartitionList::with_capacity(1);
                    tpl.add_partition_offset(topic, partition, offset)?;
                    consumer.commit(&tpl, CommitMode::Async)?;

                    // update the tracked offsets if necessary
                    // this has the effect that newer acks on that partition will actually store the newer offsets
                    if let Some(raw_offset) = offset.to_raw() {
                        offsets
                            .entry(stream_id)
                            .and_modify(|stored_offset| *stored_offset = raw_offset) // avoid off by one when storing commits
                            .or_insert(raw_offset);
                    }

                    // reset the local in-memory pointer to the message we want to consume next
                    // this will flush all the pre-fetched data from the partition, thus is quite expensive
                    consumer.seek(topic, partition, offset, self.seek_timeout)?;
                } else {
                    error!("{} Could not seek back to failed event with stream={}, pull_id={}. Unable to detect topic from internal state.", &ctx, stream_id, pull_id);
                }
            }
        }
        Ok(())
    }

    async fn on_cb_close(&mut self, _ctx: &SourceContext) -> Result<()> {
        let assignment = self.get_assignment()?;
        if let Some(consumer) = self.consumer.as_ref() {
            consumer.pause(&assignment)?;
        }
        Ok(())
    }
    async fn on_cb_open(&mut self, _ctx: &SourceContext) -> Result<()> {
        let assignment = self.get_assignment()?;
        if let Some(consumer) = self.consumer.as_mut() {
            consumer.resume(&assignment)?;
        }
        Ok(())
    }

    async fn on_pause(&mut self, _ctx: &SourceContext) -> Result<()> {
        let assignment = self.get_assignment()?;
        if let Some(consumer) = self.consumer.as_mut() {
            consumer.pause(&assignment)?;
        }
        Ok(())
    }

    async fn on_resume(&mut self, _ctx: &SourceContext) -> Result<()> {
        let assignment = self.get_assignment()?;
        if let Some(consumer) = self.consumer.as_mut() {
            consumer.resume(&assignment)?;
        }
        Ok(())
    }

    async fn on_stop(&mut self, ctx: &SourceContext) -> Result<()> {
        // free references, see: https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#high-level-kafkaconsumer
        self.cached_assignment.take();

        // clear out the consumer
        if let Some(consumer) = self.consumer.take() {
            drop(consumer);
        }

        debug!("{ctx} Consumer dropped");

        // stop the consumer task
        if let Some(consumer_task) = self.consumer_task.take() {
            consumer_task.cancel().await;
        }

        info!("{ctx} Consumer stopped.");
        Ok(())
    }

    fn is_transactional(&self) -> bool {
        self.offsets.is_some()
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

/// Kafka consumer main loop - consuming from a kafka stream
async fn consumer_task(
    task_consumer: Arc<StreamConsumer<TremorConsumerContext, SmolRuntime>>,
    topic_resolver: TopicResolver,
    consumer_origin_uri: EventOriginUri,
    connect_result_tx: Sender<KafkaError>,
    source_tx: Sender<(SourceReply, Option<u64>)>,
    source_ctx: SourceContext,
) {
    info!("{} Consumer started.", &source_ctx);
    let mut stream = task_consumer.stream();
    let mut connect_result_channel = Some(connect_result_tx);

    loop {
        match stream.next().await {
            Some(Ok(kafka_msg)) => {
                //debug!("{source_ctx} Received kafka msg: {kafka_msg:?}");
                if let Some(tx) = connect_result_channel.take() {
                    if !tx.is_closed() {
                        source_ctx
                            .swallow_err(tx.try_send(NO_ERROR), "Error sending to connect channel");
                    }
                }
                // handle kafka msg
                let (stream_id, pull_id) = topic_resolver.resolve_stream_and_pull_ids(&kafka_msg);
                let mut origin_uri = consumer_origin_uri.clone();
                origin_uri.path = vec![
                    kafka_msg.topic().to_string(),
                    kafka_msg.partition().to_string(),
                    kafka_msg.offset().to_string(),
                ];
                let data: Vec<u8> = kafka_msg.payload().map(<[u8]>::to_vec).unwrap_or_default();

                let meta = kafka_meta(&kafka_msg);
                let reply = SourceReply::Data {
                    origin_uri,
                    data,
                    meta: Some(meta),
                    stream: Some(stream_id),
                    port: Some(OUT),
                    codec_overwrite: None,
                };
                if let Err(e) = source_tx.send((reply, Some(pull_id))).await {
                    error!("{source_ctx} Error sending kafka message to source: {e}");
                    source_ctx.swallow_err(
                        source_ctx.notifier().connection_lost().await,
                        "Error notifying the runtime of a disfunctional source channel.",
                    );
                    break;
                };
            }
            Some(Err(e)) => {
                // handle kafka error
                match e {
                    // Those we consider fatal
                    KafkaError::MessageConsumption(
                        error_code @ (RDKafkaErrorCode::UnknownTopicOrPartition
                        | RDKafkaErrorCode::TopicAuthorizationFailed
                        | RDKafkaErrorCode::UnknownTopic),
                    ) => {
                        error!("{source_ctx} Subscription failed: {error_code}.");

                        if let Some(tx) = connect_result_channel.take() {
                            if tx.try_send(e).is_err() {
                                // in case the connect_result_channel has already been closed,
                                // lets initiate a reconnect via the notifier
                                // this might happen when we subscribe to multiple topics
                                source_ctx.swallow_err(
                                    source_ctx.notifier().connection_lost().await,
                                    "Error notifying the runtime about a failing consumer.",
                                );
                            }
                        } else {
                            // Initiate reconnect (if configured)
                            source_ctx.swallow_err(
                                source_ctx.notifier().connection_lost().await,
                                "Error notifying the runtime about a failing consumer.",
                            );
                        }
                        break;
                    }
                    err => {
                        // TODO: gather some more fatal errors that require a reconnect
                        error!("{} Error consuming from kafka: {}", &source_ctx, err);
                    }
                }
            }
            None => {
                // handle kafka being done
                // this shouldn't happen
                warn!(
                    "{} Consumer is done consuming. Initiating reconnect...",
                    source_ctx
                );
                if let Some(tx) = connect_result_channel.take() {
                    if !tx.is_closed() {
                        source_ctx.swallow_err(
                            tx.try_send(KafkaError::Global(RDKafkaErrorCode::End)), // consumer done
                            "Send failed",
                        );
                    }
                } else {
                    source_ctx.swallow_err(
                        source_ctx.notifier().connection_lost().await,
                        "Error notifying the runtime of finished consumer.",
                    );
                }
                break;
            }
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
    #[allow(clippy::cast_possible_wrap)] // we are limited by rdkafka types
    fn resolve_topic(&self, stream_id: u64, pull_id: u64) -> Option<(&str, i32, Offset)> {
        let partition = (stream_id >> 32) as i32;
        let topic_id = stream_id & 0xffff_ffff;

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

    /// Resolve `stream_id` and `pull_id` for the given `kafka_msg`
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

    // We allow this since we're limited to the return value from kafka but partitions are never negative
    #[allow(clippy::cast_sign_loss)]
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

    use super::{Config, Offset, TopicResolver};
    use crate::errors::Result;
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

    #[test]
    fn mode_to_config() -> Result<()> {
        let mut config = r#"
        {
            "topics": ["topic"],
            "brokers": ["broker1"],
            "group_id": "snot",
            "mode": {
                "custom": {
                    "rdkafka_options": {
                        "snot": true,
                        "badger": null,
                        "float": 1.543,
                        "int": -42,
                        "string": "string"
                    },
                    "retry_failed_events": true
                }
            }
        }
        "#
        .as_bytes()
        .to_vec();
        let value = tremor_value::parse_to_value(config.as_mut_slice())?;
        let config: Config = tremor_value::structurize(value)?;
        let mode = config.mode;
        let client_config = mode.to_config()?;
        assert_eq!(client_config.get("snot"), Some("true"));
        assert_eq!(client_config.get("badger"), Some("null"));
        assert_eq!(client_config.get("float"), Some("1.543"));
        assert_eq!(client_config.get("int"), Some("-42"));
        assert_eq!(client_config.get("string"), Some("string"));
        Ok(())
    }
}
