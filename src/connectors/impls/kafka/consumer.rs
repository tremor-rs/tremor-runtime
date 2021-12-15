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

use std::time::Duration;

use super::SmolRuntime;
use crate::connectors::prelude::*;
use async_std::channel::{bounded, unbounded, Receiver, Sender, TryRecvError};
use async_std::task::{self, JoinHandle};
use async_std::stream::StreamExt;
use futures::select;
use futures::FutureExt;
use halfbrown::HashMap;
use hashbrown::HashSet;
use indexmap::IndexMap;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::{ClientContext, TopicPartitionList, Offset};
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
            // TODO: or rather use bounded(128) ?
            let (consumer_tx, consumer_rx) = unbounded();
            let (source_tx, source_rx) = bounded(128);
            // returns the first broker if all are valid
            let (host, port) = Self::verify_brokers(alias, &config.brokers)?;
            let origin_uri = EventOriginUri {
                scheme: "tremor-kafka".to_string(),
                host,
                port,
                path: vec![],
            };
            Ok(Box::new(KafkaConsumerConnector {
                config,
                origin_uri,
                consumer_task: None,
                consumer_tx,
                consumer_rx,
                source_tx,
                source_rx,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(alias.to_string()).into())
        }
    }
}

struct TremorConsumerContext {
    alias: String,
    err_notifier: ConnectionLostNotifier,
}

impl ClientContext for TremorConsumerContext {
    fn stats(&self, statistics: rdkafka::Statistics) {
        // FIXME: expose as metrics to the source
        info!("[Connector::{}] Client stats: {:?}", &self.alias, statistics);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        error!(
            "[Connector::{}] Kafka Error {}: {}",
            &self.alias, error, reason
        );
        // check for errors that manifest a non-working consumer, so we bail out
        if let KafkaError::Subscription(_)
            | KafkaError::ClientConfig(_, _, _, _)
            | KafkaError::ClientCreation(_)
            // TODO: what else?
            | KafkaError::Global(RDKafkaErrorCode::UnknownTopicOrPartition | RDKafkaErrorCode::UnknownTopic) =
            error
        {
            let notifier = self.err_notifier.clone();
            let url = self.alias.clone();
            async_std::task::spawn(async move {
                if let Err(e) = notifier.notify().await {
                    error!(
                        "[Source::{}] Error notifying the connector of a failure: {}",
                        &url, e
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

type TremorConsumer = StreamConsumer<TremorConsumerContext, SmolRuntime>;

/*
pub(crate) struct ConsumerStream {
    consumer: TremorConsumer,
    stream: MessageStream<'static>,
}

impl ConsumerStream {
    #[must_use]
    fn suffix(&self) -> &MessageStream {
        &self.stream
    }
}

impl From<TremorConsumer> for ConsumerStream {
    fn from(consumer: TremorConsumer) -> Self {
        let stream = consumer.stream();
        Self {}
    }
}
*/

struct KafkaConsumerConnector {
    config: Config,
    origin_uri: EventOriginUri,
    consumer_task: Option<JoinHandle<Result<()>>>,
    consumer_tx: Sender<ConsumerMsg>,
    consumer_rx: Receiver<ConsumerMsg>,
    source_tx: Sender<(SourceReply, Option<u64>)>,
    source_rx: Receiver<(SourceReply, Option<u64>)>,
}

#[async_trait::async_trait()]
impl Connector for KafkaConsumerConnector {
    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let auto_commit = self
            .config
            .rdkafka_options
            .as_ref()
            .and_then(|m| m.get("enable.auto.commit"))
            .map_or(true, |v| v == "true");
        let source = KafkaConsumerSource {
            transactional: auto_commit,
            source_rx: self.source_rx.clone(),
            consumer_tx: self.consumer_tx.clone(),
        };
        builder.spawn(source, source_context).map(Some)
    }

    async fn connect(&mut self, ctx: &ConnectorContext, attempt: &Attempt) -> Result<bool> {
        if let Some(consumer_task) = self.consumer_task.take() {
            consumer_task.cancel().await;
        }
        let (version_n, version_s) = rdkafka::util::get_rdkafka_version();
        info!(
            "{} Connecting using rdkafka 0x{:08x}, {}",
            &ctx, version_n, version_s
        );
        let tid = task::current().id();
        let mut client_config = ClientConfig::new();
        let client_id = format!("tremor-{}-{}-{:?}", hostname(), ctx.alias, tid);
        client_config
            .set("group.id", self.config.group_id.clone())
            .set("client.id", &client_id)
            .set("bootstrap.servers", &self.config.brokers.join(","))
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "5000")
            .set("enable.auto.offset.store", "true");
        self.config
            .rdkafka_options
            .iter()
            .flat_map(halfbrown::HashMap::iter)
            .for_each(|(k, v)| {
                client_config.set(k, v);
            });
        debug!("{} Kafka Consumer Config: {:?}", &ctx, &client_config);

        let consumer_context = TremorConsumerContext {
            alias: ctx.alias.to_string(),
            err_notifier: ctx.notifier.clone(),
        };
        let consumer: TremorConsumer = client_config.create_with_context(consumer_context)?;

        let mut topics: Vec<&str> = self
            .config
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

        // used for generating stream nums
        topics.sort(); // sort them alphabetically
        let topic_nums: IndexMap<String, u64> = topics
            .into_iter()
            .enumerate()
            .map(|(num, topic)| (topic.to_string(), num as u64))
            .collect();
            
        let back_channel = self.consumer_rx.clone();
        let source_tx = self.source_tx.clone();
        let connector_ctx = ctx.clone();
        let retry_failed_events = self.config.retry_failed_events;
        let consumer_origin_uri = self.origin_uri.clone();
        let handle = task::Builder::new()
            .name(format!("{}-{}", &client_id, attempt.overall()))
            .spawn(async move {
                // We need to ensure we continuously poll the consumer stream
                // to not run into timeouts with the consumer
                let mut stream = consumer.stream();
                let mut seen_partitions = HashSet::new();
                loop {
                    let mut kafka_msg_future = stream.next().fuse();
                    let mut back_channel_future = back_channel.recv().fuse();
                    select! {
                        maybe_msg = kafka_msg_future => {
                            match maybe_msg {
                                Some(Ok(kafka_msg)) => {
                                    // ### HANDLE KAFKA MESSAGE

                                    // stream id generated from topic and partition
                                    // higher 32 bits are the kafka partition number
                                    // lower 32 bits are the position of the topic in an alphabetically sorted list
                                    let partition = kafka_msg.partition() as u64;
                                    let topic = kafka_msg.topic();
                                    let offset = kafka_msg.offset();
                                    let stream_id = topic_nums.get(topic).copied().unwrap_or_default() & (partition << 32);
                                    // the pull id only needs to be unique per stream
                                    // with a stream being a partition, we can simply use the offset
                                    let pull_id = Some(offset as u64);

                                    // we create 1 stream per partition we receive messages from
                                    if seen_partitions.insert(partition) {
                                        source_tx.send((SourceReply::StartStream(stream_id), None)).await?;
                                    }
                                    let mut origin_uri = consumer_origin_uri.clone();
                                    origin_uri.path = vec![
                                            kafka_msg.topic().to_string(),
                                            kafka_msg.partition().to_string(),
                                            kafka_msg.offset().to_string()
                                        ];
                                    let data: Vec<u8> = kafka_msg.payload().map(|slice| slice.to_vec()).unwrap_or_default();

                                    let meta = kafka_meta(&kafka_msg);
                                    let reply = SourceReply::Data {
                                        origin_uri,
                                        data,
                                        meta: Some(meta),
                                        stream: stream_id,
                                        port: Some(OUT)
                                    };
                                    source_tx.send((reply, pull_id)).await?;

                                }
                                Some(Err(e)) => {
                                    // handle kafka error
                                    match e {
                                        KafkaError::MessageConsumption(
                                            e
                                            @
                                            (RDKafkaErrorCode::UnknownTopicOrPartition
                                            | RDKafkaErrorCode::TopicAuthorizationFailed
                                            | RDKafkaErrorCode::UnknownTopic),
                                        ) => {
                                            error!(
                                                "{} Subscription failed: {}.",
                                                &connector_ctx, e
                                            );
                                        }
                                        err => {
                                            debug!("{} Error consuming from kafka: {}", &connector_ctx, err);
                                        }
                                    }
                                }
                                None => {
                                    // stream done
                                    // this shouldn't happen
                                    break;
                                }
                            }
                        },
                        control_msg = back_channel_future => {
                            match control_msg {
                                Ok(ConsumerMsg::Pause) => {
                                    // TODO: check for fatal errors and notify the connector
                                    connector_ctx.log_err(consumer.assignment().and_then(|partitions| {
                                        consumer.pause(&partitions)
                                    }), "Error pausing consumer");
                                }
                                Ok(ConsumerMsg::Resume) => {
                                    // TODO: check for fatal errors and notify the connector
                                    connector_ctx.log_err(consumer.assignment().and_then(|partitions| {
                                        consumer.resume(&partitions)
                                    }), "Error resuming consumer");
                                }
                                Ok(ConsumerMsg::Ack(stream, pull)) => {
                                    let topic_id = stream >> 32;
                                    let partition = (stream & 0xFFFFFFFF) as i32;
                                    let offset = Offset::Offset(pull as i64);
                                    // insertion order should be the same as the actual indices, 
                                    // so the index lookup can be misused as a reverse lookup
                                    if let Some((topic, v)) = topic_nums.get_index(topic_id as usize) {
                                        if *v != topic_id {
                                            // shouldn't happen, just a safety net
                                            error!("{} Invalid topic id in ack message. Expected={}, got={}", &connector_ctx, topic_id, v);
                                        } else {

                                            let mut tpl: TopicPartitionList = TopicPartitionList::with_capacity(1);
                                            connector_ctx.log_err(tpl.add_partition_offset(topic.as_str(), partition as i32, offset), "Error populating TopicPartitionList for ack");
                                            connector_ctx.log_err(consumer.commit(&tpl, CommitMode::Async), "Error committing offsets");
                                        }
                                    } else {
                                        error!("{} Could not ack event with stream={}, pull_id={}. Unable to detect topic from internal state.", &connector_ctx, stream, pull);
                                    }
                                }
                                Ok(ConsumerMsg::Fail(stream, pull)) => {
                                    if retry_failed_events {
                                        let topic_id = stream >> 32;
                                        let partition = (stream & 0xFFFFFFFF) as i32;
                                        let offset = Offset::Offset(pull as i64);
                                        if let Some((topic, v)) = topic_nums.get_index(topic_id as usize) {
                                            if *v != topic_id {
                                                // shouldn't happen, just a safety net
                                                error!("{} Invalid topic id in ack message. Expected={}, got={}", &connector_ctx, topic_id, v);
                                            } else {
                                                let timeout = Duration::from_secs(1);
                                                connector_ctx.log_err(
                                                    consumer.seek(topic.as_str(), partition, offset, timeout), 
                                                    "Error resetting offsets to retry failed messages");
                                            }
                                        }
                                    }
                                }
                                Err(_) => {
                                    error!("{}", &connector_ctx)
                                }
                            }
                        }
                    };
                }
                Ok(())
            })?;
        self.consumer_task = Some(handle);
        Ok(true)
    }

    fn default_codec(&self) -> &str {
        "json"
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

enum ConsumerMsg {
    Pause,
    Resume,
    /// first field is stream_id, second is pull_id
    Ack(u64, u64),
    Fail(u64, u64),
}

struct KafkaConsumerSource {
    transactional: bool,
    source_rx: Receiver<(SourceReply, Option<u64>)>,
    consumer_tx: Sender<ConsumerMsg>,
}
#[async_trait::async_trait()]
impl Source for KafkaConsumerSource {
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
                return Err("Consumer unavailable.".into())
            }
        }
    }

    async fn ack(&mut self, stream_id: u64, pull_id: u64) -> Result<()> {
        if self.transactional {
            self.consumer_tx
                .send(ConsumerMsg::Ack(stream_id, pull_id))
                .await?;
        }
        Ok(())
    }

    async fn fail(&mut self, stream_id: u64, pull_id: u64) -> Result<()> {
        if self.transactional {
            self.consumer_tx
                .send(ConsumerMsg::Fail(stream_id, pull_id))
                .await?;
        }
        Ok(())
    }

    async fn on_cb_close(&mut self, _ctx: &SourceContext) -> Result<()> {
        self.consumer_tx.send(ConsumerMsg::Pause).await?;
        Ok(())
    }
    async fn on_cb_open(&mut self, _ctx: &SourceContext) -> Result<()> {
        self.consumer_tx.send(ConsumerMsg::Resume).await?;
        Ok(())
    }

    async fn on_pause(&mut self, _ctx: &SourceContext) -> Result<()> {
        self.consumer_tx.send(ConsumerMsg::Pause).await?;
        Ok(())
    }

    async fn on_resume(&mut self, _ctx: &SourceContext) -> Result<()> {
        self.consumer_tx.send(ConsumerMsg::Resume).await?;
        Ok(())
    }

    fn is_transactional(&self) -> bool {
        self.transactional
    }

    fn asynchronous(&self) -> bool {
        true
    }
}
