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

//! Kafka Producer Connector
//! Sending events from tremor to a kafka topic

use std::time::Duration;

use super::SmolRuntime;
use crate::connectors::prelude::*;
use async_std::channel::Sender;
use async_std::task;
use halfbrown::HashMap;
use rdkafka::config::{ClientConfig, FromClientConfigAndContext};
use rdkafka::producer::{DeliveryFuture, Producer};
use rdkafka::ClientContext;
use rdkafka::{
    error::KafkaError,
    message::OwnedHeaders,
    producer::{FutureProducer, FutureRecord},
};
use rdkafka_sys::RDKafkaErrorCode;
use tremor_common::time::nanotime;

#[derive(Deserialize, Clone)]
pub struct Config {
    /// list of brokers forming a cluster. 1 is enough
    brokers: Vec<String>,
    /// the topic to send to
    topic: String,
    // key to use for messages, defaults to none
    // Overwritten by `kafka.key` in metadata if present.
    #[serde(default = "Default::default")]
    key: Option<String>,
    // a map (string keys and string values) of [librdkafka options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) (default: None) - Note this can overwrite default settings.
    ///
    /// Default settings for librdkafka:
    ///
    /// * `client.id` - `"tremor-<hostname>-0"`
    /// * `bootstrap.servers` - `brokers` from the config concatinated by `,`
    /// * `message.timeout.ms` - `"5000"`
    /// * `queue.buffering.max.ms` - `"0"` - don't buffer for lower latency (high)
    #[serde(default = "Default::default")]
    pub rdkafka_options: Option<HashMap<String, String>>,
}

impl ConfigImpl for Config {}

#[derive(Default, Debug)]
pub(crate) struct Builder {}

#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "kafka_producer".into()
    }

    async fn from_config(
        &self,
        alias: &str,
        config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(raw_config) = config {
            let config = Config::new(raw_config)?;

            let _ = super::verify_brokers(alias, &config.brokers)?;
            let mut producer_config = ClientConfig::new();

            // ENABLE LIBRDKAFKA DEBUGGING:
            // - set librdkafka logger to debug in logger.yaml
            // - configure: debug: "all" for this onramp
            producer_config
                .set(
                    "client.id",
                    &format!("tremor-{}-{}", hostname(), alias.to_string()),
                )
                .set("bootstrap.servers", config.brokers.join(","))
                .set("message.timeout.ms", "5000")
                .set("queue.buffering.max.ms", "0"); // set to 0 for sending each message out immediately without kafka client internal batching --> low latency, busy network
            config
                .rdkafka_options
                .iter()
                .flat_map(halfbrown::HashMap::iter)
                .for_each(|(k, v)| {
                    producer_config.set(k, v);
                });
            Ok(Box::new(KafkaProducerConnector {
                config,
                producer_config,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(alias.to_string()).into())
        }
    }
}

struct TremorProducerContext {
    ctx: SinkContext,
}

impl ClientContext for TremorProducerContext {
    fn stats(&self, statistics: rdkafka::Statistics) {
        // TODO: expose metrics to the sink
        info!("{} Client stats: {statistics:?}", self.ctx);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        error!("{} Kafka Error: {}: {}", &self.ctx, error, reason);
        if is_fatal(&error) {
            let notifier = self.ctx.notifier().clone();
            task::spawn(async move {
                notifier.notify().await?;
                Ok::<(), Error>(())
            });
        }
    }
}

struct KafkaProducerConnector {
    config: Config,
    producer_config: ClientConfig,
}

#[async_trait::async_trait()]
impl Connector for KafkaProducerConnector {
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = KafkaProducerSink::new(
            self.config.clone(),
            self.producer_config.clone(),
            builder.reply_tx(),
        );
        builder.spawn(sink, sink_context).map(Some)
    }
    fn default_codec(&self) -> &str {
        "json"
    }
}

struct KafkaProducerSink {
    config: Config,
    producer_config: ClientConfig,
    producer: Option<FutureProducer<TremorProducerContext, SmolRuntime>>,
    reply_tx: Sender<AsyncSinkReply>,
}

impl KafkaProducerSink {
    fn new(
        config: Config,
        producer_config: ClientConfig,
        reply_tx: Sender<AsyncSinkReply>,
    ) -> Self {
        Self {
            config,
            producer_config,
            producer: None,
            reply_tx,
        }
    }
}

#[async_trait::async_trait()]
impl Sink for KafkaProducerSink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        start: u64,
    ) -> Result<SinkReply> {
        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| ErrorKind::ProducerNotAvailable(ctx.alias().to_string()))?;
        let transactional = event.transactional;
        let mut delivery_futures: Vec<DeliveryFuture> = if transactional {
            Vec::with_capacity(event.len())
        } else {
            // no need to wait for anything, fire and forget
            vec![]
        };
        let ingest_ns = event.ingest_ns;
        for (value, meta) in event.value_meta_iter() {
            let kafka_meta = meta.get("kafka");
            // expecting string or bytes as kafka key in metadata, both works
            let kafka_key = kafka_meta
                .get("key")
                .and_then(Value::as_bytes)
                .or_else(|| self.config.key.as_ref().map(|s| s.as_bytes()));
            for payload in serializer.serialize(value, ingest_ns)? {
                let mut record = FutureRecord::to(self.config.topic.as_str());
                if let Some(key) = kafka_key {
                    record = record.key(key);
                }
                if let Some(headers_obj) = kafka_meta.get_object("headers") {
                    let mut headers = OwnedHeaders::new_with_capacity(headers_obj.len());
                    for (k, v) in headers_obj.iter() {
                        // supporting string or bytes as headers value
                        if let Some(v_bytes) = v.as_bytes() {
                            headers = headers.add(k, v_bytes);
                        }
                    }
                    record = record.headers(headers);
                }
                if let Some(timestamp) = kafka_meta.get_i64("timestamp") {
                    record = record.timestamp(timestamp);
                }
                if let Some(partition) = kafka_meta.get_i32("partition") {
                    record = record.partition(partition);
                }
                record = record.payload(&payload);
                match producer.send_result(record) {
                    Ok(delivery_future) => {
                        delivery_futures.push(delivery_future);
                    }
                    Err((e, _)) => {
                        error!("{ctx} Failed to enqueue message: {e}");
                        if is_fatal(&e) {
                            error!("{ctx} Fatal Kafka Error: {e}. Attempting a reconnect.");
                            ctx.notifier.notify().await?;
                        }
                        return Err(e.into());
                    }
                }
            }
        }
        if !delivery_futures.is_empty() {
            let cf_data = ContraflowData::from(&event);
            task::spawn(wait_for_delivery(
                ctx.clone(),
                cf_data,
                start,
                delivery_futures,
                self.reply_tx.clone(),
            ));
        }
        Ok(SinkReply::NONE)
    }

    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        // enforce cleaning out the previous producer
        // We might lose some in flight messages
        if let Some(old_producer) = self.producer.take() {
            let in_flight_msgs = old_producer.in_flight_count();
            if in_flight_msgs > 0 {
                info!("{ctx} Dropping old producer. Losing {in_flight_msgs} in-flight messages not yet sent to brokers.");
            }
            drop(old_producer);
        };

        let context = TremorProducerContext { ctx: ctx.clone() };
        let (version_n, version_s) = rdkafka::util::get_rdkafka_version();
        info!("{ctx} Connecting kafka producer with rdkafka 0x{version_n:08x} {version_s}");

        let producer_config = self.producer_config.clone();
        let producer: FutureProducer<TremorProducerContext, SmolRuntime> =
            FutureProducer::from_config_and_context(&producer_config, context)?;
        self.producer = Some(producer);

        Ok(true)
    }

    async fn on_stop(&mut self, ctx: &SinkContext) -> Result<()> {
        if let Some(producer) = self.producer.take() {
            let wait_secs = Duration::from_secs(1);
            if producer.in_flight_count() > 0 {
                info!("{ctx} Flushing messages. Waiting for {wait_secs:?} seconds.");
                producer.flush(wait_secs);
            }
        }
        Ok(())
    }

    fn auto_ack(&self) -> bool {
        false
    }
}

async fn wait_for_delivery(
    ctx: SinkContext,
    cf_data: ContraflowData,
    start: u64,
    futures: Vec<DeliveryFuture>,
    reply_tx: Sender<AsyncSinkReply>,
) -> Result<()> {
    let cb = match futures::future::try_join_all(futures).await {
        Ok(results) => {
            // FIXME: only Fail on KafkaError::Transaction ??
            if let Some((kafka_error, _)) = results.into_iter().find_map(std::result::Result::err) {
                error!("{ctx} Error delivering kafka record: {kafka_error}");
                if is_fatal(&kafka_error) {
                    if ctx.notifier().notify().await.is_err() {
                        error!("{ctx} Error notifying runtime of fatal Kafka error");
                    }
                }
                AsyncSinkReply::Fail(cf_data)
            } else {
                AsyncSinkReply::Ack(cf_data, nanotime() - start)
            }
        }
        Err(e) => {
            error!("{ctx} Kafka record delivery cancelled. Record delivery status unclear, consider it failed: {e}.");
            AsyncSinkReply::Fail(cf_data)
        }
    };
    if reply_tx.send(cb).await.is_err() {
        error!("{ctx} Error sending insight for kafka record delivery");
    };
    Ok(())
}

/// check if a kafka error is fatal
fn is_fatal(e: &KafkaError) -> bool {
    match e {
        // Generic fatal errors
        KafkaError::AdminOp(code)
        | KafkaError::ConsumerCommit(code)
        | KafkaError::Global(code)
        | KafkaError::GroupListFetch(code)
        | KafkaError::MessageConsumption(code)
        | KafkaError::MessageProduction(code)
        | KafkaError::MetadataFetch(code)
        | KafkaError::OffsetFetch(code)
        | KafkaError::SetPartitionOffset(code)
        | KafkaError::StoreOffset(code) => matches!(code, RDKafkaErrorCode::Fatal),
        // FFI error this is very bad
        KafkaError::Nul(_) => true,
        // Check if it is a fatal transaction error
        KafkaError::Transaction(e) => e.is_fatal(),
        // Creational errors, this should never apear during send
        KafkaError::AdminOpCreation(_) => false,
        KafkaError::ClientCreation(_) => false,
        KafkaError::ClientConfig(_, _, _, _) => false,
        KafkaError::Subscription(_) => false,
        // Consumer error - we never seek on a producer
        KafkaError::PartitionEOF(_) => false,
        KafkaError::Seek(_) => false,
        // Not sure what this exactly does - verify
        KafkaError::Canceled => false,
        KafkaError::NoMessageReceived => false,
        // is not fatal might want to look into this for an enhancement later
        KafkaError::PauseResume(_) => false,
        // This is required due to `KafkaError` being mared as `#[non_exhaustive]`
        _ => false,
    }
}
