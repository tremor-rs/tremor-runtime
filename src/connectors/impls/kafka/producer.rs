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

//! Kafka Producer Connector
//! Sending events from tremor to a kafka topic

use std::time::Duration;

use crate::connectors::impls::kafka::{
    is_failed_connect_error, is_fatal_error, SmolRuntime, TremorRDKafkaContext,
    KAFKA_CONNECT_TIMEOUT,
};
use crate::connectors::metrics::make_metrics_payload;
use crate::connectors::prelude::*;
use async_broadcast::{broadcast, Receiver as BroadcastReceiver};
use async_std::channel::{bounded, Sender};
use async_std::prelude::FutureExt;
use async_std::task;
use beef::Cow;
use halfbrown::HashMap;
use rdkafka::config::{ClientConfig, FromClientConfigAndContext};
use rdkafka::producer::{DeliveryFuture, Producer, ProducerContext};
use rdkafka::{
    error::KafkaError,
    message::OwnedHeaders,
    producer::{FutureProducer, FutureRecord},
};
use rdkafka::{ClientContext, Statistics};
use rdkafka_sys::RDKafkaErrorCode;
use tremor_common::time::nanotime;

const KAFKA_PRODUCER_META_KEY: &str = "kafka_producer";

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

    async fn build_cfg(
        &self,
        alias: &str,
        config: &ConnectorConfig,
        raw_config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let metrics_interval_s = config.metrics_interval_s;
        let config = Config::new(raw_config)?;

        super::verify_brokers(alias, &config.brokers)?;
        let mut producer_config = ClientConfig::new();

        // ENABLE LIBRDKAFKA DEBUGGING:
        // - set librdkafka logger to debug in logger.yaml
        // - configure: debug: "all" for this onramp
        let tid = task::current().id();
        let client_id = format!("tremor-{}-{}-{:?}", hostname(), alias, tid);
        producer_config
            .set("client.id", &client_id)
            .set("bootstrap.servers", config.brokers.join(","));
        // .set("message.timeout.ms", "5000")
        // .set("queue.buffering.max.ms", "0"); // set to 0 for sending each message out immediately without kafka client internal batching --> low latency, busy network
        if let Some(metrics_interval_s) = metrics_interval_s {
            // enable stats collection
            producer_config.set(
                "statistics.interval.ms",
                format!("{}", metrics_interval_s * 1000),
            );
        }
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

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

type TremorProducer = FutureProducer<TremorRDKafkaContext<SinkContext>, SmolRuntime>;

struct KafkaProducerSink {
    config: Config,
    producer_config: ClientConfig,
    producer: Option<TremorProducer>,
    reply_tx: Sender<AsyncSinkReply>,
    metrics_rx: Option<BroadcastReceiver<EventPayload>>,
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
            metrics_rx: None,
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
            let kafka_meta = meta.get(KAFKA_PRODUCER_META_KEY);
            // expecting string or bytes as kafka key in metadata, both works
            let kafka_key = kafka_meta
                .get("key")
                .and_then(Value::as_bytes)
                .or_else(|| self.config.key.as_ref().map(String::as_bytes));
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
                    // our timestamp is in nanos, kafkas timestamp in is millis
                    record = record.timestamp(timestamp / 1_000_000);
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
                        if is_fatal_error(&e) {
                            error!("{ctx} Fatal Kafka Error: {e}. Attempting a reconnect.");
                            ctx.notifier.connection_lost().await?;
                        }
                        return Err(e.into());
                    }
                }
            }
        }
        if !delivery_futures.is_empty() {
            let cf_data = if transactional {
                Some(ContraflowData::from(&event))
            } else {
                None
            };
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

        let (tx, rx) = bounded(1);
        let (mut metrics_tx, metrics_rx) = broadcast(1);
        metrics_tx.set_overflow(true);
        self.metrics_rx = Some(metrics_rx);
        let context = TremorRDKafkaContext::new(ctx.clone(), tx, metrics_tx);
        let (version_n, version_s) = rdkafka::util::get_rdkafka_version();
        info!("{ctx} Connecting kafka producer with rdkafka 0x{version_n:08x} {version_s}");

        let producer_config = self.producer_config.clone();
        let producer: TremorProducer =
            FutureProducer::from_config_and_context(&producer_config, context)?;
        // check if we receive any error callbacks
        match rx.recv().timeout(KAFKA_CONNECT_TIMEOUT).await {
            Err(_timeout) => {
                // timeout error, everything is ok, no error
                self.producer = Some(producer);
                Ok(true)
            }
            Ok(Err(e)) => {
                // receive error - we cannot tell what happened, better error here to trigger a retry
                Err(e.into())
            }
            Ok(Ok(kafka_error)) => {
                // we received an error from rdkafka - fail it big time
                Err(kafka_error.into())
            }
        }
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

    async fn metrics(&mut self, _timestamp: u64, _ctx: &SinkContext) -> Vec<EventPayload> {
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

async fn wait_for_delivery(
    ctx: SinkContext,
    cf_data: Option<ContraflowData>,
    start: u64,
    futures: Vec<DeliveryFuture>,
    reply_tx: Sender<AsyncSinkReply>,
) -> Result<()> {
    let cb = match futures::future::try_join_all(futures).await {
        Ok(results) => {
            if let Some((kafka_error, _)) = results.into_iter().find_map(std::result::Result::err) {
                error!("{ctx} Error delivering kafka record: {kafka_error}");
                if is_fatal_error(&kafka_error) && ctx.notifier().connection_lost().await.is_err() {
                    error!("{ctx} Error notifying runtime of fatal Kafka error");
                }
                cf_data.map(AsyncSinkReply::Fail)
            } else {
                cf_data.map(|cf| AsyncSinkReply::Ack(cf, nanotime() - start))
            }
        }
        Err(e) => {
            error!("{ctx} Kafka record delivery cancelled. Record delivery status unclear, consider it failed: {e}.");
            cf_data.map(AsyncSinkReply::Fail)
        }
    };
    if let Some(cb) = cb {
        if reply_tx.send(cb).await.is_err() {
            error!("{ctx} Error sending insight for kafka record delivery");
        };
    }
    Ok(())
}
