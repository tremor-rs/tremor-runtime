// Copyright 2020, The Tremor Team
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

//! # Kafka Offramp
//!
//! The `kafka` offramp allows persisting events to a kafka queue.
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use crate::sink::prelude::*;
use async_channel::{bounded, Receiver, Sender};
use halfbrown::HashMap;
use rdkafka::config::ClientConfig;
use rdkafka::{
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
};
use std::{fmt, time::Duration};

#[derive(Deserialize)]
pub struct Config {
    /// list of brokers
    pub brokers: Vec<String>,
    /// the topic to send to
    pub topic: String,
    /// a map (string keys and string values) of [librdkafka options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) (default: None) - Note this can overwrite default settings.
    ///
    /// Default settings for librdkafka:
    ///
    /// * `client.id` - `"tremor-<hostname>-0"`
    /// * `bootstrap.servers` - `brokers` from the config concatinated by `,`
    /// * `message.timeout.ms` - `"5000"`
    /// * `queue.buffering.max.ms` - `"0"` - don't buffer for lower latency (high)
    #[serde(default = "Default::default")]
    pub rdkafka_options: HashMap<String, String>,
    /// hostname to use, defaults to the hostname of the system
    #[serde(default = "d_host")]
    pub hostname: String,
    /// key to use for messages, defaults to none
    #[serde(default = "Default::default")]
    pub key: Option<String>,
}

impl Config {
    fn producer(&self) -> Result<FutureProducer> {
        let mut producer_config = ClientConfig::new();

        // ENABLE LIBRDKAFKA DEBUGGING:
        // - set librdkafka logger to debug in logger.yaml
        // - configure: debug: "all" for this onramp
        let producer_config = producer_config
            .set("client.id", &format!("tremor-{}-{}", self.hostname, 0))
            .set("bootstrap.servers", &self.brokers.join(","))
            .set("message.timeout.ms", "5000")
            .set("queue.buffering.max.ms", "0"); // set to 0 for sending each message out immediately without kafka client internal batching --> low latency, busy network

        Ok(self
            .rdkafka_options
            .iter()
            .fold(producer_config, |c: &mut ClientConfig, (k, v)| c.set(k, v))
            .create()?)
    }
}

impl ConfigImpl for Config {}

fn d_host() -> String {
    hostname()
}

/// Kafka offramp connectoz
pub struct Kafka {
    sink_url: TremorURL,
    config: Config,
    producer: FutureProducer,
    postprocessors: Postprocessors,
    reply_tx: Sender<sink::Reply>,
    error_rx: Receiver<KafkaError>,
    error_tx: Sender<KafkaError>,
}

impl fmt::Debug for Kafka {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[Sink::{}] Kafka: {}", &self.sink_url, self.config.topic)
    }
}

impl offramp::Impl for Kafka {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let producer = config.producer()?;
            // Create the thread pool where the expensive computation will be performed.
            let (dummy_tx, _) = bounded(1);

            // TODO: does this need to be unbounded?
            let (error_tx, error_rx) = bounded(crate::QSIZE);
            Ok(SinkManager::new_box(Self {
                sink_url: TremorURL::from_offramp_id("kafka")?, // dummy
                config,
                producer,
                postprocessors: vec![],
                reply_tx: dummy_tx,
                error_rx,
                error_tx,
            }))
        } else {
            Err("Kafka offramp requires a config".into())
        }
    }
}

fn is_fatal(e: &KafkaError) -> bool {
    match e {
        KafkaError::AdminOp(rdkafka::error::RDKafkaError::Fatal)
        | KafkaError::ConsumerCommit(rdkafka::error::RDKafkaError::Fatal)
        | KafkaError::Global(rdkafka::error::RDKafkaError::Fatal)
        | KafkaError::GroupListFetch(rdkafka::error::RDKafkaError::Fatal)
        | KafkaError::MessageConsumption(rdkafka::error::RDKafkaError::Fatal)
        | KafkaError::MessageProduction(rdkafka::error::RDKafkaError::Fatal)
        | KafkaError::MetadataFetch(rdkafka::error::RDKafkaError::Fatal)
        | KafkaError::OffsetFetch(rdkafka::error::RDKafkaError::Fatal)
        | KafkaError::SetPartitionOffset(rdkafka::error::RDKafkaError::Fatal)
        | KafkaError::StoreOffset(rdkafka::error::RDKafkaError::Fatal) => true,
        _ => false,
    }
}

unsafe fn get_fatal_error<C>(
    client: &rdkafka::client::Client<C>,
) -> Option<(rdkafka::types::RDKafkaRespErr, String)>
where
    C: rdkafka::ClientContext,
{
    const LEN: usize = 4096;
    let mut buf: [i8; LEN] = std::mem::MaybeUninit::uninit().assume_init();
    let client_ptr = client.native_ptr();

    let code = rdkafka_sys::bindings::rd_kafka_fatal_error(client_ptr, buf.as_mut_ptr(), LEN);
    if code == rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR {
        None
    } else {
        Some((code, rdkafka::util::cstr_to_owned(buf.as_ptr())))
    }
}

/// Waits for actual delivery to kafka cluster and sends ack or fail.
/// Also sends fatal errors for handling in offramp task.
async fn wait_for_delivery(
    sink_url: String,
    futures: Vec<rdkafka::producer::DeliveryFuture>,
    mut insight_event: Event,
    reply_tx: Sender<sink::Reply>,
    error_tx: Sender<KafkaError>,
) -> Result<()> {
    let cb = match futures::future::try_join_all(futures).await {
        Ok(results) => {
            if let Some((kafka_error, _)) = results.into_iter().find_map(std::result::Result::err) {
                error!(
                    "[Sink::{}] Error delivering kafka record: {}",
                    sink_url, &kafka_error
                );
                if is_fatal(&kafka_error) {
                    error_tx.send(kafka_error).await?;
                }
                CBAction::Fail
            } else {
                // all good. send ack
                CBAction::Ack
            }
        }
        Err(e) => {
            error!(
                "[Sink::{}] DeliveryFuture cancelled. Message delivery status unclear, considering it failed.: {}",
                sink_url, e
            );
            // oh noes, send fail
            CBAction::Fail
        }
    };
    insight_event.cb = cb;
    reply_tx.send(sink::Reply::Insight(insight_event)).await?;
    Ok(())
}

impl Kafka {
    fn drain_fatal_errors(&mut self) -> Result<()> {
        let mut handled = false;
        while let Ok(e) = self.error_rx.try_recv() {
            if !handled {
                // only handle on first fatal error
                self.handle_fatal_error(&e)?;
                handled = true;
            }
        }
        Ok(())
    }

    fn handle_fatal_error(&mut self, _fatal_error: &KafkaError) -> Result<()> {
        let maybe_fatal_error = unsafe { get_fatal_error(self.producer.client()) };
        if let Some(fatal_error) = maybe_fatal_error {
            error!(
                "[Sink::{}] Fatal Error({:?}): {}",
                &self.sink_url, fatal_error.0, fatal_error.1
            );
        }
        error!("[Sink::{}] Reinitiating client...", &self.sink_url);
        self.producer = self.config.producer()?;
        error!("[Sink::{}] Client reinitiated.", &self.sink_url);

        Ok(())
    }
}

#[async_trait::async_trait]
impl Sink for Kafka {
    async fn on_event(
        &mut self,
        _input: &str,
        codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        mut event: Event,
    ) -> ResultVec {
        // ensure we handle any fatal errors occured during last on_event invocation
        self.drain_fatal_errors()?;

        let ingest_ns = event.ingest_ns;
        let mut delivery_futures = Vec::with_capacity(event.len()); // might not be enough
        let mut insight_event = event.insight_ack(); // we gonna change the success status later, if need be
        for (value, meta) in event.value_meta_iter() {
            let encoded = codec.encode(value)?;
            let processed = postprocess(self.postprocessors.as_mut_slice(), ingest_ns, encoded)?;
            let meta_kafka_key = meta.get("kafka_key").and_then(Value::as_str);
            for payload in processed {
                // TODO: allow defining partition and timestamp in meta
                let mut record = FutureRecord::to(self.config.topic.as_str());
                record = record.payload(&payload);
                if let Some(kafka_key) = meta_kafka_key {
                    record = record.key(kafka_key);
                } else if let Some(kafka_key) = &self.config.key {
                    record = record.key(kafka_key.as_str());
                }

                // send out without blocking on delivery
                match self.producer.send_result(record) {
                    Ok(delivery_future) => {
                        delivery_futures.push(delivery_future);
                    }
                    Err((e, _)) => {
                        error!("[Sink::{}] failed to enque message: {}", &self.sink_url, e);
                        if is_fatal(&e) {
                            // handle fatal errors right here, without enqueueing
                            self.handle_fatal_error(&e)?;
                        }
                        // bail out with a CB fail on enqueue error
                        insight_event.cb = CBAction::Fail;
                        return Ok(Some(vec![sink::Reply::Insight(insight_event)]));
                    }
                }
            }
        }
        // successfully enqueued all messages
        // spawn the task waiting for delivery and send acks/fails then
        task::spawn(wait_for_delivery(
            self.sink_url.to_string(),
            delivery_futures,
            insight_event,
            self.reply_tx.clone(),
            self.error_tx.clone(),
        ));
        Ok(None)
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        _sink_uid: u64,
        sink_url: &TremorURL,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        _is_linked: bool,
        reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        self.postprocessors = make_postprocessors(processors.post)?;
        self.reply_tx = reply_channel;
        self.sink_url = sink_url.clone();
        Ok(())
    }
    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        self.drain_fatal_errors()?;
        Ok(None)
    }
    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        false
    }
    async fn terminate(&mut self) {
        if self.producer.in_flight_count() > 0 {
            // wait a second in order to flush messages.
            let wait_secs = 1;
            info!(
                "[Sink::{}] Flushing messages. Waiting for {} seconds.",
                wait_secs, &self.sink_url
            );
            self.producer.flush(Duration::from_secs(1));
            info!("[Sink::{}] Terminating.", &self.sink_url);
        }
    }
}
