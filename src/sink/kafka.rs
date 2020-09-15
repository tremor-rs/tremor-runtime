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

//! # Kafka Offramp
//!
//! The `kafka` offramp allows persisting events to a kafka queue.
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use crate::sink::prelude::*;
use crate::source::kafka::SmolRuntime;
use halfbrown::HashMap;
use rdkafka::config::ClientConfig;
use rdkafka::{
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
};
use std::fmt;
use std::time::Duration;

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
    #[serde(default = "dflt::d")]
    pub rdkafka_options: HashMap<String, String>,
    /// hostname to use, defaults to the hostname of the system
    #[serde(default = "d_host")]
    pub hostname: String,
    #[serde(default = "dflt::d")]
    pub key: Option<String>,
}

impl Config {
    fn producer(&self) -> Result<FutureProducer> {
        let mut producer_config = ClientConfig::new();
        let producer_config = producer_config
            .set("client.id", &format!("tremor-{}-{}", self.hostname, 0))
            .set("bootstrap.servers", &self.brokers.join(","))
            .set("message.timeout.ms", "5000")
            .set("queue.buffering.max.ms", "0");

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
    config: Config,
    producer: FutureProducer,
    postprocessors: Postprocessors,
}

impl fmt::Debug for Kafka {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Kafka: {}", self.config.topic)
    }
}

impl offramp::Impl for Kafka {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let producer = config.producer()?;
            // Create the thread pool where the expensive computation will be performed.

            Ok(SinkManager::new_box(Self {
                config,
                producer,
                postprocessors: vec![],
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

#[async_trait::async_trait]
impl Sink for Kafka {
    #[allow(clippy::used_underscore_binding)]
    async fn on_event(&mut self, _input: &str, codec: &dyn Codec, event: Event) -> ResultVec {
        let mut success = true;
        for value in event.value_iter() {
            let raw = codec.encode(value)?;
            let mut record = FutureRecord::to(&self.config.topic);
            record = record.payload(&raw);

            let record = if let Some(ref k) = self.config.key {
                record.key(k.as_str())
            } else {
                record
            };
            match self
                .producer
                .send_with_runtime::<SmolRuntime, _, _, _>(record, Duration::from_secs(0))
                .await
            {
                Ok(_) => {}
                Err((e, _r)) => {
                    error!("[Kafka Offramp] failed to enque message: {}", e);
                    if is_fatal(&e) {
                        if let Some((code, fatal)) =
                            unsafe { get_fatal_error(self.producer.client()) }
                        {
                            error!("[Kafka Offramp] Fatal Error({:?}): {}", code, fatal);
                        }
                        self.producer = self.config.producer()?;
                        error!("[Kafka Offramp] reinitiating client");
                    }
                    success = false;
                    break;
                }
            }
        }
        Ok(Some(vec![event.insight(success)]))
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    async fn init(&mut self, postprocessors: &[String]) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }
    #[allow(clippy::used_underscore_binding)]
    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        Ok(None)
    }
    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        false
    }
}
