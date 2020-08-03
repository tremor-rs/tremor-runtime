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

use crate::offramp::prelude::*;
use halfbrown::HashMap;
use rdkafka::config::ClientConfig;
use rdkafka::{
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
};
use std::fmt;

#[derive(Deserialize)]
pub struct Config {
    /// list of brokers
    pub brokers: Vec<String>,
    /// the topic to send to
    pub topic: String,
    /// the number of threads in the async worker pool handling writing to kafka (default: 4)
    #[serde(default = "dflt::d_4")]
    pub threads: usize,
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

impl ConfigImpl for Config {}

fn d_host() -> String {
    hostname()
}

/// Kafka offramp connectoz
pub struct Kafka {
    config: Config,
    producer: FutureProducer,
    key: Option<String>,
    pipelines: HashMap<TremorURL, pipeline::Addr>,
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
            let mut producer_config = ClientConfig::new();
            let producer_config = producer_config
                .set("client.id", &format!("tremor-{}-{}", config.hostname, 0))
                .set("bootstrap.servers", &config.brokers.join(","))
                .set("message.timeout.ms", "5000")
                .set("queue.buffering.max.ms", "0");

            let producer = config
                .rdkafka_options
                .iter()
                .fold(producer_config, |c: &mut ClientConfig, (k, v)| c.set(k, v))
                .create()?;
            let key = config.key.clone();
            // Create the thread pool where the expensive computation will be performed.

            Ok(Box::new(Self {
                config,
                producer,

                pipelines: HashMap::new(),
                postprocessors: vec![],
                key,
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

impl Offramp for Kafka {
    // TODO
    fn on_event(&mut self, codec: &Box<dyn Codec>, _input: String, event: Event) -> Result<()> {
        for value in event.value_iter() {
            let raw = codec.encode(value)?;
            let mut record = FutureRecord::to(&self.config.topic);
            record = record.payload(&raw);
            //TODO: Key

            let record = if let Some(ref k) = self.key {
                record.key(k.as_str())
            } else {
                record
            };
            match self.producer.send_result(record) {
                Ok(f) => {
                    task::spawn(f);
                }
                Err((e, _r)) => {
                    error!("[Kafka Offramp] failed to enque message: {}", e);
                    if is_fatal(&e) {
                        if let Some((code, fatal)) =
                            unsafe { get_fatal_error(self.producer.client()) }
                        {
                            error!("[Kafka Offramp] Fatal Error({:?}): {}", code, fatal);
                        }

                        let mut producer_config = ClientConfig::new();
                        let producer_config = producer_config
                            .set(
                                "client.id",
                                &format!("tremor-{}-{}", self.config.hostname, 0),
                            )
                            .set("bootstrap.servers", &self.config.brokers.join(","))
                            .set("message.timeout.ms", "5000")
                            .set("queue.buffering.max.ms", "0");

                        self.producer = self
                            .config
                            .rdkafka_options
                            .iter()
                            .fold(producer_config, |c: &mut ClientConfig, (k, v)| c.set(k, v))
                            .create()?;
                        error!("[Kafka Offramp] reinitiating client");
                    } else {
                        self.producer.poll(std::time::Duration::from_millis(10));
                    }
                }
            }
        }
        Ok(())
    }
    fn add_pipeline(&mut self, id: TremorURL, addr: pipeline::Addr) {
        self.pipelines.insert(id, addr);
    }
    fn remove_pipeline(&mut self, id: TremorURL) -> bool {
        self.pipelines.remove(&id);
        self.pipelines.is_empty()
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    fn start(&mut self, _codec: &Box<dyn Codec>, postprocessors: &[String]) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }
}
