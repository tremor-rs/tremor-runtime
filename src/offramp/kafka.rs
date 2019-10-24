// Copyright 2018-2019, Wayfair GmbH
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
use futures::Future;
use halfbrown::HashMap;
use hostname::get_hostname;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::fmt;
use tokio_threadpool as thread_pool;

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
    match get_hostname() {
        Some(h) => h,
        None => "tremor-host.local".to_string(),
    }
}

/// Kafka offramp connectoz
pub struct Kafka {
    producer: FutureProducer,
    pool: thread_pool::ThreadPool,
    topic: String,
    key: Option<String>,
    pipelines: HashMap<TremorURL, PipelineAddr>,
    postprocessors: Postprocessors,
}

impl fmt::Debug for Kafka {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Kafka: {}", self.topic)
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
            let pool = thread_pool::Builder::new()
                .name_prefix("kafka-pool-")
                .pool_size(config.threads)
                .build();
            Ok(Box::new(Self {
                pool,
                producer,
                topic: config.topic.clone(),
                pipelines: HashMap::new(),
                postprocessors: vec![],
                key,
            }))
        } else {
            Err("Kafka offramp requires a config".into())
        }
    }
}

impl Offramp for Kafka {
    // TODO
    fn on_event(&mut self, codec: &Box<dyn Codec>, _input: String, event: Event) -> Result<()> {
        for value in event.value_iter() {
            let raw = codec.encode(value)?;
            let mut record = FutureRecord::to(&self.topic);
            record = record.payload(&raw);
            //TODO: Key
            let r = if let Some(ref k) = self.key {
                self.producer.send(record.key(k.as_str()), 1)
            } else {
                self.producer.send(record, 1)
            };
            let producer_future = r.then(|_result| {
                // match result {
                //     Ok(Ok(_delivery)) => ret.send(),
                //     Ok(Err((e, _))) => ret.with_value(Err(e.into())).send(),
                //     Err(_) => ret.with_value(Err("Future cancled".into())).send(),
                // }
                Ok(())
            });
            self.pool.spawn(producer_future);
        }
        Ok(())
    }
    fn add_pipeline(&mut self, id: TremorURL, addr: PipelineAddr) {
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
