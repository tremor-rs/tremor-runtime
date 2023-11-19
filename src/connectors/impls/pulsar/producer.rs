// Copyright 2023, The Tremor Team
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

//! pulsar Producer Connector
//! Sending events from tremor to a pulsar topic

use std::collections::HashMap;
use std::time::Duration;

use pulsar::{Producer, TokioExecutor};
use tokio::time::timeout;

use crate::connectors::impls::pulsar::client::get_client;
use crate::connectors::impls::pulsar::PulsarProducer;
use crate::connectors::prelude::*;
use crate::{connectors::impls::pulsar::PULSAR_CONNECT_TIMEOUT, utils::task_id};

const PULSAR_PRODUCER_META_KEY: &str = "pulsar_producer";

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// list of brokers forming a cluster. 1 is enough
    address: String,
    /// the topic to send to
    topic: String,
}

#[derive(Clone)]
pub(crate) struct ProducerConfig {
    tremor_config: Config,
    name: String,
}

impl tremor_config::Impl for Config {}

#[derive(Default, Debug)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        ConnectorType("pulsar_producer".into())
    }

    // TODO: Add other possible configurations and metrics
    async fn build_cfg(
        &self,
        alias: &alias::Connector,
        _config: &ConnectorConfig,
        raw_config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let tremor_config = Config::new(raw_config)?;

        let tid = task_id();
        let name = format!("tremor-{}-{alias}-{tid}", hostname());

        let config = ProducerConfig {
            tremor_config,
            name,
        };

        Ok(Box::new(PulsarProducerConnector { config }))
    }
}

#[derive(Clone)]
struct PulsarProducerConnector {
    config: ProducerConfig,
}

#[async_trait::async_trait()]
impl Connector for PulsarProducerConnector {
    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = PulsarProducerSink::new(self.config.clone());
        Ok(Some(builder.spawn(sink, ctx)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

struct PulsarProducerSink {
    config: ProducerConfig,
    producer: Option<PulsarProducer>,
}

impl PulsarProducerSink {
    fn new(config: ProducerConfig) -> Self {
        Self {
            config,
            producer: None,
        }
    }

    async fn get_producer(&mut self) -> Result<Producer<TokioExecutor>> {
        let client = get_client(&self.config.tremor_config.address).await?;
        let producer = client
            .producer()
            .with_topic(self.config.tremor_config.topic.clone())
            .with_name(self.config.name.clone())
            .build()
            .await?;
        Ok(producer)
    }
}

#[async_trait::async_trait]
impl Sink for PulsarProducerSink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let producer = self
            .producer
            .as_mut()
            .ok_or_else(|| ErrorKind::ProducerNotAvailable(ctx.alias().to_string()))?;

        let ingest_ns = event.ingest_ns;
        for (value, meta) in event.value_meta_iter() {
            let pulsar_meta = meta.get(PULSAR_PRODUCER_META_KEY);

            for payload in serializer.serialize(value, meta, ingest_ns).await? {
                let mut properties = HashMap::new();
                let partition_key = pulsar_meta
                    .get("key")
                    .and_then(|k| k.as_str())
                    .map(String::from);
                if let Some(headers_obj) = pulsar_meta.get_object("headers") {
                    for (k, v) in headers_obj {
                        if let Some(value_str) = v.as_str() {
                            properties.insert(k.to_string(), value_str.to_string());
                        }
                    }
                }

                let message = pulsar::producer::Message {
                    partition_key,
                    properties,
                    payload,
                    ..Default::default()
                };
                producer.send(message).await?;
            }
        }
        Ok(SinkReply::ACK)
    }

    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        // enforce cleaning out the previous producer
        // We might lose some in flight messages in case of failure on the broker side

        if let Some(mut old_producer) = self.producer.take() {
            old_producer.close().await?;
            drop(old_producer);
        };

        info!("{ctx} Connecting pulsar producer");
        // check if we receive any error callbacks
        match timeout(PULSAR_CONNECT_TIMEOUT, self.get_producer()).await {
            Err(timeout) => {
                return Err(timeout.into());
            }
            Ok(Err(pulsar_error)) => Err(pulsar_error.into()),
            Ok(Ok(producer)) => {
                self.producer = Some(producer);
                Ok(true)
            }
        }
    }

    async fn on_stop(&mut self, ctx: &SinkContext) -> Result<()> {
        if let Some(mut producer) = self.producer.take() {
            let wait_secs = Duration::from_secs(1);
            info!("{ctx} Flushing messages. Waiting for {wait_secs:?} seconds.");
            timeout(wait_secs, producer.close()).await??;
        }
        Ok(())
    }

    fn auto_ack(&self) -> bool {
        false
    }
}
