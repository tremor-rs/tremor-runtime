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

mod sink;

use crate::connectors::impls::gbq::writer::sink::GbqSink;
use crate::connectors::prelude::*;
use crate::connectors::{Connector, ConnectorBuilder, ConnectorConfig, ConnectorType};
use serde::Deserialize;
use tremor_pipeline::ConfigImpl;

#[derive(Deserialize, Clone)]
pub(crate) struct Config {
    pub table_id: String,
    pub connect_timeout: u64,
    pub request_timeout: u64,
    // #[serde(default = "default_request_size_limit")]
    // pub request_size_limit: usize,
}
impl ConfigImpl for Config {}

// fn default_request_size_limit() -> usize {
//     // 10MB
//     10 * 1024 * 1024
// }

#[derive(Debug, Default)]
pub(crate) struct Builder {}

struct Gbq {
    config: Config,
}

#[async_trait::async_trait]
impl Connector for Gbq {
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = GbqSink::new(self.config.clone());

        builder.spawn(sink, sink_context).map(Some)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }
}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "gbq".into()
    }

    async fn build_cfg(
        &self,
        _: &Alias,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(config)?;
        Ok(Box::new(Gbq { config }))
    }
}

#[cfg(test)]
mod tests {
    use tremor_common::ids::SinkId;

    use super::*;
    use crate::connectors::reconnect::ConnectionLostNotifier;
    use crate::connectors::sink::builder;
    use crate::connectors::{metrics::SinkReporter, utils::quiescence::QuiescenceBeacon};

    #[async_std::test]
    pub async fn can_spawn_sink() -> Result<()> {
        let mut connector = Gbq {
            config: Config {
                table_id: "test".into(),
                connect_timeout: 1,
                request_timeout: 1,
                //                request_size_limit: 10 * 1024 * 1024,
            },
        };

        let sink_address = connector
            .create_sink(
                SinkContext {
                    uid: SinkId::default(),
                    alias: Alias::new("a", "b"),
                    connector_type: ConnectorType::default(),
                    quiescence_beacon: QuiescenceBeacon::default(),
                    notifier: ConnectionLostNotifier::new(async_std::channel::unbounded().0),
                },
                builder(
                    &ConnectorConfig::default(),
                    CodecReq::Structured,
                    &Alias::new("a", "b"),
                    128,
                    SinkReporter::new(Alias::new("a", "b"), async_broadcast::broadcast(1).0, None),
                )?,
            )
            .await?;
        assert!(sink_address.is_some());
        Ok(())
    }
}
