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

use crate::connectors::google::GouthTokenProvider;
use crate::connectors::impls::gbq::writer::sink::{GbqSink, TonicChannelFactory};
use crate::connectors::prelude::*;
use crate::connectors::{Connector, ConnectorBuilder, ConnectorConfig, ConnectorType};
use serde::Deserialize;
use tremor_pipeline::ConfigImpl;

#[derive(Deserialize, Clone)]
pub(crate) struct Config {
    pub table_id: String,
    pub connect_timeout: u64,
    pub request_timeout: u64,
}
impl ConfigImpl for Config {}

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
        let sink = GbqSink::<GouthTokenProvider, _>::new(
            self.config.clone(),
            Box::new(TonicChannelFactory),
        );

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
    use super::*;
    use crate::connectors::metrics::SinkReporter;
    use crate::connectors::reconnect::ConnectionLostNotifier;
    use crate::connectors::sink::builder;

    #[async_std::test]
    pub async fn can_spawn_sink() {
        let mut connector = Gbq {
            config: Config {
                table_id: "test".into(),
                connect_timeout: 1,
                request_timeout: 1,
            },
        };

        let sink_address = connector
            .create_sink(
                SinkContext {
                    uid: Default::default(),
                    alias: Alias::new("a", "b"),
                    connector_type: Default::default(),
                    quiescence_beacon: Default::default(),
                    notifier: ConnectionLostNotifier::new(async_std::channel::unbounded().0),
                },
                builder(
                    &ConnectorConfig::default(),
                    CodecReq::Structured,
                    &Alias::new("a", "b"),
                    128,
                    SinkReporter::new(Alias::new("a", "b"), async_broadcast::broadcast(1).0, None),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(sink_address.is_some());
    }
}
