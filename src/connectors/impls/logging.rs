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

use crate::connectors::prelude::*;
use async_broadcast::Receiver;
use tremor_pipeline::{LoggingMsg, LOGGING_CHANNEL};

use crate::connectors::{prelude::KillSwitch, ConnectorBuilder, ConnectorType};

#[derive()]
pub(crate) struct PluggableLoggingConnector {
    rx: Receiver<LoggingMsg>,
}

impl PluggableLoggingConnector {
    pub(crate) fn new() -> Self {
        Self {
            rx: LOGGING_CHANNEL.rx(),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "logging".into()
    }
    async fn build(
        &self,
        _id: &Alias,
        _config: &ConnectorConfig,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        Ok(Box::new(PluggableLoggingConnector::new()))
    }
}
#[async_trait::async_trait()]
impl Connector for PluggableLoggingConnector {
    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        Ok(true)
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = LoggingSource::new(self.rx.clone());
        let addr = builder.spawn(source, source_context)?;
        Ok(Some(addr))
    }

    async fn create_sink(
        &mut self,
        _sink_context: SinkContext,
        _builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        Ok(None)
    }
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }
}
pub(crate) struct LoggingSource {
    rx: Receiver<LoggingMsg>,
    origin_uri: EventOriginUri,
}

impl LoggingSource {
    pub(crate) fn new(rx: Receiver<LoggingMsg>) -> Self {
        Self {
            rx,
            origin_uri: EventOriginUri {
                scheme: "tremor-logging".to_string(),
                host: hostname(),
                port: None,
                path: vec![],
            },
        }
    }
}

#[async_trait::async_trait()]
impl Source for LoggingSource {
    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        let msg = self
            .rx
            .recv()
            .await
            .map_err(|e| Error::from(format!("error: {}", e)))?;
        Ok(SourceReply::Structured {
            payload: msg.payload,
            origin_uri: msg.origin_uri.unwrap_or_else(|| self.origin_uri.clone()),
            stream: DEFAULT_STREAM_ID,
            port: None,
        })
    }

    fn is_transactional(&self) -> bool {
        false
    }

    /// The logging connector is actually `asynchronous` in that its data is produced outside the source task
    /// (and outside of the control of the `pull_data` function).
    ///
    /// But we set it to `false` here, as in case of quiescence
    /// we don't need to flush logging data. Also the producing ends do not use the quiescence_beacon
    /// which would tell them to stop sending. There could be multiple logging connectors running at the same time
    /// and one connector quiescing should not lead to logging being stopped for each and every other connector.
    fn asynchronous(&self) -> bool {
        false
    }
}
