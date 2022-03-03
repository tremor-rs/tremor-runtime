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

use async_std::{
    channel::{bounded, Receiver, Sender, TryRecvError},
    task,
};

use super::{logs, metrics, trace};
use crate::connectors::prelude::*;
use async_std::task::JoinHandle;
use tremor_otelapis::all::{self, OpenTelemetryEvents};
const CONNECTOR_TYPE: &str = "otel_server";

// TODO Consider concurrency cap?

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    // TODO FIXME replace host/port pair with http(s) `endpoint`
    /// The hostname or IP address for the remote OpenTelemetry collector endpoint
    #[serde(default = "default_localhost")]
    pub host: String,
    /// The TCP port for the remote OpenTelemetry collector endpoint
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "d_true")]
    pub logs: bool,
    /// Enables the trace service
    #[serde(default = "d_true")]
    pub trace: bool,
    /// Enables the metrics service
    #[serde(default = "d_true")]
    pub metrics: bool,
}

fn default_localhost() -> String {
    "localhost".to_string()
}

fn default_port() -> u16 {
    4317
}

fn d_true() -> bool {
    true
}

impl ConfigImpl for Config {}

/// The OpenTelemetry client connector
pub struct OtelServer {
    config: Config,
    #[allow(dead_code)]
    id: String,
    origin_uri: EventOriginUri,
    accept_task: Option<JoinHandle<Result<()>>>,
    tx: Sender<OpenTelemetryEvents>,
    rx: Receiver<OpenTelemetryEvents>,
}

impl std::fmt::Debug for OtelServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OtelServer")
    }
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        CONNECTOR_TYPE.into()
    }

    async fn from_config(
        &self,
        id: &str,
        connector_config: &ConnectorConfig,
    ) -> Result<Box<dyn Connector>> {
        let origin_uri = EventOriginUri {
            scheme: "tremor-otel-server".to_string(),
            host: "localhost".to_string(),
            port: None,
            path: vec![],
        };
        let (tx, rx) = bounded(128);
        if let Some(config) = &connector_config.config {
            Ok(Box::new(OtelServer {
                config: Config::new(&config)?,
                id: id.to_string(),
                origin_uri,
                accept_task: None,
                tx,
                rx,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(String::from("OtelServer")).into())
        }
    }
}

#[async_trait::async_trait]
impl Connector for OtelServer {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = OtelSource {
            origin_uri: self.origin_uri.clone(),
            config: self.config.clone(),
            rx: self.rx.clone(),
        };
        builder.spawn(source, source_context).map(Some)
    }

    async fn create_sink(
        &mut self,
        _sink_context: SinkContext,
        _builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        Ok(None)
    }

    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        let endpoint = format!("{}:{}", self.config.host, self.config.port).parse()?;

        if let Some(previous_handle) = self.accept_task.take() {
            previous_handle.cancel().await;
        }

        let tx = self.tx.clone();
        task::spawn(async move {
            match all::make(endpoint, tx).await {
                Ok(()) => (),
                Err(e) => {
                    dbg!(&e);
                    error!("Could not start Otel gRPC server: {}", e);
                }
            };
        });
        Ok(true)
    }
}

/// Time to await an answer before handing control back to the source manager
const SOURCE_RECV_TIMEOUT: u64 = 50;

struct OtelSource {
    origin_uri: EventOriginUri,
    config: Config,
    rx: Receiver<OpenTelemetryEvents>,
}

#[async_trait::async_trait()]
impl Source for OtelSource {
    async fn pull_data(&mut self, _pull_id: &mut u64, ctx: &SourceContext) -> Result<SourceReply> {
        match self.rx.try_recv() {
            Ok(OpenTelemetryEvents::Metrics(metrics)) => {
                if self.config.metrics {
                    let data: Value = metrics::resource_metrics_to_json(metrics);
                    return Ok(SourceReply::Structured {
                        origin_uri: self.origin_uri.clone(),
                        payload: data.into(),
                        stream: DEFAULT_STREAM_ID,
                        port: None,
                    });
                }
                warn!("Otel Source received metrics event when trace support is disabled. Dropping trace");
            }
            Ok(OpenTelemetryEvents::Logs(logs)) => {
                if self.config.logs {
                    let data: Value = logs::resource_logs_to_json(logs)?;
                    return Ok(SourceReply::Structured {
                        origin_uri: self.origin_uri.clone(),
                        payload: data.into(),
                        stream: DEFAULT_STREAM_ID,
                        port: None,
                    });
                }
                warn!(
                    "Otel Source received log event when trace support is disabled. Dropping trace"
                );
            }
            Ok(OpenTelemetryEvents::Trace(traces)) => {
                if self.config.trace {
                    let data: Value = trace::resource_spans_to_json(traces);
                    return Ok(SourceReply::Structured {
                        origin_uri: self.origin_uri.clone(),
                        payload: data.into(),
                        stream: DEFAULT_STREAM_ID,
                        port: None,
                    });
                }
                warn!("Otel Source received trace event when trace support is disabled. Dropping trace");
            }
            Err(TryRecvError::Closed) => {
                ctx.notifier().notify().await?;
            }
            _ => (),
        };
        Ok(SourceReply::Empty(SOURCE_RECV_TIMEOUT))
    }

    fn is_transactional(&self) -> bool {
        true
    }

    fn asynchronous(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use env_logger;
    // use http_types::Method;

    #[async_std::test]
    async fn otel_client_builder() -> Result<()> {
        let with_processors = literal!({
            "id": "my_otel_server",
            "type": "otel_server",
            "config": {
                "host": "localhost",
                "port": 4317,
            },
        });
        let config: ConnectorConfig = crate::config::Connector::from_config(
            "snot",
            ConnectorType("otel_server".into()),
            with_processors,
        )?;

        let builder = super::Builder::default();
        let _connector = builder.from_config("foo", &config).await?;

        Ok(())
    }
}
