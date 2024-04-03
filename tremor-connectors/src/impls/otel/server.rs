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

use super::{common::OtelDefaults, logs, metrics, trace};
use crate::prelude::*;
use async_std::channel::{bounded, Receiver, Sender};
use tokio::task::JoinHandle;
use tremor_otelapis::all::{self, OpenTelemetryEvents};
const CONNECTOR_TYPE: &str = "otel_server";

// TODO Consider concurrency cap?

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// The hostname or IP address for the remote OpenTelemetry collector endpoint
    #[serde(default = "Default::default")]
    pub(crate) url: Url<OtelDefaults>,
    #[serde(default = "default_true")]
    pub(crate) logs: bool,
    /// Enables the trace service
    #[serde(default = "default_true")]
    pub(crate) trace: bool,
    /// Enables the metrics service
    #[serde(default = "default_true")]
    pub(crate) metrics: bool,
}

impl tremor_config::Impl for Config {}

/// The `OpenTelemetry` client connector
pub(crate) struct Server {
    config: Config,
    #[allow(dead_code)]
    id: String,
    origin_uri: EventOriginUri,
    accept_task: Option<JoinHandle<anyhow::Result<()>>>,
    tx: Sender<OpenTelemetryEvents>,
    rx: Option<Receiver<OpenTelemetryEvents>>,
}

// #[cfg_attr(coverage, no_coverage)]
impl std::fmt::Debug for Server {
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

    async fn build_cfg(
        &self,
        id: &alias::Connector,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
        let origin_uri = EventOriginUri {
            scheme: "tremor-otel-server".to_string(),
            host: "localhost".to_string(),
            port: None,
            path: vec![],
        };
        let (tx, rx) = bounded(128);
        Ok(Box::new(Server {
            config: Config::new(config)?,
            id: id.to_string(),
            origin_uri,
            accept_task: None,
            tx,
            rx: Some(rx),
        }))
    }
}

#[async_trait::async_trait]
impl Connector for Server {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> anyhow::Result<Option<SourceAddr>> {
        let source = OtelSource {
            origin_uri: self.origin_uri.clone(),
            config: self.config.clone(),
            rx: self
                .rx
                .take()
                .ok_or(GenericImplementationError::AlreadyConnected)?,
        };
        Ok(Some(builder.spawn(source, ctx)))
    }

    async fn create_sink(
        &mut self,
        _ctx: SinkContext,
        _builder: SinkManagerBuilder,
    ) -> anyhow::Result<Option<SinkAddr>> {
        Ok(None)
    }

    async fn connect(
        &mut self,
        ctx: &ConnectorContext,
        _attempt: &Attempt,
    ) -> anyhow::Result<bool> {
        let host = self.config.url.host_str().ok_or_else(|| {
            crate::Error::InvalidConfiguration(ctx.alias().clone(), "Missing host for otel server")
        })?;
        let port = self.config.url.port().ok_or_else(|| {
            crate::Error::InvalidConfiguration(ctx.alias().clone(), "Missing port for otel server")
        })?;
        let endpoint = format!("{host}:{port}").parse()?;

        if let Some(previous_handle) = self.accept_task.take() {
            previous_handle.abort();
        }

        let tx = self.tx.clone();

        spawn_task(
            ctx.clone(),
            async move { Ok(all::make(endpoint, tx).await?) },
        );
        Ok(true)
    }
}

struct OtelSource {
    origin_uri: EventOriginUri,
    config: Config,
    rx: Receiver<OpenTelemetryEvents>,
}

#[async_trait::async_trait()]
impl Source for OtelSource {
    async fn pull_data(
        &mut self,
        pull_id: &mut u64,
        ctx: &SourceContext,
    ) -> anyhow::Result<SourceReply> {
        let (data, remote) = match self.rx.recv().await? {
            OpenTelemetryEvents::Metrics(metrics, remote) if self.config.metrics => {
                (metrics::resource_metrics_to_json(metrics), remote)
            }
            OpenTelemetryEvents::Logs(logs, remote) if self.config.logs => {
                (logs::resource_logs_to_json(logs)?, remote)
            }
            OpenTelemetryEvents::Trace(traces, remote) if self.config.trace => {
                (trace::resource_spans_to_json(traces), remote)
            }
            _ => {
                warn!("{ctx} Source received event when support is disabled. Dropping.");
                return self.pull_data(pull_id, ctx).await;
            }
        };

        let mut origin_uri = self.origin_uri.clone();
        if let Some(remote) = remote {
            origin_uri.host = remote.ip().to_string();
            origin_uri.port = Some(remote.port());
        }
        Ok(SourceReply::Structured {
            origin_uri,
            payload: data.into(),
            stream: DEFAULT_STREAM_ID,
            port: None,
        })
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

    #[tokio::test(flavor = "multi_thread")]
    async fn otel_client_builder() -> anyhow::Result<()> {
        let alias = alias::Connector::new("test", "my_otel_server");
        let with_processors = literal!({
            "config": {
                "url": "localhost:4317",
            },
        });
        let config: ConnectorConfig = crate::config::Connector::from_config(
            &alias,
            ConnectorType("otel_server".into()),
            &with_processors,
        )?;
        let alias = alias::Connector::new("flow", "my_otel_server");

        let builder = super::Builder::default();
        let kill_switch = KillSwitch::dummy();
        let _connector = builder.build(&alias, &config, &kill_switch).await?;

        Ok(())
    }
}
