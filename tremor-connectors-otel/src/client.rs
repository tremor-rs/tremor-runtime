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
use log::{error, warn};
use serde::Deserialize;
use tonic::transport::{Channel as TonicChannel, Endpoint as TonicEndpoint};
use tremor_common::url::Url;
use tremor_connectors::sink::prelude::*;
use tremor_otelapis::opentelemetry::proto::collector::{
    logs::v1::{logs_service_client::LogsServiceClient, ExportLogsServiceRequest},
    metrics::v1::{metrics_service_client::MetricsServiceClient, ExportMetricsServiceRequest},
    trace::v1::{trace_service_client::TraceServiceClient, ExportTraceServiceRequest},
};
use tremor_script::EventOriginUri;
use tremor_value::prelude::*;
const CONNECTOR_TYPE: &str = "otel_client";

// TODO Consider concurrency cap?

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// The hostname or IP address for the remote OpenTelemetry collector endpoint
    #[serde(default = "Default::default")]
    pub(crate) url: Url<OtelDefaults>,
    #[serde(default = "tremor_common::default_true")]
    pub(crate) logs: bool,
    /// Enables the trace service
    #[serde(default = "tremor_common::default_true")]
    pub(crate) trace: bool,
    /// Enables the metrics service
    #[serde(default = "tremor_common::default_true")]
    pub(crate) metrics: bool,
}

impl tremor_config::Impl for Config {}

/// The `OpenTelemetry` client connector
pub(crate) struct Client {
    config: Config,
    origin_uri: EventOriginUri,
}

// #[cfg_attr(coverage, no_coverage)]
impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OtelClient")
    }
}

/// The `OpenTelemetry` client connector builder
#[derive(Debug, Default)]
pub struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        CONNECTOR_TYPE.into()
    }

    async fn build_cfg(
        &self,
        _id: &alias::Connector,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
        let config = Config::new(config)?;
        let origin_uri = EventOriginUri {
            scheme: "tremor-otel-client".to_string(),
            host: config.url.host_or_local().to_string(),
            port: config.url.port(),
            path: vec![],
        };

        Ok(Box::new(Client { config, origin_uri }))
    }
}

#[derive(Clone)]
pub(crate) struct RemoteOpenTelemetryEndpoint {
    logs: LogsServiceClient<TonicChannel>,
    metrics: MetricsServiceClient<TonicChannel>,
    trace: TraceServiceClient<TonicChannel>,
}

#[async_trait::async_trait]
impl Connector for Client {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> anyhow::Result<Option<SinkAddr>> {
        let sink = OtelSink {
            origin_uri: self.origin_uri.clone(),
            config: self.config.clone(),
            remote: None,
        };
        Ok(Some(builder.spawn(sink, ctx)))
    }
}

#[allow(dead_code)]
struct OtelSink {
    origin_uri: EventOriginUri,
    config: Config,
    remote: Option<RemoteOpenTelemetryEndpoint>,
}

#[async_trait::async_trait()]
impl StructuredSink for OtelSink {
    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> anyhow::Result<bool> {
        let endpoint = self.config.url.to_string();
        let channel = TonicEndpoint::from_shared(endpoint)?.connect().await?;

        self.remote = Some(RemoteOpenTelemetryEndpoint {
            logs: LogsServiceClient::new(channel.clone()),
            metrics: MetricsServiceClient::new(channel.clone()),
            trace: TraceServiceClient::new(channel),
        });

        Ok(true)
    }
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        _start: u64,
    ) -> anyhow::Result<SinkReply> {
        if let Some(remote) = &mut self.remote {
            for value in event.value_iter() {
                let err = if self.config.metrics && value.contains_key("metrics") {
                    let request = ExportMetricsServiceRequest {
                        resource_metrics: ctx.bail_err(
                            metrics::resource_metrics_to_pb(Some(value)),
                            "Error converting payload to otel metrics",
                        )?,
                    };
                    remote.metrics.export(request).await.err()
                } else if self.config.logs && value.contains_key("logs") {
                    let request = ExportLogsServiceRequest {
                        resource_logs: ctx.bail_err(
                            logs::resource_logs_to_pb(value),
                            "Error converting payload to otel logs",
                        )?,
                    };
                    remote.logs.export(request).await.err()
                } else if self.config.trace && value.contains_key("trace") {
                    let request = ExportTraceServiceRequest {
                        resource_spans: ctx.bail_err(
                            trace::resource_spans_to_pb(Some(value)),
                            "Error converting payload to otel span",
                        )?,
                    };
                    remote.trace.export(request).await.err()
                } else {
                    warn!("{ctx} Invalid or disabled otel payload: {value}");
                    None
                };
                if let Some(e) = err {
                    error!("{ctx} Failed to dispatch otel event: {e}");
                    ctx.swallow_err(
                        ctx.notifier().connection_lost().await,
                        "Error notifying the runtime of connection loss",
                    );
                    return Ok(SinkReply::fail_or_none(event.transactional));
                }
            }

            Ok(SinkReply::ack_or_none(event.transactional))
        } else {
            error!("{ctx} Sending to a non connected sink!");
            ctx.notifier().connection_lost().await?;
            Ok(SinkReply::fail_or_none(event.transactional))
        }
    }

    fn auto_ack(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use tremor_common::alias;
    use tremor_connectors::config;
    use tremor_value::literal;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn otel_client_builder() -> anyhow::Result<()> {
        let alias = alias::Connector::new("flow", "my_otel_client");
        let with_processors = literal!({
            "config": {
                "url": "localhost:4317",
            },
        });
        let config: ConnectorConfig =
            config::Connector::from_config(&alias, "otel_client".into(), &with_processors)?;

        let builder = super::Builder::default();
        let kill_switch = KillSwitch::dummy();
        let _connector = builder.build(&alias, &config, &kill_switch).await?;

        Ok(())
    }
}
