// Copyright 2020-2021, The Tremor Team
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

//! # File Offramp
//!
//! Writes events to a file, one event per line
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

#![cfg(not(tarpaulin_include))]

use crate::connectors::otel::{logs, metrics, trace};
use crate::sink::prelude::*;
use halfbrown::HashMap;
use tonic::transport::Channel as TonicChannel;
use tonic::transport::Endpoint as TonicEndpoint;
use tremor_otelapis::opentelemetry::proto::collector::{
    logs::v1::{logs_service_client::LogsServiceClient, ExportLogsServiceRequest},
    metrics::v1::{metrics_service_client::MetricsServiceClient, ExportMetricsServiceRequest},
    trace::v1::{trace_service_client::TraceServiceClient, ExportTraceServiceRequest},
};

#[allow(dead_code)]
pub struct RemoteOpenTelemetryEndpoint {
    logs_client: LogsServiceClient<TonicChannel>,
    metrics_client: MetricsServiceClient<TonicChannel>,
    trace_client: TraceServiceClient<TonicChannel>,
}

#[allow(dead_code)]
pub struct OpenTelemetry {
    config: Config,
    endpoint: String,
    remote: Option<RemoteOpenTelemetryEndpoint>,
}

#[derive(Deserialize)]
pub struct Config {
    /// The gRPC endpoint hostname for the remote OpenTelemetry collector endpoint
    pub host: String,
    /// The gRPC endpoint TCP port for the remote OpenTelemetry collector endpoint
    pub port: u16,
}

impl ConfigImpl for Config {}

impl offramp::Impl for OpenTelemetry {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let endpoint = format!("https://{}:{}", config.host.clone().as_str(), config.port);
            Ok(SinkManager::new_box(Self {
                config,
                endpoint,
                remote: None,
            }))
        } else {
            Err("Offramp otel requires a config".into())
        }
    }
}

fn json_otel_logs_to_pb<'event>(json: &Value<'event>) -> Result<ExportLogsServiceRequest> {
    let pb = ExportLogsServiceRequest {
        resource_logs: logs::resource_logs_to_pb(json)?,
    };
    Ok(pb)
}

fn json_otel_trace_to_pb<'event>(json: &Value<'event>) -> Result<ExportTraceServiceRequest> {
    let pb = ExportTraceServiceRequest {
        resource_spans: trace::resource_spans_to_pb(json.get("trace"))?,
    };
    Ok(pb)
}

fn json_otel_metrics_to_pb<'event>(json: &Value<'event>) -> Result<ExportMetricsServiceRequest> {
    let pb = ExportMetricsServiceRequest {
        resource_metrics: metrics::resource_metrics_to_pb(json.get("metrics"))?,
    };
    Ok(pb)
}

#[async_trait::async_trait]
impl Sink for OpenTelemetry {
    async fn terminate(&mut self) {}

    async fn on_event(
        &mut self,
        _input: &str,
        _codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        mut event: Event,
    ) -> ResultVec {
        if let Some(_remote) = &mut self.remote {
            for value in event.value_iter() {
                if let Some(ref mut remote) = &mut self.remote {
                    if let Value::Object(o) = value {
                        if o.contains_key("metrics") {
                            let request = json_otel_metrics_to_pb(value)?;
                            match remote.metrics_client.export(request).await {
                                Ok(_response) => {}
                                Err(_e) => {
                                    // TODO enhance error handling
                                    error!("Failed to dispatch otel/gRPC metrics message");
                                }
                            };
                        } else if o.contains_key("logs") {
                            let request = json_otel_logs_to_pb(value)?;
                            match remote.logs_client.export(request).await {
                                Ok(_response) => {}
                                Err(_e) => {
                                    // TODO enhance error handling
                                    error!("Failed to dispatch otel/gRPC log message");
                                }
                            };
                        } else if o.contains_key("trace") {
                            let request = json_otel_trace_to_pb(value)?;
                            match remote.trace_client.export(request).await {
                                Ok(_response) => {}
                                Err(_e) => {
                                    // TODO enhance error handling
                                    error!("Failed to dispatch otel/gRPC trace message");
                                }
                            };
                        } else {
                            // unknown / drop
                        }
                    } else {
                        // Error
                    }
                }
            }
        }

        Ok(Some(vec![sink::Reply::Insight(event.insight_ack())]))
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        _sink_uid: u64,
        _sink_url: &TremorURL,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        _processors: Processors<'_>,
        _is_linked: bool,
        _reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        let channel = TonicEndpoint::from_shared(self.endpoint.clone())
            .map_err(|e| format!("Unable to connect to remote otel endpoint: {}", e))?
            .connect()
            .await;

        let channel = match channel {
            Ok(channel) => channel,
            Err(e) => return Err(format!("Unable to open remote otel channel {}", e).into()),
        };

        let logs_client = LogsServiceClient::new(channel.clone());
        let metrics_client = MetricsServiceClient::new(channel.clone());
        let trace_client = TraceServiceClient::new(channel);

        self.remote = Some(RemoteOpenTelemetryEndpoint {
            logs_client,
            metrics_client,
            trace_client,
        });

        Ok(())
    }
    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        Ok(None)
    }
    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        true
    }
}
