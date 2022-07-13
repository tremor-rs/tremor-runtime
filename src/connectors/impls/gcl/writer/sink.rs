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

use super::meta;
use crate::connectors::google::AuthInterceptor;
use crate::connectors::impls::gcl::writer::Config;
use crate::connectors::prelude::*;
use crate::connectors::utils::pb;
use async_std::prelude::FutureExt;
use googapis::google::logging::v2::log_entry::Payload;
use googapis::google::logging::v2::logging_service_v2_client::LoggingServiceV2Client;
use googapis::google::logging::v2::{
    LogEntry,
    WriteLogEntriesRequest,
};
use gouth::Token;
use prost_types::Timestamp;
use std::time::Duration;
use tonic::codegen::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::{Code, Status};

pub(crate) struct GclSink {
    client: Option<LoggingServiceV2Client<InterceptedService<Channel, AuthInterceptor>>>,
    config: Config,
}

fn value_to_log_entry(
    timestamp: Timestamp,
    config: &Config,
    data: &Value,
    meta: Option<&Value>,
) -> Result<LogEntry> {
    Ok(LogEntry {
        log_name: config.log_name(meta),
        resource: super::value_to_monitored_resource(config.resource.as_ref())?,
        timestamp: Some(timestamp),
        receive_timestamp: None,
        severity: config.log_severity(meta)?,
        insert_id: meta::insert_id(meta),
        http_request: meta::http_request(meta),
        labels: Config::labels(meta),
        operation: meta::operation(meta),
        trace: meta::trace(meta),
        span_id: meta::span_id(meta),
        trace_sampled: meta::trace_sampled(meta)?,
        source_location: meta::source_location(meta),
        payload: Some(Payload::JsonPayload(pb::value_to_prost_struct(data)?)),
    })
}

impl GclSink {
    pub fn new(config: Config) -> Self {
        Self {
            client: None,
            config,
        }
    }

    #[cfg(test)]
    pub fn set_client(
        &mut self,
        client: LoggingServiceV2Client<InterceptedService<Channel, AuthInterceptor>>,
    ) {
        self.client = Some(client);
    }
}

#[async_trait::async_trait]
impl Sink for GclSink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let client = self.client.as_mut().ok_or(ErrorKind::ClientNotAvailable(
            "Google Cloud Logging",
            "The client is not connected",
        ))?;

        let mut entries = Vec::with_capacity(event.len());
        for (data, meta) in event.value_meta_iter() {
            let meta = meta.get("gcl_writer").or(None);
            #[allow(clippy::cast_precision_loss)]
            #[allow(clippy::cast_possible_wrap)]
            let mut timestamp = Timestamp {
                seconds: event.ingest_ns as i64 / 1_000_000_000i64,
                nanos: (event.ingest_ns % 1_000_000_000) as i32,
            };
            timestamp.normalize();
            entries.push(value_to_log_entry(timestamp, &self.config, data, meta)?);
        }

        let log_entries_response = client
            .write_log_entries(WriteLogEntriesRequest {
                log_name: self.config.log_name(None),
                resource: super::value_to_monitored_resource(self.config.resource.as_ref())?,
                labels: self.config.labels.clone(),
                entries,
                partial_success: self.config.partial_success,
                dry_run: self.config.dry_run,
            })
            .timeout(Duration::from_nanos(self.config.request_timeout))
            .await?;

        if let Err(error) = log_entries_response {
            error!("Failed to write a log entries: {}", error);

            if matches!(
                error.code(),
                Code::Aborted
                    | Code::Cancelled
                    | Code::DataLoss
                    | Code::DeadlineExceeded
                    | Code::Internal
                    | Code::ResourceExhausted
                    | Code::Unavailable
                    | Code::Unknown
            ) {
                ctx.swallow_err(
                    ctx.notifier.connection_lost().await,
                    "Failed to notify about Google Cloud Logging connection loss",
                );
            }

            Ok(SinkReply::FAIL)
        } else {
            Ok(SinkReply::ACK)
        }
    }

    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        info!("{} Connecting to Google Cloud Logging", ctx);
        let token = Token::new()?;

        let tls_config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(googapis::CERTIFICATES))
            .domain_name("logging.googleapis.com");

        let channel = Channel::from_static("https://logging.googleapis.com")
            .connect_timeout(Duration::from_nanos(self.config.connect_timeout))
            .tls_config(tls_config)?
            .connect()
            .await?;

        let client = LoggingServiceV2Client::with_interceptor(
            channel,
            AuthInterceptor {
                token: Box::new(move || match token.header_value() {
                    Ok(val) => Ok(val),
                    Err(e) => {
                        error!("Failed to get token for Google Logging Client: {}", e);

                        Err(Status::unavailable(
                            "Failed to retrieve authentication token.",
                        ))
                    }
                }),
            },
        );

        self.client = Some(client);

        Ok(true)
    }

    fn auto_ack(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::connectors::impls::gcl;
    use crate::connectors::tests::ConnectorHarness;
    use crate::connectors::ConnectionLostNotifier;
    use googapis::google::logging::r#type::LogSeverity;
    use std::sync::Arc;
    use tremor_value::{literal, structurize};

    #[test]
    fn fails_if_the_event_is_not_an_object() -> Result<()> {
        let now = tremor_common::time::nanotime();
        let mut timestamp = Timestamp {
            seconds: now as i64 / 1_000_000_000i64,
            nanos: (now % 1_000_000_000) as i32,
        };
        timestamp.normalize();
        let data = &literal!("snot");
        let config = Config::new(&literal!({}))?;
        let meta = literal!({});
        let meta = meta.get("gcl_writer").or(None);

        let result = value_to_log_entry(timestamp, &config, data, meta);
        if let Err(Error(ErrorKind::GclTypeMismatch("Value::Object", x), _)) = result {
            assert_eq!(x, ValueType::String);
            Ok(())
        } else {
            Err("Mapping did not fail on non-object event".into())
        }
    }

    #[async_std::test]
    async fn sink_succeeds_if_config_is_empty() -> Result<()> {
        let config = literal!({
            "config": {}
        });

        let result =
            ConnectorHarness::new(function_name!(), &gcl::writer::Builder::default(), &config)
                .await;

        assert!(result.is_ok());

        Ok(())
    }

    #[async_std::test]
    async fn on_event_fails_if_client_is_not_conected() -> Result<()> {
        let (rx, _tx) = async_std::channel::unbounded();
        let config = Config::new(&literal!({
            "connect_timeout": 1_000_000
        }))
        .unwrap();

        let mut sink = GclSink::new(config);

        let result = sink
            .on_event(
                "",
                Event::signal_tick(),
                &SinkContext {
                    uid: Default::default(),
                    alias: "".to_string(),
                    connector_type: Default::default(),
                    quiescence_beacon: Default::default(),
                    notifier: ConnectionLostNotifier::new(rx),
                },
                &mut EventSerializer::new(
                    None,
                    CodecReq::Structured,
                    vec![],
                    &ConnectorType::from(""),
                    "",
                )
                .unwrap(),
                0,
            )
            .await;

        assert!(result.is_err());
        Ok(())
    }

    #[async_std::test]
    async fn on_event_fails_if_write_stream_is_not_connected() -> Result<()> {
        let (rx, _tx) = async_std::channel::unbounded();
        let config = Config::new(&literal!({
            "connect_timeout": 1_000_000,
            "request_timeout": 1_000_000
        }))
        .unwrap();

        let mut sink = GclSink::new(config);
        sink.set_client(LoggingServiceV2Client::with_interceptor(
            Channel::from_static("http://example.com").connect_lazy(),
            AuthInterceptor {
                token: Box::new(|| Ok(Arc::new(String::new()))),
            },
        ));

        assert!(!sink.auto_ack());

        let result = sink
            .on_event(
                "",
                Event::signal_tick(),
                &SinkContext {
                    uid: Default::default(),
                    alias: "".to_string(),
                    connector_type: Default::default(),
                    quiescence_beacon: Default::default(),
                    notifier: ConnectionLostNotifier::new(rx),
                },
                &mut EventSerializer::new(
                    None,
                    CodecReq::Structured,
                    vec![],
                    &ConnectorType::from(""),
                    "",
                )
                .unwrap(),
                0,
            )
            .await;
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn log_name_override() -> Result<()> {
        let now = tremor_common::time::nanotime();
        let mut timestamp = Timestamp {
            seconds: now as i64 / 1_000_000_000i64,
            nanos: (now % 1_000_000_000) as i32,
        };
        timestamp.normalize();
        let config: Config = structurize(literal!({ "log_name": "snot" }))?;
        let data = literal!({"snot": "badger"});
        let meta = literal!({"log_name": "override"});
        let le = value_to_log_entry(timestamp, &config, &data, Some(&meta))?;
        assert_eq!("override", &le.log_name);

        Ok(())
    }

    #[test]
    fn log_severity_override() -> Result<()> {
        let now = tremor_common::time::nanotime();
        let mut timestamp = Timestamp {
            seconds: now as i64 / 1_000_000_000i64,
            nanos: (now % 1_000_000_000) as i32,
        };
        timestamp.normalize();
        let config: Config = structurize(literal!({}))?;
        let data = literal!({"snot": "badger"});
        let meta = literal!({"log_name": "override", "log_severity": LogSeverity::Debug as i32});
        let le = value_to_log_entry(timestamp, &config, &data, Some(&meta))?;
        assert_eq!("override", &le.log_name);
        assert_eq!(LogSeverity::Debug as i32, le.severity);

        Ok(())
    }
}
