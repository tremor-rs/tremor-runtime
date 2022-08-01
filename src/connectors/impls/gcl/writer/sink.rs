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

use std::time::Duration;

use super::meta;
use crate::connectors::google::AuthInterceptor;
use crate::connectors::impls::gcl::writer::Config;
use crate::connectors::prelude::*;
use crate::connectors::utils::pb;
use async_std::channel::bounded;
use async_std::channel::Sender;
use async_std::prelude::FutureExt;
use async_std::task::JoinHandle;
use googapis::google::logging::v2::log_entry::Payload;
use googapis::google::logging::v2::logging_service_v2_client::LoggingServiceV2Client;
use googapis::google::logging::v2::{LogEntry, WriteLogEntriesRequest};
use gouth::Token;
use prost_types::Timestamp;
use tonic::codegen::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::{Code, Status};
use tremor_common::time::nanotime;

#[allow(clippy::large_enum_variant)]
pub(crate) enum GclEvent {
    #[allow(dead_code)] // NOTE This is used by the tests and not intended for use in production
    Shutdown,
    LogEvent(Event, Vec<LogEntry>),
}

impl GclEvent {
    async fn log_event(config: &Config, event: Event) -> Result<GclEvent> {
        let timestamp = event.ingest_ns;
        let mut entries = Vec::with_capacity(event.len());
        for (data, meta) in event.value_meta_iter() {
            let meta = meta.get("gcl_writer").or(None);
            #[allow(clippy::cast_precision_loss)]
            #[allow(clippy::cast_possible_wrap)]
            let mut timestamp = Timestamp {
                seconds: (timestamp / 1_000_000_000) as i64,
                nanos: (timestamp % 1_000_000_000) as i32,
            };
            timestamp.normalize();
            entries.push(value_to_log_entry(timestamp, config, data, meta)?);
        }

        Ok(GclEvent::LogEvent(event, entries))
    }
}

pub(crate) struct GclClient {
    #[allow(unused)] // NOTE FIXME consider kill switch and quiescence
    join_handle: JoinHandle<Result<()>>,
    #[allow(unused)] // NOTE Preserved so that auth token can be refreshed and renewed
    token: Token,
    sender: Sender<GclEvent>,
}

pub(crate) struct GclClients {
    client_senders: Vec<GclClient>,
    idx: usize,
}

impl GclClients {
    // FIXME This will need to be refactored for fake/mock unit testing
    async fn make_client(
        config: &Config,
        token: &Token,
    ) -> std::result::Result<
        LoggingServiceV2Client<InterceptedService<Channel, AuthInterceptor>>,
        Error,
    > {
        let tls_config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(googapis::CERTIFICATES))
            .domain_name("logging.googleapis.com");

        let channel = Channel::from_static("https://logging.googleapis.com")
            .connect_timeout(Duration::from_nanos(config.connect_timeout))
            .tls_config(tls_config)?
            .keep_alive_while_idle(true)
            .tcp_keepalive(Some(Duration::from_secs(10)))
            .http2_keep_alive_interval(Duration::from_secs(10))
            .connect()
            .await?;

        let token_header_value = token.header_value();
        Ok(LoggingServiceV2Client::with_interceptor(
            channel,
            AuthInterceptor {
                token: Box::new(move || match &token_header_value {
                    Ok(val) => Ok(val.clone()),
                    Err(e) => {
                        error!("Failed to get token for Google Logging Client: {}", e);

                        // Fail and bail
                        Err(Status::unavailable(
                            "Failed to retrieve authentication token.",
                        ))
                    }
                }),
            },
        ))
    }

    async fn with_capacity(config: &Config, reply_tx: &Sender<AsyncSinkReply>) -> Result<Self> {
        let mut client_senders = Vec::with_capacity(config.concurrency);
        for i in 0..config.concurrency {
            let (sender, receiver) = bounded(1);
            let token = Token::new()?;
            let config = config.clone();
            let mut client = GclClients::make_client(&config, &token).await?;
            let reply_tx = reply_tx.clone();
            let join_handle = async_std::task::Builder::new()
                .name(format!("gcl_client_task-{}-of-{}", i, config.concurrency))
                .spawn::<_, Result<()>>(async move {
                    loop {
                        match receiver.recv().await {
                            Ok(GclEvent::LogEvent(ref event, entries)) => {
                                let log_entries_response = client
                                    .write_log_entries(WriteLogEntriesRequest {
                                        log_name: config.log_name(None),
                                        resource: super::value_to_monitored_resource(
                                            config.resource.as_ref(),
                                        )?,
                                        labels: config.labels.clone(),
                                        entries,
                                        partial_success: config.partial_success,
                                        dry_run: config.dry_run,
                                    })
                                    .timeout(Duration::from_nanos(config.request_timeout))
                                    .await;

                                if let Err(error) = log_entries_response? {
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
                                        // return Err("Failed to notify about Google Cloud Logging connection loss".into());
                                        if event.transactional {
                                            reply_tx
                                                .send(AsyncSinkReply::Fail(ContraflowData::from(
                                                    event,
                                                )))
                                                .await?;
                                        }
                                        continue;
                                    }
                                    // return Err("Unexpected gRPC failure with Google Cloud Logging endpoint".into());
                                    if event.transactional {
                                        reply_tx
                                            .send(AsyncSinkReply::Fail(ContraflowData::from(event)))
                                            .await?;
                                    }
                                };

                                if event.transactional {
                                    reply_tx
                                        .send(AsyncSinkReply::Ack(
                                            ContraflowData::from(event),
                                            nanotime() - event.ingest_ns,
                                        ))
                                        .await?;
                                }
                            }
                            Ok(GclEvent::Shutdown) => {
                                info!("GCL client task - graceful channel shutdown");
                                return Ok(()); // Because the channel is empty and closed.
                            }
                            Err(e) => {
                                error!("GCL client task - unexpected channel closed: {}", e);
                                return Ok(()); // Because the channel is empty and closed.
                            }
                        }
                    }
                })?;
            client_senders.push(GclClient {
                join_handle,
                token,
                sender,
            });
        }

        Ok(Self {
            client_senders,
            idx: 0,
        })
    }

    fn next(&mut self) -> &mut Sender<GclEvent> {
        let idx = self.idx;
        self.idx = (self.idx + 1) % self.client_senders.len();
        &mut self.client_senders[idx].sender
    }

    #[cfg(test)]
    async fn shutdown(&mut self) -> Result<()> {
        for client in &mut self.client_senders.iter_mut() {
            client.sender.send(GclEvent::Shutdown).await?;
        }
        Ok(())
    }
}

pub(crate) struct GclSink {
    clients: Option<GclClients>,
    config: Config,
    _origin_uri: EventOriginUri,
    reply_tx: Sender<AsyncSinkReply>,
    _response_tx: Sender<SourceReply>,
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
    pub fn new(
        config: Config,
        response_tx: Sender<SourceReply>,
        reply_tx: Sender<AsyncSinkReply>,
    ) -> Self {
        Self {
            clients: None,
            config,
            _response_tx: response_tx,
            reply_tx,
            _origin_uri: EventOriginUri {
                scheme: String::from("gcl_writer"),
                host: String::from("dummy"), // will be replaced in `on_event`
                port: None,
                path: vec![],
            },
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    async fn shutdown(&mut self) -> Result<()> {
        if let Some(clients) = &mut self.clients {
            clients.shutdown().await?;
        }
        Ok(())
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
        if event.is_empty() {
            debug!("{ctx} Received empty event. Won't send it to GCL");
            return Ok(SinkReply::NONE);
        }

        trace!("{ctx} sending event [{}] to Google Cloud Logging", event.id);
        if let Some(clients) = &mut self.clients {
            let client = clients.next();
            client
                .send(GclEvent::log_event(&self.config, event).await?)
                .await?;
        } else {
            error!("{} No GCL client available.", &ctx);
            if event.transactional {
                self.reply_tx
                    .send(AsyncSinkReply::Fail(ContraflowData::from(event)))
                    .await?;
            };
        }

        Ok(SinkReply::NONE)
    }

    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        info!("{} Connecting to Google Cloud Logging", ctx);
        self.clients = Some(GclClients::with_capacity(&self.config, &self.reply_tx).await?);

        Ok(true)
    }

    fn auto_ack(&self) -> bool {
        false
    }
}

/// handle an error for the whole event
///
/// send event to err port
#[allow(dead_code)]
async fn handle_error<E>(
    e: E,
    event: &Event,
    origin_uri: &EventOriginUri,
    response_tx: &Sender<SourceReply>,
) -> Result<()>
where
    E: std::error::Error,
{
    let e_str = e.to_string();
    let mut meta = literal!({
        "gcl_writer": {
            "success": false
        },
        "error": e_str.clone()
    });
    if let Some(correlation) = event.correlation_meta() {
        meta.try_insert("correlation", correlation);
    }

    let data = Value::object_with_capacity(1);
    let event_payload: EventPayload = (data, meta).into();
    let source_reply = SourceReply::Structured {
        origin_uri: origin_uri.clone(),
        payload: event_payload,
        stream: DEFAULT_STREAM_ID,
        port: Some(ERR),
    };
    response_tx.send(source_reply).await?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::connectors::impls::gcl;
    use crate::connectors::tests::ConnectorHarness;
    //    use crate::connectors::ConnectionLostNotifier;
    use googapis::google::logging::r#type::LogSeverity;
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

    #[cfg(see_make_client_comment)]
    #[async_std::test]
    async fn on_event_fails_if_client_is_not_connected() -> Result<()> {
        let (rx, _tx) = async_std::channel::unbounded();
        let config = Config::new(&literal!({
            "connect_timeout": 1_000_000,
            "concurrency": 1,
        }))?;

        let (sr_sender, sr_receiver) = bounded(1);
        let (asr_sender, _asr_receiver) = bounded(1);
        let mut sink = GclSink::new(config, sr_sender, asr_sender);

        let _result = sink
            .on_event(
                "",
                Event::signal_tick(),
                &SinkContext {
                    uid: Default::default(),
                    alias: Alias::new("", ""),
                    connector_type: Default::default(),
                    quiescence_beacon: Default::default(),
                    notifier: ConnectionLostNotifier::new(rx),
                },
                &mut EventSerializer::new(
                    None,
                    CodecReq::Structured,
                    vec![],
                    &ConnectorType::from(""),
                    &Alias::new("", ""),
                )
                .unwrap(),
                0,
            )
            .await?;

        //        assert_eq!(SinkReply::NONE, result);
        // let disposition = asr_receiver.try_recv()?;
        // dbg!(disposition);
        // assert!(disposition.is_fail());
        // assert!(!disposition.is_cb());
        // assert!(!disposition.is_ack());
        let _result = sr_receiver.try_recv()?;
        dbg!(_result);

        sink.shutdown().await?; // signal background gcl client tasks to shutdown
                                //        drop(sink);
        Ok(())
    }

    #[cfg(see_make_client_comment)]
    #[async_std::test]
    async fn on_event_fails_if_write_stream_is_not_connected() -> Result<()> {
        let (rx, _tx) = async_std::channel::unbounded();
        let config = Config::new(&literal!({
            "connect_timeout": 1_000_000,
            "request_timeout": 1_000_000,
            "concurrency": 1,
        }))?;

        let (sr_sender, sr_receiver) = bounded(1);
        let (asr_sender, asr_receiver) = bounded(1);
        let mut sink = GclSink::new(config, sr_sender, asr_sender);

        assert!(!sink.auto_ack());

        let result = sink
            .on_event(
                "",
                Event::signal_tick(),
                &SinkContext {
                    uid: Default::default(),
                    alias: Alias::new("", ""),
                    connector_type: Default::default(),
                    quiescence_beacon: Default::default(),
                    notifier: ConnectionLostNotifier::new(rx),
                },
                &mut EventSerializer::new(
                    None,
                    CodecReq::Structured,
                    vec![],
                    &ConnectorType::from(""),
                    &Alias::new("", ""),
                )
                .unwrap(),
                0,
            )
            .await?;

        assert_eq!(SinkReply::NONE, result);
        let disposition = asr_receiver.recv().await?;
        assert!(disposition.is_fail());
        assert!(!disposition.is_cb());
        assert!(!disposition.is_ack());
        let _result = sr_receiver.recv().await?;

        sink.shutdown().await?; // signal background gcl client tasks to shutdown
        drop(sink);
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
