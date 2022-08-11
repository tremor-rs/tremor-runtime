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

use crate::connectors::impls::gcs::api_client::{
    ApiClient, DefaultApiClient, ExponentialBackoffRetryStrategy, FileId, HttpTaskCommand,
    HttpTaskRequest,
};
use crate::connectors::impls::gcs::chunked_buffer::ChunkedBuffer;
use crate::connectors::prelude::*;
use crate::connectors::sink::{AsyncSinkReply, ContraflowData, Sink};
use crate::system::KillSwitch;
use crate::{connectors, QSIZE};
use async_std::channel::{bounded, Receiver, Sender};
use async_std::prelude::FutureExt;
use http_client::h1::H1Client;
use http_client::HttpClient;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tremor_pipeline::{ConfigImpl, Event, EventId, OpMeta};
use tremor_value::Value;
use value_trait::ValueAccess;

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct Config {
    #[serde(default = "default_endpoint")]
    url: Url<HttpsDefaults>,
    #[serde(default = "default_connect_timeout")]
    connect_timeout: u64,
    #[serde(default = "default_buffer_size")]
    buffer_size: usize,
    bucket: Option<String>,

    #[serde(default = "default_max_retries")]
    max_retries: u32,
    #[serde(default = "default_backoff_base_time")]
    default_backoff_base_time: u64,
}

#[allow(clippy::unwrap_used)]
fn default_endpoint() -> Url<HttpsDefaults> {
    // ALLOW: this URL is hardcoded, so the only reason for parse failing would be if it was changed
    Url::parse("https://storage.googleapis.com/upload/storage/v1").unwrap()
}

fn default_connect_timeout() -> u64 {
    10_000_000_000
}

fn default_buffer_size() -> usize {
    1024 * 1024 * 8 // 8MB - the recommended minimum
}

fn default_max_retries() -> u32 {
    3
}

fn default_backoff_base_time() -> u64 {
    25_000_000
}

impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        ConnectorType("gcs_streamer".into())
    }

    async fn build_cfg(
        &self,
        _alias: &Alias,
        _config: &ConnectorConfig,
        connector_config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(connector_config)?;

        if config.buffer_size % (256 * 1024) != 0 {
            return Err("Buffer size must be a multiple of 256kiB".into());
        }

        Ok(Box::new(GCSWriterConnector { config }))
    }
}

struct GCSWriterConnector {
    config: Config,
}

#[async_trait::async_trait]
impl Connector for GCSWriterConnector {
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let reply_tx = builder.reply_tx();

        let sink = GCSWriterSink::new(self.config.clone(), reply_tx);
        builder.spawn(sink, sink_context).map(Some)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

fn create_client(connect_timeout: Duration) -> Result<H1Client> {
    let mut client = H1Client::new();
    client.set_config(http_client::Config::new().set_timeout(Some(connect_timeout)))?;

    Ok(client)
}

/// msg handled by the `http_task`
enum HttpTaskMsg {
    /// execute the request
    Request(HttpTaskRequest),
    /// stop the task
    Shutdown,
}

async fn http_task(
    command_rx: Receiver<HttpTaskMsg>,
    done_until: Arc<AtomicUsize>,
    reply_tx: Sender<AsyncSinkReply>,
    config: Config,
    mut api_client: impl ApiClient,
) -> Result<()> {
    while let Ok(msg) = command_rx.recv().await {
        match msg {
            HttpTaskMsg::Request(request) => {
                execute_http_call(
                    done_until.clone(),
                    reply_tx.clone(),
                    &config,
                    &mut api_client,
                    request,
                )
                .await?
            }
            HttpTaskMsg::Shutdown => break,
        };
    }

    Ok(())
}

async fn execute_http_call(
    done_until: Arc<AtomicUsize>,
    reply_tx: Sender<AsyncSinkReply>,
    config: &Config,
    api_client: &mut impl ApiClient,
    request: HttpTaskRequest,
) -> Result<()> {
    let send_ack = matches!(request.command, HttpTaskCommand::FinishUpload { .. });
    let result = api_client
        .handle_http_command(done_until.clone(), &config.url, request.command)
        .await;

    match result {
        Ok(_) => {
            if send_ack {
                // we only ack the event upon the finished upload, not before
                // as we cannot be sure yet that it was actually uploaded
                if let Some(contraflow_data) = request.contraflow_data {
                    reply_tx
                        .send(AsyncSinkReply::Ack(
                            contraflow_data,
                            0, // duration of event handling is a little bit meaningless for this sink
                               // the actual duration will be very long due to the accumulating nature of this sink
                               // it will vary a lot when we do an actual upload vs. when we only accumulate
                               // ergo we don't track it here
                        ))
                        .await?;
                }
            }
        }
        Err(e) => {
            warn!("Failed to handle a message: {:?}", e);
            if let Some(contraflow_data) = request.contraflow_data {
                reply_tx.send(AsyncSinkReply::Fail(contraflow_data)).await?;
            }
        }
    }

    Ok(())
}

struct GCSWriterSink {
    client_tx: Option<Sender<HttpTaskMsg>>,
    config: Config,
    buffers: ChunkedBuffer,
    current_name: Option<String>,
    current_bucket: Option<String>,
    /// tracking the ids for all accumulated events
    current_event_id: EventId,
    /// tracking the traversed operators for each accumulated event for correct sink-reply handling
    current_op_meta: OpMeta,
    /// tracking transactional status of events
    current_transactional: bool,
    default_bucket: Option<Value<'static>>,
    done_until: Arc<AtomicUsize>,
    reply_tx: Sender<AsyncSinkReply>,
}

impl GCSWriterSink {
    fn new(config: Config, reply_tx: Sender<AsyncSinkReply>) -> Self {
        let buffers = ChunkedBuffer::new(config.buffer_size);
        let default_bucket = config.bucket.as_ref().cloned().map(Value::from);
        Self {
            client_tx: None,
            config,
            buffers,
            current_name: None,
            current_bucket: None,
            current_event_id: EventId::default(), // dummy - will be replaced with an actual sensible value
            current_op_meta: OpMeta::default(), // dummy - will be replaced with an actual sensible value
            current_transactional: false,
            default_bucket,
            done_until: Arc::new(AtomicUsize::new(0)),
            reply_tx,
        }
    }
}

#[async_trait::async_trait]
impl Sink for GCSWriterSink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        self.buffers
            .mark_done_until(self.done_until.load(Ordering::Acquire))?;
        let mut event_tracked = false;
        for (value, meta) in event.value_meta_iter() {
            let meta = ctx.extract_meta(meta);

            let name = meta
                .get("name")
                .ok_or(ErrorKind::GoogleCloudStorageError(
                    "Metadata is missing the file name",
                ))?
                .as_str()
                .ok_or(ErrorKind::GoogleCloudStorageError(
                    "The file name in metadata is not a string",
                ))?;

            self.finish_upload_if_needed(name, event.ingest_ns).await?;

            if self.needs_new_upload() {
                self.start_upload(meta, name, &event).await?;
            } else if !event_tracked {
                // only track the current event if we didn't do it already and we didn't just start a new upload
                // as in that case we just tracked this event
                //
                // this should only happen upon the first element in the event when we didn't start a new upload
                // we cannot do it once before looping over the event value iter,
                // as then a finish_upload call would ack/fail this event as well, which would be wrong
                self.current_event_id.track(&event.id);
                if !event.op_meta.is_empty() {
                    self.current_op_meta.merge(event.op_meta.clone());
                }
                self.current_transactional |= event.transactional;
                event_tracked = true;
            }

            let serialized_data = serializer.serialize(value, event.ingest_ns)?;
            for item in serialized_data {
                self.buffers.write(&item);
            }

            if let Some(data) = self.buffers.read_current_block() {
                let client_tx = self
                    .client_tx
                    .as_mut()
                    .ok_or(ErrorKind::ClientNotAvailable(
                        "Google Cloud Storage",
                        "not connected",
                    ))?;

                let bucket = get_bucket_name(self.default_bucket.as_ref(), meta)?.to_string();
                self.current_bucket = Some(bucket.clone());

                let command = HttpTaskCommand::UploadData {
                    file: FileId::new(bucket, name),
                    data,
                };
                let contraflow_data = if event.transactional {
                    Some(ContraflowData::from(&event))
                } else {
                    None
                };
                client_tx
                    .send(HttpTaskMsg::Request(HttpTaskRequest {
                        command,
                        contraflow_data,
                    }))
                    .await?;
            }
        }

        Ok(SinkReply::NONE)
    }

    async fn on_stop(&mut self, _ctx: &SinkContext) -> Result<()> {
        // we finish the upload with acking/failing it
        // it would most likely not be handled at this point in the shutdown process
        self.finish_upload(None).await?;
        // shutdown the task
        if let Some(tx) = self.client_tx.take() {
            tx.send(HttpTaskMsg::Shutdown).await?;
        }
        Ok(())
    }

    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let (tx, rx) = bounded(QSIZE.load(Ordering::Relaxed));
        let client = create_client(Duration::from_nanos(self.config.connect_timeout))?;

        let api_client = DefaultApiClient::new(
            client,
            ExponentialBackoffRetryStrategy::new(
                self.config.max_retries,
                Duration::from_nanos(self.config.default_backoff_base_time),
            ),
        )?;

        // stop the previous task
        if let Some(tx) = self.client_tx.take() {
            tx.send(HttpTaskMsg::Shutdown).await?;
        }

        connectors::spawn_task(
            ctx.clone(),
            http_task(
                rx,
                self.done_until.clone(),
                self.reply_tx.clone(),
                self.config.clone(),
                api_client,
            ),
        );

        self.client_tx = Some(tx);

        self.current_name = None;
        self.buffers = ChunkedBuffer::new(self.config.buffer_size);

        Ok(true)
    }

    fn auto_ack(&self) -> bool {
        false // we only ever ack events once their upload has finished
    }
}

impl GCSWriterSink {
    async fn finish_upload_if_needed(&mut self, name: &str, ingest_ns: u64) -> Result<()> {
        if self
            .current_name
            .as_ref()
            .map_or(false, |current_name| current_name.as_str() != name)
        {
            // only send ack/fail if any of the accumulated events was transactional
            let contraflow_data = if self.current_transactional {
                // we have to use the accumulated event ids and op_meta here, so the right events get acked/failed
                Some(ContraflowData::new(
                    self.current_event_id.clone(),
                    ingest_ns,
                    self.current_op_meta.clone(),
                ))
            } else {
                None
            };
            return self.finish_upload(contraflow_data).await;
        }

        Ok(())
    }

    async fn finish_upload(&mut self, contraflow_data: Option<ContraflowData>) -> Result<()> {
        if let Some(current_name) = self.current_name.as_ref() {
            let client_tx = self
                .client_tx
                .as_mut()
                .ok_or(ErrorKind::ClientNotAvailable(
                    "Google Cloud Storage",
                    "not connected",
                ))?;

            let mut buffers = ChunkedBuffer::new(self.config.buffer_size);

            std::mem::swap(&mut self.buffers, &mut buffers);

            let final_data = buffers.final_block();

            let bucket = self
                .current_bucket
                .as_ref()
                .ok_or(ErrorKind::GoogleCloudStorageError(
                    "Current bucket not known",
                ))?;
            let command = HttpTaskCommand::FinishUpload {
                file: FileId::new(bucket, current_name),
                data: final_data,
            };
            client_tx
                .send(HttpTaskMsg::Request(HttpTaskRequest {
                    command,
                    contraflow_data,
                }))
                .await?;

            self.current_name = None;
        }

        Ok(())
    }

    fn needs_new_upload(&self) -> bool {
        self.current_name.is_none()
    }

    /// Starts a new upload
    async fn start_upload(
        &mut self,
        meta: Option<&Value<'_>>,
        name: &str,
        event: &Event,
    ) -> Result<()> {
        let client_tx = self
            .client_tx
            .as_mut()
            .ok_or(ErrorKind::ClientNotAvailable(
                "Google Cloud Storage",
                "not connected",
            ))?;

        let bucket = get_bucket_name(self.default_bucket.as_ref(), meta)?;
        self.current_bucket = Some(bucket.clone());
        // first event of the new buffer, overwrite the `current_event_id` with the current one
        self.current_event_id = event.id.clone();
        // and overwrite the current_op_meta with the one from the current event
        self.current_op_meta = event.op_meta.clone();
        // overwrite the transactional status of the accumulated events
        self.current_transactional = event.transactional;

        let command = HttpTaskCommand::StartUpload {
            file: FileId::new(bucket, name),
        };
        let contraflow_data = if event.transactional {
            Some(ContraflowData::from(event))
        } else {
            None
        };
        client_tx
            .send(HttpTaskMsg::Request(HttpTaskRequest {
                command,
                contraflow_data,
            }))
            .await?;

        self.current_name = Some(name.to_string());

        Ok(())
    }
}

fn get_bucket_name(
    default_bucket: Option<&Value<'static>>,
    meta: Option<&Value>,
) -> Result<String> {
    let bucket = meta
        .get("bucket")
        .or(default_bucket)
        .ok_or(ErrorKind::GoogleCloudStorageError(
            "No bucket name in the metadata",
        ))
        .as_str()
        .ok_or(ErrorKind::GoogleCloudStorageError(
            "Bucket name is not a string",
        ))?
        .to_string();

    Ok(bucket)
}

#[cfg(test)]
mod tests {
    use super::Builder;
    use super::*;
    use crate::config::Codec;
    use crate::connectors::impls::gcs::chunked_buffer::BufferPart;
    use crate::connectors::reconnect::ConnectionLostNotifier;
    use beef::Cow;
    use std::sync::atomic::AtomicBool;
    use tremor_script::{EventPayload, ValueAndMeta};
    use tremor_value::literal;

    #[test]
    pub fn default_endpoint_does_not_panic() {
        // This test will fail if this panics (it should never)
        default_endpoint();
    }

    #[async_std::test]
    pub async fn fails_when_buffer_size_is_not_divisible_by_256ki() {
        let raw_config = literal!({
            "buffer_size": 256 * 1000
        });

        let builder = Builder {};
        let result = builder
            .build_cfg(
                &Alias::new("", ""),
                &ConnectorConfig {
                    connector_type: Default::default(),
                    codec: None,
                    config: None,
                    preprocessors: None,
                    postprocessors: None,
                    reconnect: Default::default(),
                    metrics_interval_s: None,
                },
                &raw_config,
                &KillSwitch::dummy(),
            )
            .await;

        assert!(result.is_err());
    }

    // panic if we don't get one
    fn expect_request(rx: &Receiver<HttpTaskMsg>) -> HttpTaskRequest {
        if let HttpTaskMsg::Request(request) = rx.try_recv().unwrap() {
            Some(request)
        } else {
            None
        }
        .expect("Expected a HttpTaskRequest")
    }

    #[async_std::test]
    pub async fn starts_upload_on_first_event() {
        let (client_tx, client_rx) = bounded(10);
        let (reply_tx, _) = bounded(10);

        let mut sink = GCSWriterSink {
            client_tx: Some(client_tx),
            config: Config {
                url: Default::default(),
                connect_timeout: 1000000000,
                buffer_size: 10,
                bucket: None,
                max_retries: 3,
                default_backoff_base_time: 1,
            },
            buffers: ChunkedBuffer::new(10),
            current_name: None,
            current_bucket: None,
            current_event_id: EventId::default(),
            current_op_meta: OpMeta::default(),
            current_transactional: false,
            default_bucket: None,
            done_until: Arc::new(Default::default()),
            reply_tx,
        };

        let (connection_lost_tx, _) = bounded(10);

        let alias = Alias::new("a", "b");
        let context = SinkContext {
            uid: Default::default(),
            alias: alias.clone(),
            connector_type: "gcs_streamer".into(),
            quiescence_beacon: Default::default(),
            notifier: ConnectionLostNotifier::new(connection_lost_tx),
        };
        let mut serializer = EventSerializer::new(
            Some(Codec::from("json")),
            CodecReq::Required,
            vec![],
            &"gcs_streamer".into(),
            &alias,
        )
        .unwrap();

        let value = literal!({});
        let meta = literal!({
            "gcs_streamer": {
                "name": "test.txt",
                "bucket": "woah"
            }
        });

        let event_payload = EventPayload::from(ValueAndMeta::from_parts(value, meta));

        let event = Event {
            id: Default::default(),
            data: event_payload,
            ingest_ns: 0,
            origin_uri: None,
            kind: None,
            is_batch: false,
            cb: Default::default(),
            op_meta: Default::default(),
            transactional: false,
        };
        sink.on_event("", event.clone(), &context, &mut serializer, 1234)
            .await
            .unwrap();

        let response = expect_request(&client_rx);
        assert_eq!(
            response.command,
            HttpTaskCommand::StartUpload {
                file: FileId::new("woah", "test.txt")
            }
        );
        assert!(response.contraflow_data.is_none()); // event is not transactional
    }

    #[async_std::test]
    pub async fn uploads_data_when_the_buffer_gets_big_enough() {
        let (client_tx, client_rx) = bounded(10);
        let (reply_tx, _) = bounded(10);

        let mut sink = GCSWriterSink {
            client_tx: Some(client_tx),
            config: Config {
                url: Default::default(),
                connect_timeout: 1000000000,
                buffer_size: 10,
                bucket: None,
                max_retries: 3,
                default_backoff_base_time: 1,
            },
            buffers: ChunkedBuffer::new(10),
            current_name: None,
            current_bucket: None,
            current_event_id: EventId::default(),
            current_op_meta: OpMeta::default(),
            current_transactional: false,
            default_bucket: None,
            done_until: Arc::new(Default::default()),
            reply_tx,
        };

        let (connection_lost_tx, _) = bounded(10);

        let alias = Alias::new("a", "b");
        let context = SinkContext {
            uid: Default::default(),
            alias: alias.clone(),
            connector_type: "gcs_streamer".into(),
            quiescence_beacon: Default::default(),
            notifier: ConnectionLostNotifier::new(connection_lost_tx),
        };
        let mut serializer = EventSerializer::new(
            Some(Codec::from("binary")),
            CodecReq::Required,
            vec![],
            &"gcs_streamer".into(),
            &alias,
        )
        .unwrap();

        let value = Value::Bytes(Cow::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        let meta = literal!({
            "gcs_streamer": {
                "name": "test.txt",
                "bucket": "woah"
            }
        });

        let event_payload = EventPayload::from(ValueAndMeta::from_parts(value, meta));

        let event = Event {
            id: Default::default(),
            data: event_payload,
            ingest_ns: 0,
            origin_uri: None,
            kind: None,
            is_batch: false,
            cb: Default::default(),
            op_meta: Default::default(),
            transactional: true,
        };
        sink.on_event("", event.clone(), &context, &mut serializer, 1234)
            .await
            .unwrap();

        // ignore the upload start
        let _ = client_rx.try_recv().unwrap();

        let response = expect_request(&client_rx);

        assert_eq!(
            response.command,
            HttpTaskCommand::UploadData {
                file: FileId::new("woah", "test.txt"),
                data: BufferPart {
                    data: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                    start: 0
                }
            }
        );
        assert!(response.contraflow_data.is_some()); // event is transactional
    }

    #[async_std::test]
    pub async fn finishes_upload_on_filename_change() {
        let (client_tx, client_rx) = bounded(10);
        let (reply_tx, _) = bounded(10);

        let mut sink = GCSWriterSink {
            client_tx: Some(client_tx),
            config: Config {
                url: Default::default(),
                connect_timeout: 1000000000,
                buffer_size: 10,
                bucket: None,
                max_retries: 3,
                default_backoff_base_time: 1,
            },
            buffers: ChunkedBuffer::new(10),
            current_name: None,
            current_bucket: None,
            current_event_id: EventId::default(),
            current_op_meta: OpMeta::default(),
            current_transactional: false,
            default_bucket: None,
            done_until: Arc::new(Default::default()),
            reply_tx,
        };

        let (connection_lost_tx, _) = bounded(10);

        let alias = Alias::new("a", "b");
        let context = SinkContext {
            uid: Default::default(),
            alias: alias.clone(),
            connector_type: "gcs_streamer".into(),
            quiescence_beacon: Default::default(),
            notifier: ConnectionLostNotifier::new(connection_lost_tx),
        };
        let mut serializer = EventSerializer::new(
            Some(Codec::from("json")),
            CodecReq::Required,
            vec![],
            &"gcs_streamer".into(),
            &alias,
        )
        .unwrap();

        let value = literal!({});
        let meta = literal!({
            "gcs_streamer": {
                "name": "test.txt",
                "bucket": "woah"
            }
        });

        let event_payload = EventPayload::from(ValueAndMeta::from_parts(value, meta));

        let event = Event {
            id: EventId::from_id(0, 0, 1),
            data: event_payload,
            ingest_ns: 0,
            origin_uri: None,
            kind: None,
            is_batch: false,
            cb: Default::default(),
            op_meta: Default::default(),
            transactional: false,
        };
        sink.on_event("", event.clone(), &context, &mut serializer, 1234)
            .await
            .unwrap();
        let value = literal!({});
        let meta = literal!({
            "gcs_streamer": {
                "name": "test_other.txt",
                "bucket": "woah"
            }
        });

        let event_payload = EventPayload::from(ValueAndMeta::from_parts(value, meta));

        let event = Event {
            id: EventId::from_id(0, 0, 2),
            data: event_payload,
            ingest_ns: 0,
            origin_uri: None,
            kind: None,
            is_batch: false,
            cb: Default::default(),
            op_meta: Default::default(),
            transactional: true,
        };
        sink.on_event("", event.clone(), &context, &mut serializer, 1234)
            .await
            .unwrap();

        // ignore the first event - upload start
        let _ = client_rx.try_recv().unwrap();

        let response = expect_request(&client_rx);

        assert_eq!(
            response.command,
            HttpTaskCommand::FinishUpload {
                file: FileId::new("woah", "test.txt"),
                data: BufferPart {
                    data: b"{}".to_vec(),
                    start: 0,
                }
            }
        );
        // second event is transactional
        assert!(
            response.contraflow_data.is_none(),
            "Expected no contraflow_data, got {:?}",
            &response.contraflow_data
        );

        let response = expect_request(&client_rx);

        assert_eq!(
            response.command,
            HttpTaskCommand::StartUpload {
                file: FileId::new("woah", "test_other.txt")
            }
        );
        assert!(response.contraflow_data.is_some());
        let cf = response.contraflow_data.expect("something is very off");
        let id = cf.into_ack(0).id;
        assert!(id.is_tracking(&EventId::from_id(0, 0, 2)));
    }

    #[async_std::test]
    pub async fn finishes_upload_on_stop() {
        let (client_tx, client_rx) = bounded(10);
        let (reply_tx, _) = bounded(10);

        let mut sink = GCSWriterSink {
            client_tx: Some(client_tx),
            config: Config {
                url: Default::default(),
                connect_timeout: 1000000000,
                buffer_size: 10,
                bucket: None,
                max_retries: 3,
                default_backoff_base_time: 1,
            },
            buffers: ChunkedBuffer::new(10),
            current_name: None,
            current_bucket: None,
            current_event_id: EventId::default(),
            current_op_meta: OpMeta::default(),
            current_transactional: false,
            default_bucket: None,
            done_until: Arc::new(Default::default()),
            reply_tx,
        };

        let (connection_lost_tx, _) = bounded(10);

        let alias = Alias::new("a", "b");
        let context = SinkContext {
            uid: Default::default(),
            alias: alias.clone(),
            connector_type: "gcs_streamer".into(),
            quiescence_beacon: Default::default(),
            notifier: ConnectionLostNotifier::new(connection_lost_tx),
        };
        let mut serializer = EventSerializer::new(
            Some(Codec::from("json")),
            CodecReq::Required,
            vec![],
            &"gcs_streamer".into(),
            &alias,
        )
        .unwrap();

        let value = literal!({});
        let meta = literal!({
            "gcs_streamer": {
                "name": "test.txt",
                "bucket": "woah"
            }
        });

        let event_payload = EventPayload::from(ValueAndMeta::from_parts(value, meta));

        let event = Event {
            id: Default::default(),
            data: event_payload,
            ingest_ns: 0,
            origin_uri: None,
            kind: None,
            is_batch: false,
            cb: Default::default(),
            op_meta: Default::default(),
            transactional: false,
        };
        sink.on_event("", event.clone(), &context, &mut serializer, 1234)
            .await
            .unwrap();

        sink.on_stop(&context).await.unwrap();

        // ignore the first event - upload start
        let _ = client_rx.try_recv().unwrap();

        let response = expect_request(&client_rx);

        assert_eq!(
            response.command,
            HttpTaskCommand::FinishUpload {
                file: FileId::new("woah", "test.txt"),
                data: BufferPart {
                    data: b"{}".to_vec(),
                    start: 0,
                }
            }
        );
        assert!(response.contraflow_data.is_none());
    }

    struct MockApiClient {
        pub inject_failure: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl ApiClient for MockApiClient {
        async fn handle_http_command(
            &mut self,
            _done_until: Arc<AtomicUsize>,
            _url: &Url<HttpsDefaults>,
            _command: HttpTaskCommand,
        ) -> Result<()> {
            if self.inject_failure.swap(false, Ordering::AcqRel) {
                return Err("injected failure".into());
            }

            Ok(())
        }
    }

    #[async_std::test]
    pub async fn sends_event_reply_after_finish_upload() {
        let (reply_tx, reply_rx) = bounded(10);

        let value = literal!({});
        let meta = literal!({
            "gcs_streamer": {
                "name": "test.txt",
                "bucket": "woah"
            }
        });

        let event_payload = EventPayload::from(ValueAndMeta::from_parts(value, meta));

        let event = Event {
            data: event_payload,
            transactional: true,
            ..Event::default()
        };
        let config = Config {
            url: Default::default(),
            connect_timeout: 100000000,
            buffer_size: 1000,
            bucket: None,
            max_retries: 3,
            default_backoff_base_time: 1,
        };

        let contraflow_data = ContraflowData::from(event);
        execute_http_call(
            Arc::new(Default::default()),
            reply_tx.clone(),
            &config,
            &mut MockApiClient {
                inject_failure: Arc::new(AtomicBool::new(false)),
            },
            HttpTaskRequest {
                command: HttpTaskCommand::StartUpload {
                    file: FileId::new("somebucket", "something.txt"),
                },
                contraflow_data: Some(contraflow_data.clone()),
            },
        )
        .await
        .unwrap();
        // no ack on start upload
        assert!(reply_rx.is_empty());

        execute_http_call(
            Arc::new(Default::default()),
            reply_tx,
            &config,
            &mut MockApiClient {
                inject_failure: Arc::new(AtomicBool::new(false)),
            },
            HttpTaskRequest {
                command: HttpTaskCommand::FinishUpload {
                    file: FileId::new("somebucket", "something.txt"),
                    data: BufferPart {
                        data: vec![],
                        start: 0,
                    },
                },
                contraflow_data: Some(contraflow_data.clone()),
            },
        )
        .await
        .unwrap();

        let reply = reply_rx.recv().await.unwrap();
        if let AsyncSinkReply::Ack(data, duration) = reply {
            assert_eq!(contraflow_data.into_ack(duration), data.into_ack(duration));
        } else {
            panic!("did not receive an ACK");
        }
    }

    #[async_std::test]
    pub async fn http_task_test() {
        let (command_tx, command_rx) = bounded(10);
        let (reply_tx, reply_rx) = bounded(10);
        let done_until = Arc::new(AtomicUsize::new(0));

        async_std::task::spawn(http_task(
            command_rx,
            done_until,
            reply_tx,
            Config {
                url: Default::default(),
                connect_timeout: 10000000,
                buffer_size: 1000,
                bucket: None,
                max_retries: 3,
                default_backoff_base_time: 1,
            },
            MockApiClient {
                inject_failure: Arc::new(AtomicBool::new(true)),
            },
        ));

        let value = literal!({});
        let meta = literal!({
            "gcs_streamer": {
                "name": "test.txt",
                "bucket": "woah"
            }
        });

        let event_payload = EventPayload::from(ValueAndMeta::from_parts(value, meta));

        let event = Event {
            id: Default::default(),
            data: event_payload,
            ingest_ns: 0,
            origin_uri: None,
            kind: None,
            is_batch: false,
            cb: Default::default(),
            op_meta: Default::default(),
            transactional: false,
        };

        let contraflow_data = ContraflowData::from(event);

        command_tx
            .send(HttpTaskMsg::Request(HttpTaskRequest {
                command: HttpTaskCommand::StartUpload {
                    file: FileId::new("somebucket", "something.txt"),
                },
                contraflow_data: Some(contraflow_data.clone()),
            }))
            .await
            .unwrap();

        let reply = reply_rx.recv().await.unwrap();

        if let AsyncSinkReply::Fail(data) = reply {
            assert_eq!(contraflow_data.into_ack(1), data.into_ack(1));
        } else {
            panic!("did not receive an ACK");
        }
    }

    #[async_std::test]
    pub async fn http_task_failure_test() -> Result<()> {
        let (command_tx, command_rx) = bounded(10);
        let (reply_tx, reply_rx) = bounded(10);
        let done_until = Arc::new(AtomicUsize::new(0));

        async_std::task::spawn(http_task(
            command_rx,
            done_until,
            reply_tx,
            Config {
                url: Default::default(),
                connect_timeout: 10000000,
                buffer_size: 1000,
                bucket: None,
                max_retries: 3,
                default_backoff_base_time: 1,
            },
            MockApiClient {
                inject_failure: Arc::new(AtomicBool::new(true)),
            },
        ));

        let value = literal!({});
        let meta = literal!({
            "gcs_streamer": {
                "name": "test.txt",
                "bucket": "woah"
            }
        });

        let event_payload = EventPayload::from(ValueAndMeta::from_parts(value, meta));

        let event = Event {
            id: Default::default(),
            data: event_payload,
            ingest_ns: 0,
            origin_uri: None,
            kind: None,
            is_batch: false,
            cb: Default::default(),
            op_meta: Default::default(),
            transactional: false,
        };

        let contraflow_data = ContraflowData::from(event);

        command_tx
            .send(HttpTaskMsg::Request(HttpTaskRequest {
                command: HttpTaskCommand::StartUpload {
                    file: FileId::new("somebucket", "something.txt"),
                },
                contraflow_data: Some(contraflow_data.clone()),
            }))
            .await
            .unwrap();

        let reply = reply_rx.recv().timeout(Duration::from_secs(5)).await??;

        if let AsyncSinkReply::Fail(data) = reply {
            assert_eq!(contraflow_data.into_fail(), data.into_fail());
        } else {
            panic!("did not receive an ACK");
        }
        Ok(())
    }
}
