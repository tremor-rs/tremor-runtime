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

use crate::connectors::impls::gcs::chunked_buffer::ChunkedBuffer;
use crate::connectors::impls::gcs::resumable_upload_client::{
    DefaultClient, ExponentialBackoffRetryStrategy, FileId, ResumableUploadClient,
};
use crate::connectors::prelude::*;
use crate::connectors::sink::{AsyncSinkReply, ContraflowData, Sink};
use crate::errors::{Error, Kind as ErrorKind};
use crate::system::KillSwitch;
use crate::{connectors, QSIZE};
use async_std::channel::{bounded, Receiver, Sender};
use async_std::prelude::FutureExt;
use http_client::h1::H1Client;
use http_client::HttpClient;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tremor_common::time::nanotime;
use tremor_pipeline::{ConfigImpl, Event, EventId, OpMeta};
use tremor_value::Value;
use value_trait::ValueAccess;

use super::chunked_buffer::BufferPart;

const CONNECTOR_TYPE: &'static str = "gcs_streamer";

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

impl Config {
    fn normalize(&mut self, alias: &Alias) {
        let buffer_size = next_multiple_of(self.buffer_size, 256 * 1024);

        if self.buffer_size < buffer_size {
            warn!(
                "[Connector::{alias}] Rounding buffer_size up to the next multiple of 256KiB: {}KiB.",
                buffer_size / 1024
            );
            self.buffer_size = buffer_size;
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

/// stolen from rust std-lib, where it is still guarded behind an unstable feature #yolo
const fn next_multiple_of(l: usize, multiple: usize) -> usize {
    match l % multiple {
        0 => l,
        r => l + (multiple - r),
    }
}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        ConnectorType(CONNECTOR_TYPE.into())
    }

    async fn build_cfg(
        &self,
        alias: &Alias,
        _config: &ConnectorConfig,
        connector_config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let mut config = Config::new(connector_config)?;
        config.normalize(alias);

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
        let http_client = create_client(Duration::from_nanos(self.config.connect_timeout))?;
        let backoff_strategy = ExponentialBackoffRetryStrategy::new(
            self.config.max_retries,
            Duration::from_nanos(self.config.default_backoff_base_time),
        );
        let upload_client = Box::new(DefaultClient::new(http_client, backoff_strategy)?);
        let sink = GCSWriterSink::new(self.config.clone(), upload_client, reply_tx);
        builder.spawn(sink, sink_context).map(Some)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

#[derive(Debug, Clone)]
struct CurrentUpload {
    // bucket + object_name
    file_id: FileId,
    // session URI as returned in the Location header by the start-upload request.
    // See: https://cloud.google.com/storage/docs/performing-resumable-uploads#initiate-session
    session_uri: url::Url,

    event_id: EventId,
    op_meta: OpMeta,
    transactional: bool,
}

impl CurrentUpload {
    fn new(file_id: FileId, session_uri: url::Url, event: &Event) -> Self {
        Self {
            file_id,
            session_uri,
            event_id: event.id.clone(),
            op_meta: event.op_meta.clone(),
            transactional: event.transactional,
        }
    }

    fn track(&mut self, event: &Event) {
        self.event_id.track(&event.id);
        if !event.op_meta.is_empty() {
            self.op_meta.merge(event.op_meta.clone());
        }
        self.transactional |= event.transactional;
    }
}

struct GCSWriterSink<Client: ResumableUploadClient> {
    config: Config,
    buffers: ChunkedBuffer,
    upload_client: Client,
    current_upload: Option<CurrentUpload>,
    default_bucket: Option<Value<'static>>,
    reply_tx: Sender<AsyncSinkReply>,
}

#[async_trait::async_trait]
impl<Client: ResumableUploadClient + Send> Sink for GCSWriterSink<Client> {
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let mut event_tracked = false;
        for (value, meta) in event.value_meta_iter() {
            let meta = ctx.extract_meta(meta);

            let name = meta
                .get(Self::NAME)
                .ok_or(ErrorKind::GoogleCloudStorageError(format!(
                    "`${CONNECTOR_TYPE}.{}` metadata is missing",
                    Self::NAME
                )))?
                .as_str()
                .ok_or(ErrorKind::GoogleCloudStorageError(format!(
                    "`${CONNECTOR_TYPE}.{}` metadata is not a string",
                    Self::NAME
                )))?;

            let bucket = self.get_bucket_name(meta)?;
            let file_id = FileId::new(bucket, name);

            self.finish_upload_if_needed(&file_id, event.ingest_ns)
                .await?;

            match self.current_upload.as_mut() {
                Some(current_upload) => {
                    if !event_tracked {
                        current_upload.track(&event);
                        event_tracked = true;
                    }
                }
                None => self.start_upload(&file_id).await?,
            }

            let serialized_data = serializer.serialize(value, event.ingest_ns)?;
            for item in serialized_data {
                self.buffers.write(&item);
            }

            while let Some(data) = self.buffers.read_current_block() {
                let done_until = self.upload_data(data).await?;
                self.buffers.mark_done_until(done_until);
            }
        }

        Ok(SinkReply::NONE)
    }

    async fn on_stop(&mut self, ctx: &SinkContext) -> Result<()> {
        self.finish_upload(nanotime(), ctx).await
    }

    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        // clear out the previous upload
        self.current_upload = None;
        self.buffers = ChunkedBuffer::new(self.config.buffer_size);
        Ok(true)
    }

    fn auto_ack(&self) -> bool {
        false
    }
}

impl<Client: ResumableUploadClient> GCSWriterSink<Client> {
    const NAME: &'static str = "name";
    const BUCKET: &'static str = "bucket";

    fn new(config: Config, upload_client: Client, reply_tx: Sender<AsyncSinkReply>) -> Self {
        let buffers = ChunkedBuffer::new(config.buffer_size);
        let default_bucket = config.bucket.as_ref().cloned().map(Value::from);
        Self {
            config,
            buffers,
            upload_client,
            current_upload: None,
            default_bucket,
            reply_tx,
        }
    }

    /// only finish the current upload if the new incoming event has a different `file_id`
    async fn finish_upload_if_needed(
        &mut self,
        new_file_id: &FileId,
        ingest_ns: u64,
    ) -> Result<()> {
        if let Some(CurrentUpload { file_id, .. }) = self.current_upload.as_ref() {
            if file_id != new_file_id {
                self.finish_upload(ingest_ns).await?;
            }
        }
        Ok(())
    }

    /// finish the current upload, ack all accumulated events and clear out the `current_upload`
    async fn finish_upload(&mut self, ingest_ns: u64, ctx: &SinkContext) -> Result<()> {
        if let Some(CurrentUpload {
            file_id,
            session_uri,
            event_id,
            op_meta,
            transactional,
        }) = self.current_upload.take()
        {
            let mut buffers = ChunkedBuffer::new(self.config.buffer_size);

            std::mem::swap(&mut self.buffers, &mut buffers);

            let final_data = buffers.final_block();

            let res = self
                .upload_client
                .finish_upload(&session_uri, final_data)
                .await
                .map_err(|e| {
                    Error::from(ErrorKind::GoogleCloudStorageError(format!(
                        "Error finishing upload for {file_id}: {e}"
                    )))
                });
            // send ack/fail if we have a transactional event
            if transactional {
                let cf_data = ContraflowData::new(event_id, ingest_ns, op_meta);
                let reply = match res {
                    Ok(()) => AsyncSinkReply::Ack(cf_data, 0),
                    Err(_) => AsyncSinkReply::Fail(cf_data),
                };
                ctx.swallow_err(
                    self.reply_tx.send(reply).await,
                    &format!("Error sending ack/fail for upload to {file_id}"),
                );
            }
            res?;
        }

        Ok(())
    }

    /// Starts a new upload
    async fn start_upload(&mut self, file_id: &FileId) -> Result<()> {
        let session_url = self
            .upload_client
            .start_upload(&self.config.url, file_id.clone())
            .await
            .map_err(|e| {
                Error::from(ErrorKind::GoogleCloudStorageError(format!(
                    "Error starting upload for {file_id}: {e}",
                )))
            })?;
        self.current_upload = Some(CurrentUpload::new(file_id.clone(), session_url));
        Ok(())
    }

    async fn upload_data(&mut self, data: BufferPart) -> Result<usize> {
        let CurrentUpload {
            session_uri,
            file_id,
        } = self
            .current_upload
            .as_ref()
            .ok_or(Error::from(ErrorKind::GoogleCloudStorageError(
                "Invalid state, no current upload running.".to_string(),
            )))?;
        self.upload_client
            .upload_data(session_uri, data)
            .await
            .map_err(|e| {
                Error::from(ErrorKind::GoogleCloudStorageError(format!(
                    "Error uploading data for {file_id}: {e}"
                )))
            })
    }

    fn get_bucket_name(&self, meta: Option<&Value>) -> Result<String> {
        let bucket = meta
            .get(Self::BUCKET)
            .or(self.default_bucket.as_ref())
            .ok_or(ErrorKind::GoogleCloudStorageError(format!(
                "Metadata `${CONNECTOR_TYPE}.{}` missing",
                Self::BUCKET
            )))
            .as_str()
            .ok_or(ErrorKind::GoogleCloudStorageError(format!(
                "Metadata `${CONNECTOR_TYPE}.{}` is not a string.",
                Self::BUCKET
            )))?
            .to_string();

        Ok(bucket)
    }
}

fn create_client(connect_timeout: Duration) -> Result<H1Client> {
    let mut client = H1Client::new();
    client.set_config(
        http_client::Config::new()
            .set_timeout(Some(connect_timeout))
            .set_http_keep_alive(true),
    )?;

    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Codec;
    use crate::connectors::impls::gcs::chunked_buffer::BufferPart;
    use crate::connectors::reconnect::ConnectionLostNotifier;
    use async_std::{prelude::FutureExt, task};
    use beef::Cow;
    use std::sync::atomic::AtomicBool;
    use tremor_script::{EventPayload, ValueAndMeta};
    use tremor_value::literal;

    #[test]
    pub fn default_endpoint_does_not_panic() {
        // This test will fail if this panics (it should never)
        default_endpoint();
    }

    #[test]
    pub fn adapts_when_buffer_size_is_not_divisible_by_256ki() {
        let raw_config = literal!({
            "buffer_size": 256 * 1000
        });

        let mut config = Config::new(&raw_config).expect("config should be valid");
        let alias = Alias::new("flow", "conn");
        config.normalize(&alias);
        assert_eq!(256 * 1024, config.buffer_size);
    }

    // panic if we don't get one
    fn expect_request(rx: &Receiver<HttpTaskMsg>) -> HttpTaskRequest {
        if let HttpTaskMsg::Request(request) = rx.try_recv().unwrap() {
            Some(dbg!(request))
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
                cancel_upload_on_failure: true,
            },
            buffers: ChunkedBuffer::new(10),
            current_upload: None,
            default_bucket: None,
            done_until: Arc::default(),
            upload_failed: Arc::default(),
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
                cancel_upload_on_failure: true,
            },
            buffers: ChunkedBuffer::new(10),
            current_upload: None,
            default_bucket: None,
            done_until: Arc::new(Default::default()),
            upload_failed: Arc::default(),
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
                cancel_upload_on_failure: true,
            },
            buffers: ChunkedBuffer::new(10),
            current_upload: None,
            default_bucket: None,
            done_until: Arc::new(Default::default()),
            upload_failed: Arc::default(),
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

        let event_id = EventId::from_id(0, 0, 1);
        let event = Event {
            id: event_id,
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

        let event_id1 = EventId::from_id(0, 0, 2);
        let event = Event {
            id: event_id1.clone(),
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

        let start_upload = expect_request(&client_rx);

        assert_eq!(
            start_upload.command,
            HttpTaskCommand::StartUpload {
                file: FileId::new("woah", "test.txt")
            }
        );
        assert!(start_upload.contraflow_data.is_none());

        let finish_upload = expect_request(&client_rx);

        assert_eq!(
            finish_upload.command,
            HttpTaskCommand::FinishUpload {
                file: FileId::new("woah", "test.txt"),
                data: BufferPart {
                    data: b"{}".to_vec(),
                    start: 0,
                }
            }
        );
        assert!(
            finish_upload.contraflow_data.is_none(),
            "Expected no contraflow_data, got {:?}",
            &finish_upload.contraflow_data
        );

        // second event is transactional
        let start_upload2 = expect_request(&client_rx);

        assert_eq!(
            start_upload2.command,
            HttpTaskCommand::StartUpload {
                file: FileId::new("woah", "test_other.txt")
            }
        );
        assert!(start_upload2.contraflow_data.is_some());
        let cf = start_upload2
            .contraflow_data
            .expect("new upload to be transactional");
        let id = cf.into_ack(0).id;
        assert!(id.is_tracking(&event_id1));

        // send another event with the same bucket and name
        let event_id2 = EventId::from_id(0, 0, 3);
        let event_payload2 = EventPayload::from(ValueAndMeta::from_parts(
            Value::from("01234567890"),
            literal!({
                "gcs_streamer": {
                    "name": "test_other.txt",
                    "bucket": "woah"
                }
            }),
        ));
        let event2 = Event {
            id: event_id2.clone(),
            data: event_payload2,
            ingest_ns: 1,
            origin_uri: None,
            kind: None,
            is_batch: false,
            cb: Default::default(),
            op_meta: Default::default(),
            transactional: true,
        };
        sink.on_event("", event2.clone(), &context, &mut serializer, 12345)
            .await
            .unwrap();

        // another event with differing bucket
        let event_id3 = EventId::from_id(0, 0, 4);
        let event_payload3 = EventPayload::from(ValueAndMeta::from_parts(
            literal!(["snot", "badger", "upload_me_pls"]),
            literal!({
                "gcs_streamer": {
                    "name": "test_other.txt",
                    "bucket": "woah_other"
                }
            }),
        ));
        let event3 = Event {
            id: event_id3.clone(),
            data: event_payload3,
            ingest_ns: 3,
            origin_uri: None,
            kind: None,
            is_batch: false,
            cb: Default::default(),
            op_meta: Default::default(),
            transactional: true,
        };
        sink.on_event("", event3.clone(), &context, &mut serializer, 12345)
            .await
            .unwrap();
        // we exceeded chunk_size, so we do an upload
        let upload_data3 = expect_request(&client_rx);
        assert_eq!(
            upload_data3.command,
            HttpTaskCommand::UploadData {
                file: FileId::new("woah", "test_other.txt"),
                data: BufferPart {
                    data: b"{}\"0123456".to_vec(),
                    start: 0,
                }
            }
        );
        // we have contraflow_data, but we wouldn't ack this yet, only used for failing
        assert!(upload_data3.contraflow_data.is_some());
        let cf = upload_data3
            .contraflow_data
            .expect("new upload to be transactional");
        let id = cf.into_ack(0).id;
        assert!(id.is_tracking(&event_id1));
        assert!(id.is_tracking(&event_id2));

        // we finish with the rest of the data
        let finish_upload3 = expect_request(&client_rx);
        assert_eq!(
            finish_upload3.command,
            HttpTaskCommand::FinishUpload {
                file: FileId::new("woah", "test_other.txt"),
                data: BufferPart {
                    data: b"7890\"".to_vec(),
                    start: 0
                }
            }
        );
        assert!(finish_upload3.contraflow_data.is_some());
        let cf = finish_upload3
            .contraflow_data
            .expect("new upload to be transactional");
        let id = cf.into_ack(0).id;
        assert!(id.is_tracking(&event_id1));
        assert!(id.is_tracking(&event_id2));

        let start_upload3 = expect_request(&client_rx);
        assert_eq!(
            start_upload3.command,
            HttpTaskCommand::StartUpload {
                file: FileId::new("woah_other", "test_other.txt")
            }
        );
        assert!(start_upload3.contraflow_data.is_some());

        let upload_data4 = expect_request(&client_rx);
        assert_eq!(
            upload_data4.command,
            HttpTaskCommand::UploadData {
                file: FileId::new("woah_other", "test_other.txt"),
                data: BufferPart {
                    data: b"[\"snot\",\"badger\",\"upload_me_pls\"]".to_vec(),
                    start: 0
                }
            }
        )
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
                cancel_upload_on_failure: true,
            },
            buffers: ChunkedBuffer::new(10),
            current_upload: None,
            default_bucket: None,
            done_until: Arc::new(Default::default()),
            upload_failed: Arc::default(),
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
        let handle = task::spawn::<_, Result<()>>(async move {
            let mut count = 0_usize;
            loop {
                match client_rx.recv().await {
                    Ok(HttpTaskMsg::Request(req)) => {
                        if count == 0 {
                            assert_eq!(
                                req.command,
                                HttpTaskCommand::StartUpload {
                                    file: FileId::new("woah", "test.txt"),
                                }
                            );
                            assert!(req.contraflow_data.is_none());
                        } else {
                            assert_eq!(
                                req.command,
                                HttpTaskCommand::FinishUpload {
                                    file: FileId::new("woah", "test.txt"),
                                    data: BufferPart {
                                        data: b"{}".to_vec(),
                                        start: 0,
                                    }
                                }
                            );
                            assert!(req.contraflow_data.is_none());
                        }
                        count += 1;
                    }
                    Ok(HttpTaskMsg::Shutdown(tx)) => {
                        assert_eq!(2, count);
                        tx.send(()).await?;
                        break;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Ok(())
        });
        sink.on_stop(&context)
            .await
            .expect("Expected on_stop to work, lol");
        assert_eq!(Ok(()), handle.await);
    }

    struct MockApiClient {
        pub inject_failure: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl ApiClient for MockApiClient {
        async fn handle_http_command(
            &mut self,
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
            cancel_upload_on_failure: true,
        };

        let contraflow_data = ContraflowData::from(event);
        execute_http_call(
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

        async_std::task::spawn(http_task(
            command_rx,
            reply_tx,
            Config {
                url: Default::default(),
                connect_timeout: 10000000,
                buffer_size: 1000,
                bucket: None,
                max_retries: 3,
                default_backoff_base_time: 1,
                cancel_upload_on_failure: true,
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

        async_std::task::spawn(http_task(
            command_rx,
            reply_tx,
            Config {
                url: Default::default(),
                connect_timeout: 10000000,
                buffer_size: 1000,
                bucket: None,
                max_retries: 3,
                default_backoff_base_time: 1,
                cancel_upload_on_failure: true,
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
