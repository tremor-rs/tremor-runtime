// Copyright 2020-2022, The Tremor Team
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

use super::Error;
use crate::{
    impls::gcs::{
        chunked_buffer::ChunkedBuffer,
        resumable_upload_client::{
            create_client, DefaultClient, ExponentialBackoffRetryStrategy, GcsHttpClient,
            ResumableUploadClient,
        },
    },
    prelude::*,
    utils::{
        google::TokenSrc,
        object_storage::{
            BufferPart, ConsistentSink, Mode, ObjectId, ObjectStorageCommon, ObjectStorageSinkImpl,
            ObjectStorageUpload, YoloSink,
        },
    },
};
use std::time::Duration;
use tremor_common::time::nanotime;
use tremor_value::Value;

const CONNECTOR_TYPE: &str = "gcs_streamer";

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    /// endpoint url
    #[serde(default = "default_endpoint")]
    pub(super) url: Url<HttpsDefaults>,
    /// Optional default bucket
    pub(super) bucket: Option<String>,
    /// Mode of operation for this sink
    #[serde(default)]
    pub(super) mode: Mode,
    pub(super) token: TokenSrc,
    #[serde(default = "default_connect_timeout")]
    pub(super) connect_timeout: u64,
    #[serde(default = "default_buffer_size")]
    pub(super) buffer_size: usize,
    #[serde(default = "default_max_retries")]
    pub(super) max_retries: u32,
    #[serde(default = "default_backoff_base_time")]
    pub(super) backoff_base_time: u64,
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

impl tremor_config::Impl for Config {}

impl Config {
    fn normalize(&mut self, alias: &alias::Connector) {
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
        alias: &alias::Connector,
        _config: &ConnectorConfig,
        connector_config: &Value,
        _kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
        let mut config = Config::new(connector_config)?;
        config.normalize(alias);

        Ok(Box::new(GCSStreamerConnector { config }))
    }
}

struct GCSStreamerConnector {
    config: Config,
}

#[async_trait::async_trait]
impl Connector for GCSStreamerConnector {
    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> anyhow::Result<Option<SinkAddr>> {
        let token = self.config.token.clone();
        let client_factory = Box::new(move |config: &Config| {
            let http_client = create_client(Duration::from_nanos(config.connect_timeout));
            let backoff_strategy = ExponentialBackoffRetryStrategy::new(
                config.max_retries,
                Duration::from_nanos(config.backoff_base_time),
            );
            DefaultClient::new(http_client, backoff_strategy, &token)
        });

        info!(
            "{ctx} Starting {CONNECTOR_TYPE} in {} mode",
            &self.config.mode
        );
        match self.config.mode {
            Mode::Yolo => {
                let sink_impl = GCSObjectStorageSinkImpl::yolo(self.config.clone(), client_factory);
                let sink: YoloSink<
                    GCSObjectStorageSinkImpl<
                        DefaultClient<GcsHttpClient, ExponentialBackoffRetryStrategy>,
                    >,
                    GCSUpload,
                    ChunkedBuffer,
                > = YoloSink::new(sink_impl);
                Ok(Some(builder.spawn(sink, ctx)))
            }
            Mode::Consistent => {
                let sink_impl = GCSObjectStorageSinkImpl::consistent(
                    self.config.clone(),
                    client_factory,
                    builder.reply_tx(),
                );
                let sink: ConsistentSink<
                    GCSObjectStorageSinkImpl<
                        DefaultClient<GcsHttpClient, ExponentialBackoffRetryStrategy>,
                    >,
                    GCSUpload,
                    ChunkedBuffer,
                > = ConsistentSink::new(sink_impl);
                Ok(Some(builder.spawn(sink, ctx)))
            }
        }
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

struct GCSUpload {
    /// upload identifiers
    object_id: ObjectId,
    session_uri: url::Url,
    /// event tracking
    event_id: EventId,
    op_meta: OpMeta,
    transactional: bool,
    /// current state
    failed: bool,
}

impl GCSUpload {
    fn new(object_id: ObjectId, session_uri: url::Url, event: &Event) -> Self {
        Self {
            object_id,
            session_uri,
            event_id: event.id.clone(),
            op_meta: event.op_meta.clone(),
            transactional: event.transactional,
            failed: false,
        }
    }
}

impl ObjectStorageUpload for GCSUpload {
    fn object_id(&self) -> &ObjectId {
        &self.object_id
    }
    fn is_failed(&self) -> bool {
        self.failed
    }
    fn mark_as_failed(&mut self) {
        self.failed = true;
    }
    fn track(&mut self, event: &Event) {
        self.event_id.track(&event.id);
        if !event.op_meta.is_empty() {
            self.op_meta.merge(event.op_meta.clone());
        }
        self.transactional |= event.transactional;
    }
}

type UploadClientFactory<Client> = Box<dyn Fn(&Config) -> anyhow::Result<Client> + Send + Sync>;

struct GCSObjectStorageSinkImpl<Client: ResumableUploadClient> {
    config: Config,
    upload_client: Option<Client>,
    upload_client_factory: UploadClientFactory<Client>,
    reply_tx: Option<ReplySender>,
}

impl<Client: ResumableUploadClient + Send> GCSObjectStorageSinkImpl<Client> {
    fn yolo(config: Config, upload_client_factory: UploadClientFactory<Client>) -> Self {
        Self {
            config,
            upload_client: None,
            upload_client_factory,
            reply_tx: None,
        }
    }
    fn consistent(
        config: Config,
        upload_client_factory: UploadClientFactory<Client>,
        reply_tx: ReplySender,
    ) -> Self {
        Self {
            config,
            upload_client: None,
            upload_client_factory,
            reply_tx: Some(reply_tx),
        }
    }
}

impl<Client: ResumableUploadClient> ObjectStorageCommon for GCSObjectStorageSinkImpl<Client> {
    fn connector_type(&self) -> &str {
        CONNECTOR_TYPE
    }

    fn default_bucket(&self) -> Option<&String> {
        self.config.bucket.as_ref()
    }
}

#[async_trait::async_trait]
impl<Client: ResumableUploadClient + Send + Sync> ObjectStorageSinkImpl<GCSUpload>
    for GCSObjectStorageSinkImpl<Client>
{
    fn buffer_size(&self) -> usize {
        self.config.buffer_size
    }
    async fn connect(&mut self, _ctx: &SinkContext) -> anyhow::Result<()> {
        self.upload_client = Some((self.upload_client_factory)(&self.config)?);
        Ok(())
    }
    async fn bucket_exists(&mut self, bucket: &str) -> anyhow::Result<bool> {
        let client = self.upload_client.as_mut().ok_or(Error::NoClient)?;
        client.bucket_exists(&self.config.url, bucket).await
    }
    async fn start_upload(
        &mut self,
        object_id: &ObjectId,
        event: &Event,
        _ctx: &SinkContext,
    ) -> anyhow::Result<GCSUpload> {
        let client = self.upload_client.as_mut().ok_or(Error::NoClient)?;
        let session_uri = client
            .start_upload(&self.config.url, object_id.clone())
            .await?;
        let upload = GCSUpload::new(object_id.clone(), session_uri, event);
        Ok(upload)
    }

    async fn upload_data(
        &mut self,
        data: BufferPart,
        upload: &mut GCSUpload,
        ctx: &SinkContext,
    ) -> anyhow::Result<usize> {
        let client = self.upload_client.as_mut().ok_or(Error::NoClient)?;
        debug!(
            "{ctx} Uploading bytes {}-{} for {}",
            data.start(),
            data.end(),
            upload.object_id()
        );
        client.upload_data(&upload.session_uri, data).await
    }
    async fn finish_upload(
        &mut self,
        upload: GCSUpload,
        part: BufferPart,
        ctx: &SinkContext,
    ) -> anyhow::Result<()> {
        debug_assert!(
            !upload.failed,
            "finish may only be called for non-failed uploads"
        );
        let client = self.upload_client.as_mut().ok_or(Error::NoClient)?;

        let GCSUpload {
            object_id,
            session_uri,
            event_id,
            op_meta,
            transactional,
            ..
        } = upload;
        debug!("{ctx} Finishing upload for {}", object_id);
        let res = client.finish_upload(&session_uri, part).await;
        // only ack here if we have reply_tx and the upload is transactional
        if let (Some(reply_tx), true) = (self.reply_tx.as_ref(), transactional) {
            let cf_data = ContraflowData::new(event_id, nanotime(), op_meta);
            let reply = if res.is_ok() {
                AsyncSinkReply::Ack(cf_data, 0)
            } else {
                AsyncSinkReply::Fail(cf_data)
            };
            ctx.swallow_err(
                reply_tx.send(reply),
                &format!("Error sending ack/fail for upload to {object_id}"),
            );
        }
        res
    }
    async fn fail_upload(&mut self, upload: GCSUpload, ctx: &SinkContext) -> anyhow::Result<()> {
        let GCSUpload {
            object_id,
            session_uri,
            event_id,
            op_meta,
            transactional,
            ..
        } = upload;
        let client = self.upload_client.as_mut().ok_or(Error::NoClient)?;
        if let (Some(reply_tx), true) = (self.reply_tx.as_ref(), transactional) {
            ctx.swallow_err(
                reply_tx.send(AsyncSinkReply::Fail(ContraflowData::new(
                    event_id,
                    nanotime(),
                    op_meta,
                ))),
                &format!("Error sending fail for upload for {object_id}"),
            );
        }
        ctx.swallow_err(
            client.delete_upload(&session_uri).await,
            &format!("Error deleting upload for {object_id}"),
        );
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        channel::{bounded, unbounded},
        config::Reconnect,
        impls::gcs::{resumable_upload_client::ResumableUploadClient, streamer::Mode},
        reconnect::ConnectionLostNotifier,
        utils::{
            object_storage::{BufferPart, ObjectId},
            quiescence::QuiescenceBeacon,
        },
    };
    use halfbrown::HashMap;
    use http::StatusCode;
    use std::sync::atomic::AtomicUsize;
    use tremor_common::ids::{ConnectorIdGen, SinkId};
    use tremor_system::event::EventId;
    use tremor_value::literal;

    #[derive(Debug, Default)]
    pub(crate) struct TestUploadClient {
        counter: AtomicUsize,
        running: HashMap<url::Url, (ObjectId, Vec<BufferPart>)>,
        finished: HashMap<url::Url, (ObjectId, Vec<BufferPart>)>,
        deleted: HashMap<url::Url, (ObjectId, Vec<BufferPart>)>,
        inject_failure: bool,
    }

    impl TestUploadClient {
        pub(crate) fn running_uploads(&self) -> Vec<(ObjectId, Vec<BufferPart>)> {
            self.running.values().cloned().collect()
        }

        pub(crate) fn finished_uploads(&self) -> Vec<(ObjectId, Vec<BufferPart>)> {
            self.finished.values().cloned().collect()
        }

        pub(crate) fn deleted_uploads(&self) -> Vec<(ObjectId, Vec<BufferPart>)> {
            self.deleted.values().cloned().collect()
        }

        pub(crate) fn count(&self) -> usize {
            self.counter.load(Ordering::Acquire)
        }

        pub(crate) fn inject_failure(&mut self, inject_failure: bool) {
            self.inject_failure = inject_failure;
        }
    }

    #[async_trait::async_trait]
    impl ResumableUploadClient for TestUploadClient {
        async fn start_upload(
            &mut self,
            url: &Url<HttpsDefaults>,
            file_id: ObjectId,
        ) -> anyhow::Result<url::Url> {
            if self.inject_failure {
                return Err(Error::Upload(StatusCode::INTERNAL_SERVER_ERROR).into());
            }
            let count = self.counter.fetch_add(1, Ordering::AcqRel);
            let mut session_uri = url.url().clone();
            session_uri.set_path(format!("{}/{count}", url.path()).as_str());
            self.running.insert(session_uri.clone(), (file_id, vec![]));
            Ok(session_uri)
        }
        async fn upload_data(&mut self, url: &url::Url, part: BufferPart) -> anyhow::Result<usize> {
            if self.inject_failure {
                return Err(Error::Upload(StatusCode::INTERNAL_SERVER_ERROR).into());
            }
            if let Some((_file_id, buffers)) = self.running.get_mut(url) {
                let end = part.start + part.len();
                buffers.push(part);
                // TODO: create mode where it always keeps some bytes
                Ok(end)
            } else {
                Err(crate::utils::object_storage::Error::NoUpload.into())
            }
        }
        async fn finish_upload(&mut self, url: &url::Url, part: BufferPart) -> anyhow::Result<()> {
            if self.inject_failure {
                return Err(Error::Upload(StatusCode::INTERNAL_SERVER_ERROR).into());
            }
            if let Some((file_id, mut buffers)) = self.running.remove(url) {
                buffers.push(part);
                self.finished.insert(url.clone(), (file_id, buffers));
                Ok(())
            } else {
                Err(crate::utils::object_storage::Error::NoUpload.into())
            }
        }
        async fn delete_upload(&mut self, url: &url::Url) -> anyhow::Result<()> {
            // we don't fail on delete, as we want to see the effect of deletion in our tests
            if let Some(upload) = self.running.remove(url) {
                self.deleted.insert(url.clone(), upload);
                Ok(())
            } else {
                Err(crate::utils::object_storage::Error::NoUpload.into())
            }
        }
        async fn bucket_exists(
            &mut self,
            _url: &Url<HttpsDefaults>,
            bucket: &str,
        ) -> anyhow::Result<bool> {
            if self.inject_failure {
                return Err(
                    Error::Bucket(bucket.to_string(), StatusCode::INTERNAL_SERVER_ERROR).into(),
                );
            }
            Ok(true)
        }
    }

    #[test]
    pub fn default_endpoint_does_not_panic() {
        // This test will fail if this panics (it should never)
        default_endpoint();
    }

    #[test]
    pub fn config_adapts_when_buffer_size_is_not_divisible_by_256ki() {
        let raw_config = literal!({
            "token": {"file": file!().to_string()},
            "mode": "yolo",
            "buffer_size": 256 * 1000
        });

        let mut config = Config::new(&raw_config).expect("config should be valid");
        let alias = alias::Connector::new("flow", "conn");
        config.normalize(&alias);
        assert_eq!(256 * 1024, config.buffer_size);
    }

    fn upload_client(
        sink: &mut YoloSink<GCSObjectStorageSinkImpl<TestUploadClient>, GCSUpload, ChunkedBuffer>,
    ) -> &mut TestUploadClient {
        sink.sink_impl
            .upload_client
            .as_mut()
            .expect("No upload client available")
    }

    #[allow(clippy::too_many_lines)] // this is a test
    #[tokio::test(flavor = "multi_thread")]
    async fn yolo_happy_path() -> anyhow::Result<()> {
        let upload_client_factory = Box::new(|_config: &Config| Ok(TestUploadClient::default()));

        let config = Config {
            url: Url::default(),
            bucket: Some("bucket".to_string()),
            mode: Mode::Yolo,
            connect_timeout: 1_000_000_000,
            buffer_size: 10,
            max_retries: 3,
            backoff_base_time: 1,
            token: TokenSrc::dummy(),
        };

        let sink_impl = GCSObjectStorageSinkImpl::yolo(config, upload_client_factory);

        let mut sink: YoloSink<
            GCSObjectStorageSinkImpl<TestUploadClient>,
            GCSUpload,
            ChunkedBuffer,
        > = YoloSink::new(sink_impl);

        let (connection_lost_tx, _) = bounded(10);

        let alias = alias::Connector::new("a", "b");
        let context = SinkContext::new(
            SinkId::default(),
            alias.clone(),
            "gcs_streamer".into(),
            QuiescenceBeacon::default(),
            ConnectionLostNotifier::new(&alias, connection_lost_tx),
        );
        let mut serializer = EventSerializer::new(
            Some(tremor_codec::Config::from("json")),
            CodecReq::Required,
            vec![],
            &"gcs_streamer".into(),
            &alias,
        )?;

        // simulate sink lifecycle
        sink.on_start(&context).await?;
        sink.connect(&context, &Attempt::default()).await?;

        let id1 = EventId::from_id(1, 1, 1);
        let event = Event {
            id: id1.clone(),
            data: (
                literal!({
                    "snot": ["badg", "er"]
                }),
                literal!({
                    "gcs_streamer": {
                        "name": "happy.txt",
                        "bucket": "yolo"
                    }
                }),
            )
                .into(),
            transactional: true,
            ..Event::default()
        };
        let reply = sink
            .on_event("", event, &context, &mut serializer, 0)
            .await?;
        assert_eq!(SinkReply::ACK, reply);
        assert_eq!(1, upload_client(&mut sink).count());
        let mut uploads = upload_client(&mut sink).running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, mut buffers) = uploads.pop().expect("no data");
        assert_eq!(ObjectId::new("yolo", "happy.txt"), file_id);
        assert_eq!(2, buffers.len());
        let buffer = buffers.pop().expect("no data");
        assert_eq!(10, buffer.start);
        assert_eq!(10, buffer.len());
        assert_eq!("badg\",\"er\"".as_bytes(), &buffer.data);

        let buffer = buffers.pop().expect("no data");
        assert_eq!(0, buffer.start);
        assert_eq!(10, buffer.len());
        assert_eq!("{\"snot\":[\"".as_bytes(), &buffer.data);

        assert!(upload_client(&mut sink).finished_uploads().is_empty());
        assert!(upload_client(&mut sink).deleted_uploads().is_empty());

        // batched event with intermitten new upload
        let id2 = EventId::from_id(1, 1, 2);
        let event = Event {
            id: id2.clone(),
            data: (
                literal!([
                {
                    "data": {
                        "value": "snot",
                        "meta": {
                            "gcs_streamer": {
                                "name": "happy.txt",
                                "bucket": "yolo"
                            }
                        }
                    }
                },
                {
                    "data": {
                        "value": "badger",
                        "meta": {
                            "gcs_streamer": {
                                "name": "happy.txt",
                                "bucket": "yolo"
                            }
                        }
                    }
                },
                {
                    "data": {
                        "value": [1,2,3,true,false,null],
                        "meta": {
                            "gcs_streamer": {
                                "name": "sad.txt",
                                "bucket": "yolo"
                            }
                        }
                    }
                }
                ]),
                literal!({}),
            )
                .into(),
            transactional: true,
            is_batch: true,
            ..Event::default()
        };
        let reply = sink
            .on_event("", event, &context, &mut serializer, 0)
            .await?;
        assert_eq!(SinkReply::ACK, reply);

        assert_eq!(2, upload_client(&mut sink).count());
        let mut uploads = upload_client(&mut sink).running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, mut buffers) = uploads.pop().expect("no data");
        assert_eq!(ObjectId::new("yolo", "sad.txt"), file_id);
        assert_eq!(2, buffers.len());

        let buffer = buffers.pop().expect("no data");
        assert_eq!(10, buffer.start);
        assert_eq!(10, buffer.len());
        assert_eq!("e,false,nu".as_bytes(), &buffer.data);

        let buffer = buffers.pop().expect("no data");
        assert_eq!(0, buffer.start);
        assert_eq!(10, buffer.len());
        assert_eq!("[1,2,3,tru".as_bytes(), &buffer.data);

        let mut finished = upload_client(&mut sink).finished_uploads();
        assert_eq!(1, finished.len());
        let (file_id, mut buffers) = finished.pop().expect("no data");
        assert_eq!(ObjectId::new("yolo", "happy.txt"), file_id);
        assert_eq!(4, buffers.len());
        let buffer = buffers.pop().expect("no data");
        assert_eq!(30, buffer.start);
        assert_eq!("adger\"".as_bytes(), &buffer.data);

        let buffer = buffers.pop().expect("no data");
        assert_eq!(20, buffer.start);
        assert_eq!("]}\"snot\"\"b".as_bytes(), &buffer.data);

        assert!(upload_client(&mut sink).deleted_uploads().is_empty());

        sink.on_stop(&context).await?;

        // we finish outstanding upload upon stop
        let mut finished = upload_client(&mut sink).finished_uploads();
        assert_eq!(2, finished.len());
        let (file_id, mut buffers) = finished.pop().expect("no data");
        assert_eq!(ObjectId::new("yolo", "sad.txt"), file_id);
        assert_eq!(3, buffers.len());
        let last = buffers.pop().expect("no data");
        assert_eq!(20, last.start);
        assert_eq!("ll]".as_bytes(), &last.data);

        Ok(())
    }

    #[allow(clippy::too_many_lines)] // this is a test
    #[tokio::test(flavor = "multi_thread")]
    async fn yolo_on_failure() -> anyhow::Result<()> {
        // ensure that failures on calls to google don't fail events
        let upload_client_factory = Box::new(|_config: &Config| Ok(TestUploadClient::default()));
        let config = Config {
            url: Url::default(),
            bucket: None,
            mode: Mode::Yolo,
            connect_timeout: 1_000_000_000,
            buffer_size: 10,
            max_retries: 3,
            backoff_base_time: 1,
            token: TokenSrc::dummy(),
        };

        let sink_impl = GCSObjectStorageSinkImpl::yolo(config, upload_client_factory);
        let mut sink: YoloSink<
            GCSObjectStorageSinkImpl<TestUploadClient>,
            GCSUpload,
            ChunkedBuffer,
        > = YoloSink::new(sink_impl);

        let (connection_lost_tx, _) = bounded(10);

        let alias = alias::Connector::new("a", "b");
        let context = SinkContext::new(
            SinkId::default(),
            alias.clone(),
            "gcs_streamer".into(),
            QuiescenceBeacon::default(),
            ConnectionLostNotifier::new(&alias, connection_lost_tx),
        );
        let mut serializer = EventSerializer::new(
            Some(tremor_codec::Config::from("json")),
            CodecReq::Required,
            vec![],
            &"gcs_streamer".into(),
            &alias,
        )?;

        // simulate sink lifecycle
        sink.on_start(&context).await?;
        sink.connect(&context, &Attempt::default()).await?;

        upload_client(&mut sink).inject_failure(true);

        // send first event
        let id1 = EventId::from_id(1, 1, 1);
        let event = Event {
            id: id1.clone(),
            data: (
                literal!(["snot", 1]),
                literal!({
                    "gcs_streamer": {
                        "bucket": "failure",
                        "name": "test.txt"
                    }
                }),
            )
                .into(),
            ..Event::default()
        };
        assert_eq!(
            SinkReply::NONE,
            sink.on_event("", event, &context, &mut serializer, 0)
                .await?
        );
        assert_eq!(0, upload_client(&mut sink).count());

        upload_client(&mut sink).inject_failure(false);

        // second event - let it succeed, as we want to fail that running upload
        let id2 = EventId::from_id(1, 1, 2);
        let event = Event {
            id: id2.clone(),
            data: (
                literal!({ "": null }),
                literal!({
                    "gcs_streamer": {
                        "bucket": "failure",
                        "name": "test.txt"
                    }
                }),
            )
                .into(),
            transactional: true,
            ..Event::default()
        };
        assert_eq!(
            SinkReply::ACK,
            sink.on_event("", event, &context, &mut serializer, 0)
                .await?
        );
        assert_eq!(1, upload_client(&mut sink).count());
        let mut uploads = upload_client(&mut sink).running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, buffers) = uploads.pop().expect("no data");
        assert_eq!(ObjectId::new("failure", "test.txt"), file_id);
        assert!(buffers.is_empty());
        assert!(upload_client(&mut sink).finished_uploads().is_empty());
        assert!(upload_client(&mut sink).deleted_uploads().is_empty());

        // fail the next event
        upload_client(&mut sink).inject_failure(true);

        let id3 = EventId::from_id(1, 2, 3);
        let event = Event {
            id: id3.clone(),
            data: (
                literal!(42.9),
                literal!({
                    "gcs_streamer": {
                        "bucket": "failure",
                        "name": "test.txt"
                    }
                }),
            )
                .into(),
            ..Event::default()
        };
        assert_eq!(
            SinkReply::NONE,
            sink.on_event("", event, &context, &mut serializer, 0)
                .await?
        );
        // nothing changed - we just failed in the sink
        assert_eq!(1, upload_client(&mut sink).count());
        let mut uploads = upload_client(&mut sink).running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, buffers) = uploads.pop().expect("no data");
        assert_eq!(ObjectId::new("failure", "test.txt"), file_id);
        assert!(buffers.is_empty());
        assert!(upload_client(&mut sink).finished_uploads().is_empty());
        assert!(upload_client(&mut sink).deleted_uploads().is_empty());

        let event = Event {
            data: (
                literal!("snot"),
                literal!({
                    "gcs_streamer": {
                        "bucket": "failure",
                        "name": "next.txt"
                    }
                }),
            )
                .into(),
            transactional: true,
            ..Event::default()
        };
        assert_eq!(
            SinkReply::ACK,
            sink.on_event("", event, &context, &mut serializer, 0)
                .await?
        );
        // nothing changed - we just failed in the sink
        assert_eq!(1, upload_client(&mut sink).count());
        let mut uploads = upload_client(&mut sink).running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, buffers) = uploads.pop().expect("no data");
        assert_eq!(ObjectId::new("failure", "test.txt"), file_id);
        assert!(buffers.is_empty());
        assert!(upload_client(&mut sink).finished_uploads().is_empty());
        assert!(upload_client(&mut sink).deleted_uploads().is_empty());

        // everything works on stop
        upload_client(&mut sink).inject_failure(false);
        sink.on_stop(&context).await?;

        // nothing changed, because we have no running upload
        assert_eq!(1, upload_client(&mut sink).count());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn yolo_invalid_event() -> anyhow::Result<()> {
        let upload_client_factory = Box::new(|_config: &Config| Ok(TestUploadClient::default()));
        let config = Config {
            url: Url::default(),
            bucket: None,
            mode: Mode::Yolo,
            connect_timeout: 1_000_000_000,
            buffer_size: 10,
            max_retries: 3,
            backoff_base_time: 1,
            token: TokenSrc::dummy(),
        };

        let sink_impl = GCSObjectStorageSinkImpl::yolo(config, upload_client_factory);
        let mut sink: YoloSink<
            GCSObjectStorageSinkImpl<TestUploadClient>,
            GCSUpload,
            ChunkedBuffer,
        > = YoloSink::new(sink_impl);

        let (connection_lost_tx, _) = bounded(10);

        let alias = alias::Connector::new("a", "b");
        let context = SinkContext::new(
            SinkId::default(),
            alias.clone(),
            "gcs_streamer".into(),
            QuiescenceBeacon::default(),
            ConnectionLostNotifier::new(&alias, connection_lost_tx),
        );
        let mut serializer = EventSerializer::new(
            Some(tremor_codec::Config::from("json")),
            CodecReq::Required,
            vec![],
            &"gcs_streamer".into(),
            &alias,
        )?;

        // simulate sink lifecycle
        sink.on_start(&context).await?;
        sink.connect(&context, &Attempt::default()).await?;

        let event = Event {
            data: (literal!({}), literal!({})).into(), // no gcs_streamer meta
            ..Event::default()
        };
        assert!(sink
            .on_event("", event, &context, &mut serializer, 0)
            .await
            .is_err());
        let event = Event {
            data: (
                literal!({}),
                literal!({
                    "gcs_streamer": {
                        "bucket": null, // invalid bucket
                        "name": "42.24"
                    }
                }),
            )
                .into(), // no gcs_streamer meta
            ..Event::default()
        };
        assert!(sink
            .on_event("", event, &context, &mut serializer, 0)
            .await
            .is_err());
        let event = Event {
            data: (
                literal!({}),
                literal!({
                    "gcs_streamer": {
                        "bucket": "null",
                        "name": 42.24 // invalid name
                    }
                }),
            )
                .into(), // no gcs_streamer meta
            ..Event::default()
        };
        assert!(sink
            .on_event("", event, &context, &mut serializer, 0)
            .await
            .is_err());
        assert_eq!(0, upload_client(&mut sink).count());
        assert!(upload_client(&mut sink).running_uploads().is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn yolo_bucket_exists_error() -> anyhow::Result<()> {
        let upload_client_factory = Box::new(|_config: &Config| {
            let mut client = TestUploadClient::default();
            client.inject_failure(true);
            Ok(client)
        });
        let config = Config {
            url: Url::default(),
            bucket: Some("bucket".to_string()),
            mode: Mode::Yolo,
            connect_timeout: 1_000_000_000,
            buffer_size: 10,
            max_retries: 3,
            backoff_base_time: 1,
            token: TokenSrc::dummy(),
        };

        let sink_impl = GCSObjectStorageSinkImpl::yolo(config, upload_client_factory);
        let mut sink: YoloSink<
            GCSObjectStorageSinkImpl<TestUploadClient>,
            GCSUpload,
            ChunkedBuffer,
        > = YoloSink::new(sink_impl);

        let (connection_lost_tx, _) = bounded(10);

        let alias = alias::Connector::new("a", "b");
        let context = SinkContext::new(
            SinkId::default(),
            alias.clone(),
            "gcs_streamer".into(),
            QuiescenceBeacon::default(),
            ConnectionLostNotifier::new(&alias, connection_lost_tx),
        );

        // simulate sink lifecycle
        sink.on_start(&context).await?;
        assert!(sink.connect(&context, &Attempt::default()).await.is_err());
        Ok(())
    }

    fn test_client(
        sink: &mut ConsistentSink<
            GCSObjectStorageSinkImpl<TestUploadClient>,
            GCSUpload,
            ChunkedBuffer,
        >,
    ) -> &mut TestUploadClient {
        sink.sink_impl
            .upload_client
            .as_mut()
            .expect("No upload client available")
    }
    #[allow(clippy::too_many_lines)] // this is a test
    #[tokio::test(flavor = "multi_thread")]
    pub async fn consistent_happy_path() -> anyhow::Result<()> {
        let (reply_tx, mut reply_rx) = unbounded();
        let upload_client_factory = Box::new(|_config: &Config| Ok(TestUploadClient::default()));
        let config = Config {
            url: Url::default(),
            bucket: None,
            mode: Mode::Consistent,
            connect_timeout: 1_000_000_000,
            buffer_size: 10,
            max_retries: 3,
            backoff_base_time: 1,
            token: TokenSrc::dummy(),
        };

        let sink_impl =
            GCSObjectStorageSinkImpl::consistent(config, upload_client_factory, reply_tx);
        let mut sink: ConsistentSink<
            GCSObjectStorageSinkImpl<TestUploadClient>,
            GCSUpload,
            ChunkedBuffer,
        > = ConsistentSink::new(sink_impl);

        let (connection_lost_tx, _) = bounded(10);

        let alias = alias::Connector::new("a", "b");
        let context = SinkContext::new(
            SinkId::default(),
            alias.clone(),
            "gcs_streamer".into(),
            QuiescenceBeacon::default(),
            ConnectionLostNotifier::new(&alias, connection_lost_tx),
        );
        let mut serializer = EventSerializer::new(
            Some(tremor_codec::Config::from("json")),
            CodecReq::Required,
            vec![],
            &"gcs_streamer".into(),
            &alias,
        )?;

        // simulate standard sink lifecycle
        sink.on_start(&context).await?;
        sink.connect(&context, &Attempt::default()).await?;

        let id1 = EventId::from_id(0, 0, 1);
        let event = Event {
            id: id1.clone(),
            data: (
                literal!({}),
                literal!({
                    "gcs_streamer": {
                        "name": "test.txt",
                        "bucket": "woah"
                    }
                }),
            )
                .into(),
            ..Event::default()
        };
        sink.on_event("", event.clone(), &context, &mut serializer, 1234)
            .await?;

        // verify it started the upload upon first request
        let mut uploads = test_client(&mut sink).running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, buffers) = uploads.pop().expect("no data");
        assert_eq!(ObjectId::new("woah", "test.txt"), file_id);
        assert!(buffers.is_empty());
        assert_eq!(1, test_client(&mut sink).count());
        assert!(test_client(&mut sink).finished_uploads().is_empty());
        assert!(test_client(&mut sink).deleted_uploads().is_empty());

        let id2 = EventId::from_id(0, 0, 2);
        let event = Event {
            id: id2.clone(),
            data: (
                literal!(["abcdefghijklmnopqrstuvwxyz"]),
                literal!({
                    "gcs_streamer": {
                        "name": "test.txt",
                        "bucket": "woah"
                    }
                }),
            )
                .into(),
            transactional: true,
            ..Event::default()
        };
        sink.on_event("", event.clone(), &context, &mut serializer, 1234)
            .await?;

        // verify it did upload some parts
        let mut uploads = test_client(&mut sink).running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, mut buffers) = uploads.pop().expect("no data");
        assert_eq!(ObjectId::new("woah", "test.txt"), file_id);
        assert_eq!(3, buffers.len());
        let part = buffers.pop().expect("no data");
        assert_eq!(20, part.start);
        assert_eq!(10, part.len());
        assert_eq!("qrstuvwxyz".as_bytes(), &part.data);
        let part = buffers.pop().expect("no data");
        assert_eq!(10, part.start);
        assert_eq!(10, part.len());
        assert_eq!("ghijklmnop".as_bytes(), &part.data);
        let part = buffers.pop().expect("no data");
        assert_eq!(0, part.start);
        assert_eq!(10, part.len());
        assert_eq!("{}[\"abcdef".as_bytes(), &part.data);

        assert_eq!(1, test_client(&mut sink).count());
        assert!(test_client(&mut sink).finished_uploads().is_empty());
        assert!(test_client(&mut sink).deleted_uploads().is_empty());

        // finishes upload on filename change - bucket changes
        let id3 = EventId::from_id(1, 1, 1);
        let event = Event {
            id: id3.clone(),
            data: (
                Value::from(42_u64),
                literal!({
                    "gcs_streamer": {
                        "name": "test.txt",
                        "bucket": "woah2"
                    }
                }),
            )
                .into(),
            transactional: true,
            ..Event::default()
        };
        sink.on_event("", event.clone(), &context, &mut serializer, 1234)
            .await?;

        let mut uploads = test_client(&mut sink).running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, buffers) = uploads.pop().expect("no data");
        assert_eq!(ObjectId::new("woah2", "test.txt"), file_id);
        assert!(buffers.is_empty());

        assert_eq!(2, test_client(&mut sink).count());

        // 1 finished upload
        let mut finished = test_client(&mut sink).finished_uploads();
        assert_eq!(1, finished.len());
        let (file_id, mut buffers) = finished.pop().expect("no data");

        assert_eq!(ObjectId::new("woah", "test.txt"), file_id);
        assert_eq!(4, buffers.len());
        let last = buffers.pop().expect("no data");
        assert_eq!(30, last.start);
        assert_eq!(2, last.len());
        assert_eq!("\"]".as_bytes(), &last.data);

        assert!(test_client(&mut sink).deleted_uploads().is_empty());
        let res = reply_rx.try_recv();
        if let Ok(AsyncSinkReply::Ack(cf_data, _duration)) = res {
            let id = cf_data.event_id();
            assert!(id.is_tracking(&id1));
            assert!(id.is_tracking(&id2));
            assert!(!id.is_tracking(&id3));
        } else {
            panic!("Expected an ack, got {res:?}");
        }

        // finishes upload on filename change - name changes
        let id4 = EventId::from_id(2, 2, 2);
        let event = Event {
            id: id4.clone(),
            data: (
                literal!(true),
                literal!({
                    "gcs_streamer": {
                        "name": "test5.txt",
                        "bucket": "woah2"
                    }
                }),
            )
                .into(),
            transactional: true,
            ..Event::default()
        };
        sink.on_event("", event.clone(), &context, &mut serializer, 1234)
            .await?;

        let mut uploads = test_client(&mut sink).running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, buffers) = uploads.pop().expect("no data");
        assert_eq!(ObjectId::new("woah2", "test5.txt"), file_id);
        assert!(buffers.is_empty());

        assert_eq!(3, test_client(&mut sink).count());

        // 2 finished uploads
        let mut finished = test_client(&mut sink).finished_uploads();
        assert_eq!(2, finished.len());
        let (file_id, mut buffers) = finished.pop().expect("no data");

        assert_eq!(ObjectId::new("woah2", "test.txt"), file_id);
        assert_eq!(1, buffers.len());
        let last = buffers.pop().expect("no data");
        assert_eq!(0, last.start);
        assert_eq!(2, last.len());
        assert_eq!("42".as_bytes(), &last.data);

        assert!(test_client(&mut sink).deleted_uploads().is_empty());
        let res = reply_rx.try_recv();
        if let Ok(AsyncSinkReply::Ack(cf_data, _duration)) = res {
            let id = cf_data.event_id();
            assert!(id.is_tracking(&id3));
            assert!(!id.is_tracking(&id4));
        } else {
            panic!("Expected an ack, got {res:?}");
        }

        // finishes upload on stop
        sink.on_stop(&context).await?;
        assert_eq!(3, test_client(&mut sink).finished_uploads().len());

        Ok(())
    }

    #[allow(clippy::too_many_lines)] // this is a test
    #[tokio::test(flavor = "multi_thread")]
    async fn consistent_on_failure() -> anyhow::Result<()> {
        let (reply_tx, mut reply_rx) = unbounded();
        let upload_client_factory = Box::new(|_config: &Config| Ok(TestUploadClient::default()));
        let config = Config {
            url: Url::default(),
            bucket: None,
            mode: Mode::Consistent,
            connect_timeout: 1_000_000_000,
            buffer_size: 10,
            max_retries: 3,
            backoff_base_time: 1,
            token: TokenSrc::dummy(),
        };

        let sink_impl =
            GCSObjectStorageSinkImpl::consistent(config, upload_client_factory, reply_tx);
        let mut sink: ConsistentSink<
            GCSObjectStorageSinkImpl<TestUploadClient>,
            GCSUpload,
            ChunkedBuffer,
        > = ConsistentSink::new(sink_impl);

        let (connection_lost_tx, _) = bounded(10);

        let alias = alias::Connector::new("a", "b");
        let context = SinkContext::new(
            SinkId::default(),
            alias.clone(),
            "gcs_streamer".into(),
            QuiescenceBeacon::default(),
            ConnectionLostNotifier::new(&alias, connection_lost_tx),
        );
        let mut serializer = EventSerializer::new(
            Some(tremor_codec::Config::from("json")),
            CodecReq::Required,
            vec![],
            &"gcs_streamer".into(),
            &alias,
        )?;

        // simulate standard sink lifecycle
        sink.on_start(&context).await?;
        sink.connect(&context, &Attempt::default()).await?;

        let id1 = EventId::from_id(1, 1, 1);
        let event = Event {
            id: id1.clone(),
            data: (
                literal!({"snot": "badger"}),
                literal!({
                    "gcs_streamer": {
                        "name": "woah.txt",
                        "bucket": "failure"
                    }
                }),
            )
                .into(),
            ..Event::default()
        };
        sink.on_event("", event, &context, &mut serializer, 0)
            .await?;

        assert_eq!(1, test_client(&mut sink).running_uploads().len());
        assert_eq!(1, test_client(&mut sink).count());

        // make the client fail requests
        debug!("INJECT FAILURE");
        test_client(&mut sink).inject_failure(true);

        let id2 = EventId::from_id(3, 3, 3);
        let event = Event {
            id: id2.clone(),
            data: (
                literal!([1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
                literal!({
                    "gcs_streamer": {
                        "name": "woah.txt",
                        "bucket": "failure"
                    }
                }),
            )
                .into(),
            ..Event::default()
        };
        sink.on_event("", event, &context, &mut serializer, 0)
            .await?;
        assert_eq!(1, test_client(&mut sink).count());
        assert_eq!(1, test_client(&mut sink).running_uploads().len());
        assert_eq!(0, test_client(&mut sink).deleted_uploads().len());

        test_client(&mut sink).inject_failure(false);
        // trigger deletion of the old failed upload, trigger a fail for the old upload
        // and start a new one for the second batch element
        let id3 = EventId::from_id(4, 4, 100);
        let event = Event {
            id: id3.clone(),
            data: (
                literal!([
                    {
                        "data": {
                            "meta": {
                                "gcs_streamer": {
                                    "name": "woah.txt",
                                    "bucket": "failure"
                                }
                            },
                            "value": "snot"
                        }
                    },
                    {
                        "data": {
                            "meta": {
                                "gcs_streamer": {
                                    "name": "woah2.txt",
                                    "bucket": "failure"
                                }
                            },
                            "value": "badger"
                        }
                    }
                ]),
                literal!({}),
            )
                .into(),
            transactional: true,
            is_batch: true,
            ..Event::default()
        };
        sink.on_event("", event, &context, &mut serializer, 0)
            .await?;

        assert_eq!(1, test_client(&mut sink).running_uploads().len());
        assert_eq!(2, test_client(&mut sink).count());
        assert_eq!(1, test_client(&mut sink).deleted_uploads().len());
        assert_eq!(0, test_client(&mut sink).finished_uploads().len());

        let reply = reply_rx.try_recv();
        if let Ok(AsyncSinkReply::Fail(cf)) = reply {
            let id = cf.event_id();
            assert!(id.is_tracking(&id1));
            assert!(id.is_tracking(&id2));
            assert!(id.is_tracking(&id3));
        } else {
            panic!("Expected a fail, got {reply:?}");
        }

        sink.on_stop(&context).await?;

        assert_eq!(0, test_client(&mut sink).running_uploads().len());
        assert_eq!(2, test_client(&mut sink).count());
        assert_eq!(1, test_client(&mut sink).deleted_uploads().len());
        assert_eq!(1, test_client(&mut sink).finished_uploads().len());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connector_yolo_mode() -> anyhow::Result<()> {
        let config = literal!({
            "token": {"file": file!().to_string()},
            "mode": "yolo"
        });
        let builder = super::Builder::default();
        let cfg = ConnectorConfig {
            connector_type: CONNECTOR_TYPE.into(),
            codec: Some("json".into()),
            config: Some(config),
            preprocessors: None,
            postprocessors: None,
            reconnect: Reconnect::None,
            metrics_interval_s: None,
        };
        let kill_switch = KillSwitch::dummy();
        let alias = alias::Connector::new("snot", "badger");
        let mut connector_id_gen = ConnectorIdGen::default();

        // lets cover create-sink here
        let addr = crate::spawn(&alias, &mut connector_id_gen, &builder, cfg, &kill_switch).await?;
        let (tx, mut rx) = bounded(1);
        addr.stop(tx).await?;
        assert!(rx.recv().await.expect("rx empty").res.is_ok());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connector_consistent_mode() -> anyhow::Result<()> {
        let config = literal!({
            "token": {"file": file!().to_string()},
            "mode": "consistent"
        });
        let builder = super::Builder::default();
        let cfg = ConnectorConfig {
            connector_type: CONNECTOR_TYPE.into(),
            codec: Some("json".into()),
            config: Some(config),
            preprocessors: None,
            postprocessors: None,
            reconnect: Reconnect::None,
            metrics_interval_s: None,
        };
        let kill_switch = KillSwitch::dummy();
        let alias = alias::Connector::new("snot", "badger");
        let mut connector_id_gen = ConnectorIdGen::default();

        // lets cover create-sink here
        let addr = crate::spawn(&alias, &mut connector_id_gen, &builder, cfg, &kill_switch).await?;
        let (tx, mut rx) = bounded(1);
        addr.stop(tx).await?;
        assert!(rx.recv().await.expect("rx empty").res.is_ok());
        Ok(())
    }
}
