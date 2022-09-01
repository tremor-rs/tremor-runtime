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

//! `gcs_streamer` sink implementation that is blindly acking all events it receives
//!
//! Only exceptions are if we can't find out where to upload the event due to invalid metadata
//! or if we fail serializing the event payload.
//!
//! No errors are raised coming from any operation against gcs.
//!
//! This is a fire-and-forget sink.
use super::chunked_buffer::{BufferPart, ChunkedBuffer};
use super::resumable_upload_client::{FileId, ResumableUploadClient};
use super::streamer::{Config, GCSCommons};
use crate::connectors::prelude::*;
use crate::errors::err_gcs;

struct YoloUpload {
    file_id: FileId,
    session_uri: url::Url,
}

pub(super) struct GCSYoloSink<Client: ResumableUploadClient> {
    config: Config,
    buffers: ChunkedBuffer,
    upload_client: Client,
    default_bucket: Option<Value<'static>>,
    current_upload: Option<YoloUpload>,
}

impl<Client: ResumableUploadClient + Send> GCSCommons<Client> for GCSYoloSink<Client> {
    fn default_bucket(&self) -> Option<&Value<'_>> {
        self.default_bucket.as_ref()
    }

    fn client(&self) -> &Client {
        &self.upload_client
    }
}

#[async_trait::async_trait]
impl<Client: ResumableUploadClient + Send> Sink for GCSYoloSink<Client> {
    // we deliberately don't error here if a call to google fails
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        for (value, meta) in event.value_meta_iter() {
            let meta = ctx.extract_meta(meta);
            let file_id = self.get_file_id(meta)?;

            if let Some(YoloUpload {
                file_id: current_file_id,
                ..
            }) = self.current_upload.as_ref()
            {
                if file_id != *current_file_id {
                    // if finishing the previous one failed, we continue anyways
                    ctx.swallow_err(
                        self.finish_upload(ctx).await,
                        "Error finishing previous upload",
                    );
                }
            }
            if self.current_upload.is_none() {
                // we gotta stop here, otherwise we add to no upload
                if let Err(e) = self.start_upload(&file_id).await {
                    error!("{ctx} Error starting upload for {file_id}: {e}");
                    break;
                }
            }
            let serialized_data = serializer.serialize(value, event.ingest_ns)?;
            for item in serialized_data {
                self.buffers.write(&item);
            }

            while let Some(data) = self.buffers.read_current_block() {
                let start = data.start;
                let end = data.start + data.len();
                if let Ok(done_until) = ctx.bail_err(
                    self.upload_data(data, ctx).await,
                    &format!(
                        "Error uploading data for bytes {}-{} for {}",
                        start, end, file_id
                    ),
                ) {
                    // if this fails, we corrupted our internal state somehow
                    self.buffers.mark_done_until(done_until)?;
                } else {
                    // stop attempting writes on error
                    break;
                }
            }
        }
        Ok(SinkReply::ack_or_none(event.transactional))
    }

    async fn on_stop(&mut self, ctx: &SinkContext) -> Result<()> {
        ctx.swallow_err(
            self.finish_upload(ctx).await,
            "Error finishing upload on stop",
        );
        Ok(())
    }

    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        // clear out the previous upload
        self.current_upload = None;
        self.buffers = ChunkedBuffer::new(self.config.buffer_size);
        // if we have a default bucket, check that it is accessible
        Ok(if let Some(default_bucket) = self.default_bucket.as_str() {
            self.upload_client
                .bucket_exists(&self.config.url, default_bucket)
                .await?
        } else {
            true
        })
    }

    fn auto_ack(&self) -> bool {
        false // we always ack
    }
}

impl<Client: ResumableUploadClient> GCSYoloSink<Client> {
    pub(super) fn new(config: Config, upload_client: Client) -> Self {
        let buffers = ChunkedBuffer::new(config.buffer_size);
        let default_bucket = config.bucket.as_ref().cloned().map(Value::from);
        Self {
            config,
            buffers,
            upload_client,
            default_bucket,
            current_upload: None,
        }
    }

    async fn start_upload(&mut self, file_id: &FileId) -> Result<()> {
        let session_uri = self
            .upload_client
            .start_upload(&self.config.url, file_id.clone())
            .await
            .map_err(|e| err_gcs(format!("Error starting upload for {file_id}: {e}",)))?;
        self.current_upload = Some(YoloUpload {
            file_id: file_id.clone(),
            session_uri,
        });
        Ok(())
    }

    async fn finish_upload(&mut self, ctx: &SinkContext) -> Result<()> {
        if let Some(YoloUpload {
            file_id,
            session_uri,
        }) = self.current_upload.take()
        {
            let upload_buffer = std::mem::replace(
                &mut self.buffers,
                ChunkedBuffer::new(self.config.buffer_size),
            );
            let final_data = upload_buffer.final_block();
            debug!(
                "{ctx} Finishing upload for {file_id}. Uploading bytes: {}-{}",
                final_data.start,
                final_data.start + final_data.len()
            );
            self.upload_client
                .finish_upload(&session_uri, final_data)
                .await
                .map_err(|e| err_gcs(format!("Error finishing upload for {file_id}: {e}")))?;
        }
        Ok(())
    }

    async fn upload_data(&mut self, data: BufferPart, ctx: &SinkContext) -> Result<usize> {
        let YoloUpload {
            file_id,
            session_uri,
        } = self
            .current_upload
            .as_ref()
            .ok_or_else(|| err_gcs("Invalid state: No current Upload available."))?;
        debug!(
            "{ctx} Uploading Bytes {}-{} for {file_id}",
            data.start,
            data.start + data.len()
        );
        self.upload_client.upload_data(session_uri, data).await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Codec as CodecConfig,
        connectors::{
            impls::gcs::streamer::{tests::TestUploadClient, Mode},
            reconnect::ConnectionLostNotifier,
        },
    };
    use async_std::channel::bounded;
    use tremor_pipeline::EventId;

    #[async_std::test]
    async fn happy_path() -> Result<()> {
        _ = env_logger::try_init();
        let upload_client = TestUploadClient::default();

        let mut sink = GCSYoloSink {
            config: Config {
                url: Default::default(),
                bucket: None,
                mode: Mode::Yolo,
                connect_timeout: 1000000000,
                buffer_size: 10,
                max_retries: 3,
                default_backoff_base_time: 1,
            },
            buffers: ChunkedBuffer::new(10),
            upload_client,
            current_upload: None,
            default_bucket: None,
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
            Some(CodecConfig::from("json")),
            CodecReq::Required,
            vec![],
            &"gcs_streamer".into(),
            &alias,
        )
        .unwrap();

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
        assert_eq!(1, sink.upload_client.count());
        let mut uploads = sink.upload_client.running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, mut buffers) = uploads.pop().unwrap();
        assert_eq!(
            FileId {
                name: "happy.txt".to_string(),
                bucket: "yolo".to_string()
            },
            file_id
        );
        assert_eq!(2, buffers.len());
        let buffer = buffers.pop().unwrap();
        assert_eq!(10, buffer.start);
        assert_eq!(10, buffer.len());
        assert_eq!("badg\",\"er\"".as_bytes(), &buffer.data);

        let buffer = buffers.pop().unwrap();
        assert_eq!(0, buffer.start);
        assert_eq!(10, buffer.len());
        assert_eq!("{\"snot\":[\"".as_bytes(), &buffer.data);

        assert!(sink.upload_client.finished_uploads().is_empty());
        assert!(sink.upload_client.deleted_uploads().is_empty());

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

        assert_eq!(2, sink.upload_client.count());
        let mut uploads = sink.upload_client.running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, mut buffers) = uploads.pop().unwrap();
        assert_eq!(
            FileId {
                name: "sad.txt".to_string(),
                bucket: "yolo".to_string()
            },
            file_id
        );
        assert_eq!(2, buffers.len());

        let buffer = buffers.pop().unwrap();
        assert_eq!(10, buffer.start);
        assert_eq!(10, buffer.len());
        assert_eq!("e,false,nu".as_bytes(), &buffer.data);

        let buffer = buffers.pop().unwrap();
        assert_eq!(0, buffer.start);
        assert_eq!(10, buffer.len());
        assert_eq!("[1,2,3,tru".as_bytes(), &buffer.data);

        let mut finished = sink.upload_client.finished_uploads();
        assert_eq!(1, finished.len());
        let (file_id, mut buffers) = finished.pop().unwrap();
        assert_eq!(
            FileId {
                name: "happy.txt".to_string(),
                bucket: "yolo".to_string()
            },
            file_id
        );
        assert_eq!(4, buffers.len());
        let buffer = buffers.pop().unwrap();
        assert_eq!(30, buffer.start);
        assert_eq!("adger\"".as_bytes(), &buffer.data);

        let buffer = buffers.pop().unwrap();
        assert_eq!(20, buffer.start);
        assert_eq!("]}\"snot\"\"b".as_bytes(), &buffer.data);

        assert!(sink.upload_client.deleted_uploads().is_empty());

        sink.on_stop(&context).await?;

        // we finish outstanding upload upon stop
        let mut finished = sink.upload_client.finished_uploads();
        assert_eq!(2, finished.len());
        let (file_id, mut buffers) = finished.pop().unwrap();
        assert_eq!(
            FileId {
                name: "sad.txt".to_string(),
                bucket: "yolo".to_string()
            },
            file_id
        );
        assert_eq!(3, buffers.len());
        let last = buffers.pop().unwrap();
        assert_eq!(20, last.start);
        assert_eq!("ll]".as_bytes(), &last.data);

        Ok(())
    }

    #[async_std::test]
    async fn on_failure() -> Result<()> {
        // ensure that failures on calls to google don't fail events
        _ = env_logger::try_init();
        let upload_client = TestUploadClient::default();

        let mut sink = GCSYoloSink {
            config: Config {
                url: Default::default(),
                bucket: None,
                mode: Mode::Yolo,
                connect_timeout: 1000000000,
                buffer_size: 10,
                max_retries: 3,
                default_backoff_base_time: 1,
            },
            buffers: ChunkedBuffer::new(10),
            upload_client,
            current_upload: None,
            default_bucket: None,
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
            Some(CodecConfig::from("json")),
            CodecReq::Required,
            vec![],
            &"gcs_streamer".into(),
            &alias,
        )
        .unwrap();

        // simulate sink lifecycle
        sink.on_start(&context).await?;
        sink.connect(&context, &Attempt::default()).await?;

        sink.upload_client.inject_failure(true);

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
        assert_eq!(0, sink.upload_client.count());

        sink.upload_client.inject_failure(false);

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
        assert_eq!(1, sink.upload_client.count());
        let mut uploads = sink.upload_client.running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, buffers) = uploads.pop().unwrap();
        assert_eq!(
            FileId {
                bucket: "failure".to_string(),
                name: "test.txt".to_string()
            },
            file_id
        );
        assert!(buffers.is_empty());
        assert!(sink.upload_client.finished_uploads().is_empty());
        assert!(sink.upload_client.deleted_uploads().is_empty());

        // fail the next event
        sink.upload_client.inject_failure(true);

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
        assert_eq!(1, sink.upload_client.count());
        let mut uploads = sink.upload_client.running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, buffers) = uploads.pop().unwrap();
        assert_eq!(
            FileId {
                bucket: "failure".to_string(),
                name: "test.txt".to_string()
            },
            file_id
        );
        assert!(buffers.is_empty());
        assert!(sink.upload_client.finished_uploads().is_empty());
        assert!(sink.upload_client.deleted_uploads().is_empty());

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
        assert_eq!(1, sink.upload_client.count());
        let mut uploads = sink.upload_client.running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, buffers) = uploads.pop().unwrap();
        assert_eq!(
            FileId {
                bucket: "failure".to_string(),
                name: "test.txt".to_string()
            },
            file_id
        );
        assert!(buffers.is_empty());
        assert!(sink.upload_client.finished_uploads().is_empty());
        assert!(sink.upload_client.deleted_uploads().is_empty());

        // everything works on stop
        sink.upload_client.inject_failure(false);
        sink.on_stop(&context).await?;

        // nothing changed, because we have no running upload
        assert_eq!(1, sink.upload_client.count());

        Ok(())
    }

    #[async_std::test]
    async fn invalid_event() -> Result<()> {
        _ = env_logger::try_init();
        let upload_client = TestUploadClient::default();

        let mut sink = GCSYoloSink {
            config: Config {
                url: Default::default(),
                bucket: None,
                mode: Mode::Yolo,
                connect_timeout: 1000000000,
                buffer_size: 10,
                max_retries: 3,
                default_backoff_base_time: 1,
            },
            buffers: ChunkedBuffer::new(10),
            upload_client,
            current_upload: None,
            default_bucket: None,
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
            Some(CodecConfig::from("json")),
            CodecReq::Required,
            vec![],
            &"gcs_streamer".into(),
            &alias,
        )
        .unwrap();

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
        assert_eq!(0, sink.upload_client.count());
        assert!(sink.upload_client.running_uploads().is_empty());
        Ok(())
    }
}
