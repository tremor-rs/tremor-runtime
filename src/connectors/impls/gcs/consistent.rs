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

//! gcs_streamer sink implementation that ensures that an upload is completely finished
//! and contains all received events and all uploaded parts
//! at the cost of possibly lost uploads.
//!
//! Whenever we encounter an error, we will fail the whole upload and discard all future events from
//! this same upload, until another upload starts.
//! This ensures that there should(tm) never be a corrupted file being uploaded. It also means
//! that some files might be missing.

use crate::connectors::prelude::*;
use crate::{
    connectors::impls::gcs::{
        chunked_buffer::{BufferPart, ChunkedBuffer},
        resumable_upload_client::{FileId, ResumableUploadClient},
        streamer::{Config, GCSCommons},
    },
    errors::err_gcs,
};
use async_std::channel::Sender;
use tremor_common::time::nanotime;
use tremor_pipeline::{Event, EventId, OpMeta};

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
    failed: bool,
}

impl CurrentUpload {
    fn new(file_id: FileId, session_uri: url::Url, event: &Event) -> Self {
        Self {
            file_id,
            session_uri,
            event_id: event.id.clone(),
            op_meta: event.op_meta.clone(),
            transactional: event.transactional,
            failed: false,
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

pub(super) struct GCSConsistentSink<Client: ResumableUploadClient> {
    config: Config,
    buffers: ChunkedBuffer,
    upload_client: Client,
    current_upload: Option<CurrentUpload>,
    default_bucket: Option<Value<'static>>,
    reply_tx: Sender<AsyncSinkReply>,
}

impl<Client: ResumableUploadClient> GCSCommons<Client> for GCSConsistentSink<Client> {
    fn default_bucket(&self) -> Option<&Value<'_>> {
        self.default_bucket.as_ref()
    }

    fn client(&self) -> &Client {
        &self.upload_client
    }
}

#[async_trait::async_trait]
impl<Client: ResumableUploadClient + Send> Sink for GCSConsistentSink<Client> {
    /// This sink implementation will fail the whole upload if anything goes bad
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let res = self.on_event_inner(&event, serializer, ctx).await;
        if let Err(e) = res {
            error!("{ctx} {e}");
            // mark the current upload as failed
            if let Some(current_upload) = self.current_upload.as_mut() {
                warn!(
                    "{ctx} Marking upload for {} as failed",
                    current_upload.file_id
                );
                current_upload.failed = true;
                // we don't fail it yet, only when the next upload comes or we stop
            } else {
                // if we don't have an upload at this point we can also just error out and trigger a fail
                return Err(e);
            }
        }
        Ok(SinkReply::NONE)
    }

    async fn on_stop(&mut self, ctx: &SinkContext) -> Result<()> {
        // lets not error the stop-process if something goes wrong, we are shutting down anyways
        ctx.swallow_err(
            self.fail_or_finish_upload(ctx).await,
            "Error finishing the current upload",
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
        false
    }
}

impl<Client: ResumableUploadClient> GCSConsistentSink<Client> {
    pub(super) fn new(
        config: Config,
        upload_client: Client,
        reply_tx: Sender<AsyncSinkReply>,
    ) -> Self {
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

    async fn on_event_inner(
        &mut self,
        event: &Event,
        serializer: &mut EventSerializer,
        ctx: &SinkContext,
    ) -> Result<()> {
        let mut event_tracked = false;

        for (value, meta) in event.value_meta_iter() {
            let meta = ctx.extract_meta(meta);
            let file_id = self.get_file_id(meta)?;

            // ignore current event (or part of it if batched) if current upload with same file_id is failed
            if let Some(current_upload) = self.current_upload.as_mut() {
                if file_id == current_upload.file_id {
                    // track the event as being part of the current upload
                    if !event_tracked {
                        current_upload.track(&event);
                        event_tracked = true
                    }
                    if current_upload.failed {
                        // same file_id for a failed upload ==> ignore and continue with the next event
                        debug!("{ctx} Ignore event for {file_id} as current upload is failed");
                        continue;
                    }
                } else {
                    // new file_id, current upload is failed  ==> FAIL & DELETE UPLOAD \
                    //                                                                 |--> START NEW UPLOAD
                    // new file_id, current upload not failed ==> FINISH UPLOAD ______/
                    self.fail_or_finish_upload(ctx).await?;

                    self.start_upload(&file_id, &event, ctx).await?;
                }
            } else {
                // no current upload available ==> START UPLOAD
                self.start_upload(&file_id, &event, ctx).await?;
            }

            // At this point we defo have a healthy upload
            // accumulate event payload for the current upload
            let serialized_data = serializer.serialize(value, event.ingest_ns)?;
            for item in serialized_data {
                self.buffers.write(&item);
            }

            // upload some data if necessary
            while let Some(data) = self.buffers.read_current_block() {
                let done_until = self.upload_data(data, ctx).await?;
                self.buffers.mark_done_until(done_until)?;
            }
        }
        Ok(())
    }

    /// delete the failed upload from GCS, fail all accumulated events
    async fn fail_upload(&mut self, upload: CurrentUpload, ctx: &SinkContext) -> Result<()> {
        let CurrentUpload {
            file_id,
            session_uri,
            event_id,
            op_meta,
            transactional,
            ..
        } = upload;

        // clear out the current buffer
        self.buffers = ChunkedBuffer::new(self.config.buffer_size);

        if transactional {
            self.reply_tx
                .send(AsyncSinkReply::Fail(ContraflowData::new(
                    event_id,
                    nanotime(),
                    op_meta,
                )))
                .await?;
        }
        debug!("{ctx} Deleting failed upload for {file_id}");
        // we don't really care if deletion fails, the upload will expire after a week anyways
        ctx.swallow_err(
            self.upload_client.delete_upload(&session_uri).await,
            &format!("Error deleting upload for {file_id}"),
        );
        Ok(())
    }

    /// finish the current upload, ack all accumulated events
    async fn finish_upload(&mut self, upload: CurrentUpload, ctx: &SinkContext) -> Result<()> {
        let CurrentUpload {
            file_id,
            session_uri,
            event_id,
            op_meta,
            transactional,
            ..
        } = upload;
        let mut buffers = ChunkedBuffer::new(self.config.buffer_size);

        std::mem::swap(&mut self.buffers, &mut buffers);

        let final_data = buffers.final_block();

        debug!(
            "{ctx} Finishing upload for {file_id} with Bytes {}-{}",
            final_data.start,
            final_data.start + final_data.len()
        );
        let res = self
            .upload_client
            .finish_upload(&session_uri, final_data)
            .await
            .map_err(|e| err_gcs(format!("Error finishing upload for {file_id}: {e}")));
        // send ack/fail if we have a transactional event
        if transactional {
            let cf_data = ContraflowData::new(event_id, nanotime(), op_meta);
            let reply = match res {
                Ok(()) => AsyncSinkReply::Ack(cf_data, 0),
                Err(_) => AsyncSinkReply::Fail(cf_data),
            };
            ctx.swallow_err(
                self.reply_tx.send(reply).await,
                &format!("Error sending ack/fail for upload to {file_id}"),
            );
        }
        res
    }

    async fn fail_or_finish_upload(&mut self, ctx: &SinkContext) -> Result<()> {
        if let Some(upload) = self.current_upload.take() {
            if upload.failed {
                self.fail_upload(upload, ctx).await?;
            } else {
                self.finish_upload(upload, ctx).await?;
            }
        }
        Ok(())
    }

    /// Starts a new upload
    async fn start_upload(
        &mut self,
        file_id: &FileId,
        event: &Event,
        ctx: &SinkContext,
    ) -> Result<()> {
        debug!("{ctx} Starting upload for {file_id}");
        let session_url = self
            .upload_client
            .start_upload(&self.config.url, file_id.clone())
            .await
            .map_err(|e| err_gcs(format!("Error starting upload for {file_id}: {e}",)))?;
        self.current_upload = Some(CurrentUpload::new(file_id.clone(), session_url, event));
        Ok(())
    }

    async fn upload_data(&mut self, data: BufferPart, ctx: &SinkContext) -> Result<usize> {
        let CurrentUpload {
            session_uri,
            file_id,
            ..
        } = self
            .current_upload
            .as_ref()
            .ok_or(err_gcs("Invalid state, no current upload running."))?;
        debug!(
            "{ctx} Uploading Bytes {}-{} for {file_id}",
            data.start,
            data.start + data.len()
        );
        self.upload_client
            .upload_data(session_uri, data)
            .await
            .map_err(|e| err_gcs(format!("Error uploading data for {file_id}: {e}")))
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

    #[async_std::test]
    pub async fn consistent_happy_path() -> Result<()> {
        _ = env_logger::try_init();
        let (reply_tx, reply_rx) = bounded(10);
        let upload_client = TestUploadClient::default();

        let mut sink = GCSConsistentSink {
            config: Config {
                url: Default::default(),
                bucket: None,
                mode: Mode::Consistent,
                connect_timeout: 1000000000,
                buffer_size: 10,
                max_retries: 3,
                default_backoff_base_time: 1,
            },
            buffers: ChunkedBuffer::new(10),
            upload_client,
            current_upload: None,
            default_bucket: None,
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
            Some(CodecConfig::from("json")),
            CodecReq::Required,
            vec![],
            &"gcs_streamer".into(),
            &alias,
        )
        .unwrap();

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
            .await
            .unwrap();

        // verify it started the upload upon first request
        let mut uploads = sink.upload_client.running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, buffers) = uploads.pop().unwrap();
        assert_eq!(FileId::new("woah", "test.txt"), file_id);
        assert!(buffers.is_empty());
        assert_eq!(1, sink.upload_client.count());
        assert!(sink.upload_client.finished_uploads().is_empty());
        assert!(sink.upload_client.deleted_uploads().is_empty());
        assert!(reply_rx.is_empty()); // no ack/fail sent by now

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
            .await
            .unwrap();

        // verify it did upload some parts
        let mut uploads = sink.upload_client.running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, mut buffers) = uploads.pop().unwrap();
        assert_eq!(FileId::new("woah", "test.txt"), file_id);
        assert_eq!(3, buffers.len());
        let part = buffers.pop().unwrap();
        assert_eq!(20, part.start);
        assert_eq!(10, part.len());
        assert_eq!("qrstuvwxyz".as_bytes(), &part.data);
        let part = buffers.pop().unwrap();
        assert_eq!(10, part.start);
        assert_eq!(10, part.len());
        assert_eq!("ghijklmnop".as_bytes(), &part.data);
        let part = buffers.pop().unwrap();
        assert_eq!(0, part.start);
        assert_eq!(10, part.len());
        assert_eq!("{}[\"abcdef".as_bytes(), &part.data);

        assert_eq!(1, sink.upload_client.count());
        assert!(sink.upload_client.finished_uploads().is_empty());
        assert!(sink.upload_client.deleted_uploads().is_empty());
        assert!(reply_rx.is_empty()); // no ack/fail sent by now

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
            .await
            .unwrap();

        let mut uploads = sink.upload_client.running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, buffers) = uploads.pop().unwrap();
        assert_eq!(FileId::new("woah2", "test.txt"), file_id);
        assert!(buffers.is_empty());

        assert_eq!(2, sink.upload_client.count());

        // 1 finished upload
        let mut finished = sink.upload_client.finished_uploads();
        assert_eq!(1, finished.len());
        let (file_id, mut buffers) = finished.pop().unwrap();

        assert_eq!(FileId::new("woah", "test.txt"), file_id);
        assert_eq!(4, buffers.len());
        let last = buffers.pop().unwrap();
        assert_eq!(30, last.start);
        assert_eq!(2, last.len());
        assert_eq!("\"]".as_bytes(), &last.data);

        assert!(sink.upload_client.deleted_uploads().is_empty());
        let res = reply_rx.try_recv();
        if let Ok(AsyncSinkReply::Ack(cf_data, _duration)) = res {
            let id = cf_data.event_id();
            assert!(id.is_tracking(&id1));
            assert!(id.is_tracking(&id2));
            assert!(!id.is_tracking(&id3));
        } else {
            assert!(false, "Expected an ack, got {res:?}");
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
            .await
            .unwrap();

        let mut uploads = sink.upload_client.running_uploads();
        assert_eq!(1, uploads.len());
        let (file_id, buffers) = uploads.pop().unwrap();
        assert_eq!(FileId::new("woah2", "test5.txt"), file_id);
        assert!(buffers.is_empty());

        assert_eq!(3, sink.upload_client.count());

        // 2 finished uploads
        let mut finished = sink.upload_client.finished_uploads();
        assert_eq!(2, finished.len());
        let (file_id, mut buffers) = finished.pop().unwrap();

        assert_eq!(FileId::new("woah2", "test.txt"), file_id);
        assert_eq!(1, buffers.len());
        let last = buffers.pop().unwrap();
        assert_eq!(0, last.start);
        assert_eq!(2, last.len());
        assert_eq!("42".as_bytes(), &last.data);

        assert!(sink.upload_client.deleted_uploads().is_empty());
        let res = reply_rx.try_recv();
        if let Ok(AsyncSinkReply::Ack(cf_data, _duration)) = res {
            let id = cf_data.event_id();
            assert!(id.is_tracking(&id3));
            assert!(!id.is_tracking(&id4));
        } else {
            assert!(false, "Expected an ack, got {res:?}");
        }

        // finishes upload on stop
        sink.on_stop(&context)
            .await
            .expect("Expected on_stop to work, lol");
        assert_eq!(3, sink.upload_client.finished_uploads().len());

        Ok(())
    }

    #[async_std::test]
    async fn consistent_on_failure() -> Result<()> {
        _ = env_logger::try_init();
        let (reply_tx, reply_rx) = bounded(10);
        let upload_client = TestUploadClient::default();

        let mut sink = GCSConsistentSink {
            config: Config {
                url: Default::default(),
                bucket: None,
                mode: Mode::Consistent,
                connect_timeout: 1000000000,
                buffer_size: 10,
                max_retries: 3,
                default_backoff_base_time: 1,
            },
            buffers: ChunkedBuffer::new(10),
            upload_client,
            current_upload: None,
            default_bucket: None,
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
            Some(CodecConfig::from("json")),
            CodecReq::Required,
            vec![],
            &"gcs_streamer".into(),
            &alias,
        )
        .unwrap();

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

        assert_eq!(1, sink.upload_client.running_uploads().len());
        assert_eq!(1, sink.upload_client.count());

        // make the client fail requests
        debug!("INJECT FAILURE");
        sink.upload_client.inject_failure(true);

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
        assert_eq!(1, sink.upload_client.count());
        assert_eq!(1, sink.upload_client.running_uploads().len());
        assert_eq!(0, sink.upload_client.deleted_uploads().len());

        sink.upload_client.inject_failure(false);
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

        assert_eq!(1, sink.upload_client.running_uploads().len());
        assert_eq!(2, sink.upload_client.count());
        assert_eq!(1, sink.upload_client.deleted_uploads().len());
        assert_eq!(0, sink.upload_client.finished_uploads().len());

        let reply = reply_rx.try_recv();
        if let Ok(AsyncSinkReply::Fail(cf)) = reply {
            let id = cf.event_id();
            assert!(id.is_tracking(&id1));
            assert!(id.is_tracking(&id2));
            assert!(id.is_tracking(&id3));
        } else {
            assert!(false, "Expected a fail, got {reply:?}");
        }

        sink.on_stop(&context).await?;

        assert_eq!(0, sink.upload_client.running_uploads().len());
        assert_eq!(2, sink.upload_client.count());
        assert_eq!(1, sink.upload_client.deleted_uploads().len());
        assert_eq!(1, sink.upload_client.finished_uploads().len());
        Ok(())
    }
}
