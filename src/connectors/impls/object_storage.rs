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

//! Common abstractions for object storage connectors
//!
//! Currently home of two kinds of sinks implementing different modes: `yolo` and `consistent` See docs on `Mode`.

use crate::connectors::prelude::*;
use crate::errors::{err_object_storage, Result};
use tremor_pipeline::Event;
use tremor_value::Value;
use value_trait::ValueAccess;

/// mode of operation for object storage connectors
#[derive(Debug, Default, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Mode {
    /// automatically ack all incoming events regardless
    /// of delivery success or not
    #[default]
    Yolo,
    /// only ack events when their upload was completely finished
    /// and it is completely visible in gcs.
    /// Whenever an upload fails for whatever reason the whole current upload
    /// and any further events for that upload are discarded
    /// until a new upload is started for another bucket-pair combination.
    ///
    /// This mode ensures consistent results on GCS, no file will ever be uploaded with wrong order or missing parts.
    Consistent,
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Mode::Yolo => "yolo",
                Mode::Consistent => "consistent",
            }
        )
    }
}

#[derive(Hash, PartialEq, Eq, Debug, Clone)]
pub(crate) struct ObjectId {
    bucket: String,
    name: String,
}

impl ObjectId {
    pub(super) fn new(bucket: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            bucket: bucket.into(),
        }
    }

    pub(super) fn bucket(&self) -> &str {
        self.bucket.as_str()
    }

    pub(super) fn name(&self) -> &str {
        self.name.as_str()
    }
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.bucket, self.name)
    }
}

pub(crate) const NAME: &str = "name";
pub(crate) const BUCKET: &str = "bucket";

pub(crate) trait ObjectStorageCommon {
    fn connector_type(&self) -> &str;
    fn default_bucket(&self) -> Option<&String>;

    fn get_bucket_name(&self, meta: Option<&Value>) -> Result<String> {
        let res = match (meta.get(BUCKET), self.default_bucket()) {
            (Some(meta_bucket), _) => meta_bucket.as_str().ok_or_else(|| {
                err_object_storage(format!(
                    "Metadata `${}.{BUCKET}` is not a string.",
                    self.connector_type()
                ))
            }),
            (None, Some(default_bucket)) => Ok(default_bucket.as_str()),
            (None, None) => Err(err_object_storage(format!(
                "Metadata `${}.{BUCKET}` missing",
                self.connector_type()
            ))),
        };

        res.map(ToString::to_string)
    }

    fn get_object_id(&self, meta: Option<&Value<'_>>) -> Result<ObjectId> {
        let name = meta
            .get(NAME)
            .ok_or_else(|| {
                err_object_storage(format!(
                    "`${}.{NAME}` metadata is missing",
                    self.connector_type()
                ))
            })?
            .as_str()
            .ok_or_else(|| {
                err_object_storage(format!(
                    "`${}.{NAME}` metadata is not a string",
                    self.connector_type()
                ))
            })?;
        let bucket = self.get_bucket_name(meta)?;
        Ok(ObjectId::new(bucket, name))
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct BufferPart {
    pub(crate) data: Vec<u8>,
    pub(crate) start: usize,
}

impl BufferPart {
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(crate) fn start(&self) -> usize {
        self.start
    }
    pub(crate) fn end(&self) -> usize {
        self.start + self.data.len()
    }
}

pub(crate) trait ObjectStorageUpload {
    fn object_id(&self) -> &ObjectId;
    fn is_failed(&self) -> bool;
    fn mark_as_failed(&mut self);
    fn track(&mut self, event: &Event);
}

pub(crate) trait ObjectStorageBuffer {
    fn new(size: usize) -> Self;
    fn write(&mut self, data: Vec<u8>);
    fn read_current_block(&mut self) -> Option<BufferPart>;
    fn mark_done_until(&mut self, idx: usize) -> Result<()>;
    /// reset the buffer and return the final part
    fn reset(&mut self) -> BufferPart;
}

#[async_trait::async_trait]
pub(crate) trait ObjectStorageSinkImpl<Upload>: ObjectStorageCommon
where
    Upload: ObjectStorageUpload,
{
    fn buffer_size(&self) -> usize;
    async fn connect(&mut self, ctx: &SinkContext) -> Result<()>;
    async fn bucket_exists(&mut self, bucket: &str) -> Result<bool>;
    async fn start_upload(
        &mut self,
        object_id: &ObjectId,
        event: &Event,
        ctx: &SinkContext,
    ) -> Result<Upload>;
    async fn upload_data(
        &mut self,
        data: BufferPart,
        upload: &mut Upload,
        ctx: &SinkContext,
    ) -> Result<usize>;
    async fn finish_upload(
        &mut self,
        upload: Upload,
        part: BufferPart,
        ctx: &SinkContext,
    ) -> Result<()>;
    async fn fail_upload(&mut self, upload: Upload, ctx: &SinkContext) -> Result<()>;
}

pub(crate) struct ConsistentSink<Impl, Upload, Buffer>
where
    Buffer: ObjectStorageBuffer,
    Upload: ObjectStorageUpload,
    Impl: ObjectStorageSinkImpl<Upload>,
{
    pub(crate) sink_impl: Impl,
    current_upload: Option<Upload>,
    buffers: Buffer,
}

impl<Impl, Upload, Buffer> ConsistentSink<Impl, Upload, Buffer>
where
    Buffer: ObjectStorageBuffer,
    Upload: ObjectStorageUpload,
    Impl: ObjectStorageSinkImpl<Upload>,
{
    pub(crate) fn new(sink_impl: Impl) -> Self {
        let buffers = Buffer::new(sink_impl.buffer_size());
        Self {
            sink_impl,
            current_upload: None,
            buffers,
        }
    }
}

#[async_trait::async_trait]
impl<Impl, Upload, Buffer> Sink for ConsistentSink<Impl, Upload, Buffer>
where
    Buffer: ObjectStorageBuffer + Send + Sync,
    Upload: ObjectStorageUpload + Send + Sync,
    Impl: ObjectStorageSinkImpl<Upload> + Send + Sync,
{
    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        self.buffers = Buffer::new(self.sink_impl.buffer_size());
        self.sink_impl.connect(ctx).await?;

        // Check for the existence of the bucket.
        let bucket_exists = if let Some(bucket) = self.sink_impl.default_bucket() {
            self.sink_impl.bucket_exists(&bucket.clone()).await?
        } else {
            true
        };

        // fail the current upload, if any - we killed the buffer above so no way to upload that old shit anymore
        if let Some(current_upload) = self.current_upload.take() {
            ctx.swallow_err(
                self.sink_impl.fail_upload(current_upload, ctx).await,
                "Error failing previous upload",
            );
        }

        Ok(bucket_exists)
    }

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
            if let Some(current_upload) = self.current_upload.as_mut() {
                warn!(
                    "{ctx} Marking upload for s3://{} as failed",
                    current_upload.object_id()
                );
                current_upload.mark_as_failed();
                // we don't fail it yet, only when the next upload comes or we stop
            } else {
                // if we don't have an upload at this point we can also just error out and trigger a fail
                return Err(e);
            }
        }
        Ok(SinkReply::NONE)
    }

    async fn on_stop(&mut self, ctx: &SinkContext) -> Result<()> {
        // Commit the final upload.
        self.fail_or_finish_upload(ctx).await?;
        Ok(())
    }

    fn asynchronous(&self) -> bool {
        false
    }

    fn auto_ack(&self) -> bool {
        false
    }
}

impl<Impl, Upload, Buffer> ConsistentSink<Impl, Upload, Buffer>
where
    Buffer: ObjectStorageBuffer,
    Upload: ObjectStorageUpload,
    Impl: ObjectStorageSinkImpl<Upload>,
{
    async fn on_event_inner(
        &mut self,
        event: &Event,
        serializer: &mut EventSerializer,
        ctx: &SinkContext,
    ) -> Result<()> {
        let mut event_tracked = false;

        for (value, meta) in event.value_meta_iter() {
            let meta = ctx.extract_meta(meta);
            let new_object_id: ObjectId = self.sink_impl.get_object_id(meta)?;

            // ignore current event (or part of it if batched) if current upload with same file_id is failed
            if let Some(current_upload) = self.current_upload.as_mut() {
                if *current_upload.object_id() == new_object_id {
                    // track the event as being part of the current upload
                    if !event_tracked {
                        current_upload.track(event);
                        event_tracked = true;
                    }
                    if current_upload.is_failed() {
                        // same file_id for a failed upload ==> ignore and continue with the next event
                        debug!(
                            "{ctx} Ignore event for s3://{} as current upload is failed",
                            current_upload.object_id()
                        );
                        continue;
                    }
                } else {
                    // new file_id, current upload is failed  ==> FAIL & DELETE UPLOAD \
                    //                                                                 |--> START NEW UPLOAD
                    // new file_id, current upload not failed ==> FINISH UPLOAD ______/
                    self.fail_or_finish_upload(ctx).await?;

                    self.current_upload = Some(
                        self.sink_impl
                            .start_upload(&new_object_id, event, ctx)
                            .await?,
                    );
                }
            } else {
                // no current upload available ==> START UPLOAD
                self.current_upload = Some(
                    self.sink_impl
                        .start_upload(&new_object_id, event, ctx)
                        .await?,
                );
            };

            // At this point we defo have a healthy upload
            // accumulate event payload for the current upload
            let serialized_data = serializer.serialize(value, event.ingest_ns)?;
            for item in serialized_data {
                self.buffers.write(item);
                // upload some data if necessary
                while let Some(data) = self.buffers.read_current_block() {
                    let done_until = self
                        .sink_impl
                        .upload_data(
                            data,
                            self.current_upload
                                .as_mut()
                                .ok_or_else(|| err_object_storage("No upload available"))?,
                            ctx,
                        )
                        .await?;
                    self.buffers.mark_done_until(done_until)?;
                }
            }
        }
        Ok(())
    }

    async fn fail_or_finish_upload(&mut self, ctx: &SinkContext) -> Result<()> {
        if let Some(upload) = self.current_upload.take() {
            let final_part = self.buffers.reset();
            if upload.is_failed() {
                self.sink_impl.fail_upload(upload, ctx).await?;
            } else {
                // clean out the buffer
                self.sink_impl
                    .finish_upload(upload, final_part, ctx)
                    .await?;
            }
        }
        Ok(())
    }
}
pub(crate) struct YoloSink<Impl, Upload, Buffer>
where
    Impl: ObjectStorageSinkImpl<Upload>,
    Upload: ObjectStorageUpload,
    Buffer: ObjectStorageBuffer,
{
    pub(crate) sink_impl: Impl,
    current_upload: Option<Upload>,
    buffer: Buffer,
}

impl<Impl, Upload, Buffer> YoloSink<Impl, Upload, Buffer>
where
    Impl: ObjectStorageSinkImpl<Upload> + Send,
    Upload: ObjectStorageUpload + Send,
    Buffer: ObjectStorageBuffer + Send,
{
    pub(crate) fn new(sink_impl: Impl) -> Self {
        let buffer = Buffer::new(sink_impl.buffer_size());
        Self {
            sink_impl,
            current_upload: None,
            buffer,
        }
    }
}

#[async_trait::async_trait]
impl<Impl, Upload, Buffer> Sink for YoloSink<Impl, Upload, Buffer>
where
    Impl: ObjectStorageSinkImpl<Upload> + Send + Sync,
    Upload: ObjectStorageUpload + Send + Sync,
    Buffer: ObjectStorageBuffer + Send + Sync,
{
    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        // clear out the previous upload
        self.current_upload = None;
        self.buffer = Buffer::new(self.sink_impl.buffer_size());
        self.sink_impl.connect(ctx).await?;

        // if we have a default bucket, check that it is accessible
        Ok(if let Some(bucket) = self.sink_impl.default_bucket() {
            self.sink_impl.bucket_exists(&bucket.clone()).await?
        } else {
            true
        })
    }

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
            let object_id = self.sink_impl.get_object_id(meta)?;

            if let Some(current_upload) = self.current_upload.as_mut() {
                if object_id != *current_upload.object_id() {
                    // if finishing the previous one failed, we continue anyways
                    if let Some(current_upload) = self.current_upload.take() {
                        let final_part = self.buffer.reset();
                        ctx.swallow_err(
                            self.sink_impl
                                .finish_upload(current_upload, final_part, ctx)
                                .await,
                            "Error finishing previous upload",
                        );
                    }
                }
            }
            if self.current_upload.is_none() {
                let upload = match self.sink_impl.start_upload(&object_id, &event, ctx).await {
                    Ok(upload) => upload,
                    Err(e) => {
                        // we gotta stop here, otherwise we add to no upload
                        error!("{ctx} Error starting upload for {object_id}: {e}");
                        break;
                    }
                };
                self.current_upload = Some(upload);
            }
            let serialized_data = serializer.serialize(value, event.ingest_ns)?;
            for item in serialized_data {
                self.buffer.write(item);
                while let Some(data) = self.buffer.read_current_block() {
                    let start = data.start;
                    let end = data.start + data.len();
                    if let Ok(done_until) = ctx.bail_err(
                        self.sink_impl
                            .upload_data(
                                data,
                                self.current_upload
                                    .as_mut()
                                    .ok_or_else(|| err_object_storage("No upload available"))?,
                                ctx,
                            )
                            .await,
                        &format!(
                            "Error uploading data for bytes {}-{} for {}",
                            start, end, object_id
                        ),
                    ) {
                        // if this fails, we corrupted our internal state somehow
                        self.buffer.mark_done_until(done_until)?;
                    } else {
                        // stop attempting more writes on error
                        break;
                    }
                }
            }
        }
        Ok(SinkReply::ack_or_none(event.transactional))
    }

    async fn on_stop(&mut self, ctx: &SinkContext) -> Result<()> {
        if let Some(current_upload) = self.current_upload.take() {
            let final_part = self.buffer.reset();
            ctx.swallow_err(
                self.sink_impl
                    .finish_upload(current_upload, final_part, ctx)
                    .await,
                "Error finishing upload on stop",
            );
        }
        Ok(())
    }

    fn auto_ack(&self) -> bool {
        false // we always ack
    }
}
