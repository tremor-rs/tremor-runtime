// Copyright 2022-2024, The Tremor Team
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
#![deny(warnings)]
#![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic,
    clippy::mod_module_files
)]

//! Tremor object storage helpers
use log::{debug, error, warn};
use serde::Deserialize;
use tremor_connectors::sink::prelude::*;
use tremor_system::event::DEFAULT_STREAM_ID;
use tremor_value::prelude::*;

/// mode of operation for object storage connectors
#[derive(Debug, Default, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
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

/// Object id for object storage
#[derive(Hash, PartialEq, Eq, Debug, Clone)]
pub struct ObjectId {
    bucket: String,
    name: String,
}

impl ObjectId {
    /// Create a new object id
    pub fn new(bucket: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            bucket: bucket.into(),
        }
    }
    /// Returns the bucket name
    #[must_use]
    pub fn bucket(&self) -> &str {
        self.bucket.as_str()
    }

    /// Returns the object name
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.bucket, self.name)
    }
}

const NAME: &str = "name";
const BUCKET: &str = "bucket";

/// Error type for object storage operations
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error when metadata is invalid or missing
    #[error("Metadata `${0}.{1}` is invalid or missing.")]
    MetadataError(String, &'static str),
    /// Error when no upload is in progress
    #[error("No upload in progress")]
    NoUpload,
}

/// Generic trait for common object storage operations
pub trait Common {
    /// Returns the type of the connector
    fn connector_type(&self) -> &str;
    /// Returns the default bucket for the connector
    fn default_bucket(&self) -> Option<&String>;

    /// Returns the bucket name from the metadata
    /// # Errors
    /// Fails if the bucket name is missing or not a string
    fn get_bucket_name(&self, meta: Option<&Value>) -> Result<String, Error> {
        let res = match (meta.get(BUCKET), self.default_bucket()) {
            (Some(meta_bucket), _) => meta_bucket
                .as_str()
                .ok_or_else(|| Error::MetadataError(self.connector_type().to_string(), BUCKET)),
            (None, Some(default_bucket)) => Ok(default_bucket.as_str()),
            (None, None) => Err(Error::MetadataError(
                self.connector_type().to_string(),
                BUCKET,
            )),
        };

        res.map(ToString::to_string)
    }
    /// Returns the object id from the metadata
    /// # Errors
    /// Fails if the object id is missing or not a string or if the bucket name is missing or not a string
    fn get_object_id(&self, meta: Option<&Value<'_>>) -> Result<ObjectId, Error> {
        let name = meta
            .get(NAME)
            .ok_or_else(|| Error::MetadataError(self.connector_type().to_string(), NAME))?
            .as_str()
            .ok_or_else(|| Error::MetadataError(self.connector_type().to_string(), NAME))?;
        let bucket = self.get_bucket_name(meta)?;
        Ok(ObjectId::new(bucket, name))
    }
}

/// A buffer part that is part of a larger buffer used to allow for a sliding buffer over data that
/// prevents the need to move data around in memory.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BufferPart {
    data: Vec<u8>,
    start: usize,
}

impl BufferPart {
    /// Create a new buffer part
    #[must_use]
    pub fn new(data: Vec<u8>, start: usize) -> Self {
        Self { data, start }
    }
    /// Returns the length of the buffer part
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }
    /// Returns true if the buffer part is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Returns the start index of the buffer part
    #[must_use]
    pub fn start(&self) -> usize {
        self.start
    }
    /// Returns the end index of the buffer part
    #[must_use]
    pub fn end(&self) -> usize {
        self.start + self.data.len()
    }

    /// Returns the data of the buffer part
    #[must_use]
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

/// Trait for objects that can be uploaded to object storage
pub trait Upload {
    /// Returns the object id of the upload
    fn object_id(&self) -> &ObjectId;
    /// Returns true if the upload is failed
    fn is_failed(&self) -> bool;
    /// Marks the upload as failed
    fn mark_as_failed(&mut self);
    /// Tracks an event for the upload
    fn track(&mut self, event: &Event);
    /// The stream id of the upload
    fn stream_id(&self) -> u64 {
        DEFAULT_STREAM_ID
    }
}

/// Trait for objects that can be used as a buffer for object storage uploads
pub trait Buffer {
    /// Create a new buffer with the given size
    fn new(size: usize) -> Self;

    /// Write data to the buffer
    fn write(&mut self, data: Vec<u8>);

    /// Read the current block of data from the buffer
    fn read_current_block(&mut self) -> Option<BufferPart>;

    /// Mark the buffer as done until the given index
    /// # Errors
    /// Fails if the index is out of bounds
    fn mark_done_until(&mut self, idx: usize) -> anyhow::Result<()>;

    /// reset the buffer and return the final part
    fn reset(&mut self) -> BufferPart;
}

/// Trait for structs that can be used as a sink for object storage
#[async_trait::async_trait]
pub trait SinkImpl<U>: Common
where
    U: Upload,
{
    /// Returns the size of the buffer
    fn buffer_size(&self) -> usize;

    /// Connect to the object storage
    /// # Errors
    /// Fails if the connection fails
    async fn connect(&mut self, ctx: &SinkContext) -> anyhow::Result<()>;

    /// Check if a bucket exists
    /// # Errors
    /// Fails if the state of the bucket cannot be determined
    async fn bucket_exists(&mut self, bucket: &str) -> anyhow::Result<bool>;

    /// Start an upload
    /// # Errors
    /// Fails if the upload cannot be started
    async fn start_upload(
        &mut self,
        object_id: &ObjectId,
        event: &Event,
        ctx: &SinkContext,
    ) -> anyhow::Result<U>;

    /// Upload data to a started upload
    /// # Errors
    /// Fails if the data cannot be uploaded
    async fn upload_data(
        &mut self,
        data: BufferPart,
        upload: &mut U,
        ctx: &SinkContext,
    ) -> anyhow::Result<usize>;

    /// Finish a started upload
    /// # Errors
    /// Fails if the upload cannot be finished
    async fn finish_upload(
        &mut self,
        upload: U,
        part: BufferPart,
        ctx: &SinkContext,
    ) -> anyhow::Result<()>;

    /// Marsk a started upload as failed
    /// # Errors
    /// Fails if the upload cannot be marked as failed
    async fn fail_upload(&mut self, upload: U, ctx: &SinkContext) -> anyhow::Result<()>;
}

/// Trait for objects that can be used as a sink for object storage
pub struct ConsistentSink<Impl, U, B>
where
    B: Buffer,
    U: Upload,
    Impl: SinkImpl<U>,
{
    sink_impl: Impl,
    current_upload: Option<U>,
    buffers: B,
}

impl<Impl, U, B> ConsistentSink<Impl, U, B>
where
    B: Buffer,
    U: Upload,
    Impl: SinkImpl<U>,
{
    /// Create a new consistent sink
    #[must_use]
    pub fn new(sink_impl: Impl) -> Self {
        let buffers = Buffer::new(sink_impl.buffer_size());
        Self {
            sink_impl,
            current_upload: None,
            buffers,
        }
    }

    /// Returns a mutable reference to the current sink
    #[must_use]
    pub fn sink_impl_mut(&mut self) -> &mut Impl {
        &mut self.sink_impl
    }
}

#[async_trait::async_trait]
impl<Impl, U, B> Sink for ConsistentSink<Impl, U, B>
where
    B: Buffer + Send + Sync,
    U: Upload + Send + Sync,
    Impl: SinkImpl<U> + Send + Sync,
{
    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> anyhow::Result<bool> {
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
            let r = self.sink_impl.fail_upload(current_upload, ctx).await;
            ctx.swallow_err(r, "Error failing previous upload");
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
    ) -> anyhow::Result<SinkReply> {
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

    async fn finalize(
        &mut self,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
    ) -> anyhow::Result<()> {
        // Commit the final upload.
        self.fail_or_finish_upload(ctx, serializer).await?;
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &SinkContext) -> anyhow::Result<()> {
        Ok(())
    }

    fn asynchronous(&self) -> bool {
        false
    }

    fn auto_ack(&self) -> bool {
        false
    }
}

impl<Impl, U, B> ConsistentSink<Impl, U, B>
where
    B: Buffer,
    U: Upload,
    Impl: SinkImpl<U>,
{
    async fn on_event_inner(
        &mut self,
        event: &Event,
        serializer: &mut EventSerializer,
        ctx: &SinkContext,
    ) -> anyhow::Result<()> {
        let mut event_tracked = false;

        for (value, meta) in event.value_meta_iter() {
            let sink_meta = ctx.extract_meta(meta);
            let new_object_id: ObjectId = self.sink_impl.get_object_id(sink_meta)?;

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
                    self.fail_or_finish_upload(ctx, serializer).await?;

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
            let serialized_data = serializer
                .serialize_for_stream(
                    value,
                    meta,
                    event.ingest_ns,
                    self.current_upload
                        .as_ref()
                        .ok_or(Error::NoUpload)?
                        .stream_id(),
                )
                .await?;
            for item in serialized_data {
                self.buffers.write(item);
                // upload some data if necessary
                while let Some(data) = self.buffers.read_current_block() {
                    let done_until = self
                        .sink_impl
                        .upload_data(
                            data,
                            self.current_upload.as_mut().ok_or(Error::NoUpload)?,
                            ctx,
                        )
                        .await?;
                    self.buffers.mark_done_until(done_until)?;
                }
            }
        }
        Ok(())
    }

    async fn fail_or_finish_upload(
        &mut self,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
    ) -> anyhow::Result<()> {
        if let Some(upload) = self.current_upload.take() {
            if upload.is_failed() {
                self.sink_impl.fail_upload(upload, ctx).await?;
            } else {
                // make sure we send the data left in the serializer
                for data in serializer.finish_stream(upload.stream_id())? {
                    self.buffers.write(data);
                }
                while let Some(data) = self.buffers.read_current_block() {
                    let done_until = self
                        .sink_impl
                        .upload_data(
                            data,
                            self.current_upload.as_mut().ok_or(Error::NoUpload)?,
                            ctx,
                        )
                        .await?;
                    self.buffers.mark_done_until(done_until)?;
                }

                let final_part = self.buffers.reset();

                // clean out the buffer
                self.sink_impl
                    .finish_upload(upload, final_part, ctx)
                    .await?;
            }
        }
        Ok(())
    }
}
/// A sink that acks all events immediately and does not guarantee delivery
pub struct YoloSink<Impl, U, B>
where
    U: Upload,
    B: Buffer,
    Impl: SinkImpl<U>,
{
    pub(crate) sink_impl: Impl,
    current_upload: Option<U>,
    buffer: B,
}

impl<Impl, U, B> YoloSink<Impl, U, B>
where
    U: Upload + Send,
    B: Buffer + Send,
    Impl: SinkImpl<U> + Send,
{
    /// Create a new yolo sink
    #[must_use]
    pub fn new(sink_impl: Impl) -> Self {
        let buffer = B::new(sink_impl.buffer_size());
        Self {
            sink_impl,
            current_upload: None,
            buffer,
        }
    }

    /// Returns a mutable reference to the current sink
    #[must_use]
    pub fn sink_impl_mut(&mut self) -> &mut Impl {
        &mut self.sink_impl
    }
    async fn fail_or_finish_upload(
        &mut self,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
    ) -> anyhow::Result<()> {
        if let Some(u) = self.current_upload.as_mut() {
            for data in serializer.finish_stream(u.stream_id())? {
                self.buffer.write(data);
            }
        }

        self.upload_buffer_parts(ctx).await?;

        if let Some(upload) = self.current_upload.take() {
            if upload.is_failed() {
                return self.sink_impl.fail_upload(upload, ctx).await;
            }

            // make sure we send the data left in the serializer
            let final_part = self.buffer.reset();

            // clean out the buffer
            self.sink_impl
                .finish_upload(upload, final_part, ctx)
                .await?;
        }
        Ok(())
    }
    async fn upload_buffer_parts(&mut self, ctx: &SinkContext) -> anyhow::Result<()> {
        let upload = self.current_upload.as_mut().ok_or(Error::NoUpload)?;
        while let Some(data) = self.buffer.read_current_block() {
            let start = data.start;
            let end = data.start + data.len();
            let done_until = ctx.bail_err(
                self.sink_impl.upload_data(data, upload, ctx).await,
                &format!(
                    "Error uploading data for bytes {start}-{end} for {}",
                    upload.object_id()
                ),
            )?;
            // if this fails, we corrupted our internal state somehow
            self.buffer.mark_done_until(done_until)?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<Impl, U, B> Sink for YoloSink<Impl, U, B>
where
    U: Upload + Send + Sync,
    B: Buffer + Send + Sync,
    Impl: SinkImpl<U> + Send + Sync,
{
    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> anyhow::Result<bool> {
        // clear out the previous upload
        self.current_upload = None;
        self.buffer = B::new(self.sink_impl.buffer_size());
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
    ) -> anyhow::Result<SinkReply> {
        for (value, meta) in event.value_meta_iter() {
            let sink_meta = ctx.extract_meta(meta);
            let object_id = self.sink_impl.get_object_id(sink_meta)?;

            if let Some(current_upload) = self.current_upload.as_mut() {
                if object_id != *current_upload.object_id() {
                    // if finishing the previous one failed, we continue anyways
                    let r = self.fail_or_finish_upload(ctx, serializer).await;
                    ctx.swallow_err(r, "Error finishing previous upload");
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
            let serialized_data = serializer.serialize(value, meta, event.ingest_ns).await?;
            for item in serialized_data {
                self.buffer.write(item);
                let r = self.upload_buffer_parts(ctx).await;
                ctx.swallow_err(r, &format!("Error uploading chunk for {object_id}"));
            }
        }
        Ok(SinkReply::ack_or_none(event.transactional))
    }

    async fn finalize(
        &mut self,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
    ) -> anyhow::Result<()> {
        // Commit the final upload.
        self.fail_or_finish_upload(ctx, serializer).await?;
        Ok(())
    }
    async fn on_stop(&mut self, ctx: &SinkContext) -> anyhow::Result<()> {
        if let Some(current_upload) = self.current_upload.take() {
            let final_part = self.buffer.reset();
            let r = self
                .sink_impl
                .finish_upload(current_upload, final_part, ctx)
                .await;
            ctx.swallow_err(r, "Error finishing upload on stop");
        }
        Ok(())
    }

    fn auto_ack(&self) -> bool {
        false // we always ack
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn mode_display() {
        assert_eq!(Mode::Yolo.to_string(), "yolo");
        assert_eq!(Mode::Consistent.to_string(), "consistent");
    }
}
