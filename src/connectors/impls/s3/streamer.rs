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

use crate::connectors::impls::{
    object_storage::{
        BufferPart, ObjectId, ObjectStorageBuffer, ObjectStorageCommon, ObjectStorageSinkImpl,
        ObjectStorageUpload,
    },
    s3::auth,
};
use crate::connectors::prelude::*;
use async_std::channel::Sender;
use tremor_common::time::nanotime;
use tremor_pipeline::{Event, EventId, OpMeta};

use aws_sdk_s3 as s3;
use s3::model::{CompletedMultipartUpload, CompletedPart};
use s3::Client as S3Client;

use crate::connectors::impls::object_storage::{ConsistentSink, Mode, YoloSink};

pub(crate) const CONNECTOR_TYPE: &str = "s3_streamer";

const MORE_THEN_FIVEMBS: usize = 5 * 1024 * 1024 + 100; // Some extra bytes to keep aws happy.

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    aws_region: Option<String>,
    url: Option<Url<HttpsDefaults>>,
    /// optional default bucket
    bucket: Option<String>,
    #[serde(default = "Default::default")]
    mode: Mode,

    #[serde(default = "Config::fivembs")]
    buffer_size: usize,
}

// Defaults for the config.
impl Config {
    fn fivembs() -> usize {
        MORE_THEN_FIVEMBS
    }

    fn normalize(&mut self, alias: &Alias) {
        if self.buffer_size < MORE_THEN_FIVEMBS {
            warn!("[Connector::{alias}] Setting `buffer_size` up to minimum of 5MB.");
            self.buffer_size = MORE_THEN_FIVEMBS;
        }
    }
}

impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        ConnectorType::from(CONNECTOR_TYPE)
    }

    async fn build_cfg(
        &self,
        id: &Alias,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let mut config = Config::new(config)?;
        config.normalize(id);
        Ok(Box::new(S3Connector { config }))
    }
}

struct S3Connector {
    config: Config,
}

#[async_trait::async_trait]
impl Connector for S3Connector {
    /// Stream the events to the bucket
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        match self.config.mode {
            Mode::Yolo => {
                let sink_impl = S3ObjectStorageSinkImpl::yolo(self.config.clone());
                let sink: YoloSink<S3ObjectStorageSinkImpl, S3Upload, S3Buffer> =
                    YoloSink::new(sink_impl);
                builder.spawn(sink, sink_context).map(Some)
            }
            Mode::Consistent => {
                let sink_impl =
                    S3ObjectStorageSinkImpl::consistent(self.config.clone(), builder.reply_tx());
                let sink: ConsistentSink<S3ObjectStorageSinkImpl, S3Upload, S3Buffer> =
                    ConsistentSink::new(sink_impl);
                builder.spawn(sink, sink_context).map(Some)
            }
        }
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

// TODO: Maybe: https://docs.rs/object_store/latest/object_store/ is the better abstraction ?
pub(super) struct S3ObjectStorageSinkImpl {
    config: Config,
    client: Option<S3Client>,
    reply_tx: Option<Sender<AsyncSinkReply>>,
}

impl ObjectStorageCommon for S3ObjectStorageSinkImpl {
    fn default_bucket(&self) -> Option<&String> {
        self.config.bucket.as_ref()
    }

    fn connector_type(&self) -> &str {
        CONNECTOR_TYPE
    }
}

impl S3ObjectStorageSinkImpl {
    pub(crate) fn yolo(config: Config) -> Self {
        Self {
            config,
            client: None,
            reply_tx: None,
        }
    }
    pub(crate) fn consistent(config: Config, reply_tx: Sender<AsyncSinkReply>) -> Self {
        Self {
            config,
            client: None,
            reply_tx: Some(reply_tx),
        }
    }

    fn get_client(&self) -> Result<&S3Client> {
        self.client
            .as_ref()
            .ok_or_else(|| ErrorKind::S3Error("no s3 client available".to_string()).into())
    }
}

pub(crate) struct S3Buffer {
    block_size: usize,
    data: Vec<u8>,
    cursor: usize,
}

impl ObjectStorageBuffer for S3Buffer {
    fn new(size: usize) -> Self {
        Self {
            block_size: size,
            data: Vec::with_capacity(size * 2),
            cursor: 0,
        }
    }

    fn write(&mut self, mut data: Vec<u8>) {
        self.data.append(&mut data);
    }

    fn read_current_block(&mut self) -> Option<BufferPart> {
        if self.data.len() >= self.block_size {
            let data = self.data.clone();
            self.cursor += data.len();
            self.data.clear();
            Some(BufferPart {
                data,
                start: self.cursor,
            })
        } else {
            None
        }
    }

    fn mark_done_until(&mut self, _idx: usize) -> Result<()> {
        // no-op
        Ok(())
    }

    fn reset(&mut self) -> BufferPart {
        let data = self.data.clone(); // we only clone up to len, not up to capacity
        let start = self.cursor;
        self.data.clear();
        self.cursor = 0;
        BufferPart { data, start }
    }
}

#[async_trait::async_trait]
impl ObjectStorageSinkImpl<S3Upload> for S3ObjectStorageSinkImpl {
    fn buffer_size(&self) -> usize {
        self.config.buffer_size
    }
    async fn connect(&mut self, _ctx: &SinkContext) -> Result<()> {
        self.client =
            Some(auth::get_client(self.config.aws_region.clone(), self.config.url.as_ref()).await?);
        Ok(())
    }

    async fn bucket_exists(&mut self, bucket: &str) -> Result<bool> {
        self.get_client()?
            .head_bucket()
            .bucket(bucket)
            .send()
            .await
            .map_err(|e| {
                let msg = format!("Failed to access Bucket `{bucket}`: {e}");
                Error::from(ErrorKind::S3Error(msg))
            })?;
        Ok(true)
    }

    async fn start_upload(
        &mut self,
        object_id: &ObjectId,
        event: &Event,
        _ctx: &SinkContext,
    ) -> Result<S3Upload> {
        let resp = self
            .get_client()?
            .create_multipart_upload()
            .bucket(object_id.bucket())
            .key(object_id.name())
            .send()
            .await?;

        //let upload = CurrentUpload::new(resp.)

        let upload_id = resp.upload_id.ok_or_else(|| {
            ErrorKind::S3Error(format!(
                "Failed to start upload for s3://{}: upload id not found in response.",
                &object_id
            ))
        })?;
        let upload = S3Upload::new(object_id.clone(), upload_id, event);

        Ok(upload)
    }
    async fn upload_data(
        &mut self,
        data: BufferPart,
        upload: &mut S3Upload,
        ctx: &SinkContext,
    ) -> Result<usize> {
        let end = data.end();
        upload.part_number += 1; // the upload part number needs to be >= 1, so we increment before uploading

        debug!(
            "{ctx} Uploading part {} for {}",
            upload.part_number,
            upload.object_id(),
        );

        // Upload the part
        let resp = self
            .get_client()?
            .upload_part()
            .body(data.data.into())
            .part_number(upload.part_number)
            .upload_id(upload.upload_id.clone())
            .bucket(upload.object_id().bucket())
            .key(upload.object_id().name())
            .send()
            .await?;

        let mut completed = CompletedPart::builder().part_number(upload.part_number);
        if let Some(e_tag) = resp.e_tag.as_ref() {
            completed = completed.e_tag(e_tag);
        }
        debug!(
            "{ctx} part {} uploaded for {}.",
            upload.part_number,
            upload.object_id()
        );
        // Insert into the list of completed parts
        upload.parts.push(completed.build());
        Ok(end)
    }

    async fn finish_upload(
        &mut self,
        mut upload: S3Upload,
        final_part: BufferPart,
        ctx: &SinkContext,
    ) -> Result<()> {
        debug_assert!(
            !upload.failed,
            "finish may only be called for non-failed uploads"
        );

        // Upload the last part if any.
        if !final_part.is_empty() {
            self.upload_data(final_part, &mut upload, ctx).await?;
        }
        let S3Upload {
            object_id,
            event_id,
            op_meta,
            transactional,
            upload_id,
            parts,
            ..
        } = upload;

        debug!("{ctx} Finishing upload {upload_id} for {object_id}");

        let res = self
            .get_client()?
            .complete_multipart_upload()
            .bucket(object_id.bucket())
            .upload_id(&upload_id)
            .key(object_id.name())
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await;

        // send an ack for all the accumulated events in the finished upload
        if let (Some(reply_tx), true) = (self.reply_tx.as_ref(), transactional) {
            let cf_data = ContraflowData::new(event_id, nanotime(), op_meta);
            let reply = if let Ok(out) = &res {
                if let Some(location) = out.location() {
                    debug!("{ctx} Finished upload {upload_id} for {location}");
                } else {
                    debug!("{ctx} Finished upload {upload_id} for {object_id}");
                }
                // the duration of handling in the sink is a little bit meaningless here
                // as a) the actual duration from the first event to the actual finishing of the upload
                //       is horribly long, and shouldn ot be considered the actual event handling time
                //    b) It will vary a lot e.g. when an actual upload call is made
                AsyncSinkReply::Ack(cf_data, 0)
            } else {
                AsyncSinkReply::Fail(cf_data)
            };
            ctx.swallow_err(
                reply_tx.send(reply).await,
                &format!("Error sending ack/fail for upload {upload_id} to {object_id}"),
            );
        }
        res?;
        Ok(())
    }

    async fn fail_upload(&mut self, upload: S3Upload, ctx: &SinkContext) -> Result<()> {
        let S3Upload {
            object_id,
            upload_id,
            event_id,
            op_meta,
            ..
        } = upload;
        if let (Some(reply_tx), true) = (self.reply_tx.as_ref(), upload.transactional) {
            ctx.swallow_err(
                reply_tx
                    .send(AsyncSinkReply::Fail(ContraflowData::new(
                        event_id,
                        nanotime(),
                        op_meta,
                    )))
                    .await,
                &format!("Error sending fail for upload {upload_id} for {object_id}"),
            );
        }
        ctx.swallow_err(
            self.get_client()?
                .abort_multipart_upload()
                .bucket(object_id.bucket())
                .key(object_id.name())
                .upload_id(&upload_id)
                .send()
                .await,
            &format!("Error aborting multipart upload {upload_id} for {object_id}"),
        );
        Ok(())
    }
}

pub(crate) struct S3Upload {
    object_id: ObjectId,
    /// tracking the ids for all accumulated events
    event_id: EventId,
    /// tracking the traversed operators for each accumulated event for correct sink-reply handling
    op_meta: OpMeta,

    /// tracking the transactional status of the accumulated events
    /// if any one of them is transactional, we send an ack for all
    transactional: bool,

    /// bookkeeping for multipart uploads.
    upload_id: String,
    part_number: i32,
    parts: Vec<CompletedPart>,
    /// whether this upload is marked as failed
    failed: bool,
}

impl S3Upload {
    fn new(object_id: ObjectId, upload_id: String, event: &Event) -> Self {
        Self {
            object_id,
            event_id: event.id.clone(),
            op_meta: event.op_meta.clone(),
            transactional: event.transactional,
            upload_id,
            part_number: 0,
            parts: Vec::with_capacity(8),
            failed: false,
        }
    }
}

impl ObjectStorageUpload for S3Upload {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tremor_value::literal;

    #[test]
    fn config_defaults() -> Result<()> {
        let config = literal!({});
        let res = Config::new(&config)?;
        assert!(res.aws_region.is_none());
        assert!(res.url.is_none());
        assert!(res.bucket.is_none());
        assert_eq!(Mode::Yolo, res.mode);
        assert_eq!(5_242_980, res.buffer_size);
        Ok(())
    }
}
