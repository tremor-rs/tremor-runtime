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
use crate::Event;
use crate::{connectors::prelude::*, errors::err_connector_def};
use async_std::channel::Sender;
use std::mem;
use tremor_common::time::nanotime;
use tremor_pipeline::{EventId, OpMeta};
use value_trait::ValueAccess;

use super::auth;
use aws_sdk_s3 as s3;
use s3::model::{CompletedMultipartUpload, CompletedPart};
use s3::Client as S3Client;

pub(crate) const CONNECTOR_TYPE: &str = "s3_streamer";

const MORE_THEN_FIVEMBS: usize = 5 * 1024 * 1024 + 100; // Some extra bytes to keep aws happy.

#[derive(Deserialize, Debug, Default, Clone)]
pub(crate) struct S3Config {
    aws_region: Option<String>,
    url: Option<Url<HttpsDefaults>>,
    bucket: String,

    #[serde(default = "S3Config::fivembs")]
    min_part_size: usize,
}

// Defaults for the config.
impl S3Config {
    fn fivembs() -> usize {
        MORE_THEN_FIVEMBS
    }
}

impl ConfigImpl for S3Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

impl Builder {
    const PART_SIZE: &'static str = "S3 doesn't allow `min_part_size` smaller than 5MB.";
}

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
        let config = S3Config::new(config)?;

        // Maintain the minimum size of 5 MBs.
        if config.min_part_size < MORE_THEN_FIVEMBS {
            return Err(err_connector_def(id, Self::PART_SIZE));
        }
        Ok(Box::new(S3Connector { config }))
    }
}

struct S3Connector {
    config: S3Config,
}

#[async_trait::async_trait]
impl Connector for S3Connector {
    /// Currently no source
    async fn create_source(
        &mut self,
        _source_context: SourceContext,
        _builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        Ok(None)
    }

    /// Stream the events to the bucket
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let s3_sink = S3Sink::new(self.config.clone(), builder.reply_tx());
        let addr = builder.spawn(s3_sink, sink_context)?;
        Ok(Some(addr))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

struct S3Sink {
    config: S3Config,
    client: Option<S3Client>,
    buffer: Vec<u8>,
    /// an empty string is not a valid s3 key, so we encode an unset key like this.
    /// When this is empty, there is no upload running at the moment.
    current_key: String,
    /// tracking the ids for all accumulated events
    current_event_id: EventId,
    /// tracking the traversed operators for each accumulated event for correct sink-reply handling
    current_op_meta: OpMeta,
    /// tracking the transactional status of the accumulated events
    /// if any one of them is transactional, we send an ack for all
    current_transactional: bool,

    // bookkeeping for multipart uploads.
    upload_id: String,
    part_number: i32,
    min_part_size: usize,
    parts: Vec<CompletedPart>,
    reply_tx: Sender<AsyncSinkReply>,
}

impl S3Sink {
    fn new(config: S3Config, reply_tx: Sender<AsyncSinkReply>) -> Self {
        let min_part_size = config.min_part_size;
        Self {
            config,
            client: None,
            buffer: Vec::with_capacity(min_part_size),
            current_key: String::from(""),
            current_event_id: EventId::default(),
            current_op_meta: OpMeta::default(),
            current_transactional: false,
            upload_id: "".to_owned(),
            part_number: 0,
            min_part_size,
            parts: Vec::new(),
            reply_tx,
        }
    }
}

#[async_trait::async_trait]
impl Sink for S3Sink {
    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let client =
            auth::get_client(self.config.aws_region.clone(), self.config.url.as_ref()).await?;

        // Check for the existence of the bucket.
        client
            .head_bucket()
            .bucket(self.config.bucket.clone())
            .send()
            .await
            .map_err(|e| {
                let bkt = &self.config.bucket;
                let msg = format!("Failed to access Bucket `{bkt}`: {e}");
                Error::from(ErrorKind::S3Error(msg))
            })?;

        self.client = Some(client);
        Ok(true)
    }
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let ingest_id = event.ingest_ns;
        let mut event_tracked = false;
        for (value, meta) in event.value_meta_iter() {
            let s3_meta = S3Meta::new(ctx.extract_meta(meta));

            let object_key = if let Some(key) = s3_meta.get_object_key().map(ToString::to_string) {
                key
            } else {
                // Handle no key in meta.
                error!("{ctx}: missing '${CONNECTOR_TYPE}.key' meta data in event");
                return Ok(SinkReply::FAIL);
            };

            if object_key != self.current_key {
                // we switched keys:
                // 1. finish the current upload, if any
                // 2. initiate a new upload and start trackign the new event
                self.prepare_new_multipart(object_key, &event, ctx).await?;
            } else if !event_tracked {
                // track event
                self.current_event_id.track(&event.id);
                if !event.op_meta.is_empty() {
                    self.current_op_meta.merge(event.op_meta.clone());
                }
                self.current_transactional |= event.transactional;
                event_tracked = true;
            }

            // Handle the aggregation.
            for data in serializer.serialize(value, ingest_id)? {
                self.buffer.extend(data);
                if self.buffer.len() >= self.min_part_size {
                    self.upload_part(ctx).await?;
                }
            }
        }
        Ok(SinkReply::NONE) //acks for completed uploads are sent when the upload is completed
    }

    async fn on_stop(&mut self, ctx: &SinkContext) -> Result<()> {
        // Commit the final upload.
        self.complete_multipart(nanotime(), ctx).await?;
        Ok(())
    }

    fn asynchronous(&self) -> bool {
        false
    }

    fn auto_ack(&self) -> bool {
        false
    }
}

impl S3Sink {
    fn get_client(&self) -> Result<&S3Client> {
        self.client
            .as_ref()
            .ok_or_else(|| ErrorKind::S3Error("no s3 client available".to_string()).into())
    }

    async fn prepare_new_multipart(
        &mut self,
        key: String,
        event: &Event,
        ctx: &SinkContext,
    ) -> Result<()> {
        // Finish the previous multipart upload if any.
        if !self.current_key.is_empty() {
            self.complete_multipart(event.ingest_ns, ctx).await?;
        }

        if !key.is_empty() {
            self.initiate_multipart(key).await?;

            // start tracking the new event
            self.current_event_id = event.id.clone();
            self.current_op_meta = event.op_meta.clone();
            self.current_transactional = event.transactional;
        }
        // NOTE: The buffers are cleared when the stuff is committed.
        Ok(())
    }

    async fn initiate_multipart(&mut self, key: String) -> Result<()> {
        self.current_key = key;
        self.part_number = 0; // Reset to new sequence.

        let resp = self
            .get_client()?
            .create_multipart_upload()
            .bucket(self.config.bucket.clone())
            .key(self.current_key.clone())
            .send()
            .await?;

        self.upload_id = resp.upload_id.ok_or_else(|| {
            ErrorKind::S3Error(format!(
            "Failed to initiate multipart upload for key \"{}\": upload id not found in response.",
            &self.current_key
        ))
        })?;

        Ok(())
    }

    async fn upload_part(&mut self, ctx: &SinkContext) -> Result<()> {
        let mut buf = Vec::with_capacity(self.min_part_size);
        mem::swap(&mut buf, &mut self.buffer);
        self.part_number += 1; // the upload part number needs to be >= 1, so we increment before uploading

        debug!(
            "{ctx} key: {} uploading part {}",
            self.current_key, self.part_number,
        );

        // Upload the part
        let resp = self
            .get_client()?
            .upload_part()
            .body(buf.into())
            .part_number(self.part_number)
            .upload_id(self.upload_id.clone())
            .bucket(self.config.bucket.clone())
            .key(self.current_key.clone())
            .send()
            .await?;

        let mut completed = CompletedPart::builder().part_number(self.part_number);
        if let Some(e_tag) = resp.e_tag.as_ref() {
            completed = completed.e_tag(e_tag);
        }
        debug!(
            "{ctx} Key {} part {} uploaded.",
            self.current_key, self.part_number
        );
        // Insert into the list of completed parts
        self.parts.push(completed.build());
        Ok(())
    }

    async fn complete_multipart(&mut self, ingest_ns: u64, ctx: &SinkContext) -> Result<()> {
        // Upload the last part if any.
        if !self.buffer.is_empty() {
            self.upload_part(ctx).await?;
        }

        let completed_parts = mem::take(&mut self.parts);
        self.get_client()?
            .complete_multipart_upload()
            .bucket(self.config.bucket.clone())
            .upload_id(mem::take(&mut self.upload_id))
            .key(self.current_key.clone())
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            )
            .send()
            .await?;

        debug!(
            "{}: completed multipart upload for key: {}",
            &ctx, self.current_key
        );
        // send an ack for all the accumulated events in the finished upload
        if self.current_transactional {
            let data = ContraflowData::new(
                self.current_event_id.clone(),
                ingest_ns,
                self.current_op_meta.clone(),
            );
            // the duration of handling in the sink is a little bit meaningless here
            // as a) the actual duration from the first event to the actual finishing of the upload
            //       is horribly long, and shouldn ot be considered the actual event handling time
            //    b) It will vary a lot e.g. when an actual upload call is made
            let duration = 0;
            self.reply_tx
                .send(AsyncSinkReply::Ack(data, duration))
                .await?;
        }
        Ok(())
    }
}

// Meta data of an event. convience struct for feature expansion.
struct S3Meta<'a, 'value> {
    meta: Option<&'a Value<'value>>,
}

impl<'a, 'value> S3Meta<'a, 'value> {
    const KEY: &'static str = "key";

    fn new(meta: Option<&'a Value<'value>>) -> Self {
        Self { meta }
    }

    fn get_object_key(&self) -> Option<&str> {
        self.meta.get_str(Self::KEY)
    }
}
