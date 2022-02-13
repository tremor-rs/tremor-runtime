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
use crate::connectors::prelude::*;
use crate::Event;
use async_std::channel;
use std::{env, mem};
use value_trait::ValueAccess;

use aws_sdk_s3 as s3;
use aws_types::{credentials::Credentials, region::Region};
use s3::model::{CompletedMultipartUpload, CompletedPart};
use s3::Client as S3Client;
use s3::Endpoint;

const FIVEMBS: usize = 5 * 1024 * 1024 + 100; // Some extra bytes to keep aws happy.

struct S3Connector {
    client_sender: Option<channel::Sender<S3Client>>,
    config: S3Config,
    endpoint: Option<http::Uri>,
}

struct S3Sink {
    client_receiver: channel::Receiver<S3Client>,
    bucket: String,
    client: Option<S3Client>,
    buffer: Vec<u8>,
    current_key: String,

    // bookeeping for multipart uploads.
    upload_id: String,
    part_number: i32,
    min_part_size: usize,
    parts: Vec<CompletedPart>,
}

#[derive(Deserialize, Debug, Default)]
pub struct S3Config {
    #[serde(default = "S3Config::default_access_key_id")]
    aws_access_key_id: String,
    #[serde(default = "S3Config::default_secret_token")]
    aws_secret_access_key: String,
    #[serde(default = "S3Config::default_aws_region")]
    aws_region: String,

    bucket: String,
    #[serde(default = "S3Config::fivembs")]
    min_part_size: usize,
    endpoint: Option<String>,
}

// Defaults for the config.
impl S3Config {
    fn fivembs() -> usize {
        FIVEMBS
    }

    fn default_access_key_id() -> String {
        "AWS_ACCESS_KEY_ID".to_string()
    }

    fn default_secret_token() -> String {
        "AWS_SECRET_ACCESS_KEY".to_string()
    }

    fn default_aws_region() -> String {
        "AWS_REGION".to_string()
    }
}

impl ConfigImpl for S3Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "s3".into()
    }

    async fn from_config(
        &self,
        id: &str,
        raw_config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(config) = raw_config {
            let mut config = S3Config::new(config)?;

            // Fetch the secrets from the env.
            config.aws_secret_access_key = env::var(config.aws_secret_access_key)?;
            config.aws_access_key_id = env::var(config.aws_access_key_id)?;
            config.aws_region = env::var(config.aws_region)?;

            // Maintain the minimum size of 5 MBs.
            if config.min_part_size < FIVEMBS {
                config.min_part_size = FIVEMBS;
            }

            // Check the validity of given url
            let endpoint = if let Some(url) = &config.endpoint {
                let url_parsed = url.parse::<http::Uri>()?;
                Some(url_parsed)
            } else {
                None
            };

            Ok(Box::new(S3Connector {
                client_sender: None,
                config,
                endpoint,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(id.to_string()).into())
        }
    }
}

/// Meta data of an event. convience struct for feature expansion.
struct S3Meta<'a, 'value> {
    meta: Option<&'a Value<'value>>,
}

impl<'a, 'value> S3Meta<'a, 'value> {
    fn new(meta: &'a Value<'value>) -> Self {
        Self {
            meta: if let Some(s3_meta) = meta.get("s3") {
                Some(s3_meta)
            } else {
                None
            },
        }
    }

    fn get_object_key(&self) -> Option<&str> {
        self.meta.get_str("key")
    }
}

#[async_trait::async_trait]
impl Sink for S3Sink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        if self.client.is_none() {
            if let Ok(new_client) = self.client_receiver.recv().await {
                self.client = Some(new_client);
            }
        } else if let Ok(new_client) = self.client_receiver.try_recv() {
            self.client = Some(new_client);
        }

        let ingest_id = event.ingest_ns;

        for (event, meta) in event.value_meta_iter() {
            // Handle no key in meta.

            let s3_meta = S3Meta::new(meta);

            let object_key = match s3_meta.get_object_key().map(ToString::to_string) {
                Some(key) => key,
                None => {
                    self.current_key.clear();
                    error!("{}: missing key meta data in event", &ctx);
                    return Ok(SinkReply::FAIL);
                }
            };

            if object_key != self.current_key {
                // Complete the uploads for previous key.
                self.set_new_key(object_key, ctx).await?;
            }

            // Handle the aggregation.
            for data in serializer.serialize(event, ingest_id)?.into_iter() {
                self.buffer.extend(data);
                if self.buffer.len() >= self.min_part_size {
                    self.upload_part(ctx).await?;
                }
            }
        }
        Ok(SinkReply::ACK)
    }

    // Required later
    async fn on_signal(
        &mut self,
        _signal: Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
    ) -> Result<SinkReply> {
        // Check if a new client is required or not.
        if let Ok(new_client) = self.client_receiver.try_recv() {
            self.client = Some(new_client);
        }
        Ok(SinkReply::default())
    }

    async fn on_stop(&mut self, ctx: &SinkContext) -> Result<()> {
        // Commit the final upload.
        self.complete_multipart(ctx).await?;
        Ok(())
    }

    fn asynchronous(&self) -> bool {
        true
    }

    fn auto_ack(&self) -> bool {
        false
    }
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
        let (client_sender, client_receiver) = channel::bounded(2);

        // set the sender and receiver for a new client
        self.client_sender = Some(client_sender);

        let s3_sink = S3Sink {
            client_receiver,
            bucket: self.config.bucket.clone(),
            client: None,
            buffer: Vec::with_capacity(self.config.min_part_size),
            current_key: "".to_owned(),
            parts: Vec::new(),
            upload_id: "".to_owned(),
            part_number: 0,
            min_part_size: self.config.min_part_size,
        };

        let addr = builder.spawn(s3_sink, sink_context)?;
        Ok(Some(addr))
    }

    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        // Create the client and establish the connection.
        let s3_config = s3::config::Config::builder()
            .credentials_provider(Credentials::new(
                self.config.aws_access_key_id.clone(),
                self.config.aws_secret_access_key.clone(),
                None,
                None,
                "Environment",
            ))
            .region(Region::new(self.config.aws_region.clone()));

        let s3_config = match &self.endpoint {
            Some(uri) => s3_config.endpoint_resolver(Endpoint::immutable(uri.clone())),
            None => (s3_config),
        };

        let client = S3Client::from_conf(s3_config.build());

        // Check for the existence of the bucket.
        client
            .head_bucket()
            .bucket(self.config.bucket.clone())
            .send()
            .await?;

        // Send the client to the sink.
        if let Some(client_sender) = &self.client_sender {
            client_sender.send(client).await?;
        }

        Ok(true)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}

impl S3Sink {
    fn get_client(&self) -> Result<&S3Client> {
        self.client
            .as_ref()
            .ok_or_else(|| ErrorKind::S3Error("no s3 client available".to_string()).into())
    }

    async fn set_new_key(&mut self, key: String, ctx: &SinkContext) -> Result<()> {
        // Finish the previous multipart upload if any.
        if self.current_key != "" {
            self.complete_multipart(ctx).await?;
        }

        if key != "" {
            self.initiate_multipart(key).await?;
        }
        // NOTE: The buffers are cleared when the stuff is commited.
        Ok(())
    }

    async fn initiate_multipart(&mut self, key: String) -> Result<()> {
        self.current_key = key;
        self.part_number = 0; // Reset to new sequence.

        let resp = self
            .get_client()?
            .create_multipart_upload()
            .bucket(self.bucket.clone())
            .key(self.current_key.clone())
            .send()
            .await?;

        self.upload_id = resp
            .upload_id
            .ok_or(ErrorKind::S3Error("upload id not found".to_string()))?;

        Ok(())
    }

    async fn upload_part(&mut self, ctx: &SinkContext) -> Result<()> {
        debug!(
            "{}: key: {} uploading part {}",
            &ctx,
            self.current_key.as_str(),
            self.part_number,
        );

        let mut buf = Vec::with_capacity(self.min_part_size);
        mem::swap(&mut buf, &mut self.buffer);
        self.part_number += 1;

        // Upload the part
        let resp = self
            .get_client()?
            .upload_part()
            .body(buf.into())
            .part_number(self.part_number)
            .upload_id(self.upload_id.clone())
            .bucket(self.bucket.clone())
            .key(self.current_key.clone())
            .send()
            .await?;

        let e_tag = resp.e_tag.ok_or(ErrorKind::S3Error(
            "response did not have e_tag".to_string(),
        ))?;

        // Insert into the list of parts to upload.
        self.parts.push(
            CompletedPart::builder()
                .e_tag(e_tag)
                .part_number(self.part_number)
                .build(),
        );
        Ok(())
    }

    async fn complete_multipart(&mut self, ctx: &SinkContext) -> Result<()> {
        // Upload the last part if any.
        if self.buffer.len() > 0 {
            self.upload_part(ctx).await?;
        }

        let completed_parts = mem::take(&mut self.parts);
        self.get_client()?
            .complete_multipart_upload()
            .bucket(self.bucket.clone())
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
        Ok(())
    }
}
