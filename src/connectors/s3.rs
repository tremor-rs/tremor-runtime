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

use crate::connectors::prelude::*;
use crate::Event;
use async_std::channel;
use aws_sdk_s3 as s3;
use s3::model::{CompletedMultipartUpload, CompletedPart};
use std::{env, mem};
use value_trait::ValueAccess;

use s3::Client as S3Client;
struct S3Connector {
    client_sender: Option<channel::Sender<S3Client>>,
    config: S3Config,
}

struct S3Sink {
    client_receiver: channel::Receiver<S3Client>,
    bucket: String,
    client: Option<S3Client>,
    buffer: Vec<u8>,
    current_key: String,
    upload_id: String,
    part_number: i32,
    // bookeeping for parts.
    parts: Vec<CompletedPart>,
}

#[derive(Deserialize, Debug, Default)]
pub struct S3Config {
    aws_access_token: String,
    aws_secret_access_key: String,
    aws_region: String,
    max_clients: Option<usize>,
    bucket: String,
    min_part_size: i32,
}

impl ConfigImpl for S3Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    async fn from_config(
        &self,
        _id: &TremorUrl,
        raw_config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(config) = raw_config {
            let mut config = S3Config::new(config)?;
            // Fetch the secrets.
            config.aws_secret_access_key = env::var(config.aws_secret_access_key)?;
            config.aws_access_token = env::var(config.aws_access_token)?;
            Ok(Box::new(S3Connector {
                client_sender: None,
                config,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(String::from("aws-s3")).into())
        }
    }
}

#[async_trait::async_trait]
impl Sink for S3Sink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        _ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        // todo!("Working on it!")

        if self.client.is_none() {
            if let Ok(new_client) = self.client_receiver.recv().await {
                self.client = Some(new_client);
            }
        }
        // FIXME: Check if a new client is required or not.
        else if let Ok(new_client) = self.client_receiver.try_recv() {
            self.client = Some(new_client);
        }

        let ingest_id = event.ingest_ns;

        for (event, meta) in event.value_meta_iter() {
            // What to do when meta-data is not present.
            // FIXME: ayyyyy
            // let operation = meta.get("operation").map(|value| value.to_string());

            let object_key = meta.get("key").map(|value| value.to_string());

            // Also handle aggreagtion here.
            // FIXME: what to do for multiple data??
            let data: Vec<u8> = serializer
                .serialize(event, ingest_id)?
                .into_iter()
                .flatten()
                .collect();

            // self.upload_object(object_key.unwrap(), data[0].clone())
            //     .await?;

            if object_key.as_ref().unwrap() != &self.current_key {
                // Complete the uploads for previous key.
                self.set_new_key(object_key.unwrap()).await?;
            }

            self.buffer.extend(data);

            // FIXME: Make a better aggregation logic.
            if self.buffer.len() > 5 * 1024 * 1024 {
                self.upload_part().await?;
            }

            // FIXME: Identify errors which indicate whether the client failed.
            // then notify.
        }

        Ok(SinkReply::default())
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

    async fn on_stop(&mut self, _ctx: &mut SinkContext) {
        // Commit the final upload.
        self.complete_multipart().await.unwrap();
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
    // Only runs once.
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        // FIXME: May be wise to increase channel size if you want to have multiple clients.
        let (client_sender, client_receiver) = channel::bounded(2);

        // set the sender and receiver for a new client
        self.client_sender = Some(client_sender);

        let s3_sink = S3Sink {
            client_receiver,
            bucket: self.config.bucket.clone(),
            client: None,
            buffer: Vec::new(),
            current_key: "".to_owned(),
            parts: Vec::new(),
            upload_id: "".to_owned(),
            part_number: 0,
        };

        let addr = builder.spawn(s3_sink, sink_context)?;
        Ok(Some(addr))
    }

    // Runs on need to connect/reconnect. (multiple times)
    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        // Create the client and establish the connection.
        let shared_config = aws_config::from_env()
            .credentials_provider(aws_types::Credentials::from_keys(
                self.config.aws_access_token.clone(),
                self.config.aws_secret_access_key.clone(),
                None,
            ))
            .region(aws_types::region::Region::new(
                self.config.aws_region.clone(),
            ))
            .load()
            .await;

        let client = S3Client::new(&shared_config);

        // Check for the existence of the bucket.
        let exist_bucket = client
            .head_bucket()
            .bucket(self.config.bucket.clone())
            .send()
            .await;

        if exist_bucket.is_err() {
            return Err(exist_bucket.unwrap_err().into());
        }

        // Send the client to the sink.
        self.client_sender.as_ref().unwrap().send(client).await?;

        Ok(true)
    }

    // Do argue for plain text..
    fn default_codec(&self) -> &str {
        "json"
    }
}

impl S3Sink {
    async fn set_new_key(&mut self, key: String) -> Result<()> {
        // Finish the previous multipart upload if any.
        if self.current_key != "" {
            self.complete_multipart().await?;
        }

        if key != "" {
            self.initiate_multipart(key).await?;
        }
        // NOTE: The buffers are cleared when the stuff is commited.
        Ok(())
    }

    // async fn upload_object(
    //     &mut self,
    //     key: String,
    //     object_bytes: Vec<u8>,
    // ) -> Result<s3::output::PutObjectOutput> {
    //     let resp = self
    //         .client
    //         .as_ref()
    //         .unwrap()
    //         .put_object()
    //         .bucket(self.bucket.clone())
    //         .key(key)
    //         .body(s3::ByteStream::from(object_bytes))
    //         .send()
    //         .await?;
    //     Ok(resp)
    // }

    async fn initiate_multipart(&mut self, key: String) -> Result<()> {
        self.current_key = key;
        self.part_number = 0; // Reset to new sequence.
        self.upload_id = self
            .client
            .as_ref()
            .unwrap()
            .create_multipart_upload()
            .bucket(self.bucket.clone())
            .key(self.current_key.clone())
            .send()
            .await
            .map(|resp| resp.upload_id.unwrap())?; // This unwrap is safe as we have a successful response.
        Ok(())
    }

    async fn upload_part(&mut self) -> Result<()> {
        let buf = mem::take(&mut self.buffer);
        self.part_number += 1;
        let resp = self
            .client
            .as_ref()
            .unwrap()
            .upload_part()
            .body(buf.into())
            .part_number(self.part_number)
            .upload_id(self.upload_id.clone())
            .bucket(self.bucket.clone())
            .key(self.current_key.clone())
            .send()
            .await?;

        // Insert into the list of parts to upload.
        self.parts.push(
            CompletedPart::builder()
                .e_tag(resp.e_tag.unwrap())
                .part_number(self.part_number)
                .build(),
        );
        Ok(())
    }

    async fn complete_multipart(&mut self) -> Result<()> {
        // Upload the last part if any.
        if !self.buffer.is_empty() {
            self.upload_part().await?;
        }

        let completed_parts = mem::take(&mut self.parts);
        self.client
            .as_ref()
            .unwrap()
            .complete_multipart_upload()
            .bucket(self.bucket.clone())
            .upload_id(mem::take(&mut self.upload_id))
            .key(mem::take(&mut self.current_key))
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            )
            .send()
            .await?;
        Ok(())
    }
}
