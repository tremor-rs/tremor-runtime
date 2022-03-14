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
use futures::stream::TryStreamExt;
use std::error::Error as StdError;

use async_std::channel::{self, Receiver, Sender};
use async_std::task::{self, JoinHandle};

use super::auth;
use aws_sdk_s3 as s3;
use s3::model::Object;
use s3::types::ByteStream;
use s3::Client as S3Client;

const MINCHUNKSIZE: i64 = 8 * 1024 * 1024; // 8 MBs

const CONNECTOR_TYPE: &str = "s3-reader";
const URL_SCHEME: &str = "tremor-s3";

#[derive(Deserialize, Debug, Default)]
pub struct S3SourceConfig {
    // if not provided here explicitly, the region is taken from environment variable or local AWS config
    // NOTE: S3 will fail if NO region could be found.
    aws_region: Option<String>,
    endpoint: Option<String>,
    bucket: String,

    /// prefix filter - if provided, it will fetch all keys with this prefix
    prefix: Option<String>,

    /// Sourcing field names and defaults from
    /// https://docs.aws.amazon.com/cli/latest/topic/s3-config.html
    #[serde(default = "S3SourceConfig::default_multipart_chunksize")]
    multipart_chunksize: i64,
    #[serde(default = "S3SourceConfig::default_multipart_threshold")]
    multipart_threshold: i64,

    #[serde(default = "S3SourceConfig::default_max_connections")]
    max_connections: usize,
}

struct KeyPayload {
    object_data: Object,
    stream: u64,
}

// Defaults for the config.
impl S3SourceConfig {
    fn default_multipart_chunksize() -> i64 {
        MINCHUNKSIZE
    }
    fn default_multipart_threshold() -> i64 {
        MINCHUNKSIZE
    }

    /// See <https://docs.aws.amazon.com/cli/latest/topic/s3-config.html#max-concurrent-requests>
    fn default_max_connections() -> usize {
        10
    }
}

impl ConfigImpl for S3SourceConfig {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        ConnectorType::from(CONNECTOR_TYPE)
    }

    async fn from_config(
        &self,
        id: &str,
        raw_config: &ConnectorConfig,
    ) -> Result<Box<dyn Connector>> {
        if let Some(config) = &raw_config.config {
            let config = S3SourceConfig::new(config)?;

            // FIXME: display a warning if chunksize lesser than some quantity
            Ok(Box::new(S3SourceConnector {
                handles: Vec::with_capacity(config.max_connections),
                config,
                tx: None,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(id.to_string()).into())
        }
    }
}

struct S3SourceConnector {
    config: S3SourceConfig,
    tx: Option<Sender<SourceReply>>,
    handles: Vec<JoinHandle<Result<()>>>,
}

#[async_trait::async_trait]
impl Connector for S3SourceConnector {
    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let (tx, rx) = channel::bounded(QSIZE.load(Ordering::Relaxed));
        let s3_source = ChannelSource::from_channel(tx.clone(), rx);

        self.tx = Some(tx);

        let addr = builder.spawn(s3_source, source_context)?;
        Ok(Some(addr))
    }

    async fn connect(&mut self, ctx: &ConnectorContext, _attemp: &Attempt) -> Result<bool> {
        // cancelling handles from previous connection, if any
        for handle in self.handles.drain(..) {
            handle.cancel().await;
        }
        let client = auth::get_client(
            self.config.aws_region.clone(),
            self.config.endpoint.as_ref(),
        )
        .await?;

        // Check the existence of the bucket.
        client
            .head_bucket()
            .bucket(self.config.bucket.clone())
            .send()
            .await
            .map_err(|e| {
                let msg = if let Some(err) = e.source() {
                    format!(
                        "Failed to access Bucket \"{}\": {}.",
                        &self.config.bucket, err
                    )
                } else {
                    format!("Failed to access Bucket \"{}\".", &self.config.bucket)
                };
                Error::from(ErrorKind::S3Error(msg))
            })?;

        let (tx_key, rx_key) = channel::bounded(QSIZE.load(Ordering::Relaxed));

        // spawn object fetcher tasks
        for i in 0..self.config.max_connections {
            let client = client.clone();
            let rx = rx_key.clone();
            let bucket = self.config.bucket.clone();

            let tx = self
                .tx
                .clone()
                .ok_or_else(|| ErrorKind::S3Error("source sender not initialized".to_string()))?;
            let origin_uri = EventOriginUri {
                scheme: URL_SCHEME.to_string(),
                host: hostname(),
                port: None,
                path: vec![bucket.clone()],
            };
            let instance = S3Instance {
                ctx: ctx.clone(),
                client,
                rx,
                tx,
                bucket,
                multipart_threshold: self.config.multipart_threshold,
                part_size: self.config.multipart_chunksize,
                origin_uri,
            };
            let handle = task::Builder::new()
                .name(format!("fetch_obj_task{}", i))
                .spawn(async move { instance.start().await })?;
            self.handles.push(handle);
        }

        // spawn key fetcher task
        let bucket = self.config.bucket.clone();
        let prefix = self.config.prefix.clone();
        task::Builder::new()
            .name("fetch_key_task".to_owned())
            .spawn(fetch_keys_task(client, bucket, prefix, tx_key))?;

        Ok(true)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }

    async fn on_stop(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        // stop all handles
        for handle in self.handles.drain(..) {
            handle.cancel().await;
        }
        Ok(())
    }
}

async fn fetch_keys_task(
    client: S3Client,
    bucket: String,
    prefix: Option<String>,
    sender: Sender<KeyPayload>,
) -> Result<()> {
    let fetch_keys = |continuation_token: Option<String>| async {
        Result::<_>::Ok(
            client
                .list_objects_v2()
                .bucket(bucket.clone())
                .set_prefix(prefix.clone())
                .set_continuation_token(continuation_token)
                .send()
                .await?,
        )
    };

    // fetch first page of keys.
    let mut continuation_token: Option<String> = None;
    let mut resp = fetch_keys(continuation_token.take()).await?;
    debug!("Fetched {} keys of {}.", resp.key_count(), resp.max_keys());

    let mut stream = 0; // for the Channel Source
    'outer: loop {
        match resp.contents.take() {
            None => {}
            Some(entries) => {
                for object_data in entries {
                    sender
                        .send(KeyPayload {
                            object_data,
                            stream,
                        })
                        .await?;
                    stream += 1;
                }
            }
        }

        if resp.is_truncated {
            continuation_token = resp.next_continuation_token().map(ToString::to_string);
        } else {
            // No more pages to fetch.
            break 'outer;
        }

        resp = fetch_keys(continuation_token.take()).await?;
    }
    Ok(())
}

struct S3Instance {
    ctx: ConnectorContext,
    client: S3Client,
    rx: Receiver<KeyPayload>,
    tx: Sender<SourceReply>,
    bucket: String,
    multipart_threshold: i64,
    part_size: i64,
    origin_uri: EventOriginUri,
}
impl S3Instance {
    fn event_from(&self, data: Vec<u8>, stream: u64) -> SourceReply {
        SourceReply::Data {
            origin_uri: self.origin_uri.clone(),
            data,
            meta: None,
            stream,
            port: None,
        }
    }
    async fn fetch_object_stream(
        &self,
        key: Option<String>,
        range: Option<String>,
    ) -> Result<ByteStream> {
        Ok(self
            .client
            .get_object()
            .bucket(&self.bucket)
            .set_key(key)
            .set_range(range)
            .send()
            .await?
            .body)
    }
    ///
    /// Receives object keys and the corresponsing stream id from the `recvr`
    /// Fetches the object from s3
    /// depending on `multipart_threshold` it downloads the object as one or in ranges.
    ///
    /// The received data is sent to the `ChannelSource` channel.
    async fn start(self) -> Result<()> {
        while let Ok(KeyPayload {
            object_data: Object { key, size, .. },
            stream,
        }) = self.rx.recv().await
        {
            let mut err = false; // marks an error
            debug!("{} Fetching key {key:?}...", self.ctx);
            if size <= self.multipart_threshold {
                // Perform a single fetch.
                err = err || self.fetch_no_multipart(stream, key).await;
            } else {
                err = err || self.fetch_multipart(stream, key, size).await;
            }

            // Close the stream
            let stream_finish_reply = if err {
                SourceReply::StreamFail(stream)
            } else {
                SourceReply::EndStream {
                    origin_uri: self.origin_uri.clone(),
                    stream,
                    meta: None,
                }
            };
            self.tx.send(stream_finish_reply).await?;
        }
        Ok(())
    }

    async fn fetch_no_multipart(&self, stream: u64, key: Option<String>) -> bool {
        match self.fetch_object_stream(key.clone(), None).await {
            Ok(mut obj_stream) => {
                loop {
                    // we iterate over the chunks the response provides
                    match obj_stream.try_next().await {
                        Ok(Some(chunk)) => {
                            debug!(
                                "{} Received chunk with {} bytes for key {key:?}.",
                                self.ctx,
                                chunk.len()
                            );
                            // meh, we need to clone the chunk :(
                            if self
                                .tx
                                .send(self.event_from(chunk.as_ref().to_vec(), stream))
                                .await
                                .is_err()
                            {
                                error!(
                                    "{} Error sending data for key {key:?} to source.",
                                    self.ctx
                                );
                                return true;
                            }
                        }
                        Ok(None) => {
                            // stream finished
                            return false;
                        }
                        Err(e) => {
                            error!("{} Error fetching data for key {key:?}: {e}", self.ctx);
                            // TODO: emit event to `err` port?
                            return true;
                        }
                    }
                }
            }
            Err(e) => {
                error!("{} Error fetching key {key:?}: {e}", self.ctx);
                true
            }
        }
    }
    async fn fetch_multipart(&self, stream: u64, key: Option<String>, size: i64) -> bool {
        let mut err = false;
        // Fetch multipart.
        let mut fetched_bytes = 0; // represent the next byte to fetch.

        while fetched_bytes < size {
            let fetch_till = fetched_bytes + self.part_size;
            let range = Some(format!("bytes={}-{}", fetched_bytes, fetch_till - 1)); // -1 is reqd here as range is inclusive.

            debug!(
                "{} Fetching byte range: bytes={}-{} for key {key:?}",
                self.ctx,
                fetched_bytes,
                fetch_till - 1
            );
            // fetch the range.
            match self.fetch_object_stream(key.clone(), range.clone()).await {
                Ok(mut obj_stream) => {
                    'inner_range: loop {
                        // stream over the response chunks
                        match obj_stream.try_next().await {
                            Ok(Some(chunk)) => {
                                debug!("{} Received chunk with {} bytes for range {range:?} for key {key:?}.", self.ctx, chunk.len());
                                // meh, we need to clone the chunk :(
                                if self
                                    .tx
                                    .send(self.event_from(chunk.as_ref().to_vec(), stream))
                                    .await
                                    .is_err()
                                {
                                    error!("{} Error sending data for range {range:?} for key {key:?} to source.", self.ctx);
                                    err = true;
                                    break 'inner_range;
                                }
                            }
                            Ok(None) => {
                                // object range stream finished
                                break 'inner_range;
                            }
                            Err(e) => {
                                error!("{} Error fetching data for range {range:?} for key {key:?}: {e}", self.ctx);
                                // TODO: emit event to `err` port?
                                err = true;
                                break 'inner_range; // wait for next key
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "{} Error fetching range {range:?} for key {key:?}: {e}",
                        self.ctx
                    );
                    err = true;
                }
            }
            // update for next iteration.
            fetched_bytes = fetch_till;
        }
        err
    }
}
