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

pub(crate) const CONNECTOR_TYPE: &str = "s3_reader";
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

    async fn build_cfg(
        &self,
        _: &ConnectorAlias,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = S3SourceConfig::new(config)?;

        // TODO: display a warning if chunksize lesser than some quantity
        Ok(Box::new(S3SourceConnector {
            handles: Vec::with_capacity(config.max_connections),
            config,
            tx: None,
        }))
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
    loop {
        if let Some(entries) = resp.contents.take() {
            for object_data in entries {
                let p = KeyPayload {
                    object_data,
                    stream,
                };
                sender.send(p).await?;
                stream += 1;
            }
        }

        if resp.is_truncated {
            continuation_token = resp.next_continuation_token().map(ToString::to_string);
        } else {
            // No more pages to fetch.
            break;
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
    fn event_from(&self, data: Vec<u8>, meta: Value<'static>, stream: u64) -> SourceReply {
        SourceReply::Data {
            origin_uri: self.origin_uri.clone(),
            data,
            meta: Some(meta),
            stream: Some(stream),
            port: None,
            codec_overwrite: None,
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
            object_data,
            stream,
        }) = self.rx.recv().await
        {
            debug!("{} Fetching key {:?}...", self.ctx, object_data.key());
            let err = if object_data.size() <= self.multipart_threshold {
                // Perform a single fetch.
                self.fetch_no_multipart(stream, object_data).await
            } else {
                self.fetch_multipart(stream, object_data).await
            };

            // Close the stream
            let stream_finish_reply = if err.is_err() {
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

    async fn fetch_no_multipart(&self, stream: u64, object_data: Object) -> Result<()> {
        let key = object_data.key().map(ToString::to_string);
        let mut obj_stream = self.fetch_object_stream(key.clone(), None).await?;

        let meta = self.to_object_meta(object_data, None);
        while let Some(chunk) = obj_stream.try_next().await? {
            // we iterate over the chunks the response provides
            debug!(
                "{} Received chunk with {} bytes for key {:?}.",
                self.ctx,
                chunk.len(),
                key
            );
            // meh, we need to clone the chunk :(
            self.tx
                .send(self.event_from(chunk.as_ref().to_vec(), meta.clone(), stream))
                .await?;
        }
        Ok(())
    }
    async fn fetch_multipart(&self, stream: u64, object_data: Object) -> Result<()> {
        // Fetch multipart.
        let mut fetched_bytes = 0; // represent the next byte to fetch.
        let size = object_data.size();
        let key = object_data.key().map(ToString::to_string);

        while fetched_bytes < size {
            let fetch_till = (fetched_bytes + self.part_size).min(size);
            let range = Some(format!("bytes={}-{}", fetched_bytes, fetch_till - 1)); // -1 is reqd here as range is inclusive.

            debug!(
                "{} Fetching byte range: bytes={fetched_bytes}-{} for key {key:?}",
                self.ctx,
                fetch_till - 1
            );
            // fetch the range.
            let mut obj_stream = self.fetch_object_stream(key.clone(), range).await?;
            while let Some(chunk) = obj_stream.try_next().await? {
                // stream over the response chunks
                debug!(
                    "{} Received chunk with {} bytes for key {key:?}.",
                    self.ctx,
                    chunk.len()
                );
                let meta =
                    self.to_object_meta(object_data.clone(), Some((fetched_bytes, fetch_till - 1)));
                self.tx
                    .send(self.event_from(chunk.as_ref().to_vec(), meta, stream))
                    .await?;
            }

            // update for next iteration.
            fetched_bytes = fetch_till;
        }
        Ok(())
    }

    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    fn to_object_meta(&self, object: Object, range: Option<(i64, i64)>) -> Value<'static> {
        let Object {
            size,
            key,
            last_modified,
            e_tag,
            .. // TODO: maybe owner and storage class are interesting for some?
        } = object;
        let range = range.map_or_else(Value::const_null, |(start, end)| {
            literal!({
                "start": start,
                "end": end
            })
        });
        let meta = literal!({
            "size": size,
            "bucket": self.bucket.clone(),
            "key": key,
            "last_modified": last_modified.map(|dt| dt.as_nanos() as u64),
            "e_tag": e_tag,
            "range": range // range is null if we have the full file in this event
        });
        self.ctx.meta(meta)
    }
}
