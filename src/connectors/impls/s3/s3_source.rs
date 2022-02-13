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

use async_std::channel::{self, Receiver, Sender};
use async_std::task::{self, JoinHandle};
use bytes::Buf;
use std::io::Read;
use std::mem;

use super::s3_auth;
use aws_sdk_s3 as s3;
use s3::model::Object;
use s3::ByteStream;
use s3::Client as S3Client;

const MINCHUNKSIZE: i64 = 8 * 1024 * 1024; // 8 MBs

struct S3SourceConnector {
    config: S3SourceConfig,
    tx: Option<Sender<SourceReply>>,
    handles: Vec<JoinHandle<Result<()>>>,
}

#[derive(Deserialize, Debug, Default)]
pub struct S3SourceConfig {
    #[serde(default = "S3SourceConfig::default_aws_region")]
    aws_region: String,
    endpoint: Option<String>,
    bucket: String,

    prefix: Option<String>,
    delimiter: Option<String>,

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
    fn default_aws_region() -> String {
        "us-east-1".to_string()
    }

    fn default_multipart_chunksize() -> i64 {
        MINCHUNKSIZE
    }
    fn default_multipart_threshold() -> i64 {
        MINCHUNKSIZE
    }

    /// https://docs.aws.amazon.com/cli/latest/topic/s3-config.html#max-concurrent-requests
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
        "s3-source".into()
    }

    async fn from_config(
        &self,
        id: &str,
        raw_config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(config) = raw_config {
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

    async fn connect(&mut self, _ctx: &ConnectorContext, _attemp: &Attempt) -> Result<bool> {
        let client = s3_auth::get_client(
            self.config.aws_region.clone(),
            self.config.endpoint.as_ref(),
        )
        .await?;

        // Check the existence of the bucket.
        client
            .head_bucket()
            .bucket(self.config.bucket.clone())
            .send()
            .await?;

        let (tx_key, rx_key) = channel::bounded(QSIZE.load(Ordering::Relaxed));

        // spawn object fetcher tasks
        for i in 0..self.config.max_connections {
            let task_client = client.clone();
            let task_rx = rx_key.clone();
            let task_bucket = self.config.bucket.clone();

            let task_sender = self
                .tx
                .clone()
                .ok_or_else(|| ErrorKind::S3Error("source sender not initialized".to_string()))?;

            let handle = task::Builder::new()
                .name(format!("fetch_obj_task{}", i))
                .spawn(fetch_object_task(
                    task_client,
                    task_bucket,
                    task_rx,
                    task_sender,
                    self.config.multipart_threshold,
                    self.config.multipart_chunksize,
                ))?;
            self.handles.push(handle);
        }

        // spawn key fetcher task
        let bucket = self.config.bucket.clone();
        let prefix = self.config.prefix.clone();
        let delim = self.config.delimiter.clone();
        task::Builder::new()
            .name("fetch_key_task".to_owned())
            .spawn(fetch_keys_task(client, bucket, prefix, delim, tx_key))?;

        Ok(true)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}

async fn fetch_keys_task(
    client: S3Client,
    bucket: String,
    prefix: Option<String>,
    delim: Option<String>,
    sender: Sender<KeyPayload>,
) -> Result<()> {
    let fetch_keys = |start_after: Option<String>| async {
        Result::<_>::Ok(
            client
                .list_objects_v2()
                .bucket(bucket.clone())
                .set_prefix(prefix.clone())
                .set_delimiter(delim.clone())
                .set_start_after(start_after)
                .send()
                .await?,
        )
    };

    // fetch first page of keys.
    let mut last_key_fetched: Option<String> = None;
    let mut resp = fetch_keys(last_key_fetched.take()).await?;

    let mut stream = 0; // for the Channel Source
    'outer: loop {
        match resp.contents.take() {
            None => {}
            Some(entries) => {
                match entries.last() {
                    None => break 'outer, // No more keys/objects
                    Some(obj) => last_key_fetched = obj.key.clone(),
                }

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

        // No more pages to fetch.
        if !resp.is_truncated {
            break 'outer;
        }

        resp = fetch_keys(last_key_fetched.take()).await?;
    }
    Ok(())
}

/*
Gets key from the recvr
Fetches the object from s3
Send to the ChannelSource channel */
async fn fetch_object_task(
    client: S3Client,
    bucket: String,
    recvr: Receiver<KeyPayload>,
    sender: Sender<SourceReply>,
    threshold: i64,
    part_size: i64,
) -> Result<()> {
    let origin_uri = EventOriginUri {
        scheme: "s3".to_string(),
        host: hostname(),
        port: None,
        path: vec![bucket.clone()],
    };
    // Construct the event struct
    let event_from = |data, stream| SourceReply::Data {
        origin_uri: origin_uri.clone(),
        data,
        meta: None,
        stream,
        port: None,
    };
    // Set the key and range (if present)
    let object_stream = |key, range| async {
        Result::<ByteStream>::Ok(
            client
                .get_object()
                .bucket(bucket.clone())
                .set_key(key)
                .set_range(range)
                .send()
                .await?
                .body,
        )
    };

    let mut v = Vec::new();

    while let Ok(KeyPayload {
        object_data,
        stream,
    }) = recvr.recv().await
    {
        info!("key: {:?}", object_data.key());
        // FIXME: usize -> i64, should be alright.
        if object_data.size() <= threshold {
            // Perform a single fetch.
            let obj_stream: ByteStream = object_stream(object_data.key, None).await?;

            let bytes_read = obj_stream
                .collect()
                .await
                .map_err(|e| ErrorKind::S3Error(e.to_string()))?
                .reader()
                .read_to_end(&mut v)?;

            v.truncate(bytes_read);
            let event_data = mem::take(&mut v);
            sender.send(event_from(event_data, stream)).await?;
        } else {
            // Fetch multipart.
            let mut fetched_bytes = 0; // represent the next byte to fetch.

            while fetched_bytes < object_data.size {
                let fetch_till = fetched_bytes + part_size;
                let range = Some(format!("bytes={}-{}", fetched_bytes, fetch_till - 1)); // -1 is reqd here as range is inclusive.

                warn!("Fetching bytes: bytes={}-{}", fetched_bytes, fetch_till - 1);
                // fetch the range.
                let obj_stream = object_stream(object_data.key.clone(), range).await?;

                let bytes_read = obj_stream
                    .collect()
                    .await
                    .map_err(|e| ErrorKind::S3Error(e.to_string()))?
                    .reader()
                    .read_to_end(&mut v)?;

                v.truncate(bytes_read);

                // update for next iteration.
                fetched_bytes = fetch_till;

                let event_data = mem::take(&mut v);
                sender.send(event_from(event_data, stream)).await?;
            }
        }

        // Close the stream
        sender
            .send(SourceReply::EndStream {
                origin_uri: origin_uri.clone(),
                stream,
                meta: None,
            })
            .await?;
    }

    Ok(())
}
