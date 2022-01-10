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
use std::env;
use std::io::Read;
use std::mem;

use aws_sdk_s3 as s3;
use aws_types::{credentials::Credentials, region::Region};
use s3::model::Object;
use s3::Client as S3Client;
use s3::Endpoint;

struct S3SourceConnector {
    config: S3SourceConfig,
    endpoint: Option<http::Uri>,
    tx: Option<Sender<SourceReply>>,
    handles: Vec<JoinHandle<Result<()>>>,
}

#[derive(Deserialize, Debug, Default)]
pub struct S3SourceConfig {
    #[serde(default = "S3SourceConfig::default_access_key_id")]
    aws_access_key_id: String,
    #[serde(default = "S3SourceConfig::default_secret_token")]
    aws_secret_access_key: String,
    #[serde(default = "S3SourceConfig::default_aws_region")]
    aws_region: String,

    max_connections: usize,
    endpoint: Option<String>,

    bucket: String,
    prefix: Option<String>,
    delimiter: Option<String>,
}

// FIXME: insert better name here
struct KeyPayload {
    object_data: Object,
    stream: u64,
}

// Defaults for the config.
impl S3SourceConfig {
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
            let mut config = S3SourceConfig::new(config)?;

            // Fetch the secrets from the env.
            config.aws_secret_access_key = env::var(config.aws_secret_access_key)?;
            config.aws_access_key_id = env::var(config.aws_access_key_id)?;
            config.aws_region = env::var(config.aws_region)?;

            // Check the validity of given url
            let endpoint = if let Some(url) = &config.endpoint {
                let url_parsed = url.parse::<http::Uri>()?;
                Some(url_parsed)
            } else {
                None
            };

            Ok(Box::new(S3SourceConnector {
                handles: Vec::with_capacity(config.max_connections),
                config,
                endpoint,
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

        // Check the existence of the bucket.
        client
            .head_bucket()
            .bucket(self.config.bucket.clone())
            .send()
            .await?;

        let (tx_key, rx_key) = channel::bounded(QSIZE.load(Ordering::Relaxed));

        //FIXME: track on number of spawned tasks
        for _i in 0..self.config.max_connections {
            let task_client = client.clone();
            let task_rx = rx_key.clone();
            let task_bucket = self.config.bucket.clone();

            let task_sender = self
                .tx
                .clone()
                .ok_or_else(|| ErrorKind::S3Error("source sender not initialized".to_string()))?;

            let handle = task::spawn(fetch_object_task(
                task_client,
                task_bucket,
                task_rx,
                task_sender,
            ));
            self.handles.push(handle);
        }

        // Key Fetcher Task
        let bucket = self.config.bucket.clone();
        let prefix = self.config.prefix.clone();
        let delim = self.config.delimiter.clone();
        task::spawn(fetch_keys_task(client, bucket, prefix, delim, tx_key));

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
    // fetch first pool of keys.
    let mut resp = client
        .list_objects_v2()
        .bucket(bucket.clone())
        .set_prefix(prefix.clone())
        .set_delimiter(delim.clone())
        .send()
        .await?;

    let mut stream = 0; // for the Channel Source
    let mut last_key_fetched: Option<String> = None;
    'outer: loop {
        match resp.contents.take() {
            None => {}
            Some(entries) => {
                // FIXME: Could there be issues setting this before fetching all the keys
                match entries.last() {
                    None => {
                        // No more keys/objects
                        break 'outer;
                    }
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

        if !resp.is_truncated {
            // No more entries.
            break 'outer;
        }

        resp = client
            .list_objects_v2()
            .bucket(bucket.clone())
            .set_prefix(prefix.clone())
            .set_delimiter(delim.clone())
            .set_start_after(last_key_fetched.take())
            .send()
            .await?;
    }

    Ok(())
}

async fn fetch_object_task(
    client: S3Client,
    bucket: String,
    recvr: Receiver<KeyPayload>,
    sender: Sender<SourceReply>,
) -> Result<()> {
    // gets key from the recvr
    // fetches the object from s3
    // send to the ChannelSource channel

    // would make sense to use a preallocated buffer.
    let mut v = Vec::new();

    while let Ok(KeyPayload {
        object_data,
        stream,
    }) = recvr.recv().await
    {
        // Auto handled.
        // sender.send(SourceReply::StartStream(stream)).await?;

        // Fetch the object here
        let obj_stream = client
            .get_object()
            .bucket(bucket.clone())
            .set_key(object_data.key)
            .send()
            .await?
            .body;

        let bytes_read = obj_stream
            .collect()
            .await
            .map_err(|e| ErrorKind::S3Error(e.to_string()))?
            .reader()
            .read_to_end(&mut v)?;
        v.truncate(bytes_read);

        let origin_uri = EventOriginUri {
            scheme: "s3".to_string(),
            host: hostname(),
            port: None,
            path: vec![bucket.clone()],
        };

        let event = SourceReply::Data {
            origin_uri: origin_uri.clone(),
            data: mem::take(&mut v),
            meta: None,
            stream,
            port: None,
        };

        sender.send(event).await?;
        sender
            .send(SourceReply::EndStream {
                origin_uri,
                stream,
                meta: None,
            })
            .await?;
    }

    Ok(())
}
