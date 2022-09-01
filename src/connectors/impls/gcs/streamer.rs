// Copyright 2020-2022, The Tremor Team
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

use crate::{
    connectors::{
        impls::gcs::{
            consistent::GCSConsistentSink,
            resumable_upload_client::{
                DefaultClient, ExponentialBackoffRetryStrategy, FileId, ResumableUploadClient,
            },
            yolo::GCSYoloSink,
        },
        prelude::*,
    },
    errors::err_gcs,
    system::KillSwitch,
};
use http_client::{h1::H1Client, HttpClient};
use std::time::Duration;
use tremor_pipeline::ConfigImpl;
use tremor_value::Value;
use value_trait::ValueAccess;

const CONNECTOR_TYPE: &'static str = "gcs_streamer";

/// mode of operation for the gcs_streamer
#[derive(Debug, Default, Deserialize, Clone, Copy)]
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
                Self::Yolo => "yolo",
                Self::Consistent => "consistent",
            }
        )
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    /// endpoint url
    #[serde(default = "default_endpoint")]
    pub(super) url: Url<HttpsDefaults>,
    /// Optional default bucket
    pub(super) bucket: Option<String>,
    /// Mode of operation for this sink
    pub(super) mode: Mode,
    #[serde(default = "default_connect_timeout")]
    pub(super) connect_timeout: u64,
    #[serde(default = "default_buffer_size")]
    pub(super) buffer_size: usize,
    #[serde(default = "default_max_retries")]
    pub(super) max_retries: u32,
    #[serde(default = "default_backoff_base_time")]
    pub(super) default_backoff_base_time: u64,
}

#[allow(clippy::unwrap_used)]
fn default_endpoint() -> Url<HttpsDefaults> {
    // ALLOW: this URL is hardcoded, so the only reason for parse failing would be if it was changed
    Url::parse("https://storage.googleapis.com/upload/storage/v1").unwrap()
}

fn default_connect_timeout() -> u64 {
    10_000_000_000
}

fn default_buffer_size() -> usize {
    1024 * 1024 * 8 // 8MB - the recommended minimum
}

fn default_max_retries() -> u32 {
    3
}

fn default_backoff_base_time() -> u64 {
    25_000_000
}

impl ConfigImpl for Config {}

impl Config {
    fn normalize(&mut self, alias: &Alias) {
        let buffer_size = next_multiple_of(self.buffer_size, 256 * 1024);

        if self.buffer_size < buffer_size {
            warn!(
                "[Connector::{alias}] Rounding buffer_size up to the next multiple of 256KiB: {}KiB.",
                buffer_size / 1024
            );
            self.buffer_size = buffer_size;
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

/// stolen from rust std-lib, where it is still guarded behind an unstable feature #yolo
const fn next_multiple_of(l: usize, multiple: usize) -> usize {
    match l % multiple {
        0 => l,
        r => l + (multiple - r),
    }
}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        ConnectorType(CONNECTOR_TYPE.into())
    }

    async fn build_cfg(
        &self,
        alias: &Alias,
        _config: &ConnectorConfig,
        connector_config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let mut config = Config::new(connector_config)?;
        config.normalize(alias);

        Ok(Box::new(GCSStreamerConnector { config }))
    }
}

struct GCSStreamerConnector {
    config: Config,
}

#[async_trait::async_trait]
impl Connector for GCSStreamerConnector {
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let http_client = create_client(Duration::from_nanos(self.config.connect_timeout))?;
        let backoff_strategy = ExponentialBackoffRetryStrategy::new(
            self.config.max_retries,
            Duration::from_nanos(self.config.default_backoff_base_time),
        );
        let upload_client = DefaultClient::new(http_client, backoff_strategy)?;

        info!(
            "{sink_context} Starting {CONNECTOR_TYPE} in {} mode",
            &self.config.mode
        );
        match self.config.mode {
            Mode::Yolo => {
                let sink = GCSYoloSink::new(self.config.clone(), upload_client);
                builder.spawn(sink, sink_context).map(Some)
            }
            Mode::Consistent => {
                let reply_tx = builder.reply_tx();
                let sink = GCSConsistentSink::new(self.config.clone(), upload_client, reply_tx);
                builder.spawn(sink, sink_context).map(Some)
            }
        }
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

pub(crate) const NAME: &'static str = "name";
pub(crate) const BUCKET: &'static str = "bucket";

pub(crate) trait GCSCommons<Client: ResumableUploadClient> {
    fn default_bucket(&self) -> Option<&Value<'_>>;
    fn client(&self) -> &Client;

    fn get_bucket_name(&self, meta: Option<&Value>) -> Result<String> {
        let bucket = meta
            .get(BUCKET)
            .or(self.default_bucket())
            .ok_or(err_gcs(format!(
                "Metadata `${CONNECTOR_TYPE}.{BUCKET}` missing",
            )))
            .as_str()
            .ok_or(err_gcs(format!(
                "Metadata `${CONNECTOR_TYPE}.{BUCKET}` is not a string.",
            )))?
            .to_string();

        Ok(bucket)
    }

    fn get_file_id(&self, meta: Option<&Value<'_>>) -> Result<FileId> {
        let name = meta
            .get(NAME)
            .ok_or(err_gcs(format!(
                "`${CONNECTOR_TYPE}.{NAME}` metadata is missing",
            )))?
            .as_str()
            .ok_or(err_gcs(format!(
                "`${CONNECTOR_TYPE}.{NAME}` metadata is not a string",
            )))?;
        let bucket = self.get_bucket_name(meta)?;
        Ok(FileId::new(bucket, name))
    }
}

fn create_client(connect_timeout: Duration) -> Result<H1Client> {
    let mut client = H1Client::new();
    client.set_config(
        http_client::Config::new()
            .set_timeout(Some(connect_timeout))
            .set_http_keep_alive(true),
    )?;

    Ok(client)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use halfbrown::HashMap;
    use tremor_value::literal;

    use crate::connectors::impls::gcs::chunked_buffer::BufferPart;
    // use crate::connectors::reconnect::ConnectionLostNotifier;
    // use crate::{config::Codec, connectors::impls::gcs::chunked_buffer::ChunkedBuffer};
    // use async_std::channel::bounded;
    use std::sync::atomic::AtomicUsize;
    // use tremor_script::EventPayload;

    #[derive(Debug, Default)]
    pub(crate) struct TestUploadClient {
        counter: AtomicUsize,
        running: HashMap<url::Url, (FileId, Vec<BufferPart>)>,
        finished: HashMap<url::Url, (FileId, Vec<BufferPart>)>,
        deleted: HashMap<url::Url, (FileId, Vec<BufferPart>)>,
        inject_failure: bool,
    }

    impl TestUploadClient {
        pub(crate) fn running_uploads(&self) -> Vec<(FileId, Vec<BufferPart>)> {
            self.running.values().cloned().collect()
        }

        pub(crate) fn finished_uploads(&self) -> Vec<(FileId, Vec<BufferPart>)> {
            self.finished.values().cloned().collect()
        }

        pub(crate) fn deleted_uploads(&self) -> Vec<(FileId, Vec<BufferPart>)> {
            self.deleted.values().cloned().collect()
        }

        pub(crate) fn count(&self) -> usize {
            self.counter.load(Ordering::Acquire)
        }

        pub(crate) fn inject_failure(&mut self, inject_failure: bool) {
            self.inject_failure = inject_failure;
        }
    }

    #[async_trait::async_trait]
    impl ResumableUploadClient for TestUploadClient {
        async fn start_upload(
            &mut self,
            url: &Url<HttpsDefaults>,
            file_id: FileId,
        ) -> Result<url::Url> {
            if self.inject_failure {
                return Err(err_gcs("Error on start_upload"));
            }
            let count = self.counter.fetch_add(1, Ordering::AcqRel);
            let mut session_uri = url.url().clone();
            session_uri.set_path(format!("{}/{}", url.path(), count).as_str());
            self.running.insert(session_uri.clone(), (file_id, vec![]));
            Ok(session_uri)
        }
        async fn upload_data(&mut self, url: &url::Url, part: BufferPart) -> Result<usize> {
            if self.inject_failure {
                return Err(err_gcs("Error on upload_data"));
            }
            if let Some((_file_id, buffers)) = self.running.get_mut(url) {
                let end = part.start + part.len();
                buffers.push(part);
                // TODO: create mode where it always keeps some bytes
                Ok(end)
            } else {
                Err("upload not found".into())
            }
        }
        async fn finish_upload(&mut self, url: &url::Url, part: BufferPart) -> Result<()> {
            if self.inject_failure {
                return Err(err_gcs("Error on finish_upload"));
            }
            if let Some((file_id, mut buffers)) = self.running.remove(url) {
                buffers.push(part);
                self.finished.insert(url.clone(), (file_id, buffers));
                Ok(())
            } else {
                Err("upload not found".into())
            }
        }
        async fn delete_upload(&mut self, url: &url::Url) -> Result<()> {
            // we don't fail on delete, as we want to see the effect of deletion in our tests
            if let Some(upload) = self.running.remove(url) {
                self.deleted.insert(url.clone(), upload);
                Ok(())
            } else {
                Err("upload not found".into())
            }
        }
        async fn bucket_exists(
            &mut self,
            _url: &Url<HttpsDefaults>,
            _bucket: &str,
        ) -> Result<bool> {
            if self.inject_failure {
                return Err(err_gcs("Error on bucket_exists"));
            }
            Ok(true)
        }
    }

    #[test]
    pub fn default_endpoint_does_not_panic() {
        // This test will fail if this panics (it should never)
        default_endpoint();
    }

    #[test]
    pub fn config_adapts_when_buffer_size_is_not_divisible_by_256ki() {
        let raw_config = literal!({
            "mode": "yolo",
            "buffer_size": 256 * 1000
        });

        let mut config = Config::new(&raw_config).expect("config should be valid");
        let alias = Alias::new("flow", "conn");
        config.normalize(&alias);
        assert_eq!(256 * 1024, config.buffer_size);
    }
}
