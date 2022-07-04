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

use crate::connectors::prelude::{
    Attempt, ErrorKind, EventSerializer, Result, SinkAddr, SinkContext, SinkManagerBuilder,
    SinkReply, Url,
};
use crate::connectors::sink::Sink;
use crate::connectors::utils::url::HttpsDefaults;
use crate::connectors::{
    CodecReq, Connector, ConnectorBuilder, ConnectorConfig, ConnectorType, Context,
};
use crate::system::KillSwitch;
use async_std::task::sleep;
use gouth::Token;
use http_client::h1::H1Client;
use http_client::HttpClient;
use http_types::{Method, Request};
use std::time::Duration;
use tremor_pipeline::{ConfigImpl, Event};
use tremor_value::Value;
use value_trait::ValueAccess;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    #[serde(default = "default_endpoint")]
    endpoint: String,
    #[serde(default = "default_connect_timeout")]
    #[allow(unused)] // FIXME: use or remove
    connect_timeout: u64,
    #[serde(default = "default_buffer_size")]
    buffer_size: usize, // request_timeout: u64
}

fn default_endpoint() -> String {
    "https://storage.googleapis.com/upload/storage/v1".to_string()
}

fn default_connect_timeout() -> u64 {
    10_000_000_000
}

fn default_buffer_size() -> usize {
    // 1024 * 1024 * 8 // 8MB - the recommended minimum
    512 * 1024 // FIXME: This is way too low, using only for testing
}

impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        ConnectorType("gcs_writer".into())
    }

    async fn build_cfg(
        &self,
        _alias: &str,
        _config: &ConnectorConfig,
        connector_config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(connector_config)?;

        Ok(Box::new(GCSWriterConnector { config }))
    }
}

struct GCSWriterConnector {
    config: Config,
}

#[async_trait::async_trait]
impl Connector for GCSWriterConnector {
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let url = Url::<HttpsDefaults>::parse(&self.config.endpoint)?;
        let sink = GCSWriterSink {
            client: None,
            url,
            config: self.config.clone(),
            buffers: Buffers::new(self.config.buffer_size),
            current_name: None,
            current_session_url: None,
        };

        let addr = builder.spawn(sink, sink_context)?;
        Ok(Some(addr))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

struct Buffers {
    data: Vec<u8>,
    block_size: usize,
    buffer_start: usize,
}

impl Buffers {
    pub fn new(size: usize) -> Self {
        Self {
            data: Vec::with_capacity(size * 2),
            block_size: size,
            buffer_start: 0,
        }
    }

    pub fn mark_done_until(&mut self, position: usize) {
        // FIXME assert that position > self.buffer_start
        let bytes_to_remove = position - self.buffer_start;
        self.data = Vec::from(&self.data[bytes_to_remove..]);
        self.buffer_start += bytes_to_remove;
    }

    pub fn read_current_block(&self) -> Option<&[u8]> {
        if self.data.len() < self.block_size {
            return None;
        }

        Some(&self.data[..self.block_size])
    }

    pub fn write(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }

    pub fn start(&self) -> usize {
        self.buffer_start
    }

    pub fn end(&self) -> usize {
        self.buffer_start + self.data.len().min(self.block_size)
    }

    pub fn final_block(&self) -> &[u8] {
        &self.data[..]
    }
}

struct GCSWriterSink {
    client: Option<H1Client>,
    url: Url<HttpsDefaults>,
    #[allow(unused)] // FIXME: use or remove
    config: Config,
    buffers: Buffers,
    current_name: Option<String>,
    current_session_url: Option<String>,
}

#[async_trait::async_trait]
impl Sink for GCSWriterSink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        for (value, meta) in event.value_meta_iter() {
            let meta = ctx.extract_meta(meta);

            let name = meta
                .get("name")
                .ok_or(ErrorKind::GoogleCloudStorageError(
                    "Metadata is missing the file name",
                ))?
                .as_str()
                .ok_or(ErrorKind::GoogleCloudStorageError(
                    "The file name in metadata is not a string",
                ))?;

            self.finish_upload_if_needed(name).await?;

            self.start_upload_if_needed(meta, name).await?;

            let serialized_data = serializer.serialize(value, event.ingest_ns)?;
            for item in serialized_data {
                self.buffers.write(&item);
            }

            if let Some(data) = self.buffers.read_current_block() {
                let mut response = None;
                for i in 0..3 {
                    let mut request = Request::new(
                        Method::Put,
                        url::Url::parse(self.current_session_url.as_ref().ok_or(
                            ErrorKind::GoogleCloudStorageError("No session URL is available"),
                        )?)?,
                    );
                    // -1 on the end is here, because Content-Range is inclusive and our range is exclusive
                    request.insert_header(
                        "Content-Range",
                        format!(
                            "bytes {}-{}/*",
                            self.buffers.start(),
                            self.buffers.end() - 1
                        ),
                    );
                    request.insert_header("User-Agent", "curl/7.68.0");
                    request.insert_header("Accept", "*/*");
                    // request.insert_header("Content-Length", format!("{}", self.buffers.end() - self.buffers.start()));
                    request.set_body(data);

                    let client = self.client.as_mut().ok_or(ErrorKind::ClientNotAvailable(
                        "Google Cloud Storage",
                        "not connected",
                    ))?;

                    match client.send(request).await {
                        Ok(request) => response = Some(request),
                        Err(e) => {
                            warn!("Failed to send a request to GCS: {}", e);
                            // FIXME: Adjust the timeout, this number  is pulled of my... hat
                            sleep(Duration::from_millis(250u64 * 2u64.pow(i))).await;
                            continue;
                        }
                    }

                    if let Some(response) = response.as_ref() {
                        if !response.status().is_server_error()
                            && response.header("range").is_some()
                        {
                            break;
                        }

                        // FIXME: Adjust the timeout, this number  is pulled of my... hat
                        sleep(Duration::from_millis(250u64 * 2u64.pow(i))).await;
                    }
                }

                if let Some(response) = response {
                    if response.status().is_server_error() {
                        return Err("Received server errors from Google Cloud Storage".into());
                    }

                    if let Some(raw_range) = response.header("range") {
                        let raw_range = raw_range[0].as_str();

                        // Range format: bytes=0-262143
                        let range_end = &raw_range[raw_range.find('-').ok_or(
                            ErrorKind::GoogleCloudStorageError(
                                "Did not find a - in the Range header",
                            ),
                        )? + 1..];

                        self.buffers.mark_done_until(range_end.parse()?);
                    } else {
                        return Err("No range header?".into());
                    }
                } else {
                    return Err("no response from GCS".into());
                }
            }
        }

        Ok(SinkReply::ACK)
    }

    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let mut client = H1Client::new();
        client.set_config(http_client::Config::new().set_http_keep_alive(false))?;

        self.client = Some(client);
        self.current_name = None;
        self.buffers = Buffers::new(self.config.buffer_size); // FIXME: validate that the buffer size is a multiple of 256kB, as required by GCS

        Ok(true)
    }

    fn auto_ack(&self) -> bool {
        false
    }
}

impl GCSWriterSink {
    async fn finish_upload_if_needed(&mut self, name: &str) -> Result<()> {
        if let Some(current_session_url) = self.current_session_url.as_ref() {
            if self.current_name.as_deref() != Some(name) {
                let client = self.client.as_mut().ok_or(ErrorKind::ClientNotAvailable(
                    "Google Cloud Storage",
                    "not connected",
                ))?;

                let final_data = self.buffers.final_block();

                for i in 0..3 {
                    let mut request =
                        Request::new(Method::Put, url::Url::parse(current_session_url)?);
                    // -1 on the end is here, because Content-Range is inclusive and our range is exclusive
                    request.insert_header(
                        "Content-Range",
                        format!(
                            "bytes {}-{}/{}",
                            self.buffers.start(),
                            self.buffers.start() + final_data.len() - 1,
                            self.buffers.start() + final_data.len()
                        ),
                    );
                    request.insert_header("Content-Length", format!("{}", final_data.len()));
                    request.set_body(final_data);

                    let response = client.send(request).await?;

                    if !response.status().is_server_error() {
                        self.buffers = Buffers::new(self.config.buffer_size);
                        self.current_session_url = None;
                        break;
                    }

                    // FIXME: Adjust the timeout, this number  is pulled of my... hat
                    sleep(Duration::from_millis(250u64 * 2u64.pow(i))).await;
                }
            }
        }

        Ok(())
    }

    async fn start_upload_if_needed(&mut self, meta: Option<&Value<'_>>, name: &str) -> Result<()> {
        let client = self.client.as_mut().ok_or(ErrorKind::ClientNotAvailable(
            "Google Cloud Storage",
            "not connected",
        ))?;

        let token = Token::new()?;

        if self.current_session_url.is_none() {
            let url = url::Url::parse(&format!(
                "{}/b/{}/o?name={}&uploadType=resumable",
                self.url,
                meta.get("bucket")
                    .ok_or(ErrorKind::GoogleCloudStorageError(
                        "No bucket name in the metadata"
                    ))
                    .as_str()
                    .ok_or(ErrorKind::GoogleCloudStorageError(
                        "Bucket name is not a string"
                    ))?,
                name
            ))?;
            let mut request = Request::new(Method::Post, url);
            request.insert_header("Authorization", token.header_value()?.to_string());
            let response = client.send(request).await?;

            self.current_session_url = Some(
                response
                    .header("Location")
                    .ok_or(ErrorKind::GoogleCloudStorageError(
                        "Missing Location header",
                    ))?
                    .get(0)
                    .ok_or(ErrorKind::GoogleCloudStorageError(
                        "Missing Location header value",
                    ))?
                    .to_string(),
            );
            self.current_name = Some(name.into());
        }

        Ok(())
    }
}
