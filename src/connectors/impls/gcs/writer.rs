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
use crate::connectors::sink::{AsyncSinkReply, ContraflowData, Sink};
use crate::connectors::utils::url::HttpsDefaults;
use crate::connectors::{
    Alias, CodecReq, Connector, ConnectorBuilder, ConnectorConfig, ConnectorType, Context,
};
use crate::system::KillSwitch;
use crate::{connectors, QSIZE};
use async_std::channel::{bounded, Receiver, Sender};
use async_std::task::sleep;
#[cfg(not(test))]
use gouth::Token;
#[cfg(not(test))]
use http_client::h1::H1Client;
use http_client::HttpClient;
use http_types::{Method, Request};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
#[cfg(test)]
use tests::MockHttpClient;
use tremor_common::time::nanotime;
use tremor_pipeline::{ConfigImpl, Event};
use tremor_value::Value;
use value_trait::ValueAccess;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    #[serde(default = "default_endpoint")]
    endpoint: Url<HttpsDefaults>,
    // #[cfg_attr(test, allow(unused))]
    #[serde(default = "default_connect_timeout")]
    connect_timeout: u64,
    #[serde(default = "default_buffer_size")]
    buffer_size: usize,
    bucket: Option<String>,
}

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
        _alias: &Alias,
        _config: &ConnectorConfig,
        connector_config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(connector_config)?;

        if config.buffer_size % (256 * 1024) != 0 {
            return Err("Buffer size must be a multiple of 256kiB".into());
        }

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
        let default_bucket = self
            .config
            .bucket
            .as_ref()
            .map(|bucket_name| Value::from(bucket_name.as_str()).into_static());

        let reply_tx = builder.reply_tx();

        let sink = GCSWriterSink {
            client_tx: None,
            config: self.config.clone(),
            buffers: ChunkedBuffer::new(self.config.buffer_size),
            current_name: None,
            current_bucket: None,
            default_bucket,
            done_until: Arc::new(AtomicUsize::new(0)),
            reply_tx,
        };

        builder.spawn(sink, sink_context).map(Some)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

struct ChunkedBuffer {
    data: Vec<u8>,
    block_size: usize,
    buffer_start: usize,
}

impl ChunkedBuffer {
    pub fn new(size: usize) -> Self {
        Self {
            data: Vec::with_capacity(size * 2),
            block_size: size,
            buffer_start: 0,
        }
    }

    pub fn mark_done_until(&mut self, position: usize) -> Result<()> {
        if position < self.buffer_start {
            return Err("Buffer was marked as done at index which is not in memory anymore".into());
        }

        let bytes_to_remove = position - self.buffer_start;
        self.data = Vec::from(self.data.get(bytes_to_remove..).ok_or(
            ErrorKind::GoogleCloudStorageError("Not enough data in the buffer"),
        )?);
        self.buffer_start += bytes_to_remove;

        Ok(())
    }

    pub fn read_current_block(&self) -> Option<&[u8]> {
        self.data.get(..self.block_size)
    }

    pub fn write(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }

    pub fn start(&self) -> usize {
        self.buffer_start
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn final_block(&self) -> &[u8] {
        self.data.as_slice()
    }
}

struct HttpTaskRequest {
    command: HttpTaskCommand,
    contraflow_data: Option<ContraflowData>,
    start: u64,
}

enum HttpTaskCommand {
    FinishUpload {
        name: String,
        bucket: String,
        data: Vec<u8>,
        data_start: usize,
        data_length: usize,
    },
    StartUpload {
        name: String,
        bucket: String,
    },
    UploadData {
        name: String,
        bucket: String,
        data: Vec<u8>,
        data_start: usize,
        data_length: usize,
    },
}

#[cfg(not(test))]
fn create_client(connect_timeout: Duration) -> Result<H1Client> {
    let mut client = H1Client::new();
    client.set_config(http_client::Config::new().set_timeout(Some(connect_timeout)))?;

    Ok(client)
}

#[cfg(test)]
fn create_client(_connect_timeout: Duration) -> Result<MockHttpClient> {
    Ok(MockHttpClient {
        config: Default::default(),
        expect_final_request: false,
        inject_failure: Default::default(),
    })
}

async fn http_task(
    command_rx: Receiver<HttpTaskRequest>,
    done_until: Arc<AtomicUsize>,
    reply_tx: Sender<AsyncSinkReply>,
    config: Config,
) -> Result<()> {
    let client = create_client(Duration::from_nanos(config.connect_timeout))?;

    #[cfg(not(test))]
    let token = Token::new()?;

    let mut sessions_per_file = HashMap::new();

    while let Ok(request) = command_rx.recv().await {
        let result = handle_http_command(
            done_until.clone(),
            &client,
            &config.endpoint,
            #[cfg(not(test))]
            &token,
            &mut sessions_per_file,
            request.command,
        )
        .await;

        match result {
            Ok(_) => {
                if let Some(contraflow_data) = request.contraflow_data {
                    reply_tx
                        .send(AsyncSinkReply::Ack(
                            contraflow_data,
                            nanotime() - request.start,
                        ))
                        .await?;
                }
            }
            Err(e) => {
                warn!("Failed to handle a message: {:?}", e);
                if let Some(contraflow_data) = request.contraflow_data {
                    reply_tx.send(AsyncSinkReply::Fail(contraflow_data)).await?;
                }
            }
        }
    }

    Ok(())
}

async fn handle_http_command(
    done_until: Arc<AtomicUsize>,
    client: &impl HttpClient,
    url: &Url<HttpsDefaults>,
    #[cfg(not(test))] token: &Token,
    sessions_per_file: &mut HashMap<(String, String), url::Url>,
    command: HttpTaskCommand,
) -> Result<()> {
    match command {
        HttpTaskCommand::FinishUpload {
            name,
            bucket,
            data,
            data_start,
            data_length,
        } => {
            let session_url: url::Url = sessions_per_file.remove(&(bucket, name)).ok_or(
                ErrorKind::GoogleCloudStorageError("No session URL is available"),
            )?;

            for i in 0..3 {
                let mut request = Request::new(Method::Put, session_url.clone());

                request.insert_header(
                    "Content-Range",
                    format!(
                        "bytes {}-{}/{}",
                        data_start,
                        // -1 on the end is here, because Content-Range is inclusive
                        data_start + data_length - 1,
                        data_start + data_length
                    ),
                );

                request.set_body(data.clone());

                let response = client.send(request).await;

                if let Ok(mut response) = response {
                    if !response.status().is_server_error() {
                        break;
                    }
                    error!(
                        "Error from Google Cloud Storage: {}",
                        response.body_string().await?
                    );
                }

                sleep(Duration::from_millis(25u64 * 2u64.pow(i))).await;
            }
        }
        HttpTaskCommand::StartUpload { bucket, name } => {
            for i in 0..3 {
                let url = url::Url::parse(&format!(
                    "{}/b/{}/o?name={}&uploadType=resumable",
                    url, bucket, name
                ))?;
                #[cfg(not(test))]
                let mut request = Request::new(Method::Post, url);
                #[cfg(test)]
                let request = Request::new(Method::Post, url);
                #[cfg(not(test))]
                {
                    request.insert_header("Authorization", token.header_value()?.to_string());
                }

                let response = client.send(request).await;

                if let Ok(mut response) = response {
                    if !response.status().is_server_error() {
                        sessions_per_file.insert(
                            (bucket.clone(), name.to_string()),
                            url::Url::parse(
                                response
                                    .header("Location")
                                    .ok_or(ErrorKind::GoogleCloudStorageError(
                                        "Missing Location header",
                                    ))?
                                    .get(0)
                                    .ok_or(ErrorKind::GoogleCloudStorageError(
                                        "Missing Location header value",
                                    ))?
                                    .as_str(),
                            )?,
                        );

                        break;
                    }

                    error!(
                        "Error from Google Cloud Storage: {}",
                        response.body_string().await?
                    );
                }

                sleep(Duration::from_millis(25u64 * 2u64.pow(i))).await;
                continue;
            }
        }
        HttpTaskCommand::UploadData {
            name,
            bucket,
            data,
            data_start,
            data_length,
        } => {
            let mut response = None;
            for i in 0..3 {
                let session_url = sessions_per_file
                    .get(&(bucket.clone(), name.clone()))
                    .ok_or(ErrorKind::GoogleCloudStorageError(
                        "No session URL is available",
                    ))?;
                let mut request = Request::new(Method::Put, session_url.clone());

                request.insert_header(
                    "Content-Range",
                    format!(
                        "bytes {}-{}/*",
                        data_start,
                        // -1 on the end is here, because Content-Range is inclusive and our range is exclusive
                        data_start + data_length - 1
                    ),
                );
                request.insert_header("User-Agent", "Tremor");
                request.insert_header("Accept", "*/*");
                request.set_body(data.clone());

                match client.send(request).await {
                    Ok(request) => response = Some(request),
                    Err(e) => {
                        warn!("Failed to send a request to GCS: {}", e);

                        sleep(Duration::from_millis(25u64 * 2u64.pow(i))).await;
                        continue;
                    }
                }

                if let Some(response) = response.as_mut() {
                    if !response.status().is_server_error() && response.header("range").is_some() {
                        break;
                    }

                    error!(
                        "Error from Google Cloud Storage: {}",
                        response.body_string().await?
                    );

                    sleep(Duration::from_millis(25u64 * 2u64.pow(i))).await;
                }
            }

            if let Some(mut response) = response {
                if response.status().is_server_error() {
                    error!(
                        "Error from Google Cloud Storage: {}",
                        response.body_string().await?
                    );
                    return Err("Received server errors from Google Cloud Storage".into());
                }

                if let Some(raw_range) = response.header("range") {
                    let raw_range = raw_range
                        .get(0)
                        .ok_or(ErrorKind::GoogleCloudStorageError(
                            "Missing Range header value",
                        ))?
                        .as_str();

                    // Range format: bytes=0-262143
                    let range_end = &raw_range
                        .get(
                            raw_range
                                .find('-')
                                .ok_or(ErrorKind::GoogleCloudStorageError(
                                    "Did not find a - in the Range header",
                                ))?
                                + 1..,
                        )
                        .ok_or(ErrorKind::GoogleCloudStorageError(
                            "Unable to get the end of the Range",
                        ))?;

                    // NOTE: The data can only be thrown away to the point of the end of the Range header,
                    // since google can persist less than we send in our request.
                    //
                    // see: https://cloud.google.com/storage/docs/performing-resumable-uploads#chunked-upload
                    done_until.store(range_end.parse()?, Ordering::Release);
                } else {
                    return Err("No range header".into());
                }
            } else {
                return Err("no response from GCS".into());
            }
        }
    }

    Ok(())
}

struct GCSWriterSink {
    client_tx: Option<Sender<HttpTaskRequest>>,
    config: Config,
    buffers: ChunkedBuffer,
    current_name: Option<String>,
    current_bucket: Option<String>,
    default_bucket: Option<Value<'static>>,
    done_until: Arc<AtomicUsize>,
    reply_tx: Sender<AsyncSinkReply>,
}

#[async_trait::async_trait]
impl Sink for GCSWriterSink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        start: u64,
    ) -> Result<SinkReply> {
        self.buffers
            .mark_done_until(self.done_until.load(Ordering::Acquire))?;
        let contraflow_data = ContraflowData::from(&event);

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

            self.finish_upload_if_needed(name, Some(contraflow_data.clone()), start)
                .await?;

            self.start_upload_if_needed(meta, name, contraflow_data.clone(), start)
                .await?;

            let serialized_data = serializer.serialize(value, event.ingest_ns)?;
            for item in serialized_data {
                self.buffers.write(&item);
            }

            if let Some(data) = self.buffers.read_current_block() {
                let client_tx = self
                    .client_tx
                    .as_mut()
                    .ok_or(ErrorKind::ClientNotAvailable(
                        "Google Cloud Storage",
                        "not connected",
                    ))?;

                let bucket = get_bucket_name(self.default_bucket.as_ref(), meta)?.to_string();
                self.current_bucket = Some(bucket.clone());

                let command = HttpTaskCommand::UploadData {
                    data: data.to_vec(),
                    name: name.to_string(),
                    bucket,
                    data_start: self.buffers.start(),
                    data_length: self.buffers.len(),
                };
                client_tx
                    .send(HttpTaskRequest {
                        command,
                        start,
                        contraflow_data: Some(contraflow_data.clone()),
                    })
                    .await?;
            }
        }

        Ok(SinkReply::NONE)
    }

    async fn on_stop(&mut self, _ctx: &SinkContext) -> Result<()> {
        self.finish_upload(None, nanotime()).await?;

        Ok(())
    }

    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let (tx, rx) = bounded(QSIZE.load(Ordering::Relaxed));
        connectors::spawn_task(
            ctx.clone(),
            http_task(
                rx,
                self.done_until.clone(),
                self.reply_tx.clone(),
                self.config.clone(),
            ),
        );

        self.client_tx = Some(tx);

        self.current_name = None;
        self.buffers = ChunkedBuffer::new(self.config.buffer_size);

        Ok(true)
    }

    fn auto_ack(&self) -> bool {
        false
    }
}

impl GCSWriterSink {
    async fn finish_upload_if_needed(
        &mut self,
        name: &str,
        contraflow_data: Option<ContraflowData>,
        start: u64,
    ) -> Result<()> {
        if self.current_name.as_deref() != Some(name) && self.current_name.is_some() {
            return self.finish_upload(contraflow_data, start).await;
        }

        Ok(())
    }

    async fn finish_upload(
        &mut self,
        contraflow_data: Option<ContraflowData>,
        start: u64,
    ) -> Result<()> {
        if let Some(current_name) = self.current_name.as_ref() {
            let client_tx = self
                .client_tx
                .as_mut()
                .ok_or(ErrorKind::ClientNotAvailable(
                    "Google Cloud Storage",
                    "not connected",
                ))?;

            let final_data = self.buffers.final_block();

            let command = HttpTaskCommand::FinishUpload {
                name: current_name.to_string(),
                bucket: self
                    .current_bucket
                    .as_ref()
                    .ok_or(ErrorKind::GoogleCloudStorageError(
                        "Current bucket not known",
                    ))?
                    .clone(),
                data: final_data.to_vec(),
                data_start: self.buffers.start(),
                data_length: self.buffers.len(),
            };

            client_tx
                .send(HttpTaskRequest {
                    command,
                    start,
                    contraflow_data,
                })
                .await?;

            self.buffers = ChunkedBuffer::new(self.config.buffer_size);
            self.current_name = None;
        }

        Ok(())
    }

    async fn start_upload_if_needed(
        &mut self,
        meta: Option<&Value<'_>>,
        name: &str,
        contraflow_data: ContraflowData,
        start: u64,
    ) -> Result<()> {
        let client_tx = self
            .client_tx
            .as_mut()
            .ok_or(ErrorKind::ClientNotAvailable(
                "Google Cloud Storage",
                "not connected",
            ))?;

        if self.current_name.is_none() {
            let bucket = get_bucket_name(self.default_bucket.as_ref(), meta)?;
            self.current_bucket = Some(bucket.clone());

            let command = HttpTaskCommand::StartUpload {
                bucket,
                name: name.to_string(),
            };
            client_tx
                .send(HttpTaskRequest {
                    command,
                    start,
                    contraflow_data: Some(contraflow_data),
                })
                .await?;

            self.current_name = Some(name.to_string());
        }

        Ok(())
    }
}

fn get_bucket_name(
    default_bucket: Option<&Value<'static>>,
    meta: Option<&Value>,
) -> Result<String> {
    let bucket = meta
        .get("bucket")
        .or(default_bucket)
        .ok_or(ErrorKind::GoogleCloudStorageError(
            "No bucket name in the metadata",
        ))
        .as_str()
        .ok_or(ErrorKind::GoogleCloudStorageError(
            "Bucket name is not a string",
        ))?
        .to_string();

    Ok(bucket)
}

#[cfg(test)]
mod tests {
    use super::*;
    // use crate::config::Codec;
    // use crate::connectors::reconnect::ConnectionLostNotifier;
    // use async_std::channel::unbounded;
    // use beef::Cow;
    use http_types::{Error, Response, StatusCode};
    use std::sync::atomic::{AtomicBool, Ordering};
    // use tremor_script::ValueAndMeta;
    use tremor_value::literal;

    #[derive(Debug)]
    pub struct MockHttpClient {
        pub config: http_client::Config,
        pub expect_final_request: bool,
        pub inject_failure: AtomicBool,
    }

    #[async_trait::async_trait]
    impl HttpClient for MockHttpClient {
        async fn send(
            &self,
            mut req: http_client::Request,
        ) -> std::result::Result<Response, Error> {
            if self.inject_failure.swap(false, Ordering::AcqRel) {
                return Err(Error::new(
                    StatusCode::InternalServerError,
                    anyhow::Error::msg("injected error"),
                ));
            }

            if req.url().host_str() == Some("start.example.com") {
                let mut response = Response::new(http_types::StatusCode::Ok);
                response.insert_header("Location", "https://upload.example.com/");

                Ok(response)
            } else if req.url().host_str() == Some("upload.example.com") {
                let content_range = req.header("Content-Range").unwrap()[0].as_str();
                let start: usize = content_range
                    .get("bytes ".len()..content_range.find("-").unwrap())
                    .unwrap()
                    .parse()
                    .unwrap();
                let end: usize = content_range
                    .get(content_range.find("-").unwrap() + 1..content_range.find('/').unwrap())
                    .unwrap()
                    .parse()
                    .unwrap();
                let total_size = &content_range[content_range.find("/").unwrap() + 1..];

                if self.expect_final_request {
                    assert_eq!(end + 1, total_size.parse::<usize>().unwrap());
                } else {
                    assert_eq!("*", total_size);
                }

                // NOTE: +1 here because Content-Range is an INCLUSIVE range
                assert_eq!(req.body_bytes().await.unwrap().len(), end - start + 1);

                let mut response = Response::new(http_types::StatusCode::Ok);
                response.insert_header("Range", format!("{}-{}", start, end));

                Ok(response)
            } else {
                panic!("Unexpected request URL: {}", req.url());
            }
        }

        fn set_config(&mut self, config: http_client::Config) -> http_types::Result<()> {
            self.config = config;

            Ok(())
        }

        fn config(&self) -> &http_client::Config {
            &self.config
        }
    }

    #[async_std::test]
    pub async fn fails_when_buffer_size_is_not_divisible_by_256ki() {
        let raw_config = literal!({
            "buffer_size": 256 * 1000
        });

        let builder = Builder {};
        let result = builder
            .build_cfg(
                &Alias::new("", ""),
                &ConnectorConfig {
                    connector_type: Default::default(),
                    codec: None,
                    config: None,
                    preprocessors: None,
                    postprocessors: None,
                    reconnect: Default::default(),
                    metrics_interval_s: None,
                },
                &raw_config,
                &KillSwitch::dummy(),
            )
            .await;

        assert!(result.is_err());
    }

    #[test]
    pub fn chunked_buffer_can_add_data() {
        let mut buffer = ChunkedBuffer::new(10);
        buffer.write(&(1..=10).collect::<Vec<u8>>());

        assert_eq!(0, buffer.start());
        assert_eq!(10, buffer.len());
        assert_eq!(
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            buffer.read_current_block().unwrap()
        );
    }

    #[test]
    pub fn chunked_buffer_will_not_return_a_block_which_is_not_full() {
        let mut buffer = ChunkedBuffer::new(10);
        buffer.write(&(1..=5).collect::<Vec<u8>>());

        assert!(buffer.read_current_block().is_none());
    }

    #[test]
    pub fn chunked_buffer_marking_as_done_removes_data() {
        let mut buffer = ChunkedBuffer::new(10);
        buffer.write(&(1..=15).collect::<Vec<u8>>());

        buffer.mark_done_until(5).unwrap();

        assert_eq!(
            &(6..=15).collect::<Vec<u8>>(),
            buffer.read_current_block().unwrap()
        );
    }

    #[test]
    pub fn chunked_buffer_returns_all_the_data_in_the_final_block() {
        let mut buffer = ChunkedBuffer::new(10);
        buffer.write(&(1..=16).collect::<Vec<u8>>());

        buffer.mark_done_until(5).unwrap();
        assert_eq!(&(6..=16).collect::<Vec<u8>>(), buffer.final_block());
    }

    /* FIXME: this test will need some changes, likely a mocked HTTP task?
    #[async_std::test]
    pub async fn upload_single_file() -> Result<()> {
        let mut sink = GCSWriterSink {
            client: None,
            client_tx: None,
            url: Url::parse("https://start.example.com").unwrap(),
            config: Config {
                endpoint: "https://start.example.com".to_string(),
                connect_timeout: 0,
                buffer_size: 100,
                bucket: None,
            },
            buffers: ChunkedBuffer::new(100),
            current_name: None,
            current_session_url: None,
            default_bucket: None,
        };

        let meta = literal!({
            "gcs_writer": {
                "name": "my-object.txt",
                "bucket": "some_bucket"
            }
        });
        let event = Event {
            id: Default::default(),
            data: ValueAndMeta::from_parts(
                Value::Bytes(Cow::from(
                    (0..=1024 * 1024).map(|_| 0x20u8).collect::<Vec<u8>>(),
                )),
                meta,
            )
            .into(),
            ingest_ns: 0,
            origin_uri: None,
            kind: None,
            is_batch: false,
            cb: Default::default(),
            op_meta: Default::default(),
            transactional: false,
        };

        let (tx, _rx) = unbounded();
        let context = SinkContext {
            uid: Default::default(),
            alias: "".to_string(),
            connector_type: "gcs_writer".into(),
            quiescence_beacon: Default::default(),
            notifier: ConnectionLostNotifier::new(tx),
        };

        let mut serializer = EventSerializer::new(
            Some(Codec::from("json")),
            CodecReq::Required,
            vec![],
            &ConnectorType("gcs_writer".into()),
            "gbq",
        )?;
        sink.connect(&context, &Attempt::default()).await?;
        sink.http_client()
            .unwrap()
            .inject_failure
            .store(true, Ordering::Release);
        sink.on_event("", event, &context, &mut serializer, 0)
            .await?;

        sink.http_client().unwrap().expect_final_request = true;
        sink.http_client()
            .unwrap()
            .inject_failure
            .store(true, Ordering::Release);
        sink.on_stop(&context).await?;

        Ok(())
    }*/
}
