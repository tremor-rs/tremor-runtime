use crate::connectors::prelude::{
    Attempt, EventSerializer, Result, SinkAddr, SinkContext, SinkManagerBuilder, SinkReply, Url,
};
use crate::connectors::sink::Sink;
use crate::connectors::utils::url::HttpsDefaults;
use crate::connectors::{
    CodecReq, Connector, ConnectorBuilder, ConnectorConfig, ConnectorType, Context,
};
use gouth::Token;
use http_client::h1::H1Client;
use http_client::{HttpClient};
use http_types::{Method, Request};
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
    buffer_size: usize
    // request_timeout: u64
}

fn default_endpoint() -> String {
    "https://storage.googleapis.com/upload/storage/v1".to_string()
}

fn default_connect_timeout() -> u64 {
    10_000_000_000
}

fn default_buffer_size() -> usize {
    // 1024 * 1024 * 8 // 8MB - the recommended minimum
    256*1024 // FIXME: This is way too low, using only for testing
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
            current_session_url: None
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
    pub fn new(size:usize) -> Self {
        Self {
            data: Vec::with_capacity(size * 2),
            block_size: size,
            buffer_start: 0,
        }
    }

    pub fn mark_done_until(&mut self, position:usize) {
        // FIXME assert that position > self.buffer_start
        let bytes_to_remove = position - self.buffer_start;
        self.data = Vec::from(&self.data[bytes_to_remove..]);
        self.buffer_start += bytes_to_remove;
    }

    pub fn read_current_block(&self) -> Option<&[u8]> {
        if self.data.len() < self.block_size {
            return None;
        }

        return Some(&self.data[..self.block_size]);
    }

    pub fn write(&mut self, data:&[u8]) {
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
    current_session_url: Option<String>
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
        let client = self.client.as_mut().unwrap(); // FIXME: error handling lol
        let token = Token::new().unwrap();

        for (value, meta) in event.value_meta_iter() {
            let meta = ctx.extract_meta(meta);

            let name = meta.get("name").unwrap().as_str().unwrap();

            dbg!(name);
            dbg!(&self.current_session_url);

            if let Some(_current_session_url) = &self.current_session_url {
                if self.current_name.as_ref().map(|x| x.as_str()) != Some(name) {
                    let final_data = self.buffers.final_block();

                    let mut request = Request::new(Method::Put, url::Url::parse(self.current_session_url.as_ref().unwrap()).unwrap());
                    // -1 on the end is here, because Content-Range is inclusive and our range is exclusive
                    request.insert_header("Content-Range", format!("bytes {}-{}/{}", self.buffers.start(), self.buffers.start() + final_data.len() - 1, self.buffers.start() + final_data.len()));
                    request.insert_header("Content-Length", format!("{}", final_data.len()));
                    request.set_body(final_data);

                    dbg!(&request);
                    let mut response = None;
                    for _ in 0..3 {
                        response = Some(client.send(request.clone()).await.unwrap());

                        if let Some(response) = response.as_mut() {
                            dbg!(response.body_string().await.unwrap());

                            if !response.status().is_server_error() {
                                break;
                            }
                        }
                    };
                    dbg!(&response);
                }
            }

            if self.current_session_url.is_none() || self.current_name.as_ref().map(|x| x.as_str()) != Some(name) {
                let url = url::Url::parse(&format!(
                    "{}/b/{}/o?name={}&uploadType=resumable",
                    self.url,
                    meta.get("bucket").unwrap().as_str().unwrap(),
                    name
                ))
                    .unwrap();
                let mut request = Request::new(Method::Post, url);
                request.insert_header("Authorization", token.header_value().unwrap().to_string());
                let response = client.send(request).await;

                let response = response.unwrap();
                self.current_session_url = Some(response.header("Location").unwrap().get(0).unwrap().to_string());
                self.current_name = Some(name.into());
            }

            self.buffers.write(&serializer.serialize(value, event.ingest_ns).unwrap()[0]);

            if let Some(data) = self.buffers.read_current_block() {
                let mut request = Request::new(Method::Put, url::Url::parse(self.current_session_url.as_ref().unwrap()).unwrap());
                // -1 on the end is here, because Content-Range is inclusive and our range is exclusive
                request.insert_header("Content-Range", format!("bytes {}-{}/*", self.buffers.start(), self.buffers.end() - 1));
                request.insert_header("Content-Length", format!("{}", self.buffers.end() - self.buffers.start()));
                request.set_body(data);

                dbg!(&request);

                let mut response = None;
                for _ in 0..3 {
                    response = Some(client.send(request.clone()).await.unwrap());

                    if let Some(response) = response.as_ref() {
                        if !response.status().is_server_error() && response.header("Range").is_some() {
                            break;
                        }

                        dbg!(&response);
                    }
                };

                if let Some(mut response) = response {
                    if  response.status().is_server_error() {
                        return Err("Received server errors from Google Cloud Storage".into());
                    }

                    dbg!(&response);

                    dbg!(response.body_string().await.unwrap());

                    if let Some(raw_range) = response.header("Range") {
                        let raw_range = raw_range[0].as_str();

                        // Range format: bytes=0-262143
                        let range_end = &raw_range[raw_range.find('-').unwrap() + 1..];

                        self.buffers.mark_done_until(range_end.parse().unwrap());
                    } else {
                        // FIXME: not sure if this is the correct behaviour - google documents that the header should always be present,  but it does not seem to be true
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
        let client = H1Client::new();

        self.client = Some(client);
        self.current_name = None;
        self.buffers = Buffers::new(self.config.buffer_size); // FIXME: validate that the buffer size is a multiple of 256kB, as required by GCS

        Ok(true)
    }

    fn auto_ack(&self) -> bool {
        false
    }
}
