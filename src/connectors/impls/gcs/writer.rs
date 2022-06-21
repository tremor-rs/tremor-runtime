use gouth::Token;
use crate::connectors::prelude::{
    Attempt, EventSerializer, Result, SinkAddr, SinkContext, SinkManagerBuilder, SinkReply, Url,
};
use crate::connectors::sink::Sink;
use crate::connectors::utils::url::HttpsDefaults;
use crate::connectors::{CodecReq, Connector, ConnectorBuilder, ConnectorConfig, ConnectorType, Context};
use http_client::h1::H1Client;
use http_client::HttpClient;
use http_types::{Method, Request};
use value_trait::ValueAccess;
use tremor_pipeline::{ConfigImpl, Event};
use tremor_value::Value;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    #[serde(default="default_endpoint")]
    endpoint: String,
    #[serde(default="default_connect_timeout")]
    #[allow(unused)] // fixme! use or remove
    connect_timeout: u64,
    // request_timeout: u64
}

fn default_endpoint() -> String {
    "https://storage.googleapis.com/upload/storage/v1".to_string()
}

fn default_connect_timeout() -> u64 {
    10_000_000_000
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
        };

        let addr = builder.spawn(sink, sink_context)?;
        Ok(Some(addr))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

struct GCSWriterSink {
    client: Option<H1Client>,
    url: Url<HttpsDefaults>,
    #[allow(unused)] // fixme! use or remove
    config: Config,
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
        let client = self.client.as_mut().unwrap(); // fixme error handling lol
        let token = Token::new().unwrap();

        for (value, meta) in event.value_meta_iter() {
            let meta = ctx.extract_meta(meta);

            let url = url::Url::parse(
                &format!(
                    "{}/b/{}/o?name={}",
                    self.url,
                    meta.get("bucket").unwrap().as_str().unwrap(),
                    meta.get("name").unwrap().as_str().unwrap()
                )
            ).unwrap();
            let mut request = Request::new(Method::Post, url);
            request.insert_header("Authorization", token.header_value().unwrap().to_string());
            request.set_body(serializer.serialize(value, event.ingest_ns).unwrap()[0].clone());
            let response = client.send(request).await;

            dbg!(response.unwrap().body_string().await.unwrap());
        }

        Ok(SinkReply::ACK)
    }

    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let client = H1Client::new();

        self.client = Some(client);

        Ok(true)
    }

    fn auto_ack(&self) -> bool {
        false
    }
}
