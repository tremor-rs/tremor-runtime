use std::time::Duration;
use googapis::google::storage::v2::storage_client::StorageClient;
use gouth::Token;
use tonic::codegen::InterceptedService;
use tonic::Status;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tremor_pipeline::{ConfigImpl, Event};
use tremor_value::Value;
use crate::connectors::{CodecReq, Connector, ConnectorBuilder, ConnectorConfig, ConnectorType};
use crate::connectors::google::AuthInterceptor;
use crate::connectors::prelude::{Attempt, EventSerializer, SinkAddr, SinkContext, SinkManagerBuilder, SinkReply, Result, Url};
use crate::connectors::sink::Sink;
use crate::connectors::utils::url::HttpsDefaults;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    endpoint: String,
    connect_timeout: u64,
    request_timeout: u64
}

impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        ConnectorType("gcs-writer".into())
    }

    async fn build_cfg(&self, _alias: &str, _config: &ConnectorConfig, connector_config: &Value) -> Result<Box<dyn Connector>> {
        let config = Config::new(connector_config)?;

        Ok(Box::new(GCSWriterConnector { config }))
    }
}

struct GCSWriterConnector {
    config:Config
}

#[async_trait::async_trait]
impl Connector for GCSWriterConnector {
    async fn create_sink(&mut self, sink_context: SinkContext, builder: SinkManagerBuilder) -> Result<Option<SinkAddr>> {
        let url = Url::<HttpsDefaults>::parse(&self.config.endpoint)?;
        let sink = GCSWriterSink{
            client: None,
            url,
            config: self.config.clone()
        };

        let addr = builder.spawn(sink, sink_context)?;
        Ok(Some(addr))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

struct GCSWriterSink {
    client: Option<StorageClient<InterceptedService<Channel, AuthInterceptor>>>,
    url: Url<HttpsDefaults>,
    config: Config
}

#[async_trait::async_trait]
impl Sink for GCSWriterSink {
    async fn on_event(&mut self, input: &str, event: Event, ctx: &SinkContext, serializer: &mut EventSerializer, start: u64) -> Result<SinkReply> {
        todo!()
    }

    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let mut channel = Channel::from_shared(self.config.endpoint.clone())?
            .connect_timeout(Duration::from_nanos(self.config.connect_timeout));
        if self.url.scheme() == "https" {
            let tls_config = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(googapis::CERTIFICATES))
                .domain_name(
                    self.url
                        .host_str()
                        .ok_or_else(|| Status::unavailable("The endpoint is missing a hostname"))?
                        .to_string(),
                );

            channel = channel.tls_config(tls_config)?;
        }

        let channel = channel.connect().await?;
        let token = Token::new()?;
        let client = StorageClient::with_interceptor(channel, AuthInterceptor {
            token: Box::new(move || {
                token.header_value().map_err(|_| {
                    Status::unavailable("Failed to retrieve authentication token.")
                })
            }),
        });

        self.client = Some(client);

        Ok(true)
    }

    fn auto_ack(&self) -> bool {
        todo!()
    }
}