use crate::connectors::google::AuthInterceptor;
use crate::connectors::prelude::{
    Attempt, ErrorKind, EventSerializer, SinkAddr, SinkContext, SinkManagerBuilder, SinkReply, Url,
};
use crate::connectors::sink::Sink;
use crate::connectors::utils::url::HttpsDefaults;
use crate::connectors::{
    CodecReq, Connector, ConnectorBuilder, ConnectorConfig, ConnectorContext, ConnectorType,
};
use crate::errors::Result;
use googapis::google::pubsub::v1::publisher_client::PublisherClient;
use googapis::google::pubsub::v1::{PublishRequest, PubsubMessage};
use gouth::Token;
use std::time::Duration;
use tonic::codegen::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::Status;
use tremor_pipeline::{ConfigImpl, Event};

#[derive(Deserialize, Clone)]
pub struct Config {
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: u64,
    #[serde(default = "default_endpoint")]
    pub endpoint: String,
    pub topic: String,
}

fn default_endpoint() -> String {
    "https://pubsub.googleapis.com".into()
}
fn default_connect_timeout() -> u64 {
    1_000_000_000u64 // 1 second
}

impl ConfigImpl for Config {}

#[derive(Default, Debug)]
pub(crate) struct Builder {}

#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        ConnectorType("gpubsub_producer".to_string())
    }

    async fn build(&self, alias: &str, config: &ConnectorConfig) -> Result<Box<dyn Connector>> {
        if let Some(raw_config) = &config.config {
            let config = Config::new(raw_config)?;

            Ok(Box::new(GpubConnector { config }))
        } else {
            Err(ErrorKind::MissingConfiguration(alias.to_string()).into())
        }
    }
}

struct GpubConnector {
    config: Config,
}

#[async_trait::async_trait()]
impl Connector for GpubConnector {
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = GpubSink {
            config: self.config.clone(),
            url: Url::parse(&self.config.endpoint)?,
            client: None,
        };
        builder.spawn(sink, sink_context).map(Some)
    }

    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        Ok(true)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

struct GpubSink {
    config: Config,
    url: Url<HttpsDefaults>,

    client: Option<PublisherClient<InterceptedService<Channel, AuthInterceptor>>>,
}

#[async_trait::async_trait()]
impl Sink for GpubSink {
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

        let client = PublisherClient::with_interceptor(
            channel.clone(),
            AuthInterceptor {
                token: Box::new(move || {
                    token.header_value().map_err(|_| {
                        Status::unavailable("Failed to retrieve authentication token.")
                    })
                }),
            },
        );

        self.client = Some(client);

        Ok(true)
    }

    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        _ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let client = self.client.as_mut().ok_or(ErrorKind::ClientNotAvailable(
            "PubSub",
            "The publisher is not connected",
        ))?;

        let mut messages = vec![];

        for e in event.value_iter() {
            messages.push(PubsubMessage {
                data: serializer.serialize(e, event.ingest_ns).unwrap()[0].clone(),
                attributes: Default::default(),
                message_id: "".to_string(),
                publish_time: None,
                ordering_key: "".to_string(),
            });
        }

        client
            .publish(PublishRequest {
                topic: self.config.topic.clone(),
                messages,
            })
            .await
            .unwrap();

        Ok(SinkReply::ACK)
    }

    fn auto_ack(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        true
    }
}
