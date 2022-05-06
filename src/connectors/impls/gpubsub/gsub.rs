use crate::connectors::google::AuthInterceptor;
use crate::connectors::prelude::*;
use dashmap::DashMap;
use googapis::google::pubsub::v1::subscriber_client::SubscriberClient;
use googapis::google::pubsub::v1::{AcknowledgeRequest, PullRequest};
use gouth::Token;
use serde::Deserialize;
use std::fmt::Debug;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tonic::codegen::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::Status;
use tremor_pipeline::ConfigImpl;

#[derive(Deserialize, Clone)]
struct Config {
    pub connect_timeout: u64,
}
impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "gsub".into()
    }

    async fn build(&self, alias: &str, raw_config: &ConnectorConfig) -> Result<Box<dyn Connector>> {
        if let Some(raw) = &raw_config.config {
            let config = Config::new(raw)?;
            Ok(Box::new(GSub { config }))
        } else {
            Err(ErrorKind::MissingConfiguration(alias.to_string()).into())
        }
    }
}

struct GSub {
    config: Config,
}

struct GSubSource {
    config: Config,
    client: Option<SubscriberClient<InterceptedService<Channel, AuthInterceptor>>>,
    ack_ids: DashMap<u64, String>,
    ack_counter: AtomicU64,
}

impl GSubSource {
    pub fn new(config: Config) -> Self {
        GSubSource {
            config,
            client: None,
            ack_ids: DashMap::new(),
            ack_counter: AtomicU64::new(0),
        }
    }
}

#[async_trait::async_trait]
impl Source for GSubSource {
    async fn connect(&mut self, _ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        let token = Token::new()?;

        let tls_config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(googapis::CERTIFICATES))
            .domain_name("pubsub.googleapis.com");

        let channel = Channel::from_static("https://pubsub.googleapis.com")
            .connect_timeout(Duration::from_nanos(self.config.connect_timeout))
            .tls_config(tls_config)?
            .connect()
            .await;

        let channel = dbg!(channel)?;

        let client = SubscriberClient::with_interceptor(
            channel,
            AuthInterceptor {
                token: Box::new(move || match token.header_value() {
                    Ok(val) => Ok(val),
                    Err(e) => {
                        error!("Failed to get token for BigQuery: {}", e);

                        Err(Status::unavailable(
                            "Failed to retrieve authentication token.",
                        ))
                    }
                }),
            },
        );

        self.client = Some(client);

        Ok(true)
    }

    async fn pull_data(&mut self, pull_id: &mut u64, ctx: &SourceContext) -> Result<SourceReply> {
        let client = self
            .client
            .as_mut()
            .ok_or(ErrorKind::BigQueryClientNotAvailable(
                // fixme use an error for GPubSub
                "The client is not connected",
            ))?;

        let response = client
            .pull(PullRequest {
                subscription: "projects/wf-gcp-us-tremor-sbx/subscriptions/test-subscription-a"
                    .to_string(),
                // fixme config?
                max_messages: 1,
                ..Default::default()
            })
            .await?
            .into_inner();

        if response.received_messages.len() == 0 {
            return self.pull_data(pull_id, ctx).await;
        }

        *pull_id = self.ack_counter.fetch_add(1, Ordering::AcqRel);
        self.ack_ids
            .insert(*pull_id, response.received_messages[0].ack_id.clone());

        Ok(SourceReply::Structured {
            origin_uri: Default::default(),
            payload: EventPayload::new(vec![], |_| {
                let message = response.received_messages[0].message.as_ref().unwrap();

                ValueAndMeta::from_parts(
                    Value::String(
                        String::from_utf8_lossy(&message.data[..])
                            .to_string()
                            .into(),
                    ),
                    Value::null(),
                )
            }),
            stream: 0,
            port: None,
        })
    }

    fn is_transactional(&self) -> bool {
        true
    }

    fn asynchronous(&self) -> bool {
        true
    }

    async fn ack(&mut self, _stream_id: u64, pull_id: u64, _ctx: &SourceContext) -> Result<()> {
        let client = self
            .client
            .as_mut()
            .ok_or(ErrorKind::BigQueryClientNotAvailable(
                // fixme use an error for GPubSub
                "The client is not connected",
            ))?;

        client
            .acknowledge(AcknowledgeRequest {
                subscription: "projects/wf-gcp-us-tremor-sbx/subscriptions/test-subscription-a"
                    .to_string(),
                ack_ids: vec![self.ack_ids.remove(&pull_id).unwrap().1],
            })
            .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl Connector for GSub {
    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = GSubSource::new(self.config.clone());
        builder.spawn(source, source_context).map(Some)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }
}
