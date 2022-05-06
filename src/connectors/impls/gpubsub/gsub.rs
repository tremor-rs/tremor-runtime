use crate::connectors::google::AuthInterceptor;
use crate::connectors::prelude::*;
use dashmap::DashMap;
use googapis::google::pubsub::v1::subscriber_client::SubscriberClient;
use googapis::google::pubsub::v1::{AcknowledgeRequest, PubsubMessage, PullRequest, ReceivedMessage};
use gouth::Token;
use serde::Deserialize;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use async_std::channel::{Receiver, Sender};
use async_std::sync::Mutex;
use tonic::codegen::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::Status;
use tremor_pipeline::ConfigImpl;

#[derive(Deserialize, Clone)]
struct Config {
    pub connect_timeout: u64,
    pub subscription_id: String,
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

type PubSubClient = SubscriberClient<InterceptedService<Channel, AuthInterceptor>>;

struct GSubSource {
    config: Config,
    client: Option<Arc<Mutex<PubSubClient>>>,
    receiver: Option<Receiver<(u64, Vec<u8>)>>,
    ack_ids: Arc<DashMap<u64, String>>,
}

impl GSubSource {
    pub fn new(config: Config) -> Self {
        GSubSource {
            config,
            client: None,
            receiver: None,
            ack_ids: Arc::new(DashMap::new()),
        }
    }
}

async fn consumer_task(client: Arc<Mutex<PubSubClient>>, sender:Sender<(u64, Vec<u8>)>, ack_ids:Arc<DashMap<u64, String>>, subscription_id:String) {
    let mut ack_counter = 0;

    loop {
        let response = client
            .lock()
            .await
            .pull(PullRequest {
                subscription: subscription_id.clone(),
                // fixme config?
                max_messages: 100,
                ..Default::default()
            })
            .await
            .unwrap()// fixme
            .into_inner();

        for message in response.received_messages {
            let ReceivedMessage { ack_id, message: msg, delivery_attempt, } = message;

            if let Some(PubsubMessage { data, .. }) = msg {
                ack_counter += 1;
                ack_ids.insert(ack_counter, ack_id);
                sender.send((ack_counter, data)).await.unwrap();
            }
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
                        Err(Status::unavailable(
                            "Failed to retrieve authentication token.",
                        ))
                    }
                }),
            },
        );

        let client = Arc::new(Mutex::new(client));

        let (tx, rx) = async_std::channel::bounded(QSIZE.load(Ordering::Relaxed));

        async_std::task::spawn(consumer_task(client.clone(), tx, self.ack_ids.clone(), self.config.subscription_id.clone()));

        self.receiver = Some(rx);
        self.client = Some(client);

        Ok(true)
    }

    async fn pull_data(&mut self, pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        let receiver = self
            .receiver
            .as_mut()
            .ok_or(ErrorKind::BigQueryClientNotAvailable(
                // fixme use an error for GPubSub
                "The receiver is not connected",
            ))?;

        let (ack_id, data) = receiver.recv().await?;

        *pull_id = ack_id;

        Ok(SourceReply::Data {
            origin_uri: Default::default(),
            data,
            meta: None,
            stream: None,
            port: None,
            codec_overwrite: None,
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
            .lock()
            .await
            .acknowledge(AcknowledgeRequest {
                subscription: self.config.subscription_id.clone(),
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
        CodecReq::Required
    }
}
