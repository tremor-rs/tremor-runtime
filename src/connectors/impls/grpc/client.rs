use async_std::channel::{Sender, Receiver, unbounded, TryRecvError};
use async_std::sync::{Arc, Mutex};

use crate::connectors::prelude::*;

pub mod tremor_grpc {
    tonic::include_proto!("grpc_client");
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
struct Config {
    host: String,
    port: u16
}

impl ConfigImpl for Config {}

#[derive(Debug, Clone)]
pub struct GrpcClient {
    tx: Sender<EventPayload>,
    rx: Receiver<EventPayload>,
    config: Config,
    event_origin_uri: EventOriginUri,
    sink: Option<GrpcClientSink>,
}

#[derive(Debug, Clone)]
struct GrpcClientSink {
    grpc_client_handler: Arc<Mutex<Option<tremor_grpc::GrpcClientHandler>>>,
    error_rx: Receiver<Error>,
    error_tx: Sender<Error>
}

#[derive(Debug, Clone)]
struct GrpcClientSource {
    rx: Receiver<EventPayload>,
    origin_uri: EventOriginUri
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "grpc_client".into()
    }

    async fn from_config(
        &self,
        _id: &TremorUrl,
        config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(raw_config) = config {
            let config = Config::new(raw_config)?;
            let (tx, rx) = unbounded();
            let event_origin_uri = EventOriginUri {
                scheme: "tremor-grpc-unary".to_string(),
                host: config.host.clone(),
                port: Some(config.port.clone()),
                path: vec![]
            };
            Ok(Box::new(GrpcClient {
                config,
                tx,
                rx,
                event_origin_uri,
                sink: None
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(String::from("GrpcClient")).into())
        }
    }
}

#[async_trait::async_trait()]
impl Connector for GrpcClient {
    fn is_structured(&self) -> bool {
        true
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
        ) -> Result<Option<SourceAddr>> {
        let source = GrpcClientSource {
            rx: self.rx.clone(),
            origin_uri: self.event_origin_uri.clone()
        };
        let addr = builder.spawn(source, ctx)?;
        Ok(Some(addr))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let (error_tx, error_rx) = unbounded();
        let sink = GrpcClientSink { 
            grpc_client_handler: Arc::new(Mutex::new(None)),
            error_rx,
            error_tx
        };
        self.sink = Some(sink.clone());
        let addr = builder.spawn(sink, ctx)?;
        Ok(Some(addr))
    }

    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        let grpc_client_handler = tremor_grpc::GrpcClientHandler::connect(format!("{}:{}", self.config.host, self.config.port), self.tx.clone()).await?;
        if let Some(sink) = &mut self.sink {
            let mut client_handler = sink.grpc_client_handler.lock().await;
            *client_handler = Some(grpc_client_handler);
        }
        Ok(true)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}

#[async_trait::async_trait]
impl Sink for GrpcClientSink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        if let Ok(error) = self.error_rx.try_recv() {
            return Err(error);
        }
        let mut grpc_client_handler = self.grpc_client_handler.lock().await;
        if let Some(ref mut client_handler) = grpc_client_handler.as_mut() {
            client_handler.send_request(event, self.error_tx.clone()).await?;
        }
        Ok(SinkReply::NONE)
    }

    async fn on_signal(
        &mut self,
        signal: Event,
        _ctx: &SinkContext,
        serializer: &mut EventSerializer,
    ) -> Result<SinkReply> {
        Ok(SinkReply::default()) 
    }

    fn asynchronous(&self) -> bool {
        // events are delivered asynchronously on their stream tasks
        true
    }

    fn auto_ack(&self) -> bool {
        // we handle ack/fail in the asynchronous streams
        false
    }
}

#[async_trait::async_trait]
impl Source for GrpcClientSource {
    async fn pull_data(&mut self, _pull_id: u64, _ctx: &SourceContext) -> Result<SourceReply> {
        match self.rx.try_recv() {
            Ok(event) => Ok(SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                payload: event,
                stream: 0,
                port: None
            }),
            Err(TryRecvError::Empty) => {
                // TODO: configure pull interval in connector config?
                Ok(SourceReply::Empty(DEFAULT_POLL_INTERVAL))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// this source is not handling acks/fails
    fn is_transactional(&self) -> bool {
        false
    }
}
