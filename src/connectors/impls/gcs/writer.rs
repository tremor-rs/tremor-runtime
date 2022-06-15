use googapis::google::storage::v2::storage_client::StorageClient;
use tremor_pipeline::{ConfigImpl, Event};
use tremor_value::Value;
use crate::connectors::{CodecReq, Connector, ConnectorBuilder, ConnectorConfig, ConnectorType};
use crate::connectors::prelude::{Attempt, EventSerializer, SinkAddr, SinkContext, SinkManagerBuilder, SinkReply, Result};
use crate::connectors::sink::Sink;

#[derive(Deserialize, Debug)]
pub struct Config {

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
    async fn create_sink(&mut self, _sink_context: SinkContext, _builder: SinkManagerBuilder) -> Result<Option<SinkAddr>> {
        todo!()
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

struct GCSWriterSink {}

#[async_trait::async_trait]
impl Sink for GCSWriterSink {
    async fn on_event(&mut self, input: &str, event: Event, ctx: &SinkContext, serializer: &mut EventSerializer, start: u64) -> Result<SinkReply> {
        todo!()
    }

    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let client = StorageClient::with_interceptor();
    }

    fn auto_ack(&self) -> bool {
        todo!()
    }
}