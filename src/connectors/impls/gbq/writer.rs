mod sink;

use crate::connectors::impls::gbq::writer::sink::GbqSink;
use crate::connectors::prelude::*;
use crate::connectors::{Connector, ConnectorBuilder, ConnectorConfig, ConnectorType};
use serde::Deserialize;
use tremor_pipeline::ConfigImpl;

#[derive(Deserialize, Clone)]
pub(crate) struct Config {
    pub table_id: String,
}
impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

struct Gbq {
    config: Config,
}

#[async_trait::async_trait]
impl Connector for Gbq {
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = GbqSink::new(self.config.clone()).await?;

        builder.spawn(sink, sink_context).map(Some)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }
}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "gbq".into()
    }

    async fn build(&self, alias: &str, config: &ConnectorConfig) -> Result<Box<dyn Connector>> {
        if let Some(raw_config) = &config.config {
            let config = Config::new(raw_config)?;
            Ok(Box::new(Gbq { config }))
        } else {
            Err(ErrorKind::MissingConfiguration(alias.to_string()).into())
        }
    }
}
