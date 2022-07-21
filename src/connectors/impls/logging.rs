use crate::connectors::prelude::*;
use async_broadcast::Receiver;
use tremor_pipeline::{LoggingMsg, LOGGING_CHANNEL};

use crate::connectors::{ConnectorBuilder, ConnectorType, prelude::KillSwitch};


struct LoggingConnector{
	rx: Receiver<LoggingMsg>,
}

impl LoggingConnector {
    pub(crate) fn new() -> Self {
        Self {
            rx: LOGGING_CHANNEL.rx(),
        }
    }
}

#[derive(Debug, Default)]
struct Builder {

}
#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "logging".into()
    }
    async fn build(
        &self,
        _id: &str,
        _config: &ConnectorConfig,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        Ok(Box::new(LoggingConnector::new()))
    }
}
#[async_trait::async_trait()]
impl Connector for LoggingConnector {
    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        Ok(true)
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = LoggingSource::new(self.rx.clone());
        let addr = builder.spawn(source, source_context)?;
        Ok(Some(addr))
    }

    async fn create_sink(
        &mut self,
        _sink_context: SinkContext,
        _builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {   
        Ok(None)
    }
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }
}
impl LoggingSource {
    pub(crate) fn new(rx: Receiver<LoggingMsg>) -> Self {
        Self {
            rx,
            origin_uri: EventOriginUri {
                scheme: "tremor-logging".to_string(),
                host: hostname(),
                port: None,
                path: vec![],
            },
        }
    }
}

pub(crate) struct LoggingSource {
    rx: Receiver<LoggingMsg>,
    origin_uri: EventOriginUri,
}

#[async_trait::async_trait()]
impl Source for LoggingSource {
    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        let msg = self
            .rx
            .recv()
            .await
            .map_err(|e| Error::from(format!("error: {}", e)))?;
        Ok(SourceReply::Structured {
            payload: msg.payload,
            origin_uri: msg.origin_uri.unwrap_or_else(|| self.origin_uri.clone()),
            stream: DEFAULT_STREAM_ID,
            port: None,
        })
    }

    fn is_transactional(&self) -> bool {
        false
    }

    /// The logging connector is actually `asynchronous` in that its data is produced outside the source task
    /// (and outside of the control of the `pull_data` function).
    ///
    /// But we set it to `false` here, as in case of quiescence
    /// we don't need to flush logging data. Also the producing ends do not use the quiescence_beacon
    /// which would tell them to stop sending. There could be multiple logging connectors running at the same time
    /// and one connector quiescing should not lead to logging being stopped for each and every other connector.
    fn asynchronous(&self) -> bool {
        false
    }
}


//
