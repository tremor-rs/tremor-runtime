// Copyright 2021, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::connectors::prelude::*;
use beef::Cow;
use tokio::sync::broadcast::{error::RecvError, Receiver, Sender};
use tremor_pipeline::{MetricsMsg, METRICS_CHANNEL};
use tremor_script::utils::hostname;

const MEASUREMENT: Cow<'static, str> = Cow::const_str("measurement");
const TAGS: Cow<'static, str> = Cow::const_str("tags");
const FIELDS: Cow<'static, str> = Cow::const_str("fields");
const TIMESTAMP: Cow<'static, str> = Cow::const_str("timestamp");

/// This is a system connector to collect and forward metrics.
/// System metrics are fed to this connector and can be received by binding this connector's `out` port to a pipeline to handle metrics events.
/// It can also be used to send custom metrics and have them handled the same way as system metrics.
/// Custom metrics need to be sent as events to the `in` port of this connector.
///
/// TODO: describe metrics event format and write stdlib function to help with that
///
/// There should be only one instance around all the time, identified by `tremor://localhost/connector/system::metrics/system`
///
pub(crate) struct MetricsConnector {
    tx: Sender<MetricsMsg>,
}

impl MetricsConnector {
    pub(crate) fn new() -> Self {
        Self {
            tx: METRICS_CHANNEL.tx(),
        }
    }
}

/// builder for the metrics connector

#[derive(Debug, Default)]
pub(crate) struct Builder {}
#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "metrics".into()
    }
    async fn build(
        &self,
        _id: &Alias,
        _config: &ConnectorConfig,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        Ok(Box::new(MetricsConnector::new()))
    }
}

#[async_trait::async_trait()]
impl Connector for MetricsConnector {
    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        Ok(true)
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = MetricsSource::new(self.tx.subscribe());
        Ok(Some(builder.spawn(source, ctx)))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = MetricsSink::new(self.tx.clone());
        Ok(Some(builder.spawn(sink, ctx)))
    }
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }
}

pub(crate) struct MetricsSource {
    rx: Receiver<MetricsMsg>,
    origin_uri: EventOriginUri,
}

impl MetricsSource {
    pub(crate) fn new(rx: Receiver<MetricsMsg>) -> Self {
        Self {
            rx,
            origin_uri: EventOriginUri {
                scheme: "tremor-metrics".to_string(),
                host: hostname(),
                port: None,
                path: vec![],
            },
        }
    }
}

#[async_trait::async_trait()]
impl Source for MetricsSource {
    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        loop {
            match self.rx.recv().await {
                Ok(msg) => {
                    break Ok(SourceReply::Structured {
                        payload: msg.payload,
                        origin_uri: msg.origin_uri.unwrap_or_else(|| self.origin_uri.clone()),
                        stream: DEFAULT_STREAM_ID,
                        port: None,
                    })
                }
                Err(RecvError::Lagged(_)) => continue, // try again, this is expected
                Err(e) => {
                    break Err(e.into());
                }
            }
        }
    }

    fn is_transactional(&self) -> bool {
        false
    }

    /// The metrics connector is actually `asynchronous` in that its data is produced outside the source task
    /// (and outside of the control of the `pull_data` function).
    ///
    /// But we set it to `false` here, as in case of quiescence
    /// we don't need to flush metrics data. Also the producing ends do not use the quiescence_beacon
    /// which would tell them to stop sending. There could be multiple metrics connectors running at the same time
    /// and one connector quiescing should not lead to metrics being stopped for each and every other connector.
    fn asynchronous(&self) -> bool {
        false
    }
}

pub(crate) struct MetricsSink {
    tx: Sender<MetricsMsg>,
}

impl MetricsSink {
    pub(crate) fn new(tx: Sender<MetricsMsg>) -> Self {
        Self { tx }
    }
}

/// verify a value for conformance with the required metrics event format
pub(crate) fn verify_metrics_value(value: &Value<'_>) -> Result<()> {
    value
        .as_object()
        .and_then(|obj| {
            // check presence of fields
            obj.get(&MEASUREMENT)
                .zip(obj.get(&TAGS))
                .zip(obj.get(&FIELDS))
                .zip(obj.get(&TIMESTAMP))
        })
        .and_then(|(((measurement, tags), fields), timestamp)| {
            // check correct types
            if measurement.is_str()
                && tags.is_object()
                && fields.is_object()
                && timestamp.is_integer()
            {
                Some(())
            } else {
                None
            }
        })
        .ok_or_else(|| ErrorKind::InvalidMetricsData.into())
}

/// passing events through to the source channel
#[async_trait::async_trait()]
impl Sink for MetricsSink {
    fn auto_ack(&self) -> bool {
        true
    }

    /// entrypoint for custom metrics events
    async fn on_event(
        &mut self,
        _input: &str,
        event: tremor_pipeline::Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        // verify event format
        for (value, _meta) in event.value_meta_iter() {
            verify_metrics_value(value)?;
        }

        let Event {
            origin_uri, data, ..
        } = event;

        let metrics_msg = MetricsMsg::new(data, origin_uri);
        let ack_or_fail = match self.tx.send(metrics_msg) {
            Err(_) => SinkReply::FAIL,
            _ => SinkReply::ACK,
        };

        Ok(ack_or_fail)
    }
}
