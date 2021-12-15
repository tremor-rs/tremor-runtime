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
use crate::connectors::utils::metrics::METRICS_CHANNEL;
use async_broadcast::{broadcast, Receiver, Sender, TryRecvError, TrySendError};
use beef::Cow;
use tremor_script::utils::hostname;

const MEASUREMENT: Cow<'static, str> = Cow::const_str("measurement");
const TAGS: Cow<'static, str> = Cow::const_str("tags");
const FIELDS: Cow<'static, str> = Cow::const_str("fields");
const TIMESTAMP: Cow<'static, str> = Cow::const_str("timestamp");

#[derive(Clone, Debug)]
pub(crate) struct MetricsChannel {
    tx: Sender<Msg>,
    rx: Receiver<Msg>,
}

impl MetricsChannel {
    pub(crate) fn new(qsize: usize) -> Self {
        let (mut tx, rx) = broadcast(qsize);
        // We user overflow so that non collected messages can be removed
        // Ffor Metrics it should be good enough we consume them quickly
        // and if not we got bigger problems
        tx.set_overflow(true);
        Self { tx, rx }
    }

    pub(crate) fn tx(&self) -> Sender<Msg> {
        self.tx.clone()
    }
    pub(crate) fn rx(&self) -> Receiver<Msg> {
        self.rx.clone()
    }
}
#[derive(Debug, Clone)]
pub struct Msg {
    payload: EventPayload,
    origin_uri: Option<EventOriginUri>,
}

impl Msg {
    /// creates a new message
    pub fn new(payload: EventPayload, origin_uri: Option<EventOriginUri>) -> Self {
        Self {
            payload,
            origin_uri,
        }
    }
}

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
    tx: Sender<Msg>,
    rx: Receiver<Msg>,
}

impl MetricsConnector {
    pub(crate) fn new() -> Self {
        Self {
            tx: METRICS_CHANNEL.tx(),
            rx: METRICS_CHANNEL.rx(),
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
    async fn from_config(
        &self,
        _id: &str,
        _config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        Ok(Box::new(MetricsConnector::new()))
    }
}

#[async_trait::async_trait()]
impl Connector for MetricsConnector {
    fn is_structured(&self) -> bool {
        true
    }

    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        Ok(!self.tx.is_closed())
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = MetricsSource::new(self.rx.clone());
        let addr = builder.spawn(source, source_context)?;
        Ok(Some(addr))
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = MetricsSink::new(self.tx.clone());
        let addr = builder.spawn(sink, sink_context)?;
        Ok(Some(addr))
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}

pub(crate) struct MetricsSource {
    rx: Receiver<Msg>,
    origin_uri: EventOriginUri,
}

impl MetricsSource {
    pub(crate) fn new(rx: Receiver<Msg>) -> Self {
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
        match self.rx.try_recv() {
            Ok(msg) => Ok(SourceReply::Structured {
                payload: msg.payload,
                origin_uri: msg.origin_uri.unwrap_or_else(|| self.origin_uri.clone()),
                stream: DEFAULT_STREAM_ID,
                port: None,
            }),
            Err(TryRecvError::Closed) => Err(TryRecvError::Closed.into()),
            Err(TryRecvError::Empty) => Ok(SourceReply::Empty(10)),
        }
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        true
    }
}

pub(crate) struct MetricsSink {
    tx: Sender<Msg>,
}

impl MetricsSink {
    pub(crate) fn new(tx: Sender<Msg>) -> Self {
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
            // if it fails here an error event is sent to the ERR port of this connector
            verify_metrics_value(value)?;
        }

        let Event {
            origin_uri, data, ..
        } = event;

        let metrics_msg = Msg::new(data, origin_uri);
        let ack_or_fail = match self.tx.try_broadcast(metrics_msg) {
            Err(TrySendError::Closed(_)) => {
                // channel is closed
                SinkReply {
                    ack: SinkAck::Fail,
                    cb: CbAction::Close,
                }
            }
            Err(TrySendError::Full(_)) => SinkReply::FAIL,
            _ => SinkReply::ACK,
        };

        Ok(ack_or_fail)
    }
}
