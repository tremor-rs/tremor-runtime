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

use std::sync::atomic::Ordering;

use crate::connectors::prelude::*;
use crate::errors::{ErrorKind, Result};
use crate::url::ports::{ERR, IN, OUT};
use crate::url::TremorUrl;
use async_broadcast::{broadcast, Receiver, Sender, TryRecvError, TrySendError};
use beef::Cow;
use halfbrown::HashMap;
use tremor_pipeline::{CbAction, Event, EventOriginUri, DEFAULT_STREAM_ID};
use tremor_script::utils::hostname;
use tremor_script::EventPayload;
use tremor_value::prelude::*;

const MEASUREMENT: Cow<'static, str> = Cow::const_str("measurement");
const TAGS: Cow<'static, str> = Cow::const_str("tags");
const FIELDS: Cow<'static, str> = Cow::const_str("fields");
const TIMESTAMP: Cow<'static, str> = Cow::const_str("timestamp");

lazy_static! {
    pub(crate) static ref METRICS_CHANNEL: MetricsChannel =
        MetricsChannel::new(crate::QSIZE.load(Ordering::Relaxed));
}
pub(crate) type MetricsSender = Sender<Msg>;
#[derive(Clone, Debug)]
pub(crate) struct MetricsChannel {
    tx: Sender<Msg>,
    rx: Receiver<Msg>,
}

impl MetricsChannel {
    pub(crate) fn new(qsize: usize) -> Self {
        let (mut tx, rx) = broadcast(qsize);
        // We user overflow so that non collected messages can be removed
        // FIXME: is this what we want? for Metrics it should be good enough
        // we consume them quickly and if not we got bigger problems
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
    fn new(payload: EventPayload, origin_uri: Option<EventOriginUri>) -> Self {
        Self {
            payload,
            origin_uri,
        }
    }
}

/// metrics reporter for connector sources
pub struct SourceReporter {
    artefact_url: TremorUrl,
    metrics_out: u64,
    metrics_err: u64,
    tx: Sender<Msg>,
    flush_interval_ns: Option<u64>,
    last_flush_ns: u64,
}

impl SourceReporter {
    pub(crate) fn new(url: TremorUrl, tx: Sender<Msg>, flush_interval_s: Option<u64>) -> Self {
        Self {
            artefact_url: url,
            metrics_out: 0,
            metrics_err: 0,
            tx,
            flush_interval_ns: flush_interval_s.map(|s| s * 1_000_000_000),
            last_flush_ns: 0,
        }
    }

    pub(crate) fn increment_out(&mut self) {
        self.metrics_out += 1;
    }

    pub(crate) fn increment_err(&mut self) {
        self.metrics_err += 1;
    }

    /// Flush the metrics and send them out if the flush interval is set and the time has come
    /// returns `Some(timestamp)` if it did flush the system metrics
    pub(crate) fn periodic_flush(&mut self, timestamp: u64) -> Option<u64> {
        if let Some(interval) = self.flush_interval_ns {
            if timestamp >= self.last_flush_ns + interval {
                let payload_out =
                    make_metrics_payload(timestamp, OUT, self.metrics_out, &self.artefact_url);
                let payload_err =
                    make_metrics_payload(timestamp, ERR, self.metrics_err, &self.artefact_url);
                send(&self.tx, payload_out, &self.artefact_url);
                send(&self.tx, payload_err, &self.artefact_url);
                self.last_flush_ns = timestamp;
                return Some(timestamp);
            }
        }
        None
    }

    /// simply send source metrics
    pub(crate) fn send_source_metrics(&self, metrics: Vec<EventPayload>) {
        for metric in metrics {
            send(&self.tx, metric, &self.artefact_url);
        }
    }
}

/// metrics reporter for connector sinks
pub(crate) struct MetricsSinkReporter {
    artefact_url: TremorUrl,
    metrics_in: u64,
    tx: Sender<Msg>,
    flush_interval_ns: Option<u64>,
    last_flush_ns: u64,
}

impl MetricsSinkReporter {
    pub(crate) fn new(url: TremorUrl, tx: Sender<Msg>, flush_interval_s: Option<u64>) -> Self {
        Self {
            artefact_url: url,
            metrics_in: 0,
            tx,
            flush_interval_ns: flush_interval_s.map(|s| s * 1_000_000_000),
            last_flush_ns: 0,
        }
    }

    pub(crate) fn increment_in(&mut self) {
        self.metrics_in += 1;
    }

    pub(crate) fn periodic_flush(&mut self, timestamp: u64) -> Option<u64> {
        if let Some(interval) = self.flush_interval_ns {
            if timestamp >= self.last_flush_ns + interval {
                let payload =
                    make_metrics_payload(timestamp, IN, self.metrics_in, &self.artefact_url);
                send(&self.tx, payload, &self.artefact_url);
                self.last_flush_ns = timestamp;
                return Some(timestamp);
            }
        }
        None
    }

    /// simply send source metrics
    pub(crate) fn send_sink_metrics(&self, metrics: Vec<EventPayload>) {
        for metric in metrics {
            send(&self.tx, metric, &self.artefact_url);
        }
    }
}

// this is simple forwarding
#[cfg(not(tarpaulin_include))]
pub(crate) fn send(tx: &Sender<Msg>, metric: EventPayload, artefact_url: &TremorUrl) {
    if let Err(_e) = tx.try_broadcast(Msg::new(metric, None)) {
        error!(
            "[Connector::{}] Error sending to system metrics connector.",
            &artefact_url
        );
    }
}

#[must_use]
fn make_metrics_payload(
    timestamp: u64,
    port: Cow<'static, str>,
    count: u64,
    artefact_url: &TremorUrl,
) -> EventPayload {
    let mut tags: HashMap<Cow<'static, str>, Value<'static>> = HashMap::with_capacity(2);
    tags.insert_nocheck(Cow::from("ramp"), artefact_url.to_string().into());
    tags.insert_nocheck(Cow::from("port"), port.into());

    let value = tremor_pipeline::influx_value(Cow::from("ramp_events"), tags, count, timestamp);
    // full metrics payload
    (value, Value::object()).into()
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
    async fn pull_data(&mut self, _pull_id: u64, _ctx: &SourceContext) -> Result<SourceReply> {
        match self.rx.try_recv() {
            Ok(msg) => Ok(SourceReply::Structured {
                payload: msg.payload,
                origin_uri: msg.origin_uri.unwrap_or_else(|| self.origin_uri.clone()),
                stream: DEFAULT_STREAM_ID,
            }),
            Err(TryRecvError::Closed) => Err(TryRecvError::Closed.into()),
            Err(TryRecvError::Empty) => Ok(SourceReply::Empty(10)),
        }
    }

    fn is_transactional(&self) -> bool {
        false
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
    /// entrypoint for custom metrics events
    async fn on_event(
        &mut self,
        _input: &str,
        event: tremor_pipeline::Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> ResultVec {
        // verify event format
        for (value, _meta) in event.value_meta_iter() {
            // if it fails here an error event is sent to the ERR port of this connector
            verify_metrics_value(value)?;
        }

        let mut res = Vec::with_capacity(1);
        let Event {
            origin_uri, data, ..
        } = event;

        let metrics_msg = Msg::new(data, origin_uri);
        let ack_or_fail = match self.tx.try_broadcast(metrics_msg) {
            Err(TrySendError::Closed(_)) => {
                // channel is closed
                res.push(SinkReply::CB(CbAction::Close));
                SinkReply::Fail
            }
            Err(TrySendError::Full(_)) => SinkReply::Fail,
            _ => SinkReply::Ack,
        };
        if event.transactional {
            res.push(ack_or_fail);
        }
        Ok(res)
    }
}
