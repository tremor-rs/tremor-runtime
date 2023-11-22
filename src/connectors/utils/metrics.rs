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

use crate::system::flow::AppContext;
use beef::Cow;
use simd_json::ObjectHasher;
use tremor_common::{
    alias,
    ports::{Port, ERR, IN, OUT},
};
use tremor_pipeline::metrics::{value, value_count};
use tremor_pipeline::MetricsSender;
use tremor_script::EventPayload;
use tremor_value::prelude::*;

const APP: Cow<'static, str> = Cow::const_str("app");
const INSTANCE: Cow<'static, str> = Cow::const_str("instance");
const FLOW: Cow<'static, str> = Cow::const_str("flow");
const CONNECTOR: Cow<'static, str> = Cow::const_str("connector");
const PORT: Cow<'static, str> = Cow::const_str("port");
const CONNECTOR_EVENTS: Cow<'static, str> = Cow::const_str("connector_events");

/// metrics reporter for connector sources
pub(crate) struct SourceReporter {
    app_ctx: AppContext,
    alias: alias::Connector,
    metrics_out: u64,
    metrics_err: u64,
    tx: MetricsSender,
    flush_interval_ns: Option<u64>,
    last_flush_ns: u64,
}

impl SourceReporter {
    pub(crate) fn new(
        app_ctx: AppContext,
        alias: alias::Connector,
        flush_interval_s: Option<u64>,
    ) -> Self {
        if let Some(flush_interval_s) = flush_interval_s {
            info!(
            "{app_ctx}[{alias}] Starting source reporter with flush interval of {flush_interval_s}s for metrics id {}",
             app_ctx.metrics.id());
        }
        let tx = app_ctx.metrics.tx();
        Self {
            tx,
            app_ctx,
            alias,
            metrics_out: 0,
            metrics_err: 0,
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
                let payload_out = make_event_count_metrics_payload(
                    timestamp,
                    OUT,
                    self.metrics_out,
                    &self.app_ctx,
                    &self.alias,
                );
                let payload_err = make_event_count_metrics_payload(
                    timestamp,
                    ERR,
                    self.metrics_err,
                    &self.app_ctx,
                    &self.alias,
                );
                send(&self.tx, payload_out, &self.alias);
                send(&self.tx, payload_err, &self.alias);
                self.last_flush_ns = timestamp;
                return Some(timestamp);
            }
        }
        None
    }

    /// simply send source metrics
    pub(crate) fn send_source_metrics(&self, metrics: Vec<EventPayload>) {
        for metric in metrics {
            send(&self.tx, metric, &self.alias);
        }
    }
}

/// metrics reporter for connector sinks
pub(crate) struct SinkReporter {
    app_ctx: AppContext,
    alias: alias::Connector,
    metrics_in: u64,
    tx: MetricsSender,
    flush_interval_ns: Option<u64>,
    last_flush_ns: u64,
}

impl SinkReporter {
    pub(crate) fn new(
        app_ctx: AppContext,
        alias: alias::Connector,
        flush_interval_s: Option<u64>,
    ) -> Self {
        if let Some(flush_interval_s) = flush_interval_s {
            info!(
            "{app_ctx}[{alias}] Starting sink reporter with flush interval of {flush_interval_s}s for metrics id {}",
             app_ctx.metrics.id());
        }
        let tx: MetricsSender = app_ctx.metrics.tx();
        Self {
            app_ctx,
            alias,
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
                let payload = make_event_count_metrics_payload(
                    timestamp,
                    IN,
                    self.metrics_in,
                    &self.app_ctx,
                    &self.alias,
                );
                send(&self.tx, payload, &self.alias);
                self.last_flush_ns = timestamp;
                return Some(timestamp);
            }
        }
        None
    }

    /// simply send source metrics
    pub(crate) fn send_sink_metrics(&self, metrics: Vec<EventPayload>) {
        for metric in metrics {
            send(&self.tx, metric, &self.alias);
        }
    }
}

pub(crate) fn send(tx: &MetricsSender, metric: EventPayload, alias: &alias::Connector) {
    use tremor_pipeline::MetricsMsg;

    if let Err(_e) = tx.send(MetricsMsg::new(metric, None)) {
        error!(
            "[Connector::{}] Error sending to system metrics connector.",
            &alias
        );
    }
}

#[must_use]
pub(crate) fn make_event_count_metrics_payload(
    timestamp: u64,
    port: Port<'static>,
    count: u64,
    app_ctx: &AppContext,
    connector_id: &alias::Connector,
) -> EventPayload {
    let mut tags = Object::with_capacity_and_hasher(5, ObjectHasher::default());
    tags.insert_nocheck(APP, Value::from(app_ctx.id().to_string()));
    tags.insert_nocheck(INSTANCE, Value::from(app_ctx.instance().to_string()));
    tags.insert_nocheck(FLOW, Value::from(connector_id.to_string()));
    tags.insert_nocheck(CONNECTOR, connector_id.to_string().into());
    tags.insert_nocheck(PORT, port.into());

    let value = value_count(CONNECTOR_EVENTS, tags, count, timestamp);
    // full metrics payload
    (value, Value::object()).into()
}

// TODO: add convenience functions for creating custom metrics payloads
#[must_use]
pub(crate) fn make_metrics_payload(
    name: &'static str,
    fields: Object<'static>,
    tags: Object<'static>,
    timestamp: u64,
) -> EventPayload {
    let value = value(Cow::const_str(name), tags, fields, timestamp);
    (value, Value::object()).into()
}
