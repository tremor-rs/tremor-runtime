// Copyright 2018-2020, Wayfair GmbH
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

use crate::pipeline;
use crate::url::TremorURL;
use halfbrown::HashMap;
use simd_json::json;
use std::borrow::Cow;
use tremor_pipeline::Event;
use tremor_script::prelude::*;

/// Metrics instance name
pub static mut INSTANCE: &str = "tremor";

#[derive(Debug)]
pub(crate) struct Ramp {
    r#in: u64,
    out: u64,
    error: u64,
}

#[derive(Debug)]
pub(crate) struct RampReporter {
    artefact_url: TremorURL,
    metrics: Ramp,
    metrics_pipeline: Option<(TremorURL, pipeline::Addr)>,
    flush_interval: Option<u64>, // as nano-seconds
    last_flush_ns: u64,
}

impl RampReporter {
    pub fn new(artefact_url: TremorURL, flush_interval_s: Option<u64>) -> Self {
        Self {
            artefact_url,
            metrics: Ramp {
                r#in: 0,
                out: 0,
                error: 0,
            },
            metrics_pipeline: None,
            flush_interval: flush_interval_s.map(|n| n * 1_000_000_000),
            last_flush_ns: 0,
        }
    }

    pub fn set_metrics_pipeline(&mut self, pipeline_tuple: (TremorURL, pipeline::Addr)) {
        self.metrics_pipeline = Some(pipeline_tuple);
    }

    // TODO inline useful on these?
    #[inline]
    pub fn increment_in(&mut self) {
        self.metrics.r#in += 1;
    }

    #[inline]
    pub fn increment_out(&mut self) {
        self.metrics.out += 1;
    }

    #[inline]
    pub fn increment_error(&mut self) {
        self.metrics.error += 1;
    }

    #[inline]
    pub fn periodic_flush(&mut self, timestamp: u64) -> Option<u64> {
        if let Some(interval) = self.flush_interval {
            if timestamp - self.last_flush_ns > interval {
                self.flush(timestamp);
                return Some(timestamp);
            }
        }
        None
    }

    fn flush(&mut self, timestamp: u64) {
        self.send_metric(timestamp, "in", self.metrics.r#in);
        self.send_metric(timestamp, "out", self.metrics.out);
        self.send_metric(timestamp, "error", self.metrics.error);
        self.last_flush_ns = timestamp;
    }

    pub(crate) fn send(&self, event: Event) {
        if let Some((metrics_input, metrics_addr)) = &self.metrics_pipeline {
            if let Some(input) = metrics_input.instance_port() {
                if !metrics_addr.try_send(pipeline::Msg::Event {
                    input: input.to_string().into(),
                    event,
                }) {
                    error!("Failed to send to system metrics pipeline");
                }
            }
        }
    }
    fn send_metric(&self, timestamp: u64, port: &'static str, count: u64) {
        // metrics tags
        let mut tags: HashMap<Cow<'static, str>, Value<'static>> = HashMap::new();
        tags.insert("ramp".into(), self.artefact_url.to_string().into());

        tags.insert("port".into(), port.into());
        let value: Value = json!({
            "measurement": "ramp_events",
            "tags": tags,
            "fields": {
                "count": count,
            },
            "timestamp": timestamp,
        })
        .into();

        // full metrics payload
        let metrics_event = Event {
            data: tremor_script::LineValue::new(vec![], |_| ValueAndMeta::from(value)),
            ingest_ns: timestamp,
            origin_uri: None, // TODO update
            ..Event::default()
        };
        self.send(metrics_event)
    }
}
