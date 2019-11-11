// Copyright 2018-2019, Wayfair GmbH
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

use crate::system::{PipelineAddr, PipelineMsg};
use crate::url::TremorURL;
use halfbrown::HashMap;
use simd_json::json;
use std::borrow::Cow;
use tremor_script::prelude::*;

pub static mut INSTANCE: &str = "tremor";

#[derive(Debug)]
pub struct RampMetricsReporter {
    artefact_url: TremorURL,
    metrics: HashMap<Cow<'static, str>, u64>,
    metrics_pipeline: Option<(TremorURL, PipelineAddr)>,
    flush_interval: Option<u64>, // as nano-seconds
    last_flush_ns: u64,
}

impl RampMetricsReporter {
    pub fn new(artefact_url: TremorURL, flush_interval_s: Option<u64>) -> Self {
        Self {
            artefact_url,
            metrics: [("in".into(), 0), ("out".into(), 0), ("error".into(), 0)]
                .iter()
                .cloned()
                .collect(),
            metrics_pipeline: None,
            flush_interval: flush_interval_s.map(|n| n * 1_000_000_000),
            last_flush_ns: 0,
        }
    }

    pub fn set_metrics_pipeline(&mut self, id: TremorURL, addr: PipelineAddr) {
        self.metrics_pipeline = Some((id, addr));
    }

    // TODO inline useful on these?
    // TODO remove all the debugs here
    #[inline]
    pub fn increment_in(&mut self) {
        //dbg!("in+");
        self.bump_metric("in");
    }

    #[inline]
    pub fn increment_out(&mut self) {
        //dbg!("out+");
        self.bump_metric("out");
    }

    #[inline]
    pub fn increment_error(&mut self) {
        //dbg!("error+");
        self.bump_metric("error");
    }

    #[inline]
    fn bump_metric(&mut self, port: &str) {
        if let Some(count) = self.metrics.get_mut(port.into()) {
            *count += 1;
        }
    }

    #[inline]
    pub fn periodic_flush(&mut self, timestamp: u64) {
        if let Some(interval) = self.flush_interval {
            //dbg!("periodic flush check");
            if timestamp - self.last_flush_ns > interval {
                self.flush(timestamp);
            }
        }
    }

    fn flush(&mut self, timestamp: u64) {
        //dbg!("flush");
        for (port, count) in &self.metrics {
            // TODO avoid clone here
            self.send_metric(timestamp, (*port).clone(), *count);
        }
        self.last_flush_ns = timestamp;
    }

    fn send_metric(&self, timestamp: u64, port: Cow<'static, str>, count: u64) {
        if let Some((metrics_input, metrics_addr)) = &self.metrics_pipeline {
            if let Some(input) = metrics_input.instance_port() {
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
                let metrics_event = tremor_pipeline::Event {
                    is_batch: false,
                    id: 0,
                    data: tremor_script::LineValue::new(vec![], |_| ValueAndMeta {
                        value,
                        meta: Value::from(Object::default()),
                    }),
                    ingest_ns: timestamp,
                    origin_uri: None, // TODO update
                    kind: None,
                };

                if let Err(e) = metrics_addr.addr.send(PipelineMsg::Event {
                    input: input.into(),
                    event: metrics_event,
                }) {
                    error!("[Onramp] failed to send to metrics pipeline: {}", e);
                }
            }
        }
    }
}
