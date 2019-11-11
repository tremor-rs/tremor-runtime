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

pub use crate::codec::{self, Codec};
pub use crate::errors::*;
pub use crate::metrics::RampMetricsReporter;
pub use crate::onramp::{self, Onramp};
pub use crate::preprocessor::{self, Preprocessor, Preprocessors};
pub use crate::repository::ServantId;
pub use crate::system::{PipelineAddr, PipelineMsg, METRICS_PIPELINE};
pub use crate::url::TremorURL;
pub use crate::utils::{nanotime, ConfigImpl};
pub use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
pub use halfbrown::HashMap;
pub use simd_json::{json, OwnedValue};
pub use std::borrow::Cow;
// TODO pub here too?
use std::mem;
pub use std::thread;
pub use tremor_pipeline::{Event, EventOriginUri};
pub use tremor_script::prelude::*;
//pub use tremor_script::LineValue;

// TODO remove. old test version
/*
static mut LAST_METRICS_FLUSH: u64 = 0;

const METRICS_INTERVAL_S: u64 = 10;

const METRICS_INTERVAL: u64 = METRICS_INTERVAL_S * 1_000_000_000;
*/

pub fn make_preprocessors(preprocessors: &[String]) -> Result<Preprocessors> {
    preprocessors
        .iter()
        .map(|n| preprocessor::lookup(&n))
        .collect()
}

pub fn handle_pp(
    preprocessors: &mut Preprocessors,
    ingest_ns: &mut u64,
    data: Vec<u8>,
) -> Result<Vec<Vec<u8>>> {
    let mut data = vec![data];
    let mut data1 = Vec::new();
    for pp in preprocessors {
        data1.clear();
        for (i, d) in data.iter().enumerate() {
            match pp.process(ingest_ns, d) {
                Ok(mut r) => data1.append(&mut r),
                Err(e) => {
                    error!("Preprocessor[{}] error {}", i, e);
                    return Err(e);
                }
            }
        }
        mem::swap(&mut data, &mut data1);
    }
    Ok(data)
}

// We are borrowing a dyn box as we don't want to pass ownership.
#[allow(clippy::borrowed_box)]
pub fn send_event(
    pipelines: &[(TremorURL, PipelineAddr)],
    preprocessors: &mut Preprocessors,
    codec: &mut Box<dyn Codec>,
    ingest_ns: &mut u64,
    origin_uri: &tremor_pipeline::EventOriginUri,
    id: u64,
    data: Vec<u8>,
) {
    if let Ok(data) = handle_pp(preprocessors, ingest_ns, data) {
        for d in data {
            match codec.decode(d, *ingest_ns) {
                Ok(Some(data)) => {
                    let event = tremor_pipeline::Event {
                        is_batch: false,
                        id,
                        data,
                        ingest_ns: *ingest_ns,
                        // TODO make origin_uri non-optional here too?
                        origin_uri: Some(origin_uri.clone()),
                        kind: None,
                    };
                    let len = pipelines.len();
                    for (input, addr) in &pipelines[0..len - 1] {
                        if let Some(input) = input.instance_port() {
                            if let Err(e) = addr.addr.send(PipelineMsg::Event {
                                input: input.into(),
                                event: event.clone(),
                            }) {
                                error!("[Onramp] failed to send to pipeline: {}", e);
                            }
                        }
                    }
                    let (input, addr) = &pipelines[len - 1];
                    if let Some(input) = input.instance_port() {
                        if let Err(e) = addr.addr.send(PipelineMsg::Event {
                            input: input.into(),
                            event,
                        }) {
                            error!("[Onramp] failed to send to pipeline: {}", e);
                        }
                    }
                }
                Ok(None) => (),
                Err(e) => error!("[Codec] {}", e),
            }
        }
    };
}

// We are borrowing a dyn box as we don't want to pass ownership.
#[allow(clippy::borrowed_box)]
pub fn send_event2(
    pipelines: &[(TremorURL, PipelineAddr)],
    preprocessors: &mut Preprocessors,
    codec: &mut Box<dyn Codec>,
    metrics_reporter: &mut RampMetricsReporter,
    ingest_ns: &mut u64,
    origin_uri: &tremor_pipeline::EventOriginUri,
    id: u64,
    data: Vec<u8>,
) {
    if let Ok(data) = handle_pp(preprocessors, ingest_ns, data) {
        for d in data {
            match codec.decode(d, *ingest_ns) {
                Ok(Some(data)) => {
                    let event = tremor_pipeline::Event {
                        is_batch: false,
                        id,
                        data,
                        ingest_ns: *ingest_ns,
                        // TODO make origin_uri non-optional here too?
                        origin_uri: Some(origin_uri.clone()),
                        kind: None,
                    };

                    let len = pipelines.len();

                    /*
                    // TODO work around unsafe
                    if unsafe { event.ingest_ns - LAST_METRICS_FLUSH } > METRICS_INTERVAL {
                        // metrics tags
                        // TODO update with right values
                        let mut tags: HashMap<Cow<'static, str>, Value<'static>> = HashMap::new();
                        //tags.insert("onramp".into(), "tremor:///onramp/main/01".into());
                        tags.insert("onramp".into(), onramp_url.to_string().into());
                        //tags.insert("direction".into(), "output".into());
                        tags.insert("port".into(), "out".into()); // error port to mark errors?
                        let value: Value = json!({
                            "measurement": "onramp_events",
                            "tags": tags,
                            "fields": {
                                "count": id + 1
                            },
                            "timestamp": *ingest_ns
                        })
                        .into();

                        // full metrics payload
                        // TODO update with right values
                        let _metrics_event = tremor_pipeline::Event {
                            is_batch: false,
                            id: 0,
                            data: tremor_script::LineValue::new(vec![], |_| ValueAndMeta {
                                value,
                                meta: Value::from(Object::default()),
                            }),
                            ingest_ns: *ingest_ns,
                            origin_uri: None,
                            kind: None,
                        };

                        // first pipeline link is always with the system metrics pipeline
                        // TODO improve this
                        let (_metrics_input, _metrics_addr) = &pipelines[0];
                        if let Some(_input) = _metrics_input.instance_port() {
                            //if let Err(e) = metrics_addr.addr.send(PipelineMsg::Event {
                            //    input: input.into(),
                            //    event: metrics_event,
                            //}) {
                            //    error!("[Onramp] failed to send to metrics pipeline: {}", e);
                            //}
                        }

                        // TODO work around unsafe
                        unsafe {
                            LAST_METRICS_FLUSH = event.ingest_ns;
                        }
                    }
                    */

                    metrics_reporter.periodic_flush(*ingest_ns);

                    metrics_reporter.increment_out();

                    for (input, addr) in &pipelines[0..len - 1] {
                        if let Some(input) = input.instance_port() {
                            if let Err(e) = addr.addr.send(PipelineMsg::Event {
                                input: input.into(),
                                event: event.clone(),
                            }) {
                                error!("[Onramp] failed to send to pipeline: {}", e);
                            }
                        }
                    }

                    let (input, addr) = &pipelines[len - 1];
                    if let Some(input) = input.instance_port() {
                        if let Err(e) = addr.addr.send(PipelineMsg::Event {
                            input: input.into(),
                            event,
                        }) {
                            error!("[Onramp] failed to send to pipeline: {}", e);
                        }
                    }
                }
                Ok(None) => (),
                Err(e) => {
                    // TODO add metric on preprocessor failures too?
                    metrics_reporter.increment_error();
                    error!("[Codec] {}", e);
                }
            }
        }
    };
}
