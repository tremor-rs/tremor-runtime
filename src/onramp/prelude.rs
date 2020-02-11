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

pub use crate::codec::{self, Codec};
pub use crate::errors::*;
pub use crate::metrics::RampReporter;
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
#[allow(
    clippy::borrowed_box,
    clippy::too_many_lines,
    clippy::too_many_arguments
)]
pub fn send_event(
    pipelines: &[(TremorURL, PipelineAddr)],
    preprocessors: &mut Preprocessors,
    codec: &mut Box<dyn Codec>,
    metrics_reporter: &mut RampReporter,
    ingest_ns: &mut u64,
    origin_uri: &tremor_pipeline::EventOriginUri,
    id: u64,
    data: Vec<u8>,
) {
    if let Ok(data) = handle_pp(preprocessors, ingest_ns, data) {
        for d in data {
            match codec.decode(d, *ingest_ns) {
                Ok(Some(data)) => {
                    metrics_reporter.periodic_flush(*ingest_ns);
                    metrics_reporter.increment_out();

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
                Err(e) => {
                    metrics_reporter.increment_error();
                    error!("[Codec] {}", e);
                }
            }
        }
    } else {
        // record preprocessor failures too
        metrics_reporter.increment_error();
    };
}
