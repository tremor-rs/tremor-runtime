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

pub use super::{Onramp, OnrampAddr, OnrampImpl, OnrampMsg};
pub use crate::codec::{self, Codec};
pub use crate::errors::*;
pub use crate::preprocessor::{self, Preprocessor, Preprocessors};
pub use crate::system::{PipelineAddr, PipelineMsg};
pub use crate::url::TremorURL;
pub use crate::utils::{nanotime, ConfigImpl};
pub use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
pub use simd_json::OwnedValue;
// TODO pub here too?
use std::mem;
pub use std::thread;
pub use tremor_pipeline::{Event, MetaMap};

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

// TODO rename as handle_pp. here right now for testing
pub fn handle_pp2(
    preprocessors: &mut Preprocessors,
    ingest_ns: &mut u64,
    source_id: &str,
    data: Vec<u8>,
) -> Result<Vec<Vec<u8>>> {
    let mut data = vec![data];
    let mut data1 = Vec::new();
    for pp in preprocessors {
        data1.clear();
        for (i, d) in data.iter().enumerate() {
            // TODO remove option matching here onece process2 is merged with process
            let result = match pp.process2(ingest_ns, source_id, d) {
                Some(r) => r,
                None => pp.process(ingest_ns, d),
            };
            match result {
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

// TODO rename as send_event. here right now for testing
// We are borrowing a dyn box as we don't want to pass ownership.
#[allow(clippy::borrowed_box)]
pub fn send_event2(
    pipelines: &[(TremorURL, PipelineAddr)],
    preprocessors: &mut Preprocessors,
    codec: &mut Box<dyn Codec>,
    ingest_ns: &mut u64,
    meta: &mut MetaMap,
    id: u64,
    data: Vec<u8>,
) {
    // TODO remove later
    //dbg!(&meta);

    // TODO remove if we don't track source id from preprocesors
    let source_id = if let Some(OwnedValue::String(source_id)) = meta.get("source_id") {
        source_id
    } else {
        // TODO handle this better
        "0"
    };

    if let Ok(data) = handle_pp2(preprocessors, ingest_ns, &source_id, data) {
        for d in data {
            match codec.decode(d, *ingest_ns) {
                Ok(Some(value)) => {
                    let event = tremor_pipeline::Event {
                        is_batch: false,
                        id,
                        value,
                        // TODO better way to do this?
                        meta: (*meta).clone(),
                        ingest_ns: *ingest_ns,
                        kind: None,
                    };
                    let len = pipelines.len();
                    for (input, addr) in &pipelines[..len - 1] {
                        if let Some(input) = input.instance_port() {
                            let _ = addr.addr.send(PipelineMsg::Event {
                                input,
                                event: event.clone(),
                            });
                        }
                    }
                    let (input, addr) = &pipelines[len - 1];
                    if let Some(input) = input.instance_port() {
                        let _ = addr.addr.send(PipelineMsg::Event { input, event });
                    }
                }
                Ok(None) => (),
                Err(e) => error!("{}", e),
            }
        }
    };
}

// We are borrowing a dyn box as we don't want to pass ownership.
#[allow(clippy::borrowed_box)]
pub fn send_event(
    pipelines: &[(TremorURL, PipelineAddr)],
    preprocessors: &mut Preprocessors,
    codec: &mut Box<dyn Codec>,
    ingest_ns: &mut u64,
    id: u64,
    data: Vec<u8>,
) {
    if let Ok(data) = handle_pp(preprocessors, ingest_ns, data) {
        for d in data {
            match codec.decode(d, *ingest_ns) {
                Ok(Some(value)) => {
                    let event = tremor_pipeline::Event {
                        is_batch: false,
                        id,
                        meta: tremor_pipeline::MetaMap::new(),
                        value,
                        ingest_ns: *ingest_ns,
                        kind: None,
                    };
                    let len = pipelines.len();
                    for (input, addr) in &pipelines[..len - 1] {
                        if let Some(input) = input.instance_port() {
                            let _ = addr.addr.send(PipelineMsg::Event {
                                input,
                                event: event.clone(),
                            });
                        }
                    }
                    let (input, addr) = &pipelines[len - 1];
                    if let Some(input) = input.instance_port() {
                        let _ = addr.addr.send(PipelineMsg::Event { input, event });
                    }
                }
                Ok(None) => (),
                Err(e) => error!("{}", e),
            }
        }
    };
}

// TODO remove. is part of tremor_pipeline crate now
/*
macro_rules! metamap {
    { $($key:expr => $value:expr),+ } => {
        {
            let mut m = tremor_pipeline::MetaMap::new();
            $(
                m.insert($key.into(), simd_json::OwnedValue::from($value));
            )+
            m
        }
     };
}
*/
