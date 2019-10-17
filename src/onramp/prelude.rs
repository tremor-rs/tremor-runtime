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
pub use crate::onramp::{self, Onramp};
pub use crate::preprocessor::{self, Preprocessor, Preprocessors};
pub use crate::system::{PipelineAddr, PipelineMsg};
pub use crate::url::TremorURL;
pub use crate::utils::nanotime;
pub use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
use std::mem;
pub use std::thread;
pub use tremor_pipeline::Event;

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
                        kind: None,
                    };
                    let len = pipelines.len();
                    for (input, addr) in &pipelines[..len - 1] {
                        if let Some(input) = input.instance_port() {
                            if let Err(e) = addr.addr.send(PipelineMsg::Event {
                                input,
                                event: event.clone(),
                            }) {
                                error!("[Onramp] failed to send to pipeline {}", e);
                            }
                        }
                    }
                    let (input, addr) = &pipelines[len - 1];
                    if let Some(input) = input.instance_port() {
                        if let Err(e) = addr.addr.send(PipelineMsg::Event { input, event }) {
                            error!("[Onramp] failed to send to pipeline {}", e);
                        }
                    }
                }
                Ok(None) => (),
                Err(e) => error!("[Codec] {}", e),
            }
        }
    };
}
