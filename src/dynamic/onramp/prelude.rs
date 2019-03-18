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
pub use crate::dynamic::codec::{self, Codec};
pub use crate::errors::*;
pub use crate::system::{PipelineAddr, PipelineMsg};
pub use crate::url::TremorURL;
use crate::utils::nanotime;
pub use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
pub use std::thread;
pub use tremor_pipeline::{Event, EventValue};

// We are borrowing a dyn box as we don't want to pass ownership.
#[allow(clippy::borrowed_box)]
pub fn send_event(
    pipelines: &[(TremorURL, PipelineAddr)],
    codec: &Box<dyn Codec>,
    id: u64,
    data: EventValue,
) {
    if let Ok(value) = codec.decode(data) {
        let event = tremor_pipeline::Event {
            is_batch: false,
            id,
            meta: tremor_pipeline::MetaMap::new(),
            value,
            ingest_ns: nanotime(),
            kind: None,
        };
        // TODO: Only send the last
        for (input, addr) in pipelines {
            let _ = addr.addr.send(PipelineMsg::Event {
                input: input.instance_port().unwrap().to_string(), // We know at this port that we have a port!
                event: event.clone(),
            });
        }
    }
}
