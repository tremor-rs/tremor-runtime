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

pub use super::{Offramp, OfframpAddr, OfframpImpl};
pub use crate::codec::{self, Codec};
pub use crate::dflt::*;
pub use crate::errors::*;
pub use crate::postprocessor::{self, Postprocessor, Postprocessors};
pub use crate::system::{PipelineAddr, PipelineMsg};
pub use crate::url::TremorURL;
use crate::utils::nanotime;
pub use crate::{Event, OpConfig};
pub use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
use std::mem;
pub use std::thread;

pub fn make_postprocessors(postprocessors: &[String]) -> Result<Postprocessors> {
    postprocessors
        .iter()
        .map(|n| postprocessor::lookup(&n))
        .collect()
}
// We are borrowing a dyn box as we don't want to pass ownership.
#[allow(clippy::borrowed_box)]
pub fn postprocess(
    postprocessors: &mut [Box<dyn Postprocessor>],
    data: Vec<u8>,
) -> Result<Vec<Vec<u8>>> {
    let egress_ns = nanotime();
    let mut data = vec![data];
    let mut data1 = Vec::new();

    for pp in postprocessors {
        data1.clear();
        for d in &data {
            match pp.process(egress_ns, d) {
                Ok(mut r) => data1.append(&mut r),
                Err(_e) => {
                    return Err("Postprocessor error {}".into());
                }
            }
        }
        mem::swap(&mut data, &mut data1);
    }

    Ok(data)
}
