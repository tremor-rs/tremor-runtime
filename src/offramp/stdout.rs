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

//! # Debug offramp reporting classification statistics.
//!
//! The debug offramp periodically reports on the number of events
//! per classification.
//!
//! ## Configuration
//!
//! This operator takes no configuration

use super::{Offramp, OfframpImpl};
use crate::codec::Codec;
use crate::errors::*;
use crate::system::PipelineAddr;
use crate::url::TremorURL;
use crate::{Event, EventValue, OpConfig};
use hashbrown::HashMap;

#[derive(Debug, Clone)]
pub struct StdOut {
    pipelines: HashMap<TremorURL, PipelineAddr>,
}

impl OfframpImpl for StdOut {
    fn from_config(_config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        Ok(Box::new(StdOut {
            pipelines: HashMap::new(),
        }))
    }
}
impl Offramp for StdOut {
    fn on_event(&mut self, codec: &Box<dyn Codec>, _input: String, event: Event) {
        for event in event.into_iter() {
            if let Ok(EventValue::Raw(raw)) = codec.encode(event.value) {
                match String::from_utf8(raw.to_vec()) {
                    Ok(s) => println!("{}", s),
                    Err(_) => println!("{:?}", raw),
                }
            }
        }
    }
    fn add_pipeline(&mut self, id: TremorURL, addr: PipelineAddr) {
        self.pipelines.insert(id, addr);
    }
    fn remove_pipeline(&mut self, id: TremorURL) -> bool {
        self.pipelines.remove(&id);
        self.pipelines.is_empty()
    }
    fn default_codec(&self) -> &str {
        "json"
    }
}
