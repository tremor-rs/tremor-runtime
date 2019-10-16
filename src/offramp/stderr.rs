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

use crate::offramp::prelude::*;
use halfbrown::HashMap;

pub struct StdErr {
    pipelines: HashMap<TremorURL, PipelineAddr>,
    postprocessors: Postprocessors,
}

impl offramp::Impl for StdErr {
    fn from_config(_config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        Ok(Box::new(StdErr {
            pipelines: HashMap::new(),
            postprocessors: vec![],
        }))
    }
}
impl Offramp for StdErr {
    fn on_event(&mut self, codec: &Box<dyn Codec>, _input: String, event: Event) {
        for value in event.value_iter() {
            if let Ok(raw) = codec.encode(value) {
                match String::from_utf8(raw.to_vec()) {
                    Ok(s) => eprintln!("{}", s),
                    Err(_) => eprintln!("{:?}", raw),
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
    fn start(&mut self, _codec: &Box<dyn Codec>, postprocessors: &[String]) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }
}
