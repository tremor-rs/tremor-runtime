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
use crate::{Event, OpConfig};
use halfbrown::HashMap;
use simd_json::OwnedValue;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
struct DebugBucket {
    cnt: u64,
}

#[derive(Debug, Clone)]
pub struct Debug {
    last: Instant,
    update_time: Duration,
    buckets: HashMap<String, DebugBucket>,
    cnt: u64,
    pipelines: HashMap<TremorURL, PipelineAddr>,
}

impl OfframpImpl for Debug {
    fn from_config(_config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        Ok(Box::new(Debug {
            last: Instant::now(),
            update_time: Duration::from_secs(1),
            buckets: HashMap::new(),
            cnt: 0,
            pipelines: HashMap::new(),
        }))
    }
}
impl Offramp for Debug {
    fn on_event(&mut self, _codec: &Box<dyn Codec>, _input: String, event: Event) {
        for event in event.into_iter() {
            if self.last.elapsed() > self.update_time {
                self.last = Instant::now();
                println!();
                println!("|{:20}| {:7}|", "classification", "total");
                println!("|{:20}| {:7}|", "TOTAL", self.cnt);
                self.cnt = 0;
                for (class, data) in &self.buckets {
                    println!("|{:20}| {:7}|", class, data.cnt,);
                }
                println!();
                self.buckets.clear();
            }
            let c = if let Some(OwnedValue::String(s)) = event.meta.get("class") {
                s.to_string()
            } else {
                "<unclassified>".into()
            };
            //TODO: return to entry
            if let Some(entry) = self.buckets.get_mut(&c) {
                entry.cnt += 1;
            } else {
                self.buckets.insert(c, DebugBucket { cnt: 1 });
            }
            self.cnt += 1;
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
