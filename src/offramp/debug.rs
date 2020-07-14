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
use std::time::{Duration, Instant};
use tremor_script::prelude::*;

#[derive(Debug, Clone)]
struct DebugBucket {
    cnt: u64,
}

pub struct Debug {
    last: Instant,
    update_time: Duration,
    buckets: HashMap<String, DebugBucket>,
    cnt: u64,
    pipelines: HashMap<TremorURL, pipeline::Addr>,
    postprocessors: Postprocessors,
}

impl offramp::Impl for Debug {
    fn from_config(_config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        Ok(Box::new(Self {
            last: Instant::now(),
            update_time: Duration::from_secs(1),
            buckets: HashMap::new(),
            cnt: 0,
            pipelines: HashMap::new(),
            postprocessors: vec![],
        }))
    }
}
#[async_trait::async_trait]
impl Offramp for Debug {
    async fn on_event(&mut self, _codec: &dyn Codec, _input: &str, event: Event) -> Result<()> {
        for (_value, meta) in event.value_meta_iter() {
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
            let c = if let Some(s) = meta.get("class").and_then(Value::as_str) {
                s
            } else {
                "<unclassified>"
            };
            //TODO: return to entry
            if let Some(entry) = self.buckets.get_mut(c) {
                entry.cnt += 1;
            } else {
                self.buckets.insert(c.to_owned(), DebugBucket { cnt: 1 });
            }
            self.cnt += 1;
        }
        Ok(())
    }
    fn add_pipeline(&mut self, id: TremorURL, addr: pipeline::Addr) {
        self.pipelines.insert(id, addr);
    }
    fn remove_pipeline(&mut self, id: TremorURL) -> bool {
        self.pipelines.remove(&id);
        self.pipelines.is_empty()
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    async fn start(&mut self, _codec: &dyn Codec, postprocessors: &[String]) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }
}
