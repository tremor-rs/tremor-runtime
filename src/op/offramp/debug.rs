// Copyright 2018, Wayfair GmbH
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

use crate::errors::*;
use crate::pipeline::prelude::*;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
struct DebugBucket {
    cnt: u64,
}

#[derive(Debug, Clone)]
pub struct Offramp {
    last: Instant,
    update_time: Duration,
    buckets: HashMap<String, DebugBucket>,
    cnt: u64,
}

impl Offramp {
    pub fn create(_opts: &ConfValue) -> Result<Self> {
        Ok(Offramp {
            last: Instant::now(),
            update_time: Duration::from_secs(1),
            buckets: HashMap::new(),
            cnt: 0,
        })
    }
}
impl Opable for Offramp {
    opable_types!(ValueType::Raw, ValueType::Raw);
    // TODO
    fn exec(&mut self, event: EventData) -> EventResult {
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
        let c = if let Some(MetaValue::String(s)) = event.var(&"classification") {
            s.clone()
        } else {
            "<unclassified>".into()
        };
        let entry = self.buckets.entry(c).or_insert(DebugBucket { cnt: 0 });
        entry.cnt += 1;
        self.cnt += 1;
        EventResult::Next(event)
    }
}
