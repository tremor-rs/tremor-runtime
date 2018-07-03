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

//! # Growing Backoff limiter
//!
//! The Backoff limiter will start backing off based on the maximum allowed time for results
//!
//! ## Config
//!   * timeout - maximum allowed time for a batch write, overstepping this will cause back pressure (required)
//!   * steps - list of backoff steps in milliseconds (default: [50, 100, 250, 500, 1000, 5000, 10000])
//!
//! ## Variables
//!   * index - index to write to
//!   * doc-type - document type for the event
//!   * pipeline - pipeline to use
//!
//!
//! ## Details
//!
//! The algoritm is as follows:
//!
//! Variables:
//!   * backoff - additional delay after timed out send.
//!
//! Pseudo variables:
//!   * batch - collection of messages.
//!   * queue - a queue of fugure sends.
//!
//! Pseudocode:
//! ```pseudo,no_run
//! for m in messages {
//!   if now() - last_done > backoff {
//!     batch.add(m)
//!     if batch.size >= batch_size {
//!       if queue.size < concurrency {
//!         queue.push(batch.send())
//!       } else {
//!         future = queue.first
//!         if future.is_done {
//!           queue.pop()
//!           if future.execution_time < timeout {
//!             backoff = 0;
//!           } else {
//!             backoff = grow_backoff(backoff) // backoff increase logic
//!           }
//!           last_done = now();
//!           queue.push(batch.send())
//!         } else {
//!           batch.drop();
//!         }
//!       }
//!     }
//!   }
//! }
//! ```

use errors::*;
use pipeline::prelude::*;
use prometheus::IntGauge; // w/ instance
use serde_yaml;
use std::time::Instant;
use utils::duration_to_millis;

lazy_static! {
    static ref BACKOFF_GAUGE: IntGauge =
        prom_int_gauge!("backoff_ms", "Current backoff in millis.");
}

pub struct Limiter {
    config: Config,
    backoff: u64,
    last_pass: Instant,
}

#[derive(Debug, Clone, Deserialize)]
struct Config {
    timeout: f64,
    #[serde(default = "d_steps")]
    steps: Vec<u64>,
}

fn d_steps() -> Vec<u64> {
    vec![50, 100, 250, 500, 1000, 5000, 10000]
}

impl Limiter {
    pub fn new(opts: serde_yaml::Value) -> Result<Self> {
        Ok(Limiter {
            config: serde_yaml::from_value(opts)?,
            backoff: 0,
            last_pass: Instant::now(),
        })
    }
    pub fn next_backoff(&mut self) {
        for backoff in &self.config.steps {
            if *backoff > self.backoff {
                self.backoff = *backoff;
                BACKOFF_GAUGE.set(self.backoff as i64);
                break;
            }
        }
    }
}

impl Opable for Limiter {
    fn exec(&mut self, event: EventData) -> EventResult {
        let d = duration_to_millis(self.last_pass.elapsed());
        if d <= self.backoff {
            EventResult::NextID(3, event)
        } else {
            self.last_pass = Instant::now();
            EventResult::Next(event)
        }
    }

    fn result(&mut self, result: EventReturn) -> EventReturn {
        match result {
            Ok(Some(v)) if v > self.config.timeout => self.next_backoff(),
            Err(_) => self.next_backoff(),
            _ => {
                self.backoff = 0;
                BACKOFF_GAUGE.set(0)
            }
        }
        result
    }

    opable_types!(ValueType::Same, ValueType::Same);
}

// We don't do this in a test module since we need to access private functions.
#[test]
fn backoff_test() {
    let mut l = Limiter {
        config: Config {
            timeout: 10.0,
            steps: vec![10, 20, 30, 40],
        },
        backoff: 0,
        last_pass: Instant::now(),
    };
    l.next_backoff();
    assert_eq!(l.backoff, 10);
    l.next_backoff();
    assert_eq!(l.backoff, 20);
    l.next_backoff();
    assert_eq!(l.backoff, 30);
    l.next_backoff();
    assert_eq!(l.backoff, 40);

    l.backoff = 5;
    l.next_backoff();
    assert_eq!(l.backoff, 10);
}
