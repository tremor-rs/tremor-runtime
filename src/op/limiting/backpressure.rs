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

//! # Incremental backoff limiter
//!
//! The Backoff limiter will start backing off based on the maximum allowed time for results
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
//!
//! ## Outputs
//!
//! The 1st additional output is used to route data that was decided to
//! be discarded.

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
pub struct Config {
    /// The maximum allowed timeout before backoff is applied
    pub timeout: f64,
    /// A list of backoff steps in ms, wich are progressed through as long
    /// as the maximum timeout is exceeded
    ///
    /// default: `[50, 100, 250, 500, 1000, 5000, 10000]`
    #[serde(default = "d_steps")]
    pub steps: Vec<u64>,
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
