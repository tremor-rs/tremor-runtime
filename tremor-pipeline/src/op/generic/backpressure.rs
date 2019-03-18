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

use crate::errors::*;
use crate::utils::duration_to_millis;
use crate::{Event, Operator};
use serde_json::{self, Value};
use serde_yaml;
use std::time::Instant;

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

#[derive(Debug, Clone)]
pub struct Backpressure {
    config: Config,
    backoff: u64,
    last_pass: Instant,
}

fn d_steps() -> Vec<u64> {
    vec![50, 100, 250, 500, 1000, 5000, 10000]
}

impl Backpressure {
    pub fn next_backoff(&mut self) {
        for backoff in &self.config.steps {
            if *backoff > self.backoff {
                self.backoff = *backoff;
                break;
            }
        }
    }
}

op!(BackpressureFactory(node) {
    if let Some(map) = &node.config {
        Ok(Box::new(Backpressure {
            config: serde_yaml::from_value(map.clone())?,
            backoff: 0,
            last_pass: Instant::now(),
        }))
    } else {
        Err(ErrorKind::MissingOpConfig(node.id.clone()).into())

    }});

impl Operator for Backpressure {
    fn on_event(&mut self, _port: &str, event: Event) -> Result<Vec<(String, Event)>> {
        let d = duration_to_millis(self.last_pass.elapsed());
        if d <= self.backoff {
            Ok(vec![("overflow".to_string(), event)])
        } else {
            Ok(vec![("out".to_string(), event)])
        }
    }
    fn handles_contraflow(&self) -> bool {
        true
    }

    fn on_contraflow(&mut self, insight: &mut Event) {
        if let Some(Value::String(_s)) = insight.meta.get("error") {
            self.next_backoff();
        } else if let Some(Value::Number(v)) = insight.meta.get("time") {
            if v.as_f64().unwrap() > self.config.timeout {
                self.next_backoff();
            } else {
                self.backoff = 0;
            }
        }
    }
}
