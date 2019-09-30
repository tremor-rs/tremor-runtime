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
use crate::{Event, Operator};
use serde_yaml;
use tremor_script::prelude::*;

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
    pub config: Config,
    pub backoff: u64,
    pub last_pass: u64,
}

fn d_steps() -> Vec<u64> {
    vec![50, 100, 250, 500, 1000, 5000, 10000]
}

impl Backpressure {
    pub fn next_backoff(&mut self) {
        for backoff in &self.config.steps {
            // convert backoff to ns
            let b = *backoff * 1_000_000;
            if b > self.backoff {
                self.backoff = b;
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
            last_pass: 0,
        }))
    } else {
        Err(ErrorKind::MissingOpConfig(node.id.clone()).into())

    }});

impl Operator for Backpressure {
    fn on_event(&mut self, _port: &str, event: Event) -> Result<Vec<(String, Event)>> {
        let d = event.ingest_ns - self.last_pass;
        if d < self.backoff {
            Ok(vec![("overflow".to_string(), event)])
        } else {
            self.last_pass = event.ingest_ns;
            Ok(vec![("out".to_string(), event)])
        }
    }
    fn handles_contraflow(&self) -> bool {
        true
    }

    fn on_contraflow(&mut self, insight: &mut Event) {
        let meta = &insight.data.suffix().meta;
        if meta.get("error").is_some() {
            self.next_backoff();
        } else if let Some(v) = meta.get("time").and_then(Value::cast_f64) {
            if v > self.config.timeout {
                self.next_backoff();
            } else {
                self.backoff = 0;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn pass_wo_error() {
        let mut op = Backpressure {
            config: Config {
                timeout: 100.0,
                steps: vec![1, 10, 100],
            },
            backoff: 0,
            last_pass: 0,
        };

        // Sent a first event, as all is initited clean
        // we syould see this pass
        let event1 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            data: Value::from("snot").into(),
            kind: None,
        };
        let mut r = op
            .on_event("in", event1.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("out", out);

        // Without a timeout event sent a second event,
        // it too should pass
        let event2 = Event {
            is_batch: false,
            id: 2,
            ingest_ns: 2,
            data: Value::from("badge").into(),
            kind: None,
        };
        let mut r = op
            .on_event("in", event2.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("out", out);
    }

    #[test]
    fn block_on_error() {
        let mut op = Backpressure {
            config: Config {
                timeout: 100.0,
                steps: vec![1, 10, 100],
            },
            backoff: 0,
            last_pass: 0,
        };

        // Sent a first event, as all is initited clean
        // we syould see this pass
        let event1 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1_000_000,
            data: Value::from("snot").into(),
            kind: None,
        };
        let mut r = op
            .on_event("in", event1.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("out", out);

        // Insert a timeout event with `time` set top `200`
        // this is over our limit of `100` so we syould move
        // one up the backup steps
        let mut m = Map::new();
        m.insert("time".into(), 200.0.into());
        let mut insight = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 2,
            data: (Value::Null, m).into(),
            kind: None,
        };

        // Verify that we now have a backoff of 1ms
        op.on_contraflow(&mut insight);
        assert_eq!(op.backoff, 1_000_000);

        // The first event was sent at exactly 1ms
        // our we should block all eventsup to
        // 1_999_999
        // this event syould overflow
        let event2 = Event {
            is_batch: false,
            id: 2,
            ingest_ns: 2_000_000 - 1,
            data: Value::from("badger").into(),
            kind: None,
        };
        let mut r = op
            .on_event("in", event2.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("overflow", out);

        // On exactly 2_000_000 we should be allowed to send
        // again
        let event3 = Event {
            is_batch: false,
            id: 3,
            ingest_ns: 2_000_000,
            data: Value::from("boo").into(),
            kind: None,
        };
        let mut r = op
            .on_event("in", event3.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("out", out);

        // Since now the last successful event was at 2_000_000
        // the next event should overflow at 2_000_001
        let event3 = Event {
            is_batch: false,
            id: 3,
            ingest_ns: 2_000_000 + 1,
            data: Value::from("badger").into(),
            kind: None,
        };
        let mut r = op
            .on_event("in", event3.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("overflow", out);
    }

    #[test]
    fn walk_backoff() {
        let mut op = Backpressure {
            config: Config {
                timeout: 100.0,
                steps: vec![1, 10, 100],
            },
            backoff: 0,
            last_pass: 0,
        };
        // An contraflow that fails the timeout
        let mut m = Map::new();
        m.insert("time".into(), 200.0.into());

        let mut insight = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 2,
            data: (Value::Null, m).into(),
            kind: None,
        };

        // A contraflow that passes the timeout
        let mut m = Map::new();
        m.insert("time".into(), 99.0.into());

        let mut insight_reset = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 2,
            data: (Value::Null, m).into(),
            kind: None,
        };

        // Assert initial state
        assert_eq!(op.backoff, 0);
        // move one step up
        op.on_contraflow(&mut insight);
        assert_eq!(op.backoff, 1_000_000);
        // move another step up
        op.on_contraflow(&mut insight);
        assert_eq!(op.backoff, 10_000_000);
        // move another another step up
        op.on_contraflow(&mut insight);
        assert_eq!(op.backoff, 100_000_000);
        // We are at the highest step everything
        // should stay  the same
        op.on_contraflow(&mut insight);
        assert_eq!(op.backoff, 100_000_000);
        // Now we should reset
        op.on_contraflow(&mut insight_reset);
        assert_eq!(op.backoff, 0);
    }

}
