// Copyright 2020, The Tremor Team
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

use crate::errors::{ErrorKind, Result};
use crate::op::prelude::*;
use tremor_script::prelude::*;

const OVERFLOW: Cow<'static, str> = Cow::Borrowed("overflow");

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// The maximum allowed timeout before backoff is applied
    pub timeout: f64,
    /// Percentage to drecrease on bad feedback as a float betwen `1.0`
    /// and `0.0`.
    ///
    /// The default is 5% (`0.05`).
    #[serde(default = "d_step_down")]
    pub step_down: f64,

    /// Percentage to increase on good feedback as a float betwen `1.0`
    /// and `0.0`.
    ///
    /// The default is 0.1% (`0.001`).
    #[serde(default = "d_step_up")]
    pub step_up: f64,
}

impl ConfigImpl for Config {}

#[derive(Debug, Clone)]
pub struct Percentile {
    pub config: Config,
    pub next: usize,
    pub perc: f64,
}

impl From<Config> for Percentile {
    fn from(config: Config) -> Self {
        Self {
            config,
            perc: 1.0,
            next: 0,
        }
    }
}

fn d_step_up() -> f64 {
    0.001
}
fn d_step_down() -> f64 {
    0.05
}

op!(PercentileFactory(node) {
    if let Some(map) = &node.config {
        let config: Config = Config::new(map)?;
        Ok(Box::new(Percentile::from(config)))
    } else {
        Err(ErrorKind::MissingOpConfig(node.id.to_string()).into())
    }
});

impl Operator for Percentile {
    fn on_event(
        &mut self,
        uid: u64,
        _port: &str,
        _state: &mut Value<'static>,
        mut event: Event,
    ) -> Result<EventAndInsights> {
        event.op_meta.insert(uid, OwnedValue::null());
        // We don't generate a real random number we use the last the 16 bit
        // of the nanosecond timestamp as a randum number.
        // This is both fairly random and completely deterministic.
        #[allow(clippy::cast_precision_loss)]
        let r = (event.ingest_ns % 0xFFFF) as f64 / f64::from(0xFFFF);
        if r > self.perc {
            Ok(vec![(OVERFLOW, event)].into())
        } else {
            Ok(event.into())
        }
    }

    fn handles_contraflow(&self) -> bool {
        true
    }

    fn on_contraflow(&mut self, uid: u64, insight: &mut Event) {
        // If the related event never touched this operator we don't take
        // action
        if !insight.op_meta.contains_key(uid) {
            return;
        }
        let (_, meta) = insight.data.parts();

        if meta.get("error").is_some()
            || insight.cb == CBAction::Fail
            || insight.cb == CBAction::Close
            || meta
                .get("time")
                .and_then(Value::cast_f64)
                .map_or(false, |v| v > self.config.timeout)
        {
            self.perc -= self.config.step_down;
            if self.perc < 0.0 {
                self.perc = 0.0;
            }
        } else {
            self.perc += self.config.step_up;
            if self.perc > 1.0 {
                self.perc = 1.0;
            }
        }
    }
}

#[cfg(tests)]
mod test {
    use super::*;
    use simd_json::value::borrowed::Object;
    use std::collections::BTreeMap;

    #[test]
    fn pass_wo_error() {
        let mut op: Backpressure = Config {
            timeout: 100.0,
            steps: vec![1, 10, 100],
            circuit_breaker: false,
        }
        .into();

        let mut state = Value::null();

        // Sent a first event, as all is initited clean
        // we syould see this pass
        let event1 = Event {
            id: 1.into(),
            ingest_ns: 1,
            ..Event::default()
        };
        let mut r = op
            .on_event(0, "in", &mut state, event1.clone())
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("out", out);

        // Without a timeout event sent a second event,
        // it too should pass
        let event2 = Event {
            id: 2.into(),
            ingest_ns: 2,
            ..Event::default()
        };
        let mut r = op
            .on_event(0, "in", &mut state, event2.clone())
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("out", out);
    }

    #[test]
    fn block_on_error() {
        let mut op: Backpressure = Config {
            timeout: 100.0,
            steps: vec![1, 10, 100],
            circuit_breaker: false,
        }
        .into();

        let mut state = Value::null();

        // Sent a first event, as all is initited clean
        // we syould see this pass
        let event1 = Event {
            id: 1.into(),
            ingest_ns: 1_000_000,
            ..Event::default()
        };
        let mut r = op
            .on_event(0, "in", &mut state, event1.clone())
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("out", out);

        // Insert a timeout event with `time` set top `200`
        // this is over our limit of `100` so we syould move
        // one up the backup steps
        let mut m = Object::new();
        m.insert("time".into(), 200.0.into());

        let mut op_meta = BTreeMap::new();
        op_meta.insert(0, OwnedValue::null());
        let mut insight = Event {
            id: 1.into(),
            ingest_ns: 1_000_000,
            data: (Value::null(), m).into(),
            op_meta,
            ..Event::default()
        };

        // Verify that we now have a backoff of 1ms
        op.on_contraflow(0, &mut insight);
        assert_eq!(op.output.backoff, 1_000_000);

        // The first event was sent at exactly 1ms
        // our we should block all eventsup to
        // 1_999_999
        // this event syould overflow
        let event2 = Event {
            id: 2.into(),
            ingest_ns: 2_000_000 - 1,
            ..Event::default()
        };
        let mut r = op
            .on_event(0, "in", &mut state, event2.clone())
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("overflow", out);

        // On exactly 2_000_000 we should be allowed to send
        // again
        let event3 = Event {
            id: 3.into(),
            ingest_ns: 2_000_000,
            ..Event::default()
        };
        let mut r = op
            .on_event(0, "in", &mut state, event3.clone())
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("out", out);

        // Since now the last successful event was at 2_000_000
        // the next event should overflow at 2_000_001
        let event3 = Event {
            id: 3.into(),
            ingest_ns: 2_000_000 + 1,
            ..Event::default()
        };
        let mut r = op
            .on_event(0, "in", &mut state, event3.clone())
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("overflow", out);
    }

    #[test]
    fn walk_backoff() {
        let mut op: Backpressure = Config {
            timeout: 100.0,
            steps: vec![1, 10, 100],
            circuit_breaker: false,
        }
        .into();
        // An contraflow that fails the timeout
        let mut m = Object::new();
        m.insert("time".into(), 200.0.into());

        let mut insight = Event {
            id: 1.into(),
            ingest_ns: 2,
            data: (Value::null(), m).into(),
            ..Event::default()
        };

        // A contraflow that passes the timeout
        let mut m = Object::new();
        m.insert("time".into(), 99.0.into());

        let mut insight_reset = Event {
            id: 1.into(),
            ingest_ns: 2,
            data: (Value::null(), m).into(),
            ..Event::default()
        };

        // Assert initial state
        assert_eq!(op.output.backoff, 0);
        // move one step up
        op.on_contraflow(0, &mut insight);
        assert_eq!(op.output.backoff, 1_000_000);
        // move another step up
        op.on_contraflow(0, &mut insight);
        assert_eq!(op.output.backoff, 10_000_000);
        // move another another step up
        op.on_contraflow(0, &mut insight);
        assert_eq!(op.output.backoff, 100_000_000);
        // We are at the highest step everything
        // should stay  the same
        op.on_contraflow(0, &mut insight);
        assert_eq!(op.output.backoff, 100_000_000);
        // Now we should reset
        op.on_contraflow(0, &mut insight_reset);
        assert_eq!(op.output.backoff, 0);
    }
}
