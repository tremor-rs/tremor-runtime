// Copyright 2020-2021, The Tremor Team
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

//! An alternative traffic shaping option to backpressure. Instead of all dropping events for a given
//! time we drop a statistical subset with an increasing percentage of events dropped the longer we
//! see errors / timeouts.
//!
//! In general `step_up` should always be significantly smaller then `step_down` to ensure we gradually
//! reapproach the ideal state.
//!
//! This operator preserves event metadata.
//!
//! **Configuration options**:
//!
//! - `timeout` - Maximum allowed 'write' time in nanoseconds.
//! - `step_down` - What additional percentile should be dropped in the case of a timeout (default 5%: `0.05`)
//! - `step_up` - What percentile should be recovered in case of a good event. (default: 0.1%: `0.001`)
//!
//! **Outputs**:
//!
//! - `out`
//! - `overflow` - Events that are not let past due to active backpressure
//!
//! **Example**:
//!
//! ```tremor
//! use std::time::nanos;
//!
//! define operator perc from qos::percentile
//! with
//!   timeout = nanos::from_millis(100),
//!   step_down = 0.1 # 10%
//! end;
//! ```

use crate::errors::{ErrorKind, Result};
use crate::op::prelude::*;
use beef::Cow;
use tremor_script::prelude::*;

const OVERFLOW: Cow<'static, str> = Cow::const_str("overflow");

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The maximum allowed timeout before backoff is applied in nanoseconds
    pub timeout: u64,
    /// Percentage to drecrease on bad feedback as a float betwen `1.0`
    /// and `0.0`.
    ///
    /// The default is 5% (`0.05`).
    #[serde(default = "default_step_down")]
    pub step_down: f64,

    /// Percentage to increase on good feedback as a float betwen `1.0`
    /// and `0.0`.
    ///
    /// The default is 0.1% (`0.001`).
    #[serde(default = "default_step_up")]
    pub step_up: f64,
}

impl ConfigImpl for Config {}

fn default_step_up() -> f64 {
    0.001
}
fn default_step_down() -> f64 {
    0.05
}
#[derive(Debug, Clone)]
struct Percentile {
    config: Config,
    perc: f64,
}

impl From<Config> for Percentile {
    fn from(config: Config) -> Self {
        Self { config, perc: 1.0 }
    }
}

op!(PercentileFactory(_uid, node) {
    if let Some(map) = &node.config {
        let config: Config = Config::new(map)?;
        Ok(Box::new(Percentile::from(config)))
    } else {
        Err(ErrorKind::MissingOpConfig(node.id.clone()).into())
    }
});

impl Operator for Percentile {
    fn on_event(
        &mut self,
        uid: OperatorId,
        _port: &str,
        _state: &mut Value<'static>,
        mut event: Event,
    ) -> Result<EventAndInsights> {
        event.op_meta.insert(uid, OwnedValue::null());
        // we need to mark the event as transactional as this triggers downstream sinks/operators
        // to send contraflow CB events which we need to maintain our drop percentage
        // if we dont set this, and the event isnt transactional already, we would not receive any CB events,
        // and wouldnt be able to adapt the percentage
        event.transactional = true;
        // We don't generate a real random number we use the last 16 bit
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

    fn on_contraflow(&mut self, uid: OperatorId, insight: &mut Event) {
        // If the related event never touched this operator we don't take
        // action
        if !insight.op_meta.contains_key(uid) {
            return;
        }
        if super::is_error_insight(insight, self.config.timeout) {
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

#[cfg(test)]
mod test {
    #![allow(clippy::float_cmp)]
    use super::*;
    use tremor_common::ids::Id;
    use tremor_value::Object;

    #[test]
    fn pass_wo_error() {
        let uid = OperatorId::new(0);
        let mut op: Percentile = Config {
            timeout: 100_000_000,
            step_up: default_step_up(),
            step_down: default_step_down(),
        }
        .into();

        let mut state = Value::null();

        // Sent a first event, as all is initited clean
        // we syould see this pass
        let event1 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1,
            ..Event::default()
        };
        let mut r = op
            .on_event(uid, "in", &mut state, event1)
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("no results");
        assert_eq!("out", out);
        assert!(event.transactional);

        // Without a timeout event sent a second event,
        // it too should pass
        let event2 = Event {
            id: (1, 1, 2).into(),
            ingest_ns: 2,
            ..Event::default()
        };
        let mut r = op
            .on_event(uid, "in", &mut state, event2)
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("no results");
        assert_eq!("out", out);
        assert!(event.transactional);
    }

    #[test]
    fn drop_on_timeout() {
        let mut op: Percentile = Config {
            timeout: 100_000_000,
            step_down: default_step_down(),
            step_up: default_step_up(),
        }
        .into();
        let uid = OperatorId::new(42);

        let mut state = Value::null();

        // Sent a first event, as all is freshly initialized
        // we should see this pass
        let event1 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1_000_000,
            ..Event::default()
        };
        let mut r = op
            .on_event(uid, "in", &mut state, event1)
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, mut event) = r.pop().expect("no results");
        assert_eq!("out", out);
        assert!(event.transactional);

        // Insert a timeout event with `time` set top `200`
        // this is over our limit of `100` so we syould move
        // one up the backup steps
        let mut m = Object::with_capacity(1);
        m.insert("time".into(), 200_000_000.into());

        // this will use the right op_meta
        let mut insight = event.insight_ack_with_timing(100_000_001);

        // Verify that we increased our percentage
        op.on_contraflow(uid, &mut insight);
        assert_eq!(0.95, op.perc);

        // now we have a good and fast event
        // this event should not overflow
        let event2 = Event {
            id: (1, 1, 2).into(),
            ingest_ns: 2_000_000,
            ..Event::default()
        };
        let mut r = op
            .on_event(uid, "in", &mut state, event2)
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, mut event) = r.pop().expect("no results");
        assert_eq!("out", out);
        assert!(event.transactional);

        // less than timeout, we reset percentage a little
        let mut insight2 = event.insight_ack_with_timing(99);
        op.on_contraflow(uid, &mut insight2);
        assert_eq!(0.951, op.perc);
    }

    #[test]
    fn drop_on_error() {
        let mut op: Percentile = Config {
            timeout: 100_000_000,
            step_down: 0.25,
            step_up: 0.1,
        }
        .into();
        let uid = OperatorId::new(123);
        // An contraflow that fails the timeout
        let mut m = Object::new();
        m.insert("time".into(), 200_000_000.into());

        let mut op_meta = OpMeta::default();
        op_meta.insert(uid, OwnedValue::null());

        let mut insight = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 2,
            cb: CbAction::Fail,
            op_meta: op_meta.clone(),
            ..Event::default()
        };

        // A contraflow ack
        let mut insight_reset = Event {
            id: (1, 1, 2).into(),
            ingest_ns: 2,
            cb: CbAction::Ack,
            op_meta,
            ..Event::default()
        };

        // Assert initial state
        assert_eq!(1.0, op.perc);
        // move one step down
        op.on_contraflow(uid, &mut insight);
        assert_eq!(0.75, op.perc);
        // move one step down
        op.on_contraflow(uid, &mut insight);
        assert_eq!(0.5, op.perc);

        let event = Event {
            id: (1, 1, 2).into(),
            ingest_ns: 2_000_001, // we chose a ingest_ns which we know will be discarded with perc of 0.5
            ..Event::default()
        };
        let mut state = Value::null();
        let mut events = op
            .on_event(uid, "in", &mut state, event)
            .expect("could not run pipeline")
            .events;

        assert_eq!(events.len(), 1);

        // should overflow
        let (out, _event) = events.pop().expect("no results");
        assert_eq!("overflow", out);

        // Now we should reset
        op.on_contraflow(uid, &mut insight_reset);
        assert_eq!(0.6, op.perc);
    }
}
