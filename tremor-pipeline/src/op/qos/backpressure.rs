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

//! The backpressure operator is used to introduce delays based on downstream systems load. Longer backpressure steps are introduced every time the latency of a downstream system reached `timeout`, or an error occurs. On a successful transmission within the timeout limit, the delay is reset.
//!
//! The operator supports two modes:
//!
//! - `discard` - the standard, when backpressure is triggered it will discard new messages by sending them to the `overflow` output port. This is designed to fulfill the need of low transport latency at the cost of loss.
//! - `pause` - it will trigger a circuit breaker and ask the sources that send it data to stop sending additional events. No Event is discarded by the backpressure operator. This is designed to deal with situations where losing events is not an option - but the garuantee of losslessness depends on the source and how it can handle circuit breaker events.
//!
//!
//! This operator preserves event metadata.
//!
//! **Configuration options**:
//!
//! - `timeout` - Maximum allowed 'write' time in nanoseconds.
//! - `steps` - Array of values to delay when a we detect backpressure. (default: `[50, 100, 250, 500, 1000, 5000, 10000]`)
//! - `method` - Either `discard` or `pause` to define how backpressure is handled (default: `discard`)
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
//! define operator bp from qos::backpressure
//! with
//!   timeout = nanos::from_millis(100),
//!   method = "discard"
//! end;
//! ```

use crate::errors::{ErrorKind, Result};
use crate::op::prelude::*;
use beef::Cow;
use tremor_script::prelude::*;

const OVERFLOW: Cow<'static, str> = Cow::const_str("overflow");

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Method {
    /// messages are discarded
    Discard,
    /// a circuit breaker is triggerd
    Pause,
}
impl Default for Method {
    fn default() -> Self {
        Method::Discard
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The maximum allowed timeout before backoff is applied in nanoseconds
    pub timeout: u64,
    /// A list of backoff steps in ms, wich are progressed through as long
    /// as the maximum timeout is exceeded
    ///
    /// default: `[50, 100, 250, 500, 1000, 5000, 10000]`
    #[serde(default = "default_steps")]
    pub steps: Vec<u64>,

    /// Defines the behaviour of the backpressure operator.
    ///
    /// - `discard`: messages are discared
    /// - `pause`: it acts as a circuit breaker but
    ///    does let the messages pass
    #[serde(default)]
    pub method: Method,
}

impl ConfigImpl for Config {}

fn default_steps() -> Vec<u64> {
    vec![50, 100, 250, 500, 1000, 5000, 10000]
}

#[derive(Debug, Clone)]
struct Backpressure {
    config: Config,
    backoff: u64,
    next: u64,
    steps: Vec<u64>,
}

impl From<Config> for Backpressure {
    fn from(config: Config) -> Self {
        let steps = config.steps.iter().map(|v| *v * 1_000_000).collect();
        Self {
            config,
            backoff: 0,
            next: 0,
            steps,
        }
    }
}

impl Backpressure {
    /// We detected an error case, so we update our backoff if necessary
    fn update_backoff(&mut self, ingest_ns: u64) {
        let backoff = Self::next_backoff(&self.steps, self.backoff);
        self.backoff = backoff;
        self.next = ingest_ns + backoff;
    }

    /// We detected a healthy downstream, so we do not apply backpressure anymore, yay!
    fn reset(&mut self) {
        self.backoff = 0;
        self.next = 0;
    }

    fn has_backoff(&self) -> bool {
        self.backoff > 0
    }

    fn next_backoff(steps: &[u64], current: u64) -> u64 {
        let mut b = 0;
        for backoff in steps {
            b = *backoff;
            if b > current {
                break;
            }
        }
        b
    }
}

op!(BackpressureFactory(_uid, node) {
    if let Some(map) = &node.config {
        let config: Config = Config::new(map)?;
        Ok(Box::new(Backpressure::from(config)))
    } else {
        Err(ErrorKind::MissingOpConfig(node.id.clone()).into())
    }
});

impl Operator for Backpressure {
    fn on_event(
        &mut self,
        uid: OperatorId,
        _port: &str,
        _state: &mut Value<'static>,
        mut event: Event,
    ) -> Result<EventAndInsights> {
        event.op_meta.insert(uid, OwnedValue::null());
        // we need to mark the event as transactional in order to reliably receive CB events
        // and thus do effective backpressure
        // if we don't and the events arent transactional themselves, we won't be notified, which would render this operator useless
        event.transactional = true;
        let output = if self.next <= event.ingest_ns {
            self.next = event.ingest_ns + self.backoff;
            OUT
        } else if self.config.method == Method::Pause {
            OUT
        } else {
            OVERFLOW
        };
        Ok(vec![(output, event)].into())
    }

    fn handles_contraflow(&self) -> bool {
        true
    }

    fn handles_signal(&self) -> bool {
        true
    }

    fn on_signal(
        &mut self,
        _uid: OperatorId,
        _state: &mut Value<'static>,
        signal: &mut Event,
    ) -> Result<EventAndInsights> {
        let insights = if self.backoff > 0 && self.next <= signal.ingest_ns {
            self.reset();
            if self.config.method == Method::Pause {
                vec![Event::cb_restore(signal.ingest_ns)]
            } else {
                vec![]
            }
        } else {
            vec![]
        };
        Ok(EventAndInsights {
            insights,
            ..EventAndInsights::default()
        })
    }

    fn on_contraflow(&mut self, uid: OperatorId, insight: &mut Event) {
        // If the related event never touched this operator we don't take
        // action
        if !insight.op_meta.contains_key(uid) {
            return;
        }

        let did_apply_backoff = self.has_backoff();
        if super::is_error_insight(insight, self.config.timeout) {
            // upon error we step up the backoff
            self.update_backoff(insight.ingest_ns);
        } else if insight.cb == CbAction::Ack {
            // downstream seems healthy again, reset backpressure
            // effectively closing the CB
            self.reset();
        }

        let does_apply_backoff_now = self.has_backoff();
        insight.cb = match self.config.method {
            // we got our first error in pause-mode, so we got to trigger the circuit breaker
            Method::Pause if !did_apply_backoff && does_apply_backoff_now => CbAction::Trigger,
            // downstream seems healthy again, so we got to restore the circuit breaker
            Method::Pause if did_apply_backoff && !does_apply_backoff_now => CbAction::Restore,
            // stop the circuit breaker from propagating further upstream, we handle it from here, kiddos!
            _ if insight.cb == CbAction::Trigger => CbAction::None,
            _ => insight.cb,
        };
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::SignalKind;
    use tremor_common::ids::Id;
    use tremor_value::Object;

    #[test]
    fn pass_wo_error() {
        let operator_id = OperatorId::new(0);
        let mut op: Backpressure = Config {
            timeout: 100_000_000,
            steps: vec![1, 10, 100],
            method: Method::Discard,
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
            .on_event(operator_id, "in", &mut state, event1)
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
            .on_event(operator_id, "in", &mut state, event2)
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("no results");
        assert_eq!("out", out);
        assert!(event.transactional);
    }

    #[test]
    fn halt_on_error() {
        let operator_id = OperatorId::new(0);
        let mut op: Backpressure = Config {
            timeout: 100_000_000,
            steps: vec![1, 10, 100],
            method: Method::Discard,
        }
        .into();

        let mut state = Value::null();

        // Sent a first event, as all is initited clean
        // we syould see this pass
        let event1 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1_000_000,
            ..Event::default()
        };
        let mut r = op
            .on_event(operator_id, "in", &mut state, event1)
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("no results");
        assert_eq!("out", out);
        assert!(event.transactional);

        // Insert a timeout event with `time` set top `200`
        // this is over our limit of `100` so we syould move
        // one up the backup steps
        let mut m = Object::new();
        m.insert("time".into(), 200_000_000.into());

        let mut op_meta = OpMeta::default();
        op_meta.insert(operator_id, OwnedValue::null());
        let mut insight = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1_000_000,
            data: (Value::null(), m).into(),
            op_meta,
            ..Event::default()
        };

        // Verify that we now have a backoff of 1ms
        op.on_contraflow(operator_id, &mut insight);
        assert_eq!(op.backoff, 1_000_000);

        // The first event was sent at exactly 1ms
        // our we should block all eventsup to
        // 1_999_999
        // this event syould overflow
        let event2 = Event {
            id: (1, 1, 2).into(),
            ingest_ns: 2_000_000 - 1,
            ..Event::default()
        };
        let mut r = op
            .on_event(operator_id, "in", &mut state, event2)
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("overflow", out);

        // On exactly 2_000_000 we should be allowed to send
        // again
        let event3 = Event {
            id: (1, 1, 3).into(),
            ingest_ns: 2_000_000,
            ..Event::default()
        };
        let mut r = op
            .on_event(operator_id, "in", &mut state, event3)
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("no results");
        assert_eq!("out", out);
        assert!(event.transactional);

        // Since now the last successful event was at 2_000_000
        // the next event should overflow at 2_000_001
        let event3 = Event {
            id: (1, 1, 3).into(),
            ingest_ns: 2_000_000 + 1,
            ..Event::default()
        };
        let mut r = op
            .on_event(operator_id, "in", &mut state, event3)
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("overflow", out);
    }

    #[test]
    fn halt_on_error_cb() -> Result<()> {
        let operator_id = OperatorId::new(0);
        let mut op: Backpressure = Config {
            timeout: 100_000_000,
            steps: vec![1, 10, 100],
            method: Method::Pause,
        }
        .into();

        let mut state = Value::null();

        // Sent a first event, as all is initited clean
        // we syould see this pass
        let event1 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1_000_000,
            ..Event::default()
        };
        let mut r = op.on_event(operator_id, "in", &mut state, event1)?.events;
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("no results");
        assert_eq!("out", out);
        assert!(event.transactional);

        // Insert a timeout event with `time` set top `200`
        // this is over our limit of `100` so we syould move
        // one up the backup steps
        let mut m = Object::new();
        m.insert("time".into(), 200_000_000.into());

        let mut op_meta = OpMeta::default();
        op_meta.insert(operator_id, OwnedValue::null());
        let mut insight = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1_000_000,
            data: (Value::null(), m).into(),
            op_meta,
            ..Event::default()
        };

        // Verify that we now have a backoff of 1ms
        op.on_contraflow(operator_id, &mut insight);
        assert_eq!(insight.cb, CbAction::Trigger);
        assert_eq!(op.backoff, 1_000_000);

        // even if we have backoff we don't want to discard events so we
        // pass it.
        let event2 = Event {
            id: (1, 1, 2).into(),
            ingest_ns: 2_000_000 - 1,
            ..Event::default()
        };
        let mut r = op.on_event(operator_id, "in", &mut state, event2)?.events;
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        // since we are in CB mode we will STILL pass an event even if we're triggered
        // that way we can handle in flight events w/o loss
        assert_eq!("out", out);

        // We had always passed this
        let event3 = Event {
            id: (1, 1, 3).into(),
            ingest_ns: 2_000_000,
            ..Event::default()
        };
        let mut r = op.on_event(operator_id, "in", &mut state, event3)?.events;
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("no results");
        assert_eq!("out", out);
        assert!(event.transactional);

        // even if we have backoff we don't want to discard events so we
        // pass it.
        let event3 = Event {
            id: (1, 1, 3).into(),
            ingest_ns: 2_000_000 + 1,
            ..Event::default()
        };
        let mut r = op.on_event(operator_id, "in", &mut state, event3)?.events;
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("out", out);

        // Inserting a tick singnal beyond the backoff should re-enable the
        // CB
        let mut signal = Event {
            id: (1, 1, 3).into(),
            ingest_ns: 3_000_000 + 1,
            kind: Some(SignalKind::Tick),
            ..Event::default()
        };
        let mut r = op.on_signal(operator_id, &mut state, &mut signal)?;
        let i = r.insights.pop().expect("No Insight received");
        // We receive a restore signal
        assert_eq!(i.cb, CbAction::Restore);
        // And reset our backoff
        assert_eq!(op.backoff, 0);
        Ok(())
    }

    #[test]
    fn walk_backoff() {
        let operator_id = OperatorId::new(0);
        let mut op: Backpressure = Config {
            timeout: 100_000_000,
            steps: vec![1, 10, 100],
            method: Method::Discard,
        }
        .into();
        // An contraflow that fails the timeout
        let mut m = Object::new();
        m.insert("time".into(), 200_000_000.into());
        let mut op_meta = OpMeta::default();
        op_meta.insert(operator_id, OwnedValue::null());

        let mut insight = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 2,
            data: (Value::null(), m).into(),
            op_meta,
            ..Event::default()
        };

        // A contraflow that passes the timeout
        let m = literal!({
            "time": 99_000_000
        });
        let mut op_meta = OpMeta::default();
        op_meta.insert(operator_id, OwnedValue::null());

        let mut insight_reset = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 2,
            data: (Value::null(), m).into(),
            op_meta,
            cb: CbAction::Ack,
            ..Event::default()
        };

        // Assert initial state
        assert_eq!(op.backoff, 0);
        // move one step up
        op.on_contraflow(operator_id, &mut insight);
        assert_eq!(op.backoff, 1_000_000);
        // move another step up
        op.on_contraflow(operator_id, &mut insight);
        assert_eq!(op.backoff, 10_000_000);
        // move another another step up
        op.on_contraflow(operator_id, &mut insight);
        assert_eq!(op.backoff, 100_000_000);
        // We are at the highest step everything
        // should stay  the same
        op.on_contraflow(operator_id, &mut insight);
        assert_eq!(op.backoff, 100_000_000);
        // Now we should reset
        op.on_contraflow(operator_id, &mut insight_reset);
        assert_eq!(op.backoff, 0);
    }
}
