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
use crate::{ConfigImpl, Event, Operator};
use std::borrow::Cow;
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

    #[serde(default = "d_outputs")]
    pub outputs: Vec<String>,
}

impl ConfigImpl for Config {}

#[derive(Debug, Clone)]
pub struct Output {
    backoff: u64,
    next: u64,
    output: String,
}

#[derive(Debug, Clone)]
pub struct Backpressure {
    pub config: Config,
    pub outputs: Vec<Output>,
    pub steps: Vec<u64>,
    pub next: usize,
}

impl From<Config> for Backpressure {
    fn from(config: Config) -> Self {
        let steps = config.steps.iter().map(|v| *v * 1_000_000).collect();
        let outputs = config.outputs.iter().cloned().map(Output::from).collect();
        Self {
            config,
            outputs,
            steps,
            next: 0,
        }
    }
}

impl From<String> for Output {
    fn from(output: String) -> Self {
        Self {
            output,
            next: 0,
            backoff: 0,
        }
    }
}

fn d_steps() -> Vec<u64> {
    vec![50, 100, 250, 500, 1000, 5000, 10000]
}

fn d_outputs() -> Vec<String> {
    vec![String::from("out")]
}

impl Backpressure {
    pub fn next_backoff(&self, current: u64) -> u64 {
        let mut b = 0;
        for backoff in &self.steps {
            b = *backoff;
            if b > current {
                break;
            }
        }
        b
    }
}

op!(BackpressureFactory(node) {
    if let Some(map) = &node.config {
        let config: Config = Config::new(map)?;
        if config.outputs.is_empty() {
            error!("No outputs supplied for backpressure operators");
            return Err(ErrorKind::MissingOpConfig(node.id.to_string()).into());
        };
        // convert backoff to ns
        Ok(Box::new(Backpressure::from(config)))
    } else {
        Err(ErrorKind::MissingOpConfig(node.id.to_string()).into())

    }});

impl Operator for Backpressure {
    fn on_event(
        &mut self,
        _port: &str,
        _state: &mut Value<'static>,
        event: Event,
    ) -> Result<Vec<(Cow<'static, str>, Event)>> {
        let mut output = None;
        for n in 0..self.outputs.len() {
            let id = (self.next + n) % self.outputs.len();
            let o = &mut self.outputs[id];
            if o.next <= event.ingest_ns {
                // :/ need pipeline lifetime to fix
                output = Some(o.output.clone());
                if o.backoff > 0 {
                    o.next = event.ingest_ns + o.backoff;
                }
                self.next = id + 1;
                break;
            }
        }
        if let Some(out) = output {
            let (_, meta) = event.data.parts();

            if let Some(meta) = meta.as_object_mut() {
                meta.insert("backpressure-output".into(), out.clone().into());
            };
            Ok(vec![(out.into(), event)])
        } else {
            Ok(vec![("overflow".into(), event)])
        }
    }

    fn handles_contraflow(&self) -> bool {
        true
    }

    fn on_contraflow(&mut self, insight: &mut Event) {
        let meta = &insight.data.suffix().meta();
        if let Some(output) = meta.get("backpressure-output").and_then(Value::as_str) {
            for (i, o) in self.outputs.iter().enumerate() {
                if o.output == output {
                    if meta.get("error").and_then(Value::as_str).is_some() {
                        let backoff = self.next_backoff(o.backoff);
                        self.outputs[i].backoff = backoff;
                        self.outputs[i].next = insight.ingest_ns + backoff;
                    } else if let Some(v) = meta.get("time").and_then(Value::cast_f64) {
                        if v > self.config.timeout {
                            let backoff = self.next_backoff(o.backoff);
                            self.outputs[i].backoff = backoff;
                            self.outputs[i].next = insight.ingest_ns + backoff;
                        } else {
                            self.outputs[i].backoff = 0;
                            self.outputs[i].next = 0;
                        }
                    }
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use simd_json::value::borrowed::Object;

    #[test]
    fn pass_wo_error() {
        let mut op: Backpressure = Config {
            timeout: 100.0,
            steps: vec![1, 10, 100],
            outputs: d_outputs(),
        }
        .into();

        let mut state = Value::null();

        // Sent a first event, as all is initited clean
        // we syould see this pass
        let event1 = Event {
            origin_uri: None,
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            data: Value::from("snot").into(),
            kind: None,
        };
        let mut r = op
            .on_event("in", &mut state, event1.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("out", out);

        // Without a timeout event sent a second event,
        // it too should pass
        let event2 = Event {
            origin_uri: None,
            is_batch: false,
            id: 2,
            ingest_ns: 2,
            data: Value::from("badger").into(),
            kind: None,
        };
        let mut r = op
            .on_event("in", &mut state, event2.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("out", out);
    }

    #[test]
    fn block_on_error() {
        let mut op: Backpressure = Config {
            timeout: 100.0,
            steps: vec![1, 10, 100],
            outputs: d_outputs(),
        }
        .into();

        let mut state = Value::null();

        // Sent a first event, as all is initited clean
        // we syould see this pass
        let event1 = Event {
            origin_uri: None,
            is_batch: false,
            id: 1,
            ingest_ns: 1_000_000,
            data: Value::from("snot").into(),
            kind: None,
        };
        let mut r = op
            .on_event("in", &mut state, event1.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("out", out);

        // Insert a timeout event with `time` set top `200`
        // this is over our limit of `100` so we syould move
        // one up the backup steps
        let mut m = Object::new();
        m.insert("time".into(), 200.0.into());
        m.insert("backpressure-output".into(), "out".into());
        let mut insight = Event {
            origin_uri: None,
            is_batch: false,
            id: 1,
            ingest_ns: 1_000_000,
            data: (Value::null(), m).into(),
            kind: None,
        };

        // Verify that we now have a backoff of 1ms
        op.on_contraflow(&mut insight);
        assert_eq!(op.outputs[0].backoff, 1_000_000);

        // The first event was sent at exactly 1ms
        // our we should block all eventsup to
        // 1_999_999
        // this event syould overflow
        let event2 = Event {
            origin_uri: None,
            is_batch: false,
            id: 2,
            ingest_ns: 2_000_000 - 1,
            data: Value::from("badger").into(),
            kind: None,
        };
        let mut r = op
            .on_event("in", &mut state, event2.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("overflow", out);

        // On exactly 2_000_000 we should be allowed to send
        // again
        let event3 = Event {
            origin_uri: None,
            is_batch: false,
            id: 3,
            ingest_ns: 2_000_000,
            data: Value::from("boo").into(),
            kind: None,
        };
        let mut r = op
            .on_event("in", &mut state, event3.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("out", out);

        // Since now the last successful event was at 2_000_000
        // the next event should overflow at 2_000_001
        let event3 = Event {
            origin_uri: None,
            is_batch: false,
            id: 3,
            ingest_ns: 2_000_000 + 1,
            data: Value::from("badger").into(),
            kind: None,
        };
        let mut r = op
            .on_event("in", &mut state, event3.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("overflow", out);
    }

    #[test]
    fn walk_backoff() {
        let mut op: Backpressure = Config {
            timeout: 100.0,
            steps: vec![1, 10, 100],
            outputs: d_outputs(),
        }
        .into();
        // An contraflow that fails the timeout
        let mut m = Object::new();
        m.insert("time".into(), 200.0.into());
        m.insert("backpressure-output".into(), "out".into());

        let mut insight = Event {
            origin_uri: None,
            is_batch: false,
            id: 1,
            ingest_ns: 2,
            data: (Value::null(), m).into(),
            kind: None,
        };

        // A contraflow that passes the timeout
        let mut m = Object::new();
        m.insert("time".into(), 99.0.into());
        m.insert("backpressure-output".into(), "out".into());

        let mut insight_reset = Event {
            origin_uri: None,
            is_batch: false,
            id: 1,
            ingest_ns: 2,
            data: (Value::null(), m).into(),
            kind: None,
        };

        // Assert initial state
        assert_eq!(op.outputs[0].backoff, 0);
        // move one step up
        op.on_contraflow(&mut insight);
        assert_eq!(op.outputs[0].backoff, 1_000_000);
        // move another step up
        op.on_contraflow(&mut insight);
        assert_eq!(op.outputs[0].backoff, 10_000_000);
        // move another another step up
        op.on_contraflow(&mut insight);
        assert_eq!(op.outputs[0].backoff, 100_000_000);
        // We are at the highest step everything
        // should stay  the same
        op.on_contraflow(&mut insight);
        assert_eq!(op.outputs[0].backoff, 100_000_000);
        // Now we should reset
        op.on_contraflow(&mut insight_reset);
        assert_eq!(op.outputs[0].backoff, 0);
    }

    #[test]
    fn multi_output_block() {
        let mut op: Backpressure = Config {
            timeout: 100.0,
            steps: vec![1, 10, 100],
            outputs: vec!["out".into(), "snot".into()],
        }
        .into();

        let mut state = Value::null();

        // Sent a first event, as all is initited clean
        // we syould see this pass
        let event1 = Event {
            origin_uri: None,
            is_batch: false,
            id: 1,
            ingest_ns: 1_000_000,
            data: Value::from("snot").into(),
            kind: None,
        };
        let mut r = op
            .on_event("in", &mut state, event1.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("out", out);

        // Sent a first event, as all is initited clean
        // we syould see this pass
        let event2 = Event {
            origin_uri: None,
            is_batch: false,
            id: 2,
            ingest_ns: 1_000_001,
            data: Value::from("snot").into(),
            kind: None,
        };
        let mut r = op
            .on_event("in", &mut state, event2.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("snot", out);

        // Insert a timeout event with `time` set top `200`
        // this is over our limit of `100` so we syould move
        // one up the backup steps
        let mut m = Object::new();
        m.insert("time".into(), 200.0.into());
        m.insert("backpressure-output".into(), "out".into());

        let mut insight = Event {
            origin_uri: None,
            is_batch: false,
            id: 1,
            ingest_ns: 1_000_000,
            data: (Value::null(), m).into(),
            kind: None,
        };

        // Verify that we now have a backoff of 1ms
        op.on_contraflow(&mut insight);
        assert_eq!(op.outputs[0].backoff, 1_000_000);
        assert_eq!(op.outputs[0].next, 2_000_000);

        // The first event was sent at exactly 1ms
        // our we should block all eventsup to
        // 1_999_999
        // this event syould overflow
        let event2 = Event {
            origin_uri: None,
            is_batch: false,
            id: 2,
            ingest_ns: 2_000_000 - 1,
            data: Value::from("badger").into(),
            kind: None,
        };
        let mut r = op
            .on_event("in", &mut state, event2.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("snot", out);

        // On exactly 2_000_000 we should be allowed to send
        // again
        let event3 = Event {
            origin_uri: None,
            is_batch: false,
            id: 3,
            ingest_ns: 2_000_000,
            data: Value::from("boo").into(),
            kind: None,
        };
        let mut r = op
            .on_event("in", &mut state, event3.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("out", out);

        // Since now the last successful event was at 2_000_000
        // the next event should overflow at 2_000_001
        let event3 = Event {
            origin_uri: None,
            is_batch: false,
            id: 3,
            ingest_ns: 2_000_000 + 1,
            data: Value::from("badger").into(),
            kind: None,
        };
        let mut r = op
            .on_event("in", &mut state, event3.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("snot", out);
    }
}
