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

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// List of outputs to round robin over
    #[serde(default = "d_outputs")]
    pub outputs: Vec<String>,
}

impl ConfigImpl for Config {}

#[derive(Debug, Clone)]
pub struct Output {
    open: bool,
    output: String,
}

#[derive(Debug, Clone)]
pub struct RoundRobin {
    pub config: Config,
    pub outputs: Vec<Output>,
    pub next: usize,
    first: bool,
}

impl From<Config> for RoundRobin {
    fn from(config: Config) -> Self {
        let outputs = config.outputs.iter().cloned().map(Output::from).collect();
        Self {
            config,
            outputs,
            next: 0,
            first: true,
        }
    }
}

impl From<String> for Output {
    fn from(output: String) -> Self {
        Self { output, open: true }
    }
}

fn d_outputs() -> Vec<String> {
    vec![String::from("out")]
}

op!(RoundRobinFactory(node) {
if let Some(map) = &node.config {
    let config: Config = Config::new(map)?;
    if config.outputs.is_empty() {
        error!("No outputs supplied for round robin operators");
        return Err(ErrorKind::MissingOpConfig(node.id.to_string()).into());
    };
    // convert backoff to ns
    Ok(Box::new(RoundRobin::from(config)))
} else {
    Err(ErrorKind::MissingOpConfig(node.id.to_string()).into())

}});

impl Operator for RoundRobin {
    fn on_event(
        &mut self,
        uid: u64,
        _port: &str,
        _state: &mut Value<'static>,
        mut event: Event,
    ) -> Result<EventAndInsights> {
        let mut output = None;
        for n in 0..self.outputs.len() {
            let id = (self.next + n) % self.outputs.len();
            let o = &mut self.outputs[id];
            if o.open {
                // :/ need pipeline lifetime to fix
                output = Some((o.output.clone(), id));
                self.next = id + 1;
                break;
            }
        }
        if let Some((out, oid)) = output {
            event.op_meta.insert(uid, oid);
            Ok(vec![(out.into(), event)].into())
        } else {
            Ok(vec![("overflow".into(), event)].into())
        }
    }

    fn handles_signal(&self) -> bool {
        true
    }
    fn on_signal(&mut self, _uid: u64, signal: &mut Event) -> Result<EventAndInsights> {
        if self.first && self.outputs.iter().any(|o| o.open) {
            let mut e = Event::cb_restore(signal.ingest_ns);
            e.origin_uri = None;
            self.first = false;

            Ok(EventAndInsights {
                insights: vec![e],
                events: vec![],
            })
        } else {
            Ok(EventAndInsights {
                insights: vec![],
                events: vec![],
            })
        }
    }
    fn handles_contraflow(&self) -> bool {
        true
    }

    fn on_contraflow(&mut self, uid: u64, insight: &mut Event) {
        let RoundRobin {
            ref mut outputs, ..
        } = *self;

        let any_were_available = outputs.iter().any(|o| o.open);
        if let Some(o) = insight
            .op_meta
            .get(uid)
            .and_then(OwnedValue::as_usize)
            .and_then(|id| outputs.get_mut(id))
        {
            if insight.cb == CBAction::Close {
                o.open = false;
            } else if insight.cb == CBAction::Open {
                o.open = true;
            }
        }
        let any_available = outputs.iter().any(|o| o.open);

        if any_available && !any_were_available {
            insight.cb = CBAction::Open;
            error!("Failed to restore circuit breaker");
        } else if any_were_available && !any_available {
            insight.cb = CBAction::Close;
            error!("Failed to trigger circuit breaker");
        } else if insight.cb.is_cb() {
            insight.cb = CBAction::None;
        };
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn multi_output_block() {
        let mut op: RoundRobin = Config {
            outputs: vec!["out".into(), "out2".into()],
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

        // Sent a first event, as all is initited clean
        // we syould see this pass
        let event2 = Event {
            id: 2.into(),
            ingest_ns: 1_000_001,
            ..Event::default()
        };
        let mut r = op
            .on_event(0, "in", &mut state, event2.clone())
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("out2", out);

        // Mark output 0 as broken
        let mut op_meta = OpMeta::default();
        op_meta.insert(0, 0);

        let mut insight = Event {
            id: 1.into(),
            ingest_ns: 1_000_000,
            cb: CBAction::Close,
            op_meta,
            ..Event::default()
        };

        // Verify that we are broken on 0
        op.on_contraflow(0, &mut insight);
        assert_eq!(op.outputs[0].open, false);
        assert_eq!(op.outputs[1].open, true);

        // Output should now come out of 1
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
        assert_eq!("out2", out);

        // Even for multiople events
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
        assert_eq!("out2", out);

        // Mark output 1 as restored
        let mut op_meta = OpMeta::default();
        op_meta.insert(0, 0);

        let mut insight = Event {
            id: 1.into(),
            ingest_ns: 1_000_000,
            cb: CBAction::Open,
            op_meta,
            ..Event::default()
        };

        // Verify that we now on disabled outputs
        op.on_contraflow(0, &mut insight);
        assert_eq!(op.outputs[0].open, true);
        assert_eq!(op.outputs[1].open, true);

        // The next event should go to the newly enabled output
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
        assert_eq!("out", out);
    }
}
