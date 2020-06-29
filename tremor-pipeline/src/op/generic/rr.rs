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
}

impl From<Config> for RoundRobin {
    fn from(config: Config) -> Self {
        let outputs = config.outputs.iter().cloned().map(Output::from).collect();
        Self {
            config,
            outputs,
            next: 0,
        }
    }
}

impl From<String> for Output {
    fn from(output: String) -> Self {
        Self {
            output,
            open: false,
        }
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
            event.op_meta.insert(uid, oid.into());
            Ok(vec![(out.into(), event)].into())
        } else {
            Ok(vec![("overflow".into(), event)].into())
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
            .get(&uid)
            .and_then(OwnedValue::as_usize)
            .and_then(|id| outputs.get_mut(id))
        {
            if let Some(CBAction::Trigger) = insight.cb {
                o.open = false;
            } else if let Some(CBAction::Restore) = insight.cb {
                o.open = true;
            }
        }
        let any_available = outputs.iter().any(|o| o.open);

        if any_available && !any_were_available {
            insight.cb = Some(CBAction::Restore);
            error!("Failed to restore circuit breaker");
        } else if any_were_available && !any_available {
            dbg!("triggered");
            insight.cb = Some(CBAction::Trigger);
            error!("Failed to trigger circuit breaker");
        } else {
            insight.cb = None;
        };
    }
}

#[cfg(test)]
mod test {
    //use super::*;
    //use simd_json::value::borrowed::Object;
}
