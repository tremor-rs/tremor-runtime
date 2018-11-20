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

//! # Store data into metadata variable
//!
//! Takes a value out of the event and stores it into a metadata variable
//!
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
//!
//!
//! ## Output Variables
//!
//! * `<var>` (only enforced if `required` is set to true)

use dflt;
use error::TSError;
use errors::*;
use pipeline::prelude::*;
use serde_json;
use std::fmt;

#[derive(Deserialize)]
pub struct Config {
    /// name of the variable to set
    pub var: String,
    /// the key to get the variable from
    pub key: String,
    /// if set to true the event will be send to the error output if the key
    /// is not in the event. (default: false)
    #[serde(default = "dflt::d_false")]
    pub required: bool,
}

pub struct Op {
    config: Config,
}
impl fmt::Debug for Op {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IntoVar(key: '{:?}', variable: '{}')",
            self.config.key, self.config.var
        )
    }
}

impl Op {
    pub fn new(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        Ok(Self { config })
    }
}

impl Opable for Op {
    fn exec(&mut self, mut event: EventData) -> EventResult {
        if !event.is_type(ValueType::JSON) {
            let t = event.value.t();
            return EventResult::Error(
                event,
                Some(TSError::from(TypeError::with_location(
                    &"op::into_var2",
                    t,
                    ValueType::JSON,
                ))),
            );
        };

        let val: Option<MetaValue> = {
            if let EventValue::JSON(ref val) = event.value {
                match val.get(&self.config.key) {
                    Some(serde_json::Value::String(v)) => Some(v.into()),
                    _ => None,
                }
            } else {
                unreachable!()
            }
        };

        match val {
            Some(val) => {
                event.set_var(&self.config.var, val);
                EventResult::Next(event)
            }
            None => if self.config.required {
                EventResult::Error(event, Some(TSError::new(&format!("Key `{:?}` needs to be present but was not. So the variable `{}` can not be set.", self.config.key, self.config.var))))
            } else {
                EventResult::Next(event)
            },
        }
    }

    fn output_vars(&self) -> HashSet<String> {
        let mut h = HashSet::new();
        if self.config.required {
            h.insert(self.config.var.clone());
        };
        h
    }

    opable_types!(ValueType::JSON, ValueType::JSON);
}
