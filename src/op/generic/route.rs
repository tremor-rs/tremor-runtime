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

//!
//! # Tremor set Operation
//!
//! Sets a variable to a static value
//!
//! ## Config
//!
//! * `var` - name of the variable to set
//! * `val` - the value to set the variable to
//!
//! ## Output Variables
//!
//! * `<var>`

use errors::*;
use pipeline::prelude::*;
use serde_yaml;
use std::collections::HashMap;

#[derive(Clone, Debug, Deserialize)]
struct Config {
    var: String,
    vals: Vec<ConfValue>,
}
#[derive(Debug, Clone)]
pub struct Op {
    config: Config,
    vals: HashMap<MetaValue, usize>,
}

impl Op {
    pub fn new(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;

        let mut vals = HashMap::new();
        // we map each value to an input, standard and error are special
        // standard is used if no case match as a default case
        let mut i = 3;
        for v in config.vals.clone() {
            match v {
                ConfValue::Number(ref n) => if let Some(n) = n.as_u64() {
                    vals.insert(MetaValue::U64(n), i);
                    i += 1;
                },
                ConfValue::String(ref s) => {
                    vals.insert(MetaValue::String(s.clone()), i);
                    i += 1;
                }
                _ => (),
            }
        }
        Ok(Op { config, vals })
    }
}

impl Opable for Op {
    fn exec(&mut self, event: EventData) -> EventResult {
        let r = match event.vars.get(&self.config.var) {
            Some(ref v) => self.vals.get(v),
            None => None,
        };
        match r {
            Some(i) => EventResult::NextID(*i, event),
            None => EventResult::Next(event),
        }
    }
    opable_types!(ValueType::Same, ValueType::Same);
}
