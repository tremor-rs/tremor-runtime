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
//! # Sets a metadata variable
//!
//! Sets a variable to a static value
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
//!
//! ## Output Variables
//!
//! * `<var>`

use errors::*;
use pipeline::prelude::*;
use serde_yaml;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// the variable to set
    pub var: String,
    /// the value to set it to
    pub val: String,
}

/// Set operator
pub struct Op {
    config: Config,
}
impl Op {
    pub fn new(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        Ok(Self { config })
    }
}

impl Opable for Op {
    fn exec(&mut self, event: EventData) -> EventResult {
        let mut event = event;
        event.set_var(&self.var, self.val.clone());
        EventResult::Next(event)
    }
    fn output_vars(&self) -> HashSet<String> {
        let mut h = HashSet::new();
        h.insert(self.var.clone());
        h
    }

    opable_types!(ValueType::Same, ValueType::Same);
}
