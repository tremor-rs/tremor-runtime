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
//! # Copy between two metadata variables
//!
//! Copies the content from one metadata variable to another
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
//!
//! ## Output Variables
//!
//! * `<to>`

use errors::*;
use pipeline::prelude::*;
use serde_yaml;

/// Copy data from one meta variable to another
#[derive(Debug, Clone, Deserialize)]
pub struct Op {
    config: Config,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Source variable to copy from
    pub from: String,
    /// Destination variable to copy to
    pub to: String,
}

impl Op {
    pub fn new(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        Ok(Op { config })
    }
}

impl Opable for Op {
    fn exec(&mut self, mut event: EventData) -> EventResult {
        event.copy_var(&self.config.from, &self.config.to);
        EventResult::Next(event)
    }
    fn output_vars(&self) -> HashSet<String> {
        let mut h = HashSet::new();
        h.insert(self.to.clone());
        h
    }

    opable_types!(ValueType::Same, ValueType::Same);
}
