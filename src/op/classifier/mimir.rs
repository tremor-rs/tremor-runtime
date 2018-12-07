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

//! # MIMIR based classifier
//!
//! Classification based on matching JSON values with mimir rules
//!
//! ## Configuration
//!
//! This operator has no configuration

use crate::error::TSError;
use crate::errors::*;
use crate::pipeline::prelude::*;
use mimir::Rules;
use serde_yaml;

#[derive(Debug)]
pub struct Classifier {
    rules: Rules<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct ConfRule {
    rule: String,
    class: String,
}

#[derive(Debug, Deserialize)]
struct Config {
    rules: Vec<ConfRule>,
}

impl Classifier {
    pub fn create(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        let mut rules: Rules<String> = Rules::default();

        for r in config.rules.iter() {
            // clone OK here as this is on init
            rules.add_rule(r.class.clone(), &r.rule)?;
        }

        Ok(Classifier { rules })
    }
}

impl Opable for Classifier {
    opable_types!(ValueType::JSON, ValueType::JSON);
    fn exec(&mut self, mut event: EventData) -> EventResult {
        if !event.is_type(ValueType::JSON) {
            let t = event.value.t();
            return EventResult::Error(
                event,
                Some(TSError::from(TypeError::with_location(
                    &"classifier::mimir",
                    t,
                    ValueType::JSON,
                ))),
            );
        };
        let c;

        if let EventValue::JSON(ref val) = event.value {
            c = self.rules.eval_first_wins(val);
        } else {
            unreachable!()
        };

        match c {
            Ok(Some(id)) => {
                event.set_var(&"classification", id);
                EventResult::Next(event)
            }
            _ => {
                event.set_var(&"classification", "default");
                EventResult::Next(event)
            }
        }
    }
}
