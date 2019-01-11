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

//! # StdOut Offramp
//!
//! The `stdout` offramp writes events to the standard output (conse).
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
//!
//! ## Input Variables
//!
//! * `prefix` - sets the prefix (overrides the configuration) for raw and is ignored for JSON

use crate::errors::*;
use crate::pipeline::prelude::*;
use serde_yaml;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// string a prepend to a message (default: '')
    pub prefix: Option<String>,
}

/// An offramp that write to stdout
#[derive(Debug, Clone)]
pub struct Offramp {
    config: Config,
}

impl Offramp {
    pub fn create(opts: &ConfValue) -> Result<Self> {
        if opts == &ConfValue::Null {
            Ok(Self {
                config: Config { prefix: None },
            })
        } else {
            let config: Config = serde_yaml::from_value(opts.clone())?;
            Ok(Self { config })
        }
    }
}

impl Opable for Offramp {
    fn exec(&mut self, event: EventData) -> EventResult {
        let pfx = if let Some(MetaValue::String(ref pfx)) = event.var(&"prefix") {
            pfx.clone()
        } else if let Some(ref pfx) = self.config.prefix {
            pfx.clone()
        } else {
            "".to_string()
        };

        match event.make_return_and_value(Ok(None)) {
            (ret, EventValue::Raw(raw)) => {
                match String::from_utf8(raw.to_vec()) {
                    Ok(s) => println!("{}{}", pfx, s),
                    Err(e) => println!("{}{:?}", pfx, e),
                }
                EventResult::Return(ret)
            }
            (ret, EventValue::JSON(json)) => {
                println!("{}", json.to_string());
                EventResult::Return(ret)
            }
        }
    }

    opable_types!(ValueType::Any, ValueType::Any);
}
