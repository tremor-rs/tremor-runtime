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
//! # File Onramp
//!
//! The `file` onramp reads lines from a provided file and enters them into the pipeline as events.
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use crate::errors::*;
use crate::onramp::{EnterReturn, Onramp as OnrampT, PipelineOnramp};
use crate::pipeline::prelude::*;
use serde_yaml;
use std::thread;
use crate::onramp::utils::process_file;

#[derive(Deserialize)]
pub struct Onramp {
    file: String,
}

#[derive(Deserialize)]
pub struct Config {
    /// File to read from
    pub file: String,
}

impl Onramp {
    pub fn create(opts: &ConfValue) -> Result<Self> {
        let Config { file } = serde_yaml::from_value(opts.clone())?;
        Ok(Onramp { file })
    }
}

impl OnrampT for Onramp {
    fn enter_loop(&mut self, pipelines: PipelineOnramp) -> EnterReturn {
        let file = self.file.clone();
        thread::spawn(move || {process_file(file, &pipelines[..])})
    }
}
