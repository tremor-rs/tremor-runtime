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
//! # Tremor file Onramp
//!
//! The `file` onramp reads lines from a provided file and enters them into the pipeline as events.
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use errors::*;
use futures::sync::mpsc::channel;
use futures::Stream;
use onramp::{EnterReturn, Onramp as OnrampT, PipelineOnramp};
use pipeline::prelude::*;
use serde_yaml;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::thread;
use utils;

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
    pub fn new(opts: &ConfValue) -> Result<Self> {
        let Config { file } = serde_yaml::from_value(opts.clone())?;
        Ok(Onramp { file })
    }
}

impl OnrampT for Onramp {
    fn enter_loop(&mut self, pipelines: PipelineOnramp) -> EnterReturn {
        let file = self.file.clone();
        let len = pipelines.len();
        thread::spawn(move || {
            let reader = BufReader::new(File::open(file).unwrap());
            let mut i = 0;
            for (_num, line) in reader.lines().enumerate() {
                if let Ok(line) = line {
                    let (tx, rx) = channel(0);
                    let msg = OnData {
                        reply_channel: Some(tx),
                        data: EventValue::Raw(line.into_bytes()),
                        vars: HashMap::new(),
                        ingest_ns: utils::nanotime(),
                    };
                    i = (i + 1) % len;
                    pipelines[i].do_send(msg);
                    for _r in rx.wait() {}
                }
            }
        })
    }
}
