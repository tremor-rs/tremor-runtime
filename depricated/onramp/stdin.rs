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

//! # StdIn Onramp
//!
//! The `stdin` onramp reads lines from stdin and enters them into the pipeline as events
//!
//! ## Configuration
//!
//! This OnRamp takes no configuration.
//!

use crate::errors::*;
use crate::onramp::{EnterReturn, Onramp as OnrampT, PipelineOnramp};
use crate::pipeline::prelude::*;
use crate::utils;
use futures::sync::mpsc::channel;
use futures::Stream;
use hashbrown::HashMap;
use std::io::{self, BufRead, BufReader};
use std::thread;

pub struct Onramp {}

impl Onramp {
    pub fn create(_opts: &ConfValue) -> Result<Self> {
        Ok(Self {})
    }
}

impl OnrampT for Onramp {
    fn enter_loop(&mut self, pipelines: PipelineOnramp) -> EnterReturn {
        let len = pipelines.len();
        thread::spawn(move || {
            let stdin = io::stdin();
            let stdin = BufReader::new(stdin);
            let i = 0;
            for line in stdin.lines() {
                match line {
                    Ok(line) => {
                        let (tx, rx) = channel(0);
                        let msg = OnData {
                            reply_channel: Some(tx),
                            data: EventValue::Raw(line.into_bytes()),
                            vars: HashMap::new(),
                            ingest_ns: utils::nanotime(),
                        };
                        let i = i + 1 % len;
                        pipelines[i].do_send(msg);

                        // NOTE Although we do nothing with the rx channel we
                        // are required to wait to avoid the receiver being
                        // dropped / going out of scope - which in turn introduces
                        // hard to debug ERRORS of the form:
                        //
                        // ERROR 2019-01-25T12:27:53Z: tremor_runtime::pipeline::onramp: Pipeline return error: send failed because receiver is gone
                        //
                        for _ in rx.wait() {}
                    }
                    Err(e) => error!("Onramp error: {}", e),
                }
            }
        })
    }
}
