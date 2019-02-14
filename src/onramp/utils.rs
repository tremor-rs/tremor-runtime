// Copyright 2019, Wayfair GmbH
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

use std::io::{BufRead, BufReader};
use std::collections::HashMap;
use crate::utils;
use futures::sync::mpsc::channel;
use std::fs::File;
use crate::pipeline::prelude::*;
use futures::Stream;
use crate::onramp::OnRampActor;
use actix::prelude::*;

// file reading function used across many pipelines
pub fn process_file(file: std::string::String, pipelines: &[Addr<OnRampActor>]) {
    let len = pipelines.len();
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
}
