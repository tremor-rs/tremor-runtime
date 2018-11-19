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

use futures::sync::mpsc::channel;
use futures::Future;
use futures::Stream;
use onramp::{EnterReturn, Onramp as OnrampT, PipelineOnramp, PipelineOnrampElem};
use pipeline::prelude::*;
use serde_yaml;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufRead, Read};
use std::path::Path;
use std::thread;
use std::time::Duration;
use utils::{nanotime, park};
use xz2::read::XzDecoder;

#[cfg(feature = "try_spmc")]
use spmc;

lazy_static! {
    static ref ONE_NS: Duration = Duration::from_nanos(1);
}

pub struct Onramp {
    config: Config,
    data: Vec<u8>,
}

#[derive(Deserialize, Debug, Clone)]
struct Config {
    source: String,
    interval: Option<u64>,
    iters: Option<u64>,
}

impl Onramp {
    pub fn new(opts: &ConfValue) -> Onramp {
        match serde_yaml::from_value(opts.clone()) {
            Ok(config @ Config { .. }) =>{
                let mut source_data_file = File::open(&config.source).unwrap();
                {
                    let ext = Path::new(&config.source).extension().and_then(OsStr::to_str).unwrap();
                    let mut raw = vec![];
                    if ext == "xz" {
                        XzDecoder::new(source_data_file).read_to_end(&mut raw).expect("Neither a readable nor valid XZ compressed file error");
                    } else {
                        source_data_file.read_to_end(&mut raw).expect("Unable to read data source file error");
                    }
                    Onramp { config: config.clone(), data: raw }
                }
            }
            e => {
                panic!("Invalid options for Blaster onramp, use `{{\"source\": \"<path/to/file.json|path/to/file.json.xz>\", \"is_coordinated\": [true|false], \"rate\": <rate>}}`\n{:?} ({:?})", e, opts)
            }
        }
    }
}
pub fn step_ival(data: &[u8], next: u64, interval: u64, pipelines: &[PipelineOnrampElem]) -> u64 {
    let mut next = next;
    for line in data.lines() {
        if let Ok(line) = line {
            while nanotime() < next {
                park(Duration::from_nanos(1));
            }
            next += interval;
            let msg = OnData {
                reply_channel: None,
                data: EventValue::Raw(line.into_bytes()),
                vars: HashMap::new(),
                ingest_ns: nanotime(),
            };
            pipelines[0].do_send(msg);
        }
    }
    next
}

pub fn step(data: &[u8], pipelines: &[PipelineOnrampElem]) {
    for line in data.lines() {
        if let Ok(line) = line {
            let (tx, rx) = channel(0);
            let msg = OnData {
                reply_channel: Some(tx),
                data: EventValue::Raw(line.into_bytes()),
                vars: HashMap::new(),
                ingest_ns: nanotime(),
            };
            pipelines[0].do_send(msg);
            for _r in rx.wait() {}
        }
    }
}

impl OnrampT for Onramp {
    fn enter_loop(&mut self, pipelines: PipelineOnramp) -> EnterReturn {
        let iters = self.config.iters;
        let interval = self.config.interval;
        let data = self.data.clone();
        thread::spawn(move || {
            let mut next;

            match interval {
                None => {
                    if let Some(iters) = iters {
                        for _ in 0..iters {
                            step(&data, &pipelines);
                        }
                    } else {
                        loop {
                            step(&data, &pipelines);
                        }
                    }
                }
                Some(interval) => {
                    next = nanotime() + interval;
                    if let Some(iters) = iters {
                        for _ in 0..iters {
                            next = step_ival(&data, next, interval, &pipelines);
                        }
                    } else {
                        loop {
                            next = step_ival(&data, next, interval, &pipelines);
                        }
                    }
                }
            }
            for p in pipelines {
                let _ = p.send(Shutdown).wait();
            }
        })
    }
}
