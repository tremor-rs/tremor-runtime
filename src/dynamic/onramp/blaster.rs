// Copyright 2018-2019, Wayfair GmbH
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

use crate::dflt;
use crate::dynamic::onramp::prelude::*;
use base64;
use serde_yaml::Value;
use std::fs::File;
use std::io::{BufRead, Read};
use std::path::Path;
use std::thread;
use std::time::Duration;
use xz2::read::XzDecoder;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// source file to read data from, it will be iterated over repeatedly,
    /// can be xz compressed
    pub source: String,
    /// Interval in nanoseconds for coordinated emission testing
    pub interval: Option<u64>,
    /// Number of iterations to stop after
    pub iters: Option<u64>,
    #[serde(default = "dflt::d_false")]
    pub base64: bool,
}

pub struct Blaster {
    pub config: Config,
    data: Vec<u8>,
}

impl OnrampImpl for Blaster {
    fn from_config(config: &Option<Value>) -> Result<Box<Onramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            let mut source_data_file = File::open(&config.source)?;
            let ext = Path::new(&config.source).extension().unwrap();
            let mut data = vec![];
            if ext == "xz" {
                XzDecoder::new(source_data_file)
                    .read_to_end(&mut data)
                    .expect("Neither a readable nor valid XZ compressed file error");
            } else {
                source_data_file
                    .read_to_end(&mut data)
                    .expect("Unable to read data source file error");
            };
            Ok(Box::new(Blaster { config, data }))
        } else {
            Err("Missing config for blaster onramp".into())
        }
    }
}

#[derive(Default)]
struct Acc {
    elements: Vec<Vec<u8>>,
    consuming: Vec<Vec<u8>>,
    count: u64,
}

fn onramp_loop(
    rx: Receiver<OnrampMsg>,
    data: Vec<u8>,
    config: Config,
    codec: Box<dyn Codec>,
) -> Result<()> {
    let mut pipelines: Vec<(TremorURL, PipelineAddr)> = Vec::new();
    let mut acc = Acc::default();
    acc.elements = data
        .lines()
        .map(|e| {
            let data = e.unwrap();
            if config.base64 {
                base64::decode(&data).unwrap()
            } else {
                data.as_bytes().to_vec()
            }
        })
        .collect();
    acc.consuming = acc.elements.clone();

    let iters = config.iters;
    let _interval = if let Some(i) = config.interval { i } else { 0 };
    let mut id = 0;
    loop {
        if pipelines.is_empty() {
            match rx.recv()? {
                OnrampMsg::Connect(mut ps) => pipelines.append(&mut ps),
                OnrampMsg::Disconnect { tx, .. } => {
                    let _ = tx.send(true);
                    return Ok(());
                }
            };
            continue;
        } else {
            // TODO better sleep perha
            if let Some(ival) = config.interval {
                thread::sleep(Duration::from_nanos(ival));
            }
            match rx.try_recv() {
                Err(TryRecvError::Empty) => (),
                Err(_e) => return Err("Crossbream receive error".into()),
                Ok(OnrampMsg::Connect(mut ps)) => pipelines.append(&mut ps),
                Ok(OnrampMsg::Disconnect { id, tx }) => {
                    pipelines.retain(|(pipeline, _)| pipeline != &id);
                    if pipelines.is_empty() {
                        let _ = tx.send(true);
                        return Ok(());
                    } else {
                        let _ = tx.send(false);
                    }
                }
            };
        }
        if Some(acc.count) == iters {
            return Ok(());
        };
        acc.count += 1;
        if acc.consuming.is_empty() {
            acc.consuming = acc.elements.clone();
        }

        let data = acc.consuming.pop().unwrap();
        send_event(&pipelines, &codec, id, EventValue::Raw(data));
        id += 1;
    }
}

impl Onramp for Blaster {
    fn start(&mut self, codec: String) -> Result<OnrampAddr> {
        let codec = codec::lookup(&codec).unwrap();
        let (tx, rx) = bounded(0);
        let data2 = self.data.clone();
        let config2 = self.config.clone();

        thread::Builder::new()
            .name(format!("onramp-blaster-{}", "???"))
            .spawn(|| onramp_loop(rx, data2, config2, codec))?;
        Ok(tx)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
