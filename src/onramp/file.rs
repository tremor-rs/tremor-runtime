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
use crate::onramp::prelude::*;
use serde_yaml::Value;
use std::fs::File as FSFile;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process;
use std::thread;
use std::time::Duration;
use xz2::read::XzDecoder;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// source file to read data from, it will be iterated over repeatedly,
    /// can be xz compressed
    pub source: String,
    #[serde(default = "dflt::d_false")]
    pub close_on_done: bool,
}

pub struct File {
    pub config: Config,
}

impl OnrampImpl for File {
    fn from_config(config: &Option<Value>) -> Result<Box<Onramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            Ok(Box::new(File { config }))
        } else {
            Err("Missing config for blaster onramp".into())
        }
    }
}

fn onramp_loop(rx: Receiver<OnrampMsg>, config: Config, codec: String) -> Result<()> {
    let source_data_file = FSFile::open(&config.source)?;
    let mut pipelines: Vec<(TremorURL, PipelineAddr)> = Vec::new();
    let mut id = 0;
    let codec = codec::lookup(&codec)?;
    let ext = Path::new(&config.source)
        .extension()
        .map(std::ffi::OsStr::to_str);
    let reader: Box<dyn BufRead> = if ext == Some(Some("xz")) {
        Box::new(BufReader::new(XzDecoder::new(source_data_file)))
    } else {
        Box::new(BufReader::new(source_data_file))
    };

    for line in reader.lines() {
        while pipelines.is_empty() {
            match rx.recv()? {
                OnrampMsg::Connect(mut ps) => pipelines.append(&mut ps),
                OnrampMsg::Disconnect { tx, .. } => {
                    let _ = tx.send(true);
                    return Ok(());
                }
            };
        }
        match rx.try_recv() {
            Err(TryRecvError::Empty) => (),
            Err(_e) => error!("Crossbream receive error"),
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

        send_event(&pipelines, &codec, id, line?.as_bytes().to_vec());
        id += 1;
    }

    //TODO: This is super gugly:
    if config.close_on_done {
        thread::sleep(Duration::from_millis(500));
        process::exit(0);
    }
    Ok(())
}

impl Onramp for File {
    fn start(&mut self, codec: String) -> Result<OnrampAddr> {
        let (tx, rx) = bounded(0);
        let config = self.config.clone();
        thread::Builder::new()
            .name(format!("onramp-file-{}", "???"))
            .spawn(move || {
                if let Err(e) = onramp_loop(rx, config, codec) {
                    error!("[Onramp] Error: {}", e)
                }
            })?;
        Ok(tx)
    }
    fn default_codec(&self) -> &str {
        "json"
    }
}
