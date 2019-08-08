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

use crate::onramp::prelude::*;
//NOTE: This is required for StreamHander's stream
use crate::utils::nanotime;
use serde_yaml::Value;
use simd_json::json;
use std::time::Duration;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// Interval in milliseconds
    pub interval: u64,
}

pub struct Metronome {
    pub config: Config,
}

impl OnrampImpl for Metronome {
    fn from_config(config: &Option<Value>) -> Result<Box<Onramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            Ok(Box::new(Metronome { config }))
        } else {
            Err("Missing config for metronome onramp".into())
        }
    }
}

fn onramp_loop(
    rx: Receiver<OnrampMsg>,
    config: Config,
    preprocessors: Vec<String>,
    codec: String,
) -> Result<()> {
    let mut pipelines: Vec<(TremorURL, PipelineAddr)> = Vec::new();
    let mut id = 0;
    let mut codec = codec::lookup(&codec)?;
    let mut preprocessors = make_preprocessors(&preprocessors)?;

    loop {
        if pipelines.is_empty() {
            match rx.recv()? {
                OnrampMsg::Connect(mut ps) => pipelines.append(&mut ps),
                OnrampMsg::Disconnect { tx, .. } => {
                    let _ = tx.send(true);
                    return Ok(());;
                }
            };
            continue;
        } else {
            // TODO better sleep
            thread::sleep(Duration::from_nanos(config.interval));
            match rx.try_recv() {
                Err(TryRecvError::Empty) => (),
                Err(_e) => return Err("Crossbream receive error".into()),
                Ok(OnrampMsg::Connect(mut ps)) => pipelines.append(&mut ps),
                Ok(OnrampMsg::Disconnect { id, tx }) => {
                    pipelines.retain(|(pipeline, _)| pipeline != &id);
                    if pipelines.is_empty() {
                        let _ = tx.send(true);
                        return Ok(());;
                    } else {
                        let _ = tx.send(false);
                    }
                }
            };
        }
        thread::sleep(Duration::from_millis(config.interval));
        let data =
            serde_json::to_vec(&json!({"onramp": "metronome", "ingest_ns": nanotime(), "id": id}));
        if let Ok(data) = data {
            send_event(&pipelines, &mut preprocessors, &mut codec, id, data);
        }
        id += 1;
    }
}

impl Onramp for Metronome {
    fn start(&mut self, codec: String, preprocessors: Vec<String>) -> Result<OnrampAddr> {
        let config = self.config.clone();
        let (tx, rx) = bounded(0);
        thread::Builder::new()
            .name(format!("onramp-metronome-{}", "???"))
            .spawn(|| onramp_loop(rx, config, preprocessors, codec))?;
        Ok(tx)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
