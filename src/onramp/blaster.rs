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
use hostname::get_hostname;
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

impl ConfigImpl for Config {}

pub struct Blaster {
    pub config: Config,
    data: Vec<u8>,
}

impl onramp::Impl for Blaster {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let mut source_data_file = File::open(&config.source)?;
            let mut data = vec![];
            let ext = Path::new(&config.source)
                .extension()
                .map(std::ffi::OsStr::to_str);
            if ext == Some(Some("xz")) {
                XzDecoder::new(source_data_file).read_to_end(&mut data)?;
            } else {
                source_data_file.read_to_end(&mut data)?;
            };
            Ok(Box::new(Self { config, data }))
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

// We got to allow this because of the way that the onramp works
// with iterating over the data.
#[allow(clippy::needless_pass_by_value)]
fn onramp_loop(
    rx: &Receiver<onramp::Msg>,
    data: Vec<u8>,
    config: &Config,
    mut preprocessors: Preprocessors,
    mut codec: Box<dyn Codec>,
    mut metrics_reporter: RampMetricsReporter,
) -> Result<()> {
    let mut pipelines: Vec<(TremorURL, PipelineAddr)> = Vec::new();
    let mut acc = Acc::default();
    let elements: Result<Vec<Vec<u8>>> = data
        .lines()
        .map(|e| -> Result<Vec<u8>> {
            if config.base64 {
                Ok(base64::decode(&e?.as_bytes())?)
            } else {
                Ok(e?.as_bytes().to_vec())
            }
        })
        .collect();
    acc.elements = elements?;
    acc.consuming = acc.elements.clone();

    let origin_uri = tremor_pipeline::EventOriginUri {
        scheme: "tremor-blaster".to_string(),
        host: get_hostname().unwrap_or_else(|| "tremor-host.local".to_string()),
        port: None,
        path: vec![config.source.clone()],
    };

    let iters = config.iters;
    let mut id = 0;
    loop {
        if pipelines.is_empty() {
            match rx.recv()? {
                onramp::Msg::Connect(mut ps) => pipelines.append(&mut ps),
                onramp::Msg::Disconnect { tx, .. } => {
                    tx.send(true)?;
                    return Ok(());
                }
            };
            continue;
        } else {
            // TODO better sleep perhaps
            if let Some(ival) = config.interval {
                thread::sleep(Duration::from_nanos(ival));
            }
            match rx.try_recv() {
                Err(TryRecvError::Empty) => (),
                Err(_e) => return Err("Crossbream receive error".into()),
                Ok(onramp::Msg::Connect(mut ps)) => pipelines.append(&mut ps),
                Ok(onramp::Msg::Disconnect { id, tx }) => {
                    pipelines.retain(|(pipeline, _)| pipeline != &id);
                    if pipelines.is_empty() {
                        tx.send(true)?;
                        return Ok(());
                    } else {
                        tx.send(false)?;
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

        if let Some(data) = acc.consuming.pop() {
            let mut ingest_ns = nanotime();
            send_event(
                &pipelines,
                &mut preprocessors,
                &mut codec,
                &mut metrics_reporter,
                &mut ingest_ns,
                &origin_uri,
                id,
                data,
            );
            id += 1;
        }
    }
}

impl Onramp for Blaster {
    fn start(
        &mut self,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampMetricsReporter,
    ) -> Result<onramp::Addr> {
        let (tx, rx) = bounded(0);
        let data2 = self.data.clone();
        let config2 = self.config.clone();
        let codec = codec::lookup(&codec)?;
        let preprocessors = make_preprocessors(&preprocessors)?;
        thread::Builder::new()
            .name(format!("onramp-blaster-{}", "???"))
            .spawn(move || {
                onramp_loop(&rx, data2, &config2, preprocessors, codec, metrics_reporter)
            })?;
        Ok(tx)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
