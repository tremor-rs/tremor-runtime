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

use crate::dflt;
use crate::onramp::prelude::*;
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

#[derive(Clone)]
pub struct Blaster {
    pub config: Config,
    onramp_id: TremorURL,
    data: Vec<u8>,
    acc: Acc,
    origin_uri: tremor_pipeline::EventOriginUri,
}
impl std::fmt::Debug for Blaster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Blaster")
    }
}

impl onramp::Impl for Blaster {
    fn from_config(id: &TremorURL, config: &Option<Value>) -> Result<Box<dyn Onramp>> {
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
            let origin_uri = tremor_pipeline::EventOriginUri {
                uid: 0,
                scheme: "tremor-blaster".to_string(),
                host: hostname(),
                port: None,
                path: vec![config.source.clone()],
            };

            Ok(Box::new(Self {
                config,
                data,
                acc: Acc::default(),
                origin_uri,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Missing config for blaster onramp".into())
        }
    }
}

#[derive(Clone, Default)]
struct Acc {
    elements: Vec<Vec<u8>>,
    consuming: Vec<Vec<u8>>,
    count: u64,
}

#[async_trait::async_trait()]
impl Source for Blaster {
    fn id(&self) -> &TremorURL {
        &self.onramp_id
    }

    async fn read(&mut self, id: u64) -> Result<SourceReply> {
        let _id = id;
        // TODO better sleep perhaps
        if let Some(ival) = self.config.interval {
            thread::sleep(Duration::from_nanos(ival));
        }
        if Some(self.acc.count) == self.config.iters {
            return Ok(SourceReply::StateChange(SourceState::Disconnected));
        };
        self.acc.count += 1;
        if self.acc.consuming.is_empty() {
            self.acc.consuming = self.acc.elements.clone();
        }

        if let Some(data) = self.acc.consuming.pop() {
            Ok(SourceReply::Data {
                origin_uri: self.origin_uri.clone(),
                data,
                stream: 0,
            })
        } else {
            Ok(SourceReply::StateChange(SourceState::Disconnected))
        }
    }
    async fn init(&mut self) -> Result<SourceState> {
        let elements: Result<Vec<Vec<u8>>> = self
            .data
            .lines()
            .map(|e| -> Result<Vec<u8>> {
                if self.config.base64 {
                    Ok(base64::decode(&e?.as_bytes())?)
                } else {
                    Ok(e?.as_bytes().to_vec())
                }
            })
            .collect();
        self.acc.elements = elements?;
        self.acc.consuming = self.acc.elements.clone();
        Ok(SourceState::Connected)
    }
}

#[async_trait::async_trait]
impl Onramp for Blaster {
    async fn start(
        &mut self,
        onramp_uid: u64,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        SourceManager::start(
            onramp_uid,
            self.clone(),
            codec,
            preprocessors,
            metrics_reporter,
        )
        .await
    }
    fn default_codec(&self) -> &str {
        "json"
    }
}
