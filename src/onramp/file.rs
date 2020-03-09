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
use async_compression::futures::bufread::XzDecoder;
use async_std::fs::File as FSFile;
use async_std::io::prelude::*;
use async_std::io::{BufReader, Lines};
use async_std::prelude::*;
use serde_yaml::Value;
use std::path::Path;
use std::process;
use std::thread;
use std::time::Duration;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// source file to read data from, it will be iterated over repeatedly,
    /// can be xz compressed
    pub source: String,
    #[serde(default = "dflt::d_false")]
    pub close_on_done: bool,
    #[serde(default = "dflt::d")]
    pub sleep_on_done: u64,
}

impl ConfigImpl for Config {}

pub struct File {
    pub config: Config,
}

enum ArghDyn {
    Xz(Lines<BufReader<XzDecoder<BufReader<FSFile>>>>),
    File(Lines<BufReader<FSFile>>),
}

impl ArghDyn {
    async fn next(&mut self) -> Option<std::io::Result<String>> {
        match self {
            ArghDyn::Xz(l) => l.next().await,
            ArghDyn::File(l) => l.next().await,
        }
    }
}

struct FileInt {
    pub config: Config,
    lines: ArghDyn,
    origin_uri: EventOriginUri,
}
impl FileInt {
    async fn from_config(config: Config) -> Result<Self> {
        let source_data_file = BufReader::new(FSFile::open(&config.source).await?);
        let ext = Path::new(&config.source)
            .extension()
            .map(std::ffi::OsStr::to_str);
        let lines = if ext == Some(Some("xz")) {
            let r = BufReader::new(XzDecoder::new(source_data_file));
            ArghDyn::Xz(r.lines())
        } else {
            let r = source_data_file;
            ArghDyn::File(r.lines())
        };

        let origin_uri = tremor_pipeline::EventOriginUri {
            scheme: "tremor-file".to_string(),
            host: hostname(),
            port: None,
            path: vec![config.source.clone()],
        };
        Ok(Self {
            config,
            lines,
            origin_uri,
        })
    }
}

impl onramp::Impl for File {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self { config }))
        } else {
            Err("Missing config for blaster onramp".into())
        }
    }
}

#[async_trait::async_trait()]
impl Source for FileInt {
    async fn read(&mut self) -> Result<SourceReply> {
        if let Some(Ok(line)) = self.lines.next().await {
            Ok(SourceReply::Data {
                origin_uri: self.origin_uri.clone(),
                data: line.as_bytes().to_vec(),
                stream: 0,
            })
        } else {
            task::sleep(Duration::from_millis(self.config.sleep_on_done)).await;
            if self.config.close_on_done {
                // ALLOW: This is on purpose, close when done tells the onramp to terminate when it's done with sending it's data - this is for one off's
                process::exit(0);
            }
            Ok(SourceReply::StateChange(SourceState::Disconnected))
        }
    }
    async fn init(&mut self) -> Result<SourceState> {
        Ok(SourceState::Connected)
    }
    fn trigger_breaker(&mut self) {}
    fn restore_breaker(&mut self) {}
}

#[async_trait::async_trait]
impl Onramp for File {
    async fn start(
        &mut self,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let source = FileInt::from_config(self.config.clone()).await?;
        let (manager, tx) =
            SourceManager::new(source, preprocessors, codec, metrics_reporter).await?;
        thread::Builder::new()
            .name(format!("onramp-file-{}", self.config.source))
            .spawn(move || task::block_on(manager.run()))?;
        Ok(tx)
    }
    fn default_codec(&self) -> &str {
        "json"
    }
}
