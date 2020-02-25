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

impl ConfigImpl for Config {}

pub struct File {
    pub config: Config,
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

fn onramp_loop(
    rx: &Receiver<onramp::Msg>,
    config: &Config,
    mut preprocessors: Preprocessors,
    mut codec: Box<dyn Codec>,
    mut metrics_reporter: RampReporter,
) -> Result<()> {
    let source_data_file = FSFile::open(&config.source)?;
    let mut pipelines: Vec<(TremorURL, pipeline::Addr)> = Vec::new();
    let mut id = 0;
    let ext = Path::new(&config.source)
        .extension()
        .map(std::ffi::OsStr::to_str);
    let reader: Box<dyn BufRead> = if ext == Some(Some("xz")) {
        Box::new(BufReader::new(XzDecoder::new(source_data_file)))
    } else {
        Box::new(BufReader::new(source_data_file))
    };

    let origin_uri = tremor_pipeline::EventOriginUri {
        scheme: "tremor-file".to_string(),
        host: hostname(),
        port: None,
        path: vec![config.source.clone()],
    };

    for line in reader.lines() {
        loop {
            match task::block_on(handle_pipelines(&rx, &mut pipelines, &mut metrics_reporter))? {
                PipeHandlerResult::Retry => continue,
                PipeHandlerResult::Terminate => return Ok(()),
                PipeHandlerResult::Normal => break,
            }
        }

        let mut ingest_ns = nanotime();
        send_event(
            &pipelines,
            &mut preprocessors,
            &mut codec,
            &mut metrics_reporter,
            &mut ingest_ns,
            &origin_uri,
            id,
            line?.as_bytes().to_vec(),
        );
        id += 1;
    }

    //TODO: This is super gugly:
    if config.close_on_done {
        thread::sleep(Duration::from_millis(500));
        // ALLOW: This is on purpose, close when done tells the onramp to terminate when it's done with sending it's data - this is for one off's
        process::exit(0);
    }
    Ok(())
}

impl Onramp for File {
    fn start(
        &mut self,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let (tx, rx) = channel(1);
        let config = self.config.clone();
        let codec = codec::lookup(&codec)?;
        let preprocessors = make_preprocessors(&preprocessors)?;
        thread::Builder::new()
            .name(format!("onramp-file-{}", "???"))
            .spawn(move || {
                if let Err(e) = onramp_loop(&rx, &config, preprocessors, codec, metrics_reporter) {
                    error!("[Onramp] Error: {}", e)
                }
            })?;
        Ok(tx)
    }
    fn default_codec(&self) -> &str {
        "json"
    }
}
