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

//! # File Offramp
//!
//! Writes events to a file, one event per line
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use crate::offramp::prelude::*;
use halfbrown::HashMap;
use serde_yaml;
use std::fs::File as FSFile;
use std::io::Write;

/// An offramp that write a given file
pub struct File {
    file: FSFile,
    pipelines: HashMap<TremorURL, PipelineAddr>,
    postprocessors: Postprocessors,
    config: Config,
}

#[derive(Deserialize)]
pub struct Config {
    /// Filename to write to
    pub file: String,
}

impl offramp::Impl for File {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            let file = FSFile::create(&config.file)?;
            Ok(Box::new(Self {
                file,
                pipelines: HashMap::new(),
                postprocessors: vec![],
                config,
            }))
        } else {
            Err("Blackhole offramp requires a config".into())
        }
    }
}

impl Offramp for File {
    // TODO
    fn on_event(&mut self, codec: &Box<dyn Codec>, _input: String, event: Event) {
        for value in event.value_iter() {
            if let Ok(ref raw) = codec.encode(value) {
                match postprocess(&mut self.postprocessors, event.ingest_ns, raw.to_vec()) {
                    Ok(packets) => {
                        for packet in packets {
                            //TODO: Error handling
                            if let Err(e) = self
                                .file
                                .write_all(&packet)
                                .and_then(|_| self.file.write_all(b"\n"))
                            {
                                error!("Failed wo write to stdout file: {}", e)
                            }
                        }
                    }
                    Err(e) => error!(
                        "Failed to postprocess before writing to file {}: {}",
                        self.config.file, e
                    ),
                }
            }
        }
    }
    fn add_pipeline(&mut self, id: TremorURL, addr: PipelineAddr) {
        self.pipelines.insert(id, addr);
    }
    fn remove_pipeline(&mut self, id: TremorURL) -> bool {
        self.pipelines.remove(&id);
        self.pipelines.is_empty()
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    fn start(&mut self, _codec: &Box<dyn Codec>, postprocessors: &[String]) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }
}
