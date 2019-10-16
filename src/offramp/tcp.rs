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

//! # TCP Offramp
//!
//! Sends each message as a tcp stream
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use crate::offramp::prelude::*;
use halfbrown::HashMap;
use serde_yaml;
use std::io::Write;
use std::net::TcpStream;

/// An offramp streams over TCP/IP
pub struct Tcp {
    stream: TcpStream,
    config: Config,
    pipelines: HashMap<TremorURL, PipelineAddr>,
    postprocessors: Postprocessors,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub host: String,
    pub port: u16,
    #[serde(default = "dflt::d_false")]
    pub is_non_blocking: bool,
    #[serde(default = "dflt::d_ttl")]
    pub ttl: u32,
    #[serde(default = "dflt::d_true")]
    pub is_no_delay: bool,
}

impl offramp::Impl for Tcp {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            let stream = TcpStream::connect((config.host.as_str(), config.port))?;
            stream.set_nonblocking(config.is_non_blocking)?;
            stream.set_ttl(config.ttl)?;
            stream.set_nodelay(config.is_no_delay)?;
            Ok(Box::new(Tcp {
                config,
                stream,
                pipelines: HashMap::new(),
                postprocessors: vec![],
            }))
        } else {
            Err("TCP offramp requires a config".into())
        }
    }
}

impl Offramp for Tcp {
    fn on_event(&mut self, codec: &Box<dyn Codec>, _input: String, event: Event) {
        for value in event.value_iter() {
            if let Ok(ref raw) = codec.encode(value) {
                match postprocess(&mut self.postprocessors, event.ingest_ns, raw.to_vec()) {
                    Ok(packets) => {
                        for packet in packets {
                            if let Err(e) = self.stream.write(&packet) {
                                error!(
                                    "Failed wo send over TCP stream to {}:{} => {}",
                                    self.config.host, self.config.port, e
                                )
                            }
                        }
                    }
                    Err(e) => error!(
                        "Failed to postprocess before sending over TCP stream to {}:{} => {}",
                        self.config.host, self.config.port, e
                    ),
                }
                if let Err(e) = self.stream.flush() {
                    error!("failed to flush stream: {}", e);
                };
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
