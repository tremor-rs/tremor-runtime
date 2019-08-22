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

use super::{Offramp, OfframpImpl};
use crate::codec::Codec;
use crate::dflt;
use crate::errors::*;
use crate::offramp::prelude::{make_postprocessors, postprocess};
use crate::postprocessor::Postprocessors;
use crate::system::PipelineAddr;
use crate::url::TremorURL;
use crate::{Event, OpConfig};
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

impl OfframpImpl for Tcp {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            let stream = TcpStream::connect((config.host.as_str(), config.port))?;
            stream
                .set_nonblocking(config.is_non_blocking)
                .expect("cannot set non-blocking");
            stream.set_ttl(config.ttl).expect("cannot set ttl");
            stream
                .set_nodelay(config.is_no_delay)
                .expect("cannot set no delay");
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
        for event in event.into_iter() {
            if let Ok(ref raw) = codec.encode(event.value) {
                match postprocess(&mut self.postprocessors, raw.to_vec()) {
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
                self.stream.flush().expect("failed to flush stream");
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
    fn start(&mut self, _codec: &Box<dyn Codec>, postprocessors: &[String]) {
        self.postprocessors = make_postprocessors(postprocessors)
            .expect("failed to setup post processors for stdout");
    }
}
