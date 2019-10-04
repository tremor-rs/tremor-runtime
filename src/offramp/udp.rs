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

//! # UDP Offramp
//!
//! Sends each message as a udp datagram
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use super::{Offramp, OfframpImpl};
use crate::codec::Codec;
use crate::errors::*;
use crate::offramp::prelude::make_postprocessors;
use crate::postprocessor::Postprocessors;
use crate::system::PipelineAddr;
use crate::url::TremorURL;
use crate::{Event, OpConfig};
use halfbrown::HashMap;
use serde_yaml;
use std::net::UdpSocket;

/// An offramp that write a given file
pub struct Udp {
    socket: UdpSocket,
    config: Config,
    pipelines: HashMap<TremorURL, PipelineAddr>,
    postprocessors: Postprocessors,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    /// Host to use as source
    pub host: String,
    pub port: u16,
    pub dst_host: String,
    pub dst_port: u16,
}

impl OfframpImpl for Udp {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            let socket = UdpSocket::bind((config.host.as_str(), config.port))?;
            socket.connect((config.dst_host.as_str(), config.dst_port))?;
            Ok(Box::new(Udp {
                config,
                socket,
                pipelines: HashMap::new(),
                postprocessors: vec![],
            }))
        } else {
            Err("Blackhole offramp requires a config".into())
        }
    }
}

impl Offramp for Udp {
    // TODO
    fn on_event(&mut self, codec: &Box<dyn Codec>, _input: String, event: Event) {
        for value in event.value_iter() {
            if let Ok(ref raw) = codec.encode(value) {
                //TODO: Error handling
                if let Err(e) = self.socket.send(&raw) {
                    error!(
                        "Failed wo send UDP datagram to {}:{} => {}",
                        self.config.dst_host, self.config.dst_port, e
                    )
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
