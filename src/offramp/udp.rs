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

//! # UDP Offramp
//!
//! Sends each message as a udp datagram
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use crate::offramp::prelude::*;
use halfbrown::HashMap;
use std::net::UdpSocket;

/// An offramp that write a given file
pub struct Udp {
    socket: UdpSocket,
    pipelines: HashMap<TremorURL, pipeline::Addr>,
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
impl ConfigImpl for Config {}

impl offramp::Impl for Udp {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let socket = UdpSocket::bind((config.host.as_str(), config.port))?;
            socket.connect((config.dst_host.as_str(), config.dst_port))?;
            Ok(Box::new(Self {
                socket,
                pipelines: HashMap::new(),
                postprocessors: vec![],
            }))
        } else {
            Err("Blackhole offramp requires a config".into())
        }
    }
}

#[async_trait::async_trait]
impl Offramp for Udp {
    // TODO
    #[allow(clippy::used_underscore_binding)]
    async fn on_event(&mut self, codec: &dyn Codec, _input: &str, event: Event) -> Result<()> {
        for value in event.value_iter() {
            let raw = codec.encode(value)?;
            //TODO: Error handling
            self.socket.send(&raw)?;
        }
        Ok(())
    }
    fn add_pipeline(&mut self, id: TremorURL, addr: pipeline::Addr) {
        self.pipelines.insert(id, addr);
    }
    fn remove_pipeline(&mut self, id: TremorURL) -> bool {
        self.pipelines.remove(&id);
        self.pipelines.is_empty()
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    #[allow(clippy::used_underscore_binding)]
    async fn start(&mut self, _codec: &dyn Codec, postprocessors: &[String]) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }
}
