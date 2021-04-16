// Copyright 2020-2021, The Tremor Team
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

#![cfg(not(tarpaulin_include))]

//! # TCP Offramp
//!
//! Sends each message as a tcp stream
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use std::time::Instant;

use crate::sink::prelude::*;
use async_std::net::TcpStream;
use halfbrown::HashMap;

/// An offramp streams over TCP/IP
pub struct Tcp {
    stream: Option<TcpStream>,
    postprocessors: Postprocessors,
    config: Config,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub host: String,
    pub port: u16,
    #[serde(default = "ttl")]
    pub ttl: u32,
    #[serde(default = "t")]
    pub is_no_delay: bool,
}

fn t() -> bool {
    true
}

fn ttl() -> u32 {
    64
}

impl ConfigImpl for Config {}

impl offramp::Impl for Tcp {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(SinkManager::new_box(Self {
                config,
                stream: None,
                postprocessors: vec![],
            }))
        } else {
            Err("TCP offramp requires a config".into())
        }
    }
}

impl Tcp {
    async fn send_event(&mut self, codec: &mut dyn Codec, event: &Event) -> Result<()> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| Error::from(ErrorKind::NoSocket))?;
        for value in event.value_iter() {
            let raw = codec.encode(value)?;
            let packets = postprocess(&mut self.postprocessors, event.ingest_ns, raw.to_vec())?;
            for packet in packets {
                stream.write_all(&packet).await?;
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Sink for Tcp {
    /// We acknowledge ourself
    fn auto_ack(&self) -> bool {
        false
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn on_event(
        &mut self,
        _input: &str,
        codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        mut event: Event,
    ) -> ResultVec {
        let processing_start = Instant::now();
        let replies = match self.send_event(codec, &event).await {
            Ok(()) => {
                if event.transactional {
                    Some(vec![sink::Reply::Insight(event.insight_ack_with_timing(
                        processing_start.elapsed().as_millis() as u64,
                    ))])
                } else {
                    None
                }
            }
            // for TCP we always trigger the CB for IO/socket related errors
            Err(e @ Error(ErrorKind::Io(_), _)) | Err(e @ Error(ErrorKind::NoSocket, _)) => {
                debug!("[Sink::TCP] Error sending event: {}.", e);
                if event.transactional {
                    Some(vec![
                        sink::Reply::Insight(event.to_fail()),
                        sink::Reply::Insight(event.insight_trigger()),
                    ])
                } else {
                    Some(vec![sink::Reply::Insight(event.insight_trigger())]) // we always send a trigger
                }
            }
            // all other errors (codec/peprocessor etc.) just result in a fail
            Err(e) => {
                // regular error, no reason for CB
                debug!("[Sink::TCP] Error sending event: {}", e);

                if event.transactional {
                    Some(vec![sink::Reply::Insight(event.to_fail())])
                } else {
                    None
                }
            }
        };
        Ok(replies)
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        _sink_uid: u64,
        _sink_url: &TremorUrl,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        _is_linked: bool,
        _reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        self.postprocessors = make_postprocessors(processors.post)?;
        let stream = TcpStream::connect((self.config.host.as_str(), self.config.port)).await?;
        stream.set_ttl(self.config.ttl)?;
        stream.set_nodelay(self.config.is_no_delay)?;
        self.stream = Some(stream);
        Ok(())
    }
    async fn on_signal(&mut self, signal: Event) -> ResultVec {
        if self.stream.is_none() {
            let stream = if let Ok(stream) =
                TcpStream::connect((self.config.host.as_str(), self.config.port)).await
            {
                stream
            } else {
                return Ok(Some(vec![sink::Reply::Insight(Event::cb_trigger(
                    signal.ingest_ns,
                ))]));
            };
            stream.set_ttl(self.config.ttl)?;
            stream.set_nodelay(self.config.is_no_delay)?;
            self.stream = Some(stream);
            Ok(Some(vec![sink::Reply::Insight(Event::cb_restore(
                signal.ingest_ns,
            ))]))
        } else {
            Ok(None)
        }
    }
    fn is_active(&self) -> bool {
        self.stream.is_some()
    }
}
