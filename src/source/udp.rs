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

use crate::source::prelude::*;
use async_std::net::UdpSocket;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// The port to listen on.
    pub port: u16,
    pub host: String,
}

impl ConfigImpl for Config {}

pub struct Udp {
    pub config: Config,
    onramp_id: TremorURL,
}

struct Int {
    config: Config,
    socket: Option<UdpSocket>,
    onramp_id: TremorURL,
    origin_uri: EventOriginUri,
}
impl std::fmt::Debug for Int {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UDP")
    }
}

impl Int {
    fn from_config(uid: u64, onramp_id: TremorURL, config: &Config) -> Self {
        let origin_uri = tremor_pipeline::EventOriginUri {
            uid,
            scheme: "tremor-udp".to_string(),
            host: String::default(),
            port: None,
            path: vec![config.port.to_string()], // captures receive port
        };
        Self {
            config: config.clone(),
            socket: None,
            onramp_id,
            origin_uri,
        }
    }
}
impl onramp::Impl for Udp {
    fn from_config(onramp_id: &TremorURL, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self {
                config,
                onramp_id: onramp_id.clone(),
            }))
        } else {
            Err("Missing config for blaster onramp".into())
        }
    }
}

#[async_trait::async_trait]
impl Source for Int {
    #[allow(unused_variables)]
    async fn pull_event(&mut self, id: u64) -> Result<SourceReply> {
        let mut buf = [0; 65535];

        if let Some(socket) = self.socket.as_mut() {
            match socket.recv_from(&mut buf).await {
                Ok((n, peer)) => {
                    let mut origin_uri = self.origin_uri.clone();

                    // TODO add a method in origin_uri for changes like this?
                    origin_uri.host = peer.ip().to_string();
                    origin_uri.port = Some(peer.port());
                    Ok(SourceReply::Data {
                        origin_uri,
                        data: buf[0..n].to_vec(),
                        stream: 0,
                    })
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        Ok(SourceReply::Empty(1))
                    } else {
                        Err(e.into())
                    }
                }
            }
        } else {
            let socket = UdpSocket::bind((self.config.host.as_str(), self.config.port)).await?;
            info!(
                "[UDP Onramp] listening on {}:{}",
                self.config.host, self.config.port
            );
            self.socket = Some(socket);
            Ok(SourceReply::StateChange(SourceState::Connected))
        }
    }
    async fn init(&mut self) -> Result<SourceState> {
        let socket = UdpSocket::bind((self.config.host.as_str(), self.config.port)).await?;
        info!(
            "[UDP Onramp] listening on {}:{}",
            self.config.host, self.config.port
        );
        self.socket = Some(socket);
        Ok(SourceState::Connected)
    }
    fn id(&self) -> &TremorURL {
        &self.onramp_id
    }
}

#[async_trait::async_trait]
impl Onramp for Udp {
    async fn start(
        &mut self,
        onramp_uid: u64,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let source = Int::from_config(onramp_uid, self.onramp_id.clone(), &self.config);
        SourceManager::start(onramp_uid, source, codec, preprocessors, metrics_reporter).await
    }
    fn default_codec(&self) -> &str {
        "string"
    }
}
