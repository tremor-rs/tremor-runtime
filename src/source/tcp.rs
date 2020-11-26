// Copyright 2020, The Tremor Team
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

use crate::source::prelude::*;
use async_channel::TryRecvError;
use async_std::net::TcpListener;

// TODO expose this as config (would have to change buffer to be vector?)
const BUFFER_SIZE_BYTES: usize = 8192;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    pub port: u16,
    pub host: String,
}

impl ConfigImpl for Config {}

pub struct Tcp {
    pub config: Config,
    onramp_id: TremorURL,
}

pub struct Int {
    uid: u64,
    config: Config,
    listener: Option<Receiver<SourceReply>>,
    onramp_id: TremorURL,
}
impl std::fmt::Debug for Int {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TCP")
    }
}
impl Int {
    fn from_config(uid: u64, onramp_id: TremorURL, config: &Config) -> Result<Self> {
        let config = config.clone();

        Ok(Self {
            uid,
            config,
            listener: None,
            onramp_id,
        })
    }
}

impl onramp::Impl for Tcp {
    fn from_config(id: &TremorURL, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self {
                config,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Missing config for tcp onramp".into())
        }
    }
}

#[async_trait::async_trait()]
impl Source for Int {
    fn id(&self) -> &TremorURL {
        &self.onramp_id
    }

    async fn pull_event(&mut self, _id: u64) -> Result<SourceReply> {
        self.listener.as_ref().map_or_else(
            || Ok(SourceReply::StateChange(SourceState::Disconnected)),
            |listener| match listener.try_recv() {
                Ok(r) => Ok(r),
                Err(TryRecvError::Empty) => Ok(SourceReply::Empty(10)),
                Err(TryRecvError::Closed) => {
                    Ok(SourceReply::StateChange(SourceState::Disconnected))
                }
            },
        )
    }

    async fn init(&mut self) -> Result<SourceState> {
        let listener = TcpListener::bind((self.config.host.as_str(), self.config.port)).await?;
        let (tx, rx) = bounded(crate::QSIZE);
        let uid = self.uid;
        let path = vec![self.config.port.to_string()];
        task::spawn(async move {
            let mut stream_id = 0;
            while let Ok((mut stream, peer)) = listener.accept().await {
                let tx = tx.clone();
                stream_id += 1;
                let origin_uri = EventOriginUri {
                    uid,
                    scheme: "tremor-tcp".to_string(),
                    host: peer.ip().to_string(),
                    port: Some(peer.port()),
                    // TODO also add token_num here?
                    path: path.clone(), // captures server port
                };
                task::spawn(async move {
                    //let (reader, writer) = &mut (&stream, &stream);
                    let mut buffer = [0; BUFFER_SIZE_BYTES];
                    if let Err(e) = tx.send(SourceReply::StartStream(stream_id)).await {
                        error!("TCP Error: {}", e);
                        return;
                    }

                    while let Ok(n) = stream.read(&mut buffer).await {
                        if n == 0 {
                            if let Err(e) = tx.send(SourceReply::EndStream(stream_id)).await {
                                error!("TCP Error: {}", e);
                            };
                            break;
                        };
                        if let Err(e) = tx
                            .send(SourceReply::Data {
                                origin_uri: origin_uri.clone(),
                                // ALLOW: we define n as part of the read
                                data: buffer[0..n].to_vec(),
                                meta: None, // TODO: add peer address etc. to meta
                                codec_override: None,
                                stream: stream_id,
                            })
                            .await
                        {
                            error!("TCP Error: {}", e);
                            break;
                        };
                    }
                });
            }
        });
        self.listener = Some(rx);

        Ok(SourceState::Connected)
    }
}

#[async_trait::async_trait]
impl Onramp for Tcp {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        let source = Int::from_config(config.onramp_uid, self.onramp_id.clone(), &self.config)?;
        SourceManager::start(source, config).await
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
