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
use crate::source::prelude::*;
use async_std::channel::TryRecvError;
use async_std::os::unix::net::UnixListener;
use smol::stream::StreamExt;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    pub path: String,
    pub permissions: Option<String>,
}

impl ConfigImpl for Config {}

pub struct UnixSocket {
    pub config: Config,
    onramp_id: TremorUrl,
}

pub struct Int {
    uid: u64,
    config: Config,
    listener: Option<Receiver<SourceReply>>,
    onramp_id: TremorUrl,
}

impl std::fmt::Debug for Int {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UnixSocket:{}", self.config.path)
    }
}

impl Int {
    fn from_config(uid: u64, onramp_id: TremorUrl, config: &Config) -> Self {
        let config = config.clone();

        Self {
            uid,
            config,
            listener: None,
            onramp_id,
        }
    }
}

#[async_trait::async_trait()]
impl Onramp for UnixSocket {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        let source = Int::from_config(config.onramp_uid, self.onramp_id.clone(), &self.config);
        SourceManager::start(source, config).await
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}

pub(crate) struct Builder {}
impl onramp::Builder for Builder {
    fn from_config(&self, id: &TremorUrl, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(UnixSocket {
                config,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Missing config for unix_socket onramp".into())
        }
    }
}

#[async_trait::async_trait()]
impl Source for Int {
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
        let path = PathBuf::from(&self.config.path);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        let stream = UnixListener::bind(&path).await?;
        if let Some(ref mode_description) = self.config.permissions {
            let mut mode = file_mode::Mode::empty();
            mode.set_str_umask(mode_description, 0)?;
            mode.set_mode_path(&path)?;
        }
        let mut stream_id = 0;
        let (tx, rx) = bounded(crate::QSIZE);

        let path = vec![self.config.path.clone()];
        let uid = self.uid;
        task::spawn(async move {
            if let Err(e) = tx.send(SourceReply::StartStream(0)).await {
                error!("Unix Socket Error: {}", e);
                return;
            }

            let origin_uri = EventOriginUri {
                uid,
                scheme: "tremor-unix-socket".to_string(),
                host: String::new(),
                port: None,
                path: path.clone(), // captures server port
            };

            let mut buffer = [0; 8192];

            let mut incoming = stream.incoming();
            loop {
                let mut connection = match incoming.try_next().await {
                    Ok(Some(connection)) => connection,
                    Ok(None) => break,
                    Err(e) => {
                        error!("Unix socket error: {}", e);
                        return;
                    }
                };

                let tx = tx.clone();
                let origin_uri = origin_uri.clone();

                task::spawn(async move {
                    if let Err(e) = tx.send(SourceReply::StartStream(stream_id)).await {
                        error!("Unix Socket Error: {}", e);
                        return;
                    };

                    while let Ok(n) = connection.read(&mut buffer).await {
                        if n == 0 {
                            if let Err(e) = tx.send(SourceReply::EndStream(stream_id)).await {
                                error!("Unix Socket Error: {}", e);
                            };
                            break;
                        };
                        if let Err(e) = tx
                            .send(SourceReply::Data {
                                origin_uri: origin_uri.clone(),
                                // ALLOW: we define n as part of the read
                                data: buffer[0..n].to_vec(),
                                meta: None,
                                codec_override: None,
                                stream: stream_id,
                            })
                            .await
                        {
                            error!("Unix Socket Error: {}", e);
                            break;
                        };
                    }
                });
                stream_id = stream_id.wrapping_add(1);
            }
        });

        self.listener = Some(rx);

        Ok(SourceState::Connected)
    }

    fn id(&self) -> &TremorUrl {
        &self.onramp_id
    }
}

#[cfg(test)]
mod tests {
    use crate::onramp::Impl;
    use crate::source::unix_socket::{Config, Int, UnixSocket};
    use crate::url::TremorUrl;
    use simd_json::json;

    #[test]
    pub fn default_codec_is_json() {
        let onramp_config = json!({
            "path": "/tmp/test.sock"
        });
        let onramp = UnixSocket::from_config(
            &TremorUrl::from_onramp_id("test").unwrap(),
            &serde_yaml::from_value(serde_yaml::to_value(onramp_config).expect("")).expect(""),
        )
        .unwrap();

        assert_eq!("json", onramp.default_codec());
    }

    #[test]
    pub fn can_be_formatted_for_debug() {
        let int = Int::from_config(
            1,
            TremorUrl::from_onramp_id("test").unwrap(),
            &Config {
                path: "/tmp/test.sock".to_string(),
                permissions: None,
            },
        );

        assert_eq!("UnixSocket:/tmp/test.sock", format!("{:?}", int));
    }
}
