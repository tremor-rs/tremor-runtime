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

use crate::source::prelude::*;
use async_channel::{Sender, TryRecvError};
use async_std::net::{TcpListener, TcpStream};
use async_std::task;
use futures::StreamExt;
use tungstenite::protocol::Message;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// The port to listen on.
    pub port: u16,
    /// Host to listen on
    pub host: String,
}

pub struct Ws {
    pub config: Config,
    onramp_id: TremorURL,
}

impl onramp::Impl for Ws {
    fn from_config(id: &TremorURL, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            Ok(Box::new(Self {
                config,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Missing config for blaster onramp".into())
        }
    }
}

pub struct Int {
    uid: u64,
    config: Config,
    listener: Option<Receiver<SourceReply>>,
    onramp_id: TremorURL,
}

impl std::fmt::Debug for Int {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WS")
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

async fn handle_connection(
    uid: u64,
    tx: Sender<SourceReply>,
    raw_stream: TcpStream,
    stream: usize,
) -> Result<()> {
    let mut ws_stream = async_tungstenite::accept_async(raw_stream).await?;

    let uri = tremor_pipeline::EventOriginUri {
        uid,
        scheme: "tremor-ws".to_string(),
        host: "tremor-ws-client-host.remote".to_string(),
        port: None,
        // TODO add server port here (like for tcp onramp) -- can be done via WsServerState
        path: vec![String::default()],
    };

    tx.send(SourceReply::StartStream(stream)).await?;

    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(Message::Text(t)) => {
                tx.send(SourceReply::Data {
                    origin_uri: uri.clone(),
                    data: t.into_bytes(),
                    stream,
                })
                .await?;
            }
            Ok(Message::Binary(data)) => {
                tx.send(SourceReply::Data {
                    origin_uri: uri.clone(),
                    data,
                    stream,
                })
                .await?;
            }
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => (),
            Ok(Message::Close(_)) => {
                tx.send(SourceReply::EndStream(stream)).await?;
                break;
            }
            Err(e) => error!("WS error returned while waiting for client data: {}", e),
        }
    }
    Ok(())
}

#[async_trait::async_trait()]
impl Source for Int {
    #[allow(unused_variables)]
    async fn pull_event(&mut self, id: u64) -> Result<SourceReply> {
        if let Some(listener) = self.listener.as_ref() {
            match listener.try_recv() {
                Ok(r) => Ok(r),
                Err(TryRecvError::Empty) => Ok(SourceReply::Empty(10)),
                Err(TryRecvError::Closed) => {
                    Ok(SourceReply::StateChange(SourceState::Disconnected))
                }
            }
        } else {
            Ok(SourceReply::StateChange(SourceState::Disconnected))
        }
    }
    async fn init(&mut self) -> Result<SourceState> {
        let listener = TcpListener::bind((self.config.host.as_str(), self.config.port)).await?;
        let (tx, rx) = bounded(crate::QSIZE);
        let uid = self.uid;

        task::spawn(async move {
            let mut stream_id = 0;
            while let Ok((stream, _socket)) = listener.accept().await {
                stream_id += 1;
                task::spawn(handle_connection(uid, tx.clone(), stream, stream_id));
            }
        });

        self.listener = Some(rx);

        Ok(SourceState::Connected)
    }
    fn id(&self) -> &TremorURL {
        &self.onramp_id
    }
}

#[async_trait::async_trait]
impl Onramp for Ws {
    async fn start(
        &mut self,
        onramp_uid: u64,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let source = Int::from_config(onramp_uid, self.onramp_id.clone(), &self.config)?;
        SourceManager::start(onramp_uid, source, codec, preprocessors, metrics_reporter).await
    }

    fn default_codec(&self) -> &str {
        "string"
    }
}
