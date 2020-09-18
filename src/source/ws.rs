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
use futures::{SinkExt, StreamExt};
use std::collections::BTreeMap;
use tungstenite::protocol::Message;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// The port to listen on.
    pub port: u16,
    /// Host to listen on
    pub host: String,
    // TODO move to root onramp config. or auto-infer based on linking
    pub link: Option<bool>,
}

impl ConfigImpl for Config {}

pub struct Ws {
    pub config: Config,
    onramp_id: TremorURL,
}

impl onramp::Impl for Ws {
    fn from_config(id: &TremorURL, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
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
    // mapping of event id to stream id
    messages: BTreeMap<u64, usize>,
    // mapping of stream id to the stream sender
    // TODO alternative to this? possible to store actual ws_tream refs here?
    streams: BTreeMap<usize, Sender<Event>>,
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
            messages: BTreeMap::new(),
            streams: BTreeMap::new(),
        })
    }

    fn get_stream_sender_for_id(&mut self, id: u64) -> Option<&Sender<Event>> {
        // TODO improve the way stream_id is retrieved -- this works as long as a
        // previous event id is used by pipelines/offramps (which is suitable only
        // for request/response style flow), but for websocket, arbitrary events can
        // come in here.
        // also messages keeps growing as events come in right now
        // .unwrap()
        if let Some(stream_id) = self.messages.get(&id) {
            self.streams.get(&stream_id)
        } else {
            None
        }
    }
}

async fn handle_connection(
    uid: u64,
    tx: Sender<SourceReply>,
    raw_stream: TcpStream,
    stream: usize,
    link: bool,
) -> Result<()> {
    let ws_stream = async_tungstenite::accept_async(raw_stream).await?;

    let uri = EventOriginUri {
        uid,
        scheme: "tremor-ws".to_string(),
        // TODO set ws client ip here
        host: "tremor-ws-client-host.remote".to_string(),
        port: None,
        // TODO add server port here (like for tcp onramp) -- can be done via WsServerState
        path: vec![String::default()],
    };

    let (mut ws_write, mut ws_read) = ws_stream.split();

    // TODO maybe send ws_write from tx and get rid of this task + extra channel?
    let stream_sender = if link {
        let (stream_tx, stream_rx): (Sender<Event>, Receiver<Event>) = bounded(crate::QSIZE);
        task::spawn::<_, Result<()>>(async move {
            while let Ok(event) = stream_rx.recv().await {
                let message = make_message(event).await?;
                ws_write.send(message).await?;
            }
            Ok(())
        });
        Some(stream_tx)
    } else {
        None
    };

    tx.send(SourceReply::StartStream(stream, stream_sender))
        .await?;

    while let Some(msg) = ws_read.next().await {
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

async fn make_message(event: Event) -> Result<Message> {
    // TODO reject batched events and handle only single event here
    let (value, _meta) = event.value_meta_iter().next().unwrap();

    // TODO make this decision based on meta value?
    let send_as_text = true;

    if send_as_text {
        let data = value
            .as_str()
            .ok_or_else(|| Error::from("Could not get event value as string"))?;

        Ok(Message::Text(data.into()))
    } else {
        // TODO consolidate this duplicate logic from string codec
        let data = if let Some(s) = value.as_str() {
            s.as_bytes().to_vec()
        } else {
            simd_json::to_vec(&value)?
        };
        Ok(Message::Binary(data))
    }
}

#[async_trait::async_trait()]
impl Source for Int {
    #[allow(unused_variables)]
    async fn pull_event(&mut self, id: u64) -> Result<SourceReply> {
        if let Some(listener) = self.listener.as_ref() {
            match listener.try_recv() {
                Ok(r) => {
                    match r {
                        SourceReply::Data { stream, .. } => {
                            self.messages.insert(id, stream);
                        }
                        SourceReply::StartStream(stream, ref sender) => {
                            if let Some(tx) = sender {
                                self.streams.insert(stream, tx.clone());
                            }
                        }
                        _ => (),
                    }
                    Ok(r)
                }
                Err(TryRecvError::Empty) => Ok(SourceReply::Empty(10)),
                Err(TryRecvError::Closed) => {
                    Ok(SourceReply::StateChange(SourceState::Disconnected))
                }
            }
        } else {
            Ok(SourceReply::StateChange(SourceState::Disconnected))
        }
    }
    async fn reply_event(&mut self, event: Event) -> Result<()> {
        // TODO report errors as well
        if let Some(eid) = event.id.get(self.uid) {
            if let Some(tx) = self.get_stream_sender_for_id(eid) {
                tx.send(event).await?;
            }
        }
        Ok(())
    }
    async fn init(&mut self) -> Result<SourceState> {
        let listener = TcpListener::bind((self.config.host.as_str(), self.config.port)).await?;
        let (tx, rx) = bounded(crate::QSIZE);
        let uid = self.uid;

        let link = self.config.link.unwrap_or(false);

        task::spawn(async move {
            let mut stream_id = 0;
            while let Ok((stream, _socket)) = listener.accept().await {
                stream_id += 1;
                task::spawn(handle_connection(uid, tx.clone(), stream, stream_id, link));
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
