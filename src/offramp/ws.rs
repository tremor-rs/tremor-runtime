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

use crate::offramp::prelude::*;
use async_std::sync::{channel, Receiver, Sender};
use async_tungstenite::async_std::connect_async;
use futures::SinkExt;
use halfbrown::HashMap;
use std::time::Duration;
use tungstenite::protocol::Message;
use url::Url;

type WsAddr = Sender<(u64, WsMessage)>;

enum WsMessage {
    Binary(Vec<u8>),
    Text(String),
}

#[derive(Deserialize, Debug)]
pub struct Config {
    /// Host to use as source
    pub url: String,
    #[serde(default)]
    pub binary: bool,
}

/// An offramp that writes to a websocket endpoint
pub struct Ws {
    addr: Option<WsAddr>,
    config: Config,
    pipelines: HashMap<TremorURL, pipeline::Addr>,
    postprocessors: Postprocessors,
    rx: Receiver<WSResult>,
}

enum WSResult {
    Connected(WsAddr),
    Disconnected,
    Ack(u64),
    Fail(u64),
}

impl Default for WSResult {
    fn default() -> Self {
        WSResult::Disconnected
    }
}
async fn ws_loop(url: String, offramp_tx: Sender<WSResult>) {
    loop {
        let mut ws_stream = if let Ok((ws_stream, _)) = connect_async(&url).await {
            ws_stream
        } else {
            error!("Failed to connect to {}, retrying in 1s", url);
            offramp_tx.send(WSResult::Disconnected).await;
            task::sleep(Duration::from_secs(1)).await;
            continue;
        };
        let (tx, rx) = channel(64);
        offramp_tx.send(WSResult::Connected(tx)).await;

        while let Ok((id, msg)) = rx.recv().await {
            let r = match msg {
                WsMessage::Text(t) => ws_stream.send(Message::Text(t)).await,
                WsMessage::Binary(t) => ws_stream.send(Message::Binary(t)).await,
            };
            if let Err(e) = r {
                error!(
                    "Websocket send error: {} for endppoint {}, reconnecting",
                    e, url
                );
                offramp_tx.send(WSResult::Fail(id)).await;
                break;
            } else {
                offramp_tx.send(WSResult::Ack(id)).await;
            }
        }
    }
}

impl offramp::Impl for Ws {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            // Ensure we have valid url
            Url::parse(&config.url)?;
            let (tx, rx) = channel(1);

            task::spawn(ws_loop(config.url.clone(), tx));

            Ok(Box::new(Self {
                addr: None,
                config,
                pipelines: HashMap::new(),
                postprocessors: vec![],
                rx,
            }))
        } else {
            Err("[WS Offramp] Offramp requires a config".into())
        }
    }
}

impl Ws {
    async fn update_ws_state(&mut self, ingest_ns: u64) -> bool {
        let mut new_connect = false;
        while !self.rx.is_empty() {
            match self.rx.recv().await.unwrap_or_default() {
                WSResult::Connected(addr) => self.addr = Some(addr),
                WSResult::Disconnected => self.addr = None,
                WSResult::Ack(id) => {
                    for p in self.pipelines.values() {
                        if let Err(e) = p.send_insight(Event::cb_ack(ingest_ns, id)) {
                            error!("[WS Offramp] failed to return CB data: {}", e);
                        }
                    }
                }
                WSResult::Fail(id) => {
                    for p in self.pipelines.values() {
                        if let Err(e) = p.send_insight(Event::cb_fail(ingest_ns, id)) {
                            error!("[WS Offramp] failed to return CB data: {}", e);
                        }
                    }
                }
            }
            new_connect = self.addr.is_some();
        }
        new_connect
    }
}
impl Offramp for Ws {
    fn auto_ack(&self) -> bool {
        false
    }

    fn is_active(&self) -> bool {
        self.addr.is_some()
    }

    fn on_signal(&mut self, event: Event) -> Option<Event> {
        task::block_on(async {
            let was_connected = self.addr.is_some();
            let new_connect = self.update_ws_state(event.ingest_ns).await;

            if was_connected && !new_connect {
                for p in self.pipelines.values() {
                    if let Err(e) = p.send_insight(Event::cb_trigger(event.ingest_ns)) {
                        error!("[WS Offramp] failed to return CB data: {}", e);
                    }
                }
            } else if !was_connected && new_connect {
                dbg!(was_connected, new_connect);
                for p in self.pipelines.values() {
                    if let Err(e) = p.send_insight(Event::cb_restore(event.ingest_ns)) {
                        error!("[WS Offramp] failed to return CB data: {}", e);
                    }
                }
            }
        });
        None
    }

    fn on_event(&mut self, codec: &Box<dyn Codec>, _input: String, event: Event) -> Result<()> {
        task::block_on(async {
            let was_connected = self.addr.is_some();
            let new_connect = self.update_ws_state(event.ingest_ns).await;

            if let Some(addr) = &self.addr {
                if new_connect && !was_connected {
                    dbg!(was_connected, new_connect);
                    for p in self.pipelines.values() {
                        if let Err(e) = p.send_insight(Event::cb_restore(event.ingest_ns)) {
                            error!("[WS Offramp] failed to return CB data: {}", e);
                        }
                    }
                }
                for value in event.value_iter() {
                    let raw = codec.encode(value)?;
                    let datas = postprocess(&mut self.postprocessors, event.ingest_ns, raw)?;
                    for raw in datas {
                        if self.config.binary {
                            addr.send((event.id, WsMessage::Binary(raw))).await;
                        } else if let Ok(txt) = String::from_utf8(raw) {
                            addr.send((event.id, WsMessage::Text(txt))).await;
                        } else {
                            error!("[WS Offramp] Invalid utf8 data for text message");
                            return Err(Error::from("Invalid utf8 data for text message"));
                        }
                    }
                }
            } else {
                for p in self.pipelines.values() {
                    if let Err(e) = p.send_insight(Event::cb_trigger(event.ingest_ns)) {
                        error!("[WS Offramp] failed to return CB data: {}", e);
                    }
                    for p in self.pipelines.values() {
                        if let Err(e) = p.send_insight(Event::cb_fail(event.ingest_ns, event.id)) {
                            error!("[WS Offramp] failed to return CB data: {}", e);
                        }
                    }
                }
                return Err(Error::from("not connected"));
            };
            Ok(())
        })
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
    fn start(&mut self, _codec: &Box<dyn Codec>, postprocessors: &[String]) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }
}
