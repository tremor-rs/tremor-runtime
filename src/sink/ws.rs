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

use crate::sink::prelude::*;
use async_channel::{bounded, unbounded, Receiver, Sender};
use async_tungstenite::async_std::connect_async;
use futures::SinkExt;
use std::time::Duration;
use tungstenite::protocol::Message;
use url::Url;

type WsAddr = Sender<(Ids, WsMessage)>;

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
    postprocessors: Postprocessors,
    rx: Receiver<WSResult>,
}

enum WSResult {
    Connected(WsAddr),
    Disconnected,
    Ack(Ids),
    Fail(Ids),
}

impl Default for WSResult {
    fn default() -> Self {
        WSResult::Disconnected
    }
}
async fn ws_loop(url: String, offramp_tx: Sender<WSResult>) -> Result<()> {
    loop {
        let mut ws_stream = if let Ok((ws_stream, _)) = connect_async(&url).await {
            ws_stream
        } else {
            error!("Failed to connect to {}, retrying in 1s", url);
            offramp_tx.send(WSResult::Disconnected).await?;
            task::sleep(Duration::from_secs(1)).await;
            continue;
        };
        let (tx, rx) = bounded(crate::QSIZE);
        offramp_tx.send(WSResult::Connected(tx)).await?;

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
                offramp_tx.send(WSResult::Fail(id)).await?;
                break;
            } else {
                offramp_tx.send(WSResult::Ack(id)).await?;
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
            let (tx, rx) = unbounded();

            task::spawn(ws_loop(config.url.clone(), tx));

            Ok(SinkManager::new_box(Self {
                addr: None,
                config,
                postprocessors: vec![],
                rx,
            }))
        } else {
            Err("[WS Offramp] Offramp requires a config".into())
        }
    }
}

impl Ws {
    async fn drain_insights(&mut self, ingest_ns: u64) -> ResultVec {
        let len = self.rx.len();
        let mut v = Vec::with_capacity(len);
        for _ in 0..len {
            match self.rx.recv().await? {
                WSResult::Connected(addr) => {
                    self.addr = Some(addr);
                    v.push(Event::cb_restore(ingest_ns));
                }
                WSResult::Disconnected => {
                    self.addr = None;
                    v.push(Event::cb_trigger(ingest_ns));
                }
                WSResult::Ack(id) => v.push(Event::cb_ack(ingest_ns, id.clone())),
                WSResult::Fail(id) => v.push(Event::cb_fail(ingest_ns, id.clone())),
            }
        }
        Ok(Some(v))
    }
}

#[async_trait::async_trait]
impl Sink for Ws {
    fn auto_ack(&self) -> bool {
        false
    }

    fn is_active(&self) -> bool {
        self.addr.is_some()
    }

    async fn on_signal(&mut self, signal: Event) -> ResultVec {
        self.drain_insights(signal.ingest_ns).await
    }

    #[allow(clippy::used_underscore_binding)]
    async fn on_event(&mut self, _input: &str, codec: &dyn Codec, event: Event) -> ResultVec {
        if let Some(addr) = &self.addr {
            for value in event.value_iter() {
                let raw = codec.encode(value)?;
                let datas = postprocess(&mut self.postprocessors, event.ingest_ns, raw)?;
                for raw in datas {
                    if self.config.binary {
                        addr.send((event.id.clone(), WsMessage::Binary(raw)))
                            .await?;
                    } else if let Ok(txt) = String::from_utf8(raw) {
                        addr.send((event.id.clone(), WsMessage::Text(txt))).await?;
                    } else {
                        error!("[WS Offramp] Invalid utf8 data for text message");
                        return Err(Error::from("Invalid utf8 data for text message"));
                    }
                }
            }
        };
        self.drain_insights(event.ingest_ns).await
    }

    fn default_codec(&self) -> &str {
        "json"
    }
    async fn init(&mut self, postprocessors: &[String]) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }
}
