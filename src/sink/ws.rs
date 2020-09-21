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

use crate::sink::prelude::*;
use async_channel::{bounded, unbounded, Receiver, Sender};
use async_tungstenite::async_std::connect_async;
use futures::SinkExt;
use halfbrown::HashMap;
use std::time::Duration;
use tungstenite::protocol::Message;
use url::Url;

type WsSender = Sender<(Ids, WsMessage)>;
type WsReceiver = Receiver<(Ids, WsMessage)>;
type WsUrl = String;

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
    config: Config,
    postprocessors: Postprocessors,
    tx: Sender<WsResult>,
    rx: Receiver<WsResult>,
    connections: HashMap<WsUrl, Option<WsSender>>,
}

enum WsResult {
    Connected(WsUrl, WsSender),
    Disconnected(WsUrl),
    Ack(Ids),
    Fail(Ids),
}

async fn ws_loop(
    url: String,
    offramp_tx: Sender<WsResult>,
    tx: WsSender,
    rx: WsReceiver,
) -> Result<()> {
    loop {
        let mut ws_stream = if let Ok((ws_stream, _)) = connect_async(&url).await {
            ws_stream
        } else {
            error!("Failed to connect to {}, retrying in 1s", url);
            offramp_tx.send(WsResult::Disconnected(url.clone())).await?;
            task::sleep(Duration::from_secs(1)).await;
            continue;
        };
        offramp_tx
            .send(WsResult::Connected(url.clone(), tx.clone()))
            .await?;

        while let Ok((id, msg)) = rx.recv().await {
            // test code to simulate slow connection
            // TODO remove
            //if &url == "ws://localhost:8139" {
            //    dbg!("sleeping...");
            //    std::thread::sleep(Duration::from_secs(5));
            //}

            let r = match msg {
                WsMessage::Text(t) => ws_stream.send(Message::Text(t)).await,
                WsMessage::Binary(t) => ws_stream.send(Message::Binary(t)).await,
            };
            if let Err(e) = r {
                error!(
                    "Websocket send error: {} for endppoint {}, reconnecting",
                    e, url
                );
                offramp_tx.send(WsResult::Fail(id)).await?;
                break;
            } else {
                offramp_tx.send(WsResult::Ack(id)).await?;
            }
        }
    }
}

impl offramp::Impl for Ws {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            // ensure we have valid url
            Url::parse(&config.url)?;

            let (tx, rx) = unbounded();

            // handle connection for the offramp config url (as default)
            let mut connections = HashMap::new();
            let (conn_tx, conn_rx) = bounded(crate::QSIZE);
            connections.insert(config.url.clone(), None);
            task::spawn(ws_loop(config.url.clone(), tx.clone(), conn_tx, conn_rx));

            Ok(SinkManager::new_box(Self {
                config,
                postprocessors: vec![],
                tx,
                rx,
                connections,
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
                WsResult::Connected(url, tx) => {
                    // TODO trigger per url/connection (only resuming events with that url)
                    if url == self.config.url {
                        v.push(SinkReply::Insight(Event::cb_restore(ingest_ns)));
                    }
                    self.connections.insert(url, Some(tx));
                }
                WsResult::Disconnected(url) => {
                    // TODO trigger per url/connection (only events with that url should be paused)
                    if url == self.config.url {
                        v.push(SinkReply::Insight(Event::cb_trigger(ingest_ns)));
                    }
                    self.connections.insert(url, None);
                }
                WsResult::Ack(id) => {
                    v.push(SinkReply::Insight(Event::cb_ack(ingest_ns, id.clone())))
                }
                WsResult::Fail(id) => {
                    v.push(SinkReply::Insight(Event::cb_fail(ingest_ns, id.clone())))
                }
            }
        }
        Ok(Some(v))
    }

    fn get_message_config(&self, meta: &Value) -> Config {
        Config {
            url: meta
                .get("url")
                // TODO simplify
                .and_then(Value::as_str)
                .unwrap_or(&self.config.url)
                .into(),
            binary: meta
                .get("binary")
                .and_then(Value::as_bool)
                .unwrap_or(self.config.binary),
        }
    }
}

#[async_trait::async_trait]
impl Sink for Ws {
    fn auto_ack(&self) -> bool {
        false
    }

    fn is_active(&self) -> bool {
        // TODO track per url/connection (instead of just using default config url)
        self.connections
            .get(&self.config.url)
            .map_or(false, |c| c.is_some())
    }

    async fn on_signal(&mut self, signal: Event) -> ResultVec {
        self.drain_insights(signal.ingest_ns).await
    }

    #[allow(clippy::used_underscore_binding)]
    async fn on_event(&mut self, _input: &str, codec: &dyn Codec, event: Event) -> ResultVec {
        for (value, meta) in event.value_meta_iter() {
            let msg_config = self.get_message_config(meta);

            // actually used when we have new connection to make (overriden from event-meta)
            #[allow(unused_assignments)]
            let mut temp_conn_tx = None;
            let ws_conn_tx = match self.connections.get(&msg_config.url) {
                Some(v) => v,
                None => {
                    let (conn_tx, conn_rx) = bounded(crate::QSIZE);
                    // separate task to handle new url connection
                    task::spawn(ws_loop(
                        msg_config.url.clone(),
                        self.tx.clone(),
                        conn_tx.clone(),
                        conn_rx,
                    ));
                    // TODO default to None for initial connection? (like what happens for
                    // default offramp config url). if we do circuit-breakers-per-url
                    // connection, this will be handled better.
                    self.connections
                        .insert(msg_config.url.clone(), Some(conn_tx.clone()));
                    // sender here ensures we don't drop the current in-flight event
                    temp_conn_tx = Some(conn_tx);
                    &temp_conn_tx
                }
            };

            if let Some(conn_tx) = ws_conn_tx {
                let raw = codec.encode(value)?;
                let datas = postprocess(&mut self.postprocessors, event.ingest_ns, raw)?;
                for raw in datas {
                    if msg_config.binary {
                        conn_tx
                            .send((event.id.clone(), WsMessage::Binary(raw)))
                            .await?;
                    } else if let Ok(txt) = String::from_utf8(raw) {
                        conn_tx
                            .send((event.id.clone(), WsMessage::Text(txt)))
                            .await?;
                    } else {
                        error!("[WS Offramp] Invalid utf8 data for text message");
                        return Err(Error::from("Invalid utf8 data for text message"));
                    }
                }
            }
        }
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
