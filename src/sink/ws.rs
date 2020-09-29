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

// TODO once we get rid of link option in offramp config,
// we can just align this with offramp Config
struct WsMessageMeta {
    url: String,
    binary: bool,
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
    is_linked: bool,
}

enum WsResult {
    Connected(WsUrl, WsSender),
    Disconnected(WsUrl),
    Ack(Ids),
    Fail(Ids),
    Response(Ids, WsMessage),
}

async fn ws_loop(
    url: String,
    offramp_tx: Sender<WsResult>,
    tx: WsSender,
    rx: WsReceiver,
    has_link: bool,
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
            //    debug!("sleeping...");
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
                // TODO avoid these clones for non linked-transport usecase
                offramp_tx.send(WsResult::Fail(id.clone())).await?;
                break;
            } else {
                offramp_tx.send(WsResult::Ack(id.clone())).await?;
            }

            // TODO should we async this (async_sink?) so that a slow request on a
            // single connection does not block subsequent requests (for that connection)
            // (over separate connections, it is already async)
            if has_link {
                if let Some(msg) = ws_stream.next().await {
                    match msg {
                        Ok(Message::Text(t)) => {
                            offramp_tx
                                .send(WsResult::Response(id, WsMessage::Text(t)))
                                .await?;
                        }
                        Ok(Message::Binary(t)) => {
                            offramp_tx
                                .send(WsResult::Response(id, WsMessage::Binary(t)))
                                .await?;
                        }
                        Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
                        Ok(Message::Close(_)) => {
                            warn!("Server {} closed websocket connection", url);
                            offramp_tx.send(WsResult::Disconnected(url.clone())).await?;
                            break;
                        }
                        Err(e) => error!("WS error returned while waiting for server data: {}", e),
                    }
                }
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

            Ok(SinkManager::new_box(Self {
                postprocessors: vec![],
                is_linked: false,
                connections: HashMap::new(),
                config,
                tx,
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
                WsResult::Response(id, msg) => {
                    v.push(SinkReply::Response(Self::make_event(id, msg)?))
                }
            }
        }
        Ok(Some(v))
    }

    fn make_event(id: Ids, msg: WsMessage) -> Result<Event> {
        let mut meta = Value::object_with_capacity(1);
        let data = match msg {
            WsMessage::Text(d) => {
                meta.insert("binary", false)?;
                d
            }
            WsMessage::Binary(d) => {
                meta.insert("binary", true)?;
                String::from_utf8(d)?
            }
        };
        Ok(Event {
            id, // TODO only eid should be preserved for this?
            //data,
            data: (data, meta).into(),
            ingest_ns: nanotime(),
            // TODO implement origin_uri
            origin_uri: None,
            ..Event::default()
        })
    }

    fn get_message_meta(&self, meta: &Value) -> WsMessageMeta {
        WsMessageMeta {
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
            .map_or(false, Option::is_some)
    }

    async fn on_signal(&mut self, signal: Event) -> ResultVec {
        self.drain_insights(signal.ingest_ns).await
    }

    #[allow(clippy::used_underscore_binding)]
    async fn on_event(
        &mut self,
        _input: &str,
        codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        if self.is_linked && event.is_batch {
            return Err("Batched events are not supported for linked websocket offramps".into());
        }

        for (value, meta) in event.value_meta_iter() {
            let msg_meta = self.get_message_meta(meta);

            // actually used when we have new connection to make (overriden from event-meta)
            #[allow(unused_assignments)]
            let mut temp_conn_tx = None;
            let ws_conn_tx = if let Some(ws_conn_tx) = self.connections.get(&msg_meta.url) {
                ws_conn_tx
            } else {
                let (conn_tx, conn_rx) = bounded(crate::QSIZE);
                // separate task to handle new url connection
                task::spawn(ws_loop(
                    msg_meta.url.clone(),
                    self.tx.clone(),
                    conn_tx.clone(),
                    conn_rx,
                    self.is_linked,
                ));
                // TODO default to None for initial connection? (like what happens for
                // default offramp config url). if we do circuit-breakers-per-url
                // connection, this will be handled better.
                self.connections
                    .insert(msg_meta.url.clone(), Some(conn_tx.clone()));
                // sender here ensures we don't drop the current in-flight event
                temp_conn_tx = Some(conn_tx);
                &temp_conn_tx
            };

            if let Some(conn_tx) = ws_conn_tx {
                let raw = codec.encode(value)?;
                let datas = postprocess(&mut self.postprocessors, event.ingest_ns, raw)?;
                for raw in datas {
                    if msg_meta.binary {
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
    #[allow(clippy::too_many_arguments, clippy::used_underscore_binding)]
    async fn init(
        &mut self,
        _sink_uid: u64,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        _preprocessors: &[String],
        postprocessors: &[String],
        is_linked: bool,
        _reply_channel: Sender<SinkReply>,
    ) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        self.is_linked = is_linked;
        // TODO use reply_channel here too (like for rest)

        // handle connection for the offramp config url (as default)
        let (conn_tx, conn_rx) = bounded(crate::QSIZE);
        self.connections.insert(self.config.url.clone(), None);
        task::spawn(ws_loop(
            self.config.url.clone(),
            self.tx.clone(),
            conn_tx,
            conn_rx,
            is_linked,
        ));

        Ok(())
    }
}
