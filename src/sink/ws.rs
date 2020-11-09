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

use crate::sink::prelude::*;
use crate::source::prelude::*;
use async_channel::{bounded, unbounded, Receiver, Sender};
use async_tungstenite::async_std::connect_async;
use futures::SinkExt;
use halfbrown::HashMap;
use std::boxed::Box;
use std::time::Duration;
use tremor_pipeline::OpMeta;
use tungstenite::protocol::Message;
use url::Url;

type WsAddr = Sender<(Ids, OpMeta, WsMessage)>;
type WsReceiver = Receiver<(Ids, OpMeta, WsMessage)>;
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

enum WsResult {
    Connected(WsUrl, WsAddr),
    Disconnected(WsUrl),
    Ack(Ids, OpMeta),
    Fail(Ids, OpMeta),
    Response(Ids, Box<Result<WsMessage>>),
}

/// An offramp that writes to a websocket endpoint
pub struct Ws {
    sink_url: TremorURL,
    config: Config,
    postprocessors: Postprocessors,
    preprocessors: Preprocessors,
    codec: Box<dyn Codec>,
    tx: Sender<WsResult>,
    rx: Receiver<WsResult>,
    connections: HashMap<WsUrl, Option<WsAddr>>,
    is_linked: bool,
    merged_meta: OpMeta,
    reply_tx: Sender<sink::Reply>,
}
async fn ws_loop(
    sink_url: TremorURL,
    url: String,
    offramp_tx: Sender<WsResult>,
    tx: WsAddr,
    rx: WsReceiver,
    has_link: bool,
) -> Result<()> {
    loop {
        let mut ws_stream = if let Ok((ws_stream, _)) = connect_async(&url).await {
            ws_stream
        } else {
            error!(
                "[Sink::{}] Failed to connect to {}, retrying in 1s",
                &sink_url, url
            );
            offramp_tx.send(WsResult::Disconnected(url.clone())).await?;
            task::sleep(Duration::from_secs(1)).await;
            continue;
        };
        offramp_tx
            .send(WsResult::Connected(url.clone(), tx.clone()))
            .await?;

        while let Ok((id, meta, msg)) = rx.recv().await {
            let r = match msg {
                WsMessage::Text(t) => ws_stream.send(Message::Text(t)).await,
                WsMessage::Binary(t) => ws_stream.send(Message::Binary(t)).await,
            };
            if let Err(e) = r {
                error!(
                    "[Sink::{}] Websocket error while sending event to server {}: {}. Reconnecting...",
                    &sink_url,
                    url, e
                );
                // TODO avoid these clones for non linked-transport usecase
                offramp_tx.send(WsResult::Fail(id.clone(), meta)).await?;
                if has_link {
                    offramp_tx
                        .send(WsResult::Response(
                            id,
                            Box::new(Err(Error::from(format!(
                                "[Sink::{}] Error sending event to server {}: {}",
                                &sink_url, url, e
                            )))),
                        ))
                        .await?;
                }
                break;
            } else {
                offramp_tx.send(WsResult::Ack(id.clone(), meta)).await?;
            }

            // TODO should we async this (async_sink?) so that a slow request on a
            // single connection does not block subsequent requests (for that connection)
            // (over separate connections, it is already async)
            if has_link {
                if let Some(msg) = ws_stream.next().await {
                    match msg {
                        Ok(Message::Text(t)) => {
                            offramp_tx
                                .send(WsResult::Response(id, Box::new(Ok(WsMessage::Text(t)))))
                                .await?;
                        }
                        Ok(Message::Binary(t)) => {
                            offramp_tx
                                .send(WsResult::Response(id, Box::new(Ok(WsMessage::Binary(t)))))
                                .await?;
                        }
                        Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
                        Ok(Message::Close(_)) => {
                            warn!(
                                "[Sink::{}] Server {} closed websocket connection",
                                &sink_url, url
                            );
                            offramp_tx.send(WsResult::Disconnected(url.clone())).await?;
                            offramp_tx
                                .send(WsResult::Response(
                                    id,
                                    Box::new(Err(Error::from(format!(
                                        "Error receiving reply from server {}: Server closed websocket connection",
                                        url
                                    )))),
                                ))
                                .await?;
                            break;
                        }
                        Err(e) => {
                            error!(
                                "[Sink::{}] Websocket error while receiving reply from server {}: {}",
                                &sink_url,
                                url, e
                            );
                            offramp_tx
                                .send(WsResult::Response(
                                    id,
                                    Box::new(Err(Error::from(format!(
                                        "Error receiving reply from server {}: {}",
                                        url, e
                                    )))),
                                ))
                                .await?;
                        }
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

            // This is a dummy so we can set it later
            let (reply_tx, _) = bounded(1);

            Ok(SinkManager::new_box(Self {
                sink_url: TremorURL::from_onramp_id("ws")?, // dummy value
                codec: Box::new(crate::codec::null::Null {}),
                preprocessors: vec![],
                postprocessors: vec![],
                is_linked: false,
                connections: HashMap::new(),
                config,
                tx,
                rx,
                merged_meta: OpMeta::default(),
                reply_tx,
            }))
        } else {
            Err("[WS Offramp] Offramp requires a config".into())
        }
    }
}

impl Ws {
    // TODO adopt similar reply mechanism as rest sink
    async fn drain_insights(&mut self, ingest_ns: u64) -> Result<()> {
        let len = self.rx.len();
        for _ in 0..len {
            match self.rx.recv().await? {
                WsResult::Connected(url, addr) => {
                    // TODO trigger per url/connection (only resuming events with that url)
                    if url == self.config.url {
                        let mut e = Event::cb_restore(ingest_ns);
                        e.op_meta = self.merged_meta.clone();
                        self.reply_tx.send(sink::Reply::Insight(e)).await?;
                    }
                    self.connections.insert(url, Some(addr));
                }
                WsResult::Disconnected(url) => {
                    // TODO trigger per url/connection (only events with that url should be paused)
                    if url == self.config.url {
                        let mut e = Event::cb_trigger(ingest_ns);
                        e.op_meta = self.merged_meta.clone();
                        self.reply_tx.send(sink::Reply::Insight(e)).await?;
                    }
                    self.connections.insert(url, None);
                }
                WsResult::Ack(id, op_meta) => {
                    let mut e = Event::cb_ack(ingest_ns, id.clone());
                    e.op_meta = op_meta;
                    self.reply_tx.send(sink::Reply::Insight(e)).await?;
                }
                WsResult::Fail(id, op_meta) => {
                    let mut e = Event::cb_fail(ingest_ns, id.clone());
                    e.op_meta = op_meta;
                    self.reply_tx.send(sink::Reply::Insight(e)).await?;
                }
                WsResult::Response(id, msg_result) => match *msg_result {
                    Ok(msg) => match self.build_response_events(&id, msg) {
                        Ok(events) => {
                            for event in events {
                                self.reply_tx
                                    .send(sink::Reply::Response(OUT, event))
                                    .await?;
                            }
                        }
                        Err(err) => {
                            error!("[Sink:{}] {}", self.sink_url, err);
                            let err_response = Self::create_error_response(&id, err.to_string());
                            self.reply_tx
                                .send(sink::Reply::Response(ERR, err_response))
                                .await?;
                        }
                    },
                    Err(err) => {
                        error!("[Sink:{}] {}", self.sink_url, err);
                        let err_response = Self::create_error_response(&id, err.to_string());
                        self.reply_tx
                            .send(sink::Reply::Response(ERR, err_response))
                            .await?;
                    }
                },
            }
        }
        Ok(())
    }

    fn build_response_events(&mut self, id: &Ids, msg: WsMessage) -> Result<Vec<Event>> {
        let mut meta = Value::object_with_capacity(1);
        let response_bytes = match msg {
            WsMessage::Text(d) => {
                meta.insert("binary", false)?;
                d.into_bytes()
            }
            WsMessage::Binary(d) => d,
        };

        let mut ingest_ns = nanotime();
        let preprocessed = preprocess(
            &mut self.preprocessors,
            &mut ingest_ns,
            response_bytes,
            &TremorURL::from_offramp_id("ws")?, // TODO: get proper url from offramp manager
        )?;
        let mut events = Vec::with_capacity(preprocessed.len());

        for pp in preprocessed {
            events.push(
                LineValue::try_new(vec![pp], |mutd| {
                    // ALLOW: mutd is vec![pp] in the line above
                    let mut_data = mutd[0].as_mut_slice();
                    let body = self
                        .codec
                        .decode(mut_data, nanotime())?
                        .unwrap_or_else(Value::object);

                    Ok(ValueAndMeta::from_parts(body, meta.clone())) // TODO: no need to clone the last element?
                })
                .map_err(|e: rental::RentalError<Error, _>| e.0)
                .map(|data| Event {
                    id: id.clone(),
                    origin_uri: None, // TODO
                    data,
                    ..Event::default()
                })?,
            );
        }
        Ok(events)
    }

    fn create_error_response(event_id: &Ids, e: String) -> Event {
        let mut error_data = simd_json::value::borrowed::Object::with_capacity(2);
        error_data.insert_nocheck("error".into(), Value::from(e.clone()));
        error_data.insert_nocheck("event_id".into(), Value::from(event_id.to_string()));

        let mut meta = simd_json::value::borrowed::Object::with_capacity(1);
        meta.insert_nocheck("error".into(), Value::from(e));

        Event {
            id: event_id.clone(),
            origin_uri: None, // TODO
            data: (error_data, meta).into(),
            ..Event::default()
        }
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
        self.drain_insights(signal.ingest_ns).await?;
        Ok(None)
    }

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

        self.merged_meta.merge(event.op_meta.clone());

        for (value, meta) in event.value_meta_iter() {
            let msg_meta = self.get_message_meta(meta);

            // actually used when we have new connection to make (overriden from event-meta)
            let temp_conn_tx;
            let ws_conn_tx = if let Some(ws_conn_tx) = self.connections.get(&msg_meta.url) {
                ws_conn_tx
            } else {
                let (conn_tx, conn_rx) = bounded(crate::QSIZE);
                // separate task to handle new url connection
                task::spawn(ws_loop(
                    self.sink_url.clone(),
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
                let raw = codec.encode(value)?; // TODO 001: handle errors with CBFail and response via ERR port, see rest sink l. 487..
                let datas = postprocess(&mut self.postprocessors, event.ingest_ns, raw)?; // TODO 001: same here
                for raw in datas {
                    if msg_meta.binary {
                        conn_tx
                            .send((
                                event.id.clone(),
                                self.merged_meta.clone(),
                                WsMessage::Binary(raw),
                            ))
                            .await?;
                    } else if let Ok(txt) = String::from_utf8(raw) {
                        conn_tx
                            .send((
                                event.id.clone(),
                                self.merged_meta.clone(),
                                WsMessage::Text(txt),
                            ))
                            .await?;
                    } else {
                        error!("[WS Offramp] Invalid utf8 data for text message");
                        let e = "Invalid utf8 data for text message";

                        self.reply_tx
                            .send(sink::Reply::Response(
                                ERR,
                                Self::create_error_response(&event.id, e.to_string()),
                            ))
                            .await?;
                        return Err(e.into());
                    }
                }
            } else {
                // connnection is in a disconnected state, but if this is a linked offramp,
                // got to send a response
                // TODO send this always and also log error here?
                if self.is_linked {
                    let err = "Error getting response for event: websocket sink is not connected";
                    self.reply_tx
                        .send(sink::Reply::Response(
                            ERR,
                            Self::create_error_response(&event.id, err.to_string()),
                        ))
                        .await?;
                }
            }
        }
        self.drain_insights(event.ingest_ns).await?;
        Ok(None)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        _sink_uid: u64,
        sink_url: &TremorURL,
        codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        is_linked: bool,
        reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        self.postprocessors = make_postprocessors(processors.post)?;
        self.preprocessors = make_preprocessors(processors.pre)?;
        self.is_linked = is_linked;
        // TODO use reply_channel here too (like for rest)

        // handle connection for the offramp config url (as default)
        let (conn_tx, conn_rx) = bounded(crate::QSIZE);
        self.connections.insert(self.config.url.clone(), None);
        self.codec = codec.boxed_clone();
        self.reply_tx = reply_channel;
        task::spawn(ws_loop(
            sink_url.clone(),
            self.config.url.clone(),
            self.tx.clone(),
            conn_tx,
            conn_rx,
            is_linked,
        ));

        Ok(())
    }
}
