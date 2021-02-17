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

#![cfg(not(tarpaulin_include))]

use crate::sink::prelude::*;
use crate::source::prelude::*;
use async_channel::{bounded, unbounded, Receiver, Sender};
use async_tungstenite::async_std::connect_async;
use async_tungstenite::tungstenite::Message;
use futures::SinkExt;
use halfbrown::HashMap;
use std::boxed::Box;
use std::time::Duration;
use tremor_pipeline::{EventId, OpMeta};
use tremor_script::LineValue;
use url::Url;

type WsUrl = String;
type WsConnectionHandle = (
    Option<Sender<ConnectionTaskMsg>>,
    task::JoinHandle<Result<()>>,
);

// TODO once we get rid of link option in offramp config,
// we can just align this with offramp Config
struct WsMessageMeta {
    url: String,
    binary: bool,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// Host to use as source
    pub url: String,
    #[serde(default)]
    pub binary: bool,
}

enum WsConnectionMsg {
    Connected(WsUrl, Sender<ConnectionTaskMsg>),
    Disconnected(WsUrl),
}

enum ConnectionTaskMsg {
    SendEvent(EventId, WsMessageMeta, OpMeta, u64, LineValue),
    //ToEvent(Ids, u64, Message),
}

/// An offramp that writes to a websocket endpoint
pub struct Ws {
    sink_url: TremorURL,
    event_origin_uri: EventOriginUri,
    config: Config,
    preprocessors: Vec<String>,
    postprocessors: Vec<String>,
    shared_codec: Box<dyn Codec>,
    connection_lifecycle_tx: Sender<WsConnectionMsg>,
    connection_lifecycle_rx: Receiver<WsConnectionMsg>,
    connections: HashMap<WsUrl, WsConnectionHandle>,
    is_linked: bool,
    merged_meta: OpMeta,
    reply_tx: Sender<sink::Reply>,
}

#[inline]
async fn handle_error_with_fail(
    sink_url: &TremorURL,
    e: &str,
    reply_tx: &Sender<sink::Reply>,
    ids: &EventId,
    event_origin_uri: &EventOriginUri,
    op_meta: OpMeta,
) -> Result<()> {
    error!("[Sink::{}] {}", sink_url, e);
    // send fail
    let mut fail_event = Event::cb_fail(nanotime(), ids.clone());
    fail_event.op_meta = op_meta;
    reply_tx.send(sink::Reply::Insight(fail_event)).await?;

    // send standardized error response
    reply_tx
        .send(sink::Reply::Response(
            ERR,
            Ws::create_error_response(ids, e, event_origin_uri),
        ))
        .await?;
    Ok(())
}

#[inline]
async fn handle_error_no_fail(
    sink_url: &TremorURL,
    e: &str,
    reply_tx: &Sender<sink::Reply>,
    ids: &EventId,
    event_origin_uri: &EventOriginUri,
) -> Result<()> {
    error!("[Sink::{}] {}", sink_url, e);
    // send standardized error response
    reply_tx
        .send(sink::Reply::Response(
            ERR,
            Ws::create_error_response(ids, e, event_origin_uri),
        ))
        .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn ws_connection_loop(
    sink_url: TremorURL,
    url: String,
    mut event_origin_url: EventOriginUri,
    connection_lifecycle_tx: Sender<WsConnectionMsg>,
    reply_tx: Sender<sink::Reply>,
    tx: Sender<ConnectionTaskMsg>,
    rx: Receiver<ConnectionTaskMsg>,
    has_link: bool,
    mut preprocessors: Preprocessors,
    mut postprocessors: Postprocessors,
    mut codec: Box<dyn Codec>,
) -> Result<()> {
    loop {
        let codec: &mut dyn Codec = codec.as_mut();
        info!("[Sink::{}] Connecting to {} ...", &sink_url, url);
        let mut ws_stream = if let Ok((ws_stream, _)) = connect_async(&url).await {
            if let Ok(peer) = ws_stream.get_ref().peer_addr() {
                event_origin_url.port = Some(peer.port());
                event_origin_url.host = peer.ip().to_string();
            }
            if let Ok(local) = ws_stream.get_ref().local_addr() {
                event_origin_url.path = vec![local.port().to_string()];
            }
            ws_stream
        } else {
            error!(
                "[Sink::{}] Failed to connect to {}, retrying in 1s",
                &sink_url, url
            );
            connection_lifecycle_tx
                .send(WsConnectionMsg::Disconnected(url.clone()))
                .await?;
            task::sleep(Duration::from_secs(1)).await;
            continue;
        };
        connection_lifecycle_tx
            .send(WsConnectionMsg::Connected(url.clone(), tx.clone()))
            .await?;

        'recv_loop: while let Ok(ConnectionTaskMsg::SendEvent(
            ids,
            message_meta,
            op_meta,
            ingest_ns,
            data,
        )) = rx.recv().await
        {
            match event_to_message(
                codec,
                &mut postprocessors,
                ingest_ns,
                &data,
                message_meta.binary,
            ) {
                Ok(iter) => {
                    for msg_result in iter {
                        match msg_result {
                            Ok(msg) => {
                                match ws_stream.send(msg).await {
                                    Ok(_) => {
                                        let mut e = Event::cb_ack(nanotime(), ids.clone());
                                        e.op_meta = op_meta.clone();
                                        reply_tx.send(sink::Reply::Insight(e)).await?;
                                    }
                                    Err(e) => {
                                        let e = format!(
                                            "Error sending event to server {}: {}.",
                                            &url, e
                                        );
                                        handle_error_with_fail(
                                            &sink_url,
                                            &e,
                                            &reply_tx,
                                            &ids,
                                            &event_origin_url,
                                            op_meta.clone(),
                                        )
                                        .await?;

                                        // close connection explicitly
                                        ws_stream.close(None).await?;
                                        connection_lifecycle_tx
                                            .send(WsConnectionMsg::Disconnected(url.clone()))
                                            .await?;
                                        break 'recv_loop; // exit recv loop in order to reconnect
                                    }
                                }
                            }
                            Err(msg_err) => {
                                let e = format!("Invalid websocket message: {}", msg_err);
                                handle_error_with_fail(
                                    &sink_url,
                                    &e,
                                    &reply_tx,
                                    &ids,
                                    &event_origin_url,
                                    op_meta.clone(),
                                )
                                .await?;
                                continue; // next message, lets hope it is better
                            }
                        }
                    }
                }
                Err(encode_error) => {
                    let e = format!(
                        "Error during serialization (codec/postprocessors): {}",
                        encode_error
                    );
                    handle_error_with_fail(
                        &sink_url,
                        &e,
                        &reply_tx,
                        &ids,
                        &event_origin_url,
                        op_meta,
                    )
                    .await?;
                    continue; // next message, lets hope it is better
                }
            }

            // TODO: split ws_stream and spawn a separate task for receiving messages.
            //       then we need to associate requests with responses and include the event id of the causing request
            if has_link {
                if let Some(msg) = ws_stream.next().await {
                    match msg {
                        Ok(message @ Message::Text(_)) | Ok(message @ Message::Binary(_)) => {
                            let mut ingest_ns = nanotime();
                            match message_to_event(
                                &sink_url,
                                &event_origin_url,
                                codec,
                                &mut preprocessors,
                                &mut ingest_ns,
                                &ids,
                                message,
                            ) {
                                Ok(events) => {
                                    for event in events {
                                        reply_tx.send(sink::Reply::Response(OUT, event)).await?;
                                    }
                                }
                                Err(decode_error) => {
                                    let e_msg = format!(
                                        "Error deserializing Response message (codec/preprocessors): {}",
                                        decode_error
                                    );
                                    handle_error_no_fail(
                                        &sink_url,
                                        &e_msg,
                                        &reply_tx,
                                        &ids,
                                        &event_origin_url,
                                    )
                                    .await?;
                                    continue;
                                }
                            }
                        }
                        Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
                        Ok(Message::Close(_)) => {
                            warn!(
                                "[Sink::{}] Server {} closed websocket connection.",
                                &sink_url, &url,
                            );
                            connection_lifecycle_tx
                                .send(WsConnectionMsg::Disconnected(url.clone()))
                                .await?;
                            break 'recv_loop; // exit recv loop in order to reconnect
                        }
                        Err(e) => {
                            let e_msg =
                                format!("Error while receiving reply from server {}: {}", &url, e);
                            handle_error_no_fail(
                                &sink_url,
                                &e_msg,
                                &reply_tx,
                                &ids,
                                &event_origin_url,
                            )
                            .await?;
                            // close connection explicitly
                            ws_stream.close(None).await?;
                            connection_lifecycle_tx
                                .send(WsConnectionMsg::Disconnected(url.clone()))
                                .await?;
                            break 'recv_loop; // exit recv loop in order to reconnect
                        }
                    }
                }
            }
        }
    }
}

fn event_to_message(
    codec: &dyn Codec,
    postprocessors: &mut Postprocessors,
    ingest_ns: u64,
    data: &LineValue,
    binary: bool,
) -> Result<impl Iterator<Item = Result<Message>>> {
    let raw = codec.encode(data.suffix().value())?;
    let datas = postprocess(postprocessors, ingest_ns, raw)?;
    Ok(datas.into_iter().map(move |raw_data| {
        if binary {
            Ok(Message::Binary(raw_data))
        } else if let Ok(txt) = String::from_utf8(raw_data) {
            Ok(Message::Text(txt))
        } else {
            let msg = "Invalid utf8 data for text message";
            Err(msg.into())
        }
    }))
}

fn message_to_event(
    sink_url: &TremorURL,
    event_origin_uri: &EventOriginUri,
    codec: &mut dyn Codec,
    preprocessors: &mut Preprocessors,
    ingest_ns: &mut u64,
    ids: &EventId,
    message: Message,
) -> Result<Vec<Event>> {
    let mut meta = Value::object_with_capacity(1);
    let response_bytes = match message {
        Message::Text(d) => {
            meta.insert("binary", false)?;
            d.into_bytes()
        }
        Message::Binary(d) => d,
        _ => {
            // we verified that we have binary or text above, all good
            let msg = "Invalid response Message";
            return Err(msg.into());
        }
    };
    let preprocessed = preprocess(preprocessors, ingest_ns, response_bytes, &sink_url)?;
    // tried using an iter, but failed, so here we go
    let mut res = Vec::with_capacity(preprocessed.len());
    for pp in preprocessed {
        let data = LineValue::try_new(vec![pp], |mutd| {
            // ALLOW: we know this is save, as we just put pp into mutd
            let mut_data = mutd[0].as_mut_slice();
            let body = codec
                .decode(mut_data, nanotime())?
                .unwrap_or_else(Value::object);

            Ok(ValueAndMeta::from_parts(body, meta.clone()))
        })
        .map_err(|e: rental::RentalError<Error, _>| e.0)?;
        res.push(Event {
            id: ids.clone(),
            origin_uri: Some(event_origin_uri.clone()),
            data,
            ..Event::default()
        });
    }
    Ok(res)
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
                sink_url: TremorURL::from_onramp_id("ws")?,  // dummy value
                event_origin_uri: EventOriginUri::default(), // dummy
                config,
                connection_lifecycle_tx: tx,
                connection_lifecycle_rx: rx,
                connections: HashMap::new(),
                is_linked: false,
                merged_meta: OpMeta::default(),
                reply_tx,
                preprocessors: vec![],  // dummy, overwritten in init
                postprocessors: vec![], // dummy, overwritten in init
                shared_codec: Box::new(crate::codec::null::Null {}), //dummy, overwritten in init
            }))
        } else {
            Err("[WS Offramp] Offramp requires a config".into())
        }
    }
}

impl Ws {
    async fn handle_connection_lifecycle_events(&mut self, ingest_ns: u64) -> Result<()> {
        let len = self.connection_lifecycle_rx.len();
        for _ in 0..len {
            match self.connection_lifecycle_rx.recv().await? {
                WsConnectionMsg::Connected(url, addr) => {
                    // TODO trigger per url/connection (only resuming events with that url)
                    if url == self.config.url {
                        let mut e = Event::cb_restore(ingest_ns);
                        e.op_meta = self.merged_meta.clone();
                        self.reply_tx.send(sink::Reply::Insight(e)).await?;
                    }
                    self.connections
                        .entry(url)
                        .and_modify(|mut tuple| tuple.0 = Some(addr));
                }
                WsConnectionMsg::Disconnected(url) => {
                    // TODO trigger per url/connection (only events with that url should be paused)
                    if url == self.config.url {
                        let mut e = Event::cb_trigger(ingest_ns);
                        e.op_meta = self.merged_meta.clone();
                        self.reply_tx.send(sink::Reply::Insight(e)).await?;
                    }
                    if let Some((_, handle)) = self.connections.remove(&url) {
                        handle.cancel().await;
                    }
                }
            }
        }
        Ok(())
    }

    fn create_error_response(event_id: &EventId, e: &str, origin_uri: &EventOriginUri) -> Event {
        let mut error_data = tremor_script::Object::with_capacity(2);
        error_data.insert_nocheck("error".into(), Value::from(e.to_string()));
        error_data.insert_nocheck("event_id".into(), Value::from(event_id.to_string()));

        let mut meta = tremor_script::Object::with_capacity(1);
        meta.insert_nocheck("error".into(), Value::from(e.to_string()));

        Event {
            id: event_id.clone(),
            origin_uri: Some(origin_uri.clone()),
            data: (error_data, meta).into(),
            ..Event::default()
        }
    }

    fn get_message_meta(&self, meta: &Value) -> WsMessageMeta {
        WsMessageMeta {
            url: meta.get_str("url").unwrap_or(&self.config.url).into(),
            binary: meta.get_bool("binary").unwrap_or(self.config.binary),
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
            .map_or(false, |(addr, _)| addr.is_some())
    }

    async fn on_signal(&mut self, signal: Event) -> ResultVec {
        self.handle_connection_lifecycle_events(signal.ingest_ns)
            .await?;
        Ok(None)
    }

    async fn on_event(
        &mut self,
        _input: &str,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        if self.is_linked && event.is_batch {
            return Err("Batched events are not supported for linked websocket offramps".into());
        }
        // check for connects or disconnects
        // otherwise, we might lose some events to a connection, where connect is in progress
        self.handle_connection_lifecycle_events(nanotime()).await?;

        let Event {
            id,
            data,
            ingest_ns,
            op_meta,
            ..
        } = event;

        self.merged_meta.merge(op_meta);
        let msg_meta = self.get_message_meta(data.suffix().meta());

        // actually used when we have new connection to make (overridden from event-meta)
        let temp_conn_tx;
        let ws_conn_tx = if let Some((ws_conn_tx, _)) = self.connections.get(&msg_meta.url) {
            ws_conn_tx
        } else {
            let (conn_tx, conn_rx) = bounded(crate::QSIZE);
            // separate task to handle new url connection
            let handle = task::spawn(ws_connection_loop(
                self.sink_url.clone(),
                msg_meta.url.clone(),
                self.event_origin_uri.clone(),
                self.connection_lifecycle_tx.clone(),
                self.reply_tx.clone(),
                conn_tx.clone(),
                conn_rx,
                self.is_linked,
                make_preprocessors(self.preprocessors.as_slice())?,
                make_postprocessors(self.postprocessors.as_slice())?,
                self.shared_codec.boxed_clone(),
            ));
            // TODO default to None for initial connection? (like what happens for
            // default offramp config url). if we do circuit-breakers-per-url
            // connection, this will be handled better.
            self.connections
                .insert(msg_meta.url.clone(), (Some(conn_tx.clone()), handle));
            // sender here ensures we don't drop the current in-flight event
            temp_conn_tx = Some(conn_tx);
            &temp_conn_tx
        };

        if let Some(conn_tx) = ws_conn_tx {
            conn_tx
                .send(ConnectionTaskMsg::SendEvent(
                    id,
                    msg_meta,
                    self.merged_meta.clone(),
                    ingest_ns,
                    data,
                ))
                .await?;
        } else {
            let err = format!("No connection available for {}.", &msg_meta.url);
            handle_error_with_fail(
                &self.sink_url,
                &err,
                &self.reply_tx,
                &id,
                &self.event_origin_uri,
                self.merged_meta.clone(),
            )
            .await?;
        }
        Ok(None)
    }

    fn default_codec(&self) -> &str {
        "json"
    }

    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        sink_uid: u64,
        sink_url: &TremorURL,
        codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        is_linked: bool,
        reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        self.shared_codec = codec.boxed_clone();
        self.postprocessors = processors.post.to_vec();
        self.preprocessors = processors.pre.to_vec();

        self.is_linked = is_linked;
        self.sink_url = sink_url.clone();
        let parsed = Url::parse(&self.config.url)?; // should not fail as it has already been verified
        let origin_url = EventOriginUri {
            uid: sink_uid,
            scheme: "tremor-ws".to_string(),
            host: parsed.host_str().unwrap_or("UNKNOWN").to_string(),
            port: parsed.port(),
            path: vec![],
        };
        self.event_origin_uri = origin_url;

        // handle connection for the offramp config url (as default)
        let (conn_tx, conn_rx) = bounded(crate::QSIZE);
        self.reply_tx = reply_channel;
        let handle = task::Builder::new()
            .name(format!("{}-connection-{}", &sink_url, &self.config.url))
            .spawn(ws_connection_loop(
                sink_url.clone(),
                self.config.url.clone(),
                self.event_origin_uri.clone(),
                self.connection_lifecycle_tx.clone(),
                self.reply_tx.clone(),
                conn_tx,
                conn_rx,
                is_linked,
                make_preprocessors(self.preprocessors.as_slice())?,
                make_postprocessors(self.postprocessors.as_slice())?,
                self.shared_codec.boxed_clone(),
            ))?;
        self.connections
            .insert(self.config.url.clone(), (None, handle));

        Ok(())
    }

    async fn terminate(&mut self) {
        futures::future::join_all(
            self.connections
                .drain()
                .map(|(_, (_, handle))| handle.cancel()),
        )
        .await;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn message_to_event_ok() -> Result<()> {
        let sink_url = TremorURL::parse("/offramp/ws/instance")?;
        let origin_uri = EventOriginUri::default();
        let mut preprocessors = make_preprocessors(&["lines".to_string()])?;
        let mut ingest_ns = 42_u64;
        let ids = EventId::default();
        let mut codec: Box<dyn Codec> = Box::new(crate::codec::string::String {});
        let message = Message::Text("hello\nworld\n".to_string());
        let events = message_to_event(
            &sink_url,
            &origin_uri,
            codec.as_mut(),
            &mut preprocessors,
            &mut ingest_ns,
            &ids,
            message,
        )?;
        assert_eq!(2, events.len());
        let event0 = events.get(0).ok_or(Error::from("no event 0"))?;
        let (data, meta) = event0.data.parts();
        assert!(meta.is_object());
        assert_eq!(Some(&Value::from(false)), meta.get("binary"));
        assert_eq!(&mut Value::from("hello"), data);

        Ok(())
    }

    #[test]
    fn event_to_message_ok() -> Result<()> {
        let mut codec: Box<dyn Codec> = Box::new(crate::codec::json::JSON::default());
        let mut postprocessors = make_postprocessors(&["lines".to_string()])?;
        let mut data = Value::object_with_capacity(2);
        data.insert("snot", "badger")?;
        data.insert("empty", Value::object())?;
        let data = (data, Value::object()).into();
        let mut messages: Vec<Result<Message>> =
            event_to_message(codec.as_mut(), &mut postprocessors, 42, &data, true)?.collect();
        assert_eq!(1, messages.len());
        let msg = messages.pop().ok_or(Error::from("no event 0"))?;
        assert!(msg.is_ok());
        assert_eq!(
            Message::Binary("{\"snot\":\"badger\",\"empty\":{}}\n".as_bytes().to_vec()),
            msg?
        );
        Ok(())
    }

    #[async_std::test]
    async fn test_failed_connection_lifecycle() -> Result<()> {
        let (conn_tx, conn_rx) = bounded(10);
        let (reply_tx, reply_rx) = bounded(1000);

        let url = TremorURL::parse("/offramp/ws/instance")?;
        let codec: Box<dyn Codec> = Box::new(crate::codec::json::JSON::default());
        let config = Config {
            url: "http://idonotexist:65535/path".to_string(),
            binary: true,
        };
        let mut sink = Ws {
            sink_url: url.clone(),
            event_origin_uri: EventOriginUri::default(),
            config: config.clone(),
            preprocessors: vec!["lines".to_string()],
            postprocessors: vec!["lines".to_string()],
            shared_codec: codec.boxed_clone(),
            connection_lifecycle_rx: conn_rx,
            connection_lifecycle_tx: conn_tx,
            connections: HashMap::new(),
            is_linked: true,
            merged_meta: OpMeta::default(),
            reply_tx: reply_tx.clone(),
        };
        sink.init(
            0,
            &url,
            codec.as_ref(),
            &HashMap::new(),
            Processors::default(),
            true,
            reply_tx.clone(),
        )
        .await?;

        // we expect connect errors
        if let Ok(WsConnectionMsg::Disconnected(url)) = sink.connection_lifecycle_rx.recv().await {
            assert_eq!(config.url, url);
        }

        // lets try to send an event
        let mut event = Event::default();
        event.id = EventId::new(1, 1, 1);
        sink.on_event("in", codec.as_ref(), &HashMap::new(), event)
            .await?;

        while let Ok(msg) = reply_rx.try_recv() {
            match msg {
                sink::Reply::Insight(event) => {
                    assert_eq!(CBAction::Fail, event.cb);
                    assert_eq!(Some((1, 1)), event.id.get_max_by_source(1));
                }
                sink::Reply::Response(port, event) => {
                    assert_eq!("err", port.as_ref());
                    assert_eq!(Some((1, 1)), event.id.get_max_by_source(1));
                }
            }
        }

        sink.terminate().await;
        Ok(())
    }
}
