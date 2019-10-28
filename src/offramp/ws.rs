// Copyright 2018-2019, Wayfair GmbH
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
use actix::io::SinkWrite;
use actix::prelude::*;
use actix_codec::Framed;
use awc::{
    error::WsProtocolError,
    ws::{CloseCode, CloseReason, Codec as AxCodec, Frame, Message},
    BoxedSocket, Client,
};
use bytes::Bytes;
use crossbeam_channel::{bounded, Receiver, Sender};
use futures::{
    lazy,
    stream::{SplitSink, Stream},
    Future,
};
use halfbrown::HashMap;
use serde_yaml;
use std::io;
use std::thread;
use std::time::Duration;

macro_rules! eat_error {
    ($e:expr) => {
        if let Err(e) = $e {
            error!("[WS Offramp] {}", e)
        }
    };
}

type WsAddr = Addr<WsOfframpWorker>;

#[derive(Deserialize, Debug)]
pub struct Config {
    /// Host to use as source
    pub url: String,
    #[serde(default = "d_false")]
    pub binary: bool,
}

/// An offramp that writes to a websocket endpoint
pub struct Ws {
    addr: Option<WsAddr>,
    config: Config,
    pipelines: HashMap<TremorURL, PipelineAddr>,
    postprocessors: Postprocessors,
    tx: Sender<Option<WsAddr>>,
    rx: Receiver<Option<WsAddr>>,
}

#[derive(Message)]
enum WsMessage {
    Binary(Vec<u8>),
    Text(String),
}

impl offramp::Impl for Ws {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            let url = config.url.clone();
            let (tx, rx) = bounded(0);
            let txe = tx.clone();
            let my_tx = tx.clone();
            thread::Builder::new()
                .name(format!("offramp-ws-actix-loop-{}", "???"))
                .spawn(move || -> io::Result<()> {
                    let sys = actix::System::new("ws-offramp");
                    Arbiter::spawn(lazy(move || {
                        Client::new()
                            .ws(&url)
                            .connect()
                            .map_err(move |e| {
                                eat_error!(txe.send(None));
                                error!("[WS Offramp]: {}", e);
                                System::current().stop();
                            })
                            .map(move |(_response, framed)| {
                                let (sink, stream) = framed.split();
                                let url = url.clone();
                                let tx1 = tx.clone();
                                let addr = WsOfframpWorker::create(|ctx| {
                                    WsOfframpWorker::add_stream(stream, ctx);
                                    WsOfframpWorker(url, tx1, SinkWrite::new(sink, ctx))
                                });
                                if let Err(e) = tx.send(Some(addr)) {
                                    error!("[WS Offramp] Failed to set up offramp: {}.", e)
                                };
                            })
                    }));
                    let r = sys.run();
                    warn!("[WS Offramp] Transient thread terminated due to upstream error ... reconnecting");
                    r
                })?;
            Ok(Box::new(Self {
                addr: None,
                config,
                pipelines: HashMap::new(),
                postprocessors: vec![],
                tx: my_tx,
                rx,
            }))
        } else {
            Err("[WS Offramp] Offramp requires a config".into())
        }
    }
}

impl Offramp for Ws {
    fn on_event(&mut self, codec: &Box<dyn Codec>, _input: String, event: Event) -> Result<()> {
        // If we are not connected yet we wait for a message to connect
        if self.addr.is_none() {
            self.addr = self.rx.recv()?;
        }
        // We eat up the entire buffer
        while let Ok(addr) = self.rx.try_recv() {
            self.addr = addr;
        }
        // If after that we're not connected yet we try to reconnect
        if self.addr.is_none() {
            std::thread::sleep(Duration::from_secs(1));
            let url = self.config.url.clone();
            let tx = self.tx.clone();
            let txe = self.tx.clone();
            thread::Builder::new()
                .name(format!("offramp-ws-actix-loop-{}", "???"))
                .spawn(move || -> io::Result<()> {
                    let sys = actix::System::new("ws-offramp");
                    Arbiter::spawn(lazy(move || {
                        Client::new()
                            .ws(&url)
                            .connect()
                            .map_err(move |e| {
                                if let Err(e) = txe.send(None) {
                                    error!("[WS Offramp] Failed to set up offramp: {}.", e)
                                };
                                error!("[WS Offramp]: {}", e);
                                System::current().stop();
                            })
                            .map(move |(_response, framed)| {
                                let (sink, stream) = framed.split();
                                let url = url.clone();
                                let tx1 = tx.clone();
                                let addr = WsOfframpWorker::create(|ctx| {
                                    WsOfframpWorker::add_stream(stream, ctx);
                                    WsOfframpWorker(url, tx1, SinkWrite::new(sink, ctx))
                                });
                                if let Err(e) = tx.send(Some(addr)) {
                                    error!("[WS Offramp] Failed to set up offramp: {}.", e)
                                };
                            })
                    }));

                    let r = sys.run();
                    warn!("[WS Offramp] Transient thread terminated due to upstream error ... reconnecting");
                    r
                })?;
        }
        if let Some(addr) = &self.addr {
            for value in event.value_iter() {
                let raw = codec.encode(value)?;
                let datas = postprocess(&mut self.postprocessors, event.ingest_ns, raw)?;
                for raw in datas {
                    if self.config.binary {
                        addr.do_send(WsMessage::Binary(raw));
                    } else if let Ok(txt) = String::from_utf8(raw) {
                        addr.do_send(WsMessage::Text(txt));
                    } else {
                        error!("[WS Offramp] Invalid utf8 data for text message");
                        return Err(Error::from("Invalid utf8 data for text message"));
                    }
                }
            }
        } else {
            error!("[WS Offramp] not connected");
            return Err(Error::from("not connected"));
        };
        Ok(())
    }

    fn add_pipeline(&mut self, id: TremorURL, addr: PipelineAddr) {
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
struct WsOfframpWorker(
    String,
    Sender<Option<WsAddr>>,
    SinkWrite<SplitSink<Framed<BoxedSocket, AxCodec>>>,
);

impl WsOfframpWorker {
    fn hb(&self, ctx: &mut Context<Self>) {
        ctx.run_later(Duration::new(1, 0), |act, ctx| {
            eat_error!(act.2.write(Message::Ping(String::from("Yay tremor!"))));
            act.hb(ctx);
        });
    }
}
impl actix::io::WriteHandler<WsProtocolError> for WsOfframpWorker {}

impl Actor for WsOfframpWorker {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.hb(ctx)
    }

    fn stopped(&mut self, _: &mut Context<Self>) {
        eat_error!(self.1.send(None));
        info!("system stopped");
        System::current().stop();
    }
}

/// Handle stdin commands
impl Handler<WsMessage> for WsOfframpWorker {
    type Result = ();

    fn handle(&mut self, msg: WsMessage, _ctx: &mut Context<Self>) {
        match msg {
            WsMessage::Binary(data) => eat_error!(self.2.write(Message::Binary(data.into()))),
            WsMessage::Text(data) => eat_error!(self.2.write(Message::Text(data))),
        }
    }
}

/// Handle server websocket messages
impl StreamHandler<Frame, WsProtocolError> for WsOfframpWorker {
    fn handle(&mut self, msg: Frame, ctx: &mut Context<Self>) {
        match msg {
            Frame::Close(_) => {
                eat_error!(self.2.write(Message::Close(None)));
            }
            Frame::Ping(data) => {
                eat_error!(self.2.write(Message::Pong(data)));
            }
            Frame::Text(Some(data)) => {
                if let Ok(txt) = String::from_utf8(data.to_vec()) {
                    eat_error!(self.2.write(Message::Text(txt)));
                } else {
                    eat_error!(self.2.write(Message::Close(Some(CloseReason {
                        code: CloseCode::Error,
                        description: None,
                    }))));
                    ctx.stop();
                }
            }
            Frame::Text(None) => {
                eat_error!(self.2.write(Message::Text(String::new())));
            }
            Frame::Binary(Some(data)) => {
                eat_error!(self.2.write(Message::Binary(data.into())));
            }
            Frame::Binary(None) => {
                eat_error!(self.2.write(Message::Binary(Bytes::new())));
            }
            Frame::Pong(_) => (),
        }
    }

    fn error(&mut self, error: WsProtocolError, _ctx: &mut Context<Self>) -> Running {
        match error {
            _ => eat_error!(self.2.write(Message::Close(Some(CloseReason {
                code: CloseCode::Protocol,
                description: None,
            })))),
        };
        // Reconnect
        Running::Continue
    }

    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!("[WS Onramp] Connection established.");
    }

    fn finished(&mut self, ctx: &mut Context<Self>) {
        info!("[WS Onramp] Connection terminated.");
        eat_error!(self.2.write(Message::Close(None)));
        ctx.stop()
    }
}
