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
use actix::*;
use actix_codec::{AsyncRead, AsyncWrite, Framed};
use awc::{
    error::WsProtocolError,
    ws::{CloseCode, CloseReason, Codec as AxCodec, Frame, Message},
    BoxedSocket, Client,
};
use bytes::Bytes;
use crossbeam_channel::bounded;
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

#[derive(Deserialize, Debug)]
pub struct Config {
    /// Host to use as source
    pub url: String,
    #[serde(default = "d_false")]
    pub binary: bool,
}

/// An offramp that write a given file
pub struct Ws {
    addr: Addr<WsOfframpWorker<BoxedSocket>>,
    config: Config,
    pipelines: HashMap<TremorURL, PipelineAddr>,
    postprocessors: Postprocessors,
}

#[derive(Message)]
enum WsMessage {
    Binary(Vec<u8>),
    Text(String),
}

impl OfframpImpl for Ws {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            let url = config.url.clone();
            let (tx, rx) = bounded(0);
            thread::Builder::new()
                .name(format!("offramp-ws-actix-loop-{}", "???"))
                .spawn(move || -> io::Result<()> {
                    let sys = actix::System::new("ws-offramp");
                    Arbiter::spawn(lazy(move || {
                        Client::new()
                            .ws(&url)
                            .connect()
                            .map_err(|e| {
                                println!("Error: {}", e);
                            })
                            .map(move |(response, framed)| {
                                println!("{:?}", response);
                                let (sink, stream) = framed.split();
                                let addr = WsOfframpWorker::create(|ctx| {
                                    WsOfframpWorker::add_stream(stream, ctx);
                                    WsOfframpWorker(SinkWrite::new(sink, ctx))
                                });
                                if let Err(e) = tx.send(addr) {
                                    error!("[WS Offramp] Failed to set up offramp: {}.", e)
                                };
                            })
                    }));

                    sys.run()
                })?;
            let addr = rx.recv()?;
            Ok(Box::new(Ws {
                addr,
                config,
                pipelines: HashMap::new(),
                postprocessors: vec![],
            }))
        } else {
            Err("[WS Offramp] Offramp requires a config".into())
        }
    }
}

impl Offramp for Ws {
    // TODO
    fn on_event(&mut self, codec: &Box<dyn Codec>, _input: String, event: Event) {
        for event in event.into_iter() {
            if let Ok(raw) = codec.encode(event.value) {
                match postprocess(&mut self.postprocessors, raw) {
                    Ok(datas) => {
                        for raw in datas {
                            if self.config.binary {
                                self.addr.do_send(WsMessage::Binary(raw));
                            } else if let Ok(txt) = String::from_utf8(raw) {
                                self.addr.do_send(WsMessage::Text(txt));
                            } else {
                                error!("[WS Offramp] Invalid utf8 data for text message")
                            }
                        }
                    }
                    Err(e) => error!("[WS Offramp] Postprocessors failed: {}", e),
                }
            } else {
                error!("[WS Offramp] Codec failed")
            }
        }
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
    fn start(&mut self, _codec: &Box<dyn Codec>, postprocessors: &[String]) {
        self.postprocessors = make_postprocessors(postprocessors)
            .expect("failed to setup post processors for stdout");
    }
}
struct WsOfframpWorker<T>(SinkWrite<SplitSink<Framed<T, AxCodec>>>)
where
    T: AsyncRead + AsyncWrite;

impl<T: 'static> WsOfframpWorker<T>
where
    T: AsyncRead + AsyncWrite,
{
    fn hb(&self, ctx: &mut Context<Self>) {
        ctx.run_later(Duration::new(1, 0), |act, ctx| {
            eat_error!(act.0.write(Message::Ping(String::from("Yay tremor!"))));
            act.hb(ctx);
        });
    }
}
impl<T: 'static> actix::io::WriteHandler<WsProtocolError> for WsOfframpWorker<T> where
    T: AsyncRead + AsyncWrite
{
}

impl<T: 'static> Actor for WsOfframpWorker<T>
where
    T: AsyncRead + AsyncWrite,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.hb(ctx)
    }

    fn stopped(&mut self, _: &mut Context<Self>) {
        System::current().stop();
    }
}

/// Handle stdin commands
impl<T: 'static> Handler<WsMessage> for WsOfframpWorker<T>
where
    T: AsyncRead + AsyncWrite,
{
    type Result = ();

    fn handle(&mut self, msg: WsMessage, _ctx: &mut Context<Self>) {
        match msg {
            WsMessage::Binary(data) => eat_error!(self.0.write(Message::Binary(data.into()))),
            WsMessage::Text(data) => eat_error!(self.0.write(Message::Text(data))),
        }
    }
}

/// Handle server websocket messages
impl<T: 'static> StreamHandler<Frame, WsProtocolError> for WsOfframpWorker<T>
where
    T: AsyncRead + AsyncWrite,
{
    fn handle(&mut self, msg: Frame, ctx: &mut Context<Self>) {
        match msg {
            Frame::Close(_) => {
                eat_error!(self.0.write(Message::Close(None)));
            }
            Frame::Ping(data) => {
                eat_error!(self.0.write(Message::Pong(data)));
            }
            Frame::Text(Some(data)) => {
                if let Ok(txt) = String::from_utf8(data.to_vec()) {
                    eat_error!(self.0.write(Message::Text(txt)));
                } else {
                    eat_error!(self.0.write(Message::Close(Some(CloseReason {
                        code: CloseCode::Error,
                        description: None,
                    }))));
                    ctx.stop();
                }
            }
            Frame::Text(None) => {
                eat_error!(self.0.write(Message::Text(String::new())));
            }
            Frame::Binary(Some(data)) => {
                eat_error!(self.0.write(Message::Binary(data.into())));
            }
            Frame::Binary(None) => {
                eat_error!(self.0.write(Message::Binary(Bytes::new())));
            }
            /* TODO: fixed in PR
            Frame::Continue => {
                //self.0.write(Message::Ping(String::new())).unwrap();
                ()
            }
            */
            Frame::Pong(_) => (),
        }
    }

    fn error(&mut self, error: WsProtocolError, _ctx: &mut Context<Self>) -> Running {
        match error {
            _ => eat_error!(self.0.write(Message::Close(Some(CloseReason {
                code: CloseCode::Protocol,
                description: None,
            })))),
        };
        Running::Stop
    }

    fn started(&mut self, _ctx: &mut Context<Self>) {
        //println!("Connected");
    }

    fn finished(&mut self, ctx: &mut Context<Self>) {
        //println!("Server disconnected");
        eat_error!(self.0.write(Message::Close(None)));
        ctx.stop()
    }
}
