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

use crate::onramp::prelude::*;
use crate::utils::nanotime;
use actix::prelude::*;
use actix_web::{middleware, web, App, Error as ActixError, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;
use crossbeam_channel::{select, Sender};
use serde_yaml::Value;
use std::io;
use std::thread;

type ActixResult<T> = std::result::Result<T, ActixError>;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// The port to listen on.
    pub port: u32,
    /// Host to listen on
    pub host: String,
}

pub struct Ws {
    pub config: Config,
}

impl OnrampImpl for Ws {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            Ok(Box::new(Ws { config }))
        } else {
            Err("Missing config for blaster onramp".into())
        }
    }
}

enum WsOnrampMessage {
    Data(Vec<u8>),
}

/// websocket connection is long running connection, it easier
/// to handle with an actor
struct TremorWebSocket {
    preprocessors: Preprocessors,
    tx: Sender<WsOnrampMessage>,
}

impl TremorWebSocket {
    fn new(tx: Sender<WsOnrampMessage>, preprocessors: Preprocessors) -> Self {
        Self { tx, preprocessors }
    }
}

impl Actor for TremorWebSocket {
    type Context = ws::WebsocketContext<Self>;
}

/// Handler for `ws::Message`
impl StreamHandler<ws::Message, ws::ProtocolError> for TremorWebSocket {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
        // process websocket messages
        match msg {
            ws::Message::Ping(msg) => {
                //FIXME: Once we get 'propper' websockets
                ctx.pong(&msg);
            }
            ws::Message::Pong(_msg) => {}
            ws::Message::Text(bin) => {
                #[cfg(feature = "ws-echo")]
                ctx.text(&bin);
                let ingest_ns = nanotime();
                if let Ok(data) = handle_pp(&mut self.preprocessors, ingest_ns, bin.into_bytes()) {
                    for d in data {
                        if let Err(e) = self.tx.send(WsOnrampMessage::Data(d)) {
                            error!("Websocket onramp message error: {}", e)
                        }
                    }
                }
            }
            ws::Message::Binary(bin) => {
                #[cfg(feature = "ws-echo")]
                ctx.binary(bin.clone());
                let ingest_ns = nanotime();
                if let Ok(data) = handle_pp(&mut self.preprocessors, ingest_ns, bin.to_vec()) {
                    for d in data {
                        if let Err(e) = self.tx.send(WsOnrampMessage::Data(d)) {
                            error!("Websocket onramp message error: {}", e)
                        }
                    }
                }
            }
            ws::Message::Close(Some(reason)) => {
                if let ws::CloseCode::Other(_) = reason.code {
                } else if reason.code == ws::CloseCode::Abnormal
                    || reason.code == ws::CloseCode::Tls
                {
                } else {
                    ctx.close(Some(reason));
                }
                ctx.stop();
            }
            ws::Message::Close(None) => {
                ctx.close(None);
                ctx.stop();
            }
            ws::Message::Nop => {}
        }
    }
}

struct WsServerState {
    tx: Sender<WsOnrampMessage>,
    preprocessors: Vec<String>,
}
/// do websocket handshake and start `MyWebSocket` actor
fn ws_index(
    r: HttpRequest,
    stream: web::Payload,
    data: web::Data<WsServerState>,
) -> ActixResult<HttpResponse> {
    info!("Starting websocket handler");
    let preprocessors = make_preprocessors(&data.preprocessors)?;
    ws::start(
        TremorWebSocket::new(data.tx.clone(), preprocessors),
        &r,
        stream,
    )
}

fn onramp_loop(
    rx: Receiver<OnrampMsg>,
    config: Config,
    preprocessors: Vec<String>,
    codec: String,
) -> Result<()> {
    let (main_tx, main_rx) = bounded(10);

    let mut codec = codec::lookup(&codec)?;

    thread::Builder::new()
        .name(format!("onramp-ws-actix-loop-{}", "???"))
        .spawn(move || -> io::Result<()> {
            info!("Starting websocket server for onramp");
            let server = HttpServer::new(move || {
                App::new()
                    // enable logger
                    .wrap(middleware::Logger::default())
                    // websocket route
                    .data(WsServerState {
                        tx: main_tx.clone(),
                        preprocessors: preprocessors.clone(),
                    })
                    .service(web::resource("/").route(web::get().to(ws_index)))
            })
            .bind(&format!("{}:{}", config.host, config.port))?;
            server.run()?;
            Ok(())
        })?;

    let mut pipelines: Vec<(TremorURL, PipelineAddr)> = Vec::new();
    let mut id = 0;
    let mut no_pp = vec![];
    // Those are needed for the select! macro
    #[allow(clippy::zero_ptr, clippy::drop_copy)]
    loop {
        select! {
            recv(rx) -> msg => match msg {
                Err(e) => return Err(format!("Crossbream receive error: {}", e).into()),

                Ok(OnrampMsg::Connect(mut ps)) => pipelines.append(&mut ps),
                Ok(OnrampMsg::Disconnect { id, tx }) => {
                    pipelines.retain(|(pipeline, _)| pipeline != &id);
                    if pipelines.is_empty() {
                        let _ = tx.send(true);
                    } else {
                        let _ = tx.send(false);
                    }
                }
            },
            recv(main_rx) -> msg => match msg {
                Err(e) => return Err(format!("Crossbream receive error: {}", e).into()),
                Ok(WsOnrampMessage::Data(data)) => {
                    id += 1;
                    send_event(&pipelines, &mut no_pp, &mut codec, id, data);
                }
            }
        }
    }
}

impl Onramp for Ws {
    fn start(&mut self, codec: String, preprocessors: Vec<String>) -> Result<OnrampAddr> {
        let (tx, rx) = bounded(0);
        let config = self.config.clone();
        thread::Builder::new()
            .name(format!("onramp-udp-{}", "???"))
            .spawn(move || {
                if let Err(e) = onramp_loop(rx, config, preprocessors, codec) {
                    error!("[Onramp] Error: {}", e)
                }
            })?;
        Ok(tx)
    }
    fn default_codec(&self) -> &str {
        "string"
    }
}
