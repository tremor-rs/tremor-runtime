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

use crate::onramp::prelude::*;
//use crate::utils::nanotime;
use async_std::sync::Sender;
use futures::{select, FutureExt, StreamExt};
use serde_yaml::Value;
//use std::io;
//use std::thread;
use tungstenite::protocol::Message;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// The port to listen on.
    pub port: u16,
    /// Host to listen on
    pub host: String,
}

pub struct Ws {
    pub config: Config,
}

impl onramp::Impl for Ws {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            Ok(Box::new(Self { config }))
        } else {
            Err("Missing config for blaster onramp".into())
        }
    }
}
enum WsOnrampMessage {
    Data(u64, EventOriginUri, Vec<u8>),
}
/*
type ActixResult<T> = std::result::Result<T, ActixError>;



/// websocket connection is long running connection, it easier
/// to handle with an actor
struct TremorWebSocket {
    preprocessors: Preprocessors,
    tx: Sender<WsOnrampMessage>,
    origin_uri: tremor_pipeline::EventOriginUri,
}

impl TremorWebSocket {
    fn new(
        tx: Sender<WsOnrampMessage>,
        preprocessors: Preprocessors,
        origin_uri: tremor_pipeline::EventOriginUri,
    ) -> Self {
        Self {
            tx,
            preprocessors,
            origin_uri,
        }
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
                //FIXME: Once we get 'proper' websockets
                ctx.pong(&msg);
            }
            ws::Message::Pong(_) | ws::Message::Nop => {}
            ws::Message::Text(bin) => {
                #[cfg(feature = "ws-echo")]
                ctx.text(&bin);
                let mut ingest_ns = nanotime();
                if let Ok(data) =
                    handle_pp(&mut self.preprocessors, &mut ingest_ns, bin.into_bytes())
                {
                    for d in data {
                        if let Err(e) = self.tx.send(WsOnrampMessage::Data(
                            ingest_ns,
                            // TODO possible to avoid clone here? we clone again inside send_event
                            self.origin_uri.clone(),
                            d,
                        )) {
                            error!("Websocket onramp message error: {}", e)
                        }
                    }
                }
            }
            ws::Message::Binary(bin) => {
                #[cfg(feature = "ws-echo")]
                ctx.binary(bin.clone());
                let mut ingest_ns = nanotime();
                if let Ok(data) = handle_pp(&mut self.preprocessors, &mut ingest_ns, bin.to_vec()) {
                    for d in data {
                        if let Err(e) = self.tx.send(WsOnrampMessage::Data(
                            ingest_ns,
                            // TODO possible to avoid clone here? we clone again inside send_event
                            self.origin_uri.clone(),
                            d,
                        )) {
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
        }
    }
}

struct WsServerState {
    tx: Sender<WsOnrampMessage>,
    preprocessors: Vec<String>,
}
/// do websocket handshake and start `MyWebSocket` actor
// The signature is enforced by a forign trait.
#[allow(clippy::needless_pass_by_value)]
fn ws_index(
    req: HttpRequest,
    stream: web::Payload,
    data: web::Data<WsServerState>,
) -> ActixResult<HttpResponse> {
    info!("Starting websocket handler");

    let preprocessors = make_preprocessors(&data.preprocessors)?;
    let origin_uri = tremor_pipeline::EventOriginUri {
        scheme: "tremor-ws".to_string(),
        host: req
            .connection_info()
            .remote()
            .unwrap_or("tremor-ws-client-host.remote")
            .to_string(),
        port: None,
        // TODO add server port here (like for tcp onramp) -- can be done via WsServerState
        path: vec![String::default()],
    };

    ws::start(
        TremorWebSocket::new(data.tx.clone(), preprocessors, origin_uri),
        &req,
        stream,
    )
}

// We got to allow this because of the way that the onramp works
// by creating new instances during runtime.
#[allow(clippy::needless_pass_by_value)]
fn onramp_loop(
    rx: &Receiver<onramp::Msg>,
    config: Config,
    preprocessors: Vec<String>,
    mut codec: Box<dyn Codec>,
    mut metrics_reporter: RampReporter,
) -> Result<()> {
    let (main_tx, main_rx) = bounded(10);

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

    let mut pipelines: Vec<(TremorURL, pipeline::Addr)> = Vec::new();
    let mut id = 0;
    let mut no_pp = vec![];

    // Those are needed for the select! macro
    #[allow(clippy::zero_ptr, clippy::drop_copy)]
    loop {
        select! {
            recv(rx) -> msg => match msg {
                Err(e) => return Err(format!("Crossbream receive error: {}", e).into()),

                Ok(onramp::Msg::Connect(ps)) => {
                    for p in &ps {
                        if p.0 == *METRICS_PIPELINE {
                            metrics_reporter.set_metrics_pipeline(p.clone());
                        } else {
                            pipelines.push(p.clone());
                        }
                    }
                }
                Ok(onramp::Msg::Disconnect { id, tx }) => {
                    pipelines.retain(|(pipeline, _)| pipeline != &id);
                    if pipelines.is_empty() {
                        tx.send(true)?
                    } else {
                        tx.send(false)?
                    }
                }
            },
            recv(main_rx) -> msg => match msg {
                Err(e) => return Err(format!("Crossbream receive error: {}", e).into()),
                Ok(WsOnrampMessage::Data(mut ingest_ns, origin_uri, data)) => {
                    id += 1;
                    send_event(
                        &pipelines,
                        &mut no_pp,
                        &mut codec,
                        &mut metrics_reporter,
                        &mut ingest_ns,
                        &origin_uri,
                        id,
                        data
                    );
                }
            }
        }
    }
}
*/
use async_std::net::{TcpListener, TcpStream};
use async_std::task;

async fn handle_connection(
    loop_tx: Sender<WsOnrampMessage>,
    raw_stream: TcpStream,
    mut preprocessors: Preprocessors,
) -> Result<()> {
    let mut ws_stream = async_tungstenite::accept_async(raw_stream).await.unwrap();

    let origin_uri = tremor_pipeline::EventOriginUri {
        scheme: "tremor-ws".to_string(),
        host: "tremor-ws-client-host.remote".to_string(),
        port: None,
        // TODO add server port here (like for tcp onramp) -- can be done via WsServerState
        path: vec![String::default()],
    };

    // Insert the write part of this peer to the peer map.
    //let (tx, rx) = channel(64);

    //let (_outgoing, _incoming) = ws_stream.split();

    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(Message::Text(t)) => {
                let mut ingest_ns = nanotime();
                if let Ok(data) = handle_pp(&mut preprocessors, &mut ingest_ns, t.into_bytes()) {
                    for d in data {
                        loop_tx
                            .send(WsOnrampMessage::Data(
                                ingest_ns,
                                // TODO possible to avoid clone here? we clone again inside send_event
                                origin_uri.clone(),
                                d,
                            ))
                            .await;
                    }
                }
            }
            Ok(Message::Binary(b)) => {
                let mut ingest_ns = nanotime();
                if let Ok(data) = handle_pp(&mut preprocessors, &mut ingest_ns, b) {
                    for d in data {
                        loop_tx
                            .send(WsOnrampMessage::Data(
                                ingest_ns,
                                // TODO possible to avoid clone here? we clone again inside send_event
                                origin_uri.clone(),
                                d,
                            ))
                            .await;
                    }
                }
            }
            Ok(Message::Ping(_)) => (),
            Ok(Message::Pong(_)) => (),
            Ok(Message::Close(_)) => break,
            Err(e) => error!("WS error returned while waiting for client data: {}", e),
        }
    }
    Ok(())
}

async fn onramp_loop(
    rx: &Receiver<onramp::Msg>,
    config: Config,
    preprocessors: Vec<String>,
    mut codec: Box<dyn Codec>,
    mut metrics_reporter: RampReporter,
) -> Result<()> {
    let (loop_tx, loop_rx) = channel(64);

    let addr = format!("{}:{}", config.host, config.port);

    let mut pipelines = Vec::new();
    let mut id = 0;
    let mut no_pp = vec![];

    // Create the event loop and TCP listener we'll accept connections on.
    let try_socket = TcpListener::bind(&addr).await;
    let listener = try_socket.expect("Failed to bind");
    println!("Listening on: {}", addr);

    // Let's spawn the handling of each connection in a separate task.

    loop {
        loop {
            match handle_pipelines(&rx, &mut pipelines, &mut metrics_reporter).await? {
                PipeHandlerResult::Retry => continue,
                PipeHandlerResult::Terminate => return Ok(()),
                PipeHandlerResult::Normal => break,
            }
        }
        select! {
            msg = listener.accept().fuse() => if let Ok((stream, _socket)) = msg {
                let preprocessors = make_preprocessors(&preprocessors)?;

                task::spawn(handle_connection(loop_tx.clone(), stream, preprocessors));
            },
            msg = loop_rx.recv().fuse() => if let Some(WsOnrampMessage::Data(mut ingest_ns, origin_uri, data)) = msg {
                id += 1;
                send_event(
                    &pipelines,
                    &mut no_pp,
                    &mut codec,
                    &mut metrics_reporter,
                    &mut ingest_ns,
                    &origin_uri,
                    id,
                    data
                );

            },
            msg = rx.recv().fuse() => if let Some(msg) = msg {
                match handle_pipelines_msg(msg, &mut pipelines, &mut metrics_reporter)? {
                    PipeHandlerResult::Retry | PipeHandlerResult::Normal => continue,
                    PipeHandlerResult::Terminate => break,
                }
            }
        }
    }
    Ok(())
}

impl Onramp for Ws {
    fn start(
        &mut self,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let (tx, rx) = channel(1);
        let config = self.config.clone();
        let codec = codec::lookup(&codec)?;
        // we need to change this here since ws is special
        let preprocessors = preprocessors.to_vec();
        task::Builder::new()
            .name(format!("onramp-ws-{}", "???"))
            .spawn(async move {
                if let Err(e) =
                    onramp_loop(&rx, config, preprocessors, codec, metrics_reporter).await
                {
                    error!("[Onramp] Error: {}", e)
                }
            })?;
        Ok(tx)
    }

    fn default_codec(&self) -> &str {
        "string"
    }
}
