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

use super::*;
use crate::raft_node::NodeId;
use async_std::net::TcpStream;
use async_tungstenite::async_std::connect_async;
//use futures::channel::mpsc::{channel, Receiver};
use futures::{select, FutureExt, SinkExt, StreamExt};
use slog::Logger;
use tungstenite::protocol::Message;
//use ws_proto::{Protocol, ProtocolSelect};
use async_channel::{bounded, Sender};

type WSStream = async_tungstenite::WebSocketStream<TcpStream>;

macro_rules! eat_error_and_blow {
    ($e:expr) => {
        match $e {
            Err(e) => {
                //error!($l, "[WS Error] {}", e);
                error!("[WS Error] {}", e);
                panic!(format!("{}: {:?}", e, e));
            }
            Ok(v) => v,
        }
    };
}

pub(crate) struct Connection {
    //    my_id: u64,
    remote_id: NodeId,
    handler: Sender<UrMsg>,
    tx: Sender<WsMessage>,
    rx: Receiver<WsMessage>,
    //logger: Logger,
    ws_stream: WSStream,
    //handshake_done: bool,
}

/// Handle server websocket messages
impl Connection {
    async fn handle(&mut self, msg: Message) {
        if msg.is_binary() {
            let msg = decode_ws(&msg.into_data());
            self.handler.try_send(UrMsg::RaftMsg(msg)).unwrap();
        } else if msg.is_text() {
            let msg: CtrlMsg = eat_error_and_blow!(serde_json::from_slice(&msg.into_data()));
            match msg {
                // sent from the server as response to CtrlMsg::Hello
                CtrlMsg::HelloAck(id, peer, peers) => {
                    self.remote_id = id;
                    eat_error_and_blow!(
                        self.handler
                            .send(UrMsg::RegisterLocal(id, peer, self.tx.clone(), peers))
                            .await
                    );
                }
                CtrlMsg::AckProposal(pid, success) => {
                    // unbounded send
                    eat_error_and_blow!(self.handler.try_send(UrMsg::AckProposal(pid, success)));
                }
                _ => (),
            }
        } else {
            dbg!(("unknown message type", msg));
            // Nothing to do
            ()
        }
    }
}

async fn worker(_logger: Logger, endpoint: String, handler: Sender<UrMsg>) -> std::io::Result<()> {
    let url = url::Url::parse(&format!("ws://{}", endpoint)).unwrap();
    loop {
        //let logger = logger.clone();
        let ws_stream = if let Ok((ws_stream, _)) = connect_async(url.clone()).await {
            ws_stream
        } else {
            error!("Failed to connect to {}", endpoint);
            break;
        };
        let (tx, rx) = bounded::<WsMessage>(crate::QSIZE);

        //ws_stream
        //    .send(Message::Text(
        //        serde_json::to_string(&ProtocolSelect::Select {
        //            rid: RequestId(1),
        //            protocol: Protocol::URing,
        //        })
        //        .unwrap(),
        //    ))
        //    .await
        //    .unwrap();
        // triggers a hello message to the peer
        handler.try_send(UrMsg::InitLocal(tx.clone())).unwrap();

        let mut c = Connection {
            //logger,
            remote_id: NodeId(0),
            handler: handler.clone(),
            ws_stream,
            //handshake_done: false,
            rx,
            tx,
        };
        loop {
            let cont = select! {
                msg = c.rx.next().fuse() =>
                    // talking to the peer from the current node
                    match msg {
                        Some(WsMessage::Raft(msg)) => {c.ws_stream.send(Message::Binary(encode_ws(msg).to_vec())).await.is_ok()},
                        Some(WsMessage::Ctrl(msg)) => {c.ws_stream.send(Message::Text(serde_json::to_string(&msg).unwrap())).await.is_ok()},
                        Some(WsMessage::Reply {data, ..}) => {c.ws_stream.send(Message::Text(serde_json::to_string(&data).unwrap())).await.is_ok()},
                        None => false
                },
                // receiving messages from the peer and handling it
                msg = c.ws_stream.next().fuse() => {
                    if let Some(Ok(msg)) = msg {
                        c.handle(msg).await;
                        true
                    } else {
                        false
                    }
                },
                complete => false
            };
            if !cont {
                break;
            }
        }
        c.handler.try_send(UrMsg::DownLocal(c.remote_id)).unwrap();
    }

    Ok(())
}

pub(crate) async fn remote_endpoint(
    endpoint: String,
    handler: Sender<UrMsg>,
    logger: Logger,
) -> std::io::Result<()> {
    worker(logger, endpoint, handler).await
}
