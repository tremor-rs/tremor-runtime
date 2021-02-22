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
// use crate::{NodeId, KV};

use super::*;
use crate::raft_node::NodeId;
use async_channel::{bounded, Receiver, Sender};
use async_std::io::{Read, Write};
use async_std::net::TcpListener;
use async_std::net::ToSocketAddrs;
use async_std::task;
//use futures::channel::mpsc::{channel, Receiver, Sender};
//use futures::io::{AsyncRead, AsyncWrite};
use futures::{select, FutureExt, SinkExt, StreamExt};
use std::io::Error;
use tungstenite::protocol::Message;

/// websocket connection is long running connection, it easier
/// to handle with an actor
pub(crate) struct Connection {
    node: Node,
    remote_id: NodeId,
    //protocol: Option<Protocol>,
    rx: Receiver<Message>,
    tx: Sender<Message>,
    ws_rx: Receiver<WsMessage>,
    ws_tx: Sender<WsMessage>,
}

impl Connection {
    pub(crate) fn new(node: Node, rx: Receiver<Message>, tx: Sender<Message>) -> Self {
        let (ws_tx, ws_rx) = bounded(crate::QSIZE);
        Self {
            node,
            remote_id: NodeId(0),
            //protocol: None,
            rx,
            tx,
            ws_tx,
            ws_rx,
        }
    }

    async fn handle_uring(&mut self, msg: Message) -> bool {
        if msg.is_text() {
            let text = msg.into_data();
            match serde_json::from_slice(&text) {
                // handling of hello from peer!
                Ok(CtrlMsg::Hello(id, peer)) => {
                    info!("Hello from {}", id);
                    self.remote_id = id;
                    self.node
                        .tx
                        .try_send(UrMsg::RegisterRemote(id, peer, self.ws_tx.clone()))
                        .is_ok()
                }
                Ok(CtrlMsg::AckProposal(pid, success)) => self
                    .node
                    .tx
                    .try_send(UrMsg::AckProposal(pid, success))
                    .is_ok(),
                Ok(CtrlMsg::ForwardProposal(from, pid, eid, value)) => self
                    .node
                    .tx
                    .try_send(UrMsg::ForwardProposal(from, pid, eid, value))
                    .is_ok(),
                Ok(_) => true,
                Err(e) => {
                    error!(
                        "Failed to decode CtrlMsg message: {} => {}",
                        e,
                        String::from_utf8(text).unwrap_or_default()
                    );
                    false
                }
            }
        } else if msg.is_binary() {
            let bin = msg.into_data();
            let msg = decode_ws(&bin);
            self.node.tx.try_send(UrMsg::RaftMsg(msg)).is_ok()
        } else {
            true
        }
    }

    pub async fn msg_loop(mut self, _logger: Logger) {
        loop {
            let cont = select! {
                msg = self.rx.next().fuse() => {
                    if let Some(msg) = msg {
                            // the only protocol we support right now
                            let handled_ok = self.handle_uring(msg).await;
                            handled_ok
                    } else {
                        false
                    }

                }
                msg = self.ws_rx.next().fuse() => {
                    match msg {
                        Some(WsMessage::Ctrl(msg)) =>self.tx.send(Message::Text(serde_json::to_string(&msg).unwrap())).await.is_ok(),
                        Some(WsMessage::Raft(msg)) => self.tx.send(Message::Binary(encode_ws(msg).to_vec())).await.is_ok(),
                        Some(WsMessage::Reply {data, ..}) =>self.tx.send(Message::Text(serde_json::to_string(&data).unwrap())).await.is_ok(),
                        None => false,
                    }
                }
                complete => false
            };
            if !cont {
                error!("Client connection to {} down.", self.remote_id);
                self.node
                    .tx
                    .try_send(UrMsg::DownRemote(self.remote_id))
                    .unwrap();
                break;
            }
        }
    }
}

pub(crate) async fn accept_connection<S>(logger: Logger, node: Node, stream: S)
where
    S: Read + Write + Unpin,
{
    let mut ws_stream = if let Ok(ws_stream) = async_tungstenite::accept_async(stream).await {
        ws_stream
    } else {
        error!("Error during the websocket handshake occurred");
        return;
    };

    // Create a channel for our stream, which other sockets will use to
    // send us messages. Then register our address with the stream to send
    // data to us.
    let (msg_tx, msg_rx) = bounded(crate::QSIZE);
    let (response_tx, mut response_rx) = bounded(crate::QSIZE);
    let c = Connection::new(node, msg_rx, response_tx);
    task::spawn(c.msg_loop(logger.clone()));

    loop {
        select! {
            message = ws_stream.next().fuse() => {
                if let Some(Ok(message)) = message {
                    msg_tx
                    .send(message).await
                    .expect("Failed to forward request");
                } else {
                    error!("Client connection down.", );
                    break;
                }
            }
            resp = response_rx.next().fuse() => {
                if let Some(resp) = resp {
                    ws_stream.send(resp).await.expect("Failed to send response");
                } else {
                    error!("Client connection down.", );
                    break;
                }

            }
            complete => {
                error!("Client connection down.", );
                break;
            }
        }
    }
}

pub(crate) async fn run(logger: Logger, node: Node, addr: String) -> Result<(), Error> {
    let addr = addr
        .to_socket_addrs()
        .await
        .expect("Not a valid address")
        .next()
        .expect("Not a socket address");

    // Create the event loop and TCP listener we'll accept connections on.
    let try_socket = TcpListener::bind(&addr).await;
    let listener = try_socket.expect("Failed to bind");
    info!("Listening on: {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        task::spawn(accept_connection(logger.clone(), node.clone(), stream));
    }

    Ok(())
}
