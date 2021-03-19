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

mod client;
mod server;

pub use crate::raft::node::NodeId;
use crate::raft::node::{EventId, ProposalId, RaftNetworkMsg};
use crate::temp_network::{Error as NetworkError, Network as NetworkTrait};
use async_channel::{unbounded, Receiver, Sender};
use async_std::task;
use async_trait::async_trait;
use bytes::Bytes;
//use futures::future::{BoxFuture, FutureExt};
use futures::StreamExt;
use halfbrown::HashMap;
use raft::eraftpb::Message as RaftMessage;
use slog::Logger;
use std::io;

#[derive(Clone)]
pub(crate) struct Node {
    id: NodeId,
    tx: Sender<UrMsg>,
    logger: Logger,
}

/// blah
#[derive(Debug)]
pub enum UrMsg {
    // Network related
    /// blah
    InitLocal(Sender<WsMessage>),
    /// blah
    RegisterLocal(NodeId, String, Sender<WsMessage>, Vec<(NodeId, String)>),
    /// blah
    RegisterRemote(NodeId, String, Sender<WsMessage>),
    /// blah
    DownLocal(NodeId),
    /// blah
    DownRemote(NodeId),
    /// blah
    Status(RequestId, Sender<WsMessage>),

    // Raft related
    /// blah
    RaftMsg(RaftMessage),
    /// blah
    AckProposal(ProposalId, bool),
    /// blah
    ForwardProposal(NodeId, ProposalId, EventId, Vec<u8>),
    /// blah
    AddNode(NodeId, Sender<bool>),

    // KV related
    /// blah
    KVGet(Vec<u8>, Sender<WsMessage>),
    /// blah
    KVPut(Vec<u8>, Vec<u8>, Sender<WsMessage>),
    /*
    /// blah
    KvDelete(Vec<u8>, Reply),
    */
}

// TODO this will be using the websocket driver for the tremor network protocol
/// Websocket Message
pub enum WsMessage {
    /// Websocket control message
    Ctrl(CtrlMsg),
    /// Raft core message
    Raft(RaftMessage),
    /// Websocket message reply
    // TODO switch to tremor type
    Reply {
        /// Status code
        code: u16,
        /// Request Id
        rid: RequestId,
        /// Message data
        data: serde_json::Value,
    },
}

#[derive(Serialize, Deserialize, Debug)]
/// Control messages
pub enum CtrlMsg {
    /// Hello message
    Hello(NodeId, String),
    /// Hello ack message
    HelloAck(NodeId, String, Vec<(NodeId, String)>),
    /// Ack proposal mesage
    AckProposal(ProposalId, bool),
    /// Forward proposal mesage
    ForwardProposal(NodeId, ProposalId, EventId, Vec<u8>),
}

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq, Ord, PartialOrd, Clone, Copy)]
/// Request ID
pub struct RequestId(pub u64);

//#[cfg(feature = "json-proto")]
fn decode_ws(bin: &[u8]) -> RaftMessage {
    let msg: crate::raft::message_types::Event = serde_json::from_slice(bin).unwrap();
    msg.into()
}

//#[cfg(not(feature = "json-proto"))]
//fn decode_ws(bin: &[u8]) -> RaftMessage {
//    use protobuf::Message;
//    let mut msg = RaftMessage::default();
//    msg.merge_from_bytes(bin).unwrap();
//    msg
//}

//#[cfg(feature = "json-proto")]
fn encode_ws(msg: RaftMessage) -> Bytes {
    let data: crate::raft::message_types::Event = msg.clone().into();
    let data = serde_json::to_string_pretty(&data);
    data.unwrap().into()
}

//#[cfg(not(feature = "json-proto"))]
//fn encode_ws(msg: RaftMessage) -> Bytes {
//    use protobuf::Message;
//    msg.write_to_bytes().unwrap().into()
//}

type LocalMailboxes = HashMap<NodeId, Sender<WsMessage>>;
type RemoteMailboxes = HashMap<NodeId, Sender<WsMessage>>;

/// Network for cluster communication
pub struct Network {
    // TODO remove pubs here
    /// blah
    pub id: NodeId,
    local_mailboxes: LocalMailboxes,
    remote_mailboxes: RemoteMailboxes,
    /// blah
    pub known_peers: HashMap<NodeId, String>,
    /// blah
    pub endpoint: String,
    /// blah
    pub logger: Logger,
    rx: Receiver<UrMsg>,
    /// blah
    pub tx: Sender<UrMsg>,
    /// blah
    next_eid: u64,
    /// blah
    pending: HashMap<EventId, Sender<WsMessage>>,
    //prot_pending: HashMap<EventId, (RequestId, protocol_driver::HandlerOutboundChannelSender)>,
}

impl Network {
    /// blah
    pub fn new(logger: &Logger, id: NodeId, endpoint: String, peers: Vec<String>) -> Self {
        // exchanges UrMsg
        let (tx, rx) = unbounded();

        // initial peer connections
        for peer in peers {
            let logger = logger.clone();
            // tx is the sender to the websocket node, for each peer (allows comm from the peer)
            // rx is stored as part of the network (receiving from all the peers)
            let tx = tx.clone();
            // client for each of the websocket peer
            task::spawn(client::remote_endpoint(peer, tx, logger));
        }

        // websocket node
        let node = Node {
            tx: tx.clone(),
            id,
            logger: logger.clone(),
        };
        // ws server for the node
        task::spawn(server::run(logger.clone(), node.clone(), endpoint.clone()));

        // uring network
        //let net_handler = crate::protocol::network::Handler::new(tx.clone());
        //let mut net_interceptor = protocol_driver::Interceptor::new(net_handler);
        //task::spawn(net_interceptor.run_loop());

        Self {
            id,
            endpoint,
            tx,
            rx,
            logger: logger.clone(),
            known_peers: HashMap::new(),
            local_mailboxes: HashMap::new(),
            remote_mailboxes: HashMap::new(),
            next_eid: 1,
            pending: HashMap::new(),
            //prot_pending: HashMap::new(),
        }
    }

    fn register_reply(&mut self, reply: Sender<WsMessage>) -> EventId {
        let eid = EventId(self.next_eid);
        self.next_eid += 1;
        self.pending.insert(eid, reply);
        eid
    }
}

#[async_trait]
impl NetworkTrait for Network {
    async fn next(&mut self) -> Option<RaftNetworkMsg> {
        let msg = if let Some(msg) = self.rx.next().await {
            msg
        } else {
            return None;
        };
        match msg {
            UrMsg::InitLocal(endpoint) => {
                info!("Initializing local endpoint (sending hello)");
                endpoint
                    .send(WsMessage::Ctrl(CtrlMsg::Hello(
                        self.id,
                        self.endpoint.clone(),
                    )))
                    .await
                    .unwrap();
                self.next().await
            }
            UrMsg::RegisterLocal(id, peer, endpoint, peers) => {
                if id != self.id {
                    info!(
                        "register(local) remote-id: {} remote-peer: {} discoverd-peers: {:?}",
                        id, peer, peers
                    );
                    self.local_mailboxes.insert(id, endpoint.clone());
                    for (peer_id, peer) in peers {
                        if !self.known_peers.contains_key(&peer_id) {
                            //info!("register(local) not a known peer: {}", peer);
                            self.known_peers.insert(peer_id, peer.clone());
                            let tx = self.tx.clone();
                            let logger = self.logger.clone();
                            task::spawn(client::remote_endpoint(peer, tx, logger));
                        }
                    }
                }
                self.next().await
            }
            UrMsg::RegisterRemote(id, peer, endpoint) => {
                if id != self.id {
                    info!("register(remote) remote-id: {} remote-peer: {}", id, &peer);
                    if !self.known_peers.contains_key(&id) {
                        //info!("register(remote) not a known peer: {}", peer);
                        self.known_peers.insert(id, peer.clone());
                        let tx = self.tx.clone();
                        let logger = self.logger.clone();
                        task::spawn(client::remote_endpoint(peer, tx, logger));
                    }
                    //dbg!(&self.known_peers);
                    endpoint
                        .clone()
                        .send(WsMessage::Ctrl(CtrlMsg::HelloAck(
                            self.id,
                            self.endpoint.clone(),
                            self.known_peers
                                .clone()
                                .into_iter()
                                .collect::<Vec<(NodeId, String)>>(),
                        )))
                        .await
                        .unwrap();
                }
                self.next().await
            }
            UrMsg::DownLocal(id) => {
                warn!("down(local) id: {}", id);
                self.local_mailboxes.remove(&id);
                if !self.remote_mailboxes.contains_key(&id) {
                    self.known_peers.remove(&id);
                }
                self.next().await
            }
            UrMsg::DownRemote(id) => {
                warn!("down(remote) id: {}", id);
                self.remote_mailboxes.remove(&id);
                if !self.local_mailboxes.contains_key(&id) {
                    self.known_peers.remove(&id);
                }
                self.next().await
            }
            UrMsg::Status(_rid, _reply) => {
                //Some(RaftNetworkMsg::Status(rid, reply))
                unimplemented!()
            }
            UrMsg::RaftMsg(msg) => Some(RaftNetworkMsg::RaftMsg(msg)),
            UrMsg::AckProposal(pid, success) => Some(RaftNetworkMsg::AckProposal(pid, success)),
            UrMsg::ForwardProposal(from, pid, eid, data) => {
                Some(RaftNetworkMsg::ForwardProposal(from, pid, eid, data))
            }
            UrMsg::AddNode(id, reply) => Some(RaftNetworkMsg::AddNode(id, reply)),
            UrMsg::KVGet(key, reply) => {
                //let eid = self.register_reply(reply);
                Some(RaftNetworkMsg::KVGet(key, reply))
            }
            UrMsg::KVPut(key, value, reply) => {
                let eid = self.register_reply(reply);
                Some(RaftNetworkMsg::Event(
                    eid,
                    serde_json::to_vec(&(key, value)).unwrap(),
                ))
            }
        }
    }

    async fn send_msg(&mut self, msg: RaftMessage) -> Result<(), NetworkError> {
        let to = NodeId(msg.to);
        if let Some(remote) = self.local_mailboxes.get_mut(&to) {
            remote
                .send(WsMessage::Raft(msg))
                .await
                .map_err(|e| NetworkError::Io(io::Error::new(io::ErrorKind::ConnectionAborted, e)))
        } else if let Some(remote) = self.remote_mailboxes.get_mut(&to) {
            remote
                .send(WsMessage::Raft(msg))
                .await
                .map_err(|e| NetworkError::Io(io::Error::new(io::ErrorKind::ConnectionAborted, e)))
        } else {
            // Err(Error::NotConnected(to)) this is not an error we'll retry
            Ok(())
        }
    }

    async fn ack_proposal(
        &mut self,
        to: NodeId,
        pid: ProposalId,
        success: bool,
    ) -> Result<(), NetworkError> {
        if let Some(remote) = self.local_mailboxes.get_mut(&to) {
            remote
                .send(WsMessage::Ctrl(CtrlMsg::AckProposal(pid, success)))
                .await
                .map_err(|e| NetworkError::Io(io::Error::new(io::ErrorKind::ConnectionAborted, e)))
        } else if let Some(remote) = self.remote_mailboxes.get_mut(&to) {
            remote
                .send(WsMessage::Ctrl(CtrlMsg::AckProposal(pid, success)))
                .await
                .map_err(|e| NetworkError::Io(io::Error::new(io::ErrorKind::ConnectionAborted, e)))
        } else {
            Err(NetworkError::Io(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                format!("send ack proposla to {} fail, let Raft retry it", to),
            )))
        }
    }

    fn connections(&self) -> Vec<NodeId> {
        let mut k1: Vec<NodeId> = self.local_mailboxes.keys().copied().collect();
        let mut k2: Vec<NodeId> = self.remote_mailboxes.keys().copied().collect();
        k1.append(&mut k2);
        k1.sort();
        k1.dedup();
        k1
    }

    async fn forward_proposal(
        &mut self,
        from: NodeId,
        to: NodeId,
        pid: ProposalId,
        eid: EventId,
        data: Vec<u8>,
    ) -> Result<(), NetworkError> {
        let msg = WsMessage::Ctrl(CtrlMsg::ForwardProposal(from, pid, eid, data));
        if let Some(remote) = self.local_mailboxes.get_mut(&to) {
            remote
                .send(msg)
                .await
                .map_err(|e| NetworkError::Generic(format!("{}", e)))
        } else if let Some(remote) = self.remote_mailboxes.get_mut(&to) {
            remote
                .send(msg)
                .await
                .map_err(|e| NetworkError::Generic(format!("{}", e)))
        } else {
            Err(NetworkError::NotConnected(to))
        }
    }

    async fn event_reply(
        &mut self,
        id: EventId,
        code: u16,
        data: Vec<u8>,
    ) -> Result<(), NetworkError> {
        if let Some(sender) = self.pending.remove(&id) {
            let data: serde_json::Value = serde_json::from_slice(&data).unwrap();
            sender
                .send(
                    WsMessage::Reply {
                        code,
                        rid: RequestId(42),
                        data,
                    }
                    .into(),
                )
                .await
                .unwrap()
        //} else if let Some((rid, mut sender)) = self.prot_pending.remove(&id) {
        //    sender
        //        .send(protocol_driver::HandlerOutboundMessage::ok(rid, data))
        //        .await
        //        .unwrap()
        } else {
            error!("Unknown event id {} for reply: {:?}", id, data)
        };

        Ok(())
    }
}
