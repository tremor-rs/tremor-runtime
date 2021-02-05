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

use crate::raft_node::{NodeId, RaftNetworkMsg};
use async_channel::{unbounded, Receiver, Sender};
use async_std::task;
//use futures::future::{BoxFuture, FutureExt};
use futures::StreamExt;
use halfbrown::HashMap;
use slog::Logger;

#[derive(Clone)]
pub(crate) struct Node {
    id: NodeId,
    tx: Sender<UrMsg>,
    logger: Logger,
}

/// blah
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
}

// TODO this will be using the websocket driver for the tremor network protocol
/// Websocket Message
pub enum WsMessage {
    /// Websocket control message
    Ctrl(CtrlMsg),
    //Raft(RaftMessage),
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
    //AckProposal(ProposalId, bool),
    //ForwardProposal(NodeId, ProposalId, ServiceId, EventId, Vec<u8>),
}

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq, Ord, PartialOrd, Clone, Copy)]
/// Request ID
pub struct RequestId(pub u64);

//#[cfg(feature = "json-proto")]
//fn decode_ws(bin: &[u8]) -> RaftMessage {
//    let msg: crate::codec::json::Event = serde_json::from_slice(bin).unwrap();
//    msg.into()
//}

//#[cfg(not(feature = "json-proto"))]
//fn decode_ws(bin: &[u8]) -> RaftMessage {
//    use protobuf::Message;
//    let mut msg = RaftMessage::default();
//    msg.merge_from_bytes(bin).unwrap();
//    msg
//}

/// Network for cluster communication
pub struct Network {
    // TODO remove pubs here
    /// blah
    pub id: NodeId,
    //local_mailboxes: LocalMailboxes,
    //remote_mailboxes: RemoteMailboxes,
    /// blah
    pub known_peers: HashMap<NodeId, String>,
    /// blah
    pub endpoint: String,
    /// blah
    pub logger: Logger,
    rx: Receiver<UrMsg>,
    /// blah
    pub tx: Sender<UrMsg>,
    //next_eid: u64,
    //pending: HashMap<EventId, Reply>,
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
            //local_mailboxes: HashMap::new(),
            //remote_mailboxes: HashMap::new(),
            //next_eid: 1,
            //pending: HashMap::new(),
            //prot_pending: HashMap::new(),
        }
    }

    /// blah
    //pub async fn next(&mut self) -> BoxFuture<'static, Option<RaftNetworkMsg>> {
    pub async fn next(&mut self) -> Option<RaftNetworkMsg> {
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
                //self.next().await.boxed()
                //self.next().await
                // FIXME
                None
            }
            UrMsg::RegisterRemote(id, peer, _endpoint) => {
                info!("register(remote) remote-id: {} remote-peer: {}", id, &peer);
                // FIXME send hello-ack from here
                None
            }
            UrMsg::Status(rid, reply) => Some(RaftNetworkMsg::Status(rid, reply)),
            _ => unimplemented!(),
        }
    }
}
