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

use crate::errors::Result;
pub use crate::network::ws::{RequestId, WsMessage};
use async_std::task::{self, JoinHandle};
use futures::{select, FutureExt, StreamExt};
use raft::{prelude::*, raw_node::RawNode, storage::MemStorage, StateRole};
use slog::Logger;
use std::fmt;
use std::time::{Duration, Instant};

// FIXME use Network type from network crate and eliminate this
type Network = async_channel::Receiver<RaftNetworkMsg>;

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq, Ord, PartialOrd, Clone, Copy)]
/// Id for a node in the Tremor raft cluster
pub struct NodeId(pub u64);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node({:?})", self)
    }
}

// TODO this will be under the tremor network protocol
/// Raft Network Message
pub enum RaftNetworkMsg {
    /// Status requests
    Status(RequestId, async_channel::Sender<WsMessage>),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct RaftNodeStatus {
    id: u64,
    role: String,
    promotable: bool,
    pass_election_timeout: bool,
    election_elapsed: usize,
    randomized_election_timeout: usize,
    term: u64,
    last_index: u64,
}

fn example_config() -> Config {
    Config {
        election_tick: 10,
        heartbeat_tick: 3,
        pre_vote: true,
        ..Default::default()
    }
}

async fn status(node: &raft::RawNode<MemStorage>) -> Result<RaftNodeStatus> {
    Ok(RaftNodeStatus {
        id: node.raft.id,
        role: format!("{:?}", node.raft.state),
        promotable: node.raft.promotable(),
        pass_election_timeout: node.raft.pass_election_timeout(),
        election_elapsed: node.raft.election_elapsed,
        randomized_election_timeout: node.raft.randomized_election_timeout(),
        term: node.raft.term,
        last_index: node.raft.raft_log.store.last_index().unwrap_or(0),
    })
}

/// Raft node
pub struct RaftNode {
    //logger: Logger,
    // None if the raft is not initialized.
    id: NodeId,
    // FIXME swap out MemStorage
    /// FIXME
    pub raft_group: Option<RawNode<MemStorage>>,
    network: Network,
    //proposals: VecDeque<Proposal>,
    //pending_proposals: HashMap<ProposalId, Proposal>,
    //pending_acks: HashMap<ProposalId, EventId>,
    //proposal_id: u64,
    tick_duration: Duration,
    //services: HashMap<ServiceId, Box<dyn Service<Storage>>>,
    last_state: StateRole,
}

impl RaftNode {
    /// Set raft tick duration
    pub fn set_raft_tick_duration(&mut self, d: Duration) {
        self.tick_duration = d;
    }

    /// Create a raft leader
    pub async fn create_raft_leader(logger: &Logger, id: NodeId, network: Network) -> Self {
        let mut config = example_config();
        config.id = id.0;
        config.validate().unwrap();

        let storage = MemStorage::new_with_conf_state((vec![id.0], vec![]));
        let mut raw_node = RawNode::new(&config, storage, logger).unwrap();

        // testing for now
        raw_node.raft.become_candidate();
        raw_node.raft.become_leader();

        let raft_group = Some(raw_node);

        Self {
            //logger: logger.clone(),
            id,
            raft_group,
            network,
            //proposals: VecDeque::new(),
            //pending_proposals: HashMap::new(),
            //pending_acks: HashMap::new(),
            //proposal_id: 0,
            tick_duration: Duration::from_millis(100),
            //services: HashMap::new(),
            last_state: StateRole::PreCandidate,
        }
    }

    /// Create a raft follower.
    pub async fn create_raft_follower(logger: &Logger, id: NodeId, network: Network) -> Self {
        /*
        let storage = Storage::new(id).await;
        let raft_group = if storage.last_index().unwrap() == 1 {
            None
        } else {
            let mut cfg = example_config();
            cfg.id = id.0;
            Some(RawNode::new(&cfg, storage, logger).unwrap())
        };
        */

        let mut config = example_config();
        config.id = id.0;
        config.validate().unwrap();

        let storage = MemStorage::new_with_conf_state((vec![id.0], vec![]));
        let mut raw_node = RawNode::new(&config, storage, logger).unwrap();

        // FIXME testing values for now
        raw_node.raft.become_candidate();
        raw_node.raft.become_follower(1, 1);

        let raft_group = Some(raw_node);

        Self {
            //logger: logger.clone(),
            id,
            raft_group,
            network,
            //proposals: VecDeque::new(),
            //pending_proposals: HashMap::new(),
            //pending_acks: HashMap::new(),
            //proposal_id: 0,
            tick_duration: Duration::from_millis(100),
            //services: HashMap::new(),
            last_state: StateRole::PreCandidate,
        }
    }
}

/// Raft start function
pub async fn start_raft(
    id: NodeId,
    bootstrap: bool,
    logger: Logger,
    mut network: Network,
) -> JoinHandle<()> {
    let mut node = if bootstrap {
        dbg!("bootstrap on");
        RaftNode::create_raft_leader(&logger, id, network.clone()).await
    } else {
        RaftNode::create_raft_follower(&logger, id, network.clone()).await
    };
    dbg!(&node.id);
    dbg!(&node.last_state);
    dbg!(&node.network);

    // node loop
    task::spawn(async move {
        let duration = Duration::from_millis(100);
        let mut ticks = async_std::stream::interval(duration);
        let mut i = Instant::now();

        loop {
            select! {
                msg = network.next().fuse() => {
                    let msg = if let Some(msg) = msg {
                        msg
                    } else {
                        break;
                    };
                    let raft = node.raft_group.as_ref().unwrap();
                    match msg {
                        RaftNetworkMsg::Status(rid, reply) => {
                            info!("Getting node status");
                            reply
                                .send(WsMessage::Reply {
                                    code: 200,
                                    rid,
                                    data: serde_json::to_value(status(&raft).await.unwrap()).unwrap(),
                                })
                                .await
                                .unwrap();
                        }
                    }
                }
                _tick = ticks.next().fuse() => {
                    if i.elapsed() >= Duration::from_secs(10) {
                        i = Instant::now();
                    }
                    let raft = node.raft_group.as_mut().unwrap();
                    raft.tick();
                }
            }
        }
    })
}
