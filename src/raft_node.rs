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
use raft::{prelude::*, raw_node::RawNode, storage::MemStorage};
use slog::Logger;
use std::fmt;
use std::time::{Duration, Instant};

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

async fn status<Storage>(node: &raft::RawNode<Storage>) -> Result<RaftNodeStatus>
where
    Storage: raft::storage::Storage,
{
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

/// Raft start function
pub async fn start_raft(
    id: NodeId,
    _bootstrap: bool,
    logger: Logger,
    // TODO this will be the network
    mut rx: async_channel::Receiver<RaftNetworkMsg>,
) -> JoinHandle<()> {
    let mut config = example_config();
    config.id = id.0;
    config.validate().unwrap();

    // FIXME use rocksdb
    let storage = MemStorage::new_with_conf_state((vec![1], vec![]));
    let mut node = RawNode::new(&config, storage, &logger).unwrap();

    // single node cluster for now
    node.raft.become_candidate();
    node.raft.become_leader();

    // node loop
    task::spawn(async move {
        let duration = Duration::from_millis(100);
        let mut ticks = async_std::stream::interval(duration);
        let mut i = Instant::now();

        loop {
            select! {
                msg = rx.next().fuse() => {
                    let msg = if let Some(msg) = msg {
                        msg
                    } else {
                        break;
                    };
                    match msg {
                        RaftNetworkMsg::Status(rid, reply) => {
                            info!("Getting node status");
                            reply
                                .send(WsMessage::Reply {
                                    code: 200,
                                    rid,
                                    data: serde_json::to_value(status(&node).await.unwrap()).unwrap(),
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

                    node.tick();
                }
            }
        }
    })
}
