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
use crate::network::ws::{Network, RequestId, WsMessage};
use crate::network::Network as NetworkTrait;
use async_std::task::{self, JoinHandle};
use futures::{select, FutureExt, StreamExt};
use raft::{
    eraftpb::Message as RaftMessage, prelude::*, raw_node::RawNode, storage::MemStorage, StateRole,
};
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

    /// Core raft mesages
    RaftMsg(RaftMessage),
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

// The message can be used to initialize a raft node or not.
fn is_initial_msg(msg: &Message) -> bool {
    let msg_type = msg.get_msg_type();
    msg_type == MessageType::MsgRequestVote
        || msg_type == MessageType::MsgRequestPreVote
        || (msg_type == MessageType::MsgHeartbeat && msg.commit == 0)
}

/// Raft node
pub struct RaftNode {
    logger: Logger,
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
    /// Raft node loop
    pub async fn node_loop(&mut self) {
        let mut ticks = async_std::stream::interval(self.tick_duration);
        let mut i = Instant::now();

        loop {
            select! {
                msg = self.network.next().fuse() => {
                    let msg = if let Some(msg) = msg {
                        msg
                    } else {
                        break;
                    };
                    match msg {
                        RaftNetworkMsg::Status(rid, reply) => {
                            info!("Getting node status");
                            let raft = self.raft_group.as_ref().unwrap();
                            reply
                                .send(WsMessage::Reply {
                                    code: 200,
                                    rid,
                                    data: serde_json::to_value(status(&raft).await.unwrap()).unwrap(),
                                })
                                .await
                                .unwrap();
                        }
                        RaftNetworkMsg::RaftMsg(msg) => {
                            dbg!("Stepping raft!");
                            if let Err(e) = self.step(msg).await {
                                error!("Raft step error: {}", e);
                            }
                        }
                    }
                }
                _tick = ticks.next().fuse() => {
                    if !self.is_running() {
                        continue
                    }
                    if i.elapsed() >= Duration::from_secs(10) {
                        self.log().await;
                        i = Instant::now();
                    }
                    let raft = self.raft_group.as_mut().unwrap();
                    raft.tick();
                    self.on_ready().await.unwrap();
                }
            }
        }
    }

    pub(crate) async fn on_ready(&mut self) -> Result<()> {
        if self.raft_group.as_ref().is_none() {
            return Ok(());
        };

        if !self.raft_group.as_ref().unwrap().has_ready() {
            return Ok(());
        }

        // Get the `Ready` with `RawNode::ready` interface.
        let mut ready = self.raft_group.as_mut().unwrap().ready();

        // Persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
        // raft logs to the latest position.
        if let Err(e) = self.append(ready.entries()).await {
            println!("persist raft log fail: {:?}, need to retry or panic", e);
            return Err(e);
        }

        // Apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
        if *ready.snapshot() != Snapshot::default() {
            let s = ready.snapshot().clone();
            if let Err(e) = self.apply_snapshot(s).await {
                println!("apply snapshot fail: {:?}, need to retry or panic", e);
                return Err(e);
            }
        }

        // Send out the messages come from the node.
        for msg in ready.messages.drain(..) {
            dbg!("Sending raft message....");
            dbg!(&msg);
            self.network.send_msg(msg).await.unwrap()
        }

        /*
        // Apply all committed proposals.
        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in &committed_entries {
                if entry.data.is_empty() {
                    // From new elected leaders.
                    continue;
                }
                if let EntryType::EntryConfChange = entry.get_entry_type() {
                    // For conf change messages, make them effective.
                    let mut cc = ConfChange::default();
                    cc.merge_from_bytes(&entry.data).unwrap();
                    let _node_id = cc.node_id;

                    let cs: ConfState = self
                        .raft_group
                        .as_mut()
                        .unwrap()
                        .try_lock()
                        .unwrap()
                        .apply_conf_change(&cc)
                        .unwrap();
                    self.set_conf_state(cs).await?;
                } else {
                    // For normal proposals, extract the key-value pair and then
                    // insert them into the kv engine.
                    if let Ok(event) = serde_json::from_slice::<Event>(&entry.data) {
                        if let Some(service) = self.services.get_mut(&event.sid) {
                            // let _store = &self
                            //     .raft_group
                            //     .as_ref()
                            //     .unwrap()
                            //     .lock()
                            //     .await
                            //     .raft
                            //     .raft_log
                            //     .store;
                            let (code, value) = service
                                .execute(
                                    self.raft_group.as_ref().unwrap(),
                                    &mut self.pubsub,
                                    event.data,
                                )
                                .await
                                .unwrap();
                            if event.nid == Some(self.id) {
                                self.network
                                    .event_reply(event.eid, code, value)
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                }
                if self.raft_group.as_ref().unwrap().raft.state == StateRole::Leader {
                    // The leader should response to the clients, tell them if their proposals
                    // succeeded or not.
                    if let Some(proposal) = self.proposals.pop_front() {
                        if proposal.proposer == self.id {
                            info!(self.logger, "Handling proposal(local)"; "proposal-id" => proposal.id);
                            self.pending_proposals.remove(&proposal.id);
                        } else {
                            info!(self.logger, "Handling proposal(remote)"; "proposal-id" => proposal.id, "proposer" => proposal.proposer);
                            self.network
                                .ack_proposal(proposal.proposer, proposal.id, true)
                                .await
                                .map_err(|e| {
                                    Error::Io(IoError::new(
                                        IoErrorKind::ConnectionAborted,
                                        format!("{}", e),
                                    ))
                                })?;
                        }
                    }
                }
            }
            if let Some(last_committed) = committed_entries.last() {
                self.set_hard_state(last_committed.index, last_committed.term)
                    .await?;
            }
        }
        */
        // Call `RawNode::advance` interface to update position flags in the raft.
        self.raft_group.as_mut().unwrap().advance(ready);
        Ok(())
    }

    /// Initialize raft for followers.
    pub async fn initialize_raft_from_message(&mut self, msg: &Message) {
        if !is_initial_msg(msg) {
            return;
        }
        let mut cfg = example_config();
        cfg.id = msg.to;
        let storage = MemStorage::new();
        self.raft_group = Some(RawNode::new(&cfg, storage, &self.logger).unwrap());
    }

    /// Step a raft message, initialize the raft if need.
    pub async fn step(&mut self, msg: Message) -> raft::Result<()> {
        if self.raft_group.is_none() {
            if is_initial_msg(&msg) {
                self.initialize_raft_from_message(&msg).await;
            } else {
                return Ok(());
            }
        }
        let raft_group = self.raft_group.as_mut().unwrap();
        raft_group.step(msg)
    }

    async fn append(&self, _entries: &[Entry]) -> Result<()> {
        //let raft_node = self.raft_group.as_ref().unwrap();
        //raft_node.store().append(entries).await
        dbg!("Dummy raft append for now");
        Ok(())
    }

    async fn apply_snapshot(&mut self, _snapshot: Snapshot) -> Result<()> {
        //let mut raft_node = self.raft_group.as_ref().unwrap();
        //raft_node.mut_store().apply_snapshot(snapshot).await
        dbg!("Dummy raft apply snapshot for now");
        Ok(())
    }

    /// blah
    pub async fn log(&self) {
        if let Some(g) = self.raft_group.as_ref() {
            let role = self.role();
            let raft = &g.raft;
            /*
            info!(
                self.logger,
                "NODE STATE";
                "node-id" => &raft.id,
                "role" => format!("{:?}", role),

                "leader-id" => raft.leader_id,
                "term" => raft.term,
                "first-index" => raft.raft_log.store.first_index().unwrap_or(0),
                "last-index" => raft.raft_log.store.last_index().unwrap_or(0),

                "vote" => raft.vote,
                "votes" => format!("{:?}", raft.prs().votes()),

                "voters" => format!("{:?}", raft.prs().conf().voters()),

                "promotable" => raft.promotable(),
                "pass-election-timeout" => raft.pass_election_timeout(),
                "election-elapsed" => raft.election_elapsed,
                "randomized-election-timeout" => raft.randomized_election_timeout(),

                "connections" => format!("{:?}", &self.network.connections()),
            )
            */
            warn!(
                "NODE STATE node-id: {} role: {:?} leader-id: {} term: {}",
                &raft.id, role, raft.leader_id, raft.term
            );
        } else {
            error!("UNINITIALIZED NODE {}", self.id)
        }
    }

    /// blah
    pub fn is_running(&self) -> bool {
        self.raft_group.is_some()
    }

    /// blah
    pub fn role(&self) -> StateRole {
        self.raft_group
            .as_ref()
            .map(|g| g.raft.state)
            .unwrap_or(StateRole::PreCandidate)
    }

    /// Set raft tick duration
    pub fn set_raft_tick_duration(&mut self, d: Duration) {
        self.tick_duration = d;
    }

    /// Create a raft leader
    pub async fn create_raft_leader(logger: &Logger, id: NodeId, network: Network) -> Self {
        let mut config = example_config();
        config.id = id.0;
        config.validate().unwrap();

        /*
        let storage = MemStorage::new_with_conf_state((vec![id.0], vec![])); // leader
        //let storage = MemStorage::new();
        let raw_node = RawNode::new(&config, storage, logger).unwrap();

        // testing for now
        //raw_node.raft.become_candidate();
        //raw_node.raft.become_leader();

        let raft_group = Some(raw_node);
        */

        let mut s = Snapshot::default();
        // Because we don't use the same configuration to initialize every node, so we use
        // a non-zero index to force new followers catch up logs by snapshot first, which will
        // bring all nodes to the same initial state.
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![1];
        let storage = MemStorage::new();
        storage.wl().apply_snapshot(s).unwrap();
        let raft_group = Some(RawNode::new(&config, storage, logger).unwrap());

        Self {
            logger: logger.clone(),
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
        //let storage = MemStorage::new_with_conf_state((vec![id.0], vec![]));
        let storage = MemStorage::new();
        let raft_group = if storage.last_index().unwrap() == 1 {
            None
        } else {
            let mut cfg = example_config();
            cfg.id = id.0;
            Some(RawNode::new(&cfg, storage, logger).unwrap())
        };

        Self {
            logger: logger.clone(),
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
    network: Network,
) -> JoinHandle<()> {
    let mut node = if bootstrap {
        dbg!("bootstrap on");
        RaftNode::create_raft_leader(&logger, id, network).await
    } else {
        RaftNode::create_raft_follower(&logger, id, network).await
    };
    dbg!(&node.id);
    dbg!(&node.last_state);

    node.set_raft_tick_duration(Duration::from_millis(100));
    node.log().await;

    task::spawn(async move { node.node_loop().await })
}
