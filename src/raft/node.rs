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
use crate::temp_network::ws::{Network, RequestId, WsMessage};
use crate::temp_network::Network as NetworkTrait;
use async_std::task::{self, JoinHandle};
use futures::{select, FutureExt, StreamExt};
use halfbrown::HashMap;
use protobuf::Message as PBMessage;
use raft::{
    eraftpb::Message as RaftMessage, prelude::*, raw_node::RawNode, storage::MemStorage, StateRole,
};
use slog::Logger;
use std::collections::VecDeque;
use std::fmt;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::time::{Duration, Instant};

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq, Ord, PartialOrd, Clone, Copy)]
/// Id for a node in the Tremor raft cluster
pub struct NodeId(pub u64);
impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node({:?})", self)
    }
}

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq, Ord, PartialOrd, Clone, Copy)]
/// Proposal Id
pub struct ProposalId(pub u64);
impl fmt::Display for ProposalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Proposal({:?})", self)
    }
}

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq, Ord, PartialOrd, Clone, Copy)]
/// Event Id
pub struct EventId(pub u64);
impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Event({:?})", self)
    }
}

/// blah
#[derive(Deserialize, Serialize, Debug)]
pub struct Event {
    nid: Option<NodeId>,
    eid: EventId,
    //sid: ServiceId,
    data: Vec<u8>,
}

// TODO this will be under the tremor network protocol
/// Raft Network Message
pub enum RaftNetworkMsg {
    /// Status requests
    Status(RequestId, async_channel::Sender<WsMessage>),

    /// Core raft mesages
    RaftMsg(RaftMessage),

    /// blah
    AckProposal(ProposalId, bool),
    /// blah
    ForwardProposal(NodeId, ProposalId, EventId, Vec<u8>),
    /// blah
    AddNode(NodeId, async_channel::Sender<bool>),

    /// blah
    KVGet(Vec<u8>, async_channel::Sender<WsMessage>),
    //KVPut(Vec<u8>, Vec<u8>, async_channel::Sender<WsMessage>),
    /// blah
    Event(EventId, Vec<u8>),
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

pub(crate) fn propose_and_check_failed_proposal(
    raft_group: &mut RawNode<MemStorage>,
    proposal: &mut Proposal,
) -> raft::Result<bool> {
    let last_index1 = raft_group.raft.raft_log.last_index() + 1;
    if let Some(ref event) = proposal.normal {
        let data = serde_json::to_vec(&event).unwrap();
        raft_group.propose(vec![], data)?;
    } else if let Some(ref cc) = proposal.conf_change {
        raft_group.propose_conf_change(vec![], cc.clone())?;
    } else if let Some(_transferee) = proposal.transfer_leader {
        // TODO 0.8
        // TODO: implement transfer leader.
        unimplemented!();
    } else {
    }

    let last_index2 = raft_group.raft.raft_log.last_index() + 1;
    if last_index2 == last_index1 {
        // Propose failed, don't forget to respond to the client.
        Ok(true)
    } else {
        proposal.proposed = last_index1;
        Ok(false)
    }
}

/// blah
pub struct Proposal {
    id: ProposalId,
    proposer: NodeId, // node id of the proposer
    normal: Option<Event>,
    conf_change: Option<ConfChange>, // conf change.
    transfer_leader: Option<u64>,
    // If it's proposed, it will be set to the index of the entry.
    pub(crate) proposed: u64,
}

impl Proposal {
    /// blah
    pub fn conf_change(id: ProposalId, proposer: NodeId, cc: &ConfChange) -> Self {
        Self {
            id,
            proposer,
            normal: None,
            conf_change: Some(cc.clone()),
            transfer_leader: None,
            proposed: 0,
        }
    }
    #[allow(dead_code)]
    /// blah
    pub fn normal(
        id: ProposalId,
        proposer: NodeId,
        eid: EventId,
        //sid: ServiceId,
        data: Vec<u8>,
    ) -> Self {
        Self {
            id,
            proposer,
            normal: Some(Event {
                nid: Some(proposer),
                eid,
                //sid,
                data,
            }),
            conf_change: None,
            transfer_leader: None,
            proposed: 0,
        }
    }
}

/// Raft node
pub struct RaftNode {
    logger: Logger,
    // None if the raft is not initialized.
    id: NodeId,
    // FIXME swap out MemStorage
    /// FIXME
    pub raft_group: Option<RawNode<MemStorage>>,
    // TODO this will be part of storage. here right now since
    // MemStorage only contains raft logs
    kv_storage: HashMap<String, String>,
    network: Network,
    proposals: VecDeque<Proposal>,
    pending_proposals: HashMap<ProposalId, Proposal>,
    pending_acks: HashMap<ProposalId, EventId>,
    proposal_id: u64,
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
                            //dbg!("Stepping raft!");
                            if let Err(e) = self.step(msg).await {
                                error!("Raft step error: {}", e);
                            }
                        }
                        RaftNetworkMsg::AckProposal(pid, success) => {
                            info!("proposal acknowledged. pid: {}", pid);
                            if let Some(proposal) = self.pending_proposals.remove(&pid) {
                                if !success {
                                    self.proposals.push_back(proposal)
                                }
                            }
                            if let Some(_eid) = self.pending_acks.remove(&pid) {
                                //self.network.event_reply(eid, Some(vec![skilled])).await.unwrap();
                            }
                        }
                        RaftNetworkMsg::ForwardProposal(from, pid, eid, data) => {
                            if let Err(e) = self.propose_event(from, pid, eid, data).await {
                                error!("Proposal forward error: {}", e);
                            }
                        }
                        RaftNetworkMsg::AddNode(id, reply) => {
                            info!("Adding node id: {}", id);
                            reply.send(self.add_node(id).await).await.unwrap();
                        }
                        RaftNetworkMsg::KVGet(key, reply) => {
                            //info!("GET KV {:?}", key);
                            //let raft = self.raft_group.as_ref().unwrap();
                            //let storage = raft.store();
                            //let scope: u16 = 0;
                            // temp thing for memstorage
                            let message = if let Some(s) = self.kv_storage
                                .get(&String::from_utf8(key).unwrap())
                            {
                                WsMessage::Reply {
                                    code: 200,
                                    // dummy
                                    rid: RequestId(42),
                                    data: serde_json::to_value(&s).unwrap(),
                                }
                            } else {
                                WsMessage::Reply {
                                    code: 400,
                                    // dummy
                                    rid: RequestId(42),
                                    data: serde_json::to_value("not found").unwrap(),
                                }
                            };
                            reply
                                .send(message)
                                .await
                                .unwrap();
                        }
                        //RaftNetworkMsg::KVPut(key, value, reply) => {
                        RaftNetworkMsg::Event(eid, data) => {
                            let pid = self.next_pid();
                            let from = self.id;
                            if let Err(e) = self.propose_event(from, pid, eid, data).await {
                                error!("Post forward error: {}", e);
                                self.network.event_reply(eid, 500u16, serde_json::to_vec(&format!("{}", e)).unwrap()).await.unwrap();
                            } else {
                                self.pending_acks.insert(pid, eid);
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
                    if self.is_leader() {
                        // Handle new proposals.
                        self.propose_all().await.unwrap();
                    }
                }
            }
        }
    }

    pub(crate) async fn propose_all(&mut self) -> raft::Result<()> {
        let raft_group = self.raft_group.as_mut().unwrap();
        let mut pending = Vec::new();
        for p in self.proposals.iter_mut().skip_while(|p| p.proposed > 0) {
            if propose_and_check_failed_proposal(&mut *raft_group, p)? {
                // if propose_and_check_failed_proposal(&mut *raft_group.lock().await, p)? {
                if p.proposer == self.id {
                    if let Some(prop) = self.pending_proposals.remove(&p.id) {
                        pending.push(prop);
                    }
                } else {
                    self.network
                        .ack_proposal(p.proposer, p.id, false)
                        .await
                        .map_err(|e| {
                            raft::Error::Io(IoError::new(
                                IoErrorKind::ConnectionAborted,
                                format!("{}", e),
                            ))
                        })?;
                }
            }
        }
        for p in pending.drain(..) {
            self.proposals.push_back(p)
        }
        Ok(())
    }

    /// blah
    pub async fn propose_event(
        &mut self,
        from: NodeId,
        pid: ProposalId,
        eid: EventId,
        data: Vec<u8>,
    ) -> raft::Result<()> {
        if self.is_leader() {
            self.proposals
                .push_back(Proposal::normal(pid, from, eid, data));
            Ok(())
        } else {
            self.network
                .forward_proposal(from, self.leader(), pid, eid, data)
                .await
                .map_err(|e| {
                    raft::Error::Io(IoError::new(
                        IoErrorKind::ConnectionAborted,
                        format!("{}", e),
                    ))
                })
        }
    }

    pub(crate) async fn add_node(&mut self, id: NodeId) -> bool {
        //dbg!("add_node fn called");
        if self.is_leader() && !self.node_known(id).await {
            let mut conf_change = ConfChange::default();
            conf_change.node_id = id.0;
            conf_change.set_change_type(ConfChangeType::AddNode);
            let pid = self.next_pid();
            let proposal = Proposal::conf_change(pid, self.id, &conf_change);

            self.proposals.push_back(proposal);
            true
        } else {
            false
        }
    }

    pub(crate) async fn on_ready(&mut self) -> raft::Result<()> {
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
            //dbg!("Sending raft message....");
            //dbg!(&msg);
            self.network.send_msg(msg).await.unwrap()
        }

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
                        .apply_conf_change(&cc)
                        .unwrap();
                    self.set_conf_state(cs).await;
                } else {
                    // For normal proposals, extract the key-value pair and then
                    // insert them into the kv engine (only supported service right now).
                    if let Ok(event) = serde_json::from_slice::<Event>(&entry.data) {
                        let (key, value): (Vec<u8>, Vec<u8>) =
                            serde_json::from_slice(&event.data).unwrap();
                        //dbg!(String::from_utf8(key.clone()).unwrap());
                        //dbg!(String::from_utf8(value.clone()).unwrap());
                        let result = self.kv_storage.insert(
                            String::from_utf8(key).unwrap(),
                            String::from_utf8(value).unwrap(),
                        );
                        if event.nid == Some(self.id) {
                            let reply_data = if let Some(old) = result {
                                serde_json::to_vec(&old).unwrap()
                            } else {
                                serde_json::to_vec(&serde_json::Value::Null).unwrap()
                            };
                            self.network
                                .event_reply(event.eid, 201u16, reply_data)
                                .await
                                .unwrap();
                        }
                    }
                }
                if self.raft_group.as_ref().unwrap().raft.state == StateRole::Leader {
                    // The leader should response to the clients, tell them if their proposals
                    // succeeded or not.
                    if let Some(proposal) = self.proposals.pop_front() {
                        if proposal.proposer == self.id {
                            info!("Handling proposal(local) proposal-id {}", proposal.id);
                            self.pending_proposals.remove(&proposal.id);
                        } else {
                            info!(
                                "Handling proposal(remote) proposal-id {} proposer: {}",
                                proposal.id, proposal.proposer
                            );
                            self.network
                                .ack_proposal(proposal.proposer, proposal.id, true)
                                .await
                                .map_err(|e| {
                                    raft::Error::Io(IoError::new(
                                        IoErrorKind::ConnectionAborted,
                                        format!("{}", e),
                                    ))
                                })
                                .unwrap();
                        }
                    }
                }
            }
            if let Some(last_committed) = committed_entries.last() {
                self.set_hard_state(last_committed.index, last_committed.term)
                    .await?;
            }
        }

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

    async fn append(&self, entries: &[Entry]) -> raft::Result<()> {
        //dbg!("Calling raft append");
        let raft_node = self.raft_group.as_ref().unwrap();
        raft_node.store().wl().append(entries)
    }

    async fn apply_snapshot(&mut self, snapshot: Snapshot) -> raft::Result<()> {
        dbg!("Calling raft apply snapshot");
        let raft_node = self.raft_group.as_ref().unwrap();
        raft_node.store().wl().apply_snapshot(snapshot)
    }

    async fn set_conf_state(&mut self, cs: ConfState) {
        //dbg!("Calling set conf state");
        let raft_node = self.raft_group.as_ref().unwrap();
        raft_node.store().wl().set_conf_state(cs)
    }

    async fn set_hard_state(&mut self, _commit: u64, _term: u64) -> raft::Result<()> {
        //dbg!("Calling set hard state");
        //let raft_node = self.raft_group.as_ref().unwrap();
        //let mut s = raft_node.store().wl();
        //s.mut_hard_state().commit = commit;
        //s.mut_hard_state().term = term;
        Ok(())
    }

    /// blah
    pub fn next_pid(&mut self) -> ProposalId {
        let pid = self.proposal_id;
        self.proposal_id += 1;
        ProposalId(pid)
    }

    /// blah
    pub async fn node_known(&self, id: NodeId) -> bool {
        if let Some(ref g) = self.raft_group {
            g.raft.prs().conf().voters().contains(id.0)
        } else {
            false
        }
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
            info!(
                "NODE STATE node-id: {} role: {:?} leader-id: {} term: {} voters: {:?} connections: {:?}",
                &raft.id, role, raft.leader_id, raft.term, raft.prs().conf().voters(), &self.network.connections()
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

    /// blah
    pub fn is_leader(&self) -> bool {
        self.raft_group
            .as_ref()
            .map(|g| g.raft.state == StateRole::Leader)
            .unwrap_or_default()
    }

    /// Set raft tick duration
    pub fn set_raft_tick_duration(&mut self, d: Duration) {
        self.tick_duration = d;
    }

    /// blah
    pub fn leader(&self) -> NodeId {
        NodeId(
            self.raft_group
                .as_ref()
                .map(|g| g.raft.leader_id)
                .unwrap_or_default(),
        )
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
            proposals: VecDeque::new(),
            pending_proposals: HashMap::new(),
            pending_acks: HashMap::new(),
            proposal_id: 0,
            tick_duration: Duration::from_millis(100),
            //services: HashMap::new(),
            last_state: StateRole::PreCandidate,
            kv_storage: HashMap::new(),
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
            proposals: VecDeque::new(),
            pending_proposals: HashMap::new(),
            pending_acks: HashMap::new(),
            proposal_id: 0,
            tick_duration: Duration::from_millis(100),
            //services: HashMap::new(),
            last_state: StateRole::PreCandidate,
            kv_storage: HashMap::new(),
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
