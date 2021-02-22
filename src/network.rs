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

/// Websocket-based network
pub mod ws;

use crate::raft_node::{EventId, NodeId, ProposalId, RaftNetworkMsg};
use async_trait::async_trait;
use raft::eraftpb::Message as RaftMessage;
use std::{fmt, io};

/// blah
#[async_trait]
pub trait Network: Send + Sync {
    /// blah
    async fn next(&mut self) -> Option<RaftNetworkMsg>;
    /// blah
    async fn send_msg(&mut self, msg: RaftMessage) -> Result<(), Error>;
    /// blah
    fn connections(&self) -> Vec<NodeId>;
    /// blah
    async fn ack_proposal(
        &mut self,
        to: NodeId,
        pid: ProposalId,
        success: bool,
    ) -> Result<(), Error>;
    /// blah
    async fn forward_proposal(
        &mut self,
        from: NodeId,
        to: NodeId,
        pid: ProposalId,
        eid: EventId,
        data: Vec<u8>,
    ) -> Result<(), Error>;
    /// blah
    async fn event_reply(&mut self, id: EventId, code: u16, reply: Vec<u8>) -> Result<(), Error>;
}

/// blah
#[derive(Debug)]
pub enum Error {
    /// blah
    Io(io::Error),
    /// blah
    Generic(String),
    /// blah
    NotConnected(NodeId),
}
impl std::error::Error for Error {}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
