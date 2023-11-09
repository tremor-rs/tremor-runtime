// Copyright 2023, The Tremor Team
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

/// API code
pub mod api;
/// Code for interacting with tremor archives
pub mod archive;
/// Code for interacting with tremor clusters
pub mod manager;
/// Code cluster networking
pub mod network;
/// Code for interacting with tremor nodes
pub mod node;
/// Code for interacting with rocksdb
pub mod store;

#[cfg(test)]
mod test;

pub(crate) use self::manager::Cluster;
use network::raft_network_impl::Network;
use openraft::{storage::Adaptor, AnyError, Config, Raft, TokioRuntime};
use std::io::Cursor;
use store::{TremorRequest, TremorResponse};

/// A `NodeId`
pub type NodeId = u64;

/// load a default raft config
/// # Errors
/// When the config isn't valid
pub fn config() -> ClusterResult<Config> {
    let config = Config {
        ..Default::default()
    };
    Ok(config.validate()?)
}

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TremorRaftConfig: D = TremorRequest, R = TremorResponse, NodeId = NodeId, Node = node::Addr,
    Entry = openraft::Entry<TremorRaftConfig>,
    SnapshotData = Cursor<Vec<u8>>,
    AsyncRuntime = TokioRuntime
);

pub(crate) type LogStore = Adaptor<TremorRaftConfig, store::Store>;
pub(crate) type StateMachineStore = Adaptor<TremorRaftConfig, store::Store>;
pub(crate) type TremorRaftImpl = Raft<TremorRaftConfig, Network, LogStore, StateMachineStore>;

/// A raft cluster error
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    /// The raft node isn't running
    #[error("The raft node isn't running")]
    RaftNotRunning,
    /// Bad bind address
    #[error("Bad bind address")]
    BadAddr,
    /// Error shutting down local raft node
    #[error("Error shutting down local raft node")]
    Shutdown,
    /// No join endpoints provided
    #[error("No join endpoints provided")]
    NoEndpoints,
}

type ClusterResult<T> = crate::Result<T>;

/// We need this since openraft and anyhow hate eachother
/// anyhow refuses to implement `std::error::Error` on it's error type
/// and openraft requires the error type to implement `std::error::Error`
/// so instead of forking we're doing the silly dance
#[derive(Debug)]
pub(crate) struct SillyError(anyhow::Error);
impl SillyError {
    /// Create a new silly error
    pub fn err(e: impl Into<anyhow::Error>) -> AnyError {
        AnyError::new(&Self(e.into()))
    }
}

impl std::fmt::Display for SillyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for SillyError {}

/// Removes a node from a cluster
/// # Errors
/// When the node can't be removed
pub async fn remove_node<T: ToString + ?Sized>(
    node_id: NodeId,
    api_addr: &T,
) -> Result<(), crate::errors::Error> {
    let client = api::client::Tremor::new(api_addr)?;
    client.demote_voter(&node_id).await?;
    client.remove_learner(&node_id).await?;
    client.remove_node(&node_id).await?;
    println!("Membership updated: node {node_id} removed.");
    Ok(())
}
