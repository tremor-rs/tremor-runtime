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

use crate::raft::node::Addr;

pub(crate) use self::manager::Cluster;
use api::client::Error;
use network::raft_network_impl::Network;
use openraft::{
    error::{Fatal, InitializeError, RaftError},
    metrics::WaitError,
    storage::Adaptor,
    Config, ConfigError, Raft, TokioRuntime,
};
use redb::{CommitError, DatabaseError, StorageError, TableError, TransactionError};
use std::{
    fmt::{Display, Formatter},
    io::Cursor,
    sync::Mutex,
};
use store::{TremorRequest, TremorResponse};
use tokio::task::JoinError;

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
#[derive(Debug)]
pub enum ClusterError {
    /// Generic error
    Other(String),
    /// Database error
    Database(DatabaseError),
    /// Transaction error
    Transaction(TransactionError),
    /// Transaction error
    Table(TableError),
    /// StorageError
    Storage(StorageError),
    /// Commit Error
    Commit(CommitError),
    /// IO error
    Io(std::io::Error),
    /// Store error
    Store(store::Error),
    /// Raft error during initialization
    Initialize(RaftError<NodeId, InitializeError<NodeId, Addr>>),
    /// MsgPack encode error
    MsgPackEncode(rmp_serde::encode::Error),
    /// MsgPack decode error
    MsgPackDecode(rmp_serde::decode::Error),
    /// Config error
    Config(ConfigError),
    /// Client error
    Client(Error),
    /// Join error
    JoinError(JoinError),
    /// Fatal error
    Fatal(Fatal<NodeId>),
    /// Wait error
    WaitError(WaitError),
    // TODO: this is a horrible hack
    /// Runtime error
    Runtime(Mutex<crate::Error>),
}

impl From<store::Error> for ClusterError {
    fn from(e: store::Error) -> Self {
        ClusterError::Store(e)
    }
}

impl From<std::io::Error> for ClusterError {
    fn from(e: std::io::Error) -> Self {
        ClusterError::Io(e)
    }
}

impl From<redb::DatabaseError> for ClusterError {
    fn from(e: redb::DatabaseError) -> Self {
        ClusterError::Database(e)
    }
}

impl From<redb::TransactionError> for ClusterError {
    fn from(e: redb::TransactionError) -> Self {
        ClusterError::Transaction(e)
    }
}

impl From<redb::TableError> for ClusterError {
    fn from(e: redb::TableError) -> Self {
        ClusterError::Table(e)
    }
}

impl From<redb::StorageError> for ClusterError {
    fn from(e: redb::StorageError) -> Self {
        ClusterError::Storage(e)
    }
}

impl From<redb::CommitError> for ClusterError {
    fn from(e: redb::CommitError) -> Self {
        ClusterError::Commit(e)
    }
}

impl From<&str> for ClusterError {
    fn from(e: &str) -> Self {
        ClusterError::Other(e.to_string())
    }
}

impl From<String> for ClusterError {
    fn from(e: String) -> Self {
        ClusterError::Other(e)
    }
}

impl From<RaftError<NodeId, InitializeError<NodeId, Addr>>> for ClusterError {
    fn from(e: RaftError<NodeId, InitializeError<NodeId, Addr>>) -> Self {
        ClusterError::Initialize(e)
    }
}

impl From<ConfigError> for ClusterError {
    fn from(e: ConfigError) -> Self {
        ClusterError::Config(e)
    }
}

impl From<Error> for ClusterError {
    fn from(e: Error) -> Self {
        Self::Client(e)
    }
}

impl From<crate::Error> for ClusterError {
    fn from(e: crate::Error) -> Self {
        ClusterError::Runtime(Mutex::new(e))
    }
}

impl From<rmp_serde::encode::Error> for ClusterError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        ClusterError::MsgPackEncode(e)
    }
}

impl From<rmp_serde::decode::Error> for ClusterError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        ClusterError::MsgPackDecode(e)
    }
}

impl From<JoinError> for ClusterError {
    fn from(e: JoinError) -> Self {
        ClusterError::JoinError(e)
    }
}

impl From<Fatal<NodeId>> for ClusterError {
    fn from(e: Fatal<NodeId>) -> Self {
        ClusterError::Fatal(e)
    }
}

impl From<WaitError> for ClusterError {
    fn from(e: WaitError) -> Self {
        ClusterError::WaitError(e)
    }
}

impl Display for ClusterError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterError::Other(e) => e.fmt(f),
            ClusterError::Database(e) => e.fmt(f),
            ClusterError::Transaction(e) => e.fmt(f),
            ClusterError::Table(e) => e.fmt(f),
            ClusterError::Storage(e) => e.fmt(f),
            ClusterError::Commit(e) => e.fmt(f),
            ClusterError::Io(e) => e.fmt(f),
            ClusterError::Store(e) => e.fmt(f),
            ClusterError::Initialize(e) => e.fmt(f),
            ClusterError::Config(e) => e.fmt(f),
            ClusterError::Runtime(e) => write!(f, "{:?}", e.lock()),
            ClusterError::MsgPackEncode(e) => e.fmt(f),
            ClusterError::MsgPackDecode(e) => e.fmt(f),
            ClusterError::Client(e) => e.fmt(f),
            ClusterError::JoinError(e) => e.fmt(f),
            ClusterError::Fatal(e) => e.fmt(f),
            ClusterError::WaitError(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for ClusterError {}

type ClusterResult<T> = Result<T, ClusterError>;

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
