pub mod api;
pub mod archive;
pub mod network;
pub mod node;
pub mod store;

#[cfg(test)]
mod test;

use api::client::Error;
use network::raft_network_impl::Network;
pub use openraft::NodeId;
use openraft::{error::InitializeError, Config, ConfigError, Raft};
use std::{
    fmt::{Display, Formatter},
    sync::Mutex,
};
use store::{TremorRequest, TremorResponse};
use tokio::task::JoinError;

/// load a default raft config
/// # Errors
/// When the config isn't valid
pub fn config() -> ClusterResult<Config> {
    let config = Config {
        heartbeat_interval: 250,
        election_timeout_min: 299,
        ..Default::default()
    };
    Ok(config.validate()?)
}

pub type TremorRaftImpl = Raft<TremorRequest, TremorResponse, Network, store::Store>;

#[derive(Debug)]
pub enum ClusterError {
    Other(String),
    Rocks(rocksdb::Error),
    Io(std::io::Error),
    Store(store::Error),
    Initialize(InitializeError),
    Serde(serde_json::Error),
    Config(ConfigError),
    Client(Error),
    JoinError(JoinError),
    // FIXME: this is a horrible hack
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

impl From<rocksdb::Error> for ClusterError {
    fn from(e: rocksdb::Error) -> Self {
        ClusterError::Rocks(e)
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

impl From<InitializeError> for ClusterError {
    fn from(e: InitializeError) -> Self {
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

impl From<serde_json::Error> for ClusterError {
    fn from(e: serde_json::Error) -> Self {
        ClusterError::Serde(e)
    }
}

impl From<JoinError> for ClusterError {
    fn from(e: JoinError) -> Self {
        ClusterError::JoinError(e)
    }
}
impl Display for ClusterError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterError::Other(e) => e.fmt(f),
            ClusterError::Rocks(e) => e.fmt(f),
            ClusterError::Io(e) => e.fmt(f),
            ClusterError::Store(e) => e.fmt(f),
            ClusterError::Initialize(e) => e.fmt(f),
            ClusterError::Config(e) => e.fmt(f),
            ClusterError::Runtime(e) => write!(f, "{:?}", e.lock()),
            ClusterError::Serde(e) => e.fmt(f),
            ClusterError::Client(e) => e.fmt(f),
            ClusterError::JoinError(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for ClusterError {}

type ClusterResult<T> = Result<T, ClusterError>;

/// Removes a node from a cluster
/// # Errors
/// When the node can't be removed
pub async fn remove_node<T: ToString + ?Sized>(
    node_id: openraft::NodeId,
    api_addr: &T,
) -> Result<(), crate::errors::Error> {
    let client = api::client::Tremor::new(api_addr)?;
    client.demote_voter(&node_id).await?;
    client.remove_learner(&node_id).await?;
    client.remove_node(&node_id).await?;
    println!("Membership updated: node {node_id} removed.");
    Ok(())
}
