use crate::raft::{
    network::raft_network_impl::Network,
    store::{Store, TremorRequest, TremorResponse},
};

use openraft::{error::InitializeError, Config, ConfigError, Raft};
use std::{
    collections::BTreeSet,
    fmt::{Display, Formatter},
    sync::{Arc, Mutex},
};

pub mod app;
pub mod archive;
pub mod client;
pub mod network;
pub mod node;
pub mod store;

#[derive(
    Default, Debug, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord, Clone,
)]
pub struct NodeId(u64);

impl NodeId {
    #[must_use]
    pub fn random() -> Self {
        Self(rand::random())
    }
}

impl From<u64> for NodeId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<NodeId> for u64 {
    fn from(id: NodeId) -> Self {
        id.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<u64> for NodeId {
    fn as_ref(&self) -> &u64 {
        &self.0
    }
}

impl std::ops::Deref for NodeId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct TremorNode {
    pub api_addr: String,
    rpc_addr: String,
}
impl Display for TremorNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TremorNode {{ api: {}, rpc: {} }}",
            self.api_addr, self.rpc_addr
        )
    }
}

pub type Tremor = Raft<TremorRequest, TremorResponse, Network, Arc<store::Store>>;
type Server = tide::Server<Arc<app::Tremor>>;

#[derive(Debug)]
pub enum ClusterError {
    Other(String),
    Rocks(rocksdb::Error),
    Io(std::io::Error),
    Store(store::Error),
    Initialize(InitializeError),
    Config(ConfigError),
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

impl From<crate::Error> for ClusterError {
    fn from(e: crate::Error) -> Self {
        ClusterError::Runtime(Mutex::new(e))
    }
}

impl Display for ClusterError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterError::Other(s) => write!(f, "{}", s),
            ClusterError::Rocks(s) => write!(f, "{}", s),
            ClusterError::Io(s) => write!(f, "{}", s),
            ClusterError::Store(s) => write!(f, "{}", s),
            ClusterError::Initialize(e) => write!(f, "{}", e),
            ClusterError::Config(e) => write!(f, "{}", e),
            ClusterError::Runtime(e) => write!(f, "{:?}", e.lock()),
        }
    }
}
impl std::error::Error for ClusterError {}

type ClusterResult<T> = Result<T, ClusterError>;

/// Removes a node from a cluster
/// # Errors
/// When the node can't be removed
pub async fn remove_node(node_id: NodeId, api_addr: String) -> Result<(), crate::errors::Error> {
    let mut client = client::Tremor::new(node_id, api_addr);
    let metrics = client.metrics().await.map_err(|e| format!("error: {e}",))?;
    let leader_id = metrics.current_leader.ok_or("No leader present!")?;
    let leader = metrics.membership_config.get_node(&leader_id);
    let leader_addr = leader.api_addr.clone();
    println!("communication with leader: {leader_addr} establisehd.",);
    let mut client = client::Tremor::new(leader_id, leader_addr.clone());
    let metrics = client.metrics().await.map_err(|e| format!("error: {e}"))?;
    let membership: BTreeSet<_> = metrics
        .membership_config
        .nodes()
        .filter_map(|(id, _)| if *id == node_id { None } else { Some(*id) })
        .collect();
    client
        .change_membership(&membership)
        .await
        .map_err(|e| format!("Failed to update membershipo learner: {e}"))?;
    println!("Membership updated: node {node_id} removed.");
    Ok(())
}
