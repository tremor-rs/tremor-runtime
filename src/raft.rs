pub mod api;
pub mod app;
pub mod archive;
pub mod network;
pub mod node;
pub mod store;

use api::client::Error;
use network::raft_network_impl::Network;
pub use openraft::NodeId;
use openraft::{error::InitializeError, Config, ConfigError, Raft};
use std::{
    fmt::{Display, Formatter},
    sync::{Arc, Mutex},
};
use store::{Store, TremorRequest, TremorResponse};

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

pub type Tremor = Raft<TremorRequest, TremorResponse, Network, store::Store>;
// FIXME: move to api module
type Server = tide::Server<Arc<app::Tremor>>;

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
            ClusterError::Serde(e) => write!(f, "{e}"),
            ClusterError::Client(e) => e.fmt(f),
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
