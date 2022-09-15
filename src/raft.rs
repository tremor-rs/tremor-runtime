use crate::raft::{
    app::TremorApp,
    network::raft_network_impl::TremorNetwork,
    store::{StoreError, TremorRequest, TremorResponse, TremorStore},
};

use openraft::{error::InitializeError, Config, ConfigError, Raft};
use std::{
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
pub struct TremorNodeId(u64);

impl TremorNodeId {
    pub fn random() -> Self {
        Self(rand::random())
    }
}

impl From<u64> for TremorNodeId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<TremorNodeId> for u64 {
    fn from(id: TremorNodeId) -> Self {
        id.0
    }
}

impl std::fmt::Display for TremorNodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<u64> for TremorNodeId {
    fn as_ref(&self) -> &u64 {
        &self.0
    }
}

impl std::ops::Deref for TremorNodeId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// load a default raft config
pub fn config() -> ClusterResult<Config> {
    let mut config = Config::default();
    config.heartbeat_interval = 250;
    config.election_timeout_min = 299;
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

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TremorTypeConfig: D = TremorRequest, R = TremorResponse, NodeId = TremorNodeId, Node = TremorNode
);

pub type TremorRaft = Raft<TremorTypeConfig, TremorNetwork, Arc<TremorStore>>;
type Server = tide::Server<Arc<TremorApp>>;

#[derive(Debug)]
pub enum ClusterError {
    Other(String),
    Rocks(rocksdb::Error),
    Io(std::io::Error),
    Store(StoreError),
    Initialize(InitializeError<TremorNodeId, TremorNode>),
    Config(ConfigError),
    // FIXME: this is a horrible hack
    Runtime(Mutex<crate::Error>),
}

impl From<StoreError> for ClusterError {
    fn from(e: StoreError) -> Self {
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

impl From<InitializeError<TremorNodeId, TremorNode>> for ClusterError {
    fn from(e: InitializeError<TremorNodeId, TremorNode>) -> Self {
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
