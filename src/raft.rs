use crate::raft::network::Raft as RaftTrait;
use crate::{
    raft::{
        app::TremorApp,
        network::{api, management, raft::RaftServer, raft_network_impl::TremorNetwork},
        store::{StoreError, TremorRequest, TremorResponse, TremorStore},
    },
    system::Runtime,
};

use async_std::task;
use futures::{future, prelude::*};
use openraft::{error::InitializeError, Config, ConfigError, Raft};
use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
    path::Path,
    sync::{Arc, Mutex},
};
use tarpc::{
    server::{self, Channel},
    tokio_serde::formats::Json,
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

pub async fn init_cluster<P>(dir: P, world: Runtime) -> ClusterResult<()>
where
    P: AsRef<Path>,
{
    // FIXME
    let store = TremorStore::load(&dir, world).await?;
    // Validate that the data is correctly stored
    let (id, api_addr, rpc_addr) = store.get_node_data()?;
    debug!("Node id: {id}");
    debug!("API addr: {api_addr}");
    debug!("RPC addr: {rpc_addr}");
    //let node = ClusterNode::new(id, api_addr.clone(), rpc_addr.clone(), config()?);

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = TremorNetwork {};

    // Create a configuration for the raft instance.
    let config = Arc::new(config()?);

    let raft = Raft::new(id, config.clone(), network, store.clone());

    let mut nodes = BTreeMap::new();
    nodes.insert(id, TremorNode { rpc_addr, api_addr });
    raft.initialize(nodes).await?;
    raft.shutdown()
        .await
        .map_err(|e| format!("shutdown failed: {e}"))?;
    Ok(())
}

/// Start the current Raft node from the data stored in `dir`.
///
/// Precondition: `dir` has been properly initialized by `TremorStore::init_node`
pub async fn start_raft_node<P>(dir: P, world: Runtime) -> ClusterResult<()>
where
    P: AsRef<Path>,
{
    let config = Arc::new(config()?);
    // Create a instance of where the Raft data will be stored.
    let store = TremorStore::load(&dir, world).await?;
    let id = store
        .get_node_id()?
        .ok_or("invalid cluster store, node_id missing")?;
    debug!("Node id: {id}");
    let api_addr = store
        .get_api_addr()?
        .ok_or("invalid cluster store, http_addr missing")?;
    debug!("API addr: {api_addr}");
    let rpc_addr = store
        .get_rpc_addr()?
        .ok_or("invalid cluster store, rpc_addr missing")?;
    debug!("RPC addr: {rpc_addr}");

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = TremorNetwork {};

    // Create a local raft instance.
    let raft = Raft::new(id, config.clone(), network, store.clone());

    let app = Arc::new(TremorApp {
        id,
        api_addr: api_addr.clone(),
        rpc_addr: rpc_addr.clone(),
        raft,
        store,
    });

    // JSON transport is provided by the json_transport tarpc module. It makes it easy
    // to start up a serde-powered json serialization strategy over TCP.
    let mut listener = tarpc::serde_transport::tcp::listen(&rpc_addr, Json::default).await?;
    listener.config_mut().max_frame_length(usize::MAX);
    let capp = app.clone();
    task::spawn(async move {
        listener
            // Ignore accept errors.
            .filter_map(|r| future::ready(r.ok()))
            .map(server::BaseChannel::with_defaults)
            // Limit channels to 1 per IP.
            // FIXME .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
            // serve is generated by the service attribute. It takes as input any type implementing
            // the generated World trait.
            .map(|channel| {
                let server = RaftServer::new(capp.clone());
                channel.execute(server.serve())
            })
            // Max 10 channels.
            .buffer_unordered(10)
            .for_each(|_| async {})
            .await
    });

    // Create an application that will store all the instances created above
    let mut app: Server = tide::Server::with_state(app);

    management::install_rest_endpoints(&mut app);
    api::install_rest_endpoints(&mut app);

    app.listen(api_addr).await?;
    Ok(())
}
