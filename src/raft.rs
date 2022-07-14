use crate::raft::network::Raft as RaftTrait;
use crate::{
    raft::{
        app::TremorApp,
        network::{api, management, raft::RaftServer, raft_network_impl::TremorNetwork},
        store::{StoreError, TremorRequest, TremorResponse, TremorStore},
    },
    system::World,
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
pub mod store;

pub type TremorNodeId = u64;

fn config() -> ClusterResult<Config> {
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
            "ExampleNode {{ api: {}, rpc: {} }}",
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

pub async fn init_raft_node<P>(
    dir: P,
    node_id: TremorNodeId,
    rpc: String,
    api: String,
) -> ClusterResult<()>
where
    P: AsRef<Path>,
{
    TremorStore::init_node(&dir, node_id, &rpc, &api).await
}

pub async fn init_cluster<P>(dir: P, world: World) -> ClusterResult<()>
where
    P: AsRef<Path>,
{
    // Validate that the data is correctly stored
    let store = TremorStore::new(&dir, world).await?;
    let id = store
        .get_node_id()?
        .ok_or("invalid cluster store, node_id missing")?;
    debug!("Node id: {id}");
    debug_assert_eq!(id, 0);
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

pub async fn start_raft_node<P>(dir: P, world: World) -> ClusterResult<()>
where
    P: AsRef<Path>,
{
    let config = Arc::new(config()?);
    // Create a instance of where the Raft data will be stored.
    let store = TremorStore::new(&dir, world.clone()).await?;
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
        rcp_addr: rpc_addr.clone(),
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

    // Create an application that will store all the instances created above, this will
    // be later used on the actix-web services.
    let mut app: Server = tide::Server::with_state(app);

    management::rest(&mut app);
    api::rest(&mut app);

    app.listen(api_addr).await?;
    Ok(())
}
