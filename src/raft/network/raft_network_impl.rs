use crate::raft::{network::RaftClient, ClusterError, TremorNode, TremorNodeId, TremorTypeConfig};
use async_trait::async_trait;
use openraft::{
    error::{
        AppendEntriesError, InstallSnapshotError, NetworkError, NodeNotFound, RPCError,
        RemoteError, VoteError,
    },
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    AnyError, RaftNetwork, RaftNetworkFactory,
};
use serde::{de::DeserializeOwned, Serialize};
use tarpc::{client, context, tokio_serde::formats::Json};

pub struct TremorNetwork {}

impl TremorNetwork {
    pub(crate) fn new() -> Self {
        Self {}
    }
    pub async fn send_rpc<Req, Resp, Err>(
        &self,
        target: TremorNodeId,
        target_node: Option<&TremorNode>,
        uri: &str,
        req: Req,
    ) -> Result<Resp, RPCError<TremorNodeId, TremorNode, Err>>
    where
        Req: Serialize,
        Err: std::error::Error + DeserializeOwned,
        Resp: DeserializeOwned,
    {
        let addr = target_node
            .map(|x| &x.rpc_addr)
            .ok_or(RPCError::NodeNotFound(NodeNotFound {
                node_id: target,
                source: AnyError::default(),
            }))?;

        let url = format!("http://{}/{}", addr, uri);
        let client = reqwest::Client::new();

        let resp = client
            .post(url)
            .json(&req)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let res: Result<Resp, Err> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| RPCError::RemoteError(RemoteError::new(target, e)))
    }
}

// NOTE: This could be implemented also on `Arc<ExampleNetwork>`, but since it's empty, implemented directly.
#[async_trait]
impl RaftNetworkFactory<TremorTypeConfig> for TremorNetwork {
    type Network = TremorNetworkConnection;
    type ConnectionError = ClusterError;
    async fn new_client(
        &mut self,
        target: TremorNodeId,
        node: &TremorNode,
    ) -> Result<Self::Network, Self::ConnectionError> {
        let client = tarpc::serde_transport::tcp::connect(&node.rpc_addr, Json::default)
            .await
            .ok()
            .map(|transport| RaftClient::new(client::Config::default(), transport).spawn());

        Ok(TremorNetworkConnection {
            addr: node.rpc_addr.clone(),
            client,
            target,
        })
    }
}

pub struct TremorNetworkConnection {
    addr: String,
    client: Option<RaftClient>,
    target: TremorNodeId,
}
impl TremorNetworkConnection {
    async fn c<E: std::error::Error + DeserializeOwned>(
        &mut self,
    ) -> Result<&RaftClient, RPCError<TremorNodeId, TremorNode, E>> {
        if self.client.is_none() {
            let transport = tarpc::serde_transport::tcp::connect(&self.addr, Json::default)
                .await
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
            self.client = Some(RaftClient::new(client::Config::default(), transport).spawn());
        }
        self.client
            .as_ref()
            .ok_or(RPCError::Network(NetworkError::from(AnyError::default())))
    }
}

#[async_trait]
impl RaftNetwork<TremorTypeConfig> for TremorNetworkConnection {
    async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest<TremorTypeConfig>,
    ) -> Result<
        AppendEntriesResponse<TremorNodeId>,
        RPCError<TremorNodeId, TremorNode, AppendEntriesError<TremorNodeId>>,
    > {
        let r = self
            .c()
            .await?
            .append(context::current(), req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)));
        if r.is_err() {
            self.client = None;
        };
        r?.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TremorTypeConfig>,
    ) -> Result<
        InstallSnapshotResponse<TremorNodeId>,
        RPCError<TremorNodeId, TremorNode, InstallSnapshotError<TremorNodeId>>,
    > {
        let r = self
            .c()
            .await?
            .snapshot(context::current(), req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)));
        if r.is_err() {
            self.client = None;
        };
        r?.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn send_vote(
        &mut self,
        req: VoteRequest<TremorNodeId>,
    ) -> Result<
        VoteResponse<TremorNodeId>,
        RPCError<TremorNodeId, TremorNode, VoteError<TremorNodeId>>,
    > {
        let r = self
            .c()
            .await?
            .vote(context::current(), req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)));
        if r.is_err() {
            self.client = None;
        };
        r?.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}
