use crate::raft::{network::RaftClient, store::TremorRequest, NodeId, TremorNode};
use anyhow::Result;
use async_trait::async_trait;
use openraft::{
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    AnyError, RaftNetwork,
};
use serde::{de::DeserializeOwned, Serialize};
use tarpc::{client, context, tokio_serde::formats::Json};

pub struct Network {}

impl Network {
    pub(crate) fn new() -> Self {
        Self {}
    }
    /// # Errors
    /// if the rpc call fails
    pub async fn send_rpc<Req, Resp, Err>(
        &self,
        target: NodeId,
        target_node: Option<&TremorNode>,
        uri: &str,
        req: Req,
    ) -> Result<Resp>
    where
        Req: Serialize,
        Err: std::error::Error + DeserializeOwned,
        Resp: DeserializeOwned,
    {
        let addr = target_node
            .map(|x| &x.rpc_addr)
            .ok_or_else(|| AnyError::default())?; // FIXME: node not found error

        let target_url = format!("http://{}/{}", addr, uri);
        let client = reqwest::Client::new();

        let resp = client.post(target_url).json(&req).send().await?;

        let result: Result<Resp, Err> = resp.json().await?;

        result
    }
}

// NOTE: This could be implemented also on `Arc<ExampleNetwork>`, but since it's empty, implemented directly.
// #[async_trait]
// impl RaftNetworkFactory<TremorTypeConfig> for Network {
//     type Network = TremorNetworkConnection;
//     type ConnectionError = ClusterError;
//     async fn new_client(
//         &mut self,
//         target: NodeId,
//         node: &TremorNode,
//     ) -> Result<Self::Network, Self::ConnectionError> {
//         Ok(TremorNetworkConnection {
//             addr: node.rpc_addr.clone(),
//             client: None,
//             target,
//         })
//     }
// }

pub struct TremorNetworkConnection {
    addr: String,
    client: Option<RaftClient>,
    target: NodeId,
}
impl TremorNetworkConnection {
    async fn c<E: std::error::Error + DeserializeOwned>(&mut self) -> Result<&RaftClient> {
        if self.client.is_none() {
            let transport = tarpc::serde_transport::tcp::connect(&self.addr, Json::default).await?;
            self.client = Some(RaftClient::new(client::Config::default(), transport).spawn());
        }
        self.client.as_ref().ok_or_else(|| AnyError::default())
    }
}

#[async_trait]
impl RaftNetwork<TremorRequest> for TremorNetworkConnection {
    async fn send_append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<TremorRequest>,
    ) -> Result<AppendEntriesResponse> {
        let r = self.c().await?.append(context::current(), rpc).await;
        if r.is_err() {
            self.reset_client(target);
        };
        r?
    }

    async fn send_install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let r = self
            .c(target)
            .await?
            .snapshot(context::current(), rpc)
            .await;
        if r.is_err() {
            self.reset_client(target);
        };
        r?
    }

    async fn send_vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        let r = self.c().await?.vote(context::current(), rpc).await;
        if r.is_err() {
            self.reset_client(target);
        };
        r?
    }
}
