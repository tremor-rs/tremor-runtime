// Copyright 2022, The Tremor Team
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

use crate::raft::{network::RaftClient, node::Addr, NodeId, TremorRaftConfig};
use async_trait::async_trait;
use openraft::{
    error::{InstallSnapshotError, NetworkError, RaftError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    AnyError, RaftNetwork, RaftNetworkFactory,
};
use std::cmp::min;
use tarpc::{client::RpcError, context};
use tokio::time::sleep;

#[derive(Clone, Debug)]
pub struct Network {}

impl Network {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

// NOTE: This could be implemented also on `Arc<ExampleNetwork>`, but since it's empty, implemented
// directly.
#[async_trait]
impl RaftNetworkFactory<TremorRaftConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &Addr) -> Self::Network {
        NetworkConnection {
            client: None,
            target,
            addr: node.clone(),
            error_count: 0,
            last_reconnect: std::time::Instant::now(),
        }
    }
}

const RECONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

pub struct NetworkConnection {
    client: Option<RaftClient>,
    target: NodeId,
    addr: Addr,
    error_count: u32,
    last_reconnect: std::time::Instant,
}

// We need this for the types :sob:
#[derive(Debug)]
struct SendError(String);

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Send error: {}", self.0)
    }
}

impl std::error::Error for SendError {}

impl NetworkConnection {
    /// Ensure we have a client to the node identified by `target`
    ///
    /// Pick it from the pool first, if that fails, create a new one
    ///     /// Ensure we have a client to the node identified by `target`
    ///
    /// Pick it from the pool first, if that fails, create a new one
    async fn ensure_client<E: std::error::Error>(&mut self) -> Result<RaftClient, RPCError<E>> {
        // TODO: get along without the cloning

        match &self.client {
            Some(client) => Ok(client.clone()),
            None => {
                sleep(RECONNECT_TIMEOUT * min(self.error_count, 10)).await;
                let client = self.new_client().await?;
                self.client = Some(client.clone());
                Ok(client)
            }
        }
    }

    /// Create a new TCP client for the given `target` node
    ///
    /// This requires the `target` to be known to the cluster state.
    async fn new_client<E: std::error::Error>(&mut self) -> Result<RaftClient, RPCError<E>> {
        let transport = tarpc::serde_transport::tcp::connect(
            self.addr.rpc(),
            tarpc::tokio_serde::formats::Bincode::default,
        )
        .await
        .map_err(|e| RPCError::Network(NetworkError::new(&e)));
        if transport.is_err() {
            self.error_count += 1;
        }
        let client = RaftClient::new(tarpc::client::Config::default(), transport?).spawn();
        self.last_reconnect = std::time::Instant::now();
        self.error_count = 0;
        Ok(client)
    }
    fn handle_response<T, E: std::error::Error>(
        &mut self,
        target: NodeId,
        response: Result<Result<T, AnyError>, RpcError>,
    ) -> Result<T, RPCError<E>> {
        match response {
            Ok(res) => res.map_err(|e| RPCError::Network(NetworkError::new(&e))),
            Err(e @ RpcError::Shutdown) => {
                self.client = None;
                self.error_count += 1;
                error!("Client disconnected from node {target}");
                Err(RPCError::Network(NetworkError::new(&e)))
            }
            Err(e @ RpcError::DeadlineExceeded) => {
                // no need to drop the client here
                // TODO: implement some form of backoff
                error!("Request against node {target} timed out");
                Err(RPCError::Network(NetworkError::new(&e)))
            }
            Err(RpcError::Server(e)) => {
                self.client = None; // TODO necessary here?
                self.error_count += 1;
                error!("Server error from node {target}: {e}");
                Err(RPCError::Network(NetworkError::new(&e)))
            }
            Err(RpcError::Send(e)) => {
                self.client = None; // TODO necessary here?
                self.error_count += 1;
                error!("Server error from node {target}: {e}");
                Err(RPCError::Network(NetworkError::new(&SendError(format!(
                    "{e}"
                )))))
            }
            Err(RpcError::Receive(e)) => {
                self.client = None; // TODO necessary here?
                self.error_count += 1;
                error!("Server error from node {target}: {e}");
                Err(RPCError::Network(NetworkError::new(&e)))
            }
        }
    }
}
type RPCError<T> = openraft::error::RPCError<NodeId, Addr, T>;
#[async_trait]
impl RaftNetwork<TremorRaftConfig> for NetworkConnection {
    // the raft engine will retry upon network failures
    // so all we need to do is to drop the client if need be to trigger a reconnect
    // it would be nice to have some kind of backoff, but this might halt the raft engine
    // and might lead to some nasty timeouts
    async fn send_append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TremorRaftConfig>,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<RaftError<NodeId>>> {
        let client = self.ensure_client().await?;
        self.handle_response(self.target, client.append(context::current(), rpc).await)
    }

    async fn send_install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TremorRaftConfig>,
    ) -> Result<InstallSnapshotResponse<NodeId>, RPCError<RaftError<NodeId, InstallSnapshotError>>>
    {
        let client = self.ensure_client().await?;
        let r = client.snapshot(context::current(), rpc).await;
        self.handle_response(self.target, r)
    }

    async fn send_vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
    ) -> Result<VoteResponse<NodeId>, RPCError<RaftError<NodeId>>> {
        let client = self.ensure_client().await?;
        self.handle_response(self.target, client.vote(context::current(), rpc).await)
    }
}
