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

use crate::raft::{
    network::RaftClient,
    store::{Store, TremorRequest},
};
use async_trait::async_trait;
use dashmap::{mapref::entry::Entry, DashMap};
use openraft::{
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    AnyError, RaftNetwork,
};
use std::sync::Arc;
use tarpc::{client::RpcError, context};

#[derive(Clone, Debug)]
pub struct Network {
    store: Arc<Store>,
    pool: DashMap<openraft::NodeId, RaftClient>,
}

impl Network {
    pub(crate) fn new(store: Arc<Store>) -> Arc<Self> {
        Arc::new(Self {
            store,
            pool: DashMap::default(),
        })
    }

    /// Create a new TCP client for the given `target` node
    ///
    /// This requires the `target` to be known to the cluster state.
    async fn new_client(&self, target: openraft::NodeId) -> anyhow::Result<RaftClient> {
        let sm = self.store.state_machine.read().await;
        let addr = sm
            .nodes
            .get_node(target)
            .ok_or_else(|| anyhow::anyhow!(format!("Node {target} not known to the cluster")))?;
        let transport = tarpc::serde_transport::tcp::connect(
            addr.rpc(),
            tarpc::tokio_serde::formats::Json::default,
        )
        .await?;
        let client = RaftClient::new(tarpc::client::Config::default(), transport).spawn();
        Ok(client)
    }

    /// Ensure we have a client to the node identified by `target`
    ///
    /// Pick it from the pool first, if that fails, create a new one
    async fn ensure_client(&self, target: openraft::NodeId) -> anyhow::Result<RaftClient> {
        // TODO: get along without the cloning
        let client = match self.pool.entry(target) {
            Entry::Occupied(oe) => oe.get().clone(),
            Entry::Vacant(ve) => {
                let client = self.new_client(target).await?;
                ve.insert(client.clone());
                client
            }
        };
        Ok(client)
    }

    fn drop_client(&self, target: openraft::NodeId) {
        self.pool.remove(&target);
    }

    fn handle_response<T>(
        &self,
        target: openraft::NodeId,
        response: Result<Result<T, AnyError>, RpcError>,
    ) -> anyhow::Result<T> {
        match response {
            Ok(res) => res.map_err(Into::into),
            Err(e @ RpcError::Disconnected) => {
                self.drop_client(target);
                error!("Client disconnected from node {target}");
                Err(e.into())
            }
            Err(e @ RpcError::DeadlineExceeded) => {
                // no need to drop the client here
                // TODO: implement some form of backoff
                error!("Request against node {target} timed out");
                Err(e.into())
            }
            Err(e @ RpcError::Server(_)) => {
                self.drop_client(target); // TODO necessary here?
                error!("Server error from node {target}: {e}");
                Err(e.into())
            }
        }
    }
}

#[async_trait]
impl RaftNetwork<TremorRequest> for Network {
    // the raft engine will retry upon network failures
    // so all we need to do is to drop the client if need be to trigger a reconnect
    // it would be nice to have some kind of backoff, but this might halt the raft engine
    // and might lead to some nasty timeouts
    async fn send_append_entries(
        &self,
        target: openraft::NodeId,
        rpc: AppendEntriesRequest<TremorRequest>,
    ) -> anyhow::Result<AppendEntriesResponse> {
        let client = self.ensure_client(target).await?;
        self.handle_response(target, client.append(context::current(), rpc).await)
    }

    async fn send_install_snapshot(
        &self,
        target: openraft::NodeId,
        rpc: InstallSnapshotRequest,
    ) -> anyhow::Result<InstallSnapshotResponse> {
        let client = self.ensure_client(target).await?;
        self.handle_response(target, client.snapshot(context::current(), rpc).await)
    }

    async fn send_vote(
        &self,
        target: openraft::NodeId,
        rpc: VoteRequest,
    ) -> anyhow::Result<VoteResponse> {
        let client = self.ensure_client(target).await?;
        self.handle_response(target, client.vote(context::current(), rpc).await)
    }
}
