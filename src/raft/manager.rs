// Copyright 2021, The Tremor Team
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

use std::{
    collections::{BTreeSet, HashMap},
    convert::Into,
};

use super::{
    node::Addr,
    store::{StateApp, TremorRequest, TremorSet},
    ClusterError, TremorRaftConfig, TremorRaftImpl,
};
use crate::raft::api::APIStoreReq;
use crate::raft::NodeId;
use crate::Result;
use crate::{
    channel::{bounded, oneshot, OneShotSender, Sender},
    connectors::prelude::Receiver,
};
use openraft::{
    error::{CheckIsLeaderError, Fatal, ForwardToLeader, RaftError},
    raft::ClientWriteResponse,
};
use simd_json::OwnedValue;
use tremor_common::alias;

#[derive(Clone, Default)]
pub(crate) struct Cluster {
    node_id: NodeId,
    raft: Option<TremorRaftImpl>,
    sender: Option<Sender<APIStoreReq>>,
}
#[derive(Clone, Default, Debug)]
pub(crate) struct ClusterInterface {
    node_id: NodeId,
    store_sender: Option<Sender<APIStoreReq>>,
    cluster_sender: Option<Sender<IFRequest>>,
}

type IsLeaderResult = std::result::Result<(), RaftError<u64, CheckIsLeaderError<u64, Addr>>>;
enum IFRequest {
    IsLeader(OneShotSender<IsLeaderResult>),
    SetKeyLocal(TremorSet, OneShotSender<Result<Vec<u8>>>),
}
async fn cluster_interface(raft: TremorRaftImpl, mut rx: Receiver<IFRequest>) {
    while let Some(msg) = rx.recv().await {
        match msg {
            IFRequest::IsLeader(tx) => {
                let res = raft.is_leader().await;
                if tx.send(res).is_err() {
                    error!("Error sending response to API");
                }
            }
            IFRequest::SetKeyLocal(set, tx) => {
                let res = match raft.client_write(set.into()).await {
                    Ok(v) => v.data.into_kv_value().map_err(anyhow::Error::from),
                    Err(e) => Err(e.into()),
                };
                if tx.send(res).is_err() {
                    error!("Error sending response to API");
                }
            }
        }
    }
}

impl ClusterInterface {
    pub(crate) fn id(&self) -> NodeId {
        self.node_id
    }
    async fn send_store(&self, command: APIStoreReq) -> Result<()> {
        self.store_sender
            .as_ref()
            .ok_or(ClusterError::RaftNotRunning)?
            .send(command)
            .await?;
        Ok(())
    }
    async fn send_cluster(&self, command: IFRequest) -> Result<()> {
        self.cluster_sender
            .as_ref()
            .ok_or(ClusterError::RaftNotRunning)?
            .send(command)
            .await?;
        Ok(())
    }
    pub(crate) async fn get_last_membership(&self) -> Result<BTreeSet<u64>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetLastMembership(tx);
        self.send_store(command).await?;
        Ok(rx.await?)
    }
    pub(crate) async fn is_leader(&self) -> IsLeaderResult {
        let (tx, rx) = oneshot();
        let command = IFRequest::IsLeader(tx);
        self.send_cluster(command)
            .await
            .map_err(|_| RaftError::Fatal(Fatal::Stopped))?;
        rx.await.map_err(|_| RaftError::Fatal(Fatal::Stopped))?
    }

    // kv
    pub(crate) async fn kv_set(&self, key: String, mut value: Vec<u8>) -> Result<Vec<u8>> {
        match self.is_leader().await {
            Ok(()) => self.kv_set_local(key, value).await,
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                leader_node: Some(n),
                ..
            }))) => {
                let client = crate::raft::api::client::Tremor::new(n.api())?;
                // TODO: there should be a better way to forward then the client
                Ok(simd_json::to_vec(
                    &client
                        .write(&crate::raft::api::kv::KVSet {
                            key,
                            value: simd_json::from_slice(&mut value)?,
                        })
                        .await?,
                )?)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn kv_set_local(&self, key: String, value: Vec<u8>) -> Result<Vec<u8>> {
        let (tx, rx) = oneshot();
        let command = IFRequest::SetKeyLocal(TremorSet { key, value }, tx);
        self.send_cluster(command).await?;
        rx.await?
    }

    pub(crate) async fn kv_get(&self, key: String) -> Result<Option<OwnedValue>> {
        match self.is_leader().await {
            Ok(()) => self.kv_get_local(key).await,
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                leader_node: Some(n),
                ..
            }))) => {
                let client = crate::raft::api::client::Tremor::new(n.api())?;
                let res = client.read(&key).await;
                match res {
                    Ok(v) => Ok(Some(v)),
                    Err(e) if e.is_not_found() => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
            Err(e) => Err(e.into()),
        }
    }
    pub(crate) async fn kv_get_local(&self, key: String) -> Result<Option<OwnedValue>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::KVGet(key, tx);
        self.send_store(command).await?;
        Ok(rx
            .await?
            .map(|mut v| simd_json::from_slice(&mut v))
            .transpose()?)
    }
}

impl From<Cluster> for ClusterInterface {
    fn from(c: Cluster) -> Self {
        let cluster_sender = c.raft.map(|r| {
            let (tx, rx) = bounded(1042);
            tokio::spawn(cluster_interface(r, rx));
            tx
        });
        Self {
            cluster_sender,
            node_id: c.node_id,
            store_sender: c.sender,
        }
    }
}

impl std::fmt::Debug for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manager")
            .field("node_id", &self.node_id)
            .field("raft", &self.raft.is_some())
            .field("sender", &self.sender.is_some())
            .finish()
    }
}

impl Cluster {
    async fn send(&self, command: APIStoreReq) -> Result<()> {
        self.sender
            .as_ref()
            .ok_or(ClusterError::RaftNotRunning)?
            .send(command)
            .await?;
        Ok(())
    }

    fn raft(&self) -> Result<&TremorRaftImpl> {
        Ok(self.raft.as_ref().ok_or(ClusterError::RaftNotRunning)?)
    }

    async fn client_write<T>(&self, command: T) -> Result<ClientWriteResponse<TremorRaftConfig>>
    where
        T: Into<TremorRequest> + Send + 'static,
    {
        Ok(self.raft()?.client_write(command.into()).await?)
    }

    pub(crate) async fn is_leader(&self) -> IsLeaderResult {
        self.raft()
            .map_err(|_| RaftError::Fatal(Fatal::Stopped))?
            .is_leader()
            .await
    }

    pub(crate) fn new(node_id: NodeId, sender: Sender<APIStoreReq>, raft: TremorRaftImpl) -> Self {
        Self {
            node_id,
            raft: Some(raft),
            sender: Some(sender),
        }
    }

    // cluster
    pub(crate) async fn get_node(&self, node_id: u64) -> Result<Option<Addr>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetNode(node_id, tx);
        self.send(command).await?;
        Ok(rx.await?)
    }
    pub(crate) async fn get_nodes(&self) -> Result<HashMap<u64, Addr>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetNodes(tx);
        self.send(command).await?;
        Ok(rx.await?)
    }
    pub(crate) async fn get_node_id(&self, addr: Addr) -> Result<Option<u64>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetNodeId(addr, tx);
        self.send(command).await?;
        Ok(rx.await?)
    }
    pub(crate) async fn get_last_membership(&self) -> Result<BTreeSet<u64>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetLastMembership(tx);
        self.send(command).await?;
        Ok(rx.await?)
    }

    // apps
    pub(crate) async fn get_app_local(&self, app_id: alias::App) -> Result<Option<StateApp>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetApp(app_id, tx);
        self.send(command).await?;
        Ok(rx.await?)
    }

    pub(crate) async fn get_apps_local(
        &self,
    ) -> Result<HashMap<alias::App, crate::raft::api::apps::AppState>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetApps(tx);
        self.send(command).await?;
        Ok(rx.await?)
    }

    // kv
    pub(crate) async fn kv_set(&self, key: String, mut value: Vec<u8>) -> Result<Vec<u8>> {
        match self.is_leader().await {
            Ok(()) => self.kv_set_local(key, value).await,
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                leader_node: Some(n),
                ..
            }))) => {
                let client = crate::raft::api::client::Tremor::new(n.api())?;
                // TODO: there should be a better way to forward then the client
                Ok(simd_json::to_vec(
                    &client
                        .write(&crate::raft::api::kv::KVSet {
                            key,
                            value: simd_json::from_slice(&mut value)?,
                        })
                        .await?,
                )?)
            }
            Err(e) => Err(e.into()),
        }
    }
    pub(crate) async fn kv_set_local(&self, key: String, value: Vec<u8>) -> Result<Vec<u8>> {
        let tremor_res = self.client_write(TremorSet { key, value }).await?;
        Ok(tremor_res.data.into_kv_value()?)
    }

    pub(crate) async fn kv_get(&self, key: String) -> Result<Option<OwnedValue>> {
        match self.is_leader().await {
            Ok(()) => self.kv_get_local(key).await,
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                leader_node: Some(n),
                ..
            }))) => {
                let client = crate::raft::api::client::Tremor::new(n.api())?;
                let res = client.read(&key).await;
                match res {
                    Ok(v) => Ok(Some(v)),
                    Err(e) if e.is_not_found() => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
            Err(e) => Err(e.into()),
        }
    }
    pub(crate) async fn kv_get_local(&self, key: String) -> Result<Option<OwnedValue>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::KVGet(key, tx);
        self.send(command).await?;
        Ok(rx
            .await?
            .map(|mut v| simd_json::from_slice(&mut v))
            .transpose()?)
    }
}
