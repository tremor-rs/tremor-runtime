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

use std::collections::{BTreeSet, HashMap};

use crate::{
    errors::{Error, ErrorKind},
    raft::NodeId,
};
use openraft::{
    error::{CheckIsLeaderError, ForwardToLeader, RaftError},
    raft::ClientWriteResponse,
};

use crate::raft::api::APIStoreReq;
use crate::Result;
use crate::{
    channel::{oneshot, Sender},
    ids::AppId,
};

use super::{
    node::Addr,
    store::{StateApp, TremorRequest, TremorSet},
    TremorRaftConfig, TremorRaftImpl,
};

#[derive(Clone, Default)]
pub(crate) struct Manager {
    node_id: NodeId,
    raft: Option<TremorRaftImpl>,
    sender: Option<Sender<APIStoreReq>>,
}

impl std::fmt::Debug for Manager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manager")
            .field("raft", &self.raft.is_some())
            .field("sender", &self.sender.is_some())
            .finish()
    }
}

impl Manager {
    pub(crate) fn id(&self) -> NodeId {
        self.node_id
    }
    async fn send(&self, command: APIStoreReq) -> Result<()> {
        self.sender
            .as_ref()
            .ok_or(crate::errors::ErrorKind::RaftNotRunning)?
            .send(command)
            .await?;
        Ok(())
    }

    fn raft(&self) -> Result<&TremorRaftImpl> {
        Ok(self
            .raft
            .as_ref()
            .ok_or(crate::errors::ErrorKind::RaftNotRunning)?)
    }

    async fn client_write<T>(&self, command: T) -> Result<ClientWriteResponse<TremorRaftConfig>>
    where
        T: Into<TremorRequest> + Send + 'static,
    {
        Ok(self.raft()?.client_write(command.into()).await?)
    }

    pub async fn is_leader(&self) -> Result<()> {
        Ok(self.raft()?.is_leader().await?)
    }

    pub(crate) fn new(node_id: NodeId, sender: Sender<APIStoreReq>, raft: TremorRaftImpl) -> Self {
        Self {
            node_id,
            raft: Some(raft),
            sender: Some(sender),
        }
    }

    // cluster
    pub async fn get_node(&self, node_id: u64) -> Result<Option<Addr>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetNode(node_id, tx);
        self.send(command).await?;
        Ok(rx.await?)
    }
    pub async fn get_nodes(&self) -> Result<HashMap<u64, Addr>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetNodes(tx);
        self.send(command).await?;
        Ok(rx.await?)
    }
    pub async fn get_node_id(&self, addr: Addr) -> Result<Option<u64>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetNodeId(addr, tx);
        self.send(command).await?;
        Ok(rx.await?)
    }
    pub async fn get_last_membership(&self) -> Result<BTreeSet<u64>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetLastMembership(tx);
        self.send(command).await?;
        Ok(rx.await?)
    }

    // apps
    pub async fn get_app_local(&self, app_id: AppId) -> Result<Option<StateApp>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetApp(app_id, tx);
        self.send(command).await?;
        Ok(rx.await?)
    }
    pub async fn get_apps_local(&self) -> Result<HashMap<AppId, crate::raft::api::apps::AppState>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetApps(tx);
        self.send(command).await?;
        Ok(rx.await?)
    }

    // kv
    pub async fn kv_set(&self, key: String, value: Vec<u8>) -> Result<Vec<u8>> {
        match self.is_leader().await {
            Ok(_) => self.kv_set_local(key, value).await,
            Err(Error(
                ErrorKind::CheckIsLeaderError(RaftError::APIError(
                    CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                        leader_node: Some(n),
                        ..
                    }),
                )),
                _,
            )) => {
                let client = crate::raft::api::client::Tremor::new(n.api())?;
                Ok(client.write(&TremorSet { key, value }).await?)
            }
            Err(e) => Err(e),
        }
    }
    pub async fn kv_set_local(&self, key: String, value: Vec<u8>) -> Result<Vec<u8>> {
        let tremor_res = self.client_write(TremorSet { key, value }).await?;
        tremor_res.data.into_kv_value()
    }
    pub async fn kv_get(&self, key: String) -> Result<Option<Vec<u8>>> {
        match self.is_leader().await {
            Ok(_) => self.kv_get_local(key).await,
            Err(Error(
                ErrorKind::CheckIsLeaderError(RaftError::APIError(
                    CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                        leader_node: Some(n),
                        ..
                    }),
                )),
                _,
            )) => {
                let client = crate::raft::api::client::Tremor::new(n.api())?;
                let res = client.read(&key).await;
                match res {
                    Ok(v) => Ok(Some(v)),
                    Err(e) if e.is_not_found() => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
            Err(e) => Err(e),
        }
    }
    pub async fn kv_get_local(&self, key: String) -> Result<Option<Vec<u8>>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::KVGet(key, tx);
        self.send(command).await?;
        Ok(rx.await?)
    }
}
