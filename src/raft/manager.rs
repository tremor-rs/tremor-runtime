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

use crate::raft::api::APIStoreReq;
use crate::Result;
use crate::{
    channel::{oneshot, Sender},
    ids::AppId,
};

use super::{node::Addr, store::StateApp};

#[derive(Debug, Clone, Default)]
pub(crate) struct Manager {
    sender: Option<Sender<APIStoreReq>>,
}

impl Manager {
    async fn send(&self, command: APIStoreReq) -> Result<()> {
        self.sender
            .as_ref()
            .ok_or(crate::errors::ErrorKind::RaftNotRunning)?
            .send(command)
            .await?;
        Ok(())
    }
    pub(crate) fn new(sender: Sender<APIStoreReq>) -> Self {
        Self {
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
    pub async fn get_app(&self, app_id: AppId) -> Result<Option<StateApp>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetApp(app_id, tx);
        self.send(command).await?;
        Ok(rx.await?)
    }
    pub async fn get_apps(&self) -> Result<HashMap<AppId, crate::raft::api::apps::AppState>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetApps(tx);
        self.send(command).await?;
        Ok(rx.await?)
    }

    // kv
    pub async fn kv_set(&self, _key: String, _value: String) -> Result<()> {
        // let command = APIStoreReq::KVSet(key, value, tx);
        // self.send(command).await?;
        // Ok(rx.await?)
        tokio::task::yield_now().await;
        unimplemented!()
    }
    pub async fn kv_get_local(&self, key: String) -> Result<Option<String>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::KVGet(key, tx);
        self.send(command).await?;
        Ok(rx.await?)
    }
}
