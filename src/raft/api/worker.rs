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

use crate::channel::{Receiver, Sender};
use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    ids::AppId,
    raft::{api::APIStoreReq, store::Store},
};

use super::apps::AppState;

async fn send<T>(tx: Sender<T>, t: T) {
    if let Err(e) = tx.send(t).await {
        error!("Error sending response to API: {e}");
    }
}

pub(super) async fn api_worker(store: Arc<Store>, mut store_rx: Receiver<APIStoreReq>) {
    while let Some(msg) = store_rx.recv().await {
        match msg {
            APIStoreReq::GetApp(app_id, tx) => {
                let sm = store.state_machine.read().await;
                send(tx, sm.apps.get_app(&app_id).cloned()).await;
            }
            APIStoreReq::GetApps(tx) => {
                let sm = store.state_machine.read().await;
                send(
                    tx,
                    sm.apps
                        .list()
                        .map(|(k, v)| (k.clone(), AppState::from(v)))
                        .collect::<HashMap<AppId, AppState>>(),
                )
                .await;
            }
            APIStoreReq::KVGet(k, tx) => {
                let sm = store.state_machine.read().await;
                let v = sm.kv.get(k.as_str()).ok().flatten(); // return errors as not-found here, this might be bad
                send(tx, v).await;
            }
            APIStoreReq::GetNode(node_id, tx) => {
                let sm = store.state_machine.read().await;
                let node = sm.nodes.get_node(node_id).cloned();
                send(tx, node).await;
            }
            APIStoreReq::GetNodes(tx) => {
                let sm = store.state_machine.read().await;
                let nodes = sm.nodes.get_nodes().clone();
                send(tx, nodes).await;
            }
            APIStoreReq::GetNodeId(addr, tx) => {
                let sm = store.state_machine.read().await;
                let node_id = sm.nodes.find_node_id(&addr).copied();
                send(tx, node_id).await;
            }
            APIStoreReq::GetLastMembership(tx) => {
                let sm = store.state_machine.read().await;
                let membership = sm.get_last_membership().ok().flatten(); // return errors as option here, this might be bad
                let last_membership = membership
                    .and_then(|m| m.membership.get_configs().last().cloned())
                    .unwrap_or_default();
                send(tx, last_membership).await;
            }
        }
    }
    info!("API Worker done.");
}
