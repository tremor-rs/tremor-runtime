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

//! nodes raft sub-statemachine
//! handling all the nodes that are known to the cluster and is responsible for assigning node ids

use redb::ReadableTable;

use crate::{
    raft::{
        node::Addr,
        store::{
            bin_to_id, id_to_bin,
            statemachine::{d_err, w_err, RaftStateMachine},
            Error as StoreError, NodesRequest, StorageResult, TremorResponse, NODES, SYSTEM,
        },
    },
    system::Runtime,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use super::r_err;

#[derive(Debug, Clone)]
pub(crate) struct NodesStateMachine {
    db: Arc<redb::Database>,
    next_node_id: Arc<AtomicU64>,
    known_nodes: HashMap<crate::raft::NodeId, Addr>,
}

impl NodesStateMachine {
    const NEXT_NODE_ID: &str = "next_node_id";
}

#[async_trait::async_trait]
impl RaftStateMachine<NodesSnapshot, NodesRequest> for NodesStateMachine {
    async fn load(db: &Arc<redb::Database>, _world: &Runtime) -> Result<Self, StoreError>
    where
        Self: std::marker::Sized,
    {
        // load known nodes
        let read_txn = db.begin_read().map_err(r_err)?;

        let table = read_txn.open_table(SYSTEM).map_err(r_err)?;
        let next_node_id =
            if let Some(next_node_id) = table.get(Self::NEXT_NODE_ID).map_err(r_err)? {
                bin_to_id(next_node_id.value().as_slice())?
            } else {
                debug!("No next_node_id stored in db, starting from 0");
                0
            };
        let table = read_txn.open_table(NODES).map_err(r_err)?;

        let known_nodes = table
            .iter()
            .map_err(r_err)?
            .map(|x| {
                let (node_id, value_raw) = x.map_err(r_err)?;

                let addr: Addr = rmp_serde::from_slice(value_raw.value())?;
                Ok((node_id.value(), addr))
            })
            .collect::<Result<HashMap<crate::raft::NodeId, Addr>, StoreError>>()?;

        Ok(Self {
            db: db.clone(),
            next_node_id: Arc::new(AtomicU64::new(next_node_id)),
            known_nodes,
        })
    }

    async fn apply_diff_from_snapshot(&mut self, snapshot: &NodesSnapshot) -> StorageResult<()> {
        // overwrite the current next_node_id
        self.set_next_node_id(snapshot.next_node_id)?;

        // overwrite the nodes in the db and the local map
        // TODO: should we remove all nodes not in the snapshot?
        for (node_id, addr) in &snapshot.nodes {
            self.store_node(*node_id, addr)?;
        }
        Ok(())
    }

    fn as_snapshot(&self) -> StorageResult<NodesSnapshot> {
        Ok(NodesSnapshot {
            next_node_id: self.next_node_id.load(Ordering::SeqCst),
            nodes: self.known_nodes.clone(),
        })
    }

    async fn transition(&mut self, cmd: &NodesRequest) -> StorageResult<TremorResponse> {
        let node_id = match cmd {
            NodesRequest::AddNode { addr } => self.add_node(addr)?,
            NodesRequest::RemoveNode { node_id } => {
                self.remove_node(*node_id)?;
                *node_id
            }
        };

        Ok(TremorResponse::NodeId(node_id))
    }
}

impl NodesStateMachine {
    fn next_node_id(&self) -> StorageResult<crate::raft::NodeId> {
        let s = self.next_node_id.fetch_add(1, Ordering::SeqCst);
        let s_bytes = id_to_bin(s)?;

        let write_txn = self.db.begin_write().map_err(w_err)?;
        {
            let mut table = write_txn.open_table(SYSTEM).map_err(w_err)?;

            table.insert(Self::NEXT_NODE_ID, s_bytes).map_err(w_err)?;
        }
        write_txn.commit().map_err(w_err)?;
        Ok(s)
    }

    fn set_next_node_id(&mut self, next_node_id: u64) -> StorageResult<u64> {
        let bytes = id_to_bin(next_node_id)?;
        let write_txn = self.db.begin_write().map_err(w_err)?;
        {
            let mut table = write_txn.open_table(SYSTEM).map_err(w_err)?;

            table.insert(Self::NEXT_NODE_ID, bytes).map_err(w_err)?;
        }
        write_txn.commit().map_err(w_err)?;

        let res = self.next_node_id.swap(next_node_id, Ordering::SeqCst);
        Ok(res)
    }

    /// find a `NodeId` for the given `addr` if it is already stored
    pub(crate) fn find_node_id(&self, addr: &Addr) -> Option<&crate::raft::NodeId> {
        self.known_nodes
            .iter()
            .find(|(_node_id, existing_addr)| *existing_addr == addr)
            .map(|(node_id, _)| node_id)
    }

    /// get the `Addr` of the node identified by `node_id` if it is stored
    pub(crate) fn get_node(&self, node_id: crate::raft::NodeId) -> Option<&Addr> {
        self.known_nodes.get(&node_id)
    }

    /// get all known nodes with their `NodeId` and `Addr`
    pub(crate) fn get_nodes(&self) -> &HashMap<crate::raft::NodeId, Addr> {
        &self.known_nodes
    }

    /// add the node identified by `addr` if not already there and assign and return a new `node_id`
    fn add_node(&mut self, addr: &Addr) -> StorageResult<crate::raft::NodeId> {
        if let Some(node_id) = self.find_node_id(addr) {
            Err(w_err(StoreError::NodeAlreadyAdded(*node_id)))
        } else {
            let node_id = self.next_node_id()?;
            self.store_node(node_id, addr)?;
            debug!(target: "TremorStateMachine::add_node", "Node {addr} added as node {node_id}");
            Ok(node_id)
        }
    }

    /// store the given addr under the given `node_id`, without creating a new one
    fn store_node(&mut self, node_id: crate::raft::NodeId, addr: &Addr) -> StorageResult<()> {
        let write_txn = self.db.begin_write().map_err(w_err)?;
        {
            let mut table = write_txn.open_table(NODES).map_err(w_err)?;

            table
                .insert(node_id, rmp_serde::to_vec(addr).map_err(w_err)?.as_slice())
                .map_err(w_err)?;
        }
        self.known_nodes.insert(node_id, addr.clone());
        write_txn.commit().map_err(w_err)
    }

    fn remove_node(&mut self, node_id: crate::raft::NodeId) -> StorageResult<()> {
        let write_txn = self.db.begin_write().map_err(d_err)?;
        {
            let mut table = write_txn.open_table(NODES).map_err(d_err)?;
            table.remove(node_id).map_err(d_err)?;
        }
        self.known_nodes.remove(&node_id);
        write_txn.commit().map_err(d_err)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct NodesSnapshot {
    next_node_id: u64,
    nodes: HashMap<crate::raft::NodeId, Addr>,
}
