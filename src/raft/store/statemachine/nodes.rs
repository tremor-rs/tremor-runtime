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

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use halfbrown::HashMap;
use openraft::NodeId;
use rocksdb::ColumnFamily;

use crate::{
    raft::{
        node::Addr,
        store::{
            bin_to_id, id_to_bin, store_w_err, Error as StoreError, StorageResult, TremorResponse,
        },
        store::{
            statemachine::{sm_d_err, sm_w_err, RaftStateMachine},
            NodesRequest,
        },
    },
    system::Runtime,
};

#[derive(Debug, Clone)]
pub(crate) struct NodesStateMachine {
    db: Arc<rocksdb::DB>,
    next_node_id: Arc<AtomicU64>,
    known_nodes: HashMap<NodeId, Addr>,
}

impl NodesStateMachine {
    const CF: &str = "nodes";
    const NEXT_NODE_ID: &str = "next_node_id";
    const COLUMN_FAMILIES: [&'static str; 1] = [Self::CF];

    fn cf(db: &Arc<rocksdb::DB>) -> Result<&ColumnFamily, StoreError> {
        db.cf_handle(Self::CF)
            .ok_or(StoreError::MissingCf(Self::CF))
    }
}

#[async_trait::async_trait]
impl RaftStateMachine<NodesSnapshot, NodesRequest> for NodesStateMachine {
    async fn load(db: &Arc<rocksdb::DB>, _world: &Runtime) -> Result<Self, StoreError>
    where
        Self: std::marker::Sized,
    {
        // load known nodes
        let next_node_id =
            if let Some(next_node_id) = db.get_cf(Self::cf(db)?, Self::NEXT_NODE_ID)? {
                bin_to_id(next_node_id.as_slice())?
            } else {
                debug!("No next_node_id stored in db, starting from 0");
                0
            };
        let known_nodes = db
            .iterator_cf(Self::cf(db)?, rocksdb::IteratorMode::Start)
            .filter(|x| {
                // filter out NEXT_NODE_ID
                if let Ok((key_raw, _)) = x {
                    key_raw.as_ref() != Self::NEXT_NODE_ID.as_bytes()
                } else {
                    true
                }
            })
            .map(|x| {
                let (key_raw, value_raw) = x?;
                let node_id = bin_to_id(&key_raw)?;
                let addr: Addr = serde_json::from_slice(&value_raw)?;
                Ok((node_id, addr))
            })
            .collect::<Result<HashMap<NodeId, Addr>, StoreError>>()?;

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

        Ok(TremorResponse {
            value: Some(node_id.to_string()),
        })
    }
    fn column_families() -> &'static [&'static str] {
        &Self::COLUMN_FAMILIES
    }
}

impl NodesStateMachine {
    fn next_node_id(&self) -> StorageResult<NodeId> {
        let s = self.next_node_id.fetch_add(1, Ordering::SeqCst);
        let s_bytes = id_to_bin(s)?;
        self.db
            .put_cf(Self::cf(&self.db)?, Self::NEXT_NODE_ID, s_bytes)
            .map_err(sm_w_err)?;
        Ok(s)
    }

    fn set_next_node_id(&mut self, next_node_id: u64) -> StorageResult<u64> {
        let bytes = id_to_bin(next_node_id)?;
        self.db
            .put_cf(Self::cf(&self.db)?, Self::NEXT_NODE_ID, bytes)
            .map_err(sm_w_err)?;
        let res = self.next_node_id.swap(next_node_id, Ordering::SeqCst);
        Ok(res)
    }

    /// find a `NodeId` for the given `addr` if it is already stored
    pub(crate) fn find_node_id(&self, addr: &Addr) -> Option<&NodeId> {
        self.known_nodes
            .iter()
            .find(|(_node_id, existing_addr)| *existing_addr == addr)
            .map(|(node_id, _)| node_id)
    }

    /// get the `Addr` of the node identified by `node_id` if it is stored
    pub(crate) fn get_node(&self, node_id: NodeId) -> Option<&Addr> {
        self.known_nodes.get(&node_id)
    }

    /// get all known nodes with their `NodeId` and `Addr`
    pub(crate) fn get_nodes(&self) -> &HashMap<NodeId, Addr> {
        &self.known_nodes
    }

    /// add the node identified by `addr` if not already there and assign and return a new `node_id`
    fn add_node(&mut self, addr: &Addr) -> StorageResult<NodeId> {
        if let Some(node_id) = self.find_node_id(addr) {
            Err(store_w_err(StoreError::NodeAlreadyAdded(*node_id)))
        } else {
            let node_id = self.next_node_id()?;
            self.store_node(node_id, addr)?;
            debug!(target: "TremorStateMachine::add_node", "Node {addr} added as node {node_id}");
            Ok(node_id)
        }
    }

    /// store the given addr under the given `node_id`, without creating a new one
    fn store_node(&mut self, node_id: NodeId, addr: &Addr) -> StorageResult<()> {
        let node_id_bytes = id_to_bin(node_id)?;
        self.db
            .put_cf(
                Self::cf(&self.db)?,
                node_id_bytes,
                serde_json::to_vec(addr).map_err(sm_w_err)?,
            )
            .map_err(sm_w_err)?;
        self.known_nodes.insert(node_id, addr.clone());
        Ok(())
    }

    fn remove_node(&mut self, node_id: NodeId) -> StorageResult<()> {
        self.known_nodes.remove(&node_id);
        let node_id_bytes = id_to_bin(node_id)?;
        self.db
            .delete_cf(Self::cf(&self.db)?, node_id_bytes)
            .map_err(sm_d_err)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct NodesSnapshot {
    next_node_id: u64,
    nodes: HashMap<NodeId, Addr>,
}
