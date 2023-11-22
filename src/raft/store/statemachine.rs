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

use crate::{
    raft::{
        store::{
            self, statemachine::nodes::NodesStateMachine, StorageResult, TremorRequest,
            TremorResponse,
        },
        SillyError,
    },
    system::Runtime,
};
use openraft::{
    AnyError, ErrorSubject, ErrorVerb, LogId, StorageError, StorageIOError, StoredMembership,
};
use redb::{Database, ReadableTable};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc};

use super::STATE_MACHINE;

// apps related state machine
pub(crate) mod apps;
// nodes related state machine
mod nodes;
// kv state machine
mod kv;

pub(crate) fn r_err<E: Into<anyhow::Error>>(e: E) -> StorageError<crate::raft::NodeId> {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Read,
        AnyError::new(&SillyError(e.into())),
    )
    .into()
}
pub(crate) fn w_err<E: Into<anyhow::Error>>(e: E) -> StorageError<crate::raft::NodeId> {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Write,
        AnyError::new(&SillyError(e.into())),
    )
    .into()
}
fn d_err<E: Into<anyhow::Error>>(e: E) -> StorageError<crate::raft::NodeId> {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Delete,
        AnyError::new(&SillyError(e.into())),
    )
    .into()
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SerializableTremorStateMachine {
    pub last_applied_log: Option<LogId<crate::raft::NodeId>>,

    pub last_membership: Option<StoredMembership<crate::raft::NodeId, crate::raft::node::Addr>>,

    /// Application data, for the k/v store
    pub(crate) kv: kv::KvSnapshot,

    pub(crate) apps: apps::AppsSnapshot,

    /// nodes known to the cluster (learners and voters)
    /// necessary for establishing a network connection to them
    pub(crate) nodes: nodes::NodesSnapshot,
}

impl SerializableTremorStateMachine {
    pub(crate) fn to_vec(&self) -> StorageResult<Vec<u8>> {
        rmp_serde::to_vec(&self).map_err(r_err)
    }
}

impl TryFrom<&TremorStateMachine> for SerializableTremorStateMachine {
    type Error = StorageError<crate::raft::NodeId>;

    fn try_from(state: &TremorStateMachine) -> Result<Self, Self::Error> {
        let nodes = state.nodes.as_snapshot()?;
        let kv = state.kv.as_snapshot()?;
        let apps = state.apps.as_snapshot()?;
        let last_membership = state.get_last_membership()?;
        let last_applied_log = state.get_last_applied_log()?;
        Ok(Self {
            last_applied_log,
            last_membership,
            kv,
            apps,
            nodes,
        })
    }
}

/// abstract raft statemachine for implementing sub-statemachines
#[async_trait::async_trait]
trait RaftStateMachine<Ser: Serialize + Deserialize<'static>, Cmd> {
    async fn load(db: &Arc<Database>, world: &Runtime) -> Result<Self, store::Error>
    where
        Self: std::marker::Sized;
    async fn apply_diff_from_snapshot(&mut self, snapshot: &Ser) -> StorageResult<()>;
    fn as_snapshot(&self) -> StorageResult<Ser>;
    async fn transition(&mut self, cmd: &Cmd) -> StorageResult<TremorResponse>;
}

#[derive(Debug, Clone)]
pub(crate) struct TremorStateMachine {
    /// sub-statemachine for nodes known to the cluster
    pub(crate) nodes: nodes::NodesStateMachine,
    pub(crate) kv: kv::KvStateMachine,
    pub(crate) apps: apps::AppsStateMachine,
    pub db: Arc<Database>,
}

/// DB Helpers
impl TremorStateMachine {
    /// storing state machine related stuff

    const LAST_MEMBERSHIP: &'static str = "last_membership";
    const LAST_APPLIED_LOG: &'static str = "last_applied_log";
}

/// Core impl
impl TremorStateMachine {
    pub(crate) async fn new(
        db: Arc<redb::Database>,
        world: Runtime,
    ) -> Result<TremorStateMachine, store::Error> {
        let nodes = NodesStateMachine::load(&db, &world).await?;
        let kv = kv::KvStateMachine::load(&db, &world).await?;
        let apps = apps::AppsStateMachine::load(&db, &world).await?;
        Ok(Self {
            db: db.clone(),
            nodes,
            kv,
            apps,
        })
    }

    fn put<T>(&self, key: &str, value: &T) -> StorageResult<()>
    where
        T: serde::Serialize,
    {
        let write_txn = self.db.begin_write().map_err(w_err)?;
        {
            let mut table = write_txn.open_table(STATE_MACHINE).map_err(w_err)?;
            table
                .insert(key, rmp_serde::to_vec(value).map_err(w_err)?)
                .map_err(w_err)?;
        }
        write_txn.commit().map_err(w_err)?;
        Ok(())
    }

    fn get<T>(&self, key: &str) -> StorageResult<Option<T>>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        // We need to use a write transaction despite just wanting a read transaction due to
        // https://github.com/cberner/redb/issues/711
        let bug_fix_txn = self.db.begin_write().map_err(w_err)?;
        {
            // ALLOW: this is just a workaround
            let _argh = bug_fix_txn.open_table(STATE_MACHINE).map_err(w_err)?;
        }
        bug_fix_txn.commit().map_err(w_err)?;

        let read_txn = self.db.begin_read().map_err(r_err)?;

        let table = read_txn.open_table(STATE_MACHINE).map_err(r_err)?;
        let r = table
            .get(key)
            .map_err(r_err)?
            .map(|v| rmp_serde::from_slice(&v.value()))
            .transpose()
            .map_err(r_err)?;
        Ok(r)
    }

    pub(crate) fn get_last_membership(
        &self,
    ) -> StorageResult<Option<StoredMembership<crate::raft::NodeId, crate::raft::node::Addr>>> {
        self.get(Self::LAST_MEMBERSHIP)
    }

    pub(crate) fn set_last_membership(
        &self,
        membership: &StoredMembership<crate::raft::NodeId, crate::raft::node::Addr>,
    ) -> StorageResult<()> {
        self.put(Self::LAST_MEMBERSHIP, &membership)
    }

    pub(crate) fn get_last_applied_log(&self) -> StorageResult<Option<LogId<crate::raft::NodeId>>> {
        self.get(Self::LAST_APPLIED_LOG)
    }

    pub(crate) fn set_last_applied_log(
        &self,
        log_id: LogId<crate::raft::NodeId>,
    ) -> StorageResult<()> {
        self.put(Self::LAST_APPLIED_LOG, &log_id)
    }

    fn delete_last_applied_log(&self) -> StorageResult<()> {
        let write_txn = self.db.begin_write().map_err(d_err)?;
        {
            let mut table = write_txn.open_table(STATE_MACHINE).map_err(d_err)?;
            table.remove(Self::LAST_APPLIED_LOG).map_err(d_err)?;
        }

        write_txn.commit().map_err(d_err)
    }

    // TODO: reason about error handling and avoid leaving the state machine in an inconsistent state
    pub(crate) async fn apply_diff_from_snapshot(
        &mut self,
        snapshot: SerializableTremorStateMachine,
    ) -> StorageResult<()> {
        // apply snapshot on the nodes state machine
        self.nodes.apply_diff_from_snapshot(&snapshot.nodes).await?;
        self.kv.apply_diff_from_snapshot(&snapshot.kv).await?;
        self.apps.apply_diff_from_snapshot(&snapshot.apps).await?;

        // TODO: delete every key that is not in the snapshot - not necessarily needed today as we don't have a DELETE op on our k/v store

        if let Some(log_id) = snapshot.last_applied_log {
            self.set_last_applied_log(log_id)?;
        } else {
            self.delete_last_applied_log()?;
        }

        if let Some(last_membership) = &snapshot.last_membership {
            self.set_last_membership(last_membership)?;
        }

        Ok(())
    }

    pub(crate) async fn handle_request(
        &mut self,
        log_id: LogId<crate::raft::NodeId>,
        req: &TremorRequest,
    ) -> StorageResult<TremorResponse> {
        match req {
            TremorRequest::Kv(kv) => {
                debug!("[{log_id}] set");
                self.kv.transition(kv).await
            }
            // forward to nodes statemachine
            TremorRequest::Nodes(nodes_req) => self.nodes.transition(nodes_req).await,
            TremorRequest::Apps(apps_req) => self.apps.transition(apps_req).await,
        }
    }
}
