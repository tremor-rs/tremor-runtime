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
    raft::store::{
        self, statemachine::nodes::NodesStateMachine, StorageResult, TremorRequest, TremorResponse,
    },
    system::Runtime,
};
use openraft::{
    AnyError, EffectiveMembership, ErrorSubject, ErrorVerb, LogId, StorageError, StorageIOError,
};
use rocksdb::ColumnFamily;
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt::Debug, sync::Arc};

// apps related state machine
pub(crate) mod apps;
// nodes related state machine
mod nodes;
// kv state machine
mod kv;

fn sm_r_err<E: Error + 'static>(e: E) -> StorageError {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Read,
        AnyError::new(&e),
    )
    .into()
}
fn sm_w_err<E: Error + 'static>(e: E) -> StorageError {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Write,
        AnyError::new(&e),
    )
    .into()
}
fn sm_d_err<E: Error + 'static>(e: E) -> StorageError {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Delete,
        AnyError::new(&e),
    )
    .into()
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SerializableTremorStateMachine {
    pub last_applied_log: Option<LogId>,

    pub last_membership: Option<EffectiveMembership>,

    /// Application data, for the k/v store
    pub(crate) kv: kv::KvSnapshot,

    pub(crate) apps: apps::AppsSnapshot,

    /// nodes known to the cluster (learners and voters)
    /// necessary for establishing a network connection to them
    pub(crate) nodes: nodes::NodesSnapshot,
}

impl SerializableTremorStateMachine {
    pub(crate) fn to_vec(&self) -> StorageResult<Vec<u8>> {
        serde_json::to_vec(&self).map_err(sm_r_err)
    }
}

impl TryFrom<&TremorStateMachine> for SerializableTremorStateMachine {
    type Error = StorageError;

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
    async fn load(db: &Arc<rocksdb::DB>, world: &Runtime) -> Result<Self, store::Error>
    where
        Self: std::marker::Sized;
    async fn apply_diff_from_snapshot(&mut self, snapshot: &Ser) -> StorageResult<()>;
    fn as_snapshot(&self) -> StorageResult<Ser>;
    async fn transition(&mut self, cmd: &Cmd) -> StorageResult<TremorResponse>;
    fn column_families() -> &'static [&'static str];
}

#[derive(Debug, Clone)]
pub(crate) struct TremorStateMachine {
    /// sub-statemachine for nodes known to the cluster
    pub(crate) nodes: nodes::NodesStateMachine,
    pub(crate) kv: kv::KvStateMachine,
    pub(crate) apps: apps::AppsStateMachine,

    pub db: Arc<rocksdb::DB>,
}

/// DB Helpers
impl TremorStateMachine {
    /// storing state machine related stuff
    const CF: &'static str = "state_machine";

    const LAST_MEMBERSHIP: &'static str = "last_membership";
    const LAST_APPLIED_LOG: &'static str = "last_applied_log";

    /// state machine column family
    fn cf_state_machine(&self) -> StorageResult<&ColumnFamily> {
        self.db
            .cf_handle(Self::CF)
            .ok_or(store::Error::MissingCf(Self::CF))
            .map_err(sm_w_err)
    }

    pub(super) fn column_families() -> impl Iterator<Item = &'static str> {
        let iter = nodes::NodesStateMachine::column_families()
            .iter()
            .copied()
            .chain(
                kv::KvStateMachine::column_families().iter().copied().chain(
                    apps::AppsStateMachine::column_families()
                        .iter()
                        .copied()
                        .chain([Self::CF].into_iter()),
                ),
            );
        iter
    }
}

/// Core impl
impl TremorStateMachine {
    pub(crate) async fn new(
        db: Arc<rocksdb::DB>,
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

    pub(crate) fn get_last_membership(&self) -> StorageResult<Option<EffectiveMembership>> {
        self.db
            .get_cf(self.cf_state_machine()?, Self::LAST_MEMBERSHIP)
            .map_err(sm_r_err)
            .and_then(|value| {
                value
                    .map(|v| serde_json::from_slice(&v).map_err(sm_r_err))
                    .transpose()
            })
    }

    pub(crate) fn set_last_membership(
        &self,
        membership: &EffectiveMembership,
    ) -> StorageResult<()> {
        self.db
            .put_cf(
                self.cf_state_machine()?,
                Self::LAST_MEMBERSHIP,
                serde_json::to_vec(&membership).map_err(sm_w_err)?,
            )
            .map_err(sm_w_err)
    }

    pub(crate) fn get_last_applied_log(&self) -> StorageResult<Option<LogId>> {
        self.db
            .get_cf(self.cf_state_machine()?, Self::LAST_APPLIED_LOG)
            .map_err(sm_r_err)
            .and_then(|value| {
                value
                    .map(|v| serde_json::from_slice(&v).map_err(sm_r_err))
                    .transpose()
            })
    }

    pub(crate) fn set_last_applied_log(&self, log_id: LogId) -> StorageResult<()> {
        self.db
            .put_cf(
                self.cf_state_machine()?,
                Self::LAST_APPLIED_LOG,
                serde_json::to_vec(&log_id).map_err(sm_w_err)?,
            )
            .map_err(sm_w_err)
    }

    fn delete_last_applied_log(&self) -> StorageResult<()> {
        self.db
            .delete_cf(self.cf_state_machine()?, Self::LAST_APPLIED_LOG)
            .map_err(sm_d_err)
    }

    // FIXME: reason about error handling and avoid leaving the state machine in an inconsistent state
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
        log_id: LogId,
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
