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

//! KV raft sub-statemachine
//! storing key value stores in database only

use crate::{
    raft::{
        store::{
            self,
            statemachine::{r_err, w_err, RaftStateMachine},
            KvRequest, StorageResult, TremorResponse, DATA,
        },
        NodeId,
    },
    system::Runtime,
};
use openraft::StorageError;
use redb::{Database, ReadableTable};
use std::{collections::BTreeMap, marker::Sized, sync::Arc};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct KvSnapshot(BTreeMap<String, String>);

#[derive(Clone, Debug)]
pub(crate) struct KvStateMachine {
    db: Arc<Database>,
}

impl KvStateMachine {
    /// Store `value` at `key` in the distributed KV store
    fn insert(&self, key: &str, value: &[u8]) -> StorageResult<()> {
        let write_txn = self.db.begin_write().map_err(w_err)?;
        {
            let mut table = write_txn.open_table(DATA).map_err(w_err)?;
            table.insert(key, value).map_err(w_err)?;
        }
        write_txn.commit().map_err(w_err)
    }

    /// try to obtain the value at the given `key`.
    /// Returns `Ok(None)` if there is no value for that key.
    pub(crate) fn get(&self, key: &str) -> StorageResult<Option<Vec<u8>>> {
        let read_txn = self.db.begin_read().map_err(r_err)?;
        let table = read_txn.open_table(DATA).map_err(r_err)?;
        table
            .get(key)
            .map_err(r_err)
            .map(|v| v.map(|v| v.value().to_vec()))
    }
}

#[async_trait::async_trait]
impl RaftStateMachine<KvSnapshot, KvRequest> for KvStateMachine {
    async fn load(db: &Arc<Database>, _world: &Runtime) -> Result<Self, store::Error>
    where
        Self: Sized,
    {
        Ok(Self { db: db.clone() })
    }

    async fn apply_diff_from_snapshot(&mut self, snapshot: &KvSnapshot) -> StorageResult<()> {
        let write_txn = self.db.begin_write().map_err(w_err)?;
        {
            let mut table = write_txn.open_table(DATA).map_err(w_err)?;
            for (key, value) in &snapshot.0 {
                table
                    .insert(key.as_str(), value.as_bytes())
                    .map_err(w_err)?;
            }
        }
        write_txn.commit().map_err(w_err)?;
        Ok(())
    }

    fn as_snapshot(&self) -> StorageResult<KvSnapshot> {
        let read_tnx = self.db.begin_read().map_err(w_err)?;
        let table = read_tnx.open_table(DATA).map_err(w_err)?;
        let data = table
            .iter()
            .map_err(w_err)?
            .map(|kv| {
                let (key, value) = kv.map_err(r_err)?;
                Ok((
                    String::from(key.value()),
                    String::from_utf8(value.value().to_vec()).map_err(r_err)?,
                ))
            })
            .collect::<Result<BTreeMap<String, String>, StorageError<NodeId>>>()?;
        Ok(KvSnapshot(data))
    }

    async fn transition(&mut self, cmd: &KvRequest) -> StorageResult<TremorResponse> {
        match cmd {
            KvRequest::Set { key, value } => {
                self.insert(key, value)?;
                Ok(TremorResponse::KvValue(value.clone()))
            }
        }
    }
}
