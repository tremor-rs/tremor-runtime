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
//! storing key value stores in rocksdb only

use std::{collections::BTreeMap, marker::Sized, sync::Arc};

use crate::{
    raft::store::{
        self,
        statemachine::{sm_r_err, sm_w_err, RaftStateMachine},
        store_r_err, store_w_err, Error as StoreError, KvRequest, StorageResult, TremorResponse,
    },
    system::Runtime,
};
use openraft::StorageError;
use rocksdb::ColumnFamily;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct KvSnapshot(BTreeMap<String, String>);

#[derive(Clone, Debug)]
pub(crate) struct KvStateMachine {
    db: Arc<rocksdb::DB>,
}

impl KvStateMachine {
    const CF: &str = "data";

    fn cf(db: &Arc<rocksdb::DB>) -> Result<&ColumnFamily, StoreError> {
        db.cf_handle(Self::CF)
            .ok_or(StoreError::MissingCf(Self::CF))
    }

    /// Store `value` at `key` in the distributed KV store
    fn insert(&self, key: &str, value: &str) -> StorageResult<()> {
        self.db
            .put_cf(Self::cf(&self.db)?, key.as_bytes(), value.as_bytes())
            .map_err(store_w_err)
    }

    /// try to obtain the value at the given `key`.
    /// Returns `Ok(None)` if there is no value for that key.
    pub(crate) fn get(&self, key: &str) -> StorageResult<Option<String>> {
        let key = key.as_bytes();
        self.db
            .get_cf(Self::cf(&self.db)?, key)
            .map(|value| {
                if let Some(value) = value {
                    Some(String::from_utf8(value).ok()?)
                } else {
                    None
                }
            })
            .map_err(store_r_err)
    }
}

#[async_trait::async_trait]
impl RaftStateMachine<KvSnapshot, KvRequest> for KvStateMachine {
    async fn load(db: &Arc<rocksdb::DB>, _world: &Runtime) -> Result<Self, store::Error>
    where
        Self: Sized,
    {
        Ok(Self { db: db.clone() })
    }

    async fn apply_diff_from_snapshot(&mut self, snapshot: &KvSnapshot) -> StorageResult<()> {
        for (key, value) in &snapshot.0 {
            self.db
                .put_cf(Self::cf(&self.db)?, key.as_bytes(), value.as_bytes())
                .map_err(sm_w_err)?;
        }
        Ok(())
    }

    fn as_snapshot(&self) -> StorageResult<KvSnapshot> {
        let data = self
            .db
            .iterator_cf(
                self.db
                    .cf_handle(Self::CF)
                    .ok_or(store::Error::MissingCf(Self::CF))?,
                rocksdb::IteratorMode::Start,
            )
            .map(|kv| {
                let (key, value) = kv.map_err(sm_r_err)?;
                Ok((
                    String::from_utf8(key.to_vec()).map_err(sm_r_err)?,
                    String::from_utf8(value.to_vec()).map_err(sm_r_err)?,
                ))
            })
            .collect::<Result<BTreeMap<String, String>, StorageError>>()?;
        Ok(KvSnapshot(data))
    }

    async fn transition(&mut self, cmd: &KvRequest) -> StorageResult<TremorResponse> {
        match cmd {
            KvRequest::Set { key, value } => {
                self.insert(key, value)?;
                Ok(TremorResponse {
                    value: Some(value.clone()),
                })
            }
        }
    }

    fn create_column_families(db: &mut rocksdb::DB) -> StorageResult<()> {
        if db.cf_handle(Self::CF).is_none() {
            db.create_cf(Self::CF, &rocksdb::Options::default())
                .map_err(sm_w_err)?;
        }
        Ok(())
    }
}
