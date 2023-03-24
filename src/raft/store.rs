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

mod statemachine;

pub(crate) use self::statemachine::apps::{FlowInstance, Instances, StateApp};
use self::statemachine::{SerializableTremorStateMachine, TremorStateMachine};
use crate::{
    channel::Sender,
    errors::Error as RuntimeError,
    ids::{AppFlowInstanceId, AppId, FlowDefinitionId},
    instance::IntendedState,
    raft::{archive::TremorAppDef, ClusterError},
    system::Runtime,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use openraft::{
    async_trait::async_trait,
    storage::{LogState, Snapshot},
    AnyError, AppData, AppDataResponse, EffectiveMembership, Entry, EntryPayload, ErrorSubject,
    ErrorVerb, HardState, LogId, RaftStorage, SnapshotMeta, StateMachineChanges, StorageError,
    StorageIOError,
};
use rocksdb::{ColumnFamily, Direction, FlushOptions, Options, DB};
use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use std::{
    error::Error as StdError,
    fmt::{Debug, Display, Formatter},
    io::Cursor,
    ops::RangeBounds,
    path::Path,
    string::FromUtf8Error,
    sync::{Arc, Mutex},
};
use tokio::sync::RwLock;

use super::{api::APIStoreReq, node::Addr};

/// Kv Operation
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KvRequest {
    /// Set a key to the provided value in the cluster state
    Set { key: String, value: String },
}

/// Operations on the nodes known to the cluster
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum NodesRequest {
    /// Add the given node to the cluster and store its metadata (only addr for now)
    /// This command should be committed before a learner is added to the cluster, so the leader can contact it via its `addr`.
    AddNode { addr: Addr },
    /// Remove Node with the given `node_id`
    /// This command should be committed after removing a learner from the cluster.
    RemoveNode { node_id: openraft::NodeId },
}

/// Operations on apps and their instances
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AppsRequest {
    /// extract archive, parse sources, save sources in arena, put app into state machine
    InstallApp { app: TremorAppDef, file: Vec<u8> },
    /// delete from statemachine, delete sources from arena
    UninstallApp {
        app: AppId,
        /// if `true`, stop all instances of this app
        force: bool,
    },
    /// Deploy and Start a flow of an installed app
    Deploy {
        app: AppId,
        flow: FlowDefinitionId,
        instance: AppFlowInstanceId,
        config: std::collections::HashMap<String, OwnedValue>,
        state: IntendedState,
    },

    /// Stopps and Undeploys an instance of a app
    Undeploy(AppFlowInstanceId),

    /// Requests a instance state change
    InstanceStateChange {
        instance: AppFlowInstanceId,
        state: IntendedState,
    },
}

/**
 * Here you will set the types of request that will interact with the raft nodes.
 * For example the `Set` will be used to write data (key and value) to the raft database.
 * The `AddNode` will append a new node to the current existing shared list of nodes.
 * You will want to add any request that can write data in all nodes here.
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TremorRequest {
    /// KV operation
    Kv(KvRequest),
    Nodes(NodesRequest),
    Apps(AppsRequest),
}

impl AppData for TremorRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TremorStart {
    pub(crate) instance: AppFlowInstanceId,
    pub(crate) config: std::collections::HashMap<String, OwnedValue>,
    pub(crate) running: bool,
}
impl TremorStart {
    pub(crate) fn state(&self) -> IntendedState {
        if self.running {
            IntendedState::Running
        } else {
            IntendedState::Paused
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
pub enum TremorInstanceState {
    /// Pauses a running instance
    Pause,
    /// Resumes a paused instance
    Resume,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TremorSet {
    pub key: String,
    pub value: String,
}

impl From<TremorSet> for TremorRequest {
    fn from(set: TremorSet) -> Self {
        TremorRequest::Kv(KvRequest::Set {
            key: set.key,
            value: set.value,
        })
    }
}

/**
 * Here you will defined what type of answer you expect from reading the data of a node.
 * In this example it will return a optional value from a given key in
 * the `ExampleRequest.Set`.
 *
 * TODO: `SHould` we explain how to create multiple `AppDataResponse`?
 *
 */
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TremorResponse {
    pub value: Option<String>,
}

impl AppDataResponse for TremorResponse {}

#[derive(Serialize, Deserialize, Debug)]
pub struct TremorSnapshot {
    pub meta: SnapshotMeta,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct Store {
    /// The Raft state machine.
    pub(crate) state_machine: RwLock<TremorStateMachine>,
    // the database
    db: Arc<rocksdb::DB>,
}

type StorageResult<T> = Result<T, StorageError>;

/// converts an id to a byte vector for storing in the database.
/// Note that we're using big endian encoding to ensure correct sorting of keys
fn id_to_bin(id: u64) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::with_capacity(8);
    buf.write_u64::<BigEndian>(id)?;
    Ok(buf)
}

fn bin_to_id(buf: &[u8]) -> Result<u64, Error> {
    Ok(buf
        .get(0..8)
        .ok_or_else(|| Error::Other(format!("Invalid buffer length: {}", buf.len()).into()))?
        .read_u64::<BigEndian>()?)
}

#[derive(Debug)]
pub enum Error {
    MissingCf(&'static str),
    Utf8(FromUtf8Error),
    StrUtf8(std::str::Utf8Error),
    JSON(serde_json::Error),
    RocksDB(rocksdb::Error),
    Io(std::io::Error),
    Storage(openraft::StorageError),
    // TODO: this is horrid, aaaaaahhhhh!
    Tremor(Mutex<RuntimeError>),
    TremorScript(Mutex<tremor_script::errors::Error>),
    MissingApp(AppId),
    MissingFlow(AppId, FlowDefinitionId),
    MissingInstance(AppFlowInstanceId),
    RunningInstances(AppId),
    NodeAlreadyAdded(openraft::NodeId),
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl<T: Send + Sync + 'static> From<std::sync::PoisonError<T>> for Error {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        Self::Other(Box::new(e))
    }
}

impl From<FromUtf8Error> for Error {
    fn from(e: FromUtf8Error) -> Self {
        Error::Utf8(e)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Self {
        Error::StrUtf8(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::JSON(e)
    }
}

impl From<rocksdb::Error> for Error {
    fn from(e: rocksdb::Error) -> Self {
        Error::RocksDB(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<crate::errors::Error> for Error {
    fn from(e: crate::errors::Error) -> Self {
        Error::Tremor(Mutex::new(e))
    }
}
impl From<tremor_script::errors::Error> for Error {
    fn from(e: tremor_script::errors::Error) -> Self {
        Error::TremorScript(Mutex::new(e))
    }
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        Error::Storage(e)
    }
}

impl StdError for Error {}
impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::MissingCf(cf) => write!(f, "missing column family: `{cf}`"),
            Error::Utf8(e) => write!(f, "invalid utf8: {e}"),
            Error::StrUtf8(e) => write!(f, "invalid utf8: {e}"),
            Error::JSON(e) => write!(f, "invalid json: {e}"),
            Error::RocksDB(e) => write!(f, "rocksdb error: {e}"),
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Tremor(e) => write!(f, "tremor error: {:?}", e.lock()),
            Error::TremorScript(e) => write!(f, "tremor script error: {:?}", e.lock()),
            Error::Other(e) => write!(f, "other error: {e}"),
            Error::MissingApp(app) => write!(f, "missing app: {app}"),
            Error::MissingFlow(app, flow) => write!(f, "missing flow: {app}::{flow}"),
            Error::MissingInstance(instance) => {
                write!(f, "missing instance: {instance}")
            }
            Error::Storage(e) => write!(f, "Storage: {e}"),
            Error::NodeAlreadyAdded(node_id) => write!(f, "Node {node_id} already added"),
            Error::RunningInstances(app_id) => {
                write!(f, "App {app_id} still has running instances")
            }
        }
    }
}

fn store_w_err(e: impl StdError + 'static) -> StorageError {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e)).into()
}
fn store_r_err(e: impl StdError + 'static) -> StorageError {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)).into()
}
fn logs_r_err(e: impl StdError + 'static) -> StorageError {
    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Read, AnyError::new(&e)).into()
}
fn logs_w_err(e: impl StdError + 'static) -> StorageError {
    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Read, AnyError::new(&e)).into()
}
fn snap_w_err(meta: &SnapshotMeta, e: impl StdError + 'static) -> StorageError {
    StorageIOError::new(
        ErrorSubject::Snapshot(meta.clone()),
        ErrorVerb::Write,
        AnyError::new(&e),
    )
    .into()
}

impl From<Error> for StorageError {
    fn from(e: Error) -> StorageError {
        StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)).into()
    }
}

pub(crate) trait GetCfHandle {
    fn cf(&self, cf: &'static str) -> Result<&ColumnFamily, Error>;

    /// store column family
    fn cf_store(&self) -> Result<&ColumnFamily, Error> {
        self.cf(Store::STORE)
    }

    fn cf_self(&self) -> Result<&ColumnFamily, Error> {
        self.cf(Store::SELF)
    }

    /// logs columns family
    fn cf_logs(&self) -> Result<&ColumnFamily, Error> {
        self.cf(Store::LOGS)
    }
}
impl GetCfHandle for rocksdb::DB {
    fn cf(&self, cf: &'static str) -> Result<&ColumnFamily, Error> {
        self.cf_handle(cf).ok_or(Error::MissingCf(cf))
    }
}

impl Store {
    pub(crate) fn flush(&self) -> Result<(), Error> {
        self.db.flush_wal(true)?;
        let mut opts = FlushOptions::default();
        opts.set_wait(true);
        self.db.flush_opt(&opts)?;
        Ok(())
    }

    fn put<K, V>(&self, cf: &ColumnFamily, key: K, value: V) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.db.put_cf(cf, key, value)?;
        self.flush()?;
        Ok(())
    }

    fn get_hard_state_(&self) -> StorageResult<Option<HardState>> {
        Ok(self
            .db
            .get_cf(self.db.cf_store()?, Self::HARD_STATE)
            .map_err(store_r_err)?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    fn set_hard_state_(&self, hard_state: &HardState) -> StorageResult<()> {
        self.put(
            self.db.cf_store()?,
            Self::HARD_STATE,
            serde_json::to_vec(hard_state)
                .map_err(Error::JSON)?
                .as_slice(),
        )
        .map_err(store_w_err)
    }

    fn get_last_purged_(&self) -> StorageResult<Option<LogId>> {
        Ok(self
            .db
            .get_cf(self.db.cf_store()?, Store::LAST_PURGED_LOG_ID)
            .map_err(store_r_err)?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    fn set_last_purged_(&self, log_id: &LogId) -> StorageResult<()> {
        self.put(
            self.db.cf_store()?,
            Self::LAST_PURGED_LOG_ID,
            serde_json::to_vec(log_id).map_err(Error::JSON)?.as_slice(),
        )
        .map_err(store_w_err)
    }

    fn get_snapshot_index_(&self) -> StorageResult<u64> {
        Ok(self
            .db
            .get_cf(self.db.cf_store()?, Self::SNAPSHOT_INDEX)
            .map_err(store_r_err)?
            .map(|v| serde_json::from_slice(&v).map_err(Error::JSON))
            .transpose()?
            .unwrap_or_default())
    }

    fn set_snapshot_index_(&self, snapshot_index: u64) -> StorageResult<()> {
        self.put(
            self.db.cf_store()?,
            Self::SNAPSHOT_INDEX,
            serde_json::to_vec(&snapshot_index)
                .map_err(store_w_err)?
                .as_slice(),
        )
        .map_err(store_w_err)?;
        Ok(())
    }

    fn get_current_snapshot_(&self) -> StorageResult<Option<TremorSnapshot>> {
        Ok(self
            .db
            .get_cf(self.db.cf_store()?, Self::SNAPSHOT)
            .map_err(store_r_err)?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    fn set_current_snapshot_(&self, snap: &TremorSnapshot) -> StorageResult<()> {
        self.put(
            self.db.cf_store()?,
            Self::SNAPSHOT,
            serde_json::to_vec(&snap).map_err(|e| snap_w_err(&snap.meta, e))?,
        )
        .map_err(|e| snap_w_err(&snap.meta, e))?;
        Ok(())
    }
}

#[async_trait]
impl RaftStorage<TremorRequest, TremorResponse> for Store {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn save_hard_state(&self, hs: &HardState) -> Result<(), StorageError> {
        self.set_hard_state_(hs)
    }

    async fn read_hard_state(&self) -> Result<Option<HardState>, StorageError> {
        self.get_hard_state_()
    }

    async fn get_log_state(&self) -> StorageResult<LogState> {
        let last = self
            .db
            .iterator_cf(self.db.cf_logs()?, rocksdb::IteratorMode::End)
            .next()
            .and_then(|d| {
                let (_, ent) = d.ok()?;
                Some(
                    serde_json::from_slice::<Entry<TremorRequest>>(&ent)
                        .ok()?
                        .log_id,
                )
            });

        let last_purged_log_id = self.get_last_purged_()?;

        let last_log_id = match last {
            None => last_purged_log_id,
            Some(x) => Some(x),
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &self,
        range: RB,
    ) -> StorageResult<Vec<Entry<TremorRequest>>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => id_to_bin(*x),
            std::ops::Bound::Excluded(x) => id_to_bin(*x + 1),
            std::ops::Bound::Unbounded => id_to_bin(0),
        }?;
        self.db
            .iterator_cf(
                self.db.cf_logs()?,
                rocksdb::IteratorMode::From(&start, Direction::Forward),
            )
            .map(|d| -> StorageResult<_> {
                let (id, val) = d.map_err(store_r_err)?;
                let entry: Entry<_> = serde_json::from_slice(&val).map_err(logs_r_err)?;
                debug_assert_eq!(bin_to_id(&id)?, entry.log_id.index);
                Ok(entry)
            })
            .take_while(|r| {
                r.as_ref()
                    .map(|e| range.contains(&e.log_id.index))
                    .unwrap_or(false)
            })
            .collect::<StorageResult<_>>()
    }

    // #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(&self, entries: &[&Entry<TremorRequest>]) -> StorageResult<()> {
        for entry in entries {
            let id = id_to_bin(entry.log_id.index)?;
            assert_eq!(bin_to_id(&id)?, entry.log_id.index);
            self.put(
                self.db.cf_logs()?,
                &id,
                serde_json::to_vec(entry).map_err(logs_w_err)?,
            )
            .map_err(logs_w_err)?;
        }
        Ok(())
    }

    // #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(&self, log_id: LogId) -> StorageResult<()> {
        debug!("delete_conflict_logs_since: [{log_id}, +oo)");

        let from = id_to_bin(log_id.index)?;
        let to = id_to_bin(0xff_ff_ff_ff_ff_ff_ff_ff)?;
        self.db
            .delete_range_cf(self.db.cf_logs()?, &from, &to)
            .map_err(logs_w_err)
    }

    // #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(&self, log_id: LogId) -> StorageResult<()> {
        debug!("purge_logs_upto: [0, {log_id}]");

        self.set_last_purged_(&log_id)?;
        let from = id_to_bin(0)?;
        let to = id_to_bin(log_id.index + 1)?;
        self.db
            .delete_range_cf(self.db.cf_logs()?, &from, &to)
            .map_err(logs_w_err)
    }

    async fn last_applied_state(
        &self,
    ) -> StorageResult<(Option<LogId>, Option<EffectiveMembership>)> {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.get_last_applied_log()?,
            state_machine.get_last_membership()?,
        ))
    }

    //#[tracing::instrument(level = "trace", skip(self, entries))]
    /// apply committed entries to the state machine, start the operation encoded in the `TremorRequest`
    async fn apply_to_state_machine(
        &self,
        entries: &[&Entry<TremorRequest>],
    ) -> StorageResult<Vec<TremorResponse>> {
        //debug!("apply_to_state_machine {entries:?}");
        let mut result = Vec::with_capacity(entries.len());

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            debug!("[{}] replicate to sm", entry.log_id);

            sm.set_last_applied_log(entry.log_id)?;

            match entry.payload {
                EntryPayload::Blank => result.push(TremorResponse { value: None }),
                EntryPayload::Normal(ref request) => {
                    result.push(sm.handle_request(entry.log_id, request).await?);
                }
                EntryPayload::Membership(ref mem) => {
                    debug!("[{}] replicate membership to sm", entry.log_id);
                    sm.set_last_membership(&EffectiveMembership {
                        log_id: entry.log_id,
                        membership: mem.clone(),
                    })?;
                    result.push(TremorResponse { value: None });
                }
            };
        }
        self.db.flush_wal(true).map_err(logs_w_err)?;
        Ok(result)
    }

    // -- snapshot stuff

    async fn build_snapshot(&self) -> StorageResult<Snapshot<Self::SnapshotData>> {
        let data;
        let last_applied_log;

        {
            // Serialize the data of the state machine.
            let state_machine =
                SerializableTremorStateMachine::try_from(&*self.state_machine.read().await)?;
            data = state_machine.to_vec()?;

            last_applied_log = state_machine.last_applied_log;
        }

        // TODO: we probably want thius to be atomic.
        let snapshot_idx: u64 = self.get_snapshot_index_()? + 1;
        self.set_snapshot_index_(snapshot_idx)?;

        let snapshot_id = format!(
            "{}-{}-{}",
            last_applied_log.map(|x| x.term).unwrap_or_default(),
            last_applied_log.map_or(0, |l| l.index),
            snapshot_idx
        );

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            snapshot_id,
        };

        let snapshot = TremorSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        self.set_current_snapshot_(&snapshot)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }

    // #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(&self) -> StorageResult<Box<Self::SnapshotData>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    // #[tracing::instrument(level = "trace", skip(self, snapshot))]
    /// installs snapshot and applies all the deltas to statemachine and runtime
    async fn install_snapshot(
        &self,
        meta: &SnapshotMeta,
        snapshot: Box<Self::SnapshotData>,
    ) -> StorageResult<StateMachineChanges> {
        info!(
            "decoding snapshot for installation size: {} ",
            snapshot.get_ref().len()
        );

        let new_snapshot = TremorSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.
        {
            let updated_state_machine: SerializableTremorStateMachine =
                serde_json::from_slice(&new_snapshot.data).map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::Snapshot(new_snapshot.meta.clone()),
                        ErrorVerb::Read,
                        AnyError::new(&e),
                    )
                })?;
            let mut state_machine = self.state_machine.write().await;
            state_machine
                .apply_diff_from_snapshot(updated_state_machine)
                .await?;
            self.set_current_snapshot_(&new_snapshot)?;
        }

        Ok(StateMachineChanges {
            last_applied: meta.last_log_id,
            is_snapshot: true,
        })
    }

    // #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(&self) -> StorageResult<Option<Snapshot<Self::SnapshotData>>> {
        match Store::get_current_snapshot_(self)? {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta,
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }
}

impl Store {
    // db column families

    /// storing `node_id` and addr of the current node
    const SELF: &'static str = "self";
    /// storing raft logs
    const LOGS: &'static str = "logs";
    /// storing `RaftStorage` related stuff
    const STORE: &'static str = "store";

    const COLUMN_FAMILIES: [&'static str; 3] = [Self::SELF, Self::LOGS, Self::STORE];

    // keys
    const LAST_PURGED_LOG_ID: &'static str = "last_purged_log_id";
    const SNAPSHOT_INDEX: &'static str = "snapshot_index";
    const SNAPSHOT: &'static str = "snapshot";
    const HARD_STATE: &'static str = "hard_state";
    /// for storing the own `node_id`
    const NODE_ID: &'static str = "node_id";
    const NODE_ADDR: &'static str = "node_addr";

    /// bootstrapping constructor - storing the given node data in the db
    pub(crate) async fn bootstrap<P: AsRef<Path>>(
        node_id: openraft::NodeId,
        addr: &Addr,
        db_path: P,
        world: Runtime,
        raft_api_tx: Sender<APIStoreReq>,
    ) -> Result<Arc<Store>, ClusterError> {
        let db = Self::init_db(db_path)?;
        Self::set_self(&db, node_id, addr)?;

        let db = Arc::new(db);
        let state_machine = RwLock::new(
            TremorStateMachine::new(db.clone(), world, raft_api_tx)
                .await
                .map_err(Error::from)?,
        );
        Ok(Arc::new(Self { state_machine, db }))
    }

    /// Initialize the database
    ///
    /// This function is safe and never cleans up or resets the current state,
    /// but creates a new db if there is none.
    pub(crate) fn init_db<P: AsRef<Path>>(db_path: P) -> Result<DB, ClusterError> {
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let cfs = TremorStateMachine::column_families().chain(Self::COLUMN_FAMILIES.into_iter());
        let db = DB::open_cf(&db_opts, db_path, cfs).map_err(ClusterError::Rocks)?;
        Ok(db)
    }

    /// loading constructor - loading the given database
    pub(crate) async fn load(
        db: Arc<DB>,
        world: Runtime,
        raft_api_tx: Sender<APIStoreReq>,
    ) -> Result<Arc<Store>, ClusterError> {
        let state_machine = RwLock::new(
            TremorStateMachine::new(db.clone(), world.clone(), raft_api_tx)
                .await
                .map_err(Error::from)?,
        );
        let this = Self { state_machine, db };
        Ok(Arc::new(this))
    }

    /// Store the information about the current node itself in the `db`
    fn set_self(db: &DB, node_id: openraft::NodeId, addr: &Addr) -> Result<(), ClusterError> {
        let node_id_bytes = id_to_bin(node_id)?;
        let addr_bytes = serde_json::to_vec(addr)?;
        let cf = db.cf_self()?;
        db.put_cf(cf, Store::NODE_ID, node_id_bytes)?;
        db.put_cf(cf, Store::NODE_ADDR, addr_bytes)?;
        Ok(())
    }

    pub(crate) fn get_self(db: &DB) -> Result<(openraft::NodeId, Addr), ClusterError> {
        let id = Self::get_self_node_id(db)?.ok_or("invalid cluster store, node_id missing")?;
        let addr = Self::get_self_addr(db)?.ok_or("invalid cluster store, node_addr missing")?;

        Ok((id, addr))
    }

    /// # Errors
    /// if the store fails to read the RPC address
    pub fn get_self_addr(db: &DB) -> Result<Option<Addr>, Error> {
        Ok(db
            .get_cf(db.cf_self()?, Store::NODE_ADDR)?
            .map(|v| serde_json::from_slice(&v))
            .transpose()?)
    }

    /// # Errors
    /// if the store fails to read the node id
    pub fn get_self_node_id(db: &DB) -> Result<Option<openraft::NodeId>, Error> {
        db.get_cf(db.cf_self()?, Store::NODE_ID)?
            .map(|v| bin_to_id(&v))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use crate::raft::ClusterResult;

    use super::*;

    #[test]
    fn init_db_is_idempotent() -> ClusterResult<()> {
        let dir = tempfile::tempdir()?;
        let db = Store::init_db(dir.path())?;
        let handle = db.cf_handle(Store::STORE).ok_or("no data")?;
        let data = vec![1_u8, 2_u8, 3_u8];
        db.put_cf(handle, "node_id", data.clone())?;
        drop(db);

        let db2 = Store::init_db(dir.path())?;
        let handle2 = db2.cf_handle(Store::STORE).ok_or("no data")?;
        let res2 = db2.get_cf(handle2, "node_id")?;
        assert_eq!(Some(data), res2);
        Ok(())
    }
}
