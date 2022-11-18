mod statemachine;

pub use self::statemachine::{AppId, FlowId, InstanceId};
pub(crate) use self::statemachine::{FlowInstance, Instances, StateApp};
use self::statemachine::{SerializableTremorStateMachine, TremorStateMachine};
use crate::{
    errors::Error as RuntimeError,
    instance::IntendedState,
    raft::{archive::TremorAppDef, ClusterError, NodeId, TremorNode},
    system::Runtime,
};
use async_std::{net::ToSocketAddrs, sync::RwLock};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use openraft::{
    async_trait::async_trait,
    storage::{LogState, Snapshot},
    AnyError, AppData, AppDataResponse, EffectiveMembership, Entry, EntryPayload, ErrorSubject,
    ErrorVerb, HardState, LogId, RaftStorage, SnapshotMeta, StateMachineChanges, StorageError,
    StorageIOError,
};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Direction, FlushOptions, Options, DB};
use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use std::{
    collections::HashMap,
    error::Error as StdError,
    fmt::{Debug, Display, Formatter},
    io::Cursor,
    ops::RangeBounds,
    path::Path,
    string::FromUtf8Error,
    sync::{Arc, Mutex},
};

/**
 * Here you will set the types of request that will interact with the raft nodes.
 * For example the `Set` will be used to write data (key and value) to the raft database.
 * The `AddNode` will append a new node to the current existing shared list of nodes.
 * You will want to add any request that can write data in all nodes here.
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TremorRequest {
    /// Set a key to the provided value in the cluster state
    Set { key: String, value: String },
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
        flow: FlowId,
        instance: InstanceId,
        config: HashMap<String, OwnedValue>,
        state: IntendedState,
    },

    /// Stopps and Undeploys an instance of a app
    Undeploy { app: AppId, instance: InstanceId },

    /// Requests a instance state change
    InstanceStateChange {
        app: AppId,
        instance: InstanceId,
        state: IntendedState,
    },
}

impl AppData for TremorRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TremorStart {
    pub(crate) instance: InstanceId,
    pub(crate) config: HashMap<String, OwnedValue>,
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
        TremorRequest::Set {
            key: set.key,
            value: set.value,
        }
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
#[derive(Serialize, Deserialize, Debug, Clone)]
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
    db: Arc<rocksdb::DB>,
    //runtime: Runtime,
    /// The Raft state machine.
    pub(crate) state_machine: RwLock<TremorStateMachine>,
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
    Ok((&buf[0..8]).read_u64::<BigEndian>()?)
}

#[derive(Debug)]
pub enum Error {
    MissingCf(&'static str),
    Utf8(FromUtf8Error),
    StrUtf8(std::str::Utf8Error),
    JSON(serde_json::Error),
    RocksDB(rocksdb::Error),
    Io(std::io::Error),
    // FIXME: this is horrid, aaaaaahhhhh!
    Tremor(Mutex<RuntimeError>),
    TremorScript(Mutex<tremor_script::errors::Error>),
    MissingApp(AppId),
    MissingFlow(AppId, FlowId),
    MissingInstance(AppId, InstanceId),
    Other(Box<dyn std::error::Error + Send + Sync>),
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

impl StdError for Error {}
impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::MissingCf(cf) => write!(f, "missing column family: `{}`", cf),
            Error::Utf8(e) => write!(f, "invalid utf8: {}", e),
            Error::StrUtf8(e) => write!(f, "invalid utf8: {}", e),
            Error::JSON(e) => write!(f, "invalid json: {}", e),
            Error::RocksDB(e) => write!(f, "rocksdb error: {}", e),
            Error::Io(e) => write!(f, "io error: {}", e),
            Error::Tremor(e) => write!(f, "tremor error: {:?}", e.lock()),
            Error::TremorScript(e) => write!(f, "tremor script error: {:?}", e.lock()),
            Error::Other(e) => write!(f, "other error: {}", e),
            Error::MissingApp(app) => write!(f, "missing app: {}", app),
            Error::MissingFlow(app, flow) => write!(f, "missing flow: {}::{}", app, flow),
            Error::MissingInstance(app, instance) => {
                write!(f, "missing instance: {}::{}", app, instance)
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
fn vote_w_err(e: impl StdError + 'static) -> StorageError {
    StorageIOError::new(ErrorSubject::Vote, ErrorVerb::Write, AnyError::new(&e)).into()
}
fn vote_r_err(e: impl StdError + 'static) -> StorageError {
    StorageIOError::new(ErrorSubject::Vote, ErrorVerb::Read, AnyError::new(&e)).into()
}
fn logs_r_err(e: impl StdError + 'static) -> StorageError {
    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Read, AnyError::new(&e)).into()
}
fn logs_w_err(e: impl StdError + 'static) -> StorageError {
    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Read, AnyError::new(&e)).into()
}
fn snap_w_err(meta: &SnapshotMeta, e: impl StdError + 'static) -> StorageError {
    StorageIOError::new(
        ErrorSubject::Snapshot(meta.signature()),
        ErrorVerb::Write,
        AnyError::new(&e),
    )
    .into()
}
// FIXME: delete this?
// fn snap_r_err(
//     meta: SnapshotMeta<ExampleNodeId>,
//     e: impl StdError + 'static,
// ) -> StorageError<ExampleNodeId> {
//     StorageIOError::new(
//         ErrorSubject::Snapshot(meta),
//         ErrorVerb::Read,
//         AnyError::new(&e),
//     )
//     .into()
// }

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

    /// node column family
    fn cf_node(&self) -> Result<&ColumnFamily, Error> {
        self.cf(Store::NODE)
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
    fn flush(&self) -> Result<(), Error> {
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
        Ok(self.put(
            self.db.cf_store()?,
            Self::HARD_STATE,
            serde_json::to_vec(hard_state)
                .map_err(Error::JSON)?
                .as_slice(),
        ))
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
impl RaftStorage<TremorRequest, TremorResponse> for Arc<Store> {
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
                &serde_json::to_vec(entry).map_err(logs_w_err)?,
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
        &mut self,
    ) -> StorageResult<(Option<LogId>, Option<EffectiveMembership>)> {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.get_last_applied_log()?,
            state_machine.get_last_membership()?,
        ))
    }

    // #[tracing::instrument(level = "trace", skip(self, entries))]
    /// apply committed entries to the state machine, start the operation encoded in the `TremorRequest`
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<TremorRequest>],
    ) -> StorageResult<Vec<TremorResponse>> {
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
                    sm.set_last_membership(&EffectiveMembership::new(
                        Some(entry.log_id),
                        mem.clone(),
                    ))?;
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
            last_applied_log.map(|x| x.leader_id).unwrap_or_default(),
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
        &mut self,
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
                        ErrorSubject::Snapshot(new_snapshot.meta.signature()),
                        ErrorVerb::Read,
                        AnyError::new(&e),
                    )
                })?;
            let mut state_machine = self.state_machine.write().await;
            state_machine
                .apply_diff_from_snapshot(updated_state_machine)
                .await?;
        }

        self.set_current_snapshot_(&new_snapshot)?;
        Ok(())
    }

    // #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> StorageResult<Option<Snapshot<Self::SnapshotData>>> {
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
    const NODE: &'static str = "node";
    const LOGS: &'static str = "logs";
    const STORE: &'static str = "store";
    const DATA: &'static str = "data";
    const APPS: &'static str = "apps";
    const INSTANCES: &'static str = "instances";
    const STATE_MACHINE: &'static str = "state_machine";

    // keys
    const LAST_MEMBERSHIP: &'static str = "last_membership";
    const LAST_APPLIED_LOG: &'static str = "last_applied_log";
    const LAST_PURGED_LOG_ID: &'static str = "last_purged_log_id";
    const SNAPSHOT_INDEX: &'static str = "snapshot_index";
    const SNAPSHOT: &'static str = "snapshot";
    const HARD_STATE: &'static str = "hard_state";

    /// Initialize the rocksdb column families.
    ///
    /// This function is safe and never cleans up or resets the current state,
    /// but creates a new db if there is none.
    fn init_db<P: AsRef<Path>>(db_path: P) -> Result<DB, ClusterError> {
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let node = ColumnFamilyDescriptor::new(Store::NODE, Options::default());
        let store = ColumnFamilyDescriptor::new(Store::STORE, Options::default());
        let state_machine = ColumnFamilyDescriptor::new(Store::STATE_MACHINE, Options::default());
        let data = ColumnFamilyDescriptor::new(Store::DATA, Options::default());
        let logs = ColumnFamilyDescriptor::new(Store::LOGS, Options::default());
        let apps = ColumnFamilyDescriptor::new(Store::APPS, Options::default());
        let instances = ColumnFamilyDescriptor::new(Store::INSTANCES, Options::default());

        DB::open_cf_descriptors(
            &db_opts,
            db_path,
            vec![node, store, state_machine, data, logs, apps, instances],
        )
        .map_err(ClusterError::Rocks)
    }

    pub(crate) fn get_node_data(&self) -> Result<(NodeId, String, String), ClusterError> {
        let id = self
            .get_node_id()?
            .ok_or("invalid cluster store, node_id missing")?;
        let api_addr = self
            .get_api_addr()?
            .ok_or("invalid cluster store, http_addr missing")?;

        let rpc_addr = self
            .get_rpc_addr()?
            .ok_or("invalid cluster store, rpc_addr missing")?;
        Ok((id, api_addr, rpc_addr))
    }

    /// bootstrapping constructor - storing the given node data in the db
    pub(crate) async fn bootstrap<P: AsRef<Path>>(
        db_path: P,
        node_id: NodeId,
        rpc_addr: impl Into<String> + ToSocketAddrs,
        api_addr: impl Into<String> + ToSocketAddrs,
        world: Runtime,
    ) -> Result<Arc<Store>, ClusterError> {
        let db = Self::init_db(db_path)?;
        let node_id = id_to_bin(*node_id)?;
        if let Err(e) = rpc_addr.to_socket_addrs().await {
            return Err(ClusterError::Other(format!("Invalid rpc_addr {e}")));
        }
        if let Err(e) = api_addr.to_socket_addrs().await {
            return Err(ClusterError::Other(format!("Invalid api_add {e}")));
        }

        let cf = db.cf_handle(Store::NODE).ok_or("no node column family")?;

        db.put_cf(cf, "node_id", node_id)?;
        db.put_cf(cf, "rpc_addr", rpc_addr.into().as_bytes())?;
        db.put_cf(cf, "api_addr", api_addr.into().as_bytes())?;
        let db = Arc::new(db);
        let state_machine = RwLock::new(
            TremorStateMachine::new(db.clone(), world.clone())
                .await
                .map_err(Error::from)?,
        );
        Ok(Arc::new(Self {
            db,
            state_machine,
            //runtime: world,
        }))
    }

    /// loading constructor - loading the given database
    ///
    /// verifying that we have some node-data stored
    pub(crate) async fn load<P: AsRef<Path>>(
        db_path: P,
        world: Runtime,
    ) -> Result<Arc<Store>, ClusterError> {
        let db = Arc::new(Self::init_db(db_path)?);
        let state_machine = RwLock::new(
            TremorStateMachine::new(db.clone(), world.clone())
                .await
                .map_err(Error::from)?,
        );
        let this = Self {
            db,
            state_machine,
            //runtime: world,
        };
        Ok(Arc::new(this))
    }

    /// # Errors
    /// if the store fails to read the API address
    pub fn get_api_addr(&self) -> Result<Option<String>, Error> {
        let api_addr = self
            .db
            .get_cf(self.db.cf_node()?, "api_addr")?
            .map(|v| String::from_utf8(v).map_err(Error::Utf8))
            .transpose()?;
        Ok(api_addr)
    }
    /// # Errors
    /// if the store fails to read the RPC address
    pub fn get_rpc_addr(&self) -> Result<Option<String>, Error> {
        self.db
            .get_cf(self.db.cf_node()?, "rpc_addr")?
            .map(|v| String::from_utf8(v).map_err(Error::Utf8))
            .transpose()
    }
    /// # Errors
    /// if the store fails to read the node id
    pub fn get_node_id(&self) -> Result<Option<NodeId>, Error> {
        self.db
            .get_cf(self.db.cf_node()?, "node_id")?
            .map(|v| bin_to_id(&v).map(NodeId::from))
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
        let handle = db.cf_handle(Store::NODE).ok_or("no data")?;
        let data = vec![1_u8, 2_u8, 3_u8];
        db.put_cf(handle, "node_id", data.clone())?;
        drop(db);

        let db2 = Store::init_db(dir.path())?;
        let handle2 = db2.cf_handle(Store::NODE).ok_or("no data")?;
        let res2 = db2.get_cf(handle2, "node_id")?;
        assert_eq!(Some(data), res2);
        Ok(())
    }
}
