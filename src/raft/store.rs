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
use crate::{
    errors::Error as RuntimeError,
    instance::IntendedState,
    raft::{
        archive::TremorAppDef,
        node::Addr,
        store::statemachine::{SerializableTremorStateMachine, TremorStateMachine},
        ClusterError, NodeId, TremorRaftConfig,
    },
    system::{flow::DeploymentType, Runtime},
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use openraft::{
    async_trait::async_trait, storage::Snapshot, AnyError, Entry, EntryPayload, ErrorSubject,
    ErrorVerb, LogId, LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage, RaftTypeConfig,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
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
use tremor_common::alias;

/// Kv Operation
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KvRequest {
    /// Set a key to the provided value in the cluster state
    Set { key: String, value: Vec<u8> },
}

/// Operations on the nodes known to the cluster
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum NodesRequest {
    /// Add the given node to the cluster and store its metadata (only addr for now)
    /// This command should be committed before a learner is added to the cluster, so the leader can contact it via its `addr`.
    AddNode { addr: Addr },
    /// Remove Node with the given `node_id`
    /// This command should be committed after removing a learner from the cluster.
    RemoveNode { node_id: NodeId },
}

/// Operations on apps and their instances
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AppsRequest {
    /// extract archive, parse sources, save sources in arena, put app into state machine
    InstallApp { app: TremorAppDef, file: Vec<u8> },
    /// delete from statemachine, delete sources from arena
    UninstallApp {
        app: alias::App,
        /// if `true`, stop all instances of this app
        force: bool,
    },
    /// Deploy and Start a flow of an installed app
    Deploy {
        app: alias::App,
        flow: alias::FlowDefinition,
        instance: alias::Flow,
        config: std::collections::HashMap<String, OwnedValue>,
        state: IntendedState,
        deployment_type: DeploymentType,
    },

    /// Stopps and Undeploys an instance of a app
    Undeploy(alias::Flow),

    /// Requests a instance state change
    InstanceStateChange {
        instance: alias::Flow,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TremorStart {
    pub(crate) instance: alias::Flow,
    pub(crate) config: std::collections::HashMap<String, OwnedValue>,
    pub(crate) running: bool,
    pub(crate) single_node: bool,
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
    pub value: Vec<u8>,
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
 * TODO: `Should` we explain how to create multiple `AppDataResponse`?
 *
 */
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub enum TremorResponse {
    #[default]
    None,
    KvValue(Vec<u8>),
    AppId(alias::App),
    NodeId(NodeId),
    AppFlowInstanceId(alias::Flow),
}

impl TremorResponse {
    pub(crate) fn into_kv_value(self) -> crate::Result<Vec<u8>> {
        match self {
            TremorResponse::KvValue(v) => Ok(v),
            _ => Err(RuntimeError::from("Not a kv value")),
        }
    }
}

impl TryFrom<TremorResponse> for alias::App {
    type Error = RuntimeError;
    fn try_from(response: TremorResponse) -> crate::Result<Self> {
        match response {
            TremorResponse::AppId(id) => Ok(id),
            _ => Err(RuntimeError::from("Not an app id")),
        }
    }
}

impl TryFrom<TremorResponse> for NodeId {
    type Error = RuntimeError;
    fn try_from(response: TremorResponse) -> crate::Result<Self> {
        match response {
            TremorResponse::NodeId(id) => Ok(id),
            _ => Err(RuntimeError::from("Not a node id")),
        }
    }
}

impl TryFrom<TremorResponse> for alias::Flow {
    type Error = RuntimeError;
    fn try_from(response: TremorResponse) -> crate::Result<Self> {
        match response {
            TremorResponse::AppFlowInstanceId(id) => Ok(id),
            _ => Err(RuntimeError::from("Not an app flow instance id")),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TremorSnapshot {
    pub meta: SnapshotMeta<NodeId, crate::raft::node::Addr>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Store {
    /// The Raft state machine.
    pub(crate) state_machine: Arc<RwLock<TremorStateMachine>>,
    // the database
    db: Arc<rocksdb::DB>,
}

type StorageResult<T> = Result<T, StorageError<crate::raft::NodeId>>;

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
    MsgPackEncode(rmp_serde::encode::Error),
    MsgPackDecode(rmp_serde::decode::Error),
    RocksDB(rocksdb::Error),
    Io(std::io::Error),
    Storage(openraft::StorageError<crate::raft::NodeId>),
    // TODO: this is horrid, aaaaaahhhhh!
    Tremor(Mutex<RuntimeError>),
    TremorScript(Mutex<tremor_script::errors::Error>),
    MissingApp(alias::App),
    MissingFlow(alias::App, alias::FlowDefinition),
    MissingInstance(alias::Flow),
    RunningInstances(alias::App),
    NodeAlreadyAdded(crate::raft::NodeId),
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

impl From<rmp_serde::encode::Error> for Error {
    fn from(e: rmp_serde::encode::Error) -> Self {
        Error::MsgPackEncode(e)
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(e: rmp_serde::decode::Error) -> Self {
        Error::MsgPackDecode(e)
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

impl From<StorageError<crate::raft::NodeId>> for Error {
    fn from(e: StorageError<crate::raft::NodeId>) -> Self {
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
            Error::MsgPackEncode(e) => write!(f, "invalid msgpack: {e}"),
            Error::MsgPackDecode(e) => write!(f, "invalid msgpack: {e}"),
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

fn store_w_err(e: impl StdError + 'static) -> StorageError<crate::raft::NodeId> {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e)).into()
}
fn store_r_err(e: impl StdError + 'static) -> StorageError<crate::raft::NodeId> {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)).into()
}
fn logs_r_err(e: impl StdError + 'static) -> StorageError<crate::raft::NodeId> {
    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Read, AnyError::new(&e)).into()
}
fn logs_w_err(e: impl StdError + 'static) -> StorageError<crate::raft::NodeId> {
    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Read, AnyError::new(&e)).into()
}
fn snap_w_err(
    meta: &SnapshotMeta<crate::raft::NodeId, crate::raft::node::Addr>,
    e: impl StdError + 'static,
) -> StorageError<crate::raft::NodeId> {
    StorageIOError::new(
        ErrorSubject::Snapshot(Some(meta.signature())),
        ErrorVerb::Write,
        AnyError::new(&e),
    )
    .into()
}

impl From<Error> for StorageError<crate::raft::NodeId> {
    fn from(e: Error) -> StorageError<crate::raft::NodeId> {
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

    fn get_vote_(&self) -> StorageResult<Option<Vote<NodeId>>> {
        Ok(self
            .db
            .get_cf(self.db.cf_store()?, Self::VOTE)
            .map_err(store_r_err)?
            .and_then(|v| rmp_serde::from_slice(&v).ok()))
    }

    fn set_vote_(&self, hard_state: &Vote<NodeId>) -> StorageResult<()> {
        self.put(
            self.db.cf_store()?,
            Self::VOTE,
            rmp_serde::to_vec(hard_state)
                .map_err(Error::MsgPackEncode)?
                .as_slice(),
        )
        .map_err(store_w_err)
    }

    fn get_last_purged_(&self) -> StorageResult<Option<LogId<crate::raft::NodeId>>> {
        Ok(self
            .db
            .get_cf(self.db.cf_store()?, Store::LAST_PURGED_LOG_ID)
            .map_err(store_r_err)?
            .and_then(|v| rmp_serde::from_slice(&v).ok()))
    }

    fn set_last_purged_(&self, log_id: &LogId<crate::raft::NodeId>) -> StorageResult<()> {
        self.put(
            self.db.cf_store()?,
            Self::LAST_PURGED_LOG_ID,
            rmp_serde::to_vec(log_id)
                .map_err(Error::MsgPackEncode)?
                .as_slice(),
        )
        .map_err(store_w_err)
    }

    fn get_snapshot_index_(&self) -> StorageResult<u64> {
        Ok(self
            .db
            .get_cf(self.db.cf_store()?, Self::SNAPSHOT_INDEX)
            .map_err(store_r_err)?
            .map(|v| rmp_serde::from_slice(&v).map_err(Error::MsgPackDecode))
            .transpose()?
            .unwrap_or_default())
    }

    fn set_snapshot_index_(&self, snapshot_index: u64) -> StorageResult<()> {
        self.put(
            self.db.cf_store()?,
            Self::SNAPSHOT_INDEX,
            rmp_serde::to_vec(&snapshot_index)
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
            .and_then(|v| rmp_serde::from_slice(&v).ok()))
    }

    fn set_current_snapshot_(&self, snap: &TremorSnapshot) -> StorageResult<()> {
        self.put(
            self.db.cf_store()?,
            Self::SNAPSHOT,
            rmp_serde::to_vec(&snap).map_err(|e| snap_w_err(&snap.meta, e))?,
        )
        .map_err(|e| snap_w_err(&snap.meta, e))?;
        Ok(())
    }
}

#[async_trait]

impl RaftLogReader<TremorRaftConfig> for Store {
    async fn get_log_state(&mut self) -> StorageResult<LogState<TremorRaftConfig>> {
        let last = self
            .db
            .iterator_cf(self.db.cf_logs()?, rocksdb::IteratorMode::End)
            .next()
            .and_then(|d| {
                let (_, ent) = d.ok()?;
                Some(
                    rmp_serde::from_slice::<Entry<TremorRaftConfig>>(&ent)
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
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<TremorRaftConfig>>> {
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
                let entry: Entry<_> = rmp_serde::from_slice(&val).map_err(logs_r_err)?;
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
}
#[async_trait]

impl RaftSnapshotBuilder<TremorRaftConfig> for Store {
    async fn build_snapshot(&mut self) -> StorageResult<Snapshot<TremorRaftConfig>> {
        let data;
        let last_applied_log;
        let last_membership;

        {
            // Serialize the data of the state machine.
            let state_machine =
                SerializableTremorStateMachine::try_from(&*self.state_machine.read().await)?;
            data = state_machine.to_vec()?;

            last_applied_log = state_machine.last_applied_log;
            last_membership = state_machine.last_membership;
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
            last_membership: last_membership.unwrap_or_default(),
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
}
#[async_trait]
impl RaftStorage<TremorRaftConfig> for Store {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<NodeId>,
    ) -> Result<(), StorageError<crate::raft::NodeId>> {
        self.set_vote_(vote)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<NodeId>>, StorageError<crate::raft::NodeId>> {
        self.get_vote_()
    }

    // #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log<I>(&mut self, entries: I) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry<TremorRaftConfig>> + Send,
    {
        for entry in entries {
            let id = id_to_bin(entry.log_id.index)?;
            assert_eq!(bin_to_id(&id)?, entry.log_id.index);
            self.put(
                self.db.cf_logs()?,
                &id,
                rmp_serde::to_vec(&entry).map_err(logs_w_err)?,
            )
            .map_err(logs_w_err)?;
        }
        Ok(())
    }

    // #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<crate::raft::NodeId>,
    ) -> StorageResult<()> {
        debug!("delete_conflict_logs_since: [{log_id}, +oo)");

        let from = id_to_bin(log_id.index)?;
        let to = id_to_bin(0xff_ff_ff_ff_ff_ff_ff_ff)?;
        self.db
            .delete_range_cf(self.db.cf_logs()?, &from, &to)
            .map_err(logs_w_err)
    }

    // #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(&mut self, log_id: LogId<crate::raft::NodeId>) -> StorageResult<()> {
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
    ) -> StorageResult<(
        std::option::Option<openraft::LogId<u64>>,
        openraft::StoredMembership<u64, crate::raft::node::Addr>,
    )> {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.get_last_applied_log()?,
            state_machine.get_last_membership()?.unwrap_or_default(),
        ))
    }

    //#[tracing::instrument(level = "trace", skip(self, entries))]
    /// apply committed entries to the state machine, start the operation encoded in the `TremorRequest`
    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TremorRaftConfig>],
    ) -> StorageResult<Vec<TremorResponse>> {
        //debug!("apply_to_state_machine {entries:?}");
        let mut result = Vec::with_capacity(entries.len());

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            debug!("[{}] replicate to sm", entry.log_id);

            sm.set_last_applied_log(entry.log_id)?;

            match entry.payload {
                EntryPayload::Blank => result.push(TremorResponse::None),
                EntryPayload::Normal(ref request) => {
                    result.push(sm.handle_request(entry.log_id, request).await?);
                }
                EntryPayload::Membership(ref mem) => {
                    debug!("[{}] replicate membership to sm", entry.log_id);
                    sm.set_last_membership(&StoredMembership::new(
                        Some(entry.log_id),
                        mem.clone(),
                    ))?;
                    result.push(TremorResponse::None);
                }
            };
        }
        self.db.flush_wal(true).map_err(logs_w_err)?;
        Ok(result)
    }

    // #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> StorageResult<Box<<TremorRaftConfig as RaftTypeConfig>::SnapshotData>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    // #[tracing::instrument(level = "trace", skip(self, snapshot))]
    /// installs snapshot and applies all the deltas to statemachine and runtime
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<crate::raft::NodeId, crate::raft::node::Addr>,
        snapshot: Box<<TremorRaftConfig as RaftTypeConfig>::SnapshotData>,
    ) -> StorageResult<()> {
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
                rmp_serde::from_slice(&new_snapshot.data).map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::Snapshot(Some(new_snapshot.meta.signature())),
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

        Ok(())
    }

    // #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(&mut self) -> StorageResult<Option<Snapshot<TremorRaftConfig>>> {
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
    const VOTE: &'static str = "vote";
    /// for storing the own `node_id`
    const NODE_ID: &'static str = "node_id";
    const NODE_ADDR: &'static str = "node_addr";

    /// bootstrapping constructor - storing the given node data in the db
    pub(crate) async fn bootstrap<P: AsRef<Path>>(
        node_id: crate::raft::NodeId,
        addr: &Addr,
        db_path: P,
        world: Runtime,
    ) -> Result<Store, ClusterError> {
        let db = Self::init_db(db_path)?;
        Self::set_self(&db, node_id, addr)?;

        let db = Arc::new(db);
        let state_machine = Arc::new(RwLock::new(
            TremorStateMachine::new(db.clone(), world)
                .await
                .map_err(Error::from)?,
        ));
        Ok(Self { state_machine, db })
    }

    /// Initialize the database
    ///
    /// This function is safe and never cleans up or resets the current state,
    /// but creates a new db if there is none.
    pub(crate) fn init_db<P: AsRef<Path>>(db_path: P) -> Result<DB, ClusterError> {
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let cfs = TremorStateMachine::column_families().chain(Self::COLUMN_FAMILIES);
        let db = DB::open_cf(&db_opts, db_path, cfs).map_err(ClusterError::Rocks)?;
        Ok(db)
    }

    /// loading constructor - loading the given database
    pub(crate) async fn load(db: Arc<DB>, world: Runtime) -> Result<Store, ClusterError> {
        let state_machine = Arc::new(RwLock::new(
            TremorStateMachine::new(db.clone(), world.clone())
                .await
                .map_err(Error::from)?,
        ));
        let this = Self { state_machine, db };
        Ok(this)
    }

    /// Store the information about the current node itself in the `db`
    fn set_self(db: &DB, node_id: crate::raft::NodeId, addr: &Addr) -> Result<(), ClusterError> {
        let node_id_bytes = id_to_bin(node_id)?;
        let addr_bytes = rmp_serde::to_vec(addr)?;
        let cf = db.cf_self()?;
        db.put_cf(cf, Store::NODE_ID, node_id_bytes)?;
        db.put_cf(cf, Store::NODE_ADDR, addr_bytes)?;
        Ok(())
    }

    pub(crate) fn get_self(db: &DB) -> Result<(crate::raft::NodeId, Addr), ClusterError> {
        let id = Self::get_self_node_id(db)?.ok_or("invalid cluster store, node_id missing")?;
        let addr = Self::get_self_addr(db)?.ok_or("invalid cluster store, node_addr missing")?;

        Ok((id, addr))
    }

    /// # Errors
    /// if the store fails to read the RPC address
    pub fn get_self_addr(db: &DB) -> Result<Option<Addr>, Error> {
        Ok(db
            .get_cf(db.cf_self()?, Store::NODE_ADDR)?
            .map(|v| rmp_serde::from_slice(&v))
            .transpose()?)
    }

    /// # Errors
    /// if the store fails to read the node id
    pub fn get_self_node_id(db: &DB) -> Result<Option<crate::raft::NodeId>, Error> {
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
