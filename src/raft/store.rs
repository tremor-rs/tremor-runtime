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
        NodeId, TremorRaftConfig,
    },
    system::{flow::DeploymentType, Runtime},
    Result,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use openraft::{
    async_trait::async_trait, storage::Snapshot, AnyError, Entry, EntryPayload, ErrorSubject,
    ErrorVerb, LogId, LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage, RaftTypeConfig,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use redb::{
    CommitError, Database, DatabaseError, ReadableTable, StorageError as DbStorageError,
    TableDefinition, TableError, TransactionError,
};
use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use std::{
    fmt::{Debug, Display, Formatter},
    io::Cursor,
    ops::RangeBounds,
    path::Path,
    string::FromUtf8Error,
    sync::{Arc, Mutex},
};
use tokio::sync::RwLock;
use tremor_common::alias;

use super::SillyError;

/// Kv Operation
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KvRequest {
    /// Set a key to the provided value in the cluster state
    Set {
        /// the Key
        key: String,
        /// the Value
        value: Vec<u8>,
    },
}

/// Operations on the nodes known to the cluster
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum NodesRequest {
    /// Add the given node to the cluster and store its metadata (only addr for now)
    /// This command should be committed before a learner is added to the cluster, so the leader can contact it via its `addr`.
    AddNode {
        /// the node address
        addr: Addr,
    },
    /// Remove Node with the given `node_id`
    /// This command should be committed after removing a learner from the cluster.
    RemoveNode {
        /// the node id
        node_id: NodeId,
    },
}

/// Operations on apps and their instances
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AppsRequest {
    /// extract archive, parse sources, save sources in arena, put app into state machine
    InstallApp {
        /// app
        app: TremorAppDef,
        /// archive
        file: Vec<u8>,
    },
    /// delete from statemachine, delete sources from arena
    UninstallApp {
        /// app
        app: alias::App,
        /// if `true`, stop all instances of this app
        force: bool,
    },
    /// Deploy and Start a flow of an installed app
    Deploy {
        /// app
        app: alias::App,
        /// flow definition
        flow: alias::FlowDefinition,
        /// instance
        instance: alias::Flow,
        ///  config
        config: std::collections::HashMap<String, OwnedValue>,
        /// initial state
        state: IntendedState,
        /// Type of the deployment
        deployment_type: DeploymentType,
    },

    /// Stopps and Undeploys an instance of a app
    Undeploy(alias::Flow),

    /// Requests a instance state change
    InstanceStateChange {
        /// the instance
        instance: alias::Flow,
        /// the new state
        state: IntendedState,
    },
}

/// A request
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TremorRequest {
    /// KV operation
    Kv(KvRequest),
    /// Node operation
    Nodes(NodesRequest),
    /// App operation
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

/// A set request
#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
pub enum TremorInstanceState {
    /// Pauses a running instance
    Pause,
    /// Resumes a paused instance
    Resume,
}

/// A set request
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TremorSet {
    /// The key
    pub key: String,
    /// The value
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
    /// No response
    None,
    /// A key value response
    KvValue(Vec<u8>),
    /// A app id response
    AppId(alias::App),
    /// A node id response
    NodeId(NodeId),
    /// A app flow instance id response
    AppFlowInstanceId(alias::Flow),
}

/// Error for a raft response
#[derive(Debug, Clone, thiserror::Error, Serialize)]
pub enum ResponseError {
    /// Not a kv value
    #[error("Not a kv value")]
    NotKv,
    /// Not an app id
    #[error("Not an app id")]
    NotAppId,
    /// Not a node id
    #[error("Not a node id")]
    NotNodeId,
    /// Not an app flow instance id
    #[error("Not an app flow instance id")]
    NotAppFlowInstanceId,
}

type ResponseResult<T> = std::result::Result<T, ResponseError>;

impl TremorResponse {
    pub(crate) fn into_kv_value(self) -> ResponseResult<Vec<u8>> {
        match self {
            TremorResponse::KvValue(v) => Ok(v),
            _ => Err(ResponseError::NotKv),
        }
    }
}

impl TryFrom<TremorResponse> for alias::App {
    type Error = ResponseError;
    fn try_from(response: TremorResponse) -> ResponseResult<Self> {
        match response {
            TremorResponse::AppId(id) => Ok(id),
            _ => Err(ResponseError::NotAppId),
        }
    }
}

impl TryFrom<TremorResponse> for NodeId {
    type Error = ResponseError;
    fn try_from(response: TremorResponse) -> ResponseResult<Self> {
        match response {
            TremorResponse::NodeId(id) => Ok(id),
            _ => Err(ResponseError::NotNodeId),
        }
    }
}

impl TryFrom<TremorResponse> for alias::Flow {
    type Error = ResponseError;
    fn try_from(response: TremorResponse) -> ResponseResult<Self> {
        match response {
            TremorResponse::AppFlowInstanceId(id) => Ok(id),
            _ => Err(ResponseError::NotAppFlowInstanceId),
        }
    }
}

/// A snapshot
#[derive(Serialize, Deserialize, Debug)]
pub struct TremorSnapshot {
    /// The meta data of the snapshot.
    pub meta: SnapshotMeta<NodeId, crate::raft::node::Addr>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/// The Raft storage
#[derive(Debug, Clone)]
pub struct Store {
    /// The Raft state machine.
    pub(crate) state_machine: Arc<RwLock<TremorStateMachine>>,
    // the database
    db: Arc<Database>,
}

type StorageResult<T> = anyhow::Result<T, StorageError<crate::raft::NodeId>>;

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

/// The Raft storage error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// invalid cluster store, node_id missing
    // #[error("invalid cluster store, node_id missing")]
    MissingNodeId,
    // #[error("invalid cluster store, node_addr missing")]
    /// invalid cluster store, node_addr missing
    MissingNodeAddr,
    /// Invalid utf8
    Utf8(FromUtf8Error),
    /// Invalid utf8
    StrUtf8(std::str::Utf8Error),
    /// MsgPack encode error
    MsgPackEncode(rmp_serde::encode::Error),
    /// MsgPack decode error
    MsgPackDecode(rmp_serde::decode::Error),
    /// Database error
    Database(DatabaseError),
    /// Transaction error
    Transaction(TransactionError),
    /// Transaction error
    Table(TableError),
    /// StorageError
    DbStorage(DbStorageError),
    /// Commit Error
    Commit(CommitError),
    /// IO error
    Io(std::io::Error),
    /// Storage error
    Storage(openraft::StorageError<crate::raft::NodeId>),
    /// Tremor error
    Tremor(Mutex<RuntimeError>),
    /// Tremor script error
    TremorScript(Mutex<tremor_script::errors::Error>),
    /// Missing app
    MissingApp(alias::App),
    /// Missing flow
    MissingFlow(alias::App, alias::FlowDefinition),
    /// Missing instance
    MissingInstance(alias::Flow),
    /// App still has running instances
    RunningInstances(alias::App),
    /// Node already added
    NodeAlreadyAdded(crate::raft::NodeId),
    /// Other error
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
impl From<redb::DatabaseError> for Error {
    fn from(e: redb::DatabaseError) -> Self {
        Error::Database(e)
    }
}
impl From<redb::TransactionError> for Error {
    fn from(e: redb::TransactionError) -> Self {
        Error::Transaction(e)
    }
}

impl From<redb::TableError> for Error {
    fn from(e: redb::TableError) -> Self {
        Error::Table(e)
    }
}

impl From<redb::StorageError> for Error {
    fn from(e: redb::StorageError) -> Self {
        Error::DbStorage(e)
    }
}

impl From<redb::CommitError> for Error {
    fn from(e: redb::CommitError) -> Self {
        Error::Commit(e)
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

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::MissingNodeId => write!(f, "missing node id"),
            Error::MissingNodeAddr => write!(f, "missing node addr"),
            Error::Utf8(e) => write!(f, "invalid utf8: {e}"),
            Error::StrUtf8(e) => write!(f, "invalid utf8: {e}"),

            Error::MsgPackEncode(e) => write!(f, "invalid msgpack: {e}"),
            Error::MsgPackDecode(e) => write!(f, "invalid msgpack: {e}"),

            Error::Database(e) => write!(f, "database error: {e}"),
            Error::Transaction(e) => write!(f, "transaction error: {e}"),
            Error::Table(e) => write!(f, "table error: {e}"),
            Error::DbStorage(e) => write!(f, "storage error: {e}"),
            Error::Commit(e) => write!(f, "commit error: {e}"),

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

fn w_err(e: impl Into<anyhow::Error>) -> StorageError<crate::raft::NodeId> {
    StorageIOError::new(
        ErrorSubject::Store,
        ErrorVerb::Write,
        AnyError::new(&SillyError(e.into())),
    )
    .into()
}
fn r_err(e: impl Into<anyhow::Error>) -> StorageError<crate::raft::NodeId> {
    StorageIOError::new(
        ErrorSubject::Store,
        ErrorVerb::Read,
        AnyError::new(&SillyError(e.into())),
    )
    .into()
}
fn logs_r_err(e: impl Into<anyhow::Error>) -> StorageError<crate::raft::NodeId> {
    StorageIOError::new(
        ErrorSubject::Logs,
        ErrorVerb::Read,
        AnyError::new(&SillyError(e.into())),
    )
    .into()
}
fn logs_w_err(e: impl Into<anyhow::Error>) -> StorageError<crate::raft::NodeId> {
    StorageIOError::new(
        ErrorSubject::Logs,
        ErrorVerb::Read,
        AnyError::new(&SillyError(e.into())),
    )
    .into()
}

impl From<Error> for StorageError<crate::raft::NodeId> {
    fn from(e: Error) -> StorageError<crate::raft::NodeId> {
        StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)).into()
    }
}

impl Store {
    fn put<T>(&self, key: &str, value: &T) -> StorageResult<()>
    where
        T: serde::Serialize,
    {
        let write_txn = self.db.begin_write().map_err(w_err)?;
        {
            let mut table = write_txn.open_table(STORE).map_err(w_err)?;
            table
                .insert(key, rmp_serde::to_vec(value).map_err(Error::MsgPackEncode)?)
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
            let _argh = bug_fix_txn.open_table(STORE).map_err(w_err)?;
        }
        bug_fix_txn.commit().map_err(w_err)?;
        let read_txn = self.db.begin_read().map_err(r_err)?;
        let table = read_txn.open_table(STORE).map_err(r_err)?;
        let r = table.get(key).map_err(r_err)?;
        if let Some(v) = r {
            let data = rmp_serde::from_slice(&v.value()).map_err(r_err)?;
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    fn get_vote_(&self) -> StorageResult<Option<Vote<NodeId>>> {
        self.get(Self::VOTE)
    }

    fn set_vote_(&self, hard_state: &Vote<NodeId>) -> StorageResult<()> {
        self.put(Self::VOTE, hard_state)
    }

    fn get_last_purged_(&self) -> StorageResult<Option<LogId<crate::raft::NodeId>>> {
        self.get(Self::LAST_PURGED_LOG_ID)
    }

    fn set_last_purged_(&self, log_id: &LogId<crate::raft::NodeId>) -> StorageResult<()> {
        self.put(Self::LAST_PURGED_LOG_ID, log_id)
    }

    fn get_snapshot_index_(&self) -> StorageResult<u64> {
        self.get(Self::SNAPSHOT_INDEX)
            .map(Option::unwrap_or_default)
    }

    fn set_snapshot_index_(&self, snapshot_index: u64) -> StorageResult<()> {
        self.put(Self::SNAPSHOT_INDEX, &snapshot_index)
    }

    fn get_current_snapshot_(&self) -> StorageResult<Option<TremorSnapshot>> {
        self.get(Self::SNAPSHOT)
    }

    fn set_current_snapshot_(&self, snap: &TremorSnapshot) -> StorageResult<()> {
        self.put(Self::SNAPSHOT, &snap)
    }
}

#[async_trait]
impl RaftLogReader<TremorRaftConfig> for Store {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<TremorRaftConfig>>> {
        // We need to use a write transaction despite just wanting a read transaction due to
        // https://github.com/cberner/redb/issues/711
        let bug_fix_txn = self.db.begin_write().map_err(w_err)?;
        {
            // ALLOW: this is just a workaround
            let _argh = bug_fix_txn.open_table(LOGS).map_err(w_err)?;
        }
        bug_fix_txn.commit().map_err(w_err)?;
        let read_txn = self.db.begin_read().map_err(r_err)?;

        let table = read_txn.open_table(LOGS).map_err(r_err)?;
        let r = table.range(range).map_err(r_err)?;
        r.map(|entry| -> StorageResult<_> {
            let (id, val) = entry.map_err(r_err)?;
            let entry: Entry<_> = rmp_serde::from_slice(&val.value()).map_err(logs_r_err)?;
            debug_assert_eq!(id.value(), entry.log_id.index);
            Ok(entry)
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
        let write_txn = self.db.begin_write().map_err(w_err)?;
        {
            let mut table = write_txn.open_table(LOGS).map_err(w_err)?;
            for entry in entries {
                table
                    .insert(
                        entry.log_id.index,
                        rmp_serde::to_vec(&entry).map_err(logs_w_err)?,
                    )
                    .map_err(logs_w_err)?;
            }
        }
        write_txn.commit().map_err(w_err)?;
        Ok(())
    }

    // #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<crate::raft::NodeId>,
    ) -> StorageResult<()> {
        debug!("delete_conflict_logs_since: [{log_id}, +oo)");

        let write_txn = self.db.begin_write().map_err(w_err)?;
        {
            let mut table = write_txn.open_table(LOGS).map_err(w_err)?;
            table.drain(log_id.index..).map_err(logs_w_err)?;
        }
        write_txn.commit().map_err(w_err)
    }

    // #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(&mut self, log_id: LogId<crate::raft::NodeId>) -> StorageResult<()> {
        debug!("purge_logs_upto: [0, {log_id}]");

        self.set_last_purged_(&log_id)?;
        let write_txn = self.db.begin_write().map_err(w_err)?;
        {
            let mut table = write_txn.open_table(LOGS).map_err(w_err)?;
            table.drain(..=log_id.index).map_err(logs_w_err)?;
        }
        write_txn.commit().map_err(w_err)
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

    async fn get_log_state(&mut self) -> StorageResult<LogState<TremorRaftConfig>> {
        // We need to use a write transaction despite just wanting a read transaction due to
        // https://github.com/cberner/redb/issues/711
        let bug_fix_txn = self.db.begin_write().map_err(w_err)?;
        {
            // ALLOW: this is just a workaround
            let _argh = bug_fix_txn.open_table(LOGS).map_err(w_err)?;
        }
        bug_fix_txn.commit().map_err(w_err)?;

        let read_txn = self.db.begin_read().map_err(r_err)?;
        let table = read_txn.open_table(LOGS).map_err(r_err)?;
        let last = table
            .last()
            .map_err(r_err)?
            .map(|(_k, v)| {
                rmp_serde::from_slice::<Entry<TremorRaftConfig>>(&v.value())
                    .map_err(r_err)
                    .map(|e| e.log_id)
            })
            .transpose()?;

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
}

/// Node Storage
const NODES: TableDefinition<u64, &[u8]> = TableDefinition::new("nodes");

/// State Machine Storage
const STATE_MACHINE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("state_machine");

/// applications
const APPS: TableDefinition<&str, &[u8]> = TableDefinition::new("apps");

/// instances
const INSTANCES: TableDefinition<&str, Vec<u8>> = TableDefinition::new("instances");

/// kv data
const DATA: TableDefinition<&str, &[u8]> = TableDefinition::new("data");

/// storing system data `node_id` and addr of the current node
const SYSTEM: TableDefinition<&str, Vec<u8>> = TableDefinition::new("self");
/// storing `RaftStorage` related stuff
const STORE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("store");
/// storing raft logs
const LOGS: TableDefinition<u64, Vec<u8>> = TableDefinition::new("logs");

impl Store {
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
    ) -> Result<Store> {
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
    pub(crate) fn init_db<P: AsRef<Path>>(db_path: P) -> Result<Database> {
        let db = Database::create(db_path)?;
        Ok(db)
    }

    /// loading constructor - loading the given database
    pub(crate) async fn load(db: Arc<Database>, world: Runtime) -> Result<Store> {
        let state_machine = Arc::new(RwLock::new(
            TremorStateMachine::new(db.clone(), world.clone())
                .await
                .map_err(Error::from)?,
        ));
        let this = Self { state_machine, db };
        Ok(this)
    }

    /// Store the information about the current node itself in the `db`
    fn set_self(db: &Database, node_id: crate::raft::NodeId, addr: &Addr) -> Result<()> {
        let node_id_bytes = id_to_bin(node_id)?;
        let addr_bytes = rmp_serde::to_vec(addr)?;
        let write_txn = db.begin_write()?;
        {
            let mut table = write_txn.open_table(SYSTEM)?;
            table.insert(Store::NODE_ID, &node_id_bytes)?;
            table.insert(Store::NODE_ADDR, addr_bytes)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub(crate) fn get_self(db: &Database) -> Result<(crate::raft::NodeId, Addr)> {
        let id = Self::get_self_node_id(db)?.ok_or(Error::MissingNodeId)?;
        let addr = Self::get_self_addr(db)?.ok_or(Error::MissingNodeAddr)?;

        Ok((id, addr))
    }

    /// # Errors
    /// if the store fails to read the RPC address
    pub fn get_self_addr(db: &Database) -> Result<Option<Addr>> {
        // We need to use a write transaction despite just wanting a read transaction due to
        // https://github.com/cberner/redb/issues/711
        let bug_fix_txn = db.begin_write()?;
        {
            // ALLOW: this is just a workaround
            let _argh = bug_fix_txn.open_table(SYSTEM).map_err(w_err)?;
        }
        bug_fix_txn.commit().map_err(w_err)?;

        let read_txn = db.begin_read().map_err(r_err)?;

        let table = read_txn.open_table(SYSTEM).map_err(r_err)?;
        let r = table.get(Store::NODE_ADDR).map_err(r_err)?;
        Ok(r.map(|v| rmp_serde::from_slice(&v.value())).transpose()?)
    }

    /// # Errors
    /// if the store fails to read the node id
    pub fn get_self_node_id(db: &Database) -> Result<Option<crate::raft::NodeId>> {
        // We need to use a write transaction despite just wanting a read transaction due to
        // https://github.com/cberner/redb/issues/711
        let bug_fix_txn = db.begin_write()?;
        {
            // ALLOW: this is just a workaround
            let _argh = bug_fix_txn.open_table(SYSTEM).map_err(w_err)?;
        }
        bug_fix_txn.commit().map_err(w_err)?;

        let read_txn = db.begin_read().map_err(r_err)?;

        let table = read_txn.open_table(SYSTEM).map_err(r_err)?;
        let r = table.get(Store::NODE_ID).map_err(r_err)?;
        Ok(r.map(|v| bin_to_id(&v.value())).transpose()?)
    }
}

#[cfg(test)]
mod tests {
    // use crate::raft::ClusterResult;

    // use super::*;

    // #[test]
    // fn init_db_is_idempotent() -> ClusterResult<()> {
    //     let dir = tempfile::tempdir()?;
    //     let db = Store::init_db(dir.path())?;
    //     let handle = db.cf_handle(Store::STORE).expect("no data");
    //     let data = vec![1_u8, 2_u8, 3_u8];
    //     db.put_cf(handle, "node_id", data.clone())?;
    //     drop(db);

    //     let db2 = Store::init_db(dir.path())?;
    //     let handle2 = db2.cf_handle(Store::STORE).expect("no data");
    //     let res2 = db2.get_cf(handle2, "node_id")?;
    //     assert_eq!(Some(data), res2);
    //     Ok(())
    // }
}