use crate::{
    errors::{Error as RuntimeError, Kind as ErrorKind},
    instance::{IntendedState, State as InstanceState},
    log_error,
    raft::{archive::TremorAppDef, ClusterError, TremorNode, TremorNodeId, TremorTypeConfig},
    system::Runtime,
};
use async_std::{channel::unbounded, net::ToSocketAddrs, prelude::FutureExt, sync::RwLock};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use openraft::{
    async_trait::async_trait,
    storage::{LogState, Snapshot},
    AnyError, EffectiveMembership, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, NodeId,
    RaftLogReader, RaftSnapshotBuilder, RaftStorage, SnapshotMeta, StorageError, StorageIOError,
    Vote,
};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Direction, FlushOptions, Options, DB};
use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    error::Error,
    fmt::{Debug, Display, Formatter},
    io::Cursor,
    ops::RangeBounds,
    path::Path,
    string::FromUtf8Error,
    sync::{Arc, Mutex},
    time::Duration,
};
use tremor_script::{
    arena,
    ast::{
        optimizer::Optimizer, visitors::ArgsRewriter, walkers::DeployWalker, CreationalWith,
        DeployFlow, FlowDefinition, Helper, Ident, ImutExpr, WithExprs,
    },
    deploy::Deploy,
    module::GetMod,
    prelude::BaseExpr,
    AggrRegistry, FN_REGISTRY,
};

use super::archive::extract;

/**
 * Here you will set the types of request that will interact with the raft nodes.
 * For example the `Set` will be used to write data (key and value) to the raft database.
 * The `AddNode` will append a new node to the current existing shared list of nodes.
 * You will want to add any request that can write data in all nodes here.
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TremorRequest {
    Set {
        key: String,
        value: String,
    },
    // extract archive, parse sources, save sources in arena, put app into state machine
    InstallApp {
        app: TremorAppDef,
        file: Vec<u8>,
    },
    // delete from statemachine, delete sources from arena
    UninstallApp {
        app: AppId,
        /// if `true`, stop all instances of this app
        force: bool,
    },
    Start {
        app: AppId,
        flow: FlowId,
        instance: InstanceId,
        config: HashMap<String, OwnedValue>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TremorStart {
    pub(crate) instance: InstanceId,
    pub(crate) config: HashMap<String, OwnedValue>,
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
 * TODO: SHould we explain how to create multiple `AppDataResponse`?
 *
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TremorResponse {
    pub value: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TremorSnapshot {
    pub meta: SnapshotMeta<TremorNodeId, TremorNode>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

fn sm_r_err<E: Error + 'static>(e: E) -> StorageError<TremorNodeId> {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Read,
        AnyError::new(&e),
    )
    .into()
}
fn sm_w_err<E: Error + 'static>(e: E) -> StorageError<TremorNodeId> {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Write,
        AnyError::new(&e),
    )
    .into()
}
fn sm_d_err<E: Error + 'static>(e: E) -> StorageError<TremorNodeId> {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Delete,
        AnyError::new(&e),
    )
    .into()
}
fn store_w_err(e: impl Error + 'static) -> StorageError<TremorNodeId> {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e)).into()
}
fn store_r_err(e: impl Error + 'static) -> StorageError<TremorNodeId> {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)).into()
}
fn vote_w_err(e: impl Error + 'static) -> StorageError<TremorNodeId> {
    StorageIOError::new(ErrorSubject::Vote, ErrorVerb::Write, AnyError::new(&e)).into()
}
fn vote_r_err(e: impl Error + 'static) -> StorageError<TremorNodeId> {
    StorageIOError::new(ErrorSubject::Vote, ErrorVerb::Read, AnyError::new(&e)).into()
}
fn logs_r_err(e: impl Error + 'static) -> StorageError<TremorNodeId> {
    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Read, AnyError::new(&e)).into()
}
fn logs_w_err(e: impl Error + 'static) -> StorageError<TremorNodeId> {
    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Read, AnyError::new(&e)).into()
}
fn snap_w_err(
    meta: SnapshotMeta<TremorNodeId, TremorNode>,
    e: impl Error + 'static,
) -> StorageError<TremorNodeId> {
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
//     e: impl Error + 'static,
// ) -> StorageError<ExampleNodeId> {
//     StorageIOError::new(
//         ErrorSubject::Snapshot(meta),
//         ErrorVerb::Read,
//         AnyError::new(&e),
//     )
//     .into()
// }

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SerializableTremorStateMachine {
    pub last_applied_log: Option<LogId<TremorNodeId>>,

    pub last_membership: EffectiveMembership<TremorNodeId, TremorNode>,

    /// Application data, for the k/v store
    pub data: BTreeMap<String, String>,

    /// Application data.
    pub archives: Vec<Vec<u8>>,

    /// Instances and their desired state
    pub instances: HashMap<AppId, Instances>,
}

impl TryFrom<&TremorStateMachine> for SerializableTremorStateMachine {
    type Error = StorageError<TremorNodeId>;

    fn try_from(state: &TremorStateMachine) -> Result<Self, Self::Error> {
        let data = state
            .db
            .iterator_cf(
                state
                    .db
                    .cf_handle(TremorStore::DATA)
                    .ok_or(StoreError::MissingCf(TremorStore::DATA))?,
                rocksdb::IteratorMode::Start,
            )
            .map(|kv| {
                let (key, value) = kv.map_err(sm_r_err)?;
                Ok((
                    String::from_utf8(key.to_vec()).map_err(sm_r_err)?,
                    String::from_utf8(value.to_vec()).map_err(sm_r_err)?,
                ))
            })
            .collect::<Result<_, Self::Error>>()?;
        let apps = state
            .db
            .iterator_cf(
                state
                    .db
                    .cf_handle(TremorStore::APPS)
                    .ok_or(StoreError::MissingCf(TremorStore::APPS))?,
                rocksdb::IteratorMode::Start,
            )
            .map(|kv| kv.map(|(_, tar)| tar.to_vec()))
            .collect::<Result<_, _>>()
            .map_err(sm_r_err)?;
        let instances = state
            .apps
            .iter()
            .map(|(k, v)| (k.clone(), v.instances.clone()))
            .collect();
        Ok(Self {
            last_applied_log: state.get_last_applied_log()?,
            last_membership: state.get_last_membership()?,
            data,
            archives: apps,
            instances,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
pub struct InstanceId(pub String);
impl Display for InstanceId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
pub struct FlowId(pub String);
impl Display for FlowId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
pub struct AppId(pub String);
impl Display for AppId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct FlowInstance {
    pub id: FlowId,
    pub config: HashMap<String, OwnedValue>,
    pub state: IntendedState,
}
pub type Instances = HashMap<InstanceId, FlowInstance>;

#[derive(Debug, Clone)]
pub(crate) struct StateApp {
    pub app: TremorAppDef,
    pub instances: Instances,
    arena_indices: Vec<arena::Index>,
    main: Deploy,
}

#[derive(Debug, Clone)]
pub(crate) struct TremorStateMachine {
    /// Application data.
    pub db: Arc<rocksdb::DB>,
    pub apps: HashMap<AppId, StateApp>,
    pub world: Runtime,
}

impl TremorStateMachine {
    /// data column family
    fn cf_data(&self) -> StorageResult<&ColumnFamily> {
        self.db
            .cf_handle(TremorStore::DATA)
            .ok_or(StoreError::MissingCf(TremorStore::DATA))
            .map_err(StorageError::from)
    }
    /// apps column family
    fn cf_apps(&self) -> StorageResult<&ColumnFamily> {
        self.db
            .cf_handle(TremorStore::APPS)
            .ok_or(StoreError::MissingCf(TremorStore::APPS))
            .map_err(StorageError::from)
    }
    /// instances column family
    fn cf_instances(&self) -> StorageResult<&ColumnFamily> {
        self.db
            .cf_handle(TremorStore::INSTANCES)
            .ok_or(StoreError::MissingCf(TremorStore::INSTANCES))
            .map_err(StorageError::from)
    }
    /// state machine column family
    fn cf_state_machine(&self) -> StorageResult<&ColumnFamily> {
        self.db
            .cf_handle(TremorStore::STATE_MACHINE)
            .ok_or(StoreError::MissingCf(TremorStore::STATE_MACHINE))
            .map_err(StorageError::from)
    }
    fn get_last_membership(&self) -> StorageResult<EffectiveMembership<TremorNodeId, TremorNode>> {
        self.db
            .get_cf(self.cf_state_machine()?, TremorStore::LAST_MEMBERSHIP)
            .map_err(sm_r_err)
            .and_then(|value| {
                value
                    .map(|v| serde_json::from_slice(&v).map_err(sm_r_err))
                    .unwrap_or_else(|| Ok(EffectiveMembership::default()))
            })
    }
    fn set_last_membership(
        &self,
        membership: EffectiveMembership<TremorNodeId, TremorNode>,
    ) -> StorageResult<()> {
        self.db
            .put_cf(
                self.cf_state_machine()?,
                TremorStore::LAST_MEMBERSHIP,
                serde_json::to_vec(&membership).map_err(sm_w_err)?,
            )
            .map_err(sm_w_err)
    }
    fn get_last_applied_log(&self) -> StorageResult<Option<LogId<TremorNodeId>>> {
        self.db
            .get_cf(self.cf_state_machine()?, TremorStore::LAST_APPLIED_LOG)
            .map_err(sm_r_err)
            .and_then(|value| {
                value
                    .map(|v| serde_json::from_slice(&v).map_err(sm_r_err))
                    .transpose()
            })
    }
    fn set_last_applied_log(&self, log_id: LogId<TremorNodeId>) -> StorageResult<()> {
        self.db
            .put_cf(
                self.cf_state_machine()?,
                TremorStore::LAST_APPLIED_LOG,
                serde_json::to_vec(&log_id).map_err(sm_w_err)?,
            )
            .map_err(sm_w_err)
    }
    fn delete_last_applied_log(&self) -> StorageResult<()> {
        self.db
            .delete_cf(self.cf_state_machine()?, TremorStore::LAST_APPLIED_LOG)
            .map_err(sm_d_err)
    }

    async fn apply_diff_from_snapshot(
        &mut self,
        snapshot: SerializableTremorStateMachine,
    ) -> Result<(), StoreError> {
        // load key value pairs
        for (key, value) in snapshot.data {
            self.db
                .put_cf(self.cf_data()?, key.as_bytes(), value.as_bytes())
                .map_err(sm_w_err)?;
        }
        // TODO: delete every key that is not in the snapshot - not necessarily needed today as we don't have a DELETE op on our k/v store

        // load archives
        for archive in snapshot.archives {
            self.load_archive(archive)?;
        }

        // load instances
        // stop and remove all instances not in the snapshot
        let snapshot_instances = snapshot
            .instances
            .iter()
            .flat_map(|(app_id, v)| {
                v.keys()
                    .map(|instance_id| (app_id.clone(), instance_id.clone()))
            })
            .collect::<HashSet<_>>();
        for (app_id, app) in &self.apps {
            for (instance, FlowInstance { id, config, state }) in &app.instances {
                if !snapshot_instances.contains((app_id, instance)) {
                    info!("Removing {app_id}::{id}::{instance}");
                    self.stop_and_remove_instance(app_id, instance).await?;
                }
            }
        }
        let (state_tx, state_rx) = unbounded();
        for (app_id, instances) in snapshot.instances {
            for (instance, FlowInstance { id, config, state }) in instances.into_iter() {
                let flow = match self.world.get_flow(id.to_string()).await {
                    Ok(flow) => flow,
                    Err(RuntimeError(ErrorKind::FlowNotFound(_), _)) => {
                        // flow is not alive, deploy it
                        self.deploy_flow(app_id, id, instance, config)
                            .await
                            .map_err(|_| StoreError::MissingFlow(app_id, id))?;
                        self.world.get_flow(id.to_string()).await?
                    }
                    Err(e) => {
                        return Err(StoreError::from(e));
                    }
                };
                flow.change_state(state, state_tx.clone()).await?;
                // TODO: if this failed, we might leave the runtime (instances) in an inconsistent state
                log_error!(
                    state_rx.recv().timeout(Duration::from_secs(2)).await??,
                    "Error changing state of flow {app_id}/{id}/{instance} to {state}: {e}"
                );
            }
        }
        if let Some(log_id) = snapshot.last_applied_log {
            self.set_last_applied_log(log_id)?;
        } else {
            self.delete_last_applied_log()?;
        }

        self.set_last_membership(snapshot.last_membership)?;

        Ok(())
    }

    async fn from_serializable(
        sm: SerializableTremorStateMachine,
        db: Arc<rocksdb::DB>,
        world: Runtime,
    ) -> StorageResult<Self> {
        let mut r = Self {
            db,
            world,
            apps: HashMap::new(),
        };

        // load archives
        for archive in sm.archives {
            r.load_archive(archive)?;
        }

        // load instances
        for (app_id, instances) in sm.instances {
            for (instance, FlowInstance { id, config, state }) in instances.into_iter() {
                r.deploy_flow(app_id.clone(), id, instance, config).await?;
            }
        }

        if let Some(log_id) = sm.last_applied_log {
            r.set_last_applied_log(log_id)?;
        }
        r.set_last_membership(sm.last_membership)?;

        Ok(r)
    }

    async fn new(db: Arc<rocksdb::DB>, world: Runtime) -> Result<TremorStateMachine, StoreError> {
        let mut r = Self {
            db: db.clone(),
            world,
            apps: HashMap::new(),
        };
        for kv in db.iterator_cf(
            db.cf_handle(TremorStore::APPS)
                .ok_or(StoreError::MissingCf(TremorStore::APPS))?,
            rocksdb::IteratorMode::Start,
        ) {
            let (_, archive) = kv?;
            r.load_archive(archive.to_vec())
                .map_err(|e| StoreError::Other(Box::new(e)))?;
        }

        // load instances
        let instances = db
            .iterator_cf(
                db.cf_handle(TremorStore::INSTANCES)
                    .ok_or(StoreError::MissingCf(TremorStore::INSTANCES))?,
                rocksdb::IteratorMode::Start,
            )
            .map(|kv| {
                let (app_id, instances) = kv?;
                let app_id = String::from_utf8(app_id.to_vec())?;
                let instances: Instances = serde_json::from_slice(&instances)?;
                Ok((AppId(app_id), instances))
            })
            .collect::<Result<HashMap<AppId, Instances>, StoreError>>()?;

        for (app_id, instances) in instances {
            for (instance, FlowInstance { id, config, state }) in instances.into_iter() {
                r.deploy_flow(app_id.clone(), id, instance, config)
                    .await
                    .map_err(|e| StoreError::Other(Box::new(e)))?;
            }
        }

        Ok(r)
    }

    fn insert(&self, key: String, value: String) -> StorageResult<()> {
        self.db
            .put_cf(self.cf_data()?, key.as_bytes(), value.as_bytes())
            .map_err(store_w_err)
    }

    /// Load app definition and `Deploy` AST from the tar-archive in `archive`
    /// and store the definition in the db and state machine
    fn load_archive(&mut self, archive: Vec<u8>) -> StorageResult<()> {
        let (app, main, arena_indices) = extract(&archive).map_err(store_w_err)?;

        self.db
            .put_cf(self.cf_apps()?, app.name().0.as_bytes(), &archive)
            .map_err(store_w_err)?;

        let app = StateApp {
            app,
            main,
            arena_indices,
            instances: HashMap::new(),
        };
        self.apps.insert(app.app.name().clone(), app);

        Ok(())
    }
    pub async fn deploy_flow(
        &mut self,
        app_id: AppId,
        flow: FlowId,
        instance: InstanceId,
        config: HashMap<String, OwnedValue>,
    ) -> StorageResult<()> {
        println!("deploy flow instance {app_id}/{flow}/{instance}");
        let app = self
            .apps
            .get_mut(&app_id)
            .ok_or_else(|| StoreError::MissingApp(app_id.clone()))?;

        let mut defn: FlowDefinition = app
            .main
            .deploy
            .scope
            .content
            .get(&flow.0)
            .ok_or_else(|| StoreError::MissingFlow(app_id.clone(), flow.clone()))?;
        let mid = defn.meta().clone();

        defn.params
            .ingest_creational_with(&CreationalWith {
                with: WithExprs(
                    config
                        .iter()
                        .map(|(k, v)| {
                            (
                                Ident::new(k.to_string().into(), Box::new(mid.clone())),
                                ImutExpr::literal(Box::new(mid.clone()), v.clone().into()),
                            )
                        })
                        .collect(),
                ),
                mid: Box::new(mid),
            })
            .map_err(|e| StoreError::from(e))?;

        let fake_aggr_reg = AggrRegistry::default();
        {
            let reg = &*FN_REGISTRY.read().map_err(store_w_err)?;
            let mut helper = Helper::new(reg, &fake_aggr_reg);
            Optimizer::new(&helper)
                .visitor
                .walk_flow_definition(&mut defn)
                .map_err(StoreError::from)?;

            let inner_args = defn.params.render().map_err(StoreError::from)?;

            ArgsRewriter::new(inner_args, &mut helper, defn.params.meta())
                .walk_flow_definition(&mut defn)
                .map_err(StoreError::from)?;
            Optimizer::new(&helper)
                .visitor
                .walk_flow_definition(&mut defn)
                .map_err(StoreError::from)?;
        }

        let deploy = DeployFlow {
            mid: Box::new(defn.meta().clone()),
            from_target: tremor_script::ast::NodeId::new(&flow.0, &[app_id.0.clone()]),
            instance_alias: instance.0.clone(),
            defn,
            docs: None,
        };
        app.instances.insert(
            instance.clone(),
            FlowInstance {
                id: flow,
                config,
                state: IntendedState::Running, // FIXME
            },
        );
        let instances = serde_json::to_vec(&app.instances).map_err(store_w_err)?;

        self.db
            .put_cf(self.cf_instances()?, app_id.0.as_bytes(), &instances)
            .map_err(store_w_err)?;

        self.world.deploy_flow(&deploy).await.map_err(store_w_err)?;

        Ok(())
    }

    pub fn get(&self, key: &str) -> StorageResult<Option<String>> {
        let key = key.as_bytes();
        self.db
            .get_cf(self.cf_data()?, key)
            .map(|value| {
                if let Some(value) = value {
                    Some(String::from_utf8(value.to_vec()).ok()?)
                } else {
                    None
                }
            })
            .map_err(store_r_err)
    }
}

#[derive(Debug)]
pub struct TremorStore {
    db: Arc<rocksdb::DB>,
    runtime: Runtime,
    /// The Raft state machine.
    pub(crate) state_machine: RwLock<TremorStateMachine>,
}
type StorageResult<T> = Result<T, StorageError<TremorNodeId>>;

/// converts an id to a byte vector for storing in the database.
/// Note that we're using big endian encoding to ensure correct sorting of keys
fn id_to_bin(id: u64) -> Result<Vec<u8>, StoreError> {
    let mut buf = Vec::with_capacity(8);
    buf.write_u64::<BigEndian>(id)?;
    Ok(buf)
}

fn bin_to_id(buf: &[u8]) -> Result<u64, StoreError> {
    Ok((&buf[0..8]).read_u64::<BigEndian>()?)
}

#[derive(Debug)]
pub enum StoreError {
    MissingCf(&'static str),
    Utf8(FromUtf8Error),
    StrUtf8(std::str::Utf8Error),
    JSON(serde_json::Error),
    RocksDB(rocksdb::Error),
    Io(std::io::Error),
    // FIXME: this is horrid, aaaaaahhhhh!
    Tremor(Mutex<crate::errors::Error>),
    TremorScript(Mutex<tremor_script::errors::Error>),
    MissingApp(AppId),
    MissingFlow(AppId, FlowId),
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl From<FromUtf8Error> for StoreError {
    fn from(e: FromUtf8Error) -> Self {
        StoreError::Utf8(e)
    }
}

impl From<std::str::Utf8Error> for StoreError {
    fn from(e: std::str::Utf8Error) -> Self {
        StoreError::StrUtf8(e)
    }
}

impl From<serde_json::Error> for StoreError {
    fn from(e: serde_json::Error) -> Self {
        StoreError::JSON(e)
    }
}

impl From<rocksdb::Error> for StoreError {
    fn from(e: rocksdb::Error) -> Self {
        StoreError::RocksDB(e)
    }
}

impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        StoreError::Io(e)
    }
}

impl From<crate::errors::Error> for StoreError {
    fn from(e: crate::errors::Error) -> Self {
        StoreError::Tremor(Mutex::new(e))
    }
}
impl From<tremor_script::errors::Error> for StoreError {
    fn from(e: tremor_script::errors::Error) -> Self {
        StoreError::TremorScript(Mutex::new(e))
    }
}

impl Error for StoreError {}
impl Display for StoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::MissingCf(cf) => write!(f, "missing column family: `{}`", cf),
            StoreError::Utf8(e) => write!(f, "invalid utf8: {}", e),
            StoreError::StrUtf8(e) => write!(f, "invalid utf8: {}", e),
            StoreError::JSON(e) => write!(f, "invalid json: {}", e),
            StoreError::RocksDB(e) => write!(f, "rocksdb error: {}", e),
            StoreError::Io(e) => write!(f, "io error: {}", e),
            StoreError::Tremor(e) => write!(f, "tremor error: {:?}", e.lock()),
            StoreError::TremorScript(e) => write!(f, "tremor script error: {:?}", e.lock()),
            StoreError::Other(e) => write!(f, "other error: {}", e),
            StoreError::MissingApp(app) => write!(f, "missing app: {}", app),
            StoreError::MissingFlow(app, flow) => write!(f, "missing flow: {}::{}", app, flow),
        }
    }
}
impl<NID: NodeId> From<StoreError> for StorageError<NID> {
    fn from(e: StoreError) -> StorageError<NID> {
        StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)).into()
    }
}

impl TremorStore {
    fn flush(&self) -> Result<(), StoreError> {
        self.db.flush_wal(true)?;
        let mut opts = FlushOptions::default();
        opts.set_wait(true);
        self.db.flush_opt(&opts)?;
        Ok(())
    }

    fn put<K, V>(&self, cf: &ColumnFamily, key: K, value: V) -> Result<(), StoreError>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.db.put_cf(cf, key, value)?;
        self.flush()?;
        Ok(())
    }
    /// store column family
    fn cf_store(&self) -> Result<&ColumnFamily, StoreError> {
        self.db
            .cf_handle(TremorStore::STORE)
            .ok_or(StoreError::MissingCf(TremorStore::STORE))
    }

    /// node column family
    fn cf_node(&self) -> Result<&ColumnFamily, StoreError> {
        self.db
            .cf_handle(TremorStore::NODE)
            .ok_or(StoreError::MissingCf(TremorStore::NODE))
    }

    /// logs columns family
    fn cf_logs(&self) -> Result<&ColumnFamily, StoreError> {
        self.db
            .cf_handle(TremorStore::LOGS)
            .ok_or(StoreError::MissingCf(TremorStore::LOGS))
    }

    fn get_last_purged_(&self) -> StorageResult<Option<LogId<TremorNodeId>>> {
        Ok(self
            .db
            .get_cf(self.cf_store()?, TremorStore::LAST_PURGED_LOG_ID)
            .map_err(|e| store_r_err(e))?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    fn set_last_purged_(&self, log_id: LogId<TremorNodeId>) -> StorageResult<()> {
        self.put(
            self.cf_store()?,
            TremorStore::LAST_PURGED_LOG_ID,
            serde_json::to_vec(&log_id)
                .map_err(StoreError::JSON)?
                .as_slice(),
        )
        .map_err(store_w_err)
    }

    fn get_snapshot_index_(&self) -> StorageResult<u64> {
        Ok(self
            .db
            .get_cf(self.cf_store()?, TremorStore::SNAPSHOT_INDEX)
            .map_err(store_r_err)?
            .map(|v| serde_json::from_slice(&v).map_err(StoreError::JSON))
            .transpose()?
            .unwrap_or_default())
    }

    fn set_snapshot_index_(&self, snapshot_index: u64) -> StorageResult<()> {
        self.put(
            self.cf_store()?,
            TremorStore::SNAPSHOT_INDEX,
            serde_json::to_vec(&snapshot_index)
                .map_err(store_w_err)?
                .as_slice(),
        )
        .map_err(store_w_err)?;
        Ok(())
    }

    fn set_vote_(&self, vote: &Vote<TremorNodeId>) -> StorageResult<()> {
        self.put(
            self.cf_store()?,
            TremorStore::VOTE,
            &serde_json::to_vec(vote).map_err(vote_w_err)?,
        )
        .map_err(vote_w_err)
    }

    fn get_vote_(&self) -> StorageResult<Option<Vote<TremorNodeId>>> {
        Ok(self
            .db
            .get_cf(self.cf_store()?, TremorStore::VOTE)
            .map_err(vote_r_err)?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    fn get_current_snapshot_(&self) -> StorageResult<Option<TremorSnapshot>> {
        Ok(self
            .db
            .get_cf(self.cf_store()?, TremorStore::SNAPSHOT)
            .map_err(store_r_err)?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    fn set_current_snapshot_(&self, snap: TremorSnapshot) -> StorageResult<()> {
        self.put(
            self.cf_store()?,
            TremorStore::SNAPSHOT,
            serde_json::to_vec(&snap).unwrap(),
        )
        .map_err(|e| snap_w_err(snap.meta, e))?;
        Ok(())
    }
}

#[async_trait]
impl RaftLogReader<TremorTypeConfig> for Arc<TremorStore> {
    async fn get_log_state(&mut self) -> StorageResult<LogState<TremorTypeConfig>> {
        let last = self
            .db
            .iterator_cf(self.cf_logs()?, rocksdb::IteratorMode::End)
            .next()
            .and_then(|d| {
                let (_, ent) = d.ok()?;
                Some(
                    serde_json::from_slice::<Entry<TremorTypeConfig>>(&ent)
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
    ) -> StorageResult<Vec<Entry<TremorTypeConfig>>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => id_to_bin(*x),
            std::ops::Bound::Excluded(x) => id_to_bin(*x + 1),
            std::ops::Bound::Unbounded => id_to_bin(0),
        }?;
        self.db
            .iterator_cf(
                self.cf_logs()?,
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
}

#[async_trait]
impl RaftSnapshotBuilder<TremorTypeConfig, Cursor<Vec<u8>>> for Arc<TremorStore> {
    // #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(
        &mut self,
    ) -> StorageResult<Snapshot<TremorNodeId, TremorNode, Cursor<Vec<u8>>>> {
        let data;
        let last_applied_log;
        let last_membership;

        {
            // Serialize the data of the state machine.
            let state_machine =
                SerializableTremorStateMachine::try_from(&*self.state_machine.read().await)?;
            data = serde_json::to_vec(&state_machine).map_err(sm_r_err)?;

            last_applied_log = state_machine.last_applied_log;
            last_membership = state_machine.last_membership.clone();
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
            last_membership,
            snapshot_id,
        };

        let snapshot = TremorSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        self.set_current_snapshot_(snapshot)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[async_trait]
impl RaftStorage<TremorTypeConfig> for Arc<TremorStore> {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    // #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(&mut self, vote: &Vote<TremorNodeId>) -> StorageResult<()> {
        self.set_vote_(vote)
    }

    async fn read_vote(&mut self) -> StorageResult<Option<Vote<TremorNodeId>>> {
        self.get_vote_()
    }

    // #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(&mut self, entries: &[&Entry<TremorTypeConfig>]) -> StorageResult<()> {
        for entry in entries {
            let id = id_to_bin(entry.log_id.index)?;
            assert_eq!(bin_to_id(&id)?, entry.log_id.index);
            self.put(
                self.cf_logs()?,
                &id,
                &serde_json::to_vec(entry).map_err(logs_w_err)?,
            )
            .map_err(logs_w_err)?;
        }
        Ok(())
    }

    // #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<TremorNodeId>,
    ) -> StorageResult<()> {
        debug!("delete_conflict_logs_since: [{log_id}, +oo)");

        let from = id_to_bin(log_id.index)?;
        let to = id_to_bin(0xff_ff_ff_ff_ff_ff_ff_ff)?;
        self.db
            .delete_range_cf(self.cf_logs()?, &from, &to)
            .map_err(logs_w_err)
    }

    // #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(&mut self, log_id: LogId<TremorNodeId>) -> StorageResult<()> {
        debug!("purge_logs_upto: [0, {log_id}]");

        self.set_last_purged_(log_id)?;
        let from = id_to_bin(0)?;
        let to = id_to_bin(log_id.index + 1)?;
        self.db
            .delete_range_cf(self.cf_logs()?, &from, &to)
            .map_err(logs_w_err)
    }

    async fn last_applied_state(
        &mut self,
    ) -> StorageResult<(
        Option<LogId<TremorNodeId>>,
        EffectiveMembership<TremorNodeId, TremorNode>,
    )> {
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
        entries: &[&Entry<TremorTypeConfig>],
    ) -> StorageResult<Vec<TremorResponse>> {
        let mut res = Vec::with_capacity(entries.len());

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            debug!("[{}] replicate to sm", entry.log_id);

            sm.set_last_applied_log(entry.log_id)?;

            match entry.payload {
                EntryPayload::Blank => res.push(TremorResponse { value: None }),
                EntryPayload::Normal(ref req) => match req {
                    TremorRequest::Set { key, value } => {
                        debug!("[{}] replicate set to sm", entry.log_id);
                        sm.insert(key.clone(), value.clone())?;
                        res.push(TremorResponse {
                            value: Some(value.clone()),
                        })
                    }
                    TremorRequest::InstallApp { app, file } => {
                        debug!("[{}] installing app {app:?}", entry.log_id);
                        sm.load_archive(file.clone())?;
                        res.push(TremorResponse {
                            value: Some(app.name().to_string()),
                        });
                    }
                    TremorRequest::Start {
                        app,
                        flow,
                        instance,
                        config,
                    } => {
                        debug!(
                            "[{}] start {app}/{flow} as {instance} with config: {config:?}",
                            entry.log_id
                        );
                        sm.deploy_flow(app.clone(), flow.clone(), instance.clone(), config.clone())
                            .await?;
                        res.push(TremorResponse {
                            value: Some(instance.to_string()),
                        });
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    debug!("[{}] replicate membership to sm", entry.log_id);
                    sm.set_last_membership(EffectiveMembership::new(
                        Some(entry.log_id),
                        mem.clone(),
                    ))?;
                    res.push(TremorResponse { value: None })
                }
            };
        }
        self.db.flush_wal(true).map_err(logs_w_err)?;
        Ok(res)
    }

    // #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(&mut self) -> StorageResult<Box<Self::SnapshotData>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    // #[tracing::instrument(level = "trace", skip(self, snapshot))]
    // FIXME: this will not tear down old states at the moment, we need to make sure that we
    // also remove / stop / pause flows and apps based on the delta
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<TremorNodeId, TremorNode>,
        snapshot: Box<Self::SnapshotData>,
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
            // FIXME: this should take the updated state machine and apply its deltas to the current `state_machine`
            // *state_machine = TremorStateMachine::from_serializable(
            //     updated_state_machine,
            //     self.db.clone(),
            //     self.runtime.clone(),
            // )
            // .await?;
        }

        self.set_current_snapshot_(new_snapshot)?;
        Ok(())
    }

    // #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> StorageResult<Option<Snapshot<TremorNodeId, TremorNode, Self::SnapshotData>>> {
        match TremorStore::get_current_snapshot_(self)? {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
impl TremorStore {
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
    const VOTE: &'static str = "vote";
    const SNAPSHOT: &'static str = "snapshot";

    /// Initialize the rocksdb column families.
    ///
    /// This function is safe and never cleans up or resets the current state,
    /// but creates a new db if there is none.
    fn init_db<P: AsRef<Path>>(db_path: P) -> Result<DB, ClusterError> {
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let node = ColumnFamilyDescriptor::new(TremorStore::NODE, Options::default());
        let store = ColumnFamilyDescriptor::new(TremorStore::STORE, Options::default());
        let state_machine =
            ColumnFamilyDescriptor::new(TremorStore::STATE_MACHINE, Options::default());
        let data = ColumnFamilyDescriptor::new(TremorStore::DATA, Options::default());
        let logs = ColumnFamilyDescriptor::new(TremorStore::LOGS, Options::default());
        let apps = ColumnFamilyDescriptor::new(TremorStore::APPS, Options::default());
        let instances = ColumnFamilyDescriptor::new(TremorStore::INSTANCES, Options::default());

        DB::open_cf_descriptors(
            &db_opts,
            db_path,
            vec![node, store, state_machine, data, logs, apps, instances],
        )
        .map_err(ClusterError::Rocks)
    }

    pub(crate) fn get_node_data(&self) -> Result<(TremorNodeId, String, String), ClusterError> {
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
        node_id: TremorNodeId,
        rpc_addr: impl Into<String> + ToSocketAddrs,
        api_addr: impl Into<String> + ToSocketAddrs,
        world: Runtime,
    ) -> Result<Arc<TremorStore>, ClusterError> {
        let db = Self::init_db(db_path)?;
        let node_id = id_to_bin(*node_id)?;
        if let Err(e) = rpc_addr.to_socket_addrs().await {
            return Err(ClusterError::Other(format!("Invalid rpc_addr {e}")));
        }
        if let Err(e) = api_addr.to_socket_addrs().await {
            return Err(ClusterError::Other(format!("Invalid api_add {e}")));
        }

        let cf = db
            .cf_handle(TremorStore::NODE)
            .ok_or("no node column family")?;

        db.put_cf(cf, "node_id", node_id)?;
        db.put_cf(cf, "rpc_addr", rpc_addr.into().as_bytes())?;
        db.put_cf(cf, "api_addr", api_addr.into().as_bytes())?;
        let db = Arc::new(db);
        let state_machine = RwLock::new(
            TremorStateMachine::new(db.clone(), world.clone())
                .await
                .map_err(StoreError::from)?,
        );
        Ok(Arc::new(Self {
            db,
            state_machine,
            runtime: world,
        }))
    }

    /// loading constructor - loading the given database
    ///
    /// verifying that we have some node-data stored
    pub(crate) async fn load<P: AsRef<Path>>(
        db_path: P,
        world: Runtime,
    ) -> Result<Arc<TremorStore>, ClusterError> {
        let db = Arc::new(Self::init_db(db_path)?);
        let state_machine = RwLock::new(
            TremorStateMachine::new(db.clone(), world.clone())
                .await
                .map_err(StoreError::from)?,
        );
        let this = Self {
            db,
            state_machine,
            runtime: world,
        };
        Ok(Arc::new(this))
    }

    pub fn get_api_addr(&self) -> Result<Option<String>, StoreError> {
        let api_addr = self
            .db
            .get_cf(self.cf_node()?, "api_addr")?
            .map(|v| String::from_utf8(v).map_err(StoreError::Utf8))
            .transpose()?;
        Ok(api_addr)
    }
    pub fn get_rpc_addr(&self) -> Result<Option<String>, StoreError> {
        self.db
            .get_cf(self.cf_node()?, "rpc_addr")?
            .map(|v| String::from_utf8(v).map_err(StoreError::Utf8))
            .transpose()
    }
    pub fn get_node_id(&self) -> Result<Option<TremorNodeId>, StoreError> {
        self.db
            .get_cf(self.cf_node()?, "node_id")?
            .map(|v| bin_to_id(&v).map(TremorNodeId::from))
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
        let db = TremorStore::init_db(dir.path())?;
        let handle = db.cf_handle(TremorStore::NODE).unwrap();
        let data = vec![1_u8, 2_u8, 3_u8];
        db.put_cf(handle, "node_id", data.clone())?;
        drop(db);

        let db2 = TremorStore::init_db(dir.path())?;
        let handle2 = db2.cf_handle(TremorStore::NODE).unwrap();
        let res2 = db2.get_cf(handle2, "node_id")?;
        assert_eq!(Some(data), res2);
        Ok(())
    }
}
