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
    instance::IntendedState,
    raft::{
        archive::{extract, get_app, TremorAppDef},
        node::Addr,
        store::{
            self, bin_to_id, id_to_bin, store_w_err, GetCfHandle, StorageResult, Store,
            TremorRequest, TremorResponse,
        },
    },
    system::{flow::Alias as FlowAlias, Runtime},
};
use openraft::{
    AnyError, EffectiveMembership, ErrorSubject, ErrorVerb, LogId, NodeId, StorageError,
    StorageIOError,
};
use rocksdb::ColumnFamily;
use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    error::Error,
    fmt::{Debug, Display, Formatter},
    hash::Hash,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tremor_script::{
    arena::{self, Arena},
    ast::{
        optimizer::Optimizer, visitors::ArgsRewriter, walkers::DeployWalker, CreationalWith,
        DeployFlow, FlowDefinition, Helper, Ident, ImutExpr, WithExprs,
    },
    deploy::Deploy,
    module::GetMod,
    prelude::BaseExpr,
    AggrRegistry, FN_REGISTRY,
};

use super::store_r_err;

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

    //pub last_membership: EffectiveMembership,
    /// Application data, for the k/v store
    pub data: BTreeMap<String, String>,

    /// Application data.
    pub archives: Vec<Vec<u8>>,

    /// Instances and their desired state
    pub instances: HashMap<AppId, Instances>,

    /// nodes known to the cluster (learners and voters)
    /// necessary for establishing a network connection to them
    pub known_nodes: HashMap<NodeId, Addr>,
}

impl SerializableTremorStateMachine {
    pub(crate) fn to_vec(&self) -> StorageResult<Vec<u8>> {
        serde_json::to_vec(&self).map_err(sm_r_err)
    }
}

impl TryFrom<&TremorStateMachine> for SerializableTremorStateMachine {
    type Error = StorageError;

    fn try_from(state: &TremorStateMachine) -> Result<Self, Self::Error> {
        let data = state
            .db
            .iterator_cf(
                state
                    .db
                    .cf_handle(Store::KV_DATA)
                    .ok_or(store::Error::MissingCf(Store::KV_DATA))?,
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
                    .cf_handle(Store::APPS)
                    .ok_or(store::Error::MissingCf(Store::APPS))?,
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
        let known_nodes = state.known_nodes.clone();
        let last_membership = state.get_last_membership()?;
        Ok(Self {
            last_applied_log: state.get_last_applied_log()?,
            last_membership,
            data,
            archives: apps,
            instances,
            known_nodes,
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

// FIXME: deduplicate those ids
impl From<InstanceId> for FlowAlias {
    fn from(instance_id: InstanceId) -> FlowAlias {
        FlowAlias::new(instance_id.0)
    }
}

impl From<&InstanceId> for FlowAlias {
    fn from(instance_id: &InstanceId) -> FlowAlias {
        FlowAlias::new(&instance_id.0)
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
    /// the id of the flow definition this instance is based upon
    pub definition: FlowId,
    pub config: HashMap<String, OwnedValue>,
    pub state: IntendedState,
}
pub type Instances = HashMap<InstanceId, FlowInstance>;

#[derive(Debug, Clone)]
pub(crate) struct StateApp {
    pub app: TremorAppDef,
    pub instances: Instances,
    /// we keep the arena indices around, so we can safely delete its contents
    arena_indices: Vec<arena::Index>,
    main: Deploy,
}

#[derive(Debug, Clone)]
pub(crate) struct TremorStateMachine {
    /// Application data.
    pub db: Arc<rocksdb::DB>,
    pub known_nodes: HashMap<NodeId, Addr>,
    pub apps: HashMap<AppId, StateApp>,
    pub world: Runtime,
    next_node_id: Arc<AtomicU64>,
}

/// DB Helpers
impl TremorStateMachine {
    /// data column family
    fn cf_data(&self) -> StorageResult<&ColumnFamily> {
        self.db
            .cf_handle(Store::KV_DATA)
            .ok_or(store::Error::MissingCf(Store::KV_DATA))
            .map_err(StorageError::from)
    }

    /// apps column family
    fn cf_apps(&self) -> StorageResult<&ColumnFamily> {
        self.db
            .cf_handle(Store::APPS)
            .ok_or(store::Error::MissingCf(Store::APPS))
            .map_err(StorageError::from)
    }

    /// instances column family
    fn cf_instances(&self) -> StorageResult<&ColumnFamily> {
        self.db
            .cf_handle(Store::INSTANCES)
            .ok_or(store::Error::MissingCf(Store::INSTANCES))
            .map_err(StorageError::from)
    }

    /// state machine column family
    fn cf_state_machine(&self) -> StorageResult<&ColumnFamily> {
        self.db
            .cf_handle(Store::STATE_MACHINE)
            .ok_or(store::Error::MissingCf(Store::STATE_MACHINE))
            .map_err(StorageError::from)
    }
}

/// Core impl
impl TremorStateMachine {
    pub(crate) async fn new(
        db: Arc<rocksdb::DB>,
        world: Runtime,
    ) -> Result<TremorStateMachine, store::Error> {
        let mut r = Self {
            db: db.clone(),
            world,
            apps: HashMap::new(),
            known_nodes: HashMap::new(),
            next_node_id: Arc::default(),
        };
        for kv in db.iterator_cf(
            db.cf_handle(Store::APPS)
                .ok_or(store::Error::MissingCf(Store::APPS))?,
            rocksdb::IteratorMode::Start,
        ) {
            let (_, archive) = kv?;
            r.load_archive(&archive)
                .map_err(|e| store::Error::Other(Box::new(e)))?;
        }

        // load instances
        let instances = db
            .iterator_cf(
                db.cf_handle(Store::INSTANCES)
                    .ok_or(store::Error::MissingCf(Store::INSTANCES))?,
                rocksdb::IteratorMode::Start,
            )
            .map(|kv| {
                let (app_id, instances) = kv?;
                let app_id = String::from_utf8(app_id.to_vec())?;
                let instances: Instances = serde_json::from_slice(&instances)?;
                Ok((AppId(app_id), instances))
            })
            .collect::<Result<HashMap<AppId, Instances>, store::Error>>()?;

        // start instances and put them into state machine state
        for (app_id, instances) in instances {
            for (
                instance,
                FlowInstance {
                    definition: id,
                    config,
                    state,
                },
            ) in instances
            {
                r.deploy_flow(&app_id, id, instance, config, state)
                    .await
                    .map_err(|e| store::Error::Other(Box::new(e)))?;
            }
        }
        // FIXME: store own node data somewhere else
        // FIXME: cf_nodes() for all `AddNode` related node ids
        // FIXME: cf_node_data for own data
        // load known nodes from db
        let known_nodes = db
            .iterator_cf(db.cf_nodes()?, rocksdb::IteratorMode::Start)
            .map(|x| {
                let (key_raw, value_raw) = x?;
                let node_id = bin_to_id(&key_raw)?;
                let addr: Addr = serde_json::from_slice(&value_raw)?;
                Ok((node_id, addr))
            })
            .collect::<Result<HashMap<NodeId, Addr>, store::Error>>()?;
        r.known_nodes = known_nodes;

        Ok(r)
    }

    pub(crate) fn get_last_membership(&self) -> StorageResult<Option<EffectiveMembership>> {
        self.db
            .get_cf(self.cf_state_machine()?, Store::LAST_MEMBERSHIP)
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
                Store::LAST_MEMBERSHIP,
                serde_json::to_vec(&membership).map_err(sm_w_err)?,
            )
            .map_err(sm_w_err)
    }

    pub(crate) fn get_last_applied_log(&self) -> StorageResult<Option<LogId>> {
        self.db
            .get_cf(self.cf_state_machine()?, Store::LAST_APPLIED_LOG)
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
                Store::LAST_APPLIED_LOG,
                serde_json::to_vec(&log_id).map_err(sm_w_err)?,
            )
            .map_err(sm_w_err)
    }

    fn delete_last_applied_log(&self) -> StorageResult<()> {
        self.db
            .delete_cf(self.cf_state_machine()?, Store::LAST_APPLIED_LOG)
            .map_err(sm_d_err)
    }

    // FIXME: reason about error handling and avoid leaving the state machine in an inconsistent state
    pub(crate) async fn apply_diff_from_snapshot(
        &mut self,
        snapshot: SerializableTremorStateMachine,
    ) -> StorageResult<()> {
        // load key value pairs
        for (key, value) in snapshot.data {
            self.db
                .put_cf(self.cf_data()?, key.as_bytes(), value.as_bytes())
                .map_err(sm_w_err)?;
        }
        // TODO: delete every key that is not in the snapshot - not necessarily needed today as we don't have a DELETE op on our k/v store

        // load archives
        let mut snapshot_apps = HashSet::with_capacity(snapshot.archives.len());
        for archive in snapshot.archives {
            let app_def = get_app(&archive).map_err(sm_r_err)?;
            snapshot_apps.insert(app_def.name().clone());
            if let Some(existing_app) = self.apps.get(&app_def.name) {
                // this is by no means secure or anything (archive contents can be forged), just a cheap way to compare for bytewise identity
                if app_def.sha256 != existing_app.app.sha256 {
                    info!(
                        "App definition '{}' changed. We need to restart all instances.",
                        &app_def.name
                    );
                    // this will stop all instances and then delete the app
                    self.uninstall_app(app_def.name(), true).await?;
                    info!("Reloading changed app '{}'", &app_def.name);
                    self.load_archive(&archive)?;
                }
            } else {
                info!("Loading app '{}'", &app_def.name);
                self.load_archive(&archive)?;
            }
        }
        // load instances, app by app
        let app_ids = self.apps.keys().cloned().collect::<Vec<_>>();
        for app_id in &app_ids {
            if let Some(snapshot_instances) = snapshot.instances.get(app_id) {
                let instances = self
                    .apps
                    .get(app_id)
                    .map(|app| app.instances.clone())
                    .expect("Dang, we just put this in the map, where is it gone?");
                for (instance_id, flow) in &instances {
                    if let Some(s_flow) = snapshot_instances.get(instance_id) {
                        // redeploy existing instance with different config
                        if s_flow.config != flow.config {
                            info!("Flow instance {app_id}/{instance_id} with parameters differ, redeploying...");
                            self.stop_and_remove_flow(app_id, instance_id).await?;
                            self.deploy_flow(
                                app_id,
                                s_flow.definition.clone(),
                                instance_id.clone(),
                                s_flow.config.clone(), // important: this is the new config
                                s_flow.state,
                            )
                            .await?;
                        } else if s_flow.state != flow.state {
                            // same flow, same config, different state - just change state

                            self.change_flow_state(app_id, instance_id, s_flow.state)
                                .await
                                .map_err(sm_w_err)?;
                        }
                    } else {
                        // stop and remove instances that are not in the snapshot
                        self.stop_and_remove_flow(app_id, instance_id).await?;
                    }
                }
                // deploy instances that are not in self
                for (s_instance_id, s_flow) in snapshot_instances {
                    if !instances.contains_key(s_instance_id) {
                        self.deploy_flow(
                            app_id,
                            s_flow.definition.clone(),
                            s_instance_id.clone(),
                            s_flow.config.clone(),
                            s_flow.state,
                        )
                        .await?;
                    }
                }
            } else {
                // uninstall apps that are not in the snapshot
                self.uninstall_app(app_id, true).await?;
            }
        }

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
            TremorRequest::Set { key, value } => {
                debug!("[{log_id}] replicate set to sm",);
                self.insert(key, value)?;
                Ok(TremorResponse {
                    value: Some(value.clone()),
                })
            }
            TremorRequest::AddNode { addr } => {
                let node_id = self.add_node(addr)?;
                Ok(TremorResponse {
                    value: Some(node_id.to_string()),
                })
            }
            TremorRequest::RemoveNode { node_id } => {
                self.remove_node(*node_id)?;
                Ok(TremorResponse {
                    value: Some(node_id.to_string()),
                })
            }
            TremorRequest::InstallApp { app, file } => {
                debug!("[{log_id}] installing app {app:?}");
                self.load_archive(file)?;
                Ok(TremorResponse {
                    value: Some(app.name().to_string()),
                })
            }
            TremorRequest::UninstallApp { app, force } => {
                debug!("[{log_id}] uninstall {app} force={force}");
                self.uninstall_app(app, *force).await?;
                Ok(TremorResponse {
                    value: Some(app.to_string()),
                })
            }
            TremorRequest::Deploy {
                app,
                flow,
                instance,
                config,
                state,
            } => {
                debug!("[{log_id}] start {app}/{flow} as {instance} with config: {config:?}",);
                self.deploy_flow(app, flow.clone(), instance.clone(), config.clone(), *state)
                    .await?;
                Ok(TremorResponse {
                    value: Some(instance.to_string()),
                })
            }
            TremorRequest::Undeploy { app, instance } => {
                debug!("[{log_id}] stop {app}/{instance}");
                self.stop_and_remove_flow(app, instance).await?;
                Ok(TremorResponse {
                    value: Some(instance.to_string()),
                })
            }
            TremorRequest::InstanceStateChange {
                app,
                instance,
                state,
            } => {
                debug!("[{log_id}] changings state for {app}/{instance} to `{state}");
                self.change_flow_state(app, instance, *state).await?;
                Ok(TremorResponse {
                    value: Some(instance.to_string()),
                })
            }
        }
    }
}

// Tremor Section
impl TremorStateMachine {
    /// stop a flow instance in the runtime
    /// and remove it from the state machine
    async fn stop_and_remove_flow(
        &mut self,
        app_id: &AppId,
        instance_id: &InstanceId,
    ) -> StorageResult<()> {
        info!("Stop and remove flow {app_id}/{instance_id}");
        if let Some(app) = self.apps.get_mut(app_id) {
            if app.instances.get(instance_id).is_some() {
                self.world
                    .stop_flow(instance_id.into())
                    .await
                    .map_err(sm_d_err)?;
                app.instances.remove(instance_id);
            }
        }
        Ok(())
    }

    /// Load app definition and `Deploy` AST from the tar-archive in `archive`
    /// and store the definition in the db and state machine
    fn load_archive(&mut self, archive: &[u8]) -> StorageResult<()> {
        let (app, main, arena_indices) = extract(archive).map_err(store_w_err)?;

        info!("Loading Archive for app: {}", app.name());

        self.db
            .put_cf(self.cf_apps()?, app.name().0.as_bytes(), archive)
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

    async fn uninstall_app(&mut self, app_id: &AppId, force: bool) -> StorageResult<()> {
        info!("Uninstall app: {app_id}");
        if let Some(app) = self.apps.remove(app_id) {
            if !app.instances.is_empty() && !force {
                // error out, we have running instances, which need to be stopped first
                return Err(sm_d_err(store::Error::RunningInstances(app_id.clone())));
            }
            // stop instances then delete the app
            for (instance_id, _instance) in app.instances {
                self.world
                    .stop_flow(instance_id.into())
                    .await
                    .map_err(sm_d_err)?;
            }
            self.db
                .delete_cf(self.cf_apps()?, app.app.name().0.as_bytes())
                .map_err(store_w_err)?;
            // delete from arena
            for aid in app.arena_indices {
                // ALLOW: we have stopped all instances, so nothing referencing those arena contents should be alive anymore (fingers crossed)
                unsafe {
                    Arena::delete_index_this_is_really_unsafe_dont_use_it(aid).map_err(sm_d_err)?;
                }
            }
        }
        Ok(())
    }

    /// Deploy flow instance and transition it to the state we want
    /// Also store the instance into the state machine
    pub async fn deploy_flow(
        &mut self,
        app_id: &AppId,
        flow: FlowId,
        instance: InstanceId,
        config: HashMap<String, OwnedValue>,
        intended_state: IntendedState,
    ) -> StorageResult<()> {
        info!("Deploying flow instance {app_id}/{flow}/{instance}");
        let app = self
            .apps
            .get_mut(app_id)
            .ok_or_else(|| store::Error::MissingApp(app_id.clone()))?;

        let mut defn: FlowDefinition = app
            .main
            .deploy
            .scope
            .content
            .get(&flow.0)
            .ok_or_else(|| store::Error::MissingFlow(app_id.clone(), flow.clone()))?;
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
            .map_err(store::Error::from)?;

        let fake_aggr_reg = AggrRegistry::default();
        {
            let reg = &*FN_REGISTRY.read().map_err(store_w_err)?;
            let mut helper = Helper::new(reg, &fake_aggr_reg);
            Optimizer::new(&helper)
                .visitor
                .walk_flow_definition(&mut defn)
                .map_err(store::Error::from)?;

            let inner_args = defn.params.render().map_err(store::Error::from)?;

            ArgsRewriter::new(inner_args, &mut helper, defn.params.meta())
                .walk_flow_definition(&mut defn)
                .map_err(store::Error::from)?;
            Optimizer::new(&helper)
                .visitor
                .walk_flow_definition(&mut defn)
                .map_err(store::Error::from)?;
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
                definition: flow,
                config,
                state: intended_state, // we are about to apply this state further below
            },
        );
        let instances = serde_json::to_vec(&app.instances).map_err(store_w_err)?;

        self.db
            .put_cf(self.cf_instances()?, app_id.0.as_bytes(), &instances)
            .map_err(store_w_err)?;

        self.world.deploy_flow(&deploy).await.map_err(sm_w_err)?;
        self.world
            .change_flow_state(instance.into(), intended_state)
            .await
            .map_err(sm_w_err)?;
        Ok(())
    }

    async fn change_flow_state(
        &mut self,
        app_id: &AppId,
        instance_id: &InstanceId,
        intended_state: IntendedState,
    ) -> StorageResult<()> {
        info!("Change flow state {app_id}/{instance_id} to {intended_state}");
        let app = self
            .apps
            .get_mut(app_id)
            .ok_or_else(|| store::Error::MissingApp(app_id.clone()))?;
        let instance = app
            .instances
            .get_mut(instance_id)
            .ok_or_else(|| store::Error::MissingInstance(app_id.clone(), instance_id.clone()))?;
        // set the intended state in our state machine
        instance.state = intended_state;
        // ... and attempt to bring the flow instance in the runtime in the desired state
        self.world
            .change_flow_state(instance_id.into(), intended_state)
            .await
            .map_err(sm_w_err)?;
        Ok(())
    }
}

// nodes section
impl TremorStateMachine {
    fn next_node_id(&self) -> NodeId {
        self.next_node_id.fetch_add(1, Ordering::SeqCst)
    }

    pub(crate) fn get_node_id(&self, addr: &Addr) -> Option<&NodeId> {
        self.known_nodes
            .iter()
            .find(|(_node_id, existing_addr)| *existing_addr == addr)
            .map(|(node_id, _)| node_id)
    }

    pub(crate) fn get_node(&self, node_id: NodeId) -> Option<&Addr> {
        self.known_nodes.get(&node_id)
    }

    fn add_node(&mut self, addr: &Addr) -> StorageResult<NodeId> {
        if let Some(node_id) = self.get_node_id(addr) {
            Err(store_w_err(store::Error::NodeAlreadyAdded(*node_id)))
        } else {
            let node_id = self.next_node_id();
            let node_id_bytes = id_to_bin(node_id)?;
            self.db
                .put_cf(
                    self.db.cf_nodes()?,
                    node_id_bytes,
                    serde_json::to_vec(addr).map_err(sm_w_err)?,
                )
                .map_err(sm_w_err)?;
            self.known_nodes.insert(node_id, addr.clone());
            debug!(target: "TremorStateMachine::add_node", "Node {addr} added as node {node_id}");
            Ok(node_id)
        }
    }

    fn remove_node(&mut self, node_id: NodeId) -> StorageResult<()> {
        self.known_nodes.remove(&node_id);
        let node_id_bytes = id_to_bin(node_id)?;
        self.db
            .delete_cf(self.db.cf_nodes()?, node_id_bytes)
            .map_err(sm_d_err)?;
        Ok(())
    }
}

// KV Section
impl TremorStateMachine {
    /// Store `value` at `key` in the distributed KV store
    pub(crate) fn insert(&self, key: &str, value: &str) -> StorageResult<()> {
        self.db
            .put_cf(self.cf_data()?, key.as_bytes(), value.as_bytes())
            .map_err(store_w_err)
    }

    /// try to obtain the value at the given `key`.
    /// Returns `Ok(None)` if there is no value for that key.
    pub(crate) fn get(&self, key: &str) -> StorageResult<Option<String>> {
        let key = key.as_bytes();
        self.db
            .get_cf(self.cf_data()?, key)
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
