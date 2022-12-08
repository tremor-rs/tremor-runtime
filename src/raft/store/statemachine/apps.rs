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

use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
    sync::Arc,
};

use halfbrown::HashMap;
use rocksdb::ColumnFamily;
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

use crate::{
    instance::IntendedState,
    raft::{
        archive::{extract, get_app, TremorAppDef},
        store::{
            self,
            statemachine::{sm_d_err, sm_r_err, sm_w_err, RaftStateMachine},
            store_w_err, AppsRequest, StorageResult, TremorResponse,
        },
    },
    system::{flow::Alias as FlowAlias, Runtime},
};

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
    pub config: HashMap<String, simd_json::OwnedValue>,
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

#[derive(Clone, Debug)]
pub(crate) struct AppsStateMachine {
    db: Arc<rocksdb::DB>,
    apps: HashMap<AppId, StateApp>,
    world: Runtime,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct AppsSnapshot {
    /// App definition archives
    archives: Vec<Vec<u8>>,

    /// Instances and their desired state
    instances: HashMap<AppId, Instances>,
}

#[async_trait::async_trait]
impl RaftStateMachine<AppsSnapshot, AppsRequest> for AppsStateMachine {
    async fn load(db: &Arc<rocksdb::DB>, world: &Runtime) -> Result<Self, store::Error>
    where
        Self: std::marker::Sized,
    {
        let mut me = Self {
            db: db.clone(),
            apps: HashMap::new(),
            world: world.clone(),
        };
        // load apps
        for kv in db.iterator_cf(Self::cf_apps(db)?, rocksdb::IteratorMode::Start) {
            let (_, archive) = kv?;
            me.load_archive(&archive)
                .map_err(|e| store::Error::Other(Box::new(e)))?;
        }

        // load instances
        let instances = db
            .iterator_cf(Self::cf_instances(db)?, rocksdb::IteratorMode::Start)
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
                me.deploy_flow(&app_id, id, instance, config, state)
                    .await
                    .map_err(store::Error::Storage)?;
            }
        }
        Ok(me)
    }

    async fn apply_diff_from_snapshot(&mut self, snapshot: &AppsSnapshot) -> StorageResult<()> {
        // load archives
        let mut snapshot_apps = HashSet::with_capacity(snapshot.archives.len());
        for archive in &snapshot.archives {
            let app_def = get_app(archive).map_err(sm_r_err)?;
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
                    self.load_archive(archive)?;
                }
            } else {
                info!("Loading app '{}'", &app_def.name);
                self.load_archive(archive)?;
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
        Ok(())
    }

    fn as_snapshot(&self) -> StorageResult<AppsSnapshot> {
        let archives = self
            .db
            .iterator_cf(Self::cf_apps(&self.db)?, rocksdb::IteratorMode::Start)
            .map(|kv| kv.map(|(_, tar)| tar.to_vec()))
            .collect::<Result<Vec<Vec<u8>>, _>>()
            .map_err(sm_r_err)?;
        let instances = self
            .apps
            .iter()
            .map(|(k, v)| (k.clone(), v.instances.clone()))
            .collect();
        Ok(AppsSnapshot {
            archives,
            instances,
        })
    }

    async fn transition(&mut self, cmd: &AppsRequest) -> StorageResult<TremorResponse> {
        match cmd {
            AppsRequest::InstallApp { app, file } => {
                self.load_archive(file)?;
                Ok(TremorResponse {
                    value: Some(app.name().to_string()),
                })
            }
            AppsRequest::UninstallApp { app, force } => {
                self.uninstall_app(app, *force).await?;
                Ok(TremorResponse {
                    value: Some(app.to_string()),
                })
            }
            AppsRequest::Deploy {
                app,
                flow,
                instance,
                config,
                state,
            } => {
                self.deploy_flow(app, flow.clone(), instance.clone(), config.clone(), *state)
                    .await?;
                Ok(TremorResponse {
                    value: Some(instance.to_string()),
                })
            }
            AppsRequest::Undeploy { app, instance } => {
                self.stop_and_remove_flow(app, instance).await?;
                Ok(TremorResponse {
                    value: Some(instance.to_string()),
                })
            }
            AppsRequest::InstanceStateChange {
                app,
                instance,
                state,
            } => {
                self.change_flow_state(app, instance, *state).await?;
                Ok(TremorResponse {
                    value: Some(instance.to_string()),
                })
            }
        }
    }
}

impl AppsStateMachine {
    const CF_APPS: &str = "apps";
    const CF_INSTANCES: &str = "instances";

    fn cf_apps(db: &Arc<rocksdb::DB>) -> Result<&ColumnFamily, store::Error> {
        db.cf_handle(Self::CF_APPS)
            .ok_or(store::Error::MissingCf(Self::CF_APPS))
    }
    fn cf_instances(db: &Arc<rocksdb::DB>) -> Result<&ColumnFamily, store::Error> {
        db.cf_handle(Self::CF_INSTANCES)
            .ok_or(store::Error::MissingCf(Self::CF_INSTANCES))
    }

    /// Load app definition and `Deploy` AST from the tar-archive in `archive`
    /// and store the definition in the db and state machine
    fn load_archive(&mut self, archive: &[u8]) -> StorageResult<()> {
        let (app, main, arena_indices) = extract(archive).map_err(store_w_err)?;

        info!("Loading Archive for app: {}", app.name());

        self.db
            .put_cf(
                Self::cf_apps(&self.db).map_err(sm_r_err)?,
                app.name().0.as_bytes(),
                archive,
            )
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

    /// Deploy flow instance and transition it to the state we want
    /// Also store the instance into the state machine
    async fn deploy_flow(
        &mut self,
        app_id: &AppId,
        flow: FlowId,
        instance: InstanceId,
        config: HashMap<String, simd_json::OwnedValue>,
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
        let instances = serde_json::to_vec(&app.instances).map_err(sm_w_err)?;

        self.db
            .put_cf(
                Self::cf_instances(&self.db)?,
                app_id.0.as_bytes(),
                &instances,
            )
            .map_err(store_w_err)?;

        // deploy the flow but don't start it yet
        self.world.deploy_flow(&deploy).await.map_err(sm_w_err)?;
        // change the flow state to the intended state
        self.world
            .change_flow_state(instance.into(), intended_state)
            .await
            .map_err(sm_w_err)?;
        Ok(())
    }

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
                .delete_cf(Self::cf_apps(&self.db)?, app.app.name().0.as_bytes())
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

impl AppsStateMachine {
    pub(crate) fn get_app(&self, app_id: &AppId) -> Option<&StateApp> {
        self.apps.get(app_id)
    }

    pub(crate) fn get_instances(&self, app_id: &AppId) -> Option<&Instances> {
        self.apps.get(app_id).map(|app| &app.instances)
    }

    pub(crate) fn list(&self) -> impl Iterator<Item = (&AppId, &StateApp)> {
        self.apps.iter()
    }
}
