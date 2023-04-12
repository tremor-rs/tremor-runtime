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
    ids::{AppFlowInstanceId, AppId, FlowDefinitionId},
    instance::IntendedState,
    raft::{
        api::{APIRequest, APIResult, AppError, ArgsError, ToAPIResult, API_WORKER_TIMEOUT},
        archive::{get_app, TremorAppDef},
        store::{
            AppsRequest as AppsCmd, FlowInstance, Instances, StateApp, TremorInstanceState,
            TremorRequest, TremorResponse, TremorStart,
        },
    },
};
use axum::{
    extract::{self, Json, State},
    routing::{delete, post},
    Router,
};
use std::collections::HashMap;
use std::fmt::Display;
use tokio::time::timeout;

pub(crate) fn endpoints() -> Router<APIRequest> {
    Router::<APIRequest>::new()
        .route("/", post(install_app).get(list))
        .route("/:app", delete(uninstall_app))
        .route("/:app/flows/:flow", post(start))
        .route(
            "/:app/instances/:instance",
            post(manage_instance).delete(stop_instance),
        )
}

async fn install_app(
    extract::State(state): extract::State<APIRequest>,
    extract::OriginalUri(uri): extract::OriginalUri,
    Json(file): Json<Vec<u8>>,
) -> APIResult<Json<AppId>> {
    let app = get_app(&file)?;
    let app_id = app.name().clone();

    if timeout(
        API_WORKER_TIMEOUT,
        state.raft_manager.get_app_local(app_id.clone()),
    )
    .await??
    .is_some()
    {
        return Err(AppError::AlreadyInstalled(app.name).into());
    }
    let request = TremorRequest::Apps(AppsCmd::InstallApp {
        app,
        file: file.clone(),
    });
    state
        .raft
        .client_write(request)
        .await
        .to_api_result(&uri, &state)
        .await?;
    Ok(Json(app_id))
}

async fn uninstall_app(
    extract::State(state): extract::State<APIRequest>,
    extract::OriginalUri(uri): extract::OriginalUri,
    extract::Path(app_id): extract::Path<AppId>,
) -> APIResult<TremorResponse> {
    let app = timeout(
        API_WORKER_TIMEOUT,
        state.raft_manager.get_app_local(app_id.clone()),
    )
    .await??;
    if let Some(app) = app {
        if !app.instances.is_empty() {
            return Err(AppError::HasInstances(
                app_id.clone(),
                app.instances
                    .keys()
                    .map(|alias| AppFlowInstanceId::new(app_id.clone(), alias.clone()))
                    .collect(),
            )
            .into());
        }
    } else {
        return Err(AppError::AppNotFound(app_id.clone()).into());
    }
    let request = TremorRequest::Apps(AppsCmd::UninstallApp {
        app: app_id.clone(),
        force: false,
    });
    state
        .raft
        .client_write(request)
        .await
        .to_api_result(&uri, &state)
        .await
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AppState {
    pub def: TremorAppDef,
    pub instances: Instances,
}

impl From<&StateApp> for AppState {
    fn from(state: &StateApp) -> Self {
        AppState {
            def: state.app.clone(),
            instances: state.instances.clone(),
        }
    }
}

impl Display for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Name: {}", self.def.name)?;
        writeln!(f, "Flows: ")?;
        for (name, flow) in &self.def.flows {
            writeln!(f, " - {name}")?;
            write!(f, "   Config:")?;
            if flow.args.is_empty() {
                writeln!(f, " -")?;
            } else {
                writeln!(f)?;
                for (name, val) in &flow.args {
                    write!(f, "    - {name}: ")?;
                    if let Some(val) = val {
                        writeln!(f, "{val}")?;
                    } else {
                        writeln!(f, "-")?;
                    }
                }
            }
        }
        writeln!(f, "   Instances: ")?;
        for (
            name,
            FlowInstance {
                definition,
                config,
                state,
                ..
            },
        ) in &self.instances
        {
            writeln!(f, "    - {name}")?;
            writeln!(f, "      Flow definition: {definition}")?;
            writeln!(f, "      Config:")?;
            if config.is_empty() {
                writeln!(f, " -")?;
            } else {
                writeln!(f)?;
                for (name, val) in config {
                    writeln!(f, "        - {name}: {val}")?;
                }
            }
            writeln!(f, "      State: {state}")?;
        }
        Ok(())
    }
}

async fn list(State(state): State<APIRequest>) -> APIResult<Json<HashMap<AppId, AppState>>> {
    let apps = timeout(API_WORKER_TIMEOUT, state.raft_manager.get_apps_local()).await??;
    Ok(Json(apps))
}

async fn start(
    extract::State(state): extract::State<APIRequest>,
    extract::OriginalUri(uri): extract::OriginalUri,
    extract::Path((app_id, flow_id)): extract::Path<(AppId, FlowDefinitionId)>,
    Json(body): Json<TremorStart>,
) -> APIResult<Json<AppFlowInstanceId>> {
    let instance_id = body.instance.clone();

    let app = timeout(
        API_WORKER_TIMEOUT,
        state.raft_manager.get_app_local(app_id.clone()),
    )
    .await??;

    if let Some(app) = app {
        if app.instances.contains_key(instance_id.instance_id()) {
            return Err(AppError::InstanceAlreadyExists(body.instance).into());
        }
        if let Some(flow) = app.app.flows.get(&flow_id) {
            if let Some(errors) = config_errors(&flow.args, &body.config) {
                return Err(AppError::InvalidArgs {
                    flow: flow_id,
                    instance: body.instance,
                    errors,
                }
                .into());
            }
        } else {
            return Err(AppError::FlowNotFound(app_id, flow_id).into());
        }
    } else {
        return Err(AppError::AppNotFound(app_id).into());
    }
    let request = TremorRequest::Apps(AppsCmd::Deploy {
        app: app_id.clone(),
        flow: flow_id.clone(),
        instance: body.instance.clone(),
        config: body.config.clone(),
        state: body.state(),
    });
    state
        .raft
        .client_write(request)
        .await
        .to_api_result(&uri, &state)
        .await?;
    Ok(Json(instance_id))
}

async fn manage_instance(
    extract::State(state): extract::State<APIRequest>,
    extract::OriginalUri(uri): extract::OriginalUri,
    extract::Path((app_id, flow_id)): extract::Path<(AppId, String)>,
    Json(body): Json<TremorInstanceState>,
) -> APIResult<Json<AppFlowInstanceId>> {
    // FIXME: this is not only for this but all the API functions as we're running in a potentially
    // problematic situation here.
    //
    // The raft algorithm expects all commands and statemachine changes to be excecutable and not fail.
    // That means we need to do all the checks before we send the command to the raft core, however
    // the checks here are executed potentially first on a follower, then on the leader and then
    // send to raft.
    // This has two problems:
    // 1) if the follow didn't catch up with the leader yet we might get a false negative here in the
    //    way that the follow claims a command would fail but the leader would accept it.
    //
    //    Example: client sends install to leader, leader accepts it, client sends start to follower,
    //             the install hasn't been replicated to the follower yet, follower rejects the start.
    //
    // 2) the leader might change it's state between the command being tested and the command being
    //    forwarded to the raft algorithm and serialized.
    //
    //    Example: leader gets two uninstall commands in quick succession, it checks the first, sends
    //             it to raft, the second one arrives and is checked before reft propagated the first
    //             request so it is accepted as well but fails.
    //
    // Solution? We might need to put a single process inbetween the API and the raft algorithm that
    // serializes all commands to ensure no command is executed before the previous one has been fully
    // handled
    let instance_id = AppFlowInstanceId::new(app_id.clone(), flow_id);

    let app = timeout(
        API_WORKER_TIMEOUT,
        state.raft_manager.get_app_local(app_id.clone()),
    )
    .await??;
    if let Some(app) = app {
        if !app.instances.contains_key(instance_id.instance_id()) {
            return Err(AppError::InstanceNotFound(instance_id).into());
        }
    } else {
        return Err(AppError::AppNotFound(app_id).into());
    }
    let body_state = match body {
        TremorInstanceState::Pause => IntendedState::Paused,
        TremorInstanceState::Resume => IntendedState::Running,
    };

    let request = TremorRequest::Apps(AppsCmd::InstanceStateChange {
        instance: instance_id.clone(),
        state: body_state,
    });
    state
        .raft
        .client_write(request)
        .await
        .to_api_result(&uri, &state)
        .await?;
    Ok(Json(instance_id))
}

async fn stop_instance(
    extract::State(state): extract::State<APIRequest>,
    extract::OriginalUri(uri): extract::OriginalUri,
    extract::Path((app_id, flow_id)): extract::Path<(AppId, String)>,
) -> APIResult<Json<AppFlowInstanceId>> {
    let instance_id = AppFlowInstanceId::new(app_id.clone(), flow_id);

    if let Some(app) = timeout(
        API_WORKER_TIMEOUT,
        state.raft_manager.get_app_local(app_id.clone()),
    )
    .await??
    {
        if !app.instances.contains_key(instance_id.instance_id()) {
            return Err(AppError::InstanceNotFound(instance_id).into());
        }
    } else {
        return Err(AppError::AppNotFound(app_id).into());
    }
    let request = TremorRequest::Apps(AppsCmd::Undeploy(instance_id.clone()));
    state
        .raft
        .client_write(request)
        .await
        .to_api_result(&uri, &state)
        .await?;
    Ok(Json(instance_id))
}

/// check the given `args` for errors according to the specified `config` from the flow definition
fn config_errors(
    args: &HashMap<String, Option<simd_json::OwnedValue>>,
    config: &HashMap<String, simd_json::OwnedValue>,
) -> Option<Vec<ArgsError>> {
    let mut errors = Vec::new();
    for present_key in config.keys() {
        if !args.contains_key(present_key) {
            errors.push(ArgsError::Invalid(present_key.to_string()));
        }
    }
    for (key, val) in args {
        if val.is_none() && !config.contains_key(key) {
            errors.push(ArgsError::Missing(key.to_string()));
        }
    }
    if errors.is_empty() {
        None
    } else {
        Some(errors)
    }
}
