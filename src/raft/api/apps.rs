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
use super::APIError;
use crate::{
    channel::bounded,
    ids::{AppId, FlowDefinitionId, FlowInstanceId},
    instance::IntendedState,
    raft::{
        api::{
            wrapp, APIRequest, APIResult, APIStoreReq, AppError, ArgsError, ServerState,
            ToAPIResult, API_WORKER_TIMEOUT,
        },
        archive::{get_app, TremorAppDef},
        store::{
            AppsRequest as AppsCmd, FlowInstance, Instances, StateApp, TremorInstanceState,
            TremorRequest, TremorResponse, TremorStart,
        },
    },
};
use std::collections::HashMap;
use std::{fmt::Display, sync::Arc};
use tide::Route;
use tokio::time::timeout;

pub(crate) fn install_rest_endpoints(parent: &mut Route<Arc<ServerState>>) {
    let mut apps_endpoint = parent.at("/apps");
    apps_endpoint.post(wrapp(install_app)).get(wrapp(list));
    apps_endpoint.at("/:app").delete(wrapp(uninstall_app));
    apps_endpoint.at("/:app/flows/:flow").post(wrapp(start));
    apps_endpoint
        .at("/:app/instances/:instance")
        .post(wrapp(manage_instance))
        .delete(wrapp(stop_instance));
}

async fn install_app(mut req: APIRequest) -> APIResult<AppId> {
    let file: Vec<u8> = req.body_json().await?;
    let app = get_app(&file)?;
    let app_id = app.name().clone();

    let (tx, mut rx) = bounded(1);
    req.state()
        .store_tx
        .send(APIStoreReq::GetApp(app_id.clone(), tx))
        .await?;
    if timeout(API_WORKER_TIMEOUT, rx.recv())
        .await?
        .flatten()
        .is_some()
    {
        return Err(AppError::AlreadyInstalled(app.name).into());
    }
    let request = TremorRequest::Apps(AppsCmd::InstallApp {
        app,
        file: file.clone(),
    });
    req.state()
        .raft
        .client_write(request)
        .await
        .to_api_result(&req)
        .await?;
    Ok(app_id)
}

async fn uninstall_app(req: APIRequest) -> APIResult<TremorResponse> {
    let app_id = AppId(req.param("app")?.to_string());
    let (tx, mut rx) = bounded(1);
    req.state()
        .store_tx
        .send(APIStoreReq::GetApp(app_id.clone(), tx))
        .await?;
    let app = timeout(API_WORKER_TIMEOUT, rx.recv()).await?.flatten();
    if let Some(app) = app {
        if !app.instances.is_empty() {
            return Err(AppError::HasInstances(
                app_id.clone(),
                app.instances
                    .keys()
                    .map(|alias| FlowInstanceId::new(app_id.clone(), alias))
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
    req.state()
        .raft
        .client_write(request)
        .await
        .to_api_result(&req)
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

async fn list(req: APIRequest) -> APIResult<HashMap<AppId, AppState>> {
    let (tx, mut rx) = bounded(1);
    req.state().store_tx.send(APIStoreReq::GetApps(tx)).await?;
    let apps = timeout(API_WORKER_TIMEOUT, rx.recv())
        .await?
        .ok_or(APIError::Recv)?;
    Ok(apps)
}

async fn start(mut req: APIRequest) -> APIResult<FlowInstanceId> {
    let body: TremorStart = req.body_json().await?;

    let instance_id = body.instance.clone();
    let app_name = AppId(req.param("app")?.to_string());
    let flow_name = FlowDefinitionId(req.param("flow")?.to_string());

    let (tx, mut rx) = bounded(1);
    req.state()
        .store_tx
        .send(APIStoreReq::GetApp(app_name.clone(), tx))
        .await?;
    let app = timeout(API_WORKER_TIMEOUT, rx.recv())
        .await?
        .ok_or(APIError::Recv)?;

    if let Some(app) = app {
        if app.instances.contains_key(instance_id.alias()) {
            return Err(AppError::InstanceAlreadyExists(body.instance).into());
        }
        if let Some(flow) = app.app.flows.get(&flow_name) {
            if let Some(errors) = config_errors(&flow.args, &body.config) {
                return Err(AppError::InvalidArgs {
                    flow: flow_name,
                    instance: body.instance,
                    errors,
                }
                .into());
            }
        } else {
            return Err(AppError::FlowNotFound(app_name, flow_name).into());
        }
    } else {
        return Err(AppError::AppNotFound(app_name).into());
    }
    let request = TremorRequest::Apps(AppsCmd::Deploy {
        app: app_name.clone(),
        flow: flow_name.clone(),
        instance: body.instance.clone(),
        config: body.config.clone(),
        // FIXME: make this a parameter
        state: body.state(),
    });
    req.state()
        .raft
        .client_write(request)
        .await
        .to_api_result(&req)
        .await?;
    Ok(instance_id)
}

async fn manage_instance(mut req: APIRequest) -> APIResult<FlowInstanceId> {
    let body: TremorInstanceState = req.body_json().await?;

    let app_id = AppId(req.param("app")?.to_string());
    let instance_id = FlowInstanceId::new(app_id.clone(), req.param("instance")?.to_string());
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
    let (tx, mut rx) = bounded(1);
    req.state()
        .store_tx
        .send(APIStoreReq::GetApp(app_id.clone(), tx))
        .await?;
    let app = timeout(API_WORKER_TIMEOUT, rx.recv())
        .await?
        .ok_or(APIError::Recv)?;
    if let Some(app) = app {
        if !app.instances.contains_key(instance_id.alias()) {
            return Err(AppError::InstanceNotFound(instance_id).into());
        }
    } else {
        return Err(AppError::AppNotFound(app_id).into());
    }
    let state = match body {
        TremorInstanceState::Pause => IntendedState::Paused,
        TremorInstanceState::Resume => IntendedState::Running,
    };

    let request = TremorRequest::Apps(AppsCmd::InstanceStateChange {
        instance: instance_id.clone(),
        state,
    });
    req.state()
        .raft
        .client_write(request)
        .await
        .to_api_result(&req)
        .await?;
    Ok(instance_id)
}

async fn stop_instance(req: APIRequest) -> APIResult<FlowInstanceId> {
    let app_id = AppId(req.param("app")?.to_string());
    let instance_id = FlowInstanceId::new(app_id.clone(), req.param("instance")?.to_string());
    let (tx, mut rx) = bounded(1);
    req.state()
        .store_tx
        .send(APIStoreReq::GetApp(app_id.clone(), tx))
        .await?;
    if let Some(app) = timeout(API_WORKER_TIMEOUT, rx.recv())
        .await?
        .ok_or(APIError::Recv)?
    {
        if !app.instances.contains_key(instance_id.alias()) {
            return Err(AppError::InstanceNotFound(instance_id).into());
        }
    } else {
        return Err(AppError::AppNotFound(app_id).into());
    }
    let request = TremorRequest::Apps(AppsCmd::Undeploy(instance_id.clone()));
    req.state()
        .raft
        .client_write(request)
        .await
        .to_api_result(&req)
        .await?;
    Ok(instance_id)
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
