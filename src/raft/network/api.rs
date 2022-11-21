use super::{wrapp, APIRequest, APIResult};
use crate::{
    instance::IntendedState,
    raft::{
        archive::{get_app, TremorAppDef},
        store::{
            AppId, FlowId, FlowInstance, InstanceId, Instances, StateApp, TremorInstanceState,
            TremorRequest, TremorResponse, TremorSet, TremorStart,
        },
        Server,
    },
};
use openraft::{error::ClientWriteError, AnyError};
use std::{collections::HashMap, fmt::Display};

pub fn install_rest_endpoints(app: &mut Server) {
    let mut api_endpoint = app.at("/api");
    api_endpoint.at("/apps").post(install).get(wrapp(list));
    api_endpoint.at("/apps/:app").delete(wrapp(uninstall_app));
    api_endpoint.at("/apps/:app/flows/:flow").post(wrapp(start));
    api_endpoint
        .at("/apps/:app/instances/:instance")
        .post(wrapp(manage_instance))
        .delete(wrapp(stop_instance));
    // test k/v store
    api_endpoint.at("/write").post(wrapp(write));
    api_endpoint.at("/read").post(wrapp(read));
    api_endpoint
        .at("/consistent_read")
        .post(wrapp(consistent_read));
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum InstallError {
    AlreadyInstalled(AppId),
    ClientWriteError(ClientWriteError),
}

async fn install(mut req: APIRequest) -> APIResult<TremorResponse> {
    let file: Vec<u8> = req.body_json().await?;
    let app = get_app(&file).map_err(|e| AnyError::new(&e))?;
    {
        let sm = req.state().store.state_machine.read().await;
        if sm.apps.get(&app.name).is_some() {
            return Err(InstallError::AlreadyInstalled(app.name).install());
        }
    }
    let request = TremorRequest::InstallApp {
        app,
        file: file.clone(),
    };
    Ok(req.state().raft.client_write(request.clone()).await?)
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
        for (name, FlowInstance { id, config, state }) in &self.instances {
            writeln!(f, "    - {name}")?;
            writeln!(f, "      Flow: {id}")?;
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

async fn list(req: APIRequest) -> APIResult<TremorResponse> {
    let state_machine = req.state().store.state_machine.read().await;
    let apps: HashMap<_, _> = state_machine
        .apps
        .iter()
        .map(|(k, v)| (k, AppState::from(v)))
        .collect();
    Ok(apps)
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum StateChangeErr {
    AppNotFound(AppId),
    FlowNotFound(AppId, FlowId),
    InstanceExists(AppId, InstanceId),
    InstanceNotFound(AppId, InstanceId),
    ConfigErrors(AppId, FlowId, InstanceId, Vec<ConfigError>),
    ClientWriteError(ClientWriteError),
}
async fn start(mut req: APIRequest) -> APIResult<TremorResponse> {
    let body: TremorStart = req.body_json().await?;

    let app_name = AppId(req.param("app")?.to_string());
    let flow_name = FlowId(req.param("flow")?.to_string());

    {
        let sm = req.state().store.state_machine.read().await;
        if let Some(app) = sm.apps.get(&app_name) {
            if app.instances.contains_key(&body.instance) {
                return Err(StateChangeErr::InstanceExists(app_name, body.instance).into());
            }
            if let Some(flow) = app.app.flows.get(&flow_name) {
                if let Some(errors) = config_errors(&flow.args, &body.config) {
                    return Err(StateChangeErr::ConfigErrors(
                        app_name,
                        flow_name,
                        body.instance,
                        errors,
                    ))
                    .into();
                }
            } else {
                return Err(StateChangeErr::FlowNotFound(app_name, flow_name).into());
            }
        } else {
            return Err(StateChangeErr::AppNotFound(app_name).into());
        }
    }
    let request = TremorRequest::Deploy {
        app: app_name.clone(),
        flow: flow_name.clone(),
        instance: body.instance.clone(),
        config: body.config.clone(),
        // FIXME: make this a parameter
        state: body.state(),
    };
    Ok(req.state().raft.client_write(request).await?)
}

async fn manage_instance(mut req: APIRequest) -> APIResult<TremorResponse> {
    let body: TremorInstanceState = req.body_json().await?;

    let app_id = AppId(req.param("app")?.to_string());
    let instance_id = InstanceId(req.param("instance")?.to_string());
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
    {
        let sm = req.state().store.state_machine.read().await;
        if let Some(app) = sm.apps.get(&app_id) {
            if !app.instances.contains_key(&instance_id) {
                return Err(StateChangeErr::InstanceNotFound(app_id, instance_id).into());
            }
        } else {
            return Err(StateChangeErr::AppNotFound(app_id).into());
        }
    }
    let request = match body {
        TremorInstanceState::Pause => TremorRequest::InstanceStateChange {
            app: app_id.clone(),
            instance: instance_id.clone(),
            state: IntendedState::Paused,
        },
        TremorInstanceState::Resume => TremorRequest::InstanceStateChange {
            app: app_id.clone(),
            instance: instance_id.clone(),
            state: IntendedState::Running,
        },
    };
    Ok(req.state().raft.client_write(request).await?)
}

async fn stop_instance(req: APIRequest) -> APIResult<TremorResponse> {
    let app_id = AppId(req.param("app")?.to_string());
    let instance_id = InstanceId(req.param("instance")?.to_string());
    {
        let sm = req.state().store.state_machine.read().await;
        if let Some(app) = sm.apps.get(&app_id) {
            if !app.instances.contains_key(&instance_id) {
                return Err(StateChangeErr::InstanceNotFound(app_id, instance_id).into());
            }
        } else {
            return Err(StateChangeErr::AppNotFound(app_id).into());
        }
    }
    let request = TremorRequest::Undeploy {
        app: app_id.clone(),
        instance: instance_id.clone(),
    };
    Ok(req.state().raft.client_write(request).await?)
}

async fn uninstall_app(req: APIRequest) -> APIResult<TremorResponse> {
    let app_id = AppId(req.param("app")?.to_string());
    {
        let sm = req.state().store.state_machine.read().await;
        if let Some(app) = sm.apps.get(&app_id) {
            if let Some(instance) = app.instances.keys().next() {
                return Err(StateChangeErr::InstanceExists(app_id, instance.clone()).into());
            }
        } else {
            return Err(StateChangeErr::AppNotFound(app_id).into());
        }
    }
    let request = TremorRequest::UninstallApp {
        app: app_id.clone(),
        force: false,
    };
    Ok(req.state().raft.client_write(request).await?)
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ConfigError {
    Missing(String),
    Invalid(String),
}
fn config_errors(
    args: &HashMap<String, Option<simd_json::OwnedValue>>,
    config: &HashMap<String, simd_json::OwnedValue>,
) -> Option<Vec<ConfigError>> {
    let mut errors = Vec::new();
    for present_key in config.keys() {
        if !args.contains_key(present_key) {
            errors.push(ConfigError::Invalid(present_key.to_string()));
        }
    }
    for (key, val) in args {
        if val.is_none() && !config.contains_key(key) {
            errors.push(ConfigError::Missing(key.to_string()));
        }
    }
    if errors.is_empty() {
        None
    } else {
        Some(errors)
    }
}

/**
 * Application API
 *
 * This is where you place your application, you can use the example below to create your
 * API. The current implementation:
 *
 *  - `POST - /write` saves a value in a key and sync the nodes.
 *  - `POST - /read` attempt to find a value from a given key.
 */
async fn write(mut req: APIRequest) -> APIResult<TremorResponse> {
    let body: TremorSet = req.body_json().await?;
    let request = body.clone().into();
    Ok(req.state().raft.client_write(request).await)?
}

async fn read(mut req: APIRequest) -> APIResult<TremorResponse> {
    let key: String = req.body_json().await?;
    let state_machine = req.state().store.state_machine.read().await;
    Ok(state_machine.get(&key)?)
}

async fn consistent_read(mut req: APIRequest) -> APIResult<TremorResponse> {
    let key: String = req.body_json().await?;
    let result = req.state().raft.is_leader().await; // this sends around appendentries requests to all current nodes
    Ok(req.state().raft.client_read().await?)
}
