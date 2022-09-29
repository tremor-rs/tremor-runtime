use crate::raft::{
    app::TremorApp,
    archive::{get_app, TremorAppDef},
    client::TremorClient,
    store::{
        AppId, FlowId, FlowInstance, InstanceId, Instances, StateApp, TremorRequest, TremorSet,
        TremorStart,
    },
    Server, TremorNode, TremorNodeId,
};
use openraft::{
    error::{CheckIsLeaderError, ClientWriteError, ForwardToLeader},
    AnyError,
};
use std::{collections::HashMap, fmt::Display, sync::Arc};
use tide::{Body, Request, Response, StatusCode};

pub fn install_rest_endpoints(app: &mut Server) {
    let mut api = app.at("/api");
    api.at("/apps").post(install);
    api.at("/apps").get(list);
    api.at("/apps/:app/flows/:flow").post(start);

    // test k/v store
    api.at("/write").post(write);
    api.at("/read").post(read);
    api.at("/consistent_read").post(consistent_read);
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum InstallError {
    AlreadyInstalled(AppId),
    ClientWriteError(ClientWriteError<TremorNodeId, TremorNode>),
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
async fn install(mut req: Request<Arc<TremorApp>>) -> tide::Result {
    let file: Vec<u8> = req.body_json().await?;
    let app = dbg!(get_app(&file)).map_err(|e| AnyError::new(&e))?;
    {
        let sm = req.state().store.state_machine.read().await;
        if sm.apps.get(&app.name).is_some() {
            return Ok(Response::builder(StatusCode::Conflict)
                .body(Body::from_json(&Result::<(), _>::Err(
                    InstallError::AlreadyInstalled(app.name),
                ))?)
                .build());
        }
    }
    let request = TremorRequest::Load {
        app,
        file: file.clone(),
    };
    match dbg!(req.state().raft.client_write(request.clone()).await) {
        Ok(res) => Ok(Response::builder(StatusCode::Ok)
            .body(Body::from_json(&Result::<_, ()>::Ok(res))?)
            .build()),
        Err(e) => match e {
            ClientWriteError::ForwardToLeader(ForwardToLeader {
                leader_node: Some(TremorNode { api_addr, .. }),
                leader_id: Some(leader_id),
            }) => {
                debug!("Forward to leader: {api_addr}");
                let mut client = TremorClient::new(leader_id, api_addr.clone());
                let response = client.install(&file).await?;
                Ok(Response::builder(StatusCode::Ok)
                    .body(Body::from_json(&response)?)
                    .build())
            }
            ClientWriteError::ForwardToLeader(_) | ClientWriteError::ChangeMembershipError(_) => {
                Ok(Response::builder(StatusCode::ServiceUnavailable)
                    .body(Body::from_json(&Result::<(), _>::Err(
                        InstallError::ClientWriteError(e),
                    ))?)
                    .build())
            }
            ClientWriteError::Fatal(_) => Ok(Response::builder(StatusCode::InternalServerError)
                .body(Body::from_json(&Result::<(), _>::Err(
                    InstallError::ClientWriteError(e),
                ))?)
                .build()),
        },
    }
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
        write!(f, "Name: {}\n", self.def.name)?;
        write!(f, "Flows: \n")?;
        for (name, flow) in &self.def.flows {
            write!(f, " - {name}\n")?;
            write!(f, "   Config:")?;
            if flow.args.is_empty() {
                write!(f, " -\n")?;
            } else {
                write!(f, "\n")?;
                for (name, val) in &flow.args {
                    write!(f, "    - {name}: ")?;
                    if let Some(val) = val {
                        write!(f, "{val}\n")?;
                    } else {
                        write!(f, "-\n")?;
                    }
                }
            }
        }
        write!(f, "   Instances: \n")?;
        for (name, FlowInstance { id, config }) in &self.instances {
            write!(f, "    - {name}\n")?;
            write!(f, "      Flow: {id}\n")?;
            write!(f, "      Config:")?;
            if config.is_empty() {
                write!(f, " -\n")?;
            } else {
                write!(f, "\n")?;
                for (name, val) in config {
                    write!(f, "        - {name}: {val}\n")?;
                }
            }
        }
        Ok(())
    }
}

async fn list(req: Request<Arc<TremorApp>>) -> tide::Result {
    let state_machine = req.state().store.state_machine.read().await;
    let apps: HashMap<_, _> = state_machine
        .apps
        .iter()
        .map(|(k, v)| (k, AppState::from(v)))
        .collect();
    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&apps)?)
        .build())
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum StartError {
    AppNotFound(AppId),
    FlowNotFound(AppId, FlowId),
    InstanceExists(AppId, InstanceId),
    ConfigErrors(AppId, FlowId, InstanceId, Vec<ConfigError>),
    ClientWriteError(ClientWriteError<TremorNodeId, TremorNode>),
}
async fn start(mut req: Request<Arc<TremorApp>>) -> tide::Result {
    let body: TremorStart = req.body_json().await?;

    let app_name = AppId(req.param("app")?.to_string());
    let flow_name = FlowId(req.param("flow")?.to_string());

    {
        let sm = req.state().store.state_machine.read().await;
        if let Some(app) = sm.apps.get(&app_name) {
            if app.instances.contains_key(&body.instance) {
                return Ok(Response::builder(StatusCode::Conflict)
                    .body(Body::from_json(&Result::<(), _>::Err(
                        StartError::InstanceExists(app_name, body.instance),
                    ))?)
                    .build());
            }
            if let Some(flow) = app.app.flows.get(&flow_name) {
                if let Some(errors) = config_errors(&flow.args, &body.config) {
                    return Ok(Response::builder(StatusCode::BadRequest)
                        .body(Body::from_json(&Result::<(), _>::Err(
                            StartError::ConfigErrors(app_name, flow_name, body.instance, errors),
                        ))?)
                        .build());
                }
            } else {
                return Ok(Response::builder(StatusCode::NotFound)
                    .body(Body::from_json(&Result::<(), _>::Err(
                        StartError::FlowNotFound(app_name, flow_name),
                    ))?)
                    .build());
            }
        } else {
            return Ok(Response::builder(StatusCode::NotFound)
                .body(Body::from_json(&Result::<(), _>::Err(
                    StartError::AppNotFound(app_name),
                ))?)
                .build());
        }
    }
    let request = TremorRequest::Start {
        app: app_name.clone(),
        flow: flow_name.clone(),
        instance: body.instance.clone(),
        config: body.config.clone(),
    };
    match req.state().raft.client_write(request).await {
        Ok(res) => Ok(Response::builder(StatusCode::Ok)
            .body(Body::from_json(&Result::<_, ()>::Ok(res))?)
            .build()),
        Err(e) => match e {
            ClientWriteError::ForwardToLeader(ForwardToLeader {
                leader_node: Some(TremorNode { api_addr, .. }),
                leader_id: Some(leader_id),
            }) => {
                debug!("Forward to leader: {api_addr}");
                let mut client = TremorClient::new(leader_id, api_addr.clone());
                let response = client
                    .start(&app_name, &flow_name, &body.instance, body.config)
                    .await?;
                Ok(Response::builder(StatusCode::Ok)
                    .body(Body::from_json(&response)?)
                    .build())
            }
            ClientWriteError::ForwardToLeader(_) | ClientWriteError::ChangeMembershipError(_) => {
                Ok(Response::builder(StatusCode::ServiceUnavailable)
                    .body(Body::from_json(&Result::<(), _>::Err(
                        StartError::ClientWriteError(e),
                    ))?)
                    .build())
            }
            ClientWriteError::Fatal(_) => Ok(Response::builder(StatusCode::InternalServerError)
                .body(Body::from_json(&Result::<(), _>::Err(
                    StartError::ClientWriteError(e),
                ))?)
                .build()),
        },
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
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
async fn write(mut req: Request<Arc<TremorApp>>) -> tide::Result {
    let body: TremorSet = req.body_json().await?;
    let request = body.clone().into();
    match req.state().raft.client_write(request).await {
        Ok(res) => Ok(Response::builder(StatusCode::Ok)
            .body(Body::from_json(&res)?)
            .build()),
        Err(e) => {
            debug!("Write Error: {e}");
            match e {
                ClientWriteError::ForwardToLeader(ForwardToLeader {
                    leader_node: Some(TremorNode { api_addr, .. }),
                    leader_id: Some(leader_id),
                }) => {
                    debug!("Forward to leader: {api_addr}");
                    let mut client = TremorClient::new(leader_id, api_addr.clone());
                    let response = client.write(&body).await?;
                    Ok(Response::builder(StatusCode::Ok)
                        .body(Body::from_json(&response)?)
                        .build())
                }
                ClientWriteError::ForwardToLeader(e) => {
                    Ok(Response::builder(StatusCode::ServiceUnavailable)
                        .body(Body::from_json(&e)?)
                        .build())
                }
                ClientWriteError::ChangeMembershipError(e) => {
                    Ok(Response::builder(StatusCode::ServiceUnavailable)
                        .body(Body::from_json(&e)?)
                        .build())
                }
                ClientWriteError::Fatal(e) => {
                    Ok(Response::builder(StatusCode::InternalServerError)
                        .body(Body::from_json(&e)?)
                        .build())
                }
            }
        }
    }
}

async fn read(mut req: Request<Arc<TremorApp>>) -> tide::Result {
    let key: String = req.body_json().await?;
    let state_machine = req.state().store.state_machine.read().await;
    let value = state_machine.get(&key)?;

    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&value)?)
        .build())
}

async fn consistent_read(mut req: Request<Arc<TremorApp>>) -> tide::Result {
    let key: String = req.body_json().await?;
    let ret = req.state().raft.is_leader().await; // this sends around appendentries requests to all current nodes
    match ret {
        Ok(_) => {
            let state_machine = req.state().store.state_machine.read().await;
            let value = state_machine.get(&key)?;

            Ok(Response::builder(StatusCode::Ok)
                .body(Body::from_json(&value)?)
                .build())
        }
        Err(e) => {
            debug!("Read Error {e}");
            match e {
                CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                    leader_node: Some(TremorNode { api_addr, .. }),
                    leader_id: Some(leader_id),
                }) => {
                    debug!("Forward to leader: {api_addr}");
                    let mut client = TremorClient::new(leader_id, api_addr.clone());
                    let value = client.consistent_read(&key).await?;
                    Ok(Response::builder(StatusCode::Ok)
                        .body(Body::from_json(&value)?)
                        .build())
                }
                CheckIsLeaderError::ForwardToLeader(e) => {
                    Ok(Response::builder(StatusCode::ServiceUnavailable)
                        .body(Body::from_json(&e)?)
                        .build())
                }
                CheckIsLeaderError::QuorumNotEnough(e) => {
                    Ok(Response::builder(StatusCode::ServiceUnavailable)
                        .body(Body::from_json(&e)?)
                        .build())
                }
                CheckIsLeaderError::Fatal(e) => {
                    Ok(Response::builder(StatusCode::InternalServerError)
                        .body(Body::from_json(&e)?)
                        .build())
                }
            }
        }
    }
}
