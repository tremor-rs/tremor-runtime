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

//! This module contains the HTTP API exposed externally.
//! for inter-node communication look into the `network` module

mod apps;
pub mod client;
mod cluster;
mod kv;
pub(crate) mod worker;

use crate::channel::{bounded, Sender};
use crate::{
    ids::{AppId, FlowDefinitionId, FlowInstanceId},
    raft::{
        node::Addr,
        store::{self, StateApp, Store, TremorResponse},
        TremorRaftImpl,
    },
};
use futures::Future;
use openraft::{
    error::{
        AddLearnerError, ChangeMembershipError, ClientReadError, ClientWriteError, Fatal,
        ForwardToLeader, QuorumNotEnough, RemoveLearnerError,
    },
    raft::{AddLearnerResponse, ClientWriteResponse},
    LogId, NodeId, StorageError,
};
use std::collections::HashMap;
use std::{collections::BTreeSet, num::ParseIntError, sync::Arc, time::Duration};
use tide::{http::headers::LOCATION, Body, Endpoint, Request, Response, StatusCode};
use tokio::{task::JoinHandle, time::timeout};

use self::apps::AppState;

type Server = tide::Server<Arc<ServerState>>;
type APIRequest = Request<Arc<ServerState>>;
pub type APIResult<T> = Result<T, APIError>;

const API_WORKER_TIMEOUT: Duration = Duration::from_secs(5);

enum APIStoreReq {
    GetApp(AppId, Sender<Option<StateApp>>),
    GetApps(Sender<HashMap<AppId, AppState>>),
    KVGet(String, Sender<Option<String>>),
    GetNode(NodeId, Sender<Option<Addr>>),
    GetNodes(Sender<HashMap<NodeId, Addr>>),
    GetNodeId(Addr, Sender<Option<NodeId>>),
    GetLastMembership(Sender<BTreeSet<NodeId>>),
}

pub(crate) struct ServerState {
    id: NodeId,
    addr: Addr,
    raft: TremorRaftImpl,
    store_tx: Sender<APIStoreReq>,
}

impl ServerState {
    pub(crate) fn id(&self) -> NodeId {
        self.id
    }
    pub(crate) fn addr(&self) -> &Addr {
        &self.addr
    }
}

pub(crate) fn initialize(
    id: NodeId,
    addr: Addr,
    raft: TremorRaftImpl,
    store: Arc<Store>,
) -> (JoinHandle<()>, Arc<ServerState>) {
    let (store_tx, store_rx) = bounded(64); // arbitrary choice to block upon too much concurrent incoming requests
    let handle = tokio::task::spawn(worker::api_worker(store, store_rx));
    let state = Arc::new(ServerState {
        id,
        addr,
        raft,
        store_tx,
    });
    (handle, state)
}

pub(crate) fn install_rest_endpoints(app: &mut Server) {
    let mut v1 = app.at("/v1");
    cluster::install_rest_endpoints(&mut v1);

    let mut api_endpoint = v1.at("/api");
    apps::install_rest_endpoints(&mut api_endpoint);
    kv::install_rest_endpoints(&mut api_endpoint);
}

/// Endpoint implementation that turns our errors into responses
/// and makes successful results automatically a 200 Ok
struct WrappingEndpoint<T, F, Fut>
where
    T: serde::Serialize + serde::Deserialize<'static>,
    F: Fn(APIRequest) -> Fut,
    Fut: Future<Output = APIResult<T>> + Send + 'static,
{
    f: F,
}

#[async_trait::async_trait]
impl<T, F, Fut> Endpoint<Arc<ServerState>> for WrappingEndpoint<T, F, Fut>
where
    T: serde::Serialize + serde::Deserialize<'static> + 'static,
    F: Send + Sync + 'static + Fn(APIRequest) -> Fut,
    Fut: Future<Output = APIResult<T>> + Send + 'static,
{
    async fn call(&self, req: APIRequest) -> tide::Result {
        Ok(match (self.f)(req).await {
            Ok(serializable) => Response::builder(StatusCode::Ok)
                .body(Body::from_json(&serializable)?)
                .build(),
            Err(e) => e.into(),
        })
    }
}

fn wrapp<T, F, Fut>(f: F) -> WrappingEndpoint<T, F, Fut>
where
    T: serde::Serialize + serde::Deserialize<'static> + 'static,
    F: Send + Sync + 'static + Fn(APIRequest) -> Fut,
    Fut: Future<Output = APIResult<T>> + Send + 'static,
{
    WrappingEndpoint { f }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ArgsError {
    Missing(String),
    Invalid(String),
}

impl std::fmt::Display for ArgsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing(m) => write!(f, "Missing required argument \"{m}\""),
            Self::Invalid(inv) => write!(f, "Unknown argument \"{inv}\""),
        }
    }
}

#[derive(Debug, Serialize)]
pub enum AppError {
    /// app is already installed
    AlreadyInstalled(AppId),

    /// app not found
    AppNotFound(AppId),
    FlowNotFound(AppId, FlowDefinitionId),
    InstanceNotFound(FlowInstanceId),
    /// App has at least 1 running instances (and thus cannot be uninstalled)
    HasInstances(AppId, Vec<FlowInstanceId>),
    InstanceAlreadyExists(FlowInstanceId),
    /// provided flow args are invalid
    InvalidArgs {
        flow: FlowDefinitionId,
        instance: FlowInstanceId,
        errors: Vec<ArgsError>,
    },
}

impl std::error::Error for AppError {}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyInstalled(app_id) => write!(f, "App \"{app_id}\" is already installed."),
            Self::AppNotFound(app_id) => write!(f, "App \"{app_id}\" not found."),
            Self::HasInstances(app_id, instances) => write!(
                f,
                "App \"{app_id}\" has running instances: {}",
                instances
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::InstanceAlreadyExists(instance_id) => write!(
                f,
                "App \"{}\" already has an instance \"{}\"",
                instance_id.app_id(),
                instance_id.alias()
            ),
            Self::InstanceNotFound(instance_id) => write!(
                f,
                "No instance \"{}\" in App \"{}\" found",
                instance_id.alias(),
                instance_id.app_id()
            ),
            Self::FlowNotFound(app_id, flow_id) => {
                write!(f, "Flow \"{flow_id}\" not found in App \"{app_id}\".")
            }
            Self::InvalidArgs {
                flow,
                instance,
                errors,
            } => write!(
                f,
                "Invalid Arguments provided for instance \"{instance}\" of Flow \"{flow}\": {}",
                errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

#[derive(Debug, Serialize)]
pub enum APIError {
    /// We need to send this API request to the leader_url
    ForwardToLeader {
        node_id: NodeId,
        // full URL, not only addr
        leader_url: String,
    },
    /// HTTP related error
    HTTP {
        status: StatusCode,
        message: String,
    },
    /// raft fatal error, includes StorageError
    Fatal(Fatal),
    /// We don't have a quorum to read
    NoQuorum(QuorumNotEnough),
    /// Error when changing a membership
    ChangeMembership(ChangeMembershipError),
    /// Error from our store/statemachine
    Store(String),
    /// openraft storage error
    Storage(StorageError),
    /// Errors around tremor apps
    App(AppError),
    /// Error in the runtime
    Runtime(String),
    // Timeout
    Timeout,
    // recv error
    Recv,
    /// fallback error type
    Other(String),
}

#[async_trait::async_trait]
trait ToAPIResult<T>
where
    T: serde::Serialize + serde::Deserialize<'static>,
{
    async fn to_api_result(self, req: &APIRequest) -> APIResult<T>;
}

#[async_trait::async_trait()]
impl<T: serde::Serialize + serde::Deserialize<'static> + Send> ToAPIResult<T>
    for Result<T, ClientReadError>
{
    async fn to_api_result(self, req: &APIRequest) -> APIResult<T> {
        match self {
            Ok(t) => Ok(t),
            Err(ClientReadError::ForwardToLeader(e)) => forward_to_leader(e, req).await,
            Err(ClientReadError::Fatal(e)) => Err(APIError::Fatal(e)),
            Err(ClientReadError::QuorumNotEnough(e)) => Err(APIError::NoQuorum(e)),
        }
    }
}

#[async_trait::async_trait]
impl ToAPIResult<Option<LogId>> for Result<AddLearnerResponse, AddLearnerError> {
    async fn to_api_result(self, req: &APIRequest) -> APIResult<Option<LogId>> {
        match self {
            Ok(response) => Ok(response.matched),
            Err(AddLearnerError::ForwardToLeader(e)) => forward_to_leader(e, req).await,
            Err(AddLearnerError::Fatal(e)) => Err(APIError::Fatal(e)),
            Err(AddLearnerError::Exists(_node_id)) => Ok(None), // we want the API call to be idempotent and not error if the node is already a learner
        }
    }
}

#[async_trait::async_trait]
impl ToAPIResult<()> for Result<(), RemoveLearnerError> {
    async fn to_api_result(self, req: &APIRequest) -> APIResult<()> {
        match self {
            Ok(()) | Err(RemoveLearnerError::NotExists(_)) => Ok(()), // if the node is not part of the cluster, the effect is the same, so we say it is fine
            Err(RemoveLearnerError::ForwardToLeader(e)) => forward_to_leader(e, req).await,
            Err(RemoveLearnerError::Fatal(e)) => Err(APIError::Fatal(e)),
            Err(e @ RemoveLearnerError::NotLearner(_)) => Err(APIError::HTTP {
                status: StatusCode::Conflict,
                message: e.to_string(),
            }),
        }
    }
}

#[async_trait::async_trait()]
impl ToAPIResult<TremorResponse> for Result<ClientWriteResponse<TremorResponse>, ClientWriteError> {
    // we need the request context here to construct the redirect url properly
    async fn to_api_result(self, req: &APIRequest) -> APIResult<TremorResponse> {
        match self {
            Ok(response) => Ok(response.data),
            Err(ClientWriteError::ForwardToLeader(e)) => forward_to_leader(e, req).await,
            Err(ClientWriteError::Fatal(e)) => Err(APIError::Fatal(e)),
            Err(ClientWriteError::ChangeMembershipError(e)) => Err(APIError::ChangeMembership(e)),
        }
    }
}

async fn forward_to_leader<T>(e: ForwardToLeader, req: &APIRequest) -> APIResult<T>
where
    T: serde::Serialize + serde::Deserialize<'static>,
{
    Err(if let Some(leader_id) = e.leader_id {
        let (tx, mut rx) = bounded(1);
        req.state()
            .store_tx
            .send(APIStoreReq::GetNode(leader_id, tx))
            .await?;
        // we can only forward to the leader if we have the node in our state machine
        if let Some(leader_addr) = timeout(API_WORKER_TIMEOUT, rx.recv()).await?.flatten() {
            let mut leader_url =
                url::Url::parse(&format!("{}://{}", req.url().scheme(), leader_addr.api()))?;
            leader_url.set_path(req.url().path());
            leader_url.set_query(req.url().query());
            debug!("Forwarding to leader: {leader_url}");
            // we don't care about fragment

            APIError::ForwardToLeader {
                node_id: leader_id,
                leader_url: leader_url.to_string(),
            }
        } else {
            APIError::Other(format!("Leader {leader_id} not known"))
        }
    } else {
        APIError::Other("No leader available".to_string())
    })
}

impl From<APIError> for Response {
    fn from(e: APIError) -> Self {
        let status = match &e {
            APIError::ForwardToLeader { .. } => StatusCode::TemporaryRedirect,
            APIError::ChangeMembership(ChangeMembershipError::LearnerNotFound(_)) => {
                StatusCode::NotFound
            }
            APIError::ChangeMembership(ChangeMembershipError::EmptyMembership(_)) => {
                StatusCode::BadRequest
            }
            APIError::Other(_)
            | APIError::ChangeMembership(_)
            | APIError::Store(_)
            | APIError::Storage(_)
            | APIError::Fatal(_)
            | APIError::NoQuorum(_)
            | APIError::Runtime(_)
            | APIError::Recv
            | APIError::App(_) => StatusCode::InternalServerError,
            APIError::Timeout => StatusCode::GatewayTimeout,
            APIError::HTTP { status, .. } => *status,
        };
        let mut builder = Response::builder(status);
        if let APIError::ForwardToLeader { leader_url, .. } = &e {
            builder = builder.header(LOCATION, leader_url);
            builder.build()
        } else {
            builder
                .body(Body::from_json(&e).expect("Serialization should work"))
                .build()
        }
    }
}

impl From<&TremorResponse> for Response {
    fn from(e: &TremorResponse) -> Self {
        Response::builder(StatusCode::Ok)
            .body(Body::from_json(&e).expect("Serialization should work"))
            .build()
    }
}

impl std::fmt::Display for APIError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ForwardToLeader {
                leader_url: leader_addr,
                node_id,
            } => f
                .debug_struct("ForwardToLeader")
                .field("leader_addr", leader_addr)
                .field("node_id", node_id)
                .finish(),
            Self::Other(s) | Self::Store(s) => write!(f, "{s}"),
            Self::HTTP { message, status } => write!(f, "HTTP {status} {message}"),
            Self::Fatal(e) => write!(f, "Fatal Error: {e}"),
            Self::ChangeMembership(e) => write!(f, "Error changing cluster membership: {e}"),
            Self::NoQuorum(e) => write!(f, "Quorum: {e}"),
            Self::Storage(e) => write!(f, "Storage: {e}"),
            Self::Runtime(e) => write!(f, "Runtime: {e}"),
            Self::App(e) => write!(f, "App: {e}"),
            Self::Timeout => write!(f, "Timeout"),
            Self::Recv => write!(f, "Recv"),
        }
    }
}
impl std::error::Error for APIError {}

impl From<store::Error> for APIError {
    fn from(e: store::Error) -> Self {
        Self::Store(e.to_string())
    }
}

impl From<url::ParseError> for APIError {
    fn from(e: url::ParseError) -> Self {
        Self::Other(e.to_string())
    }
}

impl From<tide::Error> for APIError {
    fn from(e: tide::Error) -> Self {
        Self::HTTP {
            status: e.status(),
            message: e.to_string(),
        }
    }
}

impl From<StorageError> for APIError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e)
    }
}

impl From<ParseIntError> for APIError {
    fn from(e: ParseIntError) -> Self {
        Self::Other(e.to_string())
    }
}

impl From<AppError> for APIError {
    fn from(app: AppError) -> Self {
        APIError::App(app)
    }
}

impl From<crate::Error> for APIError {
    fn from(e: crate::Error) -> Self {
        APIError::Runtime(e.to_string())
    }
}

impl<T> From<crate::channel::SendError<T>> for APIError {
    fn from(e: crate::channel::SendError<T>) -> Self {
        Self::Other(e.to_string())
    }
}

impl From<tokio::time::error::Elapsed> for APIError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout
    }
}
