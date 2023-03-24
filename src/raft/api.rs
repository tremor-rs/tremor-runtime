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

use self::apps::AppState;
use crate::{
    channel::{oneshot, OneShotSender, Receiver, Sender},
    ids::{AppFlowInstanceId, AppId, FlowDefinitionId},
    raft::{
        node::Addr,
        store::{self, StateApp, Store, TremorResponse},
        TremorRaftImpl,
    },
};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json, Router,
};
use http::{header::LOCATION, HeaderMap, Uri};
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
use tokio::{task::JoinHandle, time::timeout};

pub(crate) type APIRequest = Arc<ServerState>;
pub type APIResult<T> = Result<T, APIError>;

const API_WORKER_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) type ReplySender<T> = OneShotSender<T>;

#[derive(Debug)]
pub enum APIStoreReq {
    GetApp(AppId, ReplySender<Option<StateApp>>),
    GetApps(ReplySender<HashMap<AppId, AppState>>),
    KVGet(String, ReplySender<Option<String>>),
    GetNode(NodeId, ReplySender<Option<Addr>>),
    GetNodes(ReplySender<HashMap<NodeId, Addr>>),
    GetNodeId(Addr, ReplySender<Option<NodeId>>),
    GetLastMembership(ReplySender<BTreeSet<NodeId>>),
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
    store_tx: Sender<APIStoreReq>,
    store_rx: Receiver<APIStoreReq>,
) -> (JoinHandle<()>, Arc<ServerState>) {
    let handle = tokio::task::spawn(worker::api_worker(store, store_rx));
    let state = Arc::new(ServerState {
        id,
        addr,
        raft,
        store_tx,
    });
    (handle, state)
}

pub(crate) fn endpoints() -> Router<APIRequest> {
    Router::new()
        .nest("/v1/cluster", cluster::endpoints())
        .nest("/v1/api/apps", apps::endpoints())
        .nest("/v1/api/kv", kv::endpoints())
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
    InstanceNotFound(AppFlowInstanceId),
    /// App has at least 1 running instances (and thus cannot be uninstalled)
    HasInstances(AppId, Vec<AppFlowInstanceId>),
    InstanceAlreadyExists(AppFlowInstanceId),
    /// provided flow args are invalid
    InvalidArgs {
        flow: FlowDefinitionId,
        instance: AppFlowInstanceId,
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
                instance_id.instance_id()
            ),
            Self::InstanceNotFound(instance_id) => write!(
                f,
                "No instance \"{}\" in App \"{}\" found",
                instance_id.instance_id(),
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

#[allow(clippy::trivially_copy_pass_by_ref)]
fn round_sc<S>(x: &StatusCode, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    s.serialize_u16(x.as_u16())
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
        #[serde(serialize_with = "round_sc")]
        status: StatusCode,
        message: String,
    },
    /// raft fatal error, includes StorageError
    Fatal(Fatal),
    /// We don't have a quorum to read
    NoQuorum(QuorumNotEnough),
    /// We don't have a leader
    NoLeader,
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

impl IntoResponse for APIError {
    fn into_response(self) -> Response {
        let status = match &self {
            APIError::ForwardToLeader { .. } => StatusCode::TEMPORARY_REDIRECT,
            APIError::ChangeMembership(ChangeMembershipError::LearnerNotFound(_)) => {
                StatusCode::NOT_FOUND
            }
            APIError::ChangeMembership(ChangeMembershipError::EmptyMembership(_)) => {
                StatusCode::BAD_REQUEST
            }
            APIError::Other(_)
            | APIError::ChangeMembership(_)
            | APIError::Store(_)
            | APIError::Storage(_)
            | APIError::Fatal(_)
            | APIError::Runtime(_)
            | APIError::Recv
            | APIError::App(_) => StatusCode::INTERNAL_SERVER_ERROR,
            APIError::NoLeader | APIError::NoQuorum(_) => StatusCode::SERVICE_UNAVAILABLE,
            APIError::Timeout => StatusCode::GATEWAY_TIMEOUT,
            APIError::HTTP { status, .. } => *status,
        };

        if let APIError::ForwardToLeader { leader_url, .. } = &self {
            let mut headers = HeaderMap::new();
            if let Ok(v) = leader_url.parse() {
                headers.insert(LOCATION, v);
            }
            (status, headers).into_response()
        } else {
            (status, Json(self)).into_response()
        }
    }
}

#[async_trait::async_trait]
trait ToAPIResult<T>
where
    T: serde::Serialize + serde::Deserialize<'static>,
{
    async fn to_api_result(self, uri: &Uri, req: &APIRequest) -> APIResult<T>;
}

#[async_trait::async_trait()]
impl<T: serde::Serialize + serde::Deserialize<'static> + Send> ToAPIResult<T>
    for Result<T, ClientReadError>
{
    async fn to_api_result(self, uri: &Uri, req: &APIRequest) -> APIResult<T> {
        match self {
            Ok(t) => Ok(t),
            Err(ClientReadError::ForwardToLeader(e)) => forward_to_leader(e, uri, req).await,
            Err(ClientReadError::Fatal(e)) => Err(APIError::Fatal(e)),
            Err(ClientReadError::QuorumNotEnough(e)) => Err(APIError::NoQuorum(e)),
        }
    }
}

#[async_trait::async_trait]
impl ToAPIResult<Option<LogId>> for Result<AddLearnerResponse, AddLearnerError> {
    async fn to_api_result(self, uri: &Uri, req: &APIRequest) -> APIResult<Option<LogId>> {
        match self {
            Ok(response) => Ok(response.matched),
            Err(AddLearnerError::ForwardToLeader(e)) => forward_to_leader(e, uri, req).await,
            Err(AddLearnerError::Fatal(e)) => Err(APIError::Fatal(e)),
            Err(AddLearnerError::Exists(_node_id)) => Ok(None), // we want the API call to be idempotent and not error if the node is already a learner
        }
    }
}

#[async_trait::async_trait]
impl ToAPIResult<()> for Result<(), RemoveLearnerError> {
    async fn to_api_result(self, uri: &Uri, req: &APIRequest) -> APIResult<()> {
        match self {
            Ok(()) | Err(RemoveLearnerError::NotExists(_)) => Ok(()), // if the node is not part of the cluster, the effect is the same, so we say it is fine
            Err(RemoveLearnerError::ForwardToLeader(e)) => forward_to_leader(e, uri, req).await,
            Err(RemoveLearnerError::Fatal(e)) => Err(APIError::Fatal(e)),
            Err(e @ RemoveLearnerError::NotLearner(_)) => Err(APIError::HTTP {
                status: StatusCode::CONFLICT,
                message: e.to_string(),
            }),
        }
    }
}

#[async_trait::async_trait()]
impl ToAPIResult<TremorResponse> for Result<ClientWriteResponse<TremorResponse>, ClientWriteError> {
    // we need the request context here to construct the redirect url properly
    async fn to_api_result(self, uri: &Uri, state: &APIRequest) -> APIResult<TremorResponse> {
        match self {
            Ok(response) => Ok(response.data),
            Err(ClientWriteError::ForwardToLeader(e)) => forward_to_leader(e, uri, state).await,
            Err(ClientWriteError::Fatal(e)) => Err(APIError::Fatal(e)),
            Err(ClientWriteError::ChangeMembershipError(e)) => Err(APIError::ChangeMembership(e)),
        }
    }
}

#[allow(clippy::unused_async)]
async fn forward_to_leader<T>(e: ForwardToLeader, uri: &Uri, state: &APIRequest) -> APIResult<T>
where
    T: serde::Serialize + serde::Deserialize<'static>,
{
    Err(if let Some(leader_id) = e.leader_id {
        let (tx, rx) = oneshot();
        state
            .store_tx
            .send(APIStoreReq::GetNode(leader_id, tx))
            .await?;
        // we can only forward to the leader if we have the node in our state machine
        if let Some(leader_addr) = timeout(API_WORKER_TIMEOUT, rx).await?? {
            let mut leader_url = url::Url::parse(&format!(
                "{}://{}",
                uri.scheme()
                    .map_or_else(|| "http".to_string(), ToString::to_string),
                leader_addr.api()
            ))?;
            leader_url.set_path(uri.path());
            leader_url.set_query(uri.query());
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
        APIError::NoLeader
    })
}

impl IntoResponse for TremorResponse {
    fn into_response(self) -> Response {
        (StatusCode::OK, Json(self)).into_response()
    }
}

impl std::fmt::Display for APIError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            APIError::ForwardToLeader {
                leader_url: leader_addr,
                node_id,
            } => f
                .debug_struct("ForwardToLeader")
                .field("leader_addr", leader_addr)
                .field("node_id", node_id)
                .finish(),
            APIError::Other(s) | Self::Store(s) => write!(f, "{s}"),
            APIError::HTTP { message, status } => write!(f, "HTTP {status} {message}"),
            APIError::Fatal(e) => write!(f, "Fatal Error: {e}"),
            APIError::ChangeMembership(e) => write!(f, "Error changing cluster membership: {e}"),
            APIError::NoQuorum(e) => write!(f, "Quorum: {e}"),
            APIError::Storage(e) => write!(f, "Storage: {e}"),
            APIError::Runtime(e) => write!(f, "Runtime: {e}"),
            APIError::App(e) => write!(f, "App: {e}"),
            APIError::Timeout => write!(f, "Timeout"),
            APIError::Recv => write!(f, "Recv"),
            APIError::NoLeader => write!(f, "No Leader"),
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

impl From<tokio::sync::oneshot::error::RecvError> for APIError {
    fn from(_: tokio::sync::oneshot::error::RecvError) -> Self {
        // FIXME: add error
        Self::Recv
    }
}

impl From<tokio::time::error::Elapsed> for APIError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout
    }
}
