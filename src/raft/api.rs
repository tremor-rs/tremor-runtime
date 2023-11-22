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

pub(crate) mod apps;
pub mod client;
mod cluster;
/// API endpoints for the KV store
pub mod kv;
pub(crate) mod worker;

use self::apps::AppState;
use crate::{
    channel::{OneShotSender, Receiver, Sender},
    raft::{
        node::Addr,
        store::{self, StateApp, Store, TremorResponse},
        NodeId, TremorRaftImpl,
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
        ChangeMembershipError, ClientWriteError, Fatal, ForwardToLeader, QuorumNotEnough, RaftError,
    },
    StorageError,
};
use std::collections::HashMap;
use std::{collections::BTreeSet, num::ParseIntError, sync::Arc, time::Duration};
use tokio::{task::JoinHandle, time::timeout};
use tremor_common::alias;

use super::store::ResponseError;

pub(crate) type APIRequest = Arc<ServerState>;
/// Tyape alias for API results
pub type APIResult<T> = Result<T, APIError>;

const API_WORKER_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) type ReplySender<T> = OneShotSender<T>;

#[derive(Debug)]
pub(crate) enum APIStoreReq {
    GetApp(alias::App, ReplySender<Option<StateApp>>),
    GetApps(ReplySender<HashMap<alias::App, AppState>>),
    KVGet(String, ReplySender<Option<Vec<u8>>>),
    GetNode(NodeId, ReplySender<Option<Addr>>),
    GetNodes(ReplySender<HashMap<NodeId, Addr>>),
    GetNodeId(Addr, ReplySender<Option<NodeId>>),
    GetLastMembership(ReplySender<BTreeSet<NodeId>>),
}

pub(crate) struct ServerState {
    id: NodeId,
    addr: Addr,
    raft: TremorRaftImpl,
    raft_manager: super::Cluster,
}

impl ServerState {
    pub(crate) async fn ensure_leader(&self, uri: Option<Uri>) -> Result<(), APIError> {
        self.raft.is_leader().await.map_err(|e| match e {
            RaftError::APIError(e) => match e {
                openraft::error::CheckIsLeaderError::ForwardToLeader(e) => {
                    // forward_to_leader(e, uri, state).await
                    e.leader_id
                        .zip(e.leader_node)
                        .map_or(APIError::NoLeader, |(node_id, addr)| {
                            APIError::ForwardToLeader { node_id, addr, uri }
                        })
                }
                openraft::error::CheckIsLeaderError::QuorumNotEnough(e) => APIError::NoQuorum(e),
            },
            RaftError::Fatal(e) => APIError::Fatal(e),
        })
    }
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
    store: Store,
    store_tx: Sender<APIStoreReq>,
    store_rx: Receiver<APIStoreReq>,
) -> (JoinHandle<()>, Arc<ServerState>) {
    let handle = tokio::task::spawn(worker::api_worker(store, store_rx));
    let state = Arc::new(ServerState {
        id,
        addr,
        raft: raft.clone(),
        raft_manager: super::Cluster::new(id, store_tx, raft),
    });
    (handle, state)
}

pub(crate) fn endpoints() -> Router<APIRequest> {
    Router::new()
        .nest("/v1/cluster", cluster::endpoints())
        .nest("/v1/api/apps", apps::endpoints())
        .nest("/v1/api/kv", kv::endpoints())
}

/// Argument errors
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ArgsError {
    /// A missing argument
    Missing(String),
    /// An invalid argument
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

/// App errors
#[derive(Debug, Serialize)]
pub enum AppError {
    /// app is already installed
    AlreadyInstalled(alias::App),

    /// app not found
    AppNotFound(alias::App),
    /// flow not found
    FlowNotFound(alias::Flow),
    /// flow instance not found
    InstanceNotFound(alias::Flow),
    /// App has at least 1 running instances (and thus cannot be uninstalled)
    HasInstances(alias::App, Vec<alias::Flow>),
    /// flow instance already exists
    InstanceAlreadyExists(alias::Flow),
    /// provided flow args are invalid
    InvalidArgs {
        /// flow id
        flow: alias::Flow,
        /// instance id
        instance: alias::Flow,
        /// errors
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
            Self::FlowNotFound(flow_id) => {
                write!(f, "Flow \"{flow_id}\" not found.")
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

/// API errors
#[derive(Debug, Serialize, thiserror::Error)]
pub enum APIError {
    /// We need to send this API request to the leader_url
    ForwardToLeader {
        /// target node
        node_id: NodeId,
        /// target address
        addr: Addr,
        /// target url
        #[serde(skip)]
        uri: Option<Uri>,
    },
    /// HTTP related error
    HTTP {
        /// HTTP status code
        #[serde(serialize_with = "round_sc")]
        status: StatusCode,
        /// HTTP error message
        message: String,
    },
    /// raft fatal error, includes StorageError
    Fatal(Fatal<NodeId>),
    /// We don't have a quorum to read
    NoQuorum(QuorumNotEnough<NodeId>),
    /// We don't have a leader
    NoLeader,
    /// Error when changing a membership
    ChangeMembership(ChangeMembershipError<NodeId>),
    /// Error from our store/statemachine
    Store(String),
    /// openraft storage error
    Storage(StorageError<NodeId>),
    /// Errors around tremor apps
    App(AppError),
    /// Error in the runtime
    Runtime(String),
    /// Timeout
    Timeout,
    /// recv error
    Recv,
    /// Response error
    Response(#[from] ResponseError),
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
            | APIError::Response(_)
            | APIError::App(_) => StatusCode::INTERNAL_SERVER_ERROR,
            APIError::NoLeader | APIError::NoQuorum(_) => StatusCode::SERVICE_UNAVAILABLE,
            APIError::Timeout => StatusCode::GATEWAY_TIMEOUT,
            APIError::HTTP { status, .. } => *status,
        };

        if let APIError::ForwardToLeader { addr, uri, .. } = self {
            let mut headers = HeaderMap::new();
            let uri = if let Some(uri) = uri {
                let path_and_query = if let Some(query) = uri.query() {
                    format!("{}?{}", uri.path(), query)
                } else {
                    uri.path().to_string()
                };
                http::uri::Builder::new()
                    .scheme(uri.scheme_str().unwrap_or("http"))
                    .authority(addr.api())
                    .path_and_query(path_and_query)
                    .build()
                    .unwrap_or_default()
            } else {
                Uri::default()
            };

            if let Ok(v) = uri.to_string().parse() {
                headers.insert(LOCATION, v);
            }
            (status, headers).into_response()
        } else {
            (status, Json(self)).into_response()
        }
    }
}

pub(crate) async fn to_api_result<T>(
    r: Result<T, RaftError<NodeId, ClientWriteError<NodeId, Addr>>>,
    uri: &Uri,
    state: &APIRequest,
) -> APIResult<T>
where
    T: serde::Serialize + serde::Deserialize<'static>,
{
    match r {
        Ok(response) => Ok(response),
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(e))) => {
            forward_to_leader(e, uri, state).await
        }
        Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(e))) => {
            Err(APIError::ChangeMembership(e))
        }
        Err(RaftError::Fatal(e)) => Err(APIError::Fatal(e)),
    }
}

#[allow(clippy::unused_async)]
async fn forward_to_leader<T>(
    e: ForwardToLeader<NodeId, Addr>,
    uri: &Uri,
    state: &APIRequest,
) -> APIResult<T>
where
    T: serde::Serialize + serde::Deserialize<'static>,
{
    Err(if let Some(leader_id) = e.leader_id {
        // we can only forward to the leader if we have the node in our state machine
        if let Some(leader_addr) =
            timeout(API_WORKER_TIMEOUT, state.raft_manager.get_node(leader_id)).await??
        {
            debug!("Forwarding to leader: {uri}");
            // we don't care about fragment

            APIError::ForwardToLeader {
                node_id: leader_id,
                addr: leader_addr,
                uri: Some(uri.clone()),
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
                addr: leader_addr,
                node_id,
                uri,
            } => f
                .debug_struct("ForwardToLeader")
                .field("leader_addr", leader_addr)
                .field("node_id", node_id)
                .field("uri", uri)
                .finish(),

            APIError::Other(s) => write!(f, "{s}"),
            APIError::HTTP { message, status } => write!(f, "HTTP {status} {message}"),
            APIError::Fatal(e) => write!(f, "Fatal Error: {e}"),
            APIError::ChangeMembership(e) => write!(f, "Error changing cluster membership: {e}"),
            APIError::NoQuorum(e) => write!(f, "Quorum: {e}"),
            APIError::Storage(e) => write!(f, "Storage: {e}"),
            APIError::Runtime(e) => write!(f, "Runtime: {e}"),
            APIError::App(e) => write!(f, "App: {e}"),
            APIError::Timeout => write!(f, "Timeout"),
            APIError::Recv => write!(f, "Recv error"),
            APIError::NoLeader => write!(f, "No Leader"),
            APIError::Store(e) => write!(f, "Store error: {e}"),
            APIError::Response(e) => write!(f, "Response error: {e}"),
        }
    }
}

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

impl From<StorageError<NodeId>> for APIError {
    fn from(e: StorageError<NodeId>) -> Self {
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
        Self::Recv
    }
}

impl From<tokio::time::error::Elapsed> for APIError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout
    }
}

impl From<simd_json::Error> for APIError {
    fn from(e: simd_json::Error) -> Self {
        Self::Other(e.to_string())
    }
}
