use std::{fmt::Display, sync::Arc};

use super::{app, store::TremorRequest};
use futures::Future;
use openraft::{
    error::{AppendEntriesError, InstallSnapshotError, VoteError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use tide::{Body, Endpoint, Request, Response, StatusCode};

pub mod api;
pub mod management;
pub mod raft;
pub mod raft_network_impl;

#[tarpc::service]
pub(crate) trait Raft {
    async fn vote(vote: VoteRequest) -> Result<VoteResponse, VoteError>;
    async fn append(
        req: AppendEntriesRequest<TremorRequest>,
    ) -> Result<AppendEntriesResponse, AppendEntriesError>;
    async fn snapshot(
        req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, InstallSnapshotError>;
}

type APIRequest = Request<Arc<app::Tremor>>;
pub type APIResult<T: serde::Serialize + serde::Deserialize<'static>> = Result<T, APIError>;

#[derive(Debug, Clone)]
pub enum APIError {
    Other(String),
}

trait ResultToStatus {
    fn status(&self) -> StatusCode;
}

impl Display for APIError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            APIError::Other(s) => write!(f, "{}", s),
        }
    }
}
impl std::error::Error for APIError {}

impl ResultToStatus for APIError {
    fn status(&self) -> StatusCode {
        match self {
            APIError::Other(_) => StatusCode::InternalServerError,
        }
    }
}

impl<T> ResultToStatus for Result<T, APIError> {
    fn status(&self) -> StatusCode {
        match self {
            Ok(_) => StatusCode::Ok,
            Err(e) => e.status(),
        }
    }
}

async fn wrapp<F, R>(f: fn(APIRequest) -> F) -> impl Endpoint
where
    R: serde::Serialize,
    F: Future<Output = Result<R, APIError>> + Send + 'static,
{
    |req: Request<()>| async move {
        let res = f.call(req).await;
        let status = res.status();

        Response::builder(status)
            .body(Body::from_json(*res))
            .build()
    }
}
