use std::sync::Arc;

use openraft::{
    error::{AppendEntriesError, InstallSnapshotError, VoteError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use tarpc::context;

use crate::raft::{app, store::TremorRequest};

// --- Raft communication server side
#[derive(Clone)]
pub struct Server {
    app: Arc<app::Tremor>,
}

impl Server {
    pub(crate) fn new(app: Arc<app::Tremor>) -> Self {
        Self { app }
    }
}

#[tarpc::server]
impl super::Raft for Server {
    async fn vote(self, _: context::Context, vote: VoteRequest) -> Result<VoteResponse, VoteError> {
        self.app.raft.vote(vote).await
    }
    async fn append(
        self,
        _: context::Context,
        req: AppendEntriesRequest<TremorRequest>,
    ) -> Result<AppendEntriesResponse, AppendEntriesError> {
        self.app.raft.append_entries(req).await
    }
    async fn snapshot(
        self,
        _: context::Context,
        req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, InstallSnapshotError> {
        self.app.raft.install_snapshot(req).await
    }
}
