use std::sync::Arc;

use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::{
    error::{AppendEntriesError, InstallSnapshotError, VoteError},
    raft::AppendEntriesRequest,
};
use tarpc::context;

use crate::raft::TremorTypeConfig;
use crate::raft::{app::TremorApp, TremorNodeId};

// --- Raft communication server side
#[derive(Clone)]
pub struct RaftServer {
    app: Arc<TremorApp>,
}

impl RaftServer {
    pub(crate) fn new(app: Arc<TremorApp>) -> Self {
        Self { app }
    }
}

#[tarpc::server]
impl super::Raft for RaftServer {
    async fn vote(
        self,
        _: context::Context,
        vote: VoteRequest<TremorNodeId>,
    ) -> Result<VoteResponse<TremorNodeId>, VoteError<TremorNodeId>> {
        self.app.raft.vote(vote).await
    }
    async fn append(
        self,
        _: context::Context,
        req: AppendEntriesRequest<TremorTypeConfig>,
    ) -> Result<AppendEntriesResponse<TremorNodeId>, AppendEntriesError<TremorNodeId>> {
        self.app.raft.append_entries(req).await
    }
    async fn snapshot(
        self,
        _: context::Context,
        req: InstallSnapshotRequest<TremorTypeConfig>,
    ) -> Result<InstallSnapshotResponse<TremorNodeId>, InstallSnapshotError<TremorNodeId>> {
        self.app.raft.install_snapshot(req).await
    }
}
