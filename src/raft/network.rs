use crate::raft::{TremorNodeId, TremorTypeConfig};
use openraft::{
    error::{AppendEntriesError, InstallSnapshotError, VoteError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};

pub mod api;
pub mod management;
pub mod raft;
pub mod raft_network_impl;

#[tarpc::service]
pub(crate) trait Raft {
    async fn vote(
        vote: VoteRequest<TremorNodeId>,
    ) -> Result<VoteResponse<TremorNodeId>, VoteError<TremorNodeId>>;
    async fn append(
        req: AppendEntriesRequest<TremorTypeConfig>,
    ) -> Result<AppendEntriesResponse<TremorNodeId>, AppendEntriesError<TremorNodeId>>;
    async fn snapshot(
        req: InstallSnapshotRequest<TremorTypeConfig>,
    ) -> Result<InstallSnapshotResponse<TremorNodeId>, InstallSnapshotError<TremorNodeId>>;
}
