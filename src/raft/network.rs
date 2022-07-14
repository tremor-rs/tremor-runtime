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
    async fn vote(vote: VoteRequest<u64>) -> Result<VoteResponse<u64>, VoteError<TremorNodeId>>;
    async fn append(
        req: AppendEntriesRequest<TremorTypeConfig>,
    ) -> Result<AppendEntriesResponse<u64>, AppendEntriesError<TremorNodeId>>;
    async fn snapshot(
        req: InstallSnapshotRequest<TremorTypeConfig>,
    ) -> Result<InstallSnapshotResponse<u64>, InstallSnapshotError<TremorNodeId>>;
}
