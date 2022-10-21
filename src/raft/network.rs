use openraft::{
    error::{AppendEntriesError, InstallSnapshotError, VoteError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};

use super::store::TremorRequest;

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
