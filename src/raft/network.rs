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

use super::store::TremorRequest;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::AnyError;
use tarpc;

pub mod raft;
pub mod raft_network_impl;

#[tarpc::service]
pub(crate) trait Raft {
    async fn vote(vote: VoteRequest) -> Result<VoteResponse, AnyError>;
    async fn append(
        req: AppendEntriesRequest<TremorRequest>,
    ) -> Result<AppendEntriesResponse, AnyError>;
    async fn snapshot(req: InstallSnapshotRequest) -> Result<InstallSnapshotResponse, AnyError>;
}
