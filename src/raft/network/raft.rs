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

use std::sync::Arc;

use openraft::{
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    AnyError,
};
use tarpc::context;

use crate::raft::{TremorRaftConfig, TremorRaftImpl};

// --- Raft communication server side
#[derive(Clone)]
pub struct Server {
    raft: Arc<TremorRaftImpl>,
}

impl Server {
    pub(crate) fn new(raft: Arc<TremorRaftImpl>) -> Self {
        Self { raft }
    }
}

#[tarpc::server]
impl super::Raft for Server {
    async fn vote(
        self,
        _: context::Context,
        vote: VoteRequest<crate::raft::NodeId>,
    ) -> Result<VoteResponse<crate::raft::NodeId>, AnyError> {
        self.raft.vote(vote).await.map_err(|e| AnyError::new(&e))
    }
    async fn append(
        self,
        _: context::Context,
        req: AppendEntriesRequest<TremorRaftConfig>,
    ) -> Result<AppendEntriesResponse<crate::raft::NodeId>, AnyError> {
        self.raft
            .append_entries(req)
            .await
            .map_err(|e| AnyError::new(&e))
    }
    async fn snapshot(
        self,
        _: context::Context,
        req: InstallSnapshotRequest<TremorRaftConfig>,
    ) -> Result<InstallSnapshotResponse<crate::raft::NodeId>, AnyError> {
        self.raft
            .install_snapshot(req)
            .await
            .map_err(|e| AnyError::new(&e))
    }
}
