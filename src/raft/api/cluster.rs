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

use super::{APIRequest, API_WORKER_TIMEOUT};
use crate::raft::{
    api::{APIError, APIResult, ToAPIResult},
    node::Addr,
    store::{NodesRequest, TremorRequest},
    NodeId,
};
use axum::{
    extract::{self, Json},
    routing::{delete, get, put},
    Router,
};
use http::StatusCode;
use openraft::{ChangeMembers, LogId, RaftMetrics};
use std::collections::{BTreeSet, HashMap};
use tokio::time::timeout;

pub(crate) fn endpoints() -> Router<APIRequest> {
    Router::<APIRequest>::new()
        .route("/nodes", get(get_nodes).post(add_node))
        .route("/nodes/:node_id", delete(remove_node))
        .route(
            "/learners/:node_id",
            put(add_learner).patch(add_learner).delete(remove_learner),
        )
        .route(
            "/voters/:node_id",
            put(promote_voter).patch(promote_voter).delete(demote_voter),
        )
        .route("/metrics", get(metrics))
}

/// Get a list of all currently known nodes (be it learner, leader, voter etc.)
async fn get_nodes(
    extract::State(state): extract::State<APIRequest>,
    extract::OriginalUri(uri): extract::OriginalUri,
) -> APIResult<Json<HashMap<NodeId, Addr>>> {
    state.ensure_leader(Some(uri)).await?;

    let nodes = timeout(API_WORKER_TIMEOUT, state.raft_manager.get_nodes()).await??;
    Ok(Json(nodes))
}

/// Make a node known to cluster by putting it onto the cluster state
///
/// This is a precondition for the node being added as learner and promoted to voter
async fn add_node(
    extract::State(state): extract::State<APIRequest>,
    extract::OriginalUri(uri): extract::OriginalUri,
    Json(addr): Json<Addr>,
) -> APIResult<Json<NodeId>> {
    // 1. ensure we are on the leader, as we need to read some state-machine state
    //    in order to give a good answer here
    state.ensure_leader(Some(uri.clone())).await?;

    // 2. ensure we don't add the node twice if it is already there
    // we need to make sure we don't hold on to the state machine lock any further here

    let maybe_existing_node_id = timeout(
        API_WORKER_TIMEOUT,
        state.raft_manager.get_node_id(addr.clone()),
    )
    .await??;
    if let Some(existing_node_id) = maybe_existing_node_id {
        Ok(Json(existing_node_id))
    } else {
        // 2a. add the node with its metadata to the state machine, so the network impl can reach it
        // this will fail, when we are not on the leader
        // when this succeeds the local state machine should have the node addr stored, so the network can access it
        // in order to establish a network connection
        debug!("node {addr} not yet known to cluster");
        let response = state
            .raft
            .client_write(TremorRequest::Nodes(NodesRequest::AddNode {
                addr: addr.clone(),
            }))
            .await
            .to_api_result(&uri, &state)
            .await?;
        let node_id = response
            .data
            .value
            .ok_or_else(|| APIError::Other("Invalid node_id".to_string()))?
            .parse::<NodeId>()?;
        debug!("node {addr} added to the cluster as node {node_id}");
        Ok(Json(node_id))
    }
}

/// Remove the node from the cluster
///
/// # Errors
/// if the API call fails
async fn remove_node(
    extract::State(state): extract::State<APIRequest>,
    extract::OriginalUri(uri): extract::OriginalUri,
    extract::Path(node_id): extract::Path<NodeId>,
) -> APIResult<Json<()>> {
    // make sure the node is not a learner or a voter

    let membership =
        timeout(API_WORKER_TIMEOUT, state.raft_manager.get_last_membership()).await??;
    if membership.contains(&node_id) {
        return Err(APIError::HTTP {
            status: StatusCode::CONFLICT,
            message: format!("Node {node_id} cannot be removed as it is still a voter."),
        });
    }
    // TODO: how to check if the node is a learner?
    // remove the node metadata from the state machine
    state
        .raft
        .client_write(TremorRequest::Nodes(NodesRequest::RemoveNode { node_id }))
        .await
        .to_api_result(&uri, &state)
        .await?;
    Ok(Json(()))
}

/// Add a node as **Learner**.
///
/// A Learner receives log replication from the leader but does not vote.
/// This should be done before adding a node as a member into the cluster
async fn add_learner(
    extract::State(state): extract::State<APIRequest>,
    extract::OriginalUri(uri): extract::OriginalUri,
    extract::Path(node_id): extract::Path<NodeId>,
) -> APIResult<Json<LogId<crate::raft::NodeId>>> {
    // 1. ensure we are on the leader, as we need to read some state-machine state
    //    in order to give a good answer here
    state.ensure_leader(Some(uri.clone())).await?;

    // 2. check that the node has already been added
    // we need to make sure we don't hold on to the state machine lock any further here

    let node_addr = timeout(API_WORKER_TIMEOUT, state.raft_manager.get_node(node_id))
        .await??
        .ok_or(APIError::HTTP {
            status: StatusCode::NOT_FOUND,
            message: format!("Node {node_id} is not known to the cluster yet."),
        })?;

    // add the node as learner
    debug!("Adding node {node_id} as learner...");
    state
        .raft
        .add_learner(node_id, node_addr, true)
        .await
        .to_api_result(&uri, &state)
        .await
        .map(|d| Json(d.log_id))
}

/// Removes a node from **Learners** only
///
async fn remove_learner(
    extract::State(state): extract::State<APIRequest>,
    extract::OriginalUri(uri): extract::OriginalUri,
    extract::Path(node_id): extract::Path<NodeId>,
) -> APIResult<Json<()>> {
    debug!("[API] Removing learner {node_id}",);
    // remove the node as learner
    // let result = state.raft.remove_learner(node_id).await;
    let mut nodes = BTreeSet::new();
    nodes.insert(node_id);

    let _result = state
        .raft
        .change_membership(ChangeMembers::RemoveNodes(nodes), true)
        .await
        .to_api_result(&uri, &state)
        .await?;
    Ok(Json(()))
}

/// Changes specified learners to members, or remove members.
async fn promote_voter(
    extract::State(state): extract::State<APIRequest>,
    extract::OriginalUri(uri): extract::OriginalUri,
    extract::Path(node_id): extract::Path<NodeId>,
) -> APIResult<Json<Option<NodeId>>> {
    // we introduce a new scope here to release the lock on the state machine
    // not releasing it can lead to dead-locks, if executed on the leader (as the store is shared between the API and the raft engine)

    let mut membership =
        timeout(API_WORKER_TIMEOUT, state.raft_manager.get_last_membership()).await??;

    let value = if membership.insert(node_id) {
        // only update state if not already in the membership config
        // this call always returns TremorResponse { value: None } // see store.rs
        state
            .raft
            .change_membership(membership, true)
            .await
            .to_api_result(&uri, &state)
            .await?;
        Some(node_id)
    } else {
        None
    };
    Ok(Json(value))
}

/// Changes specified learners to members, or remove members.
async fn demote_voter(
    extract::State(state): extract::State<APIRequest>,
    extract::OriginalUri(uri): extract::OriginalUri,
    extract::Path(node_id): extract::Path<NodeId>,
) -> APIResult<Json<Option<NodeId>>> {
    // scoping here to not hold the state machine locked for too long

    let mut membership =
        timeout(API_WORKER_TIMEOUT, state.raft_manager.get_last_membership()).await??;
    let value = if membership.remove(&node_id) {
        state
            .raft
            .change_membership(membership, true)
            .await
            .to_api_result(&uri, &state)
            .await?;
        Some(node_id)
    } else {
        None
    };
    Ok(Json(value))
}

/// Get the latest metrics of the cluster (from the viewpoint of the targeted node)
#[allow(clippy::unused_async)]
async fn metrics(
    extract::State(state): extract::State<APIRequest>,
) -> APIResult<Json<RaftMetrics<crate::raft::NodeId, crate::raft::node::Addr>>> {
    Ok(Json(state.raft.metrics().borrow().clone()))
}
