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

use halfbrown::HashMap;
use tide::{http::StatusCode, Request, Route};

use crate::raft::{
    api::{wrapp, APIError, APIResult, ToAPIResult},
    app,
    node::Addr,
    store::{NodesRequest, TremorRequest},
};
use openraft::{error::LearnerNotFound, LogId, NodeId, RaftMetrics};
use std::sync::Arc;

pub(crate) fn install_rest_endpoints(app: &mut Route<Arc<app::Tremor>>) {
    let mut cluster = app.at("/cluster");
    cluster
        .at("/nodes")
        .post(wrapp(add_node))
        .get(wrapp(get_nodes));
    cluster.at("nodes/:node_id").delete(wrapp(remove_node));
    cluster
        .at("/learners/:node_id")
        .put(wrapp(add_learner))
        .patch(wrapp(add_learner))
        .delete(wrapp(remove_learner));
    cluster
        .at("/voters/:node_id")
        .put(wrapp(promote_voter))
        .patch(wrapp(promote_voter))
        .delete(wrapp(demote_voter));
    cluster.at("/metrics").get(wrapp(metrics));
}

/// Get a list of all currently known nodes (be it learner, leader, voter etc.)
async fn get_nodes(req: Request<Arc<app::Tremor>>) -> APIResult<HashMap<NodeId, Addr>> {
    let state = req.state();
    state.raft.client_read().await.to_api_result(&req).await?;

    let nodes = state
        .store
        .state_machine
        .read()
        .await
        .nodes
        .get_nodes()
        .clone();
    Ok(nodes)
}

/// Make a node known to cluster by putting it onto the cluster state
///
/// This is a precondition for the node being added as learner and promoted to voter
async fn add_node(mut req: Request<Arc<app::Tremor>>) -> APIResult<NodeId> {
    // FIXME: returns 500 if not the leader
    // FIXME: better client errors
    let addr: Addr = req.body_json().await?;
    let state = req.state();

    // 1. ensure we are on the leader, as we need to read some state-machine state
    //    in order to give a good answer here
    state.raft.client_read().await.to_api_result(&req).await?;

    // 2. ensure we don't add the node twice if it is already there
    // we need to make sure we don't hold on to the state machine lock any further here
    let maybe_existing_node_id = state
        .store
        .state_machine
        .read()
        .await
        .nodes
        .find_node_id(&addr)
        .copied();
    if let Some(existing_node_id) = maybe_existing_node_id {
        Ok(existing_node_id)
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
            .to_api_result(&req)
            .await?;
        let node_id = response
            .value
            .ok_or_else(|| APIError::Other("Invalid node_id".to_string()))?
            .parse::<NodeId>()?;
        debug!("node {addr} added to the cluster as node {node_id}");
        Ok(node_id)
    }
}

/// Remove the node from the cluster
///
/// # Errors
/// if the API call fails
async fn remove_node(req: Request<Arc<app::Tremor>>) -> APIResult<()> {
    let node_id = req.param("node_id")?.parse::<NodeId>()?;
    // make sure the node is not a learner or a voter
    {
        let sm = req.state().store.state_machine.read().await;
        if let Some(membership) = sm.get_last_membership()? {
            if membership.membership.contains(&node_id) {
                return Err(APIError::HTTP {
                    status: StatusCode::Conflict,
                    message: format!("Node {node_id} cannot be removed as it is still a voter."),
                });
            }
            // TODO: how to check if the node is a learner?
        }
    }
    // remove the node metadata from the state machine
    req.state()
        .raft
        .client_write(TremorRequest::Nodes(NodesRequest::RemoveNode { node_id }))
        .await
        .to_api_result(&req)
        .await?;
    Ok(())
}

/// Add a node as **Learner**.
///
/// A Learner receives log replication from the leader but does not vote.
/// This should be done before adding a node as a member into the cluster
async fn add_learner(req: Request<Arc<app::Tremor>>) -> APIResult<Option<LogId>> {
    let node_id = req.param("node_id")?.parse::<NodeId>()?;
    let state = req.state();

    // 1. ensure we are on the leader, as we need to read some state-machine state
    //    in order to give a good answer here
    state.raft.client_read().await.to_api_result(&req).await?;

    // 2. check that the node has already been added
    // we need to make sure we don't hold on to the state machine lock any further here
    if state
        .store
        .state_machine
        .read()
        .await
        .nodes
        .get_node(node_id)
        .is_none()
    {
        return Err(APIError::HTTP {
            status: StatusCode::NotFound,
            message: format!("Node {node_id} is not known to the cluster yet."),
        });
    }

    // add the node as learner
    debug!("Adding node {node_id} as learner...");
    req.state()
        .raft
        .add_learner(node_id, true)
        .await
        .to_api_result(&req)
        .await
}

/// Removes a node from **Learners** only
///
async fn remove_learner(req: Request<Arc<app::Tremor>>) -> APIResult<()> {
    let node_id = req.param("node_id")?.parse::<NodeId>()?;
    let state = req.state();
    // remove the node as learner
    state
        .raft
        .remove_learner(node_id)
        .await
        .to_api_result(&req)
        .await
}

/// Changes specified learners to members, or remove members.
async fn promote_voter(req: Request<Arc<app::Tremor>>) -> APIResult<Option<NodeId>> {
    let node_id: NodeId = req.param("node_id")?.parse::<NodeId>()?;
    let state = req.state();
    // we introduce a new scope here to release the lock on the state machine
    // not releasing it can lead to dead-locks, if executed on the leader (as the store is shared between the API and the raft engine)
    let mut new_membership_config = {
        let sm = state.store.state_machine.read().await;
        // check if node is known to the state machine (has been added as learner previously)
        if sm.nodes.get_node(node_id).is_none() {
            return Err(APIError::HTTP {
                status: StatusCode::NotFound,
                message: format!("Node {node_id} is not known to the cluster yet."),
            });
        }

        // update membership
        let membership = sm
            .get_last_membership()?
            .ok_or_else(|| APIError::Store("No membership stored in statemachine".to_string()))?;
        let config = membership
            .membership
            .get_configs()
            .last()
            .ok_or_else(|| APIError::Store("No membership config available".to_string()))?;
        config.clone()
    };

    let value = if new_membership_config.insert(node_id) {
        // only update state if not already in the membership config
        // this call always returns TremorResponse { value: None } // see store.rs
        state
            .raft
            .change_membership(new_membership_config, true)
            .await
            .to_api_result(&req)
            .await?;
        Some(node_id)
    } else {
        None
    };
    Ok(value)
}

/// Changes specified learners to members, or remove members.
async fn demote_voter(req: Request<Arc<app::Tremor>>) -> APIResult<Option<NodeId>> {
    let node_id: NodeId = req.param("node_id")?.parse::<NodeId>()?;
    let state = req.state();
    // scoping here to not hold the state machine locked for too long
    let mut new_membership_config = {
        let sm = state.store.state_machine.read().await;
        // check if node is known to the state machine (has been added as learner previously)
        if sm.nodes.get_node(node_id).is_none() {
            return Err(APIError::ChangeMembership(
                openraft::error::ChangeMembershipError::LearnerNotFound(LearnerNotFound {
                    node_id,
                }),
            ));
        }

        // update membership
        let membership = sm
            .get_last_membership()?
            .ok_or_else(|| APIError::Store("No membership stored in statemachine".to_string()))?;
        let config = membership
            .membership
            .get_configs()
            .last()
            .ok_or_else(|| APIError::Store("No membership config available".to_string()))?;
        config.clone()
    };
    let value = if new_membership_config.remove(&node_id) {
        req.state()
            .raft
            .change_membership(new_membership_config, true)
            .await
            .to_api_result(&req)
            .await?;
        Some(node_id)
    } else {
        None
    };
    Ok(value)
}

/// Get the latest metrics of the cluster (from the viewpoint of the targeted node)
#[allow(clippy::unused_async)]
async fn metrics(req: Request<Arc<app::Tremor>>) -> APIResult<RaftMetrics> {
    Ok(req.state().raft.metrics().borrow().clone())
}
