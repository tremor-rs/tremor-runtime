use std::collections::BTreeSet;
use std::sync::Arc;

use tide::Body;
use tide::Request;
use tide::Response;
use tide::StatusCode;

use crate::raft::NodeId;
use crate::raft::Server;
use crate::raft::{app, TremorNode};

// --- Cluster management

pub fn install_rest_endpoints(app: &mut Server) {
    let mut cluster = app.at("/cluster");
    cluster
        .at("/learners")
        .post(add_learner)
        .delete(remove_learner);
    cluster.at("/change-membership").post(change_membership);
    cluster.at("/metrics").get(metrics);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AddLearner {
    pub id: NodeId,
    pub rpc: String,
    pub api: String,
}

/// Add a node as **Learner**.
///
/// A Learner receives log replication from the leader but does not vote.
/// This should be done before adding a node as a member into the cluster
/// (by calling `change-membership`)
async fn add_learner(mut req: Request<Arc<app::Tremor>>) -> tide::Result {
    let AddLearner { id, rpc, api } = req.body_json().await?;

    let node = TremorNode {
        rpc_addr: rpc,
        api_addr: api,
    };
    let result = req.state().raft.add_learner(id, node, true).await?;
    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&result)?)
        .build())
}

/// Removes a node from **Learners**.
///
async fn remove_learner(mut req: Request<Arc<app::Tremor>>) -> tide::Result {
    let id = req.body_json().await?;

    let result = req.state().raft.remove_learner(id, true).await?;
    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&result)?)
        .build())
}

/// Changes specified learners to members, or remove members.
async fn change_membership(mut req: Request<Arc<app::Tremor>>) -> tide::Result {
    let body: BTreeSet<NodeId> = req.body_json().await?;
    let result = req
        .state()
        .raft
        .change_membership(body, true, false)
        .await?;
    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&result)?)
        .build())
}

/// Get the latest metrics of the cluster
#[allow(clippy::unused_async)]
async fn metrics(req: Request<Arc<app::Tremor>>) -> tide::Result {
    let metrics = req.state().raft.metrics().borrow().clone();

    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&metrics)?)
        .build())
}
