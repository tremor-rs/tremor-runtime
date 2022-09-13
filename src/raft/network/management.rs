use std::collections::BTreeSet;
use std::sync::Arc;

use tide::Body;
use tide::Request;
use tide::Response;
use tide::StatusCode;

use crate::raft::Server;
use crate::raft::TremorNodeId;
use crate::raft::{app::TremorApp, TremorNode};

// --- Cluster management

pub fn install_rest_endpoints(app: &mut Server) {
    let mut cluster = app.at("/cluster");
    cluster.at("/add-learner").post(add_learner);
    cluster.at("/change-membership").post(change_membership);
    cluster.at("/metrics").get(metrics);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AddLearner {
    pub id: TremorNodeId,
    pub rpc: String,
    pub api: String,
}

/// Add a node as **Learner**.
///
/// A Learner receives log replication from the leader but does not vote.
/// This should be done before adding a node as a member into the cluster
/// (by calling `change-membership`)
async fn add_learner(mut req: Request<Arc<TremorApp>>) -> tide::Result {
    let AddLearner { id, rpc, api } = req.body_json().await?;

    let node = TremorNode {
        rpc_addr: rpc,
        api_addr: api,
    };
    let res = req.state().raft.add_learner(id, node, true).await?;
    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&res)?)
        .build())
}

/// Changes specified learners to members, or remove members.
async fn change_membership(mut req: Request<Arc<TremorApp>>) -> tide::Result {
    let body: BTreeSet<TremorNodeId> = req.body_json().await?;
    let res = req
        .state()
        .raft
        .change_membership(body, true, false)
        .await?;
    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&res)?)
        .build())
}

/// Get the latest metrics of the cluster
async fn metrics(req: Request<Arc<TremorApp>>) -> tide::Result {
    let metrics = req.state().raft.metrics().borrow().clone();

    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&metrics)?)
        .build())
}
