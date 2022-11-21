use super::{wrapp, APIResult};
use crate::raft::{app, store::TremorResponse, NodeId, Server, TremorNode};
use openraft::{raft::AddLearnerResponse, RaftMetrics};
use std::collections::BTreeSet;
use std::sync::Arc;
use tide::Request;

// --- Cluster management

pub fn install_rest_endpoints(app: &mut Server) {
    let mut cluster = app.at("/cluster");
    cluster
        .at("/learners")
        .post(wrapp(add_learner))
        .delete(wrapp(remove_learner));
    cluster
        .at("/voters")
        .post(wrapp(promote_voter))
        .delete(wrapp(demote_voter));
    cluster.at("/metrics").get(wrapp(metrics));
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
async fn add_learner(mut req: Request<Arc<app::Tremor>>) -> APIResult<AddLearnerResponse> {
    let AddLearner { id, rpc, api } = req.body_json().await?;

    let node = TremorNode {
        rpc_addr: rpc,
        api_addr: api,
    };
    Ok(req.state().raft.add_learner(id, node, true).await?)
}

/// Removes a node from **Learners**.
///
async fn remove_learner(mut req: Request<Arc<app::Tremor>>) -> APIResult<()> {
    let id = req.body_json().await?;

    Ok(req.state().raft.remove_learner(id, true).await?)
}

/// Changes specified learners to members, or remove members.
async fn promote_voter(mut req: Request<Arc<app::Tremor>>) -> APIResult<TremorResponse> {
    // FIXME add this code
    let body: BTreeSet<NodeId> = req.body_json().await?;
    Ok(req
        .state()
        .raft
        .change_membership(body, true, false)
        .await?)
}

/// Changes specified learners to members, or remove members.
async fn demote_voter(mut req: Request<Arc<app::Tremor>>) -> APIResult<TremorResponse> {
    // FIXME add this code
    let body: BTreeSet<NodeId> = req.body_json().await?;
    Ok(req
        .state()
        .raft
        .change_membership(body, true, false)
        .await?)
}

/// Get the latest metrics of the cluster
#[allow(clippy::unused_async)]
async fn metrics(req: Request<Arc<app::Tremor>>) -> APIResult<RaftMetrics> {
    Ok(req.state().raft.metrics().borrow().clone())
}
