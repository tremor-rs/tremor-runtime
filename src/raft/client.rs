use super::{
    network::{
        api::{AppState, InstallError, StartError},
        management::AddLearner,
    },
    store::{AppId, FlowId, InstanceId, TremorSet, TremorStart},
    TremorNode, TremorNodeId, TremorTypeConfig,
};
use openraft::{
    error::{
        AddLearnerError, CheckIsLeaderError, ClientWriteError, ForwardToLeader, Infallible,
        InitializeError, NetworkError, RPCError,
    },
    raft::{AddLearnerResponse, ClientWriteResponse},
    MessageSummary, RaftMetrics,
};
use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use simd_json::OwnedValue;
use std::collections::{BTreeSet, HashMap};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Empty {}

pub struct TremorClient {
    /// The leader node to send request to.
    ///
    /// All traffic should be sent to the leader in a cluster.
    pub leader: (TremorNodeId, String),

    pub inner: Client,
}

impl TremorClient {
    /// Create a client with a leader node id and a node manager to get node address by node id.
    pub fn new(leader_id: TremorNodeId, leader_addr: String) -> Self {
        Self {
            leader: (leader_id, leader_addr),
            inner: reqwest::Client::new(),
        }
    }
    // --- Internal methods

    /// Send RPC to specified node.
    ///
    /// It sends out a POST request if `req` is Some. Otherwise a GET request.
    /// The remote endpoint must respond a reply in form of `Result<T, E>`.
    /// An `Err` happened on remote will be wrapped in an [`RPCError::RemoteError`].
    async fn api_req<Req, Resp, Err>(
        &self,
        uri: &str,
        req: Option<&Req>,
    ) -> Result<Resp, RPCError<TremorNodeId, TremorNode, Err>>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error + Serialize + DeserializeOwned,
    {
        let (_leader_id, url) = {
            let t = &self.leader;
            let target_addr = &t.1;
            (t.0, format!("http://{}/{}", target_addr, uri))
        };

        let resp = if let Some(r) = req {
            if let Ok(json) = serde_json::to_string_pretty(&r) {
                debug!(">>> client send request to {url}: {json}",);
            }
            self.inner.post(url.clone()).json(r)
        } else {
            debug!(">>> client send request to {}", url,);
            self.inner.get(url.clone())
        }
        .send()
        .await
        .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let res: Resp = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        if let Ok(json) = serde_json::to_string_pretty(&res) {
            debug!("<<< client recv reply from {url}: {json}",);
        }

        Ok(res)
    }

    /// Try the best to send a request to the leader.
    ///
    /// If the target node is not a leader, a `ForwardToLeader` error will be
    /// returned and this client will retry at most 3 times to contact the updated leader.
    async fn api_post_to_leader<Req, Resp, Err>(
        &mut self,
        uri: &str,
        req: Option<&Req>,
    ) -> Result<Resp, RPCError<TremorNodeId, TremorNode, Err>>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error
            + Serialize
            + DeserializeOwned
            + TryInto<ForwardToLeader<TremorNodeId, TremorNode>>
            + Clone,
    {
        // Retry at most 3 times to find a valid leader.
        let mut n_retry = 3;

        loop {
            let res: Result<Resp, RPCError<TremorNodeId, TremorNode, Err>> =
                self.api_req(uri, req).await;

            let rpc_err = match res {
                Ok(x) => return Ok(x),
                Err(rpc_err) => rpc_err,
            };

            if let RPCError::RemoteError(remote_err) = &rpc_err {
                let forward_err_res =
                    <Err as TryInto<ForwardToLeader<TremorNodeId, TremorNode>>>::try_into(
                        remote_err.source.clone(),
                    );

                if let Ok(ForwardToLeader {
                    leader_id: Some(leader_id),
                    leader_node: Some(leader_node),
                    ..
                }) = forward_err_res
                {
                    // Update target to the new leader.
                    {
                        let t = &mut self.leader;
                        let api_addr = leader_node.api_addr.clone();
                        *t = (leader_id, api_addr);
                    }

                    n_retry -= 1;
                    if n_retry > 0 {
                        continue;
                    }
                }
            }

            return Err(rpc_err);
        }
    }
}

// --- kv API
impl TremorClient {
    /// Submit a write request to the raft cluster.
    ///
    /// The request will be processed by raft protocol: it will be replicated to a quorum and then will be applied to
    /// state machine.
    ///
    /// The result of applying the request will be returned.
    pub async fn write(
        &mut self,
        req: &TremorSet,
    ) -> Result<
        ClientWriteResponse<TremorTypeConfig>,
        RPCError<TremorNodeId, TremorNode, ClientWriteError<TremorNodeId, TremorNode>>,
    > {
        self.api_post_to_leader("api/write", Some(req)).await
    }
    /// Read value by key, in an inconsistent mode.
    ///
    /// This method may return stale value because it does not force to read on a legal leader.
    pub async fn read(
        &mut self,
        req: &String,
    ) -> Result<Option<String>, RPCError<TremorNodeId, TremorNode, Infallible>> {
        self.api_req("api/read", Some(req)).await
    }

    /// Consistent Read value by key.
    ///
    /// This method MUST return consistent value or CheckIsLeaderError.
    pub async fn consistent_read(
        &mut self,
        req: &String,
    ) -> Result<
        Option<String>,
        RPCError<TremorNodeId, TremorNode, CheckIsLeaderError<TremorNodeId, TremorNode>>,
    > {
        self.api_post_to_leader("api/consistent_read", Some(req))
            .await
    }
}

// --- Application API
impl TremorClient {
    /// Submit a write request to the raft cluster.
    ///
    /// The request will be processed by raft protocol: it will be replicated to a quorum and then will be applied to
    /// state machine.
    ///
    /// The result of applying the request will be returned.
    pub async fn install(
        &mut self,
        req: &Vec<u8>,
    ) -> Result<
        Result<ClientWriteResponse<TremorTypeConfig>, InstallError>,
        RPCError<TremorNodeId, TremorNode, ClientWriteError<TremorNodeId, TremorNode>>,
    > {
        self.api_post_to_leader("api/apps", Some(req)).await
    }

    /// Submit a write request to the raft cluster.
    ///
    /// The request will be processed by raft protocol: it will be replicated to a quorum and then will be applied to
    /// state machine.
    ///
    /// The result of applying the request will be returned.
    pub async fn start(
        &mut self,
        app: &AppId,
        flow: &FlowId,
        instance: &InstanceId,
        config: HashMap<String, OwnedValue>,
    ) -> Result<
        Result<ClientWriteResponse<TremorTypeConfig>, StartError>,
        RPCError<TremorNodeId, TremorNode, ClientWriteError<TremorNodeId, TremorNode>>,
    > {
        let req = TremorStart {
            instance: instance.clone(),
            config,
        };
        self.api_post_to_leader(&format!("api/apps/{app}/flows/{flow}"), Some(&req))
            .await
    }

    /// Get the metrics about the cluster.
    ///
    /// Metrics contains various information about the cluster, such as current leader,
    /// membership config, replication status etc.
    /// See [`RaftMetrics`].
    pub async fn list(
        &mut self,
    ) -> Result<HashMap<String, AppState>, RPCError<TremorNodeId, TremorNode, Infallible>> {
        self.api_req("api/apps", None::<&()>).await
    }
}
// Cluster
impl TremorClient {
    // --- Cluster management API

    /// Initialize a cluster of only the node that receives this request.
    ///
    /// This is the first step to initialize a cluster.
    /// With a initialized cluster, new node can be added with [`write`].
    /// Then setup replication with [`add_learner`].
    /// Then make the new node a member with [`change_membership`].
    pub async fn init(
        &mut self,
    ) -> Result<(), RPCError<TremorNodeId, TremorNode, InitializeError<TremorNodeId, TremorNode>>>
    {
        self.api_req("cluster/init", Some(&Empty {})).await
    }

    /// Add a node as learner.
    ///
    /// The node to add has to exist, i.e., being added with `write(ExampleRequest::AddNode{})`
    pub async fn add_learner(
        &mut self,
        id: TremorNodeId,
        rpc: String,
        api: String,
    ) -> Result<
        AddLearnerResponse<TremorNodeId>,
        RPCError<TremorNodeId, TremorNode, AddLearnerError<TremorNodeId, TremorNode>>,
    > {
        self.api_post_to_leader("cluster/add-learner", Some(&AddLearner { id, rpc, api }))
            .await
    }

    /// Change membership to the specified set of nodes.
    ///
    /// All nodes in `req` have to be already added as learner with [`add_learner`],
    /// or an error [`LearnerNotFound`] will be returned.
    pub async fn change_membership(
        &mut self,
        req: &BTreeSet<TremorNodeId>,
    ) -> Result<
        ClientWriteResponse<TremorTypeConfig>,
        RPCError<TremorNodeId, TremorNode, ClientWriteError<TremorNodeId, TremorNode>>,
    > {
        self.api_post_to_leader("cluster/change-membership", Some(req))
            .await
    }

    /// Get the metrics about the cluster.
    ///
    /// Metrics contains various information about the cluster, such as current leader,
    /// membership config, replication status etc.
    /// See [`RaftMetrics`].
    pub async fn metrics(
        &mut self,
    ) -> Result<RaftMetrics<TremorNodeId, TremorNode>, RPCError<TremorNodeId, TremorNode, Infallible>>
    {
        self.api_req("cluster/metrics", None::<&()>).await
    }
}

fn maybe_id<T: ToString>(id: Option<T>) -> String {
    match id {
        Some(id) => id.to_string(),
        None => "-".to_string(),
    }
}
pub fn print_metrics(metrics: RaftMetrics<TremorNodeId, TremorNode>) {
    println!(
        r#"Node:
    Id: {}
    State: {:?}

Cluster:
    Term: {}
    Last log: {}
    Last applied: {}
    Leader: {}"#,
        metrics.id,
        metrics.state,
        metrics.current_term,
        maybe_id(metrics.last_log_index),
        maybe_id(metrics.last_applied),
        maybe_id(metrics.current_leader),
    );
    let membership = &metrics.membership_config;
    println!(
        r#"
Membership:
    Log: {}
    Nodes:"#,
        maybe_id(membership.log_id),
    );
    for (id, n) in membership.nodes() {
        println!("     - {id}: {n}")
    }
    println!(
        r#"
Snapshot:
    {:?}

Replication:
    {}"#,
        metrics.snapshot,
        metrics
            .replication
            .as_ref()
            .map(|x| x.summary())
            .unwrap_or_default(),
    );
}
