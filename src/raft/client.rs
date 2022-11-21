use super::{
    network::{api::AppState, management::AddLearner, APIError, APIResult},
    store::{
        AppId, FlowId, InstanceId, TremorInstanceState, TremorResponse, TremorSet, TremorStart,
    },
    NodeId, TremorNode,
};
use openraft::{
    error::ForwardToLeader,
    raft::{AddLearnerResponse, ClientWriteResponse},
    RaftMetrics,
};
use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use simd_json::OwnedValue;
use std::{
    collections::{BTreeSet, HashMap},
    fmt::Display,
};

type WriteResponse = ClientWriteResponse<TremorResponse>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum Method {
    Get,
    Post,
    Put,
    Delete,
}

impl Display for Method {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Method::Get => write!(f, "GET"),
            Method::Post => write!(f, "POST"),
            Method::Put => write!(f, "PUT"),
            Method::Delete => write!(f, "DELETE"),
        }
    }
}

pub struct Tremor<const RETRIES: usize = 3> {
    /// The leader node to send request to.
    ///
    /// All traffic should be sent to the leader in a cluster.
    pub leader: (NodeId, String),

    pub inner: Client,
}

impl<const RETRIES: usize> Tremor<RETRIES> {
    /// Create a client with a leader node id and a node manager to get node address by node id.
    #[must_use]
    pub fn new(leader_id: NodeId, leader_addr: String) -> Self {
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
    async fn api_req<Req, Resp>(
        &self,
        uri: &str,
        method: Method,
        req: Option<&Req>,
    ) -> APIResult<Resp>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
    {
        let (_leader_id, target_url) = {
            let t = &self.leader;
            let target_addr = &t.1;
            (t.0, format!("http://{}/{}", target_addr, uri))
        };

        debug!(">>> client send {method} request to {target_url}");
        let resp = match method {
            Method::Get => self.inner.get(&target_url).send().await,
            Method::Post => {
                if let Some(req) = req {
                    self.inner.post(&target_url).json(req).send().await
                } else {
                    self.inner.post(&target_url).send().await
                }
            }
            Method::Put => {
                if let Some(req) = req {
                    self.inner.put(&target_url).json(req).send().await
                } else {
                    self.inner.put(&target_url).send().await
                }
            }
            Method::Delete => self.inner.delete(&target_url).send().await,
        }?;

        let result: Resp = resp.json().await?;
        if let Ok(json) = serde_json::to_string_pretty(&result) {
            debug!("<<< client recv reply from {target_url}: {json}",);
        }

        Ok(result)
    }

    /// Try the best to send a request to the leader.
    ///
    /// If the target node is not a leader, a `ForwardToLeader` error will be
    /// returned and this client will retry at most 3 times to contact the updated leader.
    async fn api_send_to_leader<Req, Resp>(
        &mut self,
        uri: &str,
        method: Method,
        req: Option<&Req>,
    ) -> APIResult<Resp>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
    {
        // Retry a few times, default to 3
        let mut n_retry = RETRIES;

        loop {
            let result: APIResult<Resp> = self.api_req(uri, method, req).await;

            let rpc_err = match result {
                Ok(x) => return Ok(x),
                Err(APIError::ForwardToLeader(ForwardToLeader {
                    leader_id: Some(leader_id),
                    leader_node: Some(leader_node),
                    ..
                })) => {
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
                Err(e) => return Err(e),
            };
        }
    }
}

// --- kv API
impl Tremor {
    /// Submit a write request to the raft cluster.
    ///
    /// The request will be processed by raft protocol: it will be replicated to a quorum and then will be applied to
    /// state machine.
    ///
    /// The result of applying the request will be returned.
    ///
    /// # Errors
    /// if the api call fails
    pub async fn write(&mut self, req: &TremorSet) -> APIResult<WriteResponse> {
        self.api_send_to_leader("api/write", Method::Post, Some(req))
            .await
    }
    /// Read value by key, in an inconsistent mode.
    ///
    /// This method may return stale value because it does not force to read on a legal leader.
    ///
    /// # Errors
    /// if the api call fails
    pub async fn read(&mut self, req: &String) -> APIResult<Option<String>> {
        self.api_req("api/read", Method::Post, Some(req)).await
    }

    /// Consistent Read value by key.
    ///
    /// This method MUST return consistent value or `CheckIsLeaderError`.
    ///
    /// # Errors
    /// if the api call fails
    pub async fn consistent_read(&mut self, req: &String) -> APIResult<Option<String>> {
        self.api_send_to_leader("api/consistent_read", Method::Post, Some(req))
            .await
    }
}

// --- Application API
impl Tremor {
    /// Submit a write request to the raft cluster.
    ///
    /// The request will be processed by raft protocol: it will be replicated to a quorum and then will be applied to
    /// state machine.
    ///
    /// The result of applying the request will be returned.
    ///
    /// # Errors
    /// if the api call fails
    pub async fn install(&mut self, req: &Vec<u8>) -> APIResult<WriteResponse> {
        self.api_send_to_leader("api/apps", Method::Post, Some(req))
            .await
    }

    /// Submit a write request to the raft cluster.
    ///
    /// The request will be processed by raft protocol: it will be replicated to a quorum and then will be applied to
    /// state machine.
    ///
    /// The result of applying the request will be returned.
    ///
    /// # Errors
    /// if the api call fails
    pub async fn start(
        &mut self,
        app: &AppId,
        flow: &FlowId,
        instance: &InstanceId,
        config: HashMap<String, OwnedValue>,
        running: bool,
    ) -> APIResult<WriteResponse> {
        let req = TremorStart {
            instance: instance.clone(),
            config,
            running,
        };
        self.api_send_to_leader(
            &format!("api/apps/{app}/flows/{flow}"),
            Method::Post,
            Some(&req),
        )
        .await
    }

    /// Submit a write request to the raft cluster.
    ///
    /// The request will be processed by raft protocol: it will be replicated to a quorum and then will be applied to
    /// state machine.
    ///
    /// The result of applying the request will be returned.
    ///
    /// # Errors
    /// if the api call fails
    pub async fn change_instance_state(
        &mut self,
        app: &AppId,
        instance: &InstanceId,
        state: TremorInstanceState,
    ) -> APIResult<WriteResponse> {
        self.api_send_to_leader(
            &format!("api/apps/{app}/instances/{instance}"),
            Method::Post,
            Some(&state),
        )
        .await
    }

    /// Submit a write request to the raft cluster.
    ///
    /// The request will be processed by raft protocol: it will be replicated to a quorum and then will be applied to
    /// state machine.
    ///
    /// The result of applying the request will be returned.
    ///
    /// # Errors
    /// if the api call fails
    pub async fn stop_instance(
        &mut self,
        app: &AppId,
        instance: &InstanceId,
    ) -> APIResult<WriteResponse> {
        self.api_send_to_leader(
            &format!("api/apps/{app}/instances/{instance}"),
            Method::Delete,
            None::<&()>,
        )
        .await
    }

    /// Submit a write request to the raft cluster.
    ///
    /// The request will be processed by raft protocol: it will be replicated to a quorum and then will be applied to
    /// state machine.
    ///
    /// The result of applying the request will be returned.
    ///
    /// # Errors
    /// if the api call fails
    pub async fn uninstall_app(&mut self, app: &AppId) -> APIResult<WriteResponse> {
        self.api_send_to_leader(&format!("api/apps/{app}"), Method::Delete, None::<&()>)
            .await
    }

    /// Get the metrics about the cluster.
    ///
    /// Metrics contains various information about the cluster, such as current leader,
    /// membership config, replication status etc.
    /// See [`RaftMetrics`].
    ///
    /// # Errors
    /// if the api call fails
    pub async fn list(&mut self) -> APIResult<HashMap<String, AppState>> {
        self.api_req("api/apps", Method::Get, None::<&()>).await
    }
}
// Cluster
impl Tremor {
    // --- Cluster management API

    /// Initialize a cluster of only the node that receives this request.
    ///
    /// This is the first step to initialize a cluster.
    /// With a initialized cluster, new node can be added with [`write`].
    /// Then setup replication with [`add_learner`].
    /// Then make the new node a member with [`change_membership`].
    ///
    /// # Errors
    /// if the api call fails
    pub async fn init(&mut self) -> APIResult<()> {
        self.api_req("cluster/init", Method::Post, None::<&()>)
            .await
    }

    /// Add a node as learner.
    ///
    /// The node to add has to exist, i.e., being added with `write(ExampleRequest::AddNode{})`
    ///
    /// # Errors
    /// if the api call fails
    pub async fn add_learner(
        &mut self,
        id: NodeId,
        rpc: String,
        api: String,
    ) -> APIResult<AddLearnerResponse<NodeId>> {
        self.api_send_to_leader(
            "cluster/learners",
            Method::Post,
            Some(&AddLearner { id, rpc, api }),
        )
        .await
    }

    /// Add a node as learner.
    ///
    /// The node to add has to exist, i.e., being added with `write(ExampleRequest::AddNode{})`
    ///
    /// # Errors
    /// if the api call fails
    pub async fn remove_learner(&mut self, id: NodeId) -> APIResult<AddLearnerResponse<NodeId>> {
        self.api_send_to_leader("cluster/learners", Method::Post, Some(&id))
            .await
    }

    /// Change membership to the specified set of nodes.
    ///
    /// All nodes in `req` have to be already added as learner with [`add_learner`],
    /// or an error [`LearnerNotFound`] will be returned.
    ///
    /// # Errors
    /// if the api call fails
    pub async fn change_membership(&mut self, req: &BTreeSet<NodeId>) -> APIResult<WriteResponse> {
        self.api_send_to_leader("cluster/change-membership", Method::Post, Some(req))
            .await
    }

    /// Get the metrics about the cluster.
    ///
    /// Metrics contains various information about the cluster, such as current leader,
    /// membership config, replication status etc.
    /// See [`RaftMetrics`].
    ///
    /// # Errors
    /// if the api call fails
    pub async fn metrics(&mut self) -> APIResult<RaftMetrics<NodeId, TremorNode>> {
        self.api_req("cluster/metrics", Method::Get, None::<&()>)
            .await
    }
}

fn maybe_id<T: ToString>(id: Option<T>) -> String {
    match id {
        Some(id) => id.to_string(),
        None => "-".to_string(),
    }
}
pub fn print_metrics(metrics: &RaftMetrics) {
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
        membership.log_id,
    );
    for (id, n) in membership.nodes() {
        println!("     - {id}: {n}");
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
            .map(openraft::MessageSummary::summary)
            .unwrap_or_default(),
    );
}
