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

//! Tremor Rest API Client
use crate::errors::Result;
use crate::raft::{
    api::apps::AppState,
    node::Addr,
    store::{
        AppId, FlowId, InstanceId, TremorInstanceState, TremorResponse, TremorSet, TremorStart,
    },
    NodeId,
};
use openraft::{LogId, RaftMetrics};
use reqwest::Method;
use reqwest::{redirect::Policy, Client};
use serde::{de::DeserializeOwned, Serialize};
use simd_json::OwnedValue;
use std::collections::HashMap;

type ClientResult<T> = std::result::Result<T, Error>;

const DEFAULT_RETRIES: usize = 10;

pub struct Tremor {
    /// The endpoint to send requests to.
    pub endpoint: String,

    pub inner: Client,
}

impl Tremor {
    /// Create
    ///
    /// # Errors
    /// if the client cannot be created
    pub fn new<T: ToString + ?Sized>(endpoint: &T) -> Result<Self> {
        let inner = reqwest::Client::builder()
            .redirect(Policy::limited(DEFAULT_RETRIES))
            .build()?;
        Ok(Self {
            endpoint: endpoint.to_string(),
            inner,
        })
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
        method: reqwest::Method,
        req: Option<&Req>,
    ) -> ClientResult<Resp>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
    {
        let target_url = {
            let target_addr = &self.endpoint;
            format!("http://{}/v1/{}", target_addr, uri)
        };
        debug!(">>> client send {method} request to {target_url}");
        let mut request = self.inner.request(method, &target_url);
        if let Some(req) = req {
            request = request.json(req);
        }
        // FYI: 307 redirects are followed here with a default redirect limit of 10
        // if we target a non-leader, it will return with a 307 and redirect us to the leader
        let resp = request.send().await?;
        let result: Resp = resp.json().await?;
        if let Ok(json) = serde_json::to_string_pretty(&result) {
            debug!("<<< client recv reply from {target_url}: {json}",);
        }

        Ok(result)
    }
}

// --- kv API
impl Tremor {
    /// Submit a write request to the raft cluster.
    ///
    /// The request will be processed by raft protocol: it will be replicated to a quorum and then will be applied to
    /// state machine.
    ///
    /// The written value will be returned as a string.
    ///
    /// # Errors
    /// if the api call fails
    pub async fn write(&self, req: &TremorSet) -> ClientResult<String> {
        self.api_req::<TremorSet, String>("api/write", Method::POST, Some(req))
            .await
    }
    /// Read value by key, in an inconsistent mode.
    ///
    /// This method may return stale value because it does not force to read on a legal leader.
    ///
    /// # Errors
    /// if the api call fails
    pub async fn read(&self, req: &String) -> ClientResult<Option<String>> {
        let tremor_res: TremorResponse = self.api_req("api/read", Method::POST, Some(req)).await?;
        Ok(tremor_res.value)
    }

    /// Consistent Read value by key.
    ///
    /// This method MUST return consistent value or `CheckIsLeaderError`.
    ///
    /// # Errors
    /// if the api call fails
    pub async fn consistent_read(&self, req: &String) -> ClientResult<Option<String>> {
        let tremor_res: TremorResponse = self
            .api_req("api/consistent_read", Method::POST, Some(req))
            .await?;
        Ok(tremor_res.value)
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
    pub async fn install(&self, req: &Vec<u8>) -> ClientResult<AppId> {
        self.api_req::<Vec<u8>, AppId>("api/apps", Method::POST, Some(req))
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
    pub async fn uninstall_app(&self, app: &AppId) -> ClientResult<AppId> {
        self.api_req::<(), AppId>(&format!("api/apps/{app}"), Method::DELETE, None)
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
        &self,
        app: &AppId,
        flow: &FlowId,
        instance: &InstanceId,
        config: HashMap<String, OwnedValue>,
        running: bool,
    ) -> ClientResult<InstanceId> {
        let req = TremorStart {
            instance: instance.clone(),
            config,
            running,
        };
        self.api_req::<TremorStart, InstanceId>(
            &format!("api/apps/{app}/flows/{flow}"),
            Method::POST,
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
        &self,
        app: &AppId,
        instance: &InstanceId,
        state: TremorInstanceState,
    ) -> ClientResult<InstanceId> {
        self.api_req(
            &format!("api/apps/{app}/instances/{instance}"),
            Method::POST,
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
        &self,
        app: &AppId,
        instance: &InstanceId,
    ) -> ClientResult<InstanceId> {
        self.api_req(
            &format!("api/apps/{app}/instances/{instance}"),
            Method::DELETE,
            None::<&()>,
        )
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
    pub async fn list(&self) -> ClientResult<HashMap<AppId, AppState>> {
        self.api_req("api/apps", Method::GET, None::<&()>).await
    }
}
// Cluster
impl Tremor {
    // --- Cluster management API

    /// Make the given node known to the cluster and assign it a unique `node_id`
    /// # Errors
    /// If the api call fails
    pub async fn add_node(&self, addr: &Addr) -> ClientResult<NodeId> {
        self.api_req("cluster/nodes", Method::POST, Some(addr))
            .await
    }

    /// Remove a node from the cluster
    ///
    /// After this call a node is not reachable anymore for all nodes still participating in the cluster
    /// # Errors
    /// if the api call fails
    pub async fn remove_node(&self, node_id: &NodeId) -> ClientResult<()> {
        self.api_req(
            &format!("cluster/nodes/{node_id}"),
            Method::DELETE,
            None::<&()>,
        )
        .await
    }

    /// Add a node as learner.
    ///
    /// If the node has never been added to the cluster before, its address will be published in the cluster state
    /// so that all other nodes can reach it.
    ///
    /// # Errors
    /// if the api call fails e.g. because the node is already a learner
    pub async fn add_learner(&self, node_id: &NodeId) -> ClientResult<Option<LogId>> {
        self.api_req(
            &format!("cluster/learners/{node_id}"),
            Method::PUT,
            None::<&()>,
        )
        .await
    }

    /// Add a node as learner.
    ///
    /// The node to add has to exist, i.e., being added with `write(ExampleRequest::AddNode{})`
    ///
    /// # Errors
    /// if the api call fails
    pub async fn remove_learner(&self, id: &NodeId) -> ClientResult<NodeId> {
        self.api_req::<(), NodeId>(&format!("cluster/learners/{id}"), Method::POST, None)
            .await
    }

    /// Promote node with `node_id` from learner to voter.
    ///
    /// The node with `node_id` has to be already added as learner with [`add_learner`],
    /// or an error [`LearnerNotFound`] will be returned.
    ///
    /// # Errors
    /// if the api call fails
    pub async fn promote_voter(&self, id: &NodeId) -> ClientResult<Option<NodeId>> {
        self.api_req::<(), Option<NodeId>>(&format!("cluster/voters/{id}"), Method::PUT, None)
            .await
    }

    /// Demote node with `node_id` from voter back to learner.
    ///
    /// All nodes in `req` have to be already added as learner with [`add_learner`],
    /// or an error [`LearnerNotFound`] will be returned.
    ///
    /// # Errors
    /// if the api call fails
    pub async fn demote_voter(&self, id: &NodeId) -> ClientResult<Option<NodeId>> {
        self.api_req::<(), Option<NodeId>>(&format!("cluster/voters/{id}"), Method::PUT, None)
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
    pub async fn metrics(&self) -> ClientResult<RaftMetrics> {
        self.api_req::<(), RaftMetrics>("cluster/metrics", Method::GET, None)
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
    let log_id = membership.log_id;
    if let Some(config) = membership.membership.get_configs().last() {
        println!(
            r#"
Membership:
    Log: {}
    Nodes:"#,
            log_id,
        );
        for id in config {
            println!("     - {id}");
        }
    }

    println!(
        r#"
Snapshot:
    {:?}
"#,
        metrics.snapshot,
    );
}

#[derive(Debug)]
pub enum Error {
    HTTP(reqwest::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HTTP(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Self::HTTP(e)
    }
}
