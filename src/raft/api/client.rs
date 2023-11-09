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
    store::{TremorInstanceState, TremorStart},
    NodeId,
};
use halfbrown::HashMap;
use openraft::{LogId, RaftMetrics};
use reqwest::Method;
use reqwest::{redirect::Policy, Client};
use serde::{de::DeserializeOwned, Serialize};
use simd_json::OwnedValue;
use tremor_common::alias;

use super::kv::KVSet;

type ClientResult<T> = std::result::Result<T, Error>;

const DEFAULT_RETRIES: usize = 10;

/// A client for the raft cluster.
pub struct Tremor {
    /// The endpoint to send requests to.
    pub endpoint: String,

    pub(crate) inner: Client,
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
        Req: Serialize + 'static + ?Sized,
        Resp: Serialize + DeserializeOwned,
    {
        let target_url = {
            let target_addr = &self.endpoint;
            format!("http://{target_addr}/v1/{uri}")
        };
        debug!(">>> client send {method} request to {target_url}");
        let mut request = self.inner.request(method, &target_url);
        if let Some(req) = req {
            request = request.json(req);
        }
        // FYI: 307 redirects are followed here with a default redirect limit of 10
        // if we target a non-leader, it will return with a 307 and redirect us to the leader
        let resp = request.send().await?;
        if resp.status().is_success() {
            let result: Resp = resp.json().await?;
            if log::log_enabled!(log::Level::Debug) {
                if let Ok(json) = serde_json::to_string_pretty(&result) {
                    debug!("<<< client recv reply from {target_url}: {json}",);
                }
            }
            Ok(result)
        } else if let Err(err) = resp.error_for_status_ref() {
            error!(
                "Received {} with body: {}",
                resp.status(),
                resp.text().await?
            );
            Err(Error::HTTP(err))
        } else {
            Err(Error::Heisenerror)
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
    /// The written value will be returned as a string.
    ///
    /// # Errors
    /// if the api call fails
    pub async fn write(&self, req: &KVSet) -> ClientResult<OwnedValue> {
        self.api_req::<KVSet, OwnedValue>("api/kv/write", Method::POST, Some(req))
            .await
    }
    /// Read value by key, in an inconsistent mode.
    ///
    /// This method may return stale value because it does not force to read on a legal leader.
    ///
    /// # Errors
    /// if the api call fails
    pub async fn read(&self, req: &str) -> ClientResult<OwnedValue> {
        self.api_req("api/kv/read", Method::POST, Some(req)).await
    }

    /// Consistent Read value by key.
    ///
    /// This method MUST return consistent value or `CheckIsLeaderError`.
    ///
    /// # Errors
    /// if the api call fails
    pub async fn consistent_read(&self, req: &str) -> ClientResult<OwnedValue> {
        self.api_req("api/kv/consistent_read", Method::POST, Some(req))
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
    pub async fn install(&self, req: &Vec<u8>) -> ClientResult<alias::App> {
        self.api_req::<Vec<u8>, alias::App>("api/apps", Method::POST, Some(req))
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
    pub async fn uninstall_app(&self, app: &alias::App) -> ClientResult<alias::App> {
        self.api_req::<(), alias::App>(&format!("api/apps/{app}"), Method::DELETE, None)
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
        flow: &alias::FlowDefinition,
        instance: &alias::Flow,
        config: std::collections::HashMap<String, OwnedValue>,
        running: bool,
        single_node: bool,
    ) -> ClientResult<alias::Flow> {
        let req = TremorStart {
            instance: instance.clone(),
            config,
            running,
            single_node,
        };
        self.api_req::<TremorStart, alias::Flow>(
            &format!("api/apps/{}/flows/{flow}", instance.app_id()),
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
        instance: &alias::Flow,
        state: TremorInstanceState,
    ) -> ClientResult<alias::Flow> {
        self.api_req(
            &format!(
                "api/apps/{}/instances/{}",
                instance.app_id(),
                instance.instance_id()
            ),
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
    pub async fn stop_instance(&self, instance: &alias::Flow) -> ClientResult<alias::Flow> {
        self.api_req(
            &format!(
                "api/apps/{}/instances/{}",
                instance.app_id(),
                instance.instance_id()
            ),
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
    pub async fn list(&self) -> ClientResult<HashMap<alias::App, AppState>> {
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

    /// Get all the nodes with their id and address that are currently known to the cluster
    ///
    /// # Errors
    /// if the api call fails
    pub async fn get_nodes(&self) -> ClientResult<HashMap<NodeId, Addr>> {
        self.api_req("cluster/nodes", Method::GET, None::<&()>)
            .await
    }

    /// Add a node as learner.
    ///
    /// If the node has never been added to the cluster before, its address will be published in the cluster state
    /// so that all other nodes can reach it.
    ///
    /// # Errors
    /// if the api call fails e.g. because the node is already a learner
    pub async fn add_learner(&self, node_id: &NodeId) -> ClientResult<Option<LogId<NodeId>>> {
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
    pub async fn remove_learner(&self, id: &NodeId) -> ClientResult<()> {
        self.api_req::<(), ()>(&format!("cluster/learners/{id}"), Method::DELETE, None)
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
        self.api_req::<(), Option<NodeId>>(&format!("cluster/voters/{id}"), Method::DELETE, None)
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
    pub async fn metrics(&self) -> ClientResult<RaftMetrics<NodeId, Addr>> {
        self.api_req::<(), RaftMetrics<NodeId, Addr>>("cluster/metrics", Method::GET, None)
            .await
    }
}

fn maybe_id<T: ToString>(id: Option<T>) -> String {
    match id {
        Some(id) => id.to_string(),
        None => "-".to_string(),
    }
}
/// Print the metrics to stdout
pub fn print_metrics(metrics: &RaftMetrics<NodeId, Addr>) {
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
    let log_id = membership.log_id().unwrap_or_default();
    if let Some(config) = membership.membership().get_joint_config().last() {
        println!(
            r#"
Membership:
    Log: {log_id}
    Nodes:"#
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

/// Errors that can happen when calling the raft api
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// HTTP error
    #[error(transparent)]
    HTTP(#[from] reqwest::Error),
    /// Heisenerror, not error nor success
    #[error("Heisenerror, not error nor success")]
    Heisenerror,
    /// RuntimeError
    #[error(transparent)]
    Other(#[from] crate::Error),
}

impl Error {
    /// Checks if the error is a not found error
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        match self {
            Self::HTTP(e) => e.status() == Some(reqwest::StatusCode::NOT_FOUND),
            _ => false,
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::BTreeMap, sync::Arc};

    use openraft::{LeaderId, Membership, ServerState, StoredMembership, Vote};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn write() -> Result<()> {
        let mut server = mockito::Server::new();

        let tremor = Tremor::new(&server.host_with_port())?;
        let request = KVSet {
            key: "foo".to_string(),
            value: "bar".into(),
        };
        let mock = server
            .mock("POST", "/v1/api/kv/write")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body("42")
            .create();

        let res = tremor.write(&request).await?;
        assert_eq!(res, 42);
        mock.assert();

        Ok(())
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn read() -> Result<()> {
        let mut server = mockito::Server::new();

        let tremor = Tremor::new(&server.host_with_port())?;
        let request = "foo";
        let mock = server
            .mock("POST", "/v1/api/kv/read")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body("42")
            .create();

        let res = tremor.read(request).await?;
        assert_eq!(res, 42);
        mock.assert();

        Ok(())
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn consistent_read() -> Result<()> {
        let mut server = mockito::Server::new();

        let tremor = Tremor::new(&server.host_with_port())?;
        let request = "foo";
        let mock = server
            .mock("POST", "/v1/api/kv/consistent_read")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body("42")
            .create();

        let res = tremor.consistent_read(request).await?;
        assert_eq!(res, 42);
        mock.assert();

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn install() -> Result<()> {
        let mut server = mockito::Server::new();

        let tremor = Tremor::new(&server.host_with_port())?;
        let request = vec![1, 2, 3];
        let mock = server
            .mock("POST", "/v1/api/apps")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#""app""#)
            .create();

        let res = tremor.install(&request).await?;
        assert_eq!(res, "app".into());
        mock.assert();

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn uninstall_app() -> Result<()> {
        let mut server = mockito::Server::new();

        let tremor = Tremor::new(&server.host_with_port())?;
        let app = alias::App::new("app");
        let mock = server
            .mock("DELETE", "/v1/api/apps/app")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#""app""#)
            .create();

        let res = tremor.uninstall_app(&app).await?;
        assert_eq!(res, "app".into());
        mock.assert();

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn start() -> Result<()> {
        let mut server = mockito::Server::new();

        let tremor = Tremor::new(&server.host_with_port())?;
        let flow = "flow".into();
        let instance = alias::Flow::new("app", "instance");
        let mut config = std::collections::HashMap::new();
        config.insert("foo".to_string(), "bar".into());
        let mock = server
            .mock("POST", "/v1/api/apps/app/flows/flow")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&instance)?)
            .create();

        let res = tremor.start(&flow, &instance, config, true, false).await?;
        assert_eq!(res, instance);
        mock.assert();

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn change_instance_state() -> Result<()> {
        let mut server = mockito::Server::new();

        let tremor = Tremor::new(&server.host_with_port())?;
        let instance = alias::Flow::new("app", "instance");
        let mock = server
            .mock("POST", "/v1/api/apps/app/instances/instance")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&instance)?)
            .create();

        let res = tremor
            .change_instance_state(&instance, TremorInstanceState::Pause)
            .await?;
        assert_eq!(res, instance);
        mock.assert();

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stop_instance() -> Result<()> {
        let mut server = mockito::Server::new();

        let tremor = Tremor::new(&server.host_with_port())?;
        let instance = alias::Flow::new("app", "instance");
        let mock = server
            .mock("DELETE", "/v1/api/apps/app/instances/instance")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&instance)?)
            .create();

        let res = tremor.stop_instance(&instance).await?;
        assert_eq!(res, instance);
        mock.assert();

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list() -> Result<()> {
        let mut server = mockito::Server::new();

        let tremor = Tremor::new(&server.host_with_port())?;
        let mut apps = HashMap::new();
        apps.insert("app".into(), AppState::dummy());
        let mock = server
            .mock("GET", "/v1/api/apps")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&apps)?)
            .create();

        let res = tremor.list().await?;
        assert_eq!(res, apps);
        mock.assert();

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_node() -> Result<()> {
        let mut server = mockito::Server::new();

        let tremor = Tremor::new(&server.host_with_port())?;

        let addr = Addr::default();

        let mock = server
            .mock("POST", "/v1/cluster/nodes")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"42"#)
            .create();

        let res = tremor.add_node(&addr).await?;
        assert_eq!(res, 42);
        mock.assert();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_node() -> Result<()> {
        let mut server = mockito::Server::new();

        let tremor = Tremor::new(&server.host_with_port())?;

        let node_id = 42;

        let mock = server
            .mock("DELETE", "/v1/cluster/nodes/42")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body("null")
            .create();

        tremor.remove_node(&node_id).await?;
        mock.assert();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_nodes() -> Result<()> {
        let mut server = mockito::Server::new();

        let tremor = Tremor::new(&server.host_with_port())?;

        let mut nodes = HashMap::new();
        nodes.insert(42, Addr::default());

        let mock = server
            .mock("GET", "/v1/cluster/nodes")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&nodes)?)
            .create();

        let res = tremor.get_nodes().await?;
        assert_eq!(res, nodes);
        mock.assert();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_learner() -> Result<()> {
        let mut server = mockito::Server::new();

        let tremor = Tremor::new(&server.host_with_port())?;

        let node_id = 42;
        let log_id = LogId {
            leader_id: LeaderId {
                term: 23,
                node_id: 42,
            },
            index: 19,
        };

        let mock = server
            .mock("PUT", "/v1/cluster/learners/42")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&log_id)?)
            .create();

        let res = tremor.add_learner(&node_id).await?;
        assert_eq!(res, Some(log_id));
        mock.assert();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_learner() -> Result<()> {
        let mut server = mockito::Server::new();

        let tremor = Tremor::new(&server.host_with_port())?;

        let node_id = 42;

        let mock = server
            .mock("DELETE", "/v1/cluster/learners/42")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body("null")
            .create();

        tremor.remove_learner(&node_id).await?;

        mock.assert();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn promote_voter() -> Result<()> {
        let mut server = mockito::Server::new();

        let tremor = Tremor::new(&server.host_with_port())?;

        let node_id = 42;

        let mock = server
            .mock("PUT", "/v1/cluster/voters/42")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"42"#)
            .create();

        let res = tremor.promote_voter(&node_id).await?;
        assert_eq!(res, Some(42));
        mock.assert();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn demote_voter() -> Result<()> {
        let mut server = mockito::Server::new();

        let tremor = Tremor::new(&server.host_with_port())?;

        let node_id = 42;

        let mock = server
            .mock("DELETE", "/v1/cluster/voters/42")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"42"#)
            .create();

        let res = tremor.demote_voter(&node_id).await?;
        assert_eq!(res, Some(42));
        mock.assert();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn metrics() -> Result<()> {
        let mut server = mockito::Server::new();

        let tremor = Tremor::new(&server.host_with_port())?;

        let mut metrics = RaftMetrics {
            running_state: Ok(()),
            id: 42,
            current_term: 23,
            vote: Vote::new(42, 23),
            last_log_index: Some(19),
            last_applied: None,
            snapshot: None,
            purged: None,
            state: ServerState::Leader,
            current_leader: Some(42),
            membership_config: Arc::new(StoredMembership::new(
                Some(LogId {
                    leader_id: LeaderId {
                        term: 23,
                        node_id: 42,
                    },
                    index: 19,
                }),
                Membership::new(Vec::new(), BTreeMap::new()),
            )),
            replication: None,
        };
        metrics.id = 42;

        let mock = server
            .mock("GET", "/v1/cluster/metrics")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&metrics)?)
            .create();

        let res = tremor.metrics().await?;
        assert_eq!(res, metrics);
        mock.assert();
        Ok(())
    }
}
