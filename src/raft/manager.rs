// Copyright 2021, The Tremor Team
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

use std::{
    collections::{BTreeSet, HashMap},
    convert::Into,
};

use super::{
    api::apps::AppState,
    node::Addr,
    store::{StateApp, TremorSet},
    TremorRaftImpl,
};
use crate::raft::api::APIStoreReq;
use crate::raft::NodeId;
use crate::Result;
use crate::{
    channel::{bounded, oneshot, OneShotSender, Sender},
    connectors::prelude::Receiver,
};
use openraft::error::{CheckIsLeaderError, Fatal, ForwardToLeader, RaftError};
use simd_json::OwnedValue;
use tremor_common::alias;

#[derive(Clone, Debug)]
pub(crate) struct Cluster {
    node_id: NodeId,
    store: Sender<APIStoreReq>,
    cluster: Sender<IFRequest>,
}

type IsLeaderResult = std::result::Result<(), RaftError<u64, CheckIsLeaderError<u64, Addr>>>;
pub(crate) enum IFRequest {
    IsLeader(OneShotSender<IsLeaderResult>),
    SetKeyLocal(TremorSet, OneShotSender<Result<Vec<u8>>>),
}
async fn cluster_interface(raft: TremorRaftImpl, mut rx: Receiver<IFRequest>) {
    while let Some(msg) = rx.recv().await {
        match msg {
            IFRequest::IsLeader(tx) => {
                let res = raft.is_leader().await;
                if tx.send(res).is_err() {
                    error!("Error sending response to API");
                }
            }
            IFRequest::SetKeyLocal(set, tx) => {
                let res = match raft.client_write(set.into()).await {
                    Ok(v) => v.data.into_kv_value().map_err(anyhow::Error::from),
                    Err(e) => Err(e.into()),
                };
                if tx.send(res).is_err() {
                    error!("Error sending response to API");
                }
            }
        }
    }
}

impl Cluster {
    #[cfg(test)]
    pub(crate) fn dummy(store: Sender<APIStoreReq>, cluster: Sender<IFRequest>) -> Self {
        Cluster {
            node_id: 42,
            store,
            cluster,
        }
    }
    pub(crate) fn new(node_id: NodeId, store: Sender<APIStoreReq>, raft: TremorRaftImpl) -> Self {
        let (cluster, rx) = bounded(1042);
        tokio::spawn(cluster_interface(raft, rx));
        Cluster {
            node_id,
            store,
            cluster,
        }
    }
    pub(crate) fn id(&self) -> NodeId {
        self.node_id
    }

    // cluster
    pub(crate) async fn get_node(&self, node_id: u64) -> Result<Option<Addr>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetNode(node_id, tx);
        self.store.send(command).await?;
        Ok(rx.await?)
    }
    pub(crate) async fn get_nodes(&self) -> Result<HashMap<u64, Addr>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetNodes(tx);
        self.store.send(command).await?;
        Ok(rx.await?)
    }
    pub(crate) async fn get_node_id(&self, addr: Addr) -> Result<Option<u64>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetNodeId(addr, tx);
        self.store.send(command).await?;
        Ok(rx.await?)
    }

    pub(crate) async fn get_last_membership(&self) -> Result<BTreeSet<u64>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetLastMembership(tx);
        self.store.send(command).await?;
        Ok(rx.await?)
    }

    // apps
    pub(crate) async fn get_app_local(&self, app_id: alias::App) -> Result<Option<StateApp>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetApp(app_id, tx);
        self.store.send(command).await?;
        Ok(rx.await?)
    }

    pub(crate) async fn get_apps_local(&self) -> Result<HashMap<alias::App, AppState>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::GetApps(tx);
        self.store.send(command).await?;
        Ok(rx.await?)
    }

    pub(crate) async fn is_leader(&self) -> IsLeaderResult {
        let (tx, rx) = oneshot();
        let command = IFRequest::IsLeader(tx);
        self.cluster
            .send(command)
            .await
            .map_err(|_| RaftError::Fatal(Fatal::Stopped))?;
        rx.await.map_err(|_| RaftError::Fatal(Fatal::Stopped))?
    }

    // kv
    pub(crate) async fn kv_set(&self, key: String, mut value: Vec<u8>) -> Result<Vec<u8>> {
        match self.is_leader().await {
            Ok(()) => self.kv_set_local(key, value).await,
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                leader_node: Some(n),
                ..
            }))) => {
                let client = crate::raft::api::client::Tremor::new(n.api())?;
                // TODO: there should be a better way to forward then the client
                Ok(simd_json::to_vec(
                    &client
                        .write(&crate::raft::api::kv::KVSet {
                            key,
                            value: simd_json::from_slice(&mut value)?,
                        })
                        .await?,
                )?)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn kv_set_local(&self, key: String, value: Vec<u8>) -> Result<Vec<u8>> {
        let (tx, rx) = oneshot();
        let command = IFRequest::SetKeyLocal(TremorSet { key, value }, tx);
        self.cluster.send(command).await?;
        rx.await?
    }

    pub(crate) async fn kv_get(&self, key: String) -> Result<Option<OwnedValue>> {
        match self.is_leader().await {
            Ok(()) => self.kv_get_local(key).await,
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                leader_node: Some(n),
                ..
            }))) => {
                let client = crate::raft::api::client::Tremor::new(n.api())?;
                let res = client.consistent_read(&key).await;
                match res {
                    Ok(v) => Ok(Some(v)),
                    Err(e) if e.is_not_found() => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn kv_get_local(&self, key: String) -> Result<Option<OwnedValue>> {
        let (tx, rx) = oneshot();
        let command = APIStoreReq::KVGet(key, tx);
        self.store.send(command).await?;
        Ok(rx
            .await?
            .map(|mut v| simd_json::from_slice(&mut v))
            .transpose()?)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::channel::{bounded, oneshot};

    #[tokio::test(flavor = "multi_thread")]
    async fn send_store() -> Result<()> {
        let (result_tx, result_rx) = oneshot();
        let (store, mut store_rx) = bounded(8);
        let (cluster, _) = bounded(8);

        tokio::spawn(async move {
            match store_rx.recv().await.expect("rcv") {
                APIStoreReq::GetNodeId(_, result_tx) => {
                    let _ = result_tx.send(Some(42));
                }
                _ => panic!("wrong request"),
            };
        });

        let iface = Cluster {
            node_id: 0,
            store,
            cluster,
        };
        iface
            .store
            .send(APIStoreReq::GetNodeId(Addr::default(), result_tx))
            .await?;
        assert_eq!(result_rx.await?, Some(42));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_cluster() -> Result<()> {
        let (result_tx, result_rx) = oneshot();
        let (store, _) = bounded(8);
        let (cluster, mut cluster_rx) = bounded(8);

        tokio::spawn(async move {
            match cluster_rx.recv().await.expect("rcv") {
                IFRequest::IsLeader(result_tx) => {
                    let _ = result_tx.send(Ok(()));
                }
                IFRequest::SetKeyLocal(_, _) => panic!("wrong request"),
            };
        });

        let iface = Cluster {
            node_id: 0,
            store,
            cluster,
        };
        iface.cluster.send(IFRequest::IsLeader(result_tx)).await?;
        assert!(result_rx.await?.is_ok());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_node() -> Result<()> {
        let (store, mut store_rx) = bounded(8);
        let (cluster, _) = bounded(8);

        tokio::spawn(async move {
            match store_rx.recv().await.expect("rcv") {
                APIStoreReq::GetNode(node_id, result_tx) => {
                    assert_eq!(node_id, 42);
                    let _ = result_tx.send(Some(Addr::default()));
                }
                _ => panic!("wrong request"),
            };
        });

        let iface = Cluster {
            node_id: 0,
            store,
            cluster,
        };
        assert_eq!(iface.get_node(42).await?, Some(Addr::default()));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_nodes() -> Result<()> {
        let (store, mut store_rx) = bounded(8);
        let (cluster, _) = bounded(8);

        tokio::spawn(async move {
            match store_rx.recv().await.expect("rcv") {
                APIStoreReq::GetNodes(result_tx) => {
                    let mut nodes = HashMap::new();
                    nodes.insert(42, Addr::default());
                    let _ = result_tx.send(nodes);
                }
                _ => panic!("wrong request"),
            };
        });

        let iface = Cluster {
            node_id: 0,
            store,
            cluster,
        };
        let mut nodes = HashMap::new();
        nodes.insert(42, Addr::default());
        assert_eq!(iface.get_nodes().await?, nodes);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_node_id() -> Result<()> {
        let (store, mut store_rx) = bounded(8);
        let (cluster, _) = bounded(8);

        tokio::spawn(async move {
            match store_rx.recv().await.expect("rcv") {
                APIStoreReq::GetNodeId(addr, result_tx) => {
                    assert_eq!(addr, Addr::default());
                    let _ = result_tx.send(Some(42));
                }
                _ => panic!("wrong request"),
            };
        });

        let iface = Cluster {
            node_id: 0,
            store,
            cluster,
        };
        assert_eq!(iface.get_node_id(Addr::default()).await?, Some(42));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_last_membership() -> Result<()> {
        let (store, mut store_rx) = bounded(8);
        let (cluster, _) = bounded(8);

        tokio::spawn(async move {
            match store_rx.recv().await.expect("rcv") {
                APIStoreReq::GetLastMembership(result_tx) => {
                    let _ = result_tx.send(BTreeSet::new());
                }
                _ => panic!("wrong request"),
            };
        });

        let iface = Cluster {
            node_id: 0,
            store,
            cluster,
        };
        assert_eq!(iface.get_last_membership().await?, BTreeSet::new());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_app_local() -> Result<()> {
        let (store, mut store_rx) = bounded(8);
        let (cluster, _) = bounded(8);

        tokio::spawn(async move {
            match store_rx.recv().await.expect("rcv") {
                APIStoreReq::GetApp(_, result_tx) => {
                    let _ = result_tx.send(Some(StateApp::dummy()));
                }
                _ => panic!("wrong request"),
            };
        });

        let iface = Cluster {
            node_id: 0,
            store,
            cluster,
        };

        assert_eq!(
            iface.get_app_local("app".into()).await?,
            Some(StateApp::dummy())
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_apps_local() -> Result<()> {
        let (store, mut store_rx) = bounded(8);
        let (cluster, _) = bounded(8);

        tokio::spawn(async move {
            match store_rx.recv().await.expect("rcv") {
                APIStoreReq::GetApps(result_tx) => {
                    let mut apps: HashMap<alias::App, _> = HashMap::new();
                    apps.insert("app".into(), AppState::dummy());
                    let _ = result_tx.send(apps);
                }
                _ => panic!("wrong request"),
            };
        });

        let iface = Cluster {
            node_id: 0,
            store,
            cluster,
        };

        let mut apps: HashMap<alias::App, _> = HashMap::new();
        apps.insert("app".into(), AppState::dummy());
        assert_eq!(iface.get_apps_local().await?, apps);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn is_leader() -> Result<()> {
        let (store, _) = bounded(8);
        let (cluster, mut cluster_rx) = bounded(8);

        tokio::spawn(async move {
            match cluster_rx.recv().await.expect("rcv") {
                IFRequest::IsLeader(result_tx) => {
                    let _ = result_tx.send(Ok(()));
                }
                IFRequest::SetKeyLocal(_, _) => panic!("wrong request"),
            };
        });

        let iface = Cluster {
            node_id: 0,
            store,
            cluster,
        };
        assert!(iface.is_leader().await.is_ok());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn kv_set_local() -> Result<()> {
        let (store, _) = bounded(8);
        let (cluster, mut cluster_rx) = bounded(8);

        tokio::spawn(async move {
            match cluster_rx.recv().await.expect("rcv") {
                IFRequest::SetKeyLocal(set, result_tx) => {
                    let _ = result_tx.send(Ok(set.value));
                }
                IFRequest::IsLeader(_) => panic!("wrong request"),
            };
        });

        let iface = Cluster {
            node_id: 0,
            store,
            cluster,
        };
        assert_eq!(
            iface.kv_set_local("key".to_string(), vec![1, 2, 3]).await?,
            vec![1, 2, 3]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn kv_get_local() -> Result<()> {
        let (store, mut store_rx) = bounded(8);
        let (cluster, _) = bounded(8);

        tokio::spawn(async move {
            match store_rx.recv().await.expect("rcv") {
                APIStoreReq::KVGet(_key, result_tx) => {
                    let _ = result_tx.send(Some(b"42".to_vec()));
                }
                _ => panic!("wrong request"),
            };
        });

        let iface = Cluster {
            node_id: 0,
            store,
            cluster,
        };
        assert_eq!(
            iface.kv_get_local("key".to_string()).await?,
            Some(simd_json::json!(42))
        );
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn kv_set_on_leader() -> Result<()> {
        let (store, _) = bounded(8);
        let (cluster, mut cluster_rx) = bounded(8);

        tokio::spawn(async move {
            match cluster_rx.recv().await.expect("rcv") {
                IFRequest::IsLeader(result_tx) => {
                    let _ = result_tx.send(Ok(()));
                }
                IFRequest::SetKeyLocal(_, _) => panic!("wrong request"),
            };
            match cluster_rx.recv().await.expect("rcv") {
                IFRequest::SetKeyLocal(set, result_tx) => {
                    let _ = result_tx.send(Ok(set.value));
                }
                IFRequest::IsLeader(_) => panic!("wrong request"),
            };
        });

        let iface = Cluster {
            node_id: 0,
            store,
            cluster,
        };
        assert_eq!(
            iface.kv_set("key".to_string(), vec![1, 2, 3]).await?,
            vec![1, 2, 3]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn kv_get_on_leader() -> Result<()> {
        let (store, mut store_rx) = bounded(8);
        let (cluster, mut cluster_rx) = bounded(8);

        tokio::spawn(async move {
            match cluster_rx.recv().await.expect("rcv") {
                IFRequest::IsLeader(result_tx) => {
                    let _ = result_tx.send(Ok(()));
                }
                IFRequest::SetKeyLocal(_, _) => panic!("wrong request"),
            };
            match store_rx.recv().await.expect("rcv") {
                APIStoreReq::KVGet(_key, result_tx) => {
                    let _ = result_tx.send(Some(b"42".to_vec()));
                }
                _ => panic!("wrong request"),
            };
        });

        let iface = Cluster {
            node_id: 0,
            store,
            cluster,
        };
        assert_eq!(
            iface.kv_get("key".to_string()).await?,
            Some(simd_json::json!(42))
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn kv_set_no_leader() -> Result<()> {
        let (store, _) = bounded(8);
        let (cluster, mut cluster_rx) = bounded(8);

        tokio::spawn(async move {
            match cluster_rx.recv().await.expect("rcv") {
                IFRequest::IsLeader(result_tx) => {
                    let _ = result_tx.send(Err(RaftError::APIError(
                        CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                            leader_node: None,
                            leader_id: None,
                        }),
                    )));
                }
                IFRequest::SetKeyLocal(_, _) => panic!("wrong request"),
            };
        });

        let iface = Cluster {
            node_id: 0,
            store,
            cluster,
        };
        assert!(iface
            .kv_set("key".to_string(), vec![1, 2, 3])
            .await
            .is_err(),);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn kv_get_no_leader() -> Result<()> {
        let (store, _) = bounded(8);
        let (cluster, mut cluster_rx) = bounded(8);

        tokio::spawn(async move {
            match cluster_rx.recv().await.expect("rcv") {
                IFRequest::IsLeader(result_tx) => {
                    let _ = result_tx.send(Err(RaftError::APIError(
                        CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                            leader_node: None,
                            leader_id: None,
                        }),
                    )));
                }
                IFRequest::SetKeyLocal(_, _) => panic!("wrong request"),
            };
        });

        let iface = Cluster {
            node_id: 0,
            store,
            cluster,
        };
        assert!(iface.kv_get("key".to_string()).await.is_err());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn kv_set_follower() -> Result<()> {
        let (store, _) = bounded(8);
        let (cluster, mut cluster_rx) = bounded(8);

        let mut api_server = mockito::Server::new();

        let api = api_server.host_with_port();
        let rpc = api.clone();

        tokio::spawn(async move {
            match cluster_rx.recv().await.expect("rcv") {
                IFRequest::IsLeader(result_tx) => {
                    let _ = result_tx.send(Err(RaftError::APIError(
                        CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                            leader_node: Some(Addr::new(api, rpc)),
                            leader_id: Some(1),
                        }),
                    )));
                }
                IFRequest::SetKeyLocal(_, _) => panic!("wrong request"),
            };
        });

        // Create a mock
        let api_mock = api_server
            .mock("POST", "/v1/api/kv/write")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body("42")
            .create();

        let iface = Cluster {
            node_id: 0,
            store,
            cluster,
        };
        assert_eq!(
            iface.kv_set("key".to_string(), b"42".to_vec()).await?,
            b"42".to_vec()
        );

        api_mock.assert();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn kv_get_follower() -> Result<()> {
        let (store, _) = bounded(8);
        let (cluster, mut cluster_rx) = bounded(8);
        let mut api_server = mockito::Server::new();

        let api = api_server.host_with_port();
        let rpc = api.clone();

        tokio::spawn(async move {
            match cluster_rx.recv().await.expect("rcv") {
                IFRequest::IsLeader(result_tx) => {
                    let _ = result_tx.send(Err(RaftError::APIError(
                        CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                            leader_node: Some(Addr::new(api, rpc)),
                            leader_id: Some(1),
                        }),
                    )));
                }
                IFRequest::SetKeyLocal(_, _) => panic!("wrong request"),
            };
        });

        // Create a mock
        let api_mock = api_server
            .mock("POST", "/v1/api/kv/consistent_read")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body("42")
            .create();

        let iface = Cluster {
            node_id: 0,
            store,
            cluster,
        };
        assert_eq!(
            iface.kv_get("key".to_string()).await?,
            Some(simd_json::json!(42))
        );

        api_mock.assert();
        Ok(())
    }
}
