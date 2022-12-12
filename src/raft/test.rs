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
use crate::raft::{
    api::client::Tremor as ClusterClient,
    config as raft_config,
    node::{Addr, Node, Running},
    ClusterResult,
};
use crate::system::ShutdownMode;
use crate::{connectors::tests::free_port::find_free_tcp_port, errors::Result};
use std::path::Path;

use super::store::Store;

async fn free_node_addr() -> Result<Addr> {
    let api_port = find_free_tcp_port().await?;
    let rpc_port = find_free_tcp_port().await?;
    Ok(Addr::new(
        format!("127.0.0.1:{api_port}"),
        format!("127.0.0.1:{rpc_port}"),
    ))
}

struct TestNode {
    client: ClusterClient,
    //addr: Addr,
    running: Running,
}

impl TestNode {
    async fn bootstrap(path: impl AsRef<Path>) -> ClusterResult<Self> {
        let addr = free_node_addr().await?;
        let mut node = Node::new(path, raft_config()?);

        let running = node.bootstrap_as_single_node_cluster(addr.clone()).await?;
        let client = ClusterClient::new(&addr.api())?;
        Ok(Self {
            client,
            //addr,
            running,
        })
    }

    #[allow(dead_code)]
    async fn start_and_join(path: impl AsRef<Path>, join_addr: &Addr) -> ClusterResult<Self> {
        let addr = free_node_addr().await?;
        let mut node = Node::new(path, raft_config()?);
        let running = node
            .try_join(addr.clone(), vec![join_addr.api().to_string()], true)
            .await?;
        let client = ClusterClient::new(&addr.api())?;
        Ok(Self {
            client,
            //addr,
            running,
        })
    }

    #[allow(dead_code)]
    async fn just_start(path: impl AsRef<Path>) -> ClusterResult<Self> {
        let addr = free_node_addr().await?;
        let running = Node::load_from_store(path, raft_config()?).await?;
        let client = ClusterClient::new(&addr.api())?;
        Ok(Self {
            client,
            //addr,
            running,
        })
    }

    #[allow(dead_code)]
    async fn join_as_learner(path: impl AsRef<Path>, join_addr: &Addr) -> ClusterResult<Self> {
        let addr = free_node_addr().await?;
        let mut node = Node::new(path, raft_config()?);
        let running = node
            .try_join(addr.clone(), vec![join_addr.api().to_string()], false)
            .await?;
        let client = ClusterClient::new(&addr.api())?;
        Ok(Self {
            client,
            //addr,
            running,
        })
    }

    #[allow(dead_code)]
    async fn stop(self) -> ClusterResult<()> {
        self.running.kill_switch().stop(ShutdownMode::Graceful)
        //self.running.join().await
    }

    #[allow(dead_code)]
    fn client(&self) -> &ClusterClient {
        &self.client
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_test() -> ClusterResult<()> {
    let _ = env_logger::try_init();
    let dir0 = Path::new("/tmp/node0");
    std::fs::remove_dir_all(dir0)?;
    let node0 = TestNode::bootstrap(dir0).await?;

    // let dir1 = Path::new("/tmp/node1");
    // std::fs::remove_dir_all(dir1)?;
    // let _node1 = TestNode::start_and_join(dir1, &node0.addr).await?;

    // let dir2 = Path::new("/tmp/node2");
    // std::fs::remove_dir_all(dir2)?;
    // let _node2 = TestNode::start_and_join(dir2, &node1.addr).await?;

    let _client0 = node0.client();
    let _apps = _client0.list().await?;
    // let client1 = node1.client();
    //let client2 = node2.client();
    //let metrics = client0.metrics().await?;
    // let node0_leader = metrics.current_leader.expect("expect a leader from node 0");
    // let node1_leader = client1
    //     .metrics()
    //     .await?
    //     .current_leader
    //     .expect("expect a leader from node1");
    // let node2_leader = client2
    //     .metrics()
    //     .await?
    //     .current_leader
    //     .expect("expect a leader from node2");
    // assert_eq!(0, node0_leader);
    // assert_eq!(0, node1_leader);
    // assert_eq!(0, node2_leader);

    // let members = metrics
    //     .membership_config
    //     .membership
    //     .get_configs()
    //     .last()
    //     .expect("No nodes in membership config");
    // assert_eq!(3, members.len());

    // TODO: remove the leaving nodes before stopping them
    //node2.stop().await?;
    // node1.stop().await?;
    node0.stop().await?;
    Ok(())
}

#[test]
fn db_fun() {
    std::fs::remove_dir_all("/tmp/node01").unwrap();
    std::fs::remove_dir_all("/tmp/node02").unwrap();
    let store01 = Store::init_db("/tmp/node01").unwrap();
    let store02 = Store::init_db("/tmp/node02").unwrap();

    drop(store02);
    drop(store01);
}
