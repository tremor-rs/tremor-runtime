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
use std::time::Duration;

//use super::ClusterError;

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
    addr: Addr,
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
            addr,
            running,
        })
    }

    async fn start_and_join(path: impl AsRef<Path>, join_addr: &Addr) -> ClusterResult<Self> {
        let addr = free_node_addr().await?;
        let mut node = Node::new(path, raft_config()?);
        let running = node
            .try_join(addr.clone(), vec![join_addr.api().to_string()], true)
            .await?;
        let client = ClusterClient::new(&addr.api())?;
        Ok(Self {
            client,
            addr,
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
            addr,
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
            addr,
            running,
        })
    }

    async fn stop(self) -> ClusterResult<()> {
        let Self { running, .. } = self;
        let kill_switch = running.kill_switch();
        kill_switch.stop(ShutdownMode::Graceful)?;
        running.join().await

        // only ever destroy the db when the raft node is known to have stopped
        //rocksdb::DB::destroy(&rocksdb::Options::default(), &path).map_err(ClusterError::Rocks)
    }

    fn client(&self) -> &ClusterClient {
        &self.client
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_join_test() -> ClusterResult<()> {
    let _ = env_logger::try_init();
    let dir0 = tempfile::tempdir()?;
    let dir1 = tempfile::tempdir()?;
    let dir2 = tempfile::tempdir()?;
    let node0 = TestNode::bootstrap(dir0.path()).await?;
    let node1 = TestNode::start_and_join(dir1.path(), &node0.addr).await?;
    let node2 = TestNode::start_and_join(dir2.path(), &node1.addr).await?;

    // all see the same leader
    let client0 = node0.client();
    let client1 = node1.client();
    let client2 = node2.client();
    let metrics = client0.metrics().await?;
    let node0_leader = metrics.current_leader.expect("expect a leader from node 0");
    let node1_leader = client1
        .metrics()
        .await?
        .current_leader
        .expect("expect a leader from node1");
    let node2_leader = client2
        .metrics()
        .await?
        .current_leader
        .expect("expect a leader from node2");
    let node0_id = node0.running.node_data().0;
    assert_eq!(node0_id, node0_leader);
    assert_eq!(node0_id, node1_leader);
    assert_eq!(node0_id, node2_leader);

    // all are voters in the cluster
    let members = metrics
        .membership_config
        .membership
        .get_configs()
        .last()
        .expect("No nodes in membership config");
    assert_eq!(3, members.len());

    node2.stop().await?;
    node1.stop().await?;
    node0.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn add_learner_test() -> ClusterResult<()> {
    let _ = env_logger::try_init();
    let dir0 = tempfile::tempdir()?;
    let dir1 = tempfile::tempdir()?;
    let dir2 = tempfile::tempdir()?;
    let dir3 = tempfile::tempdir()?;
    let node0 = TestNode::bootstrap(dir0.path()).await?;
    let node1 = TestNode::start_and_join(dir1.path(), &node0.addr).await?;
    let node2 = TestNode::start_and_join(dir2.path(), &node1.addr).await?;
    let client0 = node0.client();
    let metrics = client0.metrics().await?;
    let members = metrics
        .membership_config
        .membership
        .get_configs()
        .last()
        .expect("No nodes in membership config");
    assert_eq!(3, members.len());

    let learner_node = TestNode::join_as_learner(dir3.path(), &node0.addr).await?;
    // learner is known to the cluster
    let nodemap = client0.get_nodes().await?;
    assert_eq!(
        Some(&learner_node.running.node_data().1),
        nodemap.get(&learner_node.running.node_data().0)
    );
    // but is not a voter
    let metrics = client0.metrics().await?;
    let members = metrics
        .membership_config
        .membership
        .get_configs()
        .last()
        .expect("No nodes in membership config");
    assert!(
        !members.contains(&learner_node.running.node_data().0),
        "learner not to be part of cluster voters"
    );

    // TODO: deploy an app and see if the learner also runs it
    // TODO: verify the whole lifecycle shenanigans of app instances with and without learner
    // TODO: verify kv stuff

    node2.stop().await?;
    node1.stop().await?;
    node0.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn kill_and_restart_voter() -> ClusterResult<()> {
    let _ = env_logger::try_init();
    let dir0 = tempfile::tempdir()?;
    let dir1 = tempfile::tempdir()?;
    let dir2 = tempfile::tempdir()?;

    let node0 = TestNode::bootstrap(dir0.path()).await?;
    let node1 = TestNode::start_and_join(dir1.path(), &node0.addr).await?;
    let node2 = TestNode::start_and_join(dir2.path(), &node1.addr).await?;

    let client0 = node0.client();
    let metrics = client0.metrics().await?;
    let members = metrics
        .membership_config
        .membership
        .get_configs()
        .last()
        .expect("No nodes in membership config");
    assert_eq!(3, members.len());

    node1.stop().await?;
    // wait until we hit some timeouts
    tokio::time::sleep(Duration::from_millis(500)).await;

    // restart the node
    let node1 = TestNode::just_start(dir1.path()).await?;

    // check that the leader is available
    // TODO: solidify to guard against timing issues
    let client1 = node0.client();
    let k = client1.consistent_read("snot").await?;
    assert!(k.is_none());

    node1.stop().await?;
    node2.stop().await?;
    node0.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn db_fun() {
    // See: https://github.com/rust-rocksdb/rust-rocksdb/issues/720
    let path = Path::new("/tmp/node01");
    if path.exists() {
        std::fs::remove_dir_all(path).expect("remove to succeed");
    }
    let mut db_opts = rocksdb::Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);

    let cf = "foo";
    let db = rocksdb::DB::open_cf(&db_opts, path, [cf]).expect("open to succeed");
    tokio::task::spawn(async move {
        let res = db
            .get_cf(&db.cf_handle(cf).expect("cf to be there"), "key")
            .expect("ok");
        assert!(res.is_none());
    });
}
