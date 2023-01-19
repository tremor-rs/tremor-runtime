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
use super::prelude::*;
use crate::{
    ids::{FlowDefinitionId, FlowInstanceId},
    raft::archive::build_archive_from_source,
};
use std::collections::HashMap;
use std::time::Duration;

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
    let (learner_node_id, learner_addr) = learner_node.running.node_data();
    // learner is known to the cluster
    let nodemap = client0.get_nodes().await?;
    assert_eq!(Some(&learner_addr), nodemap.get(&learner_node_id));
    // but is not a voter
    let metrics = client0.metrics().await?;
    let members = metrics
        .membership_config
        .membership
        .get_configs()
        .last()
        .expect("No nodes in membership config");
    assert!(
        !members.contains(&learner_node_id),
        "learner not to be part of cluster voters"
    );
    // remove the learner again
    dbg!("remove learner", &learner_node_id);
    client0.remove_learner(&learner_node_id).await?;
    dbg!("stop learner");
    learner_node.stop().await?;

    dbg!("remove learner node");
    client0.remove_node(&learner_node_id).await?;
    dbg!("removed");

    // TODO: deploy an app and see if the learner also runs it
    // TODO: verify the whole lifecycle shenanigans of app instances with and without learner
    // TODO: verify kv stuff

    node2.stop().await?;
    node1.stop().await?;
    node0.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn learner_runs_app() -> ClusterResult<()> {
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
    let (_learner_node_id, _learner_addr) = learner_node.running.node_data();
    let tmpfile = tempfile::NamedTempFile::new()?;
    let out_path = tmpfile.into_temp_path();
    let app_entrypoint = format!(
        r#"
define flow main
flow
    define pipeline pt
    pipeline
        select event from in into out;
    end;
    create pipeline pt;

    define connector output from file
    with
        codec = "string",
        config = {{
            "mode": "append",
            "path": "{}"
        }}
    end;
    create connector output;

    define connector input from oneshot
    with
        config = {{
            "value": 1
        }}
    end;
    create connector input;

    connect /connector/input to /pipeline/pt;
    connect /pipeline/pt to /connector/output;
end;
    "#,
        out_path.display()
    );
    let archive = build_archive_from_source("main", app_entrypoint.as_str())?;
    let app_id = client0.install(&archive).await?;

    let flow_id = FlowDefinitionId("main".to_string());
    let instance = FlowInstanceId::new(app_id, "01".to_string());
    let config = HashMap::new();
    let instance_id = client0.start(&flow_id, &instance, config, true).await?;

    // wait for the app to be actually started
    // wait for the file to exist
    while !out_path.exists() {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // wait another short while for all nodes to finish writing
    tokio::time::sleep(Duration::from_millis(500)).await;
    // stop the flow instance
    dbg!("Stopping instance");
    client0.stop_instance(&instance_id).await?;
    dbg!("Instance stopped");
    // shut the nodes down
    learner_node.stop().await?;
    node2.stop().await?;
    node1.stop().await?;
    node0.stop().await?;

    // verify that each node had a flow instance running and did write the even to the file
    let out_bytes = tokio::fs::read(&out_path).await?;
    assert_eq!("1111", &String::from_utf8_lossy(&out_bytes));
    out_path.close()?;
    Ok(())
}
