// Copyright 2020-2021, The Tremor Team
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

use crate::{
    cli::{AppsCommands, Cluster, ClusterCommand},
    errors::Result,
};
use async_std::{io::ReadExt, task};

use async_std::stream::StreamExt;
use signal_hook::consts::signal::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook::low_level::signal_name;
use signal_hook_async_std::Signals;
use simd_json::OwnedValue;
use std::{
    collections::{BTreeSet, HashMap},
    time::Duration,
};
use tremor_common::asy::file;
use tremor_runtime::{
    raft::{
        archive,
        client::{print_metrics, TremorClient},
        node::{ClusterNode, ClusterNodeKillSwitch},
        start_raft_node,
        store::{AppId, FlowId, InstanceId},
        TremorNodeId,
    },
    system::{Runtime, ShutdownMode, WorldConfig},
};

/// returns either the defined API address or the default defined from the environment
/// variable TREMER_API_ADDRESS
fn get_api(input: Option<String>) -> Result<String> {
    input.map_or_else(
        || {
            Ok(std::env::var("TREMER_API_ADDRESS")
                .map(|s| s.to_string())
                .map_err(|e| format!("{}", e))?)
        },
        |s| Ok(s.to_string()),
    )
}

async fn handle_signals(signals: Signals, kill_switch: ClusterNodeKillSwitch) {
    let mut signals = signals.fuse();

    while let Some(signal) = signals.next().await {
        info!(
            "Received SIGNAL: {}",
            signal_name(signal).unwrap_or(&signal.to_string())
        );
        match signal {
            SIGINT | SIGTERM => {
                if let Err(_e) = kill_switch.stop(ShutdownMode::Graceful) {
                    if let Err(e) = signal_hook::low_level::emulate_default_handler(signal) {
                        error!("Error handling signal {}: {}", signal, e);
                    }
                }
            }
            SIGQUIT => {
                if let Err(_e) = kill_switch.stop(ShutdownMode::Forceful) {
                    if let Err(e) = signal_hook::low_level::emulate_default_handler(signal) {
                        error!("Error handling signal {}: {}", signal, e);
                    }
                }
            }
            signal => {
                if let Err(e) = signal_hook::low_level::emulate_default_handler(signal) {
                    error!("Error handling signal {}: {}", signal, e);
                }
            }
        }
    }
}

impl Cluster {
    pub(crate) async fn run(self) -> Result<()> {
        match self.command {
            // rm -r temp/test-db*; cargo run -p tremor-cli -- cluster boostrap --db-dir temp/test-db1 --api 127.0.0.1:8001 --rpc 127.0.0.1:9001
            ClusterCommand::Bootstrap { rpc, api, db_dir } => {
                let node_id = TremorNodeId::default();

                let mut node =
                    ClusterNode::new(node_id, rpc, api, db_dir, tremor_runtime::raft::config()?);
                let running_node = node.bootstrap_as_single_node_cluster().await?;
                // TODO: install signal handler
                let signals = Signals::new(&[SIGTERM, SIGINT, SIGQUIT])?;
                let signal_handle = signals.handle();
                let signal_handler_task =
                    async_std::task::spawn(handle_signals(signals, running_node.kill_switch()));

                println!(
                    "Node Initialized, this will now form a cluster and elect itself as leader."
                );
                // wait for the node to be finished
                if let Err(e) = running_node.join().await {
                    error!("Error: {e}");
                }
                signal_handle.close();
                signal_handler_task.cancel().await;
            }
            // target/debug/tremor cluster join --db-dir temp/test-db2 --api 127.0.0.1:8002 --rpc 127.0.0.1:9002 --cluster-api 127.0.0.1:8001
            // target/debug/tremor cluster join --db-dir temp/test-db3 --api 127.0.0.1:8003 --rpc 127.0.0.1:9003 --cluster-api 127.0.0.1:8001
            ClusterCommand::Join {
                db_dir,
                cluster_api: leader,
                rpc,
                api,
            } => {
                let my_id = TremorNodeId::random();
                //init_raft_node(&db_dir, my_id, rpc.clone(), api.clone()).await?;
                println!("Node Initialized, we're starting the node and then connecting the leader to join the cluster.");

                let config = WorldConfig::default();
                let (world, world_handle) = Runtime::start(config).await?;

                let dir = db_dir.clone();
                let w = world.clone();
                let t = task::spawn(async move {
                    start_raft_node(&dir, w)
                        .await
                        .expect("failed to start node");
                });

                task::sleep(Duration::from_secs(1)).await;

                let mut client = TremorClient::new(my_id, get_api(leader)?);

                let metrics = client.metrics().await.map_err(|e| format!("error: {e}",))?;

                let leader_id = metrics.current_leader.ok_or("No leader present!")?;

                let leader = metrics.membership_config.get_node(&leader_id);
                let leader_addr = leader.api_addr.clone();
                println!(
                    "communication with leader: {leader_addr} establisehd, joining as a learner.",
                );

                let mut client = TremorClient::new(leader_id, leader_addr.clone());

                println!("Joining as learner");
                client
                    .add_learner(my_id, rpc, api)
                    .await
                    .map_err(|e| format!("Failed to add learner: {e}"))?;

                print!("Waiting until we have joined");
                let mut membership: BTreeSet<_>;
                loop {
                    print!(".");
                    task::sleep(Duration::from_secs(1)).await;

                    let metrics = client.metrics().await.map_err(|e| format!("error: {e}"))?;

                    if !metrics
                        .membership_config
                        .nodes()
                        .any(|(id, _)| id == &my_id)
                    {
                        continue;
                    }

                    membership = metrics
                        .membership_config
                        .nodes()
                        .map(|(id, _)| *id)
                        .collect();
                    membership.insert(my_id);
                    break;
                }
                println!(" done!");
                println!("We are now a learner, and are asking to change the cluster mebership...");

                client
                    .change_membership(&membership)
                    .await
                    .map_err(|e| format!("Failed to update membershipo learner: {e}"))?;
                println!("Membership updated, node is running, from now on you can start the cluster with `tremor cluster start`");
                t.await;
                world.stop(ShutdownMode::Graceful).await?;
                world_handle.await?;
            }
            ClusterCommand::Remove { node, api } => {
                let node_id = TremorNodeId::from(node);
                let mut client = TremorClient::new(node_id, get_api(api)?);

                let metrics = client.metrics().await.map_err(|e| format!("error: {e}",))?;

                let leader_id = metrics.current_leader.ok_or("No leader present!")?;

                let leader = metrics.membership_config.get_node(&leader_id);
                let leader_addr = leader.api_addr.clone();
                println!("communication with leader: {leader_addr} establisehd.",);

                let mut client = TremorClient::new(leader_id, leader_addr.clone());

                let metrics = client.metrics().await.map_err(|e| format!("error: {e}"))?;

                let membership: BTreeSet<_> = metrics
                    .membership_config
                    .nodes()
                    .filter_map(|(id, _)| if *id == node_id { None } else { Some(*id) })
                    .collect();

                client
                    .change_membership(&membership)
                    .await
                    .map_err(|e| format!("Failed to update membershipo learner: {e}"))?;
                println!("Membership updated: node {node} removed.");
            }
            // cargo run -p tremor-cli -- cluster start --db-dir temp/test-db1
            // cargo run -p tremor-cli -- cluster start --db-dir temp/test-db2
            // cargo run -p tremor-cli -- cluster start --db-dir temp/test-db3
            ClusterCommand::Start { db_dir } => {
                let db_dir = db_dir.clone();
                info!("Cluster Starting");
                let config = WorldConfig::default();
                let (world, world_handle) = Runtime::start(config).await?;
                start_raft_node(&db_dir, world.clone()).await?;
                world.stop(ShutdownMode::Graceful).await?;
                world_handle.await?;
            }
            // target/debug/tremor cluster status --api 127.0.0.1:8001
            ClusterCommand::Status { api, json } => {
                let mut client = TremorClient::new(TremorNodeId::default(), get_api(api)?);
                let r = client.metrics().await.map_err(|e| format!("error: {e}"))?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&r)?);
                } else {
                    print_metrics(r);
                }
            }
            ClusterCommand::Apps { api, command } => {
                command.run(get_api(api)?).await?;
            }
            ClusterCommand::Package {
                name,
                out,
                entrypoint,
            } => {
                archive::package(&out, &entrypoint, name.clone()).await?;
            }
        }
        Ok(())
    }
}

impl AppsCommands {
    pub(crate) async fn run(self, api: String) -> Result<()> {
        match self {
            AppsCommands::List { json } => {
                let mut client = TremorClient::new(TremorNodeId::default(), api);
                let r = client.list().await.map_err(|e| format!("error: {e}"))?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&r)?);
                } else {
                    for (_, a) in &r {
                        println!("{a}");
                    }
                }
            }
            AppsCommands::Install { file } => {
                let mut client = TremorClient::new(TremorNodeId::default(), api.clone());
                let mut file = file::open(&file).await?;
                let mut buf = Vec::new();
                file.read_to_end(&mut buf).await?;
                let r = client
                    .install(&buf)
                    .await
                    .map_err(|e| format!("error: {e}"))?;

                match r {
                    Ok(r) => println!(
                        "Application `{}` successfully installed",
                        r.data.value.unwrap_or_default()
                    ),
                    Err(e) => eprintln!("Application failed to install: {e:?}"),
                }
            }
            AppsCommands::Start {
                app,
                flow,
                instance,
                config,
            } => {
                let mut client = TremorClient::new(TremorNodeId::default(), api.clone());
                let config: HashMap<String, OwnedValue> = if let Some(config) = config {
                    serde_json::from_str(&config)?
                } else {
                    HashMap::default()
                };
                let flow = flow.map_or_else(|| FlowId("main".to_string()), FlowId);
                let app = AppId(app);
                let r = client
                    .start(&app, &flow, &InstanceId(instance), config)
                    .await
                    .map_err(|e| format!("error: {e}"))?;

                match r {
                    Ok(r) => println!(
                        "Instance `{app}/{flow}/{}` successfully started",
                        r.data.value.unwrap_or_default()
                    ),
                    Err(e) => eprintln!("Application start to install: {e:?}"),
                }
            }
        }
        Ok(())
    }
}

// # Cluster
//
// ## Management Commands
//
// ### `tremor cluster init`
// initializes a new cluster, sets up the DB and promote itself to leader
//
// ### `tremor cluster join`
// joins an existing cluster, first becomes a learner then joins the cluster as a full member.
//
// ### `tremor cluster start`
// starts a cluster node that was already set/
//
// ### `tremor cluster status`
// gets information from the cluster about it's current state
//
// ### `tremor cluster remove`
// removes a node from the cluster
//
// ## Packaging commands
//
// ### `tremor cluster package`
// packages a `.troy` file into a fully self contained tremor-application
//
// ### `tremor cluster download` --app my-app
// downloads a tremor application from th cluister
//
// ### `tremor cluster reinstall` --app my-app appv2.tar
// Updates an application / reinstalls it (for later versions of clustering)
//
// ## Application Commands
//
// ### `tremor cluster apps list`
// curl -v localhost:8001/api/apps
//
// ### `tremor cluster apps install`  (or app.tar)
// installs a tremor-application into the cluster, but does not start any flows.
//
// ### `tremor cluster apps uninstall` --app my-app
// uninstalls a tremor-application from the cluster if it has no running instances.
//
// ### `tremor cluster apps start` --app my-app flowymcflowface from the_flow --args '{...}'
// deploys a flow from an app with a given set of arguments. (aka starts the appllication)
// curl -v -X POST localhost:8001/api/apps/tick/tick -d '{"instance": "i2", "config":{"interval":10000000000}}'
//
// ### `tremor cluster apps stop` --app my-app flowymcflowface
// undeploys a flow from an app. (aka stops the application)
//
// ### `tremor cluster apps pause` --app my-app flowymcflowface
// undeploys a flow from an app. (aka stops the application)
//
