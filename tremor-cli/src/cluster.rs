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
    cli::{AppsCommands, Cluster, ClusterCommand, KvCommands},
    errors::{Error, Result},
};
use futures::StreamExt;
use signal_hook::{
    consts::signal::{SIGINT, SIGQUIT, SIGTERM},
    low_level::signal_name,
};
use signal_hook_tokio::Signals;
use simd_json::OwnedValue;
use std::{collections::HashMap, path::Path};
use tokio::{io::AsyncReadExt, task};
use tremor_common::asy::file;
use tremor_runtime::{
    ids::{AppFlowInstanceId, AppId, FlowDefinitionId},
    raft::{
        api::client::{print_metrics, Tremor as Client},
        archive,
        node::{Addr, ClusterNodeKillSwitch, Node},
        remove_node,
        store::TremorInstanceState,
        ClusterError, NodeId,
    },
    system::ShutdownMode,
};

/// returns either the defined API address or the default defined from the environment
/// variable `TREMOR_API_ADDRESS`
fn get_api(input: Option<String>) -> Result<String> {
    input.map_or_else(
        || Ok(std::env::var("TREMOR_API_ADDRESS").map_err(|e| e.to_string())?),
        Ok,
    )
}

async fn handle_signals(
    signals: Signals,
    kill_switch: ClusterNodeKillSwitch,
    node_to_remove_on_term: Option<(NodeId, Addr)>,
) {
    let mut signals = signals.fuse();

    while let Some(signal) = signals.next().await {
        info!(
            "Received SIGNAL: {}",
            signal_name(signal).unwrap_or(&signal.to_string())
        );
        match signal {
            SIGTERM => {
                // In k8s SIGTERM is sent to the container to indicate that it should shut down
                // gracefully as part of auto scaling events.
                // if we want tremor to auto-scale  automatically we need to be able to join and
                // leave clusters as part of the process.
                // This is not always teh intended behaviour (read: only in k8s) so we have it as
                // an option.
                // It's noteworthy that as API adress we use the own nodes address, as it's not shut
                // down yet it has the full cluster informaiton and we know for use it is up.
                // Otherwise if we'd cache a API endpoint that endpoint might have been autoscaled
                // away or replaced by now.
                if let Some((node_id, ref addr)) = node_to_remove_on_term {
                    println!("SIGTERM received, removing {node_id} from cluster");
                    if let Err(e) = remove_node(node_id, addr.api()).await {
                        error!("Cluster leave failed: {e}");
                    };
                }
                if let Err(_e) = kill_switch.stop(ShutdownMode::Graceful) {
                    if let Err(e) = signal_hook::low_level::emulate_default_handler(signal) {
                        error!("Error handling signal {}: {}", signal, e);
                    }
                }
            }
            SIGINT => {
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
    /// Run the cluster command
    pub(crate) async fn run(self) -> Result<()> {
        match self.command {
            // rm -r temp/test-db*; cargo run -p tremor-cli -- cluster bootstrap --db-dir temp/test-db1 --api 127.0.0.1:8001 --rpc 127.0.0.1:9001
            ClusterCommand::Bootstrap {
                rpc,
                api,
                db_dir,
                remove_on_sigterm,
            } => {
                // the bootstrap node always starts with a node_id of 0
                // it will be assigned a new one during bootstrap
                let node_id = NodeId::default();
                info!("Boostrapping Tremor node with id {node_id}...");

                let addr = Addr::new(api.clone(), rpc);
                let mut node = Node::new(db_dir, tremor_runtime::raft::config()?);
                let running_node = node.bootstrap_as_single_node_cluster(addr.clone()).await?;

                // install signal handler
                let signals = Signals::new([SIGTERM, SIGINT, SIGQUIT])?;
                let signal_handle = signals.handle();
                let node_to_remove_on_term = remove_on_sigterm.then_some((node_id, addr));

                let signal_handler_task = task::spawn(handle_signals(
                    signals,
                    running_node.kill_switch(),
                    node_to_remove_on_term,
                ));

                info!("Tremor node bootstrapped as single node cluster.");
                // wait for the node to be finished
                if let Err(e) = running_node.join().await {
                    error!("Error: {e}");
                }
                info!("Tremor stopped.");
                signal_handle.close();
                signal_handler_task.abort();
            }
            // target/debug/tremor cluster start --db-dir temp/test-db2 --api 127.0.0.1:8002 --rpc 127.0.0.1:9002 --join 127.0.0.1:8001
            // target/debug/tremor cluster start --db-dir temp/test-db3 --api 127.0.0.1:8003 --rpc 127.0.0.1:9003 --join 127.0.0.1:8001
            ClusterCommand::Start {
                db_dir,
                rpc,
                api,
                join,
                remove_on_sigterm,
                passive,
            } => {
                // start db and statemachine
                // start raft
                // node_id assigned during bootstrap or join
                let running_node = if Path::new(&db_dir).exists()
                    && !tremor_common::file::is_empty(&db_dir)?
                {
                    if !join.is_empty() {
                        // TODO: check if join nodes are part of the known cluster, if so, don't error
                        return Err(Error::from(
                            "Cannot join another cluster with existing db directory",
                        ));
                    }
                    info!("Loading existing Tremor node state from {db_dir}.");
                    Node::load_from_store(&db_dir, tremor_runtime::raft::config()?).await?
                } else {
                    // db dir does not exist
                    let rpc_addr = rpc.ok_or_else(|| ClusterError::from("missing rpc address"))?;
                    let api_addr = api.ok_or_else(|| ClusterError::from("missing api address"))?;
                    let addr = Addr::new(api_addr, rpc_addr);

                    info!("Bootstrapping cluster node with addr {addr} and db_dir {db_dir}");
                    let mut node = Node::new(&db_dir, tremor_runtime::raft::config()?);

                    // attempt to join any one of the given endpoints, stop once we joined one
                    node.try_join(addr, join, !passive).await?
                };

                // install signal handler
                let signals = Signals::new([SIGTERM, SIGINT, SIGQUIT])?;
                let signal_handle = signals.handle();
                let node_to_remove_on_term = remove_on_sigterm.then_some(running_node.node_data());
                let signal_handler_task = task::spawn(handle_signals(
                    signals,
                    running_node.kill_switch(),
                    node_to_remove_on_term,
                ));

                // wait for the node to be finished
                if let Err(e) = running_node.join().await {
                    error!("Error: {e}");
                }
                signal_handle.close();
                signal_handler_task.abort();
            }
            ClusterCommand::Remove { node, api } => {
                let api_addr = get_api(api)?;

                remove_node(node, &api_addr).await?;
            }
            // target/debug/tremor cluster status --api 127.0.0.1:8001
            ClusterCommand::Status { api, json } => {
                let client = Client::new(&get_api(api)?)?;
                let r = client.metrics().await.map_err(|e| format!("error: {e}"))?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&r)?);
                } else {
                    print_metrics(&r);
                }
            }
            ClusterCommand::Apps { api, command } => {
                command.run(get_api(api)?.as_str()).await?;
            }
            ClusterCommand::Package {
                name,
                out,
                entrypoint,
            } => {
                archive::package(&out, &entrypoint, name.clone()).await?;
            }
            ClusterCommand::Kv { api, command } => {
                command.run(get_api(api)?.as_str()).await?;
            }
        }
        Ok(())
    }
}

impl AppsCommands {
    pub(crate) async fn run(self, api: &str) -> Result<()> {
        let client = Client::new(api)?;
        match self {
            AppsCommands::List { json } => {
                let r = client.list().await.map_err(|e| format!("error: {e}"))?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&r)?);
                } else {
                    for a in r.values() {
                        println!("{a}");
                    }
                }
            }
            AppsCommands::Install { file } => {
                let mut file = file::open(&file).await?;
                let mut buf = Vec::new();
                file.read_to_end(&mut buf).await?;
                match client.install(&buf).await {
                    Ok(app_id) => println!("Application `{app_id}` successfully installed",),
                    Err(e) => eprintln!("Application failed to install: {e}"),
                }
            }
            AppsCommands::Uninstall { app } => {
                let app_id = AppId(app);
                match client.uninstall_app(&app_id).await {
                    Ok(app_id) => println!("App `{app_id}` successfully uninstalled",),
                    Err(e) => eprintln!("Application `{app_id}` failed to be uninstalled: {e}"),
                }
            }
            AppsCommands::Start {
                app,
                flow,
                instance,
                config,
                paused,
            } => {
                let config: HashMap<String, OwnedValue> =
                    config.map_or_else(|| Ok(HashMap::new()), |c| serde_json::from_str(&c))?;
                let flow = flow.map_or_else(|| FlowDefinitionId::from("main"), FlowDefinitionId);
                let app_id = AppId(app);
                let instance_id = AppFlowInstanceId::new(app_id, instance);
                match client.start(&flow, &instance_id, config, !paused).await {
                    Ok(instance_id) => println!("Instance `{instance_id}` successfully started",),
                    Err(e) => eprintln!("Instance `{instance_id}` failed to start: {e}"),
                }
            }
            AppsCommands::Stop { app, instance } => {
                let instance_id = AppFlowInstanceId::new(app, instance);
                match client.stop_instance(&instance_id).await {
                    Ok(instance_id) => println!("Instance `{instance_id}` stopped"),
                    Err(e) => eprintln!("Instance `{instance_id}` failed to stop: {e}"),
                }
            }
            AppsCommands::Pause { app, instance } => {
                let app_id = AppId(app);
                let instance_id = AppFlowInstanceId::new(app_id, instance);
                match client
                    .change_instance_state(&instance_id, TremorInstanceState::Pause)
                    .await
                {
                    Ok(instance_id) => println!("Instance `{instance_id}` successfully paused"),
                    Err(e) => eprintln!("Instance `{instance_id}` failed to pause: {e}"),
                }
            }
            AppsCommands::Resume { app, instance } => {
                let instance_id = AppFlowInstanceId::new(app, instance);
                match client
                    .change_instance_state(&instance_id, TremorInstanceState::Resume)
                    .await
                {
                    Ok(instance_id) => println!("Instance `{instance_id}` successfully resumed"),
                    Err(e) => eprintln!("Instance `{instance_id}` failed to resume: {e}"),
                }
            }
        }
        Ok(())
    }
}

impl KvCommands {
    async fn run(self, api: &str) -> Result<()> {
        let client = Client::new(api)?;
        match self {
            KvCommands::Get {
                key,
                consistant: true,
            } => {
                let mut r = client.read(&key).await.map_err(|e| format!("error: {e}"))?;
                let value: OwnedValue = simd_json::from_slice(&mut r)?;
                println!("{}", simd_json::to_string_pretty(&value)?);
            }
            KvCommands::Get { key, .. } => {
                let mut r = client
                    .consistent_read(&key)
                    .await
                    .map_err(|e| format!("error: {e}"))?;
                let value: OwnedValue = simd_json::from_slice(&mut r)?;
                println!("{}", simd_json::to_string_pretty(&value)?);
            }
            KvCommands::Set { key, mut value } => {
                let v = unsafe { value.as_bytes_mut() };
                let value: OwnedValue = simd_json::from_slice(v)?;
                let ts = tremor_runtime::raft::api::kv::KVSet { key, value };
                let r = client.write(&ts).await.map_err(|e| format!("error: {e}"))?;
                println!("{}", simd_json::to_string_pretty(&r)?);
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
