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

/// contains Flow definition, control plane task and lifecycle management
pub mod flow;
/// contains the runtime actor starting and maintaining flows
pub mod flow_supervisor;

use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use self::flow::{DeploymentType, Flow};
use crate::{
    channel::{oneshot, Sender},
    connectors,
    errors::{ErrorKind, Result},
    instance::IntendedState as IntendedInstanceState,
    log_error,
    raft::{self, ClusterInterface},
};
use tokio::{sync::oneshot, task::JoinHandle, time::timeout};
use tremor_common::alias;
use tremor_script::{
    ast,
    deploy::Deploy,
    highlighter::{self, Highlighter},
    FN_REGISTRY,
};

/// Configuration for the runtime
#[derive(Default)]
pub struct WorldConfig {
    /// if debug connectors should be loaded
    pub debug_connectors: bool,
}

/// default graceful shutdown timeout
pub const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// default timeout for interrogating operations, like listing deployments

#[derive(Debug, PartialEq, Eq)]
/// shutdown mode - controls how we shutdown Tremor
pub enum ShutdownMode {
    /// shut down by stopping all binding instances and wait for quiescence
    Graceful,
    /// Just stop everything and not wait
    Forceful,
}

impl std::fmt::Display for ShutdownMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Graceful => "graceful",
                Self::Forceful => "forceful",
            }
        )
    }
}

/// for draining and stopping
#[derive(Debug, Clone)]
pub struct KillSwitch(Sender<flow_supervisor::Msg>);

impl KillSwitch {
    /// stop the runtime
    ///
    /// # Errors
    /// * if draining or stopping fails
    pub(crate) async fn stop(&self, mode: ShutdownMode) -> Result<()> {
        if mode == ShutdownMode::Graceful {
            let (tx, rx) = oneshot::channel();
            self.0.send(flow_supervisor::Msg::Drain(tx)).await?;
            if let Ok(res) = timeout(DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT, rx).await {
                if res.is_err() {
                    error!("Error draining all Flows",);
                }
            } else {
                warn!(
                    "Timeout draining all Flows after {}s",
                    DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT.as_secs()
                );
            }
        }
        let res = self.0.send(flow_supervisor::Msg::Terminate).await;
        if let Err(e) = &res {
            error!("Error stopping all Flows: {e}");
        }
        Ok(res?)
    }

    #[cfg(test)]
    pub(crate) fn dummy() -> Self {
        KillSwitch(crate::channel::bounded(1).0)
    }

    #[cfg(test)]
    pub(crate) fn new(sender: Sender<flow_supervisor::Msg>) -> Self {
        KillSwitch(sender)
    }
}

/// Tremor runtime
#[derive(Clone, Debug)]
pub struct Runtime {
    pub(crate) flows: flow_supervisor::Channel,
    pub(crate) kill_switch: KillSwitch,
    pub(crate) cluster_manager: Arc<RwLock<Option<raft::Cluster>>>,
}

impl Runtime {
    // pub(crate) fn get_manager(&self) -> Result<raft::Manager> {
    //     self.cluster_manager
    //         .read().map_err(|_| ErrorKind::ReadLock)?
    //         .as_ref()
    //         .cloned()
    //         .ok_or_else(|| ErrorKind::RaftNotRunning.into())
    // }
    pub(crate) fn maybe_get_manager(&self) -> Result<Option<raft::Cluster>> {
        Ok(self
            .cluster_manager
            .read()
            .map_err(|_| ErrorKind::ReadLock)?
            .as_ref()
            .cloned())
    }

    /// Wait for the cluster to be available
    pub async fn wait_for_cluster(&self) {
        while self
            .cluster_manager
            .read()
            .ok()
            .map_or(true, |v| v.is_none())
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    /// Loads a Troy src and starts all deployed flows.
    /// Returns the number of deployed and started flows
    ///
    /// # Errors
    ///   Fails if the source can not be loaded
    pub async fn load_troy(&self, name: &str, src: &str) -> Result<usize> {
        info!("Loading troy src {}", name);

        let aggr_reg = tremor_script::registry::aggr();

        let deployable = Deploy::parse(
            &src,
            &*FN_REGISTRY.read().map_err(|_| ErrorKind::ReadLock)?,
            &aggr_reg,
        );
        let mut h = highlighter::Term::stderr();
        let deployable = match deployable {
            Ok(deployable) => {
                deployable.format_warnings_with(&mut h)?;
                deployable
            }
            Err(e) => {
                log_error!(h.format_error(&e), "Error: {e}");

                return Err(
                    anyhow::Error::from(e).context(format!("failed to load troy file: {src}"))
                );
            }
        };

        let mut count = 0;
        // first deploy them
        for flow in deployable.iter_flows() {
            self.deploy_flow(alias::App::default(), flow, DeploymentType::AllNodes)
                .await?;
        }
        // start flows in a second step
        for flow in deployable.iter_flows() {
            self.start_flow(alias::Flow::new(
                alias::App::default(),
                &flow.instance_alias,
            ))
            .await?;
            count += 1;
        }
        Ok(count)
    }

    /// Deploy a flow - create an instance of it
    ///
    /// This flow instance is not started yet.
    /// # Errors
    /// If the flow can't be deployed
    pub async fn deploy_flow(
        &self,
        app_id: alias::App,
        flow: &ast::DeployFlow<'static>,
        deployment_type: DeploymentType,
    ) -> Result<alias::Flow> {
        let (tx, rx) = oneshot::channel();
        self.flows
            .send(flow_supervisor::Msg::DeployFlow {
                app: app_id,
                flow: Box::new(flow.clone()),
                sender: tx,
                raft: self
                    .maybe_get_manager()?
                    .map(ClusterInterface::from)
                    .unwrap_or_default(),
                deployment_type,
            })
            .await?;
        match rx.await? {
            Err(e) => {
                let err_str = if let Some(e) = e.downcast_ref::<tremor_script::errors::Error>() {
                    let mut h = crate::ToStringHighlighter::new();
                    h.format_error(e)?;
                    h.finalize()?;
                    h.to_string()
                } else {
                    match e.downcast::<tremor_pipeline::errors::Error>() {
                        Ok(tremor_pipeline::errors::Error(
                            tremor_pipeline::errors::ErrorKind::Script(e),
                            _,
                        )) => {
                            //ErrorKind::Script(e)
                            let mut h = crate::ToStringHighlighter::new();
                            h.format_error(&(e.into()))?;
                            h.finalize()?;
                            h.to_string()
                        }
                        Ok(e) => e.to_string(),
                        Err(e) => e.to_string(),
                    }
                };
                error!(
                    "Error starting deployment of flow {}: {err_str}",
                    flow.instance_alias
                );
                Err(flow::Error::Deploy(flow.instance_alias.clone(), err_str).into())
            }
            Ok(flow_id) => Ok(flow_id),
        }
    }

    /// # Errors
    /// if the flow state change fails
    pub async fn change_flow_state(
        &self,
        id: alias::Flow,
        intended_state: IntendedInstanceState,
    ) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot();
        self.flows
            .send(flow_supervisor::Msg::ChangeInstanceState {
                id,
                intended_state,
                reply_tx,
            })
            .await?;
        reply_rx.await?
    }

    /// start a flow and wait for the result
    ///
    /// # Errors
    /// if the flow can't be started
    pub async fn start_flow(&self, id: alias::Flow) -> Result<()> {
        self.change_flow_state(id, IntendedInstanceState::Running)
            .await
    }

    /// stops a flow and waits for the result
    ///
    /// # Errors
    /// if the flow can't be stopped
    pub async fn stop_flow(&self, id: alias::Flow) -> Result<()> {
        self.change_flow_state(id, IntendedInstanceState::Stopped)
            .await
    }

    /// pauses a flow and waits for the result
    ///
    /// # Errors
    /// if the flow can't be paused
    pub async fn pause_flow(&self, id: alias::Flow) -> Result<()> {
        self.change_flow_state(id, IntendedInstanceState::Paused)
            .await
    }
    /// resumes a flow
    ///
    /// # Errors
    /// if the flow can't be resumed
    pub async fn resume_flow(&self, id: alias::Flow) -> Result<()> {
        self.start_flow(id).await // equivalent
    }

    /// Registers the given connector type with `type_name` and the corresponding `builder`
    ///
    /// # Errors
    ///  * If the system is unavailable
    pub(crate) async fn register_builtin_connector_type(
        &self,
        builder: Box<dyn connectors::ConnectorBuilder>,
    ) -> Result<()> {
        self.flows
            .send(flow_supervisor::Msg::RegisterConnectorType {
                connector_type: builder.connector_type(),
                builder,
            })
            .await?;
        Ok(())
    }

    // METHODS EXPOSED BECAUSE API

    /// Get a flow instance address identified by `flow_id`
    ///
    /// # Errors
    ///  * if we fail to send the request or fail to receive it
    pub async fn get_flow(&self, flow_id: alias::Flow) -> Result<Flow> {
        let (flow_tx, flow_rx) = oneshot::channel();
        self.flows
            .send(flow_supervisor::Msg::GetFlow(flow_id, flow_tx))
            .await?;
        flow_rx.await?
    }

    /// list the currently deployed flows
    ///
    /// # Errors
    ///  * if we fail to send the request or fail to receive it
    pub async fn get_flows(&self) -> Result<Vec<Flow>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.flows
            .send(flow_supervisor::Msg::GetFlows(reply_tx))
            .await?;
        reply_rx.await?
    }

    /// Starts the runtime system
    ///
    /// # Errors
    ///  * if the world manager can't be started
    pub async fn start(config: WorldConfig) -> Result<(Self, JoinHandle<Result<()>>)> {
        let cluster_manager = Arc::new(RwLock::new(None));
        let (system_h, system, kill_switch) = flow_supervisor::FlowSupervisor::new().start();

        let world = Self {
            flows: system,
            kill_switch,
            cluster_manager,
        };

        connectors::register_builtin_connector_types(&world, config.debug_connectors).await?;
        Ok((world, system_h))
    }

    /// Stop the runtime
    ///
    /// # Errors
    ///  * if the system failed to stop
    pub async fn stop(&self, mode: ShutdownMode) -> Result<()> {
        self.kill_switch.stop(mode).await
    }
}
