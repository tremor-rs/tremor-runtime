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

use crate::{
    channel::{bounded, OneShotSender, Sender},
    connectors::{self, ConnectorBuilder, ConnectorType},
    errors::{Kind as ErrorKind, Result},
    instance::IntendedState,
    log_error, qsize, raft,
    system::{
        flow::{AppContext, Flow},
        KillSwitch, DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT,
    },
};
use std::collections::{hash_map::Entry, HashMap};
use tokio::{
    sync::oneshot,
    task::{self, JoinHandle},
    time::timeout,
};
use tremor_common::{
    alias,
    uids::{ConnectorUIdGen, OperatorUIdGen},
};
use tremor_script::ast::DeployFlow;

use super::flow::DeploymentType;

pub(crate) type Channel = Sender<Msg>;

/// This is control plane
#[derive(Debug)]
pub(crate) enum Msg {
    /// Deploy a flow, instantiate it, but does not start it or any child instance (connector, pipeline)
    DeployFlow {
        /// the App this flow shall be a part of
        app: alias::App,
        /// the `deploy flow` AST
        flow: Box<DeployFlow<'static>>,
        /// result sender
        sender: OneShotSender<Result<alias::Flow>>,
        /// API request sender
        raft: raft::Cluster,
        /// Type of the deployment
        deployment_type: DeploymentType,
    },
    /// change instance state
    ChangeInstanceState {
        /// unique ID for the `Flow` instance to start
        id: alias::Flow,
        /// The state the instance should be changed to
        intended_state: IntendedState,
        /// result sender
        reply_tx: OneShotSender<Result<()>>,
    },
    RegisterConnectorType {
        /// the type of connector
        connector_type: ConnectorType,
        /// the builder
        builder: Box<dyn ConnectorBuilder>,
    },
    GetFlows(OneShotSender<Result<Vec<Flow>>>),
    GetFlow(alias::Flow, oneshot::Sender<Result<Flow>>),
    /// Initiate the Quiescence process
    Drain(OneShotSender<Result<()>>),
    /// stop this manager
    Terminate,
}

#[derive(Debug)]
pub(crate) struct FlowSupervisor {
    flows: HashMap<alias::Flow, Flow>,
    operator_id_gen: OperatorUIdGen,
    connector_id_gen: ConnectorUIdGen,
    known_connectors: connectors::Known,
}

impl FlowSupervisor {
    pub fn new() -> Self {
        Self {
            flows: HashMap::new(),
            known_connectors: connectors::Known::new(),
            operator_id_gen: OperatorUIdGen::new(),
            connector_id_gen: ConnectorUIdGen::new(),
        }
    }

    fn handle_register_connector_type(
        &mut self,
        connector_type: ConnectorType,
        builder: Box<dyn ConnectorBuilder>,
    ) {
        if let Some(old) = self.known_connectors.insert(connector_type, builder) {
            error!("Connector type {} already defined!", old.connector_type());
        }
    }

    async fn handle_deploy(
        &mut self,
        app_id: alias::App,
        flow: DeployFlow<'static>,
        sender: oneshot::Sender<Result<alias::Flow>>,
        kill_switch: &KillSwitch,
        raft: raft::Cluster,
        deployment_type: DeploymentType,
    ) {
        let id = alias::Flow::new(app_id, &flow.instance_alias);
        let res = match self.flows.entry(id.clone()) {
            Entry::Occupied(_occupied) => Err(ErrorKind::DuplicateFlow(id.to_string()).into()),
            Entry::Vacant(vacant) => {
                let ctx = AppContext {
                    id: id.clone(),
                    raft,
                    ..AppContext::default()
                };
                Flow::deploy(
                    ctx,
                    flow,
                    &mut self.operator_id_gen,
                    &mut self.connector_id_gen,
                    &self.known_connectors,
                    kill_switch,
                    deployment_type,
                )
                .await
                .map(|deploy| {
                    vacant.insert(deploy);
                    id
                })
            }
        };
        log_error!(
            sender.send(res).map_err(|_| "send error"),
            "Error sending StartDeploy Err Result: {e}"
        );
    }
    fn handle_get_flows(&self, reply_tx: oneshot::Sender<Result<Vec<Flow>>>) {
        let flows = self.flows.values().cloned().collect();
        log_error!(
            reply_tx.send(Ok(flows)).map_err(|_| "send error"),
            "Error sending ListFlows response: {e}"
        );
    }
    fn handle_get_flow(&self, id: &alias::Flow, reply_tx: oneshot::Sender<Result<Flow>>) {
        log_error!(
            reply_tx
                .send(
                    self.flows
                        .get(id)
                        .cloned()
                        .ok_or_else(|| ErrorKind::FlowNotFound(id.to_string()).into()),
                )
                .map_err(|_| "send error"),
            "Error sending GetFlow response: {e}"
        );
    }

    async fn handle_terminate(&mut self) -> Result<()> {
        info!("Stopping Manager ...");
        if !self.flows.is_empty() {
            // drain the flows, we are stopping anyways, this is the last interaction with them
            let mut rxs = Vec::with_capacity(self.flows.len());
            for (_, flow) in self.flows.drain() {
                let (tx, rx) = crate::channel::oneshot();
                if !log_error!(
                    flow.stop(tx).await,
                    "Failed to stop Deployment \"{alias}\": {e}",
                    alias = flow.id()
                ) {
                    rxs.push(rx);
                }
            }

            timeout(
                DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT,
                task::spawn(async move {
                    for rx in rxs.drain(..) {
                        log_error!(rx.await?, "Error during Stopping: {e}");
                    }
                    Result::Ok(())
                }),
            )
            .await???;
        }
        Ok(())
    }
    async fn handle_drain(&mut self, sender: oneshot::Sender<Result<()>>) {
        if self.flows.is_empty() {
            log_error!(
                sender.send(Ok(())).map_err(|_| "send error"),
                "Failed to send drain result: {e}"
            );
        } else {
            let num_flows = self.flows.len();
            info!("Draining all {num_flows} Flows ...");
            let mut rxs = Vec::with_capacity(self.flows.len());
            for (_, flow) in self.flows.drain() {
                let (tx, rx) = crate::channel::oneshot();

                if !log_error!(
                    flow.stop(tx).await,
                    "Failed to drain Deployment \"{alias}\": {e}",
                    alias = flow.id()
                ) {
                    rxs.push(rx);
                }
            }
            task::spawn(async move {
                for rx in rxs.drain(..) {
                    match rx.await {
                        Ok(Err(e)) => {
                            error!("Error during Draining: {e}");
                        }
                        Ok(_) | Err(_) => {}
                    };
                }
                info!("Flows drained.");
                sender.send(Ok(())).map_err(|_| "Failed to send reply")?;
                Result::Ok(())
            });
        }
    }

    async fn handle_change_state(
        &mut self,
        id: alias::Flow,
        intended_state: IntendedState,
        reply_tx: OneShotSender<Result<()>>,
    ) -> Result<()> {
        if let IntendedState::Stopped = intended_state {
            // we remove the flow as it won't be reachable anymore, once it is stopped
            // keeping it around will lead to errors upon stopping
            if let Some(flow) = self.flows.remove(&id) {
                flow.stop(reply_tx).await?;
                Ok(())
            } else {
                reply_tx
                    .send(Err(ErrorKind::FlowNotFound(id.to_string()).into()))
                    .map_err(|_| "can't reply")?;
                Err(ErrorKind::FlowNotFound(id.to_string()).into())
            }
        } else if let Some(flow) = self.flows.get(&id) {
            flow.change_state(intended_state, reply_tx).await?;
            Ok(())
        } else {
            reply_tx
                .send(Err(ErrorKind::FlowNotFound(id.to_string()).into()))
                .map_err(|_| "can't reply")?;
            Err(ErrorKind::FlowNotFound(id.to_string()).into())
        }
    }

    pub fn start(mut self) -> (JoinHandle<Result<()>>, Channel, KillSwitch) {
        let (tx, mut rx) = bounded(qsize());
        let kill_switch = KillSwitch(tx.clone());
        let task_kill_switch = kill_switch.clone();

        let system_h = task::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    Msg::RegisterConnectorType {
                        connector_type,
                        builder,
                        ..
                    } => self.handle_register_connector_type(connector_type, builder),
                    Msg::DeployFlow {
                        app,
                        flow,
                        sender,
                        raft: raft_api_tx,
                        deployment_type,
                    } => {
                        self.handle_deploy(
                            app,
                            *flow,
                            sender,
                            &task_kill_switch,
                            raft_api_tx,
                            deployment_type,
                        )
                        .await;
                    }
                    Msg::GetFlows(reply_tx) => self.handle_get_flows(reply_tx),
                    Msg::GetFlow(id, reply_tx) => self.handle_get_flow(&id, reply_tx),
                    Msg::Terminate => {
                        self.handle_terminate().await?;
                        break;
                    }
                    Msg::Drain(sender) => self.handle_drain(sender).await,
                    Msg::ChangeInstanceState {
                        id,
                        intended_state,
                        reply_tx,
                    } => {
                        // if an error happened here already, the reply_tx hasn't been sent anywhere so we need to send the error here
                        log_error!(
                            self.handle_change_state(id, intended_state, reply_tx).await,
                            "Error sending ChangeInstanceState reply: {e}"
                        );
                    }
                }
            }
            info!("Manager stopped.");
            Ok(())
        });
        (system_h, tx, kill_switch)
    }
}
