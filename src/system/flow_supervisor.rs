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

use super::flow::{Alias, Flow};
use super::KillSwitch;
use crate::system::DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT;
use crate::{
    channel::{bounded, Sender},
    errors::empty_error,
};
use crate::{
    connectors::{self, ConnectorBuilder, ConnectorType},
    log_error,
};
use crate::{
    errors::{Kind as ErrorKind, Result},
    qsize,
};
use hashbrown::{hash_map::Entry, HashMap};
use tokio::{
    sync::oneshot,
    task::{self, JoinHandle},
    time::timeout,
};
use tremor_common::ids::{ConnectorIdGen, OperatorIdGen};
use tremor_script::ast::DeployFlow;

pub(crate) type Channel = Sender<Msg>;

/// This is control plane
#[derive(Debug)]
pub(crate) enum Msg {
    /// deploy a Flow
    StartDeploy {
        /// deploy flow
        flow: Box<DeployFlow<'static>>,
        /// result sender
        sender: oneshot::Sender<Result<()>>,
    },
    RegisterConnectorType {
        /// the type of connector
        connector_type: ConnectorType,
        /// the builder
        builder: Box<dyn ConnectorBuilder>,
    },
    GetFlows(oneshot::Sender<Result<Vec<Flow>>>),
    GetFlow(Alias, oneshot::Sender<Result<Flow>>),
    /// Initiate the Quiescence process
    Drain(oneshot::Sender<Result<()>>),
    /// stop this manager
    Stop,
}

#[derive(Debug)]
pub(crate) struct FlowSupervisor {
    flows: HashMap<Alias, Flow>,
    operator_id_gen: OperatorIdGen,
    connector_id_gen: ConnectorIdGen,
    known_connectors: connectors::Known,
}

impl FlowSupervisor {
    pub fn new() -> Self {
        Self {
            flows: HashMap::new(),
            known_connectors: connectors::Known::new(),
            operator_id_gen: OperatorIdGen::new(),
            connector_id_gen: ConnectorIdGen::new(),
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

    async fn handle_start_deploy(
        &mut self,
        flow: DeployFlow<'static>,
        sender: oneshot::Sender<Result<()>>,
        kill_switch: &KillSwitch,
    ) {
        let id = Alias::from(&flow);
        let res = match self.flows.entry(id.clone()) {
            Entry::Occupied(_occupied) => Err(ErrorKind::DuplicateFlow(id.to_string()).into()),
            Entry::Vacant(vacant) => Flow::start(
                flow,
                &mut self.operator_id_gen,
                &mut self.connector_id_gen,
                &self.known_connectors,
                kill_switch,
            )
            .await
            .map(|deploy| {
                vacant.insert(deploy);
            }),
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
    fn handle_get_flow(&self, id: &Alias, reply_tx: oneshot::Sender<Result<Flow>>) {
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

    async fn handle_stop(&self) -> Result<()> {
        info!("Stopping Manager ...");
        if !self.flows.is_empty() {
            // send stop to each deployment
            let (tx, mut rx) = bounded(self.flows.len());
            let mut expected_stops: usize = 0;
            for flow in self.flows.values() {
                log_error!(
                    flow.stop(tx.clone()).await,
                    "Failed to stop Deployment \"{alias}\": {e}",
                    alias = flow.id()
                );
                expected_stops += 1;
            }

            timeout(
                DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT,
                task::spawn(async move {
                    while expected_stops > 0 {
                        log_error!(
                            rx.recv().await.ok_or_else(empty_error)?,
                            "Error during Stopping: {e}"
                        );
                        expected_stops = expected_stops.saturating_sub(1);
                    }
                    Result::Ok(())
                }),
            )
            .await???;
        }
        Ok(())
    }
    async fn handle_drain(&self, sender: oneshot::Sender<Result<()>>) {
        if self.flows.is_empty() {
            log_error!(
                sender.send(Ok(())).map_err(|_| "send error"),
                "Failed to send drain result: {e}"
            );
        } else {
            let num_flows = self.flows.len();
            info!("Draining all {num_flows} Flows ...");
            let mut alive_flows = 0_usize;
            let (tx, mut rx) = bounded(num_flows);
            for (_, flow) in &self.flows {
                if !log_error!(
                    flow.drain(tx.clone()).await,
                    "Failed to drain Deployment \"{alias}\": {e}",
                    alias = flow.id()
                ) {
                    alive_flows += 1;
                }
            }
            task::spawn(async move {
                while alive_flows > 0 {
                    match rx.recv().await {
                        Some(Err(e)) => {
                            error!("Error during Draining: {e}");
                        }
                        None | Some(_) => {}
                    };
                    alive_flows -= 1;
                }
                info!("Flows drained.");
                sender.send(Ok(())).map_err(|_| "Failed to send reply")?;
                Result::Ok(())
            });
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
                    Msg::StartDeploy { flow, sender } => {
                        self.handle_start_deploy(*flow, sender, &task_kill_switch)
                            .await;
                    }
                    Msg::GetFlows(reply_tx) => self.handle_get_flows(reply_tx),
                    Msg::GetFlow(id, reply_tx) => self.handle_get_flow(&id, reply_tx),
                    Msg::Stop => {
                        self.handle_stop().await?;
                        break;
                    }
                    Msg::Drain(sender) => self.handle_drain(sender).await,
                }
            }
            info!("Manager stopped.");
            Ok(())
        });
        (system_h, tx, kill_switch)
    }
}
