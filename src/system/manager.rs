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

use super::flow::{Flow, FlowId};
use crate::connectors::{ConnectorBuilder, ConnectorType, KnownConnectors};
use crate::errors::{Kind as ErrorKind, Result};
use crate::system::DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT;
use async_std::channel::{bounded, Sender};
use async_std::prelude::*;
use async_std::task::{self, JoinHandle};
use hashbrown::{hash_map::Entry, HashMap};
use tremor_common::ids::{ConnectorIdGen, OperatorIdGen};
use tremor_script::ast::DeployFlow;

pub(crate) type Channel = Sender<Msg>;

/// This is control plane
pub(crate) enum Msg {
    /// deploy a Flow
    StartDeploy {
        /// deploy flow
        flow: DeployFlow<'static>,
        /// result sender
        sender: Sender<Result<()>>,
    },
    RegisterConnectorType {
        /// the type of connector
        connector_type: ConnectorType,
        /// the builder
        builder: Box<dyn ConnectorBuilder>,
    },
    GetFlows(Sender<Result<Vec<Flow>>>),
    GetFlow(FlowId, Sender<Result<Flow>>),
    /// Initiate the Quiescence process
    Drain(Sender<Result<()>>),
    /// stop this manager
    Stop,
}

// FIXME: better name, Manager sounds stupid
#[derive(Debug)]
pub(crate) struct Manager {
    flows: HashMap<FlowId, Flow>,
    operator_id_gen: OperatorIdGen,
    connector_id_gen: ConnectorIdGen,
    known_connectors: KnownConnectors,
    qsize: usize,
}

impl Manager {
    pub fn new(qsize: usize) -> Self {
        Self {
            flows: HashMap::new(),
            known_connectors: KnownConnectors::new(),
            operator_id_gen: OperatorIdGen::new(),
            connector_id_gen: ConnectorIdGen::new(),
            qsize,
        }
    }

    pub fn start(mut self) -> (JoinHandle<Result<()>>, Channel) {
        let (tx, rx) = bounded(self.qsize);
        let system_h = task::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                match msg {
                    Msg::RegisterConnectorType {
                        connector_type,
                        builder,
                        ..
                    } => {
                        if let Some(old) = self
                            .known_connectors
                            .insert(connector_type.clone(), builder)
                        {
                            error!(
                                "FIXME: error on duplicate connectors: {}",
                                old.connector_type()
                            );
                        }
                    }
                    Msg::StartDeploy { flow, sender } => {
                        let id = FlowId::from(&flow);
                        let res = match self.flows.entry(id.clone()) {
                            Entry::Occupied(_occupied) => {
                                Err(ErrorKind::DuplicateFlow(id.0.clone()).into())
                            }
                            Entry::Vacant(vacant) => {
                                let res = Flow::start(
                                    flow,
                                    &mut self.operator_id_gen,
                                    &mut self.connector_id_gen,
                                    &self.known_connectors,
                                )
                                .await;
                                match res {
                                    Ok(deploy) => {
                                        vacant.insert(deploy);
                                        Ok(())
                                    }
                                    Err(e) => Err(e),
                                }
                            }
                        };
                        if sender.send(res).await.is_err() {
                            error!("Error sending StartDeploy Err Result");
                        }
                    }
                    Msg::GetFlows(reply_tx) => {
                        let flows = self.flows.values().cloned().collect();
                        if reply_tx.send(Ok(flows)).await.is_err() {
                            error!("Error sending ListFlows response");
                        }
                    }
                    Msg::GetFlow(id, reply_tx) => {
                        if reply_tx
                            .send(
                                self.flows
                                    .get(&id)
                                    .cloned()
                                    .ok_or_else(|| ErrorKind::FlowNotFound(id.0).into()),
                            )
                            .await
                            .is_err()
                        {
                            error!("Error sending GetFlow response");
                        }
                    }
                    Msg::Stop => {
                        info!("Stopping Manager ...");
                        let num_flows = self.flows.len();
                        if num_flows > 0 {
                            // send stop to each deployment
                            let (tx, rx) = bounded(self.flows.len());
                            let mut expected_stops = self.flows.len();
                            for (_, flow) in self.flows {
                                if let Err(e) = flow.stop(tx.clone()).await {
                                    error!(
                                        "Failed to stop Deployment \"{alias}\": {e}",
                                        alias = flow.alias()
                                    );
                                    expected_stops = expected_stops.saturating_sub(1);
                                }
                            }
                            let h = task::spawn::<_, Result<()>>(async move {
                                while expected_stops > 0 {
                                    let res = rx.recv().await?;
                                    if let Err(e) = res {
                                        error!("Error during Stopping: {}", e);
                                    }
                                    expected_stops = expected_stops.saturating_sub(1);
                                }
                                Ok(())
                            });
                            h.timeout(DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT).await??;
                        }
                        break;
                    }
                    Msg::Drain(sender) => {
                        let num_flows = self.flows.len();
                        if num_flows == 0 {
                            sender.send(Ok(())).await?;
                        } else {
                            info!("Draining all {num_flows} Flows ...");
                            let mut alive_flows = 0_usize;
                            let (tx, rx) = bounded(num_flows);
                            for (_, flow) in &self.flows {
                                if let Err(e) = flow.drain(tx.clone()).await {
                                    error!(
                                        "Failed to drain Deployment \"{alias}\": {e}",
                                        alias = flow.alias()
                                    )
                                } else {
                                    alive_flows += 1;
                                }
                            }

                            task::spawn::<_, Result<()>>(async move {
                                let rx_futures =
                                    std::iter::repeat_with(|| rx.recv()).take(alive_flows);
                                for result in futures::future::join_all(rx_futures).await {
                                    match result {
                                        Err(_) => {
                                            error!("Error receiving from Draining process.");
                                        }
                                        Ok(Err(e)) => {
                                            error!("Error during Draining: {}", e);
                                        }
                                        Ok(Ok(())) => {}
                                    }
                                }
                                info!("Flows drained.");
                                sender.send(Ok(())).await?;
                                Ok(())
                            });
                        }
                    }
                }
            }
            info!("Manager stopped.");
            Ok(())
        });
        (system_h, tx)
    }
}
