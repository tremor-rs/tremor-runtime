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

mod deployment;

use crate::connectors::{self, ConnectorBuilder};
use crate::errors::{Error, Kind as ErrorKind, Result};
use crate::QSIZE;
use async_std::channel::{bounded, Sender};
use async_std::prelude::*;
use async_std::task::{self, JoinHandle};
use deployment::{Deployment, DeploymentId};
use hashbrown::{hash_map::Entry, HashMap};
use std::sync::atomic::Ordering;
use std::time::Duration;
use tremor_common::ids::{ConnectorIdGen, OperatorIdGen};
use tremor_script::{highlighter::Highlighter, srs::DeployFlow};

/// Configuration for the runtime
pub struct WorldConfig {
    /// default size for queues
    pub qsize: usize,
    /// the storage directory
    pub storage_directory: Option<String>,
    /// if debug connectors should be loaded
    pub debug_connectors: bool,
}
impl Default for WorldConfig {
    fn default() -> Self {
        Self {
            qsize: QSIZE.load(Ordering::Relaxed),
            storage_directory: None,
            debug_connectors: false,
        }
    }
}

/// default graceful shutdown timeout
pub const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq)]
/// shutdown mode - controls how we shutdown Tremor
pub enum ShutdownMode {
    /// shut down by stopping all binding instances and wait for quiescence
    Graceful,
    /// Just stop everything and not wait
    Forceful,
}

/// This is control plane
pub(crate) enum ManagerMsg {
    ///add a deployment
    StartDeploy {
        /// deploy source
        src: String,
        /// deploy flow
        flow: DeployFlow,
        /// result sender
        sender: Sender<Result<()>>
    },
    RegisterConnectorType {
        /// the type of connector
        connector_type: ConnectorType,
        /// the builder
        builder: Box<dyn ConnectorBuilder>,
    },
    /// Initiate the Quiescence process
    Drain(Sender<Result<()>>),
    /// stop this manager
    Stop,
}
use async_std::channel::Sender as AsyncSender;

use self::connectors::{ConnectorType, KnownConnectors};
pub(crate) type ManagerSender = AsyncSender<ManagerMsg>;

#[derive(Debug)]
pub(crate) struct Manager {
    deployments: HashMap<DeploymentId, Deployment>,
    operator_id_gen: OperatorIdGen,
    connector_id_gen: ConnectorIdGen,
    known_connectors: KnownConnectors,
    pub qsize: usize,
}

impl Manager {
    pub fn start(mut self) -> (JoinHandle<Result<()>>, ManagerSender) {
        let (tx, rx) = bounded(self.qsize);
        let system_h = task::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                match msg {
                    ManagerMsg::RegisterConnectorType {
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
                    ManagerMsg::StartDeploy { src, flow, sender } => {
                        let id = DeploymentId::from(&flow);
                        let res = match self.deployments.entry(id.clone()) {
                            Entry::Occupied(_occupied) => {
                                Err(ErrorKind::DuplicateFlow(id.0.clone()).into())      
                            }
                            Entry::Vacant(vacant) => {
                                let res = Deployment::start(
                                    src.clone(),
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
                                    Err(e) => {
                                        Err(e)
                                    }
                                }
                            }
                        };
                        if sender.send(res).await.is_err() {
                            error!("Error sending StartDeploy Err Result");
                        }
                    }
                    ManagerMsg::Stop => {
                        info!("Stopping Manager ...");
                        let num_deployments = self.deployments.len();
                        if num_deployments > 0 {
                            // send stop to each deployment
                            let (tx, rx) = bounded(self.deployments.len());
                            let mut expected_stops = self.deployments.len();
                            for (_, deployment) in self.deployments {
                                deployment.stop(tx.clone()).await?;
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
                    ManagerMsg::Drain(sender) => {
                        let num_deployments = self.deployments.len();
                        if num_deployments == 0 {
                            sender.send(Ok(())).await?;
                        } else {
                            info!("Draining all Flows ...");
                            let (tx, rx) = bounded(num_deployments);
                            for (_, deployment) in &self.deployments {
                                deployment.drain(tx.clone()).await?;
                            }

                            task::spawn::<_, Result<()>>(async move {
                                let rx_futures =
                                    std::iter::repeat_with(|| rx.recv()).take(num_deployments);
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

/// Tremor runtime
#[derive(Clone, Debug)]
pub struct World {
    pub(crate) system: ManagerSender,
    storage_directory: Option<String>,
}

impl World {
    /// Instantiate a flow from
    pub(crate) async fn start_deploy(&self, src: &str, flow: &DeployFlow) -> Result<()> {
        let (tx, rx) = bounded(1);
        self.system
            .send(ManagerMsg::StartDeploy {
                src: src.to_string(),
                flow: flow.clone(),
                sender: tx
            })
            .await?;
        if let Err(e) = rx.recv().await? {
            let err_str = match e {
                Error(ErrorKind::Script(e)
                | ErrorKind::Pipeline(
                    tremor_pipeline::errors::ErrorKind::Script(e)), _) => {
                    let mut h = crate::ToStringHighlighter::new();
                    tremor_script::query::Query::format_error_from_script(
                        &src,
                        &mut h,
                        &tremor_script::errors::Error::from(e),
                    )?;
                    h.finalize()?;
                    h.to_string()
                }
                err => {
                    err.to_string()
                }
            };
            error!("Error starting deployment of flow {}: {}", flow.instance_id.id(), &err_str);
            Err(ErrorKind::DeployFlowError(flow.instance_id.id().to_string(), err_str).into())
        } else {
            Ok(())
        }
    }
    /// Registers the given connector type with `type_name` and the corresponding `builder`
    ///
    /// # Errors
    ///  * If the system is unavailable
    pub(crate) async fn register_builtin_connector_type(
        &self,
        builder: Box<dyn connectors::ConnectorBuilder>,
    ) -> Result<()> {
        self.system
            .send(ManagerMsg::RegisterConnectorType {
                connector_type: builder.connector_type(),
                builder,
            })
            .await?;
        Ok(())
    }

    /// Starts the runtime system
    ///
    /// # Errors
    ///  * if the world manager can't be started
    pub async fn start(config: WorldConfig) -> Result<(Self, JoinHandle<Result<()>>)> {
        let (system_h, system) = Manager {
            deployments: HashMap::new(),
            known_connectors: KnownConnectors::new(),
            operator_id_gen: OperatorIdGen::new(),
            connector_id_gen: ConnectorIdGen::new(),
            qsize: config.qsize,
        }
        .start();

        let world = Self {
            system,
            storage_directory: config.storage_directory,
        };

        connectors::register_builtin_connector_types(&world, config.debug_connectors).await?;
        Ok((world, system_h))
    }

    /// Drain the runtime
    ///
    /// # Errors
    ///  * if the system failed to drain
    pub async fn drain(&self, timeout: Duration) -> Result<()> {
        let (tx, rx) = bounded(1);
        self.system.send(ManagerMsg::Drain(tx)).await?;
        match rx.recv().timeout(timeout).await {
            Err(_) => {
                warn!("Timeout draining all Flows after {}s", timeout.as_secs());
                Ok(())
            }
            Ok(res) => res?,
        }
    }

    /// Stop the runtime
    ///
    /// # Errors
    ///  * if the system failed to stop
    pub async fn stop(&self, mode: ShutdownMode) -> Result<()> {
        match mode {
            ShutdownMode::Graceful => {
                if let Err(e) = self.drain(DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT).await {
                    error!("Error draining all Flows: {}", e);
                }
            }
            ShutdownMode::Forceful => {}
        }
        if let Err(e) = self.system.send(ManagerMsg::Stop).await {
            error!("Error stopping all Flows: {}", e);
        }
        Ok(self.system.send(ManagerMsg::Stop).await?)
    }
}
