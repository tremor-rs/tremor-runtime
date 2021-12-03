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

use crate::connectors::ConnectorBuilder;
use crate::errors::Result;
use crate::QSIZE;
use async_std::channel::bounded;
use async_std::task::{self, JoinHandle};
use deployment::{Deployment, DeploymentId};
use hashbrown::HashMap;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tremor_common::ids::{ConnectorIdGen, OperatorIdGen};
use tremor_script::srs::DeployFlow;

pub(crate) use crate::connectors;

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
    },
    RegisterConnectorType {
        /// the type of connector
        connector_type: ConnectorType,
        /// the builder
        builder: Box<dyn ConnectorBuilder>,
    },
    /// stop this manager
    Stop,
}
use async_std::channel::Sender as AsyncSender;

use self::connectors::{ConnectorType, KnownConnectors};
pub(crate) type Sender = AsyncSender<ManagerMsg>;

#[derive(Debug)]
pub(crate) struct Manager {
    deployments: HashMap<DeploymentId, Deployment>,
    operator_id_gen: OperatorIdGen,
    connector_id_gen: ConnectorIdGen,
    known_connectors: KnownConnectors,
    pub qsize: usize,
}

impl Manager {
    pub fn start(mut self) -> (JoinHandle<Result<()>>, Sender) {
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
                    ManagerMsg::StartDeploy { src, flow } => {
                        let id = DeploymentId::from(&flow);

                        let deploy = Deployment::start(
                            src,
                            flow,
                            &mut self.operator_id_gen,
                            &mut self.connector_id_gen,
                            &self.known_connectors,
                        )
                        .await?;

                        if self.deployments.insert(id.clone(), deploy).is_some() {
                            error!("FIXME: error on duplicate deployments: {:?}", id)
                        };
                    }
                    ManagerMsg::Stop => {
                        info!("Stopping Manager ...");

                        for _deployment in self.deployments {
                            // FIXME: stop
                        }
                        break;
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
    pub(crate) system: Sender,
    storage_directory: Option<String>,
}

impl World {
    pub(crate) async fn start_deploy(&self, src: &str, flow: &DeployFlow) -> Result<()> {
        self.system
            .send(ManagerMsg::StartDeploy {
                src: src.to_string(),
                flow: flow.clone(),
            })
            .await?;

        Ok(())
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

        crate::connectors::register_builtin_connector_types(&world).await?;
        if config.debug_connectors {
            crate::connectors::register_debug_connector_types(&world).await?;
        }
        Ok((world, system_h))
    }

    /// Stop the runtime
    ///
    /// # Errors
    ///  * if the system failed to stop
    pub async fn stop(&self, mode: ShutdownMode) -> Result<()> {
        match mode {
            ShutdownMode::Graceful => {
                todo!("FIXME: relaly fix me by shutting down the deployments")
            }
            ShutdownMode::Forceful => {}
        }
        // if let Err(e) = self
        //     .reg
        //     .stop_all_bindings(DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT)
        //     .await
        // {
        //     error!("Error stopping all bindings: {}", e);
        // }
        Ok(self.system.send(ManagerMsg::Stop).await?)
    }
}
