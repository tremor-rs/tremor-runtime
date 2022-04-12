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

use std::time::Duration;

use crate::config::{BindingVec, Config, MappingMap, OffRampVec, OnRampVec};
use crate::errors::{Error, Kind as ErrorKind, Result};
use crate::lifecycle::{ActivationState, ActivatorLifecycleFsm};
use crate::registry::{Registries, ServantId};
use crate::repository::{
    Artefact, BindingArtefact, ConnectorArtefact, OfframpArtefact, OnrampArtefact,
    PipelineArtefact, Repositories,
};
use crate::url::ports::METRICS;
use crate::url::TremorUrl;
use crate::OpConfig;
use async_channel::bounded;
use async_std::task::{self, JoinHandle};
use hashbrown::HashMap;

pub(crate) use crate::connectors;
pub(crate) use crate::offramp;
pub(crate) use crate::onramp;
pub(crate) use crate::pipeline;

use connectors::metrics::MetricsChannel;

lazy_static! {

    pub(crate) static ref METRICS_CONNECTOR: TremorUrl = {
        TremorUrl::parse("/connector/system::metrics/system/in")
        //ALLOW: We want this to panic, it only happens at startup time
        .expect("Failed to initialize id for metrics connector")
    };
    pub(crate) static ref STDOUT_CONNECTOR: TremorUrl = {
        TremorUrl::parse("/connector/system::stdout/system/in")
            //ALLOW: We want this to panic, it only happens at startup time
            .expect("Failed to initialize id for stdout connector")
    };
    pub(crate) static ref STDERR_CONNECTOR: TremorUrl = {
        TremorUrl::parse("/connector/system::stderr/system/in")
            //ALLOW: We want this to panic, it only happens at startup time
            .expect("Failed to initialize id for stderr connector")
    };
    pub(crate) static ref STDIN_CONNECTOR: TremorUrl = {
        TremorUrl::parse("/connector/system::stdin/system/out")
            //ALLOW: We want this to panic, it only happens at startup time
            .expect("Failed to initialize id for stderr connector")
    };
    pub(crate) static ref METRICS_PIPELINE: TremorUrl = {
        TremorUrl::parse("/pipeline/system::metrics/system/in")
            //ALLOW: We want this to panic, it only happens at startup time
            .expect("Failed to initialize id for metrics piepline")
    };
    pub(crate) static ref PASSTHROUGH_PIPELINE: TremorUrl = {
        TremorUrl::parse("/pipeline/system::passthrough/system/in")
            //ALLOW: We want this to panic, it only happens at startup time
            .expect("Failed to initialize id for metrics piepline")
    };
    pub(crate) static ref STDOUT_OFFRAMP: TremorUrl = {
        TremorUrl::parse("/offramp/system::stdout/system/in")
            //ALLOW: We want this to panic, it only happens at startup time
            .expect("Failed to initialize id for stdout offramp")
    };
    pub(crate) static ref STDERR_OFFRAMP: TremorUrl = {
        TremorUrl::parse("/offramp/system::stderr/system/in")
            //ALLOW: We want this to panic, it only happens at startup time
            .expect("Failed to initialize id for stderr offramp")
    };
}

/// This is control plane
pub(crate) enum ManagerMsg {
    Pipeline(pipeline::ManagerMsg),
    // CreatePipeline(
    //     async_channel::Sender<Result<pipeline::Addr>>,
    //     pipeline::Create,
    // ),
    Onramp(onramp::ManagerMsg),
    // CreateOnramp(
    //     async_channel::Sender<Result<onramp::Addr>>,
    //     Box<onramp::Create>,
    // ),
    Offramp(offramp::ManagerMsg),
    // CreateOfframp(
    //     async_channel::Sender<Result<offramp::Addr>>,
    //     Box<offramp::Create>,
    // ),
    Connector(connectors::ManagerMsg),
    // CreateConnector(
    //     async_channel::Sender<Result<connectors::Addr>>,
    //     connectors::Create,
    // ),
    Stop,
}

pub(crate) type Sender = async_channel::Sender<ManagerMsg>;

#[derive(Debug)]
pub(crate) struct Manager {
    pub connector: connectors::Sender,
    pub offramp: offramp::Sender,
    pub onramp: onramp::Sender,
    pub pipeline: pipeline::Sender,
    pub connector_h: JoinHandle<Result<()>>,
    pub offramp_h: JoinHandle<Result<()>>,
    pub onramp_h: JoinHandle<Result<()>>,
    pub pipeline_h: JoinHandle<Result<()>>,
    pub qsize: usize,
}

impl Manager {
    pub fn start(self) -> (JoinHandle<Result<()>>, Sender) {
        let (tx, rx) = bounded(crate::QSIZE);
        let system_h = task::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                match msg {
                    ManagerMsg::Pipeline(msg) => self.pipeline.send(msg).await?,
                    ManagerMsg::Onramp(msg) => self.onramp.send(msg).await?,
                    ManagerMsg::Offramp(msg) => self.offramp.send(msg).await?,
                    ManagerMsg::Connector(msg) => self.connector.send(msg).await?,
                    ManagerMsg::Stop => {
                        info!("Stopping offramps...");
                        self.offramp.send(offramp::ManagerMsg::Stop).await?;
                        info!("Stopping pipelines...");
                        self.pipeline.send(pipeline::ManagerMsg::Stop).await?;
                        info!("Stopping onramps...");
                        self.onramp.send(onramp::ManagerMsg::Stop).await?;
                        info!("Stopping connectors...");
                        self.connector
                            .send(connectors::ManagerMsg::Stop {
                                reason: "Global Manager Stop".to_string(),
                            })
                            .await?;
                        break;
                    }
                }
            }
            info!("Stopping onramps in an odd way...");
            Ok(())
        });
        (system_h, tx)
    }
}

/// Tremor runtime
#[derive(Clone, Debug)]
pub struct World {
    pub(crate) system: Sender,
    /// Repository
    pub repo: Repositories,
    /// Registry
    pub reg: Registries,
    pub(crate) metrics_channel: MetricsChannel,
}

impl World {
    /// Registers the given builtin onramp type with `type_name` and the corresponding `builder` to instantiate new onramps
    pub(crate) async fn register_builtin_onramp_type(
        &self,
        type_name: &'static str,
        builder: Box<dyn onramp::Builder>,
    ) -> Result<()> {
        self.system
            .send(ManagerMsg::Onramp(onramp::ManagerMsg::Register {
                onramp_type: type_name.to_string(),
                builder,
                builtin: true,
            }))
            .await?;
        Ok(())
    }

    /// Registers the given onramp type with `type_name` and the corresponding `builder` to instantiate new onramps
    pub async fn register_onramp_type(
        &self,
        type_name: &'static str,
        builder: Box<dyn onramp::Builder>,
    ) -> Result<()> {
        self.system
            .send(ManagerMsg::Onramp(onramp::ManagerMsg::Register {
                onramp_type: type_name.to_string(),
                builder,
                builtin: false,
            }))
            .await?;
        Ok(())
    }

    /// unregister onramp type
    pub async fn unregister_onramp_type(&self, type_name: String) -> Result<()> {
        self.system
            .send(ManagerMsg::Onramp(onramp::ManagerMsg::Unregister(
                type_name,
            )))
            .await?;
        Ok(())
    }

    /// returns true if the runtime currently supports the given onramp type
    pub async fn has_onramp_type(&self, type_name: String) -> Result<bool> {
        let (tx, rx) = bounded(1);
        self.system
            .send(ManagerMsg::Onramp(onramp::ManagerMsg::TypeExists(
                type_name, tx,
            )))
            .await?;
        Ok(rx.recv().await?)
    }

    /// Registers the given builtin offramp type with `type_name` and the corresponding `builder` to instantiate new offramps
    pub(crate) async fn register_builtin_offramp_type(
        &self,
        type_name: &'static str,
        builder: Box<dyn offramp::Builder>,
    ) -> Result<()> {
        self.system
            .send(ManagerMsg::Offramp(offramp::ManagerMsg::Register {
                offramp_type: type_name.to_string(),
                builder,
                builtin: true,
            }))
            .await?;
        Ok(())
    }

    /// Registers the given offramp type with `type_name` and the corresponding `builder` to instantiate new offramps
    pub async fn register_offramp_type(
        &self,
        type_name: &'static str,
        builder: Box<dyn offramp::Builder>,
    ) -> Result<()> {
        self.system
            .send(ManagerMsg::Offramp(offramp::ManagerMsg::Register {
                offramp_type: type_name.to_string(),
                builder,
                builtin: false,
            }))
            .await?;
        Ok(())
    }

    /// unregister offramp type
    pub async fn unregister_offramp_type(&self, type_name: String) -> Result<()> {
        self.system
            .send(ManagerMsg::Offramp(offramp::ManagerMsg::Unregister(
                type_name,
            )))
            .await?;
        Ok(())
    }

    /// returns true if the runtime currently supports the given offramp type
    pub async fn has_offramp_type(&self, type_name: String) -> Result<bool> {
        let (tx, rx) = bounded(1);
        self.system
            .send(ManagerMsg::Offramp(offramp::ManagerMsg::TypeExists(
                type_name, tx,
            )))
            .await?;
        Ok(rx.recv().await?)
    }

    /// Registers the given connector type with `type_name` and the corresponding `builder`
    pub async fn register_builtin_connector_type(
        &self,
        type_name: &'static str,
        builder: Box<dyn connectors::ConnectorBuilder>,
    ) -> Result<()> {
        self.system
            .send(ManagerMsg::Connector(connectors::ManagerMsg::Register {
                connector_type: type_name.to_string(),
                builder,
                builtin: true,
            }))
            .await?;
        Ok(())
    }
    /// Registers the given connector type with `type_name` and the corresponding `builder`
    pub async fn register_connector_type(
        &self,
        type_name: &'static str,
        builder: Box<dyn connectors::ConnectorBuilder>,
    ) -> Result<()> {
        self.system
            .send(ManagerMsg::Connector(connectors::ManagerMsg::Register {
                connector_type: type_name.to_string(),
                builder,
                builtin: false,
            }))
            .await?;
        Ok(())
    }

    /// unregister a connector type
    pub async fn unregister_connector_type(&self, type_name: String) -> Result<()> {
        self.system
            .send(ManagerMsg::Connector(connectors::ManagerMsg::Unregister(
                type_name,
            )))
            .await?;
        Ok(())
    }

    /// Ensures the existance of an onramp, creating it if required.
    ///
    /// # Errors
    ///  * if we can't ensure the onramp is bound
    pub async fn ensure_onramp(&self, id: &TremorUrl) -> Result<()> {
        if self.reg.find_onramp(id).await?.is_none() {
            info!(
                "Onramp not found during binding process, binding {} to create a new instance.",
                &id
            );
            self.bind_onramp(id).await?;
        } else {
            info!("Existing onramp {} found", id);
        }
        Ok(())
    }

    /// Ensures the existance of an offramp, creating it if required.
    ///
    /// # Errors
    ///  * if we can't ensure the offramp is bound
    pub async fn ensure_offramp(&self, id: &TremorUrl) -> Result<()> {
        if self.reg.find_offramp(id).await?.is_none() {
            info!(
                "Offramp not found during binding process, binding {} to create a new instance.",
                &id
            );
            self.bind_offramp(id).await?;
        } else {
            info!("Existing offramp {} found", id);
        }
        Ok(())
    }
    /// Ensures the existance of an pipeline, creating it if required.
    ///
    /// # Errors
    ///  * if we can't ensure the pipeline is bound
    pub async fn ensure_pipeline(&self, id: &TremorUrl) -> Result<()> {
        if self.reg.find_pipeline(id).await?.is_none() {
            info!(
                "Pipeline not found during binding process, binding {} to create a new instance.",
                &id
            );
            self.bind_pipeline(id).await?;
        } else {
            info!("Existing pipeline {} found", id);
        }
        Ok(())
    }

    /// Bind a pipeline
    ///
    /// # Errors
    ///  * if the id isn't a pipeline instance or can't be bound
    pub async fn bind_pipeline(&self, id: &TremorUrl) -> Result<ActivationState> {
        info!("Binding pipeline {}", id);
        match (&self.repo.find_pipeline(id).await?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let servant =
                    ActivatorLifecycleFsm::new(self.clone(), artefact.artefact.clone(), id.clone())
                        .await?;
                self.repo.bind_pipeline(id).await?;
                // We link to the metrics pipeline
                let res = self.reg.publish_pipeline(id, servant).await?;
                let mut id = id.clone();
                id.set_port(&METRICS);
                let m = vec![(METRICS.to_string(), METRICS_PIPELINE.clone())]
                    .into_iter()
                    .collect();
                self.link_existing_pipeline(&id, m).await?;
                Ok(res)
            }
            (None, _) => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
            (_, None) => Err(ErrorKind::InvalidInstanceUrl(id.to_string()).into()),
        }
    }

    /// Unbind a pipeline
    ///
    /// # Errors
    ///  * if the id isn't an pipeline instance or the pipeline can't be unbound
    pub async fn unbind_pipeline(&self, id: &TremorUrl) -> Result<ActivationState> {
        info!("Unbinding pipeline {}", id);
        match (&self.repo.find_pipeline(id).await?, &id.instance()) {
            (Some(_artefact), Some(_instance_id)) => {
                let r = self.reg.unpublish_pipeline(id).await?;
                self.repo.unbind_pipeline(id).await?;
                Ok(r)
            }
            (None, _) => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
            (_, None) => Err(ErrorKind::InvalidInstanceUrl(id.to_string()).into()),
        }
    }

    /// Stop the runtime
    ///
    /// # Errors
    ///  * if the system failed to stop
    pub async fn stop(&self) -> Result<()> {
        Ok(self.system.send(ManagerMsg::Stop).await?)
    }
    /// Links a pipeline
    ///
    /// # Errors
    ///  * if the id isn't a pipeline or the pipeline can't be linked
    pub async fn link_pipeline(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <PipelineArtefact as Artefact>::LinkLHS,
            <PipelineArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<PipelineArtefact as Artefact>::LinkResult> {
        info!(
            "Linking pipeline {} to {}",
            id,
            mappings
                .iter()
                .map(|(port, url)| format!("{} -> {}", port, url))
                .collect::<Vec<_>>()
                .join(", ")
        );
        if let Some(pipeline_a) = self.repo.find_pipeline(id).await? {
            if self.reg.find_pipeline(id).await?.is_none() {
                self.bind_pipeline(id).await?;
            };
            pipeline_a.artefact.link(self, id, mappings).await
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    /// Links a pipeline
    async fn link_existing_pipeline(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <PipelineArtefact as Artefact>::LinkLHS,
            <PipelineArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<PipelineArtefact as Artefact>::LinkResult> {
        info!(
            "Linking pipeline {}\n\t{}",
            id,
            mappings
                .iter()
                .map(|(port, url)| format!("{} -> {}", port, url))
                .collect::<Vec<_>>()
                .join("\n\t")
        );
        if let Some(pipeline_a) = self.repo.find_pipeline(id).await? {
            pipeline_a.artefact.link(self, id, mappings).await
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    /// Unlink a pipelein
    ///
    /// # Errors
    ///  * if the id isn't a pipeline or the pipeline can't be unlinked
    pub async fn unlink_pipeline(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <PipelineArtefact as Artefact>::LinkLHS,
            <PipelineArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<PipelineArtefact as Artefact>::LinkResult> {
        if let Some(pipeline_a) = self.repo.find_pipeline(id).await? {
            let r = pipeline_a.artefact.unlink(self, id, mappings).await;
            if self.reg.find_pipeline(id).await?.is_some() {
                self.unbind_pipeline(id).await?;
            };
            r
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    #[cfg(test)]
    pub async fn bind_pipeline_from_artefact(
        &self,
        id: &TremorUrl,
        artefact: PipelineArtefact,
    ) -> Result<ActivationState> {
        self.repo.publish_pipeline(id, false, artefact).await?;
        self.bind_pipeline(id).await
    }
    /// Bind an onramp
    ///
    /// # Errors
    ///  * if the id isn't a onramp instance or the onramp can't be bound
    pub async fn bind_onramp(&self, id: &TremorUrl) -> Result<ActivationState> {
        info!("Binding onramp {}", id);
        match (&self.repo.find_onramp(id).await?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let servant =
                    ActivatorLifecycleFsm::new(self.clone(), artefact.artefact.clone(), id.clone())
                        .await?;
                self.repo.bind_onramp(id).await?;
                // We link to the metrics pipeline
                let res = self.reg.publish_onramp(id, servant).await?;
                let mut id = id.clone();
                id.set_port(&METRICS);
                let m = vec![(METRICS.to_string(), METRICS_PIPELINE.clone())]
                    .into_iter()
                    .collect();
                self.link_existing_onramp(&id, m).await?;
                Ok(res)
            }
            (None, _) => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
            (_, None) => Err(ErrorKind::InvalidInstanceUrl(id.to_string()).into()),
        }
    }
    /// Unbind an onramp
    ///
    /// # Errors
    ///  * if the id isn't an onramp or the onramp can't be unbound
    pub async fn unbind_onramp(&self, id: &TremorUrl) -> Result<ActivationState> {
        info!("Unbinding onramp {}", id);
        match (&self.repo.find_onramp(id).await?, &id.instance()) {
            (Some(_artefact), Some(_instsance_id)) => {
                let r = self.reg.unpublish_onramp(id).await;
                self.repo.unbind_onramp(id).await?;
                r
            }
            (None, _) => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
            (_, None) => Err(ErrorKind::InvalidInstanceUrl(id.to_string()).into()),
        }
    }

    /// Link an onramp
    ///
    /// # Errors
    ///  * if the id isn't an onramp or the onramp can't be linked
    pub async fn link_onramp(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <OnrampArtefact as Artefact>::LinkLHS,
            <OnrampArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<OnrampArtefact as Artefact>::LinkResult> {
        if let Some(onramp_a) = self.repo.find_onramp(id).await? {
            if self.reg.find_onramp(id).await?.is_none() {
                self.bind_onramp(id).await?;
            };
            onramp_a.artefact.link(self, id, mappings).await
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    async fn link_existing_onramp(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <OnrampArtefact as Artefact>::LinkLHS,
            <OnrampArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<OnrampArtefact as Artefact>::LinkResult> {
        if let Some(onramp_a) = self.repo.find_onramp(id).await? {
            onramp_a.artefact.link(self, id, mappings).await
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    /// Unlink an onramp
    ///
    /// # Errors
    ///  * if the id isn't a onramp or it cna't be unlinked
    pub async fn unlink_onramp(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <OnrampArtefact as Artefact>::LinkLHS,
            <OnrampArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<OnrampArtefact as Artefact>::LinkResult> {
        if let Some(onramp_a) = self.repo.find_onramp(id).await? {
            let r = onramp_a.artefact.unlink(self, id, mappings).await?;
            if r {
                self.unbind_onramp(id).await?;
            };
            Ok(r)
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    /// Bind an offramp
    ///
    /// # Errors
    ///  * if the id isn't a offramp instance or it can't be bound
    pub async fn bind_offramp(&self, id: &TremorUrl) -> Result<ActivationState> {
        info!("Binding offramp {}", id);
        match (&self.repo.find_offramp(id).await?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let servant =
                    ActivatorLifecycleFsm::new(self.clone(), artefact.artefact.clone(), id.clone())
                        .await?;
                self.repo.bind_offramp(id).await?;
                // We link to the metrics pipeline
                let res = self.reg.publish_offramp(id, servant).await?;
                let mut metrics_id = id.clone();
                metrics_id.set_port(&METRICS);
                let m = vec![(METRICS_PIPELINE.clone(), metrics_id.clone())]
                    .into_iter()
                    .collect();
                self.link_existing_offramp(id, m).await?;
                Ok(res)
            }
            (None, _) => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
            (_, None) => Err(ErrorKind::InvalidInstanceUrl(id.to_string()).into()),
        }
    }

    /// Unbind an offramp
    ///
    /// # Errors
    ///  * if the id isn't an offramp instance or the offramp can't be unbound
    pub async fn unbind_offramp(&self, id: &TremorUrl) -> Result<ActivationState> {
        info!("Unbinding offramp {} ..", id);
        match (&self.repo.find_offramp(id).await?, &id.instance()) {
            (Some(_artefact), Some(_instsance_id)) => {
                let r = self.reg.unpublish_offramp(id).await;
                self.repo.unbind_offramp(id).await?;
                r
            }
            (None, _) => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
            (_, None) => Err(ErrorKind::InvalidInstanceUrl(id.to_string()).into()),
        }
    }

    /// Link an offramp
    ///
    /// # Errors
    ///  * if the id isn't an offramp or can't be linked
    pub async fn link_offramp(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <OfframpArtefact as Artefact>::LinkLHS,
            <OfframpArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<OfframpArtefact as Artefact>::LinkResult> {
        if let Some(offramp_a) = self.repo.find_offramp(id).await? {
            if self.reg.find_offramp(id).await?.is_none() {
                self.bind_offramp(id).await?;
            };
            offramp_a.artefact.link(self, id, mappings).await
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    async fn link_existing_offramp(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <OfframpArtefact as Artefact>::LinkLHS,
            <OfframpArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<OfframpArtefact as Artefact>::LinkResult> {
        if let Some(offramp_a) = self.repo.find_offramp(id).await? {
            offramp_a.artefact.link(self, id, mappings).await
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    /// Unlink an offramp
    ///
    /// # Errors
    ///  * if the id isn't an offramp or it cna't be unlinked
    pub async fn unlink_offramp(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <OfframpArtefact as Artefact>::LinkLHS,
            <OfframpArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<OfframpArtefact as Artefact>::LinkResult> {
        if let Some(offramp_a) = self.repo.find_offramp(id).await? {
            let r = offramp_a.artefact.unlink(self, id, mappings).await?;
            if r {
                self.unbind_offramp(id).await?;
            };
            Ok(r)
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    /// Bind a connector - create an instance and stick it into the registry
    ///
    /// # Errors
    ///  * if the id isn't a connector instance or it can't be bound
    pub async fn bind_connector(&self, id: &TremorUrl) -> Result<ActivationState> {
        info!("Binding connector {}", id);
        match (&self.repo.find_connector(id).await?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let servant =
                    ActivatorLifecycleFsm::new(self.clone(), artefact.artefact.clone(), id.clone())
                        .await?;
                self.repo.bind_connector(id).await?;
                let res = self.reg.publish_connector(id, servant).await?;
                Ok(res)
            }
            (None, _) => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
            (_, None) => Err(ErrorKind::InvalidInstanceUrl(id.to_string()).into()),
        }
    }

    /// Start a connector from a bound instance, identified by `id`.
    ///
    /// Starting a connector will make it begin emitting events.
    ///
    /// # Errors
    ///  * if finding
    pub async fn start_connector(&self, id: &TremorUrl) -> Result<()> {
        info!("Starting connector {}", id);
        if let Some(connector) = self.reg.find_connector(id).await? {
            connector.send(connectors::Msg::Start).await?;
        } else {
            return Err(ErrorKind::InstanceNotFound(id.to_string()).into());
        }
        Ok(())
    }

    /// Ensures the existance of a connector instance, bdingin it if required.
    ///
    /// # Errors
    ///  * if we can't ensure the connector is bound
    pub async fn ensure_connector(&self, id: &TremorUrl) -> Result<()> {
        if self.reg.find_connector(id).await?.is_none() {
            info!(
                "Connector not found in registry, binding {} to create a new instance.",
                &id
            );
            self.bind_connector(id).await?;
        } else {
            info!("Existing connector {} found", id);
        }
        Ok(())
    }

    /// Link a connector
    ///
    /// # Errors
    ///  * if the id isn't a connector or can't be linked
    pub async fn link_connector(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <ConnectorArtefact as Artefact>::LinkLHS,
            <ConnectorArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<ConnectorArtefact as Artefact>::LinkResult> {
        if let Some(connector_a) = self.repo.find_connector(id).await? {
            //if self.reg.find_connector(id).await?.is_none() {
            //    self.bind_connector(id).await?;
            //}
            connector_a.artefact.link(self, id, mappings).await
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    pub(crate) async fn bind_binding_a(
        &self,
        id: &TremorUrl,
        artefact: &BindingArtefact,
    ) -> Result<ActivationState> {
        info!("Binding binding {}", id);
        match &id.instance() {
            Some(_instance_id) => {
                let servant =
                    ActivatorLifecycleFsm::new(self.clone(), artefact.clone(), id.clone()).await?;
                self.repo.bind_binding(id).await?;
                self.reg.publish_binding(id, servant).await
            }
            None => Err(ErrorKind::InvalidInstanceUrl(id.to_string()).into()),
        }
    }

    pub(crate) async fn unbind_binding_a(
        &self,
        id: &TremorUrl,
        _artefact: &BindingArtefact,
    ) -> Result<ActivationState> {
        info!("Unbinding binding {}", id);
        match &id.instance() {
            Some(_instance_id) => {
                let servant = self.reg.unpublish_binding(id).await?;
                self.repo.unbind_binding(id).await?;
                Ok(servant)
            }
            None => Err(ErrorKind::InvalidInstanceUrl(id.to_string()).into()),
        }
    }

    /// Links a binding
    ///
    /// # Errors
    ///  * If the id isn't a binding or the bindig can't be linked
    pub async fn link_binding(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <BindingArtefact as Artefact>::LinkLHS,
            <BindingArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<BindingArtefact as Artefact>::LinkResult> {
        if let Some(binding_a) = self.repo.find_binding(id).await? {
            let r = binding_a.artefact.link(self, id, mappings).await?;
            if self.reg.find_binding(id).await?.is_none() {
                self.bind_binding_a(id, &r).await?;
            };
            Ok(r)
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    /// Turns the running system into a config
    ///
    /// # Errors
    ///  * If the systems configuration can't be stored
    pub async fn to_config(&self) -> Result<Config> {
        let onramp: OnRampVec = self.repo.serialize_onramps().await?;
        let offramp: OffRampVec = self.repo.serialize_offramps().await?;
        let binding: BindingVec = self
            .repo
            .serialize_bindings()
            .await?
            .into_iter()
            .map(|b| b.binding)
            .collect();
        let mapping: MappingMap = self.reg.serialize_mappings().await?;
        let config = crate::config::Config {
            onramp,
            offramp,
            connector: vec![],
            binding,
            mapping,
        };
        Ok(config)
    }

    /// Unlinks a binding
    ///
    /// # Errors
    ///  * if the id isn't an binding or the binding can't be unbound
    pub async fn unlink_binding(
        &self,
        id: &TremorUrl,
        mappings: HashMap<
            <BindingArtefact as Artefact>::LinkLHS,
            <BindingArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<BindingArtefact as Artefact>::LinkResult> {
        if let Some(binding) = self.reg.find_binding(id).await? {
            if binding.unlink(self, id, mappings).await? {
                self.unbind_binding_a(id, &binding).await?;
            }
            return Ok(binding);
        }

        Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
    }

    /// Starts the runtime system
    ///
    /// # Errors
    ///  * if the world manager can't be started
    pub async fn start(qsize: usize) -> Result<(Self, JoinHandle<Result<()>>)> {
        let metrics_channel = connectors::metrics::MetricsChannel::new(qsize);
        let (connector_h, connector) =
            connectors::Manager::new(qsize, metrics_channel.sender()).start();
        // TODO: use metrics channel for pipelines as well
        let (onramp_h, onramp) = onramp::Manager::new(qsize).start();
        let (offramp_h, offramp) = offramp::Manager::new(qsize).start();
        let (pipeline_h, pipeline) = pipeline::Manager::new(qsize).start();

        let (system_h, system) = Manager {
            connector,
            offramp,
            onramp,
            pipeline,
            connector_h,
            offramp_h,
            onramp_h,
            pipeline_h,
            qsize,
        }
        .start();

        let repo = Repositories::new();
        let reg = Registries::new();
        let mut world = Self {
            system,
            repo,
            reg,
            metrics_channel,
        };

        crate::sink::register_builtin_sinks(&world).await?;
        crate::source::register_builtin_sources(&world).await?;
        crate::connectors::register_builtin_connector_types(&world).await?;

        world.register_system().await?;
        Ok((world, system_h))
    }

    async fn register_system(&mut self) -> Result<()> {
        // register metrics connector
        let artefact: ConnectorArtefact = serde_yaml::from_str(
            r#"
id: system::metrics
type: metrics
            "#,
        )?;
        self.repo
            .publish_connector(&METRICS_CONNECTOR, true, artefact)
            .await?;
        self.bind_connector(&METRICS_CONNECTOR).await?;
        self.reg
            .find_connector(&METRICS_CONNECTOR)
            .await?
            .ok_or_else(|| Error::from("Failed to initialize system::metrics connector."))?;
        // we need to make sure the metrics connector is consuming metrics events
        // before anything else is started, so we don't fill up the metrics_channel and thus lose messages
        self.start_connector(&METRICS_CONNECTOR).await?;

        // register metrics pipeline
        let module_path = &tremor_script::path::ModulePath { mounts: Vec::new() };
        let aggr_reg = tremor_script::aggr_registry();
        let artefact_metrics = tremor_pipeline::query::Query::parse(
            module_path,
            "#!config id = \"system::metrics\"\nselect event from in into out;",
            "<metrics>",
            Vec::new(),
            &*tremor_pipeline::FN_REGISTRY.lock()?,
            &aggr_reg,
        )?;
        self.repo
            .publish_pipeline(&METRICS_PIPELINE, true, artefact_metrics)
            .await?;
        self.bind_pipeline(&METRICS_PIPELINE).await?;

        self.reg
            .find_pipeline(&METRICS_PIPELINE)
            .await?
            .ok_or_else(|| Error::from("Failed to initialize metrics pipeline."))?;

        // register passthrough pipeline
        let artefact_passthrough = tremor_pipeline::query::Query::parse(
            module_path,
            "#!config id = \"system::passthrough\"\nselect event from in into out;",
            "<passthrough>",
            Vec::new(),
            &*tremor_pipeline::FN_REGISTRY.lock()?,
            &aggr_reg,
        )?;
        self.repo
            .publish_pipeline(&PASSTHROUGH_PIPELINE, true, artefact_passthrough)
            .await?;

        // Register stdout connector
        let stdout_artefact: ConnectorArtefact = serde_yaml::from_str(
            r#"
id: system::stdout
type: std_stream
config:
  stream: stdout
            "#,
        )?;
        self.repo
            .publish_connector(&STDOUT_CONNECTOR, true, stdout_artefact)
            .await?;
        self.bind_connector(&STDOUT_CONNECTOR).await?;
        self.reg
            .find_connector(&STDOUT_CONNECTOR)
            .await?
            .ok_or_else(|| Error::from("Failed to initialize system::stdout connector"))?;

        // Register stderr connector
        let stderr_artefact: ConnectorArtefact = serde_yaml::from_str(
            r#"
id: system::stderr
type: std_stream
config:
  stream: stderr
            "#,
        )?;
        self.repo
            .publish_connector(&STDERR_CONNECTOR, true, stderr_artefact)
            .await?;
        self.bind_connector(&STDERR_CONNECTOR).await?;
        self.reg
            .find_connector(&STDERR_CONNECTOR)
            .await?
            .ok_or_else(|| Error::from("Failed to initialize system::stderr connector"))?;

        // Register stdin connector
        let stdin_artefact: ConnectorArtefact = serde_yaml::from_str(
            r#"
id: system::stdin
type: std_stream
config:
  stream: stdin
            "#,
        )?;
        self.repo
            .publish_connector(&STDIN_CONNECTOR, true, stdin_artefact)
            .await?;
        self.bind_connector(&STDIN_CONNECTOR).await?;
        self.reg
            .find_connector(&STDIN_CONNECTOR)
            .await?
            .ok_or_else(|| Error::from("Failed to initialize system::stdin connector"))?;

        // Register stdout offramp
        let artefact: OfframpArtefact = serde_yaml::from_str(
            r#"
id: system::stdout
type: stdout
"#,
        )?;
        self.repo
            .publish_offramp(&STDOUT_OFFRAMP, true, artefact)
            .await?;
        self.bind_offramp(&STDOUT_OFFRAMP).await?;
        self.reg
            .find_offramp(&STDOUT_OFFRAMP)
            .await?
            .ok_or_else(|| Error::from("Failed to initialize stdout offramp."))?;

        // Register stderr offramp
        let artefact: OfframpArtefact = serde_yaml::from_str(
            r#"
id: system::stderr
type: stderr
"#,
        )?;
        self.repo
            .publish_offramp(&STDERR_OFFRAMP, true, artefact)
            .await?;
        self.bind_offramp(&STDERR_OFFRAMP).await?;
        self.reg
            .find_offramp(&STDERR_OFFRAMP)
            .await?
            .ok_or_else(|| Error::from("Failed to initialize stderr offramp."))?;

        Ok(())
    }

    pub(crate) async fn start_pipeline(
        &self,
        config: PipelineArtefact,
        id: ServantId,
    ) -> Result<pipeline::Addr> {
        let (tx, rx) = bounded(1);
        self.system
            .send(ManagerMsg::Pipeline(pipeline::ManagerMsg::Create(
                tx,
                Box::new(pipeline::Create { config, id }),
            )))
            .await?;
        rx.recv().await?
    }

    /// convenience wrapper for instantiating an offramp from a given config
    pub(crate) async fn instantiate_offramp(
        &self,
        offramp_type: String,
        config: Option<OpConfig>,
        timeout: Duration,
    ) -> Result<Box<dyn offramp::Offramp>> {
        let (tx, rx) = bounded(1);
        let msg = offramp::ManagerMsg::Instantiate {
            offramp_type,
            config,
            sender: tx,
        };
        self.system.send(ManagerMsg::Offramp(msg)).await?;
        async_std::future::timeout(timeout, rx.recv()).await??
    }

    pub(crate) async fn instantiate_onramp(
        &self,
        onramp_type: String,
        url: TremorUrl,
        config: Option<OpConfig>,
        timeout: Duration,
    ) -> Result<Box<dyn onramp::Onramp>> {
        let (tx, rx) = bounded(1);
        let msg = onramp::ManagerMsg::Instantiate {
            onramp_type,
            url,
            config,
            sender: tx,
        };
        self.system.send(ManagerMsg::Onramp(msg)).await?;
        async_std::future::timeout(timeout, rx.recv()).await??
    }
}
