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

use std::net::SocketAddr;

use crate::config::{BindingVec, Config, MappingMap, OffRampVec, OnRampVec};
use crate::errors::{Error, ErrorKind, Result};
use crate::lifecycle::{ActivationState, ActivatorLifecycleFsm};
use crate::raft::node::{start_raft, NodeId, RaftSender};
use crate::registry::{Registries, ServantId};
use crate::repository::{
    Artefact, BindingArtefact, OfframpArtefact, OnrampArtefact, PipelineArtefact, Repositories,
};
use crate::temp_network::ws::{self, UrMsg};
use crate::url::ports::METRICS;
use crate::url::TremorURL;
use async_channel::bounded;
use async_std::io::prelude::*;
use async_std::path::Path;
use async_std::task::{self, JoinHandle};
use hashbrown::HashMap;
use slog::{o, Drain};
use tremor_common::asy::file;
use tremor_common::time::nanotime;

pub(crate) use crate::network;
pub(crate) use crate::offramp;
pub(crate) use crate::onramp;
pub(crate) use crate::pipeline;
pub(crate) use crate::uring;

lazy_static! {
    pub(crate) static ref METRICS_PIPELINE: TremorURL = {
        TremorURL::parse("/pipeline/system::metrics/system/in")
            //ALLOW: We want this to panic, it only happens at startup time
            .expect("Failed to initialize id for metrics piepline")
    };
    pub(crate) static ref PASSTHROUGH_PIPELINE: TremorURL = {
        TremorURL::parse("/pipeline/system::passthrough/system/in")
            //ALLOW: We want this to panic, it only happens at startup time
            .expect("Failed to initialize id for metrics piepline")
    };
    pub(crate) static ref STDOUT_OFFRAMP: TremorURL = {
        TremorURL::parse("/offramp/system::stdout/system/in")
            //ALLOW: We want this to panic, it only happens at startup time
            .expect("Failed to initialize id for stdout offramp")
    };
    pub(crate) static ref STDERR_OFFRAMP: TremorURL = {
        TremorURL::parse("/offramp/system::stderr/system/in")
            //ALLOW: We want this to panic, it only happens at startup time
            .expect("Failed to initialize id for stderr offramp")
    };
}

/// This is the node runtime control plane
pub(crate) enum ManagerMsg {
    /// Create a pipeline
    CreatePipeline(
        async_channel::Sender<Result<pipeline::Addr>>,
        pipeline::Create,
    ),
    /// Create an onramp
    CreateOnramp(
        async_channel::Sender<Result<onramp::Addr>>,
        Box<onramp::Create>,
    ),
    /// Create an offramp
    CreateOfframp(
        async_channel::Sender<Result<offramp::Addr>>,
        Box<offramp::Create>,
    ),
    /// Stop command
    Stop,
}

pub(crate) type Sender = async_channel::Sender<ManagerMsg>;

#[derive(Debug)]
pub(crate) struct Manager {
    pub offramp: offramp::Sender,
    pub onramp: onramp::Sender,
    pub pipeline: pipeline::Sender,
    pub network: network::NetworkSender,
    pub uring: uring::NetworkSender,
    pub offramp_h: JoinHandle<Result<()>>,
    pub onramp_h: JoinHandle<Result<()>>,
    pub pipeline_h: JoinHandle<Result<()>>,
    pub network_h: JoinHandle<Result<()>>,
    pub uring_h: JoinHandle<Result<()>>,
    pub qsize: usize,
}

impl Manager {
    pub fn start(self) -> (JoinHandle<Result<()>>, Sender) {
        let (tx, rx) = bounded(crate::QSIZE);
        let system_h = task::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                match msg {
                    ManagerMsg::CreatePipeline(r, c) => {
                        self.pipeline
                            .send(pipeline::ManagerMsg::Create(r, c))
                            .await?
                    }
                    ManagerMsg::CreateOnramp(r, c) => {
                        self.onramp.send(onramp::ManagerMsg::Create(r, c)).await?
                    }
                    ManagerMsg::CreateOfframp(r, c) => {
                        self.offramp.send(offramp::ManagerMsg::Create(r, c)).await?
                    }
                    ManagerMsg::Stop => {
                        info!("Stopping offramps...");
                        self.offramp.send(offramp::ManagerMsg::Stop).await?;
                        info!("Stopping pipelines...");
                        self.pipeline.send(pipeline::ManagerMsg::Stop).await?;
                        info!("Stopping onramps...");
                        self.onramp.send(onramp::ManagerMsg::Stop).await?;
                        info!("Stopping network...");
                        self.network.send(network::ManagerMsg::Stop).await?;
                        info!("Stopping uring...");
                        self.uring.send(uring::ManagerMsg::Stop).await?;
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
    /// Sender for the raft-based micro-ring
    pub uring: async_channel::Sender<UrMsg>,
    /// Runtime type information
    pub(crate) system: Sender,
    /// Conductor
    pub conductor: Conductor,
    /// Storage directory
    storage_directory: Option<String>,
}

#[derive(Clone, Debug)]
/// Encapsulates registry+repository interactions.
/// These are the set of events that govern user provided
/// managed deployment state
///
pub struct Conductor {
    /// Runtime type information
    pub(crate) system: Sender,

    /// Sender for the raft-based micro-ring
    pub(crate) uring: RaftSender,

    /// Sender for the raft-based micro-ring
    pub(crate) temp_uring: async_channel::Sender<UrMsg>,

    /// Configuration ( code ) models of managed tremor artefacts
    pub reg: Registries,

    /// Instances of managed tremor artefacts
    pub repo: Repositories,
}

impl Conductor {
    /// Create a new conductor
    pub(crate) fn new(
        system: Sender,
        uring: RaftSender,
        temp_uring: async_channel::Sender<UrMsg>,
    ) -> Self {
        let repo = Repositories::new();
        let reg = Registries::new();
        Self {
            system,
            uring,
            temp_uring,
            reg,
            repo,
        }
    }

    /// Ensures the existance of an onramp, creating it if required.
    ///
    /// # Errors
    ///  * if we can't ensure the onramp is bound
    pub async fn ensure_onramp(&self, id: &TremorURL) -> Result<()> {
        if self.reg.find_onramp(&id).await?.is_none() {
            info!(
                "Onramp not found during binding process, binding {} to create a new instance.",
                &id
            );
            self.bind_onramp(&id).await?;
        } else {
            info!("Existing onramp {} found", id);
        }
        Ok(())
    }

    /// Ensures the existance of an offramp, creating it if required.
    ///
    /// # Errors
    ///  * if we can't ensure the offramp is bound
    pub async fn ensure_offramp(&self, id: &TremorURL) -> Result<()> {
        if self.reg.find_offramp(&id).await?.is_none() {
            info!(
                "Offramp not found during binding process, binding {} to create a new instance.",
                &id
            );
            self.bind_offramp(&id).await?;
        } else {
            info!("Existing offramp {} found", id);
        }
        Ok(())
    }

    /// Ensures the existance of an pipeline, creating it if required.
    ///
    /// # Errors
    ///  * if we can't ensure the pipeline is bound
    pub async fn ensure_pipeline(&self, id: &TremorURL) -> Result<()> {
        if self.reg.find_pipeline(&id).await?.is_none() {
            info!(
                "Pipeline not found during binding process, binding {} to create a new instance.",
                &id
            );
            self.bind_pipeline(&id).await?;
        } else {
            info!("Existing pipeline {} found", id);
        }
        Ok(())
    }

    /// Bind a pipeline
    ///
    /// # Errors
    ///  * if the id isn't a pipeline instance or can't be bound
    pub async fn bind_pipeline(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Binding pipeline {}", id);
        match (&self.repo.find_pipeline(id).await?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let servant =
                    ActivatorLifecycleFsm::new(self, artefact.artefact.to_owned(), id.clone())
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
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    /// Unbind a pipeline
    ///
    /// # Errors
    ///  * if the id isn't an pipeline instance or the pipeline can't be unbound
    pub async fn unbind_pipeline(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Unbinding pipeline {}", id);
        match (&self.repo.find_pipeline(id).await?, &id.instance()) {
            (Some(_artefact), Some(_instance_id)) => {
                let r = self.reg.unpublish_pipeline(id).await?;
                self.repo.unbind_pipeline(id).await?;
                Ok(r)
            }
            (None, _) => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
            (_, None) => Err(format!("Invalid URI for instance {}", id).into()),
        }
    }

    /// Links a pipeline
    ///
    /// # Errors
    ///  * if the id isn't a pipeline or the pipeline can't be linked
    pub async fn link_pipeline(
        &self,
        id: &TremorURL,
        mappings: HashMap<
            <PipelineArtefact as Artefact>::LinkLHS,
            <PipelineArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<PipelineArtefact as Artefact>::LinkResult> {
        info!("Linking pipeline {} to {:?}", id, mappings);
        if let Some(pipeline_a) = self.repo.find_pipeline(id).await? {
            if self.reg.find_pipeline(id).await?.is_none() {
                self.bind_pipeline(&id).await?;
            };
            pipeline_a.artefact.link(self, id, mappings).await
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    /// Links a pipeline
    async fn link_existing_pipeline(
        &self,
        id: &TremorURL,
        mappings: HashMap<
            <PipelineArtefact as Artefact>::LinkLHS,
            <PipelineArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<PipelineArtefact as Artefact>::LinkResult> {
        info!("Linking pipeline {} to {:?}", id, mappings);
        if let Some(pipeline_a) = self.repo.find_pipeline(id).await? {
            pipeline_a.artefact.link(self, id, mappings).await
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    /// Unlink a pipeline
    ///
    /// # Errors
    ///  * if the id isn't a pipeline or the pipeline can't be unlinked
    pub async fn unlink_pipeline(
        &self,
        id: &TremorURL,
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
        id: &TremorURL,
        artefact: PipelineArtefact,
    ) -> Result<ActivationState> {
        self.repo.publish_pipeline(id, false, artefact).await?;
        self.bind_pipeline(id).await
    }

    /// Bind an onramp
    ///
    /// # Errors
    ///  * if the id isn't a onramp instance or the onramp can't be bound
    pub async fn bind_onramp(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Binding onramp {}", id);
        match (&self.repo.find_onramp(id).await?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let servant =
                    ActivatorLifecycleFsm::new(self, artefact.artefact.to_owned(), id.clone())
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
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    /// Unbind an onramp
    ///
    /// # Errors
    ///  * if the id isn't an onramp or the onramp can't be unbound
    pub async fn unbind_onramp(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Unbinding onramp {}", id);
        match (&self.repo.find_onramp(id).await?, &id.instance()) {
            (Some(_artefact), Some(_instsance_id)) => {
                let r = self.reg.unpublish_onramp(id).await;
                self.repo.unbind_onramp(id).await?;
                r
            }
            (None, _) => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    /// Link an onramp
    ///
    /// # Errors
    ///  * if the id isn't an onramp or the onramp can't be linked
    pub async fn link_onramp(
        &self,
        id: &TremorURL,
        mappings: HashMap<
            <OnrampArtefact as Artefact>::LinkLHS,
            <OnrampArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<OnrampArtefact as Artefact>::LinkResult> {
        if let Some(onramp_a) = self.repo.find_onramp(id).await? {
            if self.reg.find_onramp(id).await?.is_none() {
                self.bind_onramp(&id).await?;
            };
            onramp_a.artefact.link(self, id, mappings).await
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    async fn link_existing_onramp(
        &self,
        id: &TremorURL,
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
        id: &TremorURL,
        mappings: HashMap<
            <OnrampArtefact as Artefact>::LinkLHS,
            <OnrampArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<OnrampArtefact as Artefact>::LinkResult> {
        if let Some(onramp_a) = self.repo.find_onramp(id).await? {
            let r = onramp_a.artefact.unlink(self, id, mappings).await?;
            if r {
                self.unbind_onramp(&id).await?;
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
    pub async fn bind_offramp(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Binding offramp {}", id);
        match (&self.repo.find_offramp(id).await?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let servant =
                    ActivatorLifecycleFsm::new(self, artefact.artefact.to_owned(), id.clone())
                        .await?;
                self.repo.bind_offramp(id).await?;
                // We link to the metrics pipeline
                let res = self.reg.publish_offramp(id, servant).await?;
                let mut metrics_id = id.clone();
                metrics_id.set_port(&METRICS);
                let m = vec![(METRICS_PIPELINE.clone(), metrics_id.clone())]
                    .into_iter()
                    .collect();
                self.link_existing_offramp(&id, m).await?;
                Ok(res)
            }
            (None, _) => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    /// Unbind an offramp
    ///
    /// # Errors
    ///  * if the id isn't an offramp instance or the offramp can't be unbound
    pub async fn unbind_offramp(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Unbinding offramp {} ..", id);
        match (&self.repo.find_offramp(id).await?, &id.instance()) {
            (Some(_artefact), Some(_instsance_id)) => {
                let r = self.reg.unpublish_offramp(id).await;
                self.repo.unbind_offramp(id).await?;
                r
            }
            (None, _) => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    /// Link an offramp
    ///
    /// # Errors
    ///  * if the id isn't an offramp or can't be linked
    pub async fn link_offramp(
        &self,
        id: &TremorURL,
        mappings: HashMap<
            <OfframpArtefact as Artefact>::LinkLHS,
            <OfframpArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<OfframpArtefact as Artefact>::LinkResult> {
        if let Some(offramp_a) = self.repo.find_offramp(id).await? {
            if self.reg.find_offramp(id).await?.is_none() {
                self.bind_offramp(&id).await?;
            };
            offramp_a.artefact.link(self, id, mappings).await
        } else {
            Err(ErrorKind::ArtefactNotFound(id.to_string()).into())
        }
    }

    async fn link_existing_offramp(
        &self,
        id: &TremorURL,
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
        id: &TremorURL,
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

    pub(crate) async fn bind_binding_a(
        &self,
        id: &TremorURL,
        artefact: &BindingArtefact,
    ) -> Result<ActivationState> {
        info!("Binding binding {}", id);
        match &id.instance() {
            Some(_instance_id) => {
                let servant =
                    ActivatorLifecycleFsm::new(self, artefact.to_owned(), id.clone()).await?;
                self.repo.bind_binding(id).await?;
                self.reg.publish_binding(id, servant).await
            }
            None => Err(format!("Invalid URI for instance {}", id).into()),
        }
    }

    pub(crate) async fn unbind_binding_a(
        &self,
        id: &TremorURL,
        _artefact: &BindingArtefact,
    ) -> Result<ActivationState> {
        info!("Unbinding binding {}", id);
        match &id.instance() {
            Some(_instance_id) => {
                let servant = self.reg.unpublish_binding(id).await?;
                self.repo.unbind_binding(id).await?;
                Ok(servant)
            }
            None => Err(format!("Invalid URI for instance {}", id).into()),
        }
    }

    /// Links a binding
    ///
    /// # Errors
    ///  * If the id isn't a binding or the binding can't be linked
    pub async fn link_binding(
        &self,
        id: &TremorURL,
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

    /// Unlinks a binding
    ///
    /// # Errors
    ///  * if the id isn't an binding or the binding can't be unbound
    pub async fn unlink_binding(
        &self,
        id: &TremorURL,
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

    ///
    ///
    ///

    pub(crate) async fn start_pipeline(
        &self,
        config: PipelineArtefact,
        id: ServantId,
    ) -> Result<pipeline::Addr> {
        let (tx, rx) = bounded(1);
        self.system
            .send(ManagerMsg::CreatePipeline(
                tx,
                pipeline::Create { id, config },
            ))
            .await?;
        rx.recv().await?
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
            binding,
            mapping,
        };
        Ok(config)
    }
}

impl World {
    /// Stop the runtime
    ///
    /// # Errors
    ///  * if the system failed to stop
    pub async fn stop(&self) -> Result<()> {
        Ok(self.system.send(ManagerMsg::Stop).await?)
    }

    /// Saves the current config
    ///
    /// # Errors
    ///  * if the config can't be saved
    pub async fn save_config(&self) -> Result<String> {
        if let Some(storage_directory) = &self.storage_directory {
            let config = self.conductor.to_config().await?;
            let path = Path::new(storage_directory);
            let file_name = format!("config_{}.yaml", nanotime());
            let mut file_path = path.to_path_buf();
            file_path.push(Path::new(&file_name));
            info!(
                "Serializing configuration to file {}",
                file_path.to_string_lossy()
            );
            let mut f = file::create(&file_path).await?;
            f.write_all(&serde_yaml::to_vec(&config)?).await?;
            // lets really sync this!
            f.sync_all().await?;
            f.sync_all().await?;
            f.sync_all().await?;
            Ok(file_path.to_string_lossy().to_string())
        } else {
            Ok("".to_string())
        }
    }

    /// Starts the runtime system
    ///
    /// # Errors
    ///  * if the world manager can't be started
    pub async fn start(
        qsize: usize,
        storage_directory: Option<String>,
        network_addr: SocketAddr,
        temp_cluster_endpoint: String,
        cluster_addr: SocketAddr,
        cluster_peers: Vec<String>,
        cluster_bootstrap: bool,
    ) -> Result<(Self, JoinHandle<Result<()>>)> {
        // TODO direct these logs to a separate file? also include the json option
        let logger = {
            let decorator = slog_term::TermDecorator::new().build();
            let drain = slog_term::FullFormat::new(decorator).build().fuse();
            let drain = slog_async::Async::new(drain).build().fuse();
            // temp filter for only showing warnings
            let drain = slog::LevelFilter::new(drain, slog::Level::Warning).fuse();
            slog::Logger::root(drain, o!())
        };
        // FIXME allow for non-numeric
        //let numeric_instance_id = instance!().parse::<u64>()?;
        let numeric_instance_id = instance!().parse::<u64>().unwrap_or(42);
        let node_id = NodeId(numeric_instance_id);
        let temp_network = ws::Network::new(
            &logger,
            node_id,
            temp_cluster_endpoint,
            cluster_peers.clone(),
        );

        let (fake_system_tx, _not_used) = bounded(crate::QSIZE);

        let (raft_tx, raft_rx) = bounded(crate::QSIZE);

        let mut conductor =
            Conductor::new(fake_system_tx.clone(), raft_tx, temp_network.tx.clone());

        let (onramp_h, onramp) = onramp::Manager::new(qsize).start();
        let (offramp_h, offramp) = offramp::Manager::new(qsize).start();
        let (pipeline_h, pipeline) = pipeline::Manager::new(qsize).start();

        let (network_h, network) = network::Manager::new(&conductor, network_addr, qsize).start();

        let (uring_h, uring) =
            uring::Manager::new(&conductor, cluster_addr, Some(cluster_peers), qsize).start();

        let (system_h, system) = Manager {
            offramp,
            onramp,
            pipeline,
            network: network.clone(),
            uring: uring.clone(),
            offramp_h,
            onramp_h,
            pipeline_h,
            network_h,
            uring_h,
            qsize,
        }
        .start();

        // Rebind system in conductor
        conductor.system = system.clone();

        let mut world = Self {
            uring: temp_network.tx.clone(),
            system,
            conductor,
            storage_directory,
        };

        // TODO shift this to a cluster manager?
        start_raft(
            node_id,
            cluster_bootstrap,
            logger,
            raft_rx,
            uring,
            temp_network,
        )
        .await;

        world.register_system().await?;
        Ok((world, system_h))
    }

    async fn register_system(&mut self) -> Result<()> {
        // register metrics pipeline

        let module_path = &tremor_script::path::ModulePath { mounts: Vec::new() };
        let aggr_reg = tremor_script::aggr_registry();
        let artefact_metrics = tremor_pipeline::query::Query::parse(
            &module_path,
            "#!config id = \"system::metrics\"\nselect event from in into out;",
            "<metrics>",
            Vec::new(),
            &*tremor_pipeline::FN_REGISTRY.lock()?,
            &aggr_reg,
        )?;
        self.conductor
            .repo
            .publish_pipeline(&METRICS_PIPELINE, true, artefact_metrics)
            .await?;
        self.conductor.bind_pipeline(&METRICS_PIPELINE).await?;

        self.conductor
            .reg
            .find_pipeline(&METRICS_PIPELINE)
            .await?
            .ok_or_else(|| Error::from("Failed to initialize metrics pipeline."))?;

        let artefact_passthrough = tremor_pipeline::query::Query::parse(
            &module_path,
            "#!config id = \"system::passthrough\"\nselect event from in into out;",
            "<passthrough>",
            Vec::new(),
            &*tremor_pipeline::FN_REGISTRY.lock()?,
            &aggr_reg,
        )?;
        self.conductor
            .repo
            .publish_pipeline(&PASSTHROUGH_PIPELINE, true, artefact_passthrough)
            .await?;
        // Register stdout offramp
        let artefact: OfframpArtefact = serde_yaml::from_str(
            r#"
id: system::stdout
type: stdout
"#,
        )?;
        self.conductor
            .repo
            .publish_offramp(&STDOUT_OFFRAMP, true, artefact)
            .await?;
        self.conductor.bind_offramp(&STDOUT_OFFRAMP).await?;
        self.conductor
            .reg
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
        self.conductor
            .repo
            .publish_offramp(&STDERR_OFFRAMP, true, artefact)
            .await?;
        self.conductor.bind_offramp(&STDERR_OFFRAMP).await?;
        self.conductor
            .reg
            .find_offramp(&STDERR_OFFRAMP)
            .await?
            .ok_or_else(|| Error::from("Failed to initialize stderr offramp."))?;

        Ok(())
    }
}
