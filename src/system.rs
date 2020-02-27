// Copyright 2018-2020, Wayfair GmbH
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

use crate::config::{BindingVec, Config, MappingMap, OffRampVec, OnRampVec, PipelineVec};
use crate::errors::*;
use crate::lifecycle::{ActivationState, ActivatorLifecycleFsm};
use crate::registry::{Registries, ServantId};
use crate::repository::{
    Artefact, BindingArtefact, OfframpArtefact, OnrampArtefact, PipelineArtefact, Repositories,
};
use crate::url::TremorURL;
use crate::utils::nanotime;
use async_std::{
    sync::{self, channel},
    task::{self, JoinHandle},
};
//use crossbeam_channel::Sender as CbSender;
use async_std::fs::File;
use async_std::io::prelude::*;
use async_std::path::Path;
use hashbrown::HashMap;
use tremor_pipeline;

pub(crate) use crate::offramp;
pub(crate) use crate::onramp;
pub(crate) use crate::pipeline;

lazy_static! {
    pub(crate) static ref METRICS_PIPELINE: TremorURL = {
        TremorURL::parse("/pipeline/system::metrics/system/in")
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

pub(crate) enum ManagerMsg {
    CreatePipeline(sync::Sender<Result<pipeline::Addr>>, pipeline::Create),
    CreateOnrampt(sync::Sender<Result<onramp::Addr>>, onramp::Create),
    CreateOfframp(sync::Sender<Result<offramp::Addr>>, offramp::Create),
    Stop,
    //Count,
}

//pub(crate) type Addr = Sender<ManagerMsg>;
pub(crate) type Sender = async_std::sync::Sender<ManagerMsg>;
//pub type Addr = CbSender<Msg>;

#[derive(Debug)]
pub(crate) struct Manager {
    pub offramp: offramp::Sender,
    pub onramp: onramp::Sender,
    pub pipeline: pipeline::Sender,
    pub offramp_h: JoinHandle<bool>,
    pub onramp_h: JoinHandle<bool>,
    pub pipeline_h: JoinHandle<bool>,
    pub qsize: usize,
}

impl Manager {
    pub fn start(self) -> (JoinHandle<()>, Sender) {
        let (tx, rx) = channel(64);
        let system_h = task::spawn(async move {
            loop {
                match rx.recv().await {
                    Some(ManagerMsg::CreatePipeline(r, c)) => {
                        self.pipeline.send(pipeline::ManagerMsg::Create(r, c)).await
                    }
                    Some(ManagerMsg::CreateOnrampt(r, c)) => {
                        self.onramp.send(onramp::ManagerMsg::Create(r, c)).await
                    }
                    Some(ManagerMsg::CreateOfframp(r, c)) => {
                        self.offramp.send(offramp::ManagerMsg::Create(r, c)).await
                    }
                    Some(ManagerMsg::Stop) => {
                        info!("Stopping offramps...");
                        self.offramp.send(offramp::ManagerMsg::Stop).await;
                        info!("Stopping pipelines...");
                        self.pipeline.send(pipeline::ManagerMsg::Stop).await;
                        info!("Stopping onramps...");
                        self.onramp.send(onramp::ManagerMsg::Stop).await;
                        break;
                    }
                    None => {
                        info!("Stopping onramps in an odd way...");
                        break;
                    }
                }
            }
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
    //system_pipelines: HashMap<ServantId, pipeline::Addr>,
    //system_onramps: HashMap<ServantId, onramp::Addr>,
    //system_offramps: HashMap<ServantId, offramp::Addr>,
    storage_directory: Option<String>,
}

impl World {
    /// Bind a pipeline
    pub async fn bind_pipeline(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Binding pipeline {}", id);
        match (&self.repo.find_pipeline(id).await?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let servant = ActivatorLifecycleFsm::new(
                    self.clone(),
                    artefact.artefact.to_owned(),
                    id.clone(),
                )
                .await?;
                self.repo.bind_pipeline(id).await?;
                // We link to the metrics pipeline
                let res = self.reg.publish_pipeline(id, servant).await?;
                let mut id = id.clone();
                id.set_port("metrics".to_owned());
                let m = vec![("metrics".to_string(), METRICS_PIPELINE.clone())]
                    .into_iter()
                    .collect();
                self.link_existing_pipeline(&id, m).await?;
                Ok(res)
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    /// Unbind a pipeline
    pub async fn unbind_pipeline(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Unbinding pipeline {}", id);
        match (&self.repo.find_pipeline(id).await?, &id.instance()) {
            (Some(_artefact), Some(_instance_id)) => {
                let r = self.reg.unpublish_pipeline(id).await?;
                self.repo.unbind_pipeline(id).await?;
                Ok(r)
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {}", id).into()),
        }
    }

    /// Stop the runtime
    pub async fn stop(&self) {
        self.system.send(ManagerMsg::Stop).await;
    }
    /// Links a pipeline
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
            Err(format!("Pipeline {} not found.", id).into())
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
            Err(format!("Pipeline {} not found.", id).into())
        }
    }

    /// Unlink a pipelein
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
            Err(format!("Pipeline {} not found", id).into())
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
    pub async fn bind_onramp(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Binding onramp {}", id);
        match (&self.repo.find_onramp(id).await?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let servant = ActivatorLifecycleFsm::new(
                    self.clone(),
                    artefact.artefact.to_owned(),
                    id.clone(),
                )
                .await?;
                self.repo.bind_onramp(id).await?;
                // We link to the metrics pipeline
                let res = self.reg.publish_onramp(id, servant).await?;
                let mut id = id.clone();
                id.set_port("metrics".to_owned());
                let m = vec![("metrics".to_string(), METRICS_PIPELINE.clone())]
                    .into_iter()
                    .collect();
                self.link_existing_onramp(&id, m).await?;
                Ok(res)
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }
    /// Unbind an onramp
    pub async fn unbind_onramp(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Unbinding onramp {}", id);
        match (&self.repo.find_onramp(id).await?, &id.instance()) {
            (Some(_artefact), Some(_instsance_id)) => {
                let r = self.reg.unpublish_onramp(id).await;
                self.repo.unbind_onramp(id).await?;
                r
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    /// Link an onramp
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
            Err(format!("Onramp {:?} not found.", id).into())
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
            Err(format!("Onramp {:?} not found.", id).into())
        }
    }

    /// Unlink an onramp
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
            Err(format!("Onramp {:?} not found.", id).into())
        }
    }

    /// Bind an offramp
    pub async fn bind_offramp(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Binding offramp {}", id);
        match (&self.repo.find_offramp(id).await?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let servant = ActivatorLifecycleFsm::new(
                    self.clone(),
                    artefact.artefact.to_owned(),
                    id.clone(),
                )
                .await?;
                self.repo.bind_offramp(id).await?;
                // We link to the metrics pipeline
                let res = self.reg.publish_offramp(id, servant).await?;
                // TODO remove
                //let mut id = id.clone();
                //id.set_port("metrics".to_owned());
                //let m = vec![("metrics".to_string(), METRICS_PIPELINE.clone())]
                let m = vec![(METRICS_PIPELINE.clone(), id.clone())]
                    .into_iter()
                    .collect();
                self.link_existing_offramp(&id, m).await?;
                Ok(res)
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    /// Unbind an offramp
    pub async fn unbind_offramp(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Unbinding offramp {}", id);
        match (&self.repo.find_offramp(id).await?, &id.instance()) {
            (Some(_artefact), Some(_instsance_id)) => {
                let r = self.reg.unpublish_offramp(id).await;
                self.repo.unbind_offramp(id).await?;
                r
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    /// Link an offramp
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
            Err(format!("Offramp {:?} not found.", id).into())
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
            Err(format!("Offramp {:?} not found.", id).into())
        }
    }

    /// Unlink an offramp
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
            Err(format!("Offramp {:?} not found.", id).into())
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
                    ActivatorLifecycleFsm::new(self.clone(), artefact.to_owned(), id.clone())
                        .await?;
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
            Err(format!("Binding {:?} not found.", id).into())
        }
    }

    /// Turns the running system into a config
    pub async fn to_config(&self) -> Result<Config> {
        let pipeline: PipelineVec = self
            .repo
            .serialize_pipelines()
            .await
            .into_iter()
            .filter_map(|p| match p {
                PipelineArtefact::Pipeline(p) => Some(p.config),
                PipelineArtefact::Query(_q) => None, // FIXME
            })
            .collect();
        let onramp: OnRampVec = self.repo.serialize_onramps().await;
        let offramp: OffRampVec = self.repo.serialize_offramps().await;
        let binding: BindingVec = self
            .repo
            .serialize_bindings()
            .await
            .into_iter()
            .map(|b| b.binding)
            .collect();
        let mapping: MappingMap = self.reg.serialize_mappings().await;
        let config = crate::config::Config {
            pipeline,
            onramp,
            offramp,
            binding,
            mapping,
        };
        Ok(config)
    }

    /// Saves the current config
    pub async fn save_config(&self) -> Result<String> {
        if let Some(storage_directory) = &self.storage_directory {
            let config = self.to_config().await?;
            let path = Path::new(storage_directory);
            let file_name = format!("config_{}.yaml", nanotime());
            let mut file_path = path.to_path_buf();
            file_path.push(Path::new(&file_name));
            info!(
                "Serializing configuration to file {}",
                file_path.to_string_lossy()
            );
            let mut f = File::create(file_path.clone()).await?;
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

    /// Unlinks a binding
    pub async fn unlink_binding(
        &self,
        id: &TremorURL,
        mappings: HashMap<
            <BindingArtefact as Artefact>::LinkLHS,
            <BindingArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<BindingArtefact as Artefact>::LinkResult> {
        if let Some(mapping) = self.reg.find_binding(id).await? {
            if mapping.unlink(self, id, mappings).await? {
                self.unbind_binding_a(id, &mapping).await?;
            }
            return Ok(mapping);
        }
        Err(format!("Binding {:?} not found.", id).into())
    }

    /// Starts the runtime system
    pub async fn start(
        qsize: usize,
        storage_directory: Option<String>,
    ) -> Result<(Self, JoinHandle<()>)> {
        let (onramp_h, onramp) = onramp::Manager::new(qsize).start();
        let (offramp_h, offramp) = offramp::Manager::new(qsize).start();
        let (pipeline_h, pipeline) = pipeline::Manager::new(qsize).start();

        let (system_h, system) = Manager {
            offramp,
            onramp,
            pipeline,
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
            storage_directory,
            //system_pipelines: HashMap::new(),
            //system_onramps: HashMap::new(),
            //system_offramps: HashMap::new(),
        };

        world.register_system().await?;
        Ok((world, system_h))
    }

    async fn register_system(&mut self) -> Result<()> {
        // register metrics pipeline
        let metric_config: tremor_pipeline::config::Pipeline = serde_yaml::from_str(
            r#"
id: system::metrics
description: 'System metrics pipeline'
interface:
  inputs: [ in ]
  outputs: [ out ]
links:
  in: [ out ]
"#,
        )?;
        let artefact =
            PipelineArtefact::Pipeline(Box::new(tremor_pipeline::build_pipeline(metric_config)?));
        self.repo
            .publish_pipeline(&METRICS_PIPELINE, true, artefact)
            .await?;
        self.bind_pipeline(&METRICS_PIPELINE).await?;

        let _addr = self
            .reg
            .find_pipeline(&METRICS_PIPELINE)
            .await?
            .ok_or_else(|| Error::from("Failed to initialize metrics pipeline."))?;
        //self.system_pipelines.insert(METRICS_PIPELINE.clone(), addr);

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
        let _addr = self
            .reg
            .find_offramp(&STDOUT_OFFRAMP)
            .await?
            .ok_or_else(|| Error::from("Failed to initialize stdout offramp."))?;
        //self.system_offramps.insert(STDOUT_OFFRAMP.clone(), addr);

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
        let _addr = self
            .reg
            .find_offramp(&STDERR_OFFRAMP)
            .await?
            .ok_or_else(|| Error::from("Failed to initialize stderr offramp."))?;
        //self.system_offramps.insert(STDERR_OFFRAMP.clone(), addr);

        Ok(())
    }

    pub(crate) async fn start_pipeline(
        &self,
        config: PipelineArtefact,
        id: ServantId,
    ) -> Result<pipeline::Addr> {
        let (tx, rx) = channel(1);
        self.system
            .send(ManagerMsg::CreatePipeline(
                tx,
                pipeline::Create { id, config },
            ))
            .await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }
}
