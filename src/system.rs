// Copyright 2018-2019, Wayfair GmbH
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
use crate::offramp::{self, OfframpAddr, OfframpMsg};
use crate::onramp::{self, OnrampAddr};
use crate::registry::{Registries, ServantId};
use crate::repository::{
    Artefact, BindingArtefact, OfframpArtefact, OnrampArtefact, PipelineArtefact, Repositories,
};
use crate::url::TremorURL;
use crate::utils::nanotime;
use actix;
use actix::prelude::*;
use crossbeam_channel::{bounded, Sender};
use futures::future::Future;
use hashbrown::HashMap;
use maplit::hashmap;
use std::fmt;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::thread;
use std::thread::JoinHandle;
use tremor_pipeline;
use tremor_pipeline::Event;

pub use crate::offramp::CreateOfframp;
pub use crate::onramp::CreateOnramp;

lazy_static! {
    pub static ref METRICS_PIPELINE: TremorURL = {
        TremorURL::parse("/pipeline/system::metrics/system/in")
            .expect("Failed to initialize id for metrics piepline")
    };
    pub static ref STDOUT_OFFRAMP: TremorURL = {
        TremorURL::parse("/offramp/system::stdout/system/in")
            .expect("Failed to initialize id for stdout offramp")
    };
    pub static ref STDERR_OFFRAMP: TremorURL = {
        TremorURL::parse("/offramp/system::stderr/system/in")
            .expect("Failed to initialize id for stderr offramp")
    };
}

pub type SystemAddr = Addr<Manager>;
#[derive(Debug)]
pub struct Manager {
    pub offramp: Addr<offramp::Manager>,
    pub onramp: Addr<onramp::Manager>,
    pub offramp_t: JoinHandle<i32>,
    pub onramp_t: JoinHandle<i32>,
    pub qsize: usize,
}

impl Actor for Manager {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!("Pipeline manager started");
    }
}

#[derive(Clone)]
pub struct PipelineAddr {
    pub addr: Sender<PipelineMsg>,
    pub id: ServantId,
}

impl fmt::Debug for PipelineAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Pipeline({})", self.id)
    }
}

pub struct CreatePipeline {
    pub config: PipelineArtefact,
    pub id: ServantId,
}

impl Message for CreatePipeline {
    type Result = Result<PipelineAddr>;
}

#[derive(Debug)]
pub enum PipelineMsg {
    Event { event: Event, input: String },
    ConnectOfframp(String, TremorURL, OfframpAddr),
    ConnectPipeline(String, TremorURL, PipelineAddr),
    Disconnect(String, TremorURL),
    Signal(Event),
    Insight(Event),
}

#[derive(Debug)]
enum PipelineDest {
    Offramp(OfframpAddr),
    Pipeline(PipelineAddr),
}

impl PipelineDest {
    pub fn send_event(&self, input: String, event: Event) -> Result<()> {
        match self {
            PipelineDest::Offramp(addr) => addr.send(OfframpMsg::Event { input, event })?,
            PipelineDest::Pipeline(addr) => addr.addr.send(PipelineMsg::Event { input, event })?,
        }
        Ok(())
    }
}

impl Handler<CreatePipeline> for Manager {
    type Result = Result<PipelineAddr>;
    fn handle(&mut self, req: CreatePipeline, _ctx: &mut Self::Context) -> Self::Result {
        fn send_events(
            eventset: Vec<(String, Event)>,
            dests: &HashMap<String, Vec<(TremorURL, PipelineDest)>>,
        ) -> Result<()> {
            for (output, event) in eventset {
                if let Some(dest) = dests.get(&output) {
                    let len = dest.len();
                    for (id, offramp) in dest.iter().take(len - 1) {
                        offramp.send_event(
                            id.instance_port()
                                .ok_or_else(|| {
                                    Error::from(format!("missing instance port in {}.", id))
                                })?
                                .clone(),
                            event.clone(),
                        )?;
                    }
                    let (id, offramp) = &dest[len - 1];
                    offramp.send_event(
                        id.instance_port()
                            .ok_or_else(|| {
                                Error::from(format!("missing instance port in {}.", id))
                            })?
                            .clone(),
                        event,
                    )?;
                };
            }
            Ok(())
        }
        let config = req.config;
        let id = req.id.clone();
        let mut dests: HashMap<String, Vec<(TremorURL, PipelineDest)>> = HashMap::new();
        let (tx, rx) = bounded::<PipelineMsg>(self.qsize);
        let mut pipeline = config
            .pipeline
            .to_executable_graph(tremor_pipeline::buildin_ops)?;
        let mut pid = req.id.clone();
        pid.trim_to_instance();
        pipeline.id = pid.to_string();
        thread::Builder::new()
            .name(format!("pipeline-{}", id.clone()))
            .spawn(move || {
                info!("[Pipeline:{}] starting thread.", id);
                for req in rx {
                    match req {
                        PipelineMsg::Event { input, event } => {
                            match pipeline.enqueue(&input, event) {
                                Ok(eventset) => {
                                    if let Err(e) = send_events(eventset, &dests) {
                                        error!("Failed to send event: {}", e)
                                    }
                                }
                                Err(e) => error!("error: {:?}", e),
                            }
                        }
                        PipelineMsg::Insight(insight) => {
                            pipeline.contraflow(insight);
                        }
                        PipelineMsg::Signal(signal) => match pipeline.signalflow(signal) {
                            Ok(eventset) => {
                                if let Err(e) = send_events(eventset, &dests) {
                                    error!("Failed to send event: {}", e)
                                }
                            }
                            Err(e) => error!("error: {:?}", e),
                        },

                        PipelineMsg::ConnectOfframp(output, offramp_id, offramp) => {
                            info!(
                                "[Pipeline:{}] connecting {} to offramp {}",
                                id, output, offramp_id
                            );
                            dests
                                .entry(output)
                                .or_insert(vec![])
                                .push((offramp_id, PipelineDest::Offramp(offramp)));
                        }
                        PipelineMsg::ConnectPipeline(output, pipeline_id, pipeline) => {
                            info!(
                                "[Pipeline:{}] connecting {} to pipeline {}",
                                id, output, pipeline_id
                            );
                            dests
                                .entry(output)
                                .or_insert(vec![])
                                .push((pipeline_id, PipelineDest::Pipeline(pipeline)));
                        }
                        PipelineMsg::Disconnect(output, to_delete) => {
                            let mut remove = false;
                            if let Some(offramp_vec) = dests.get_mut(&output) {
                                offramp_vec.retain(|(this_id, _)| this_id != &to_delete);
                                remove = offramp_vec.is_empty();
                            }
                            if remove {
                                dests.remove(&output);
                            }
                        }
                    };
                }
                info!("[Pipeline:{}] stopping thread.", id);
            })?;
        Ok(PipelineAddr {
            id: req.id,
            addr: tx,
        })
    }
}

impl Handler<CreateOfframp> for Manager {
    type Result = Result<OfframpAddr>;
    fn handle(&mut self, req: CreateOfframp, _ctx: &mut Self::Context) -> Self::Result {
        self.offramp
            .send(req)
            .wait()
            .map_err(|e| Error::from(format!("Milbox error: {:?}", e)))?
    }
}

impl Handler<CreateOnramp> for Manager {
    type Result = Result<OnrampAddr>;
    fn handle(&mut self, req: CreateOnramp, _ctx: &mut Self::Context) -> Self::Result {
        self.onramp
            .send(req)
            .wait()
            .map_err(|e| Error::from(format!("Milbox error: {:?}", e)))?
    }
}

pub struct Stop {}

impl Message for Stop {
    type Result = ();
}

impl Handler<Stop> for Manager {
    type Result = ();
    fn handle(&mut self, _req: Stop, ctx: &mut Self::Context) -> Self::Result {
        warn!("Stopping system");
        self.onramp
            .send(Stop {})
            .into_actor(self)
            .then(|_res, act, ctx| {
                warn!("Onramp Stopped");
                // TODO: How to shut down the pool here?
                // act.pool.shutdown_now();
                act.offramp
                    .send(Stop {})
                    .into_actor(act)
                    .then(|_, _, _| {
                        System::current().stop();
                        warn!("Offramp system");
                        actix::fut::ok(())
                    })
                    .wait(ctx);
                actix::fut::ok(())
            })
            .wait(ctx);
    }
}

pub struct Count {}

impl Message for Count {
    type Result = usize;
}

#[derive(Clone, Debug)]
pub struct World {
    pub system: SystemAddr,
    pub repo: Repositories,
    pub reg: Registries,
    system_pipelines: HashMap<ServantId, PipelineAddr>,
    system_onramps: HashMap<ServantId, OnrampAddr>,
    system_offramps: HashMap<ServantId, OfframpAddr>,
    storage_directory: Option<String>,
}

impl World {
    pub fn bind_pipeline(&self, mut id: TremorURL) -> Result<ActivationState> {
        info!("Binding pipeline {}", id);
        match (&self.repo.find_pipeline(id.clone())?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let servant = ActivatorLifecycleFsm::new(
                    self.clone(),
                    artefact.artefact.to_owned(),
                    id.clone(),
                )?;
                self.repo.bind_pipeline(id.clone())?;
                // We link to the metrics pipeline
                let res = self.reg.publish_pipeline(id.clone(), servant)?;
                id.set_port("metrics".to_owned());
                self.link_pipeline(
                    id,
                    hashmap! {"metrics".to_string() => METRICS_PIPELINE.clone()
                    },
                )?;
                Ok(res)
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    pub fn unbind_pipeline(&self, id: TremorURL) -> Result<ActivationState> {
        info!("Unbinding pipeline {}", id);
        match (&self.repo.find_pipeline(id.clone())?, &id.instance()) {
            (Some(_artefact), Some(_instance_id)) => {
                let r = self.reg.unpublish_pipeline(id.clone())?;
                self.repo.unbind_pipeline(id)?;
                Ok(r)
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {}", id).into()),
        }
    }

    pub fn stop(&self) {
        self.system.do_send(Stop {});
    }
    pub fn link_pipeline(
        &self,
        id: TremorURL,
        mappings: HashMap<
            <PipelineArtefact as Artefact>::LinkLHS,
            <PipelineArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<PipelineArtefact as Artefact>::LinkResult> {
        info!("Linking pipeline {} to {:?}", id, mappings);
        if let Some(pipeline_a) = self.repo.find_pipeline(id.clone())? {
            if self.reg.find_pipeline(id.clone())?.is_none() {
                self.bind_pipeline(id.clone())?;
            };
            pipeline_a.artefact.link(self, id, mappings)
        } else {
            Err(format!("Pipeline {} not found.", id).into())
        }
    }

    pub fn unlink_pipeline(
        &self,
        id: TremorURL,
        mappings: HashMap<
            <PipelineArtefact as Artefact>::LinkLHS,
            <PipelineArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<PipelineArtefact as Artefact>::LinkResult> {
        if let Some(pipeline_a) = self.repo.find_pipeline(id.clone())? {
            let r = pipeline_a.artefact.unlink(self, id.clone(), mappings);
            if self.reg.find_pipeline(id.clone())?.is_some() {
                self.unbind_pipeline(id)?;
            };
            r
        } else {
            Err(format!("Pipeline {} not found", id).into())
        }
    }

    #[cfg(test)]
    pub fn bind_pipeline_from_artefact(
        &self,
        id: TremorURL,
        artefact: PipelineArtefact,
    ) -> Result<ActivationState> {
        self.repo.publish_pipeline(id.clone(), false, artefact)?;
        self.bind_pipeline(id)
    }

    pub fn bind_onramp(&self, id: TremorURL) -> Result<ActivationState> {
        info!("Binding onramp {}", id);
        match (&self.repo.find_onramp(id.clone())?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let servant = ActivatorLifecycleFsm::new(
                    self.clone(),
                    artefact.artefact.to_owned(),
                    id.clone(),
                )?;
                self.repo.bind_onramp(id.clone())?;
                self.reg.publish_onramp(id, servant)
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    pub fn unbind_onramp(&self, id: TremorURL) -> Result<ActivationState> {
        info!("Unbinding onramp {}", id);
        match (&self.repo.find_onramp(id.clone())?, &id.instance()) {
            (Some(_artefact), Some(_instsance_id)) => {
                let r = self.reg.unpublish_onramp(id.clone());
                self.repo.unbind_onramp(id.clone())?;
                r
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    pub fn link_onramp(
        &self,
        id: TremorURL,
        mappings: HashMap<
            <OnrampArtefact as Artefact>::LinkLHS,
            <OnrampArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<OnrampArtefact as Artefact>::LinkResult> {
        if let Some(onramp_a) = self.repo.find_onramp(id.clone())? {
            if self.reg.find_onramp(id.clone())?.is_none() {
                self.bind_onramp(id.clone())?;
            };
            onramp_a.artefact.link(self, id, mappings)
        } else {
            Err(format!("Onramp {:?} not found.", id).into())
        }
    }

    pub fn unlink_onramp(
        &self,
        id: TremorURL,
        mappings: HashMap<
            <OnrampArtefact as Artefact>::LinkLHS,
            <OnrampArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<OnrampArtefact as Artefact>::LinkResult> {
        if let Some(onramp_a) = self.repo.find_onramp(id.clone())? {
            let r = onramp_a.artefact.unlink(self, id.clone(), mappings)?;
            if r {
                self.unbind_onramp(id)?;
            };
            Ok(r)
        } else {
            Err(format!("Onramp {:?} not found.", id).into())
        }
    }

    pub fn bind_offramp(&self, id: TremorURL) -> Result<ActivationState> {
        info!("Binding offramp {}", id);
        match (&self.repo.find_offramp(id.clone())?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let servant = ActivatorLifecycleFsm::new(
                    self.clone(),
                    artefact.artefact.to_owned(),
                    id.clone(),
                )?;
                self.repo.bind_offramp(id.clone())?;
                self.reg.publish_offramp(id, servant)
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    pub fn unbind_offramp(&self, id: TremorURL) -> Result<ActivationState> {
        info!("Unbinding offramp {}", id);
        match (&self.repo.find_offramp(id.clone())?, &id.instance()) {
            (Some(_artefact), Some(_instsance_id)) => {
                let r = self.reg.unpublish_offramp(id.clone());
                self.repo.unbind_offramp(id)?;
                r
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    pub fn link_offramp(
        &self,
        id: TremorURL,
        mappings: HashMap<
            <OfframpArtefact as Artefact>::LinkLHS,
            <OfframpArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<OfframpArtefact as Artefact>::LinkResult> {
        if let Some(offramp_a) = self.repo.find_offramp(id.clone())? {
            if self.reg.find_offramp(id.clone())?.is_none() {
                self.bind_offramp(id.clone())?;
            };
            offramp_a.artefact.link(self, id, mappings)
        } else {
            Err(format!("Offramp {:?} not found.", id).into())
        }
    }

    pub fn unlink_offramp(
        &self,
        id: TremorURL,
        mappings: HashMap<
            <OfframpArtefact as Artefact>::LinkLHS,
            <OfframpArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<OfframpArtefact as Artefact>::LinkResult> {
        if let Some(offramp_a) = self.repo.find_offramp(id.clone())? {
            let r = offramp_a.artefact.unlink(self, id.clone(), mappings)?;
            if r {
                self.unbind_offramp(id)?;
            };
            Ok(r)
        } else {
            Err(format!("Offramp {:?} not found.", id).into())
        }
    }

    pub fn bind_binding_a(
        &self,
        id: TremorURL,
        artefact: BindingArtefact,
    ) -> Result<ActivationState> {
        info!("Binding binding {}", id);
        match &id.instance() {
            Some(_instance_id) => {
                let servant =
                    ActivatorLifecycleFsm::new(self.clone(), artefact.to_owned(), id.clone())?;
                self.repo.bind_binding(id.clone())?;
                self.reg.publish_binding(id, servant)
            }
            None => Err(format!("Invalid URI for instance {}", id).into()),
        }
    }

    pub fn unbind_binding_a(
        &self,
        id: TremorURL,
        _artefact: BindingArtefact,
    ) -> Result<ActivationState> {
        info!("Unbinding binding {}", id);
        match &id.instance() {
            Some(_instance_id) => {
                let servant = self.reg.unpublish_binding(id.clone())?;
                self.repo.unbind_binding(id)?;
                Ok(servant)
            }
            None => Err(format!("Invalid URI for instance {}", id).into()),
        }
    }

    pub fn link_binding(
        &self,
        id: TremorURL,
        mappings: HashMap<
            <BindingArtefact as Artefact>::LinkLHS,
            <BindingArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<BindingArtefact as Artefact>::LinkResult> {
        if let Some(binding_a) = self.repo.find_binding(id.clone())? {
            let r = binding_a.artefact.link(self, id.clone(), mappings)?;
            if self.reg.find_binding(id.clone())?.is_none() {
                self.bind_binding_a(id, r.clone())?;
            };
            Ok(r)
        } else {
            Err(format!("Binding {:?} not found.", id).into())
        }
    }

    pub fn to_config(&self) -> Result<Config> {
        let pipeline: PipelineVec = self
            .repo
            .serialize_pipelines()?
            .into_iter()
            .map(|p| p.pipeline.config)
            .collect();
        let onramp: OnRampVec = self.repo.serialize_onramps()?;
        let offramp: OffRampVec = self.repo.serialize_offramps()?;
        let binding: BindingVec = self
            .repo
            .serialize_bindings()?
            .into_iter()
            .map(|b| b.binding)
            .collect();
        let mapping: MappingMap = self.reg.serialize_mappings()?;
        let config = crate::config::Config {
            pipeline,
            onramp,
            offramp,
            binding,
            mapping,
        };
        Ok(config)
    }

    pub fn save_config(&self) -> Result<String> {
        if let Some(storage_directory) = &self.storage_directory {
            let config = self.to_config()?;
            let path = Path::new(storage_directory);
            let file_name = format!("config_{}.yaml", nanotime());
            let mut file_path = path.to_path_buf();
            file_path.push(Path::new(&file_name));
            info!(
                "Serializing configuration to file {}",
                file_path.to_string_lossy()
            );
            let mut f = File::create(file_path.clone())?;
            f.write_all(&serde_yaml::to_vec(&config)?)?;
            // lets really sync this!
            f.sync_all()?;
            f.sync_all()?;
            f.sync_all()?;
            Ok(file_path.to_string_lossy().to_string())
        } else {
            Ok("".to_string())
        }
    }

    pub fn unlink_binding(
        &self,
        id: TremorURL,
        mappings: HashMap<
            <BindingArtefact as Artefact>::LinkLHS,
            <BindingArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<BindingArtefact as Artefact>::LinkResult> {
        if let Some(mapping) = self.reg.find_binding(id.clone())? {
            if mapping.unlink(self, id.clone(), mappings)? {
                self.unbind_binding_a(id, mapping.clone())?;
            }
            return Ok(mapping);
        }
        Err(format!("Binding {:?} not found.", id).into())
    }

    pub fn start(
        qsize: usize,
        storage_directory: Option<String>,
    ) -> Result<(Self, JoinHandle<i32>)> {
        use crate::{offramp, onramp};
        use std::sync::mpsc;
        use std::thread;

        let (tx, rx) = mpsc::channel();
        let onramp_t = thread::spawn(|| {
            info!("Onramp thread started");
            actix::System::run(move || {
                let manager = onramp::Manager::create(|_ctx| onramp::Manager::default());

                if let Err(e) = tx.send(manager) {
                    error!("Failed to send manager info: {}", e)
                }
            })
        });
        let onramp = rx.recv()?;

        let (tx, rx) = mpsc::channel();
        let offramp_t = thread::spawn(move || {
            info!("Offramp thread started");
            actix::System::run(move || {
                let manager = offramp::Manager::create(move |_ctx| offramp::Manager { qsize });

                if let Err(e) = tx.send(manager) {
                    error!("Failed to send manager info: {}", e)
                }
            })
        });
        let offramp = rx.recv()?;

        let (tx, rx) = mpsc::channel();
        let system_t = thread::spawn(move || {
            actix::System::run(move || {
                let system = Manager::create(move |_ctx| Manager {
                    offramp,
                    onramp,
                    offramp_t,
                    onramp_t,
                    qsize,
                });

                if let Err(e) = tx.send((system, Repositories::new(), Registries::new())) {
                    error!("Failed to send manager info: {}", e)
                }
            })
        });

        let (system, repo, reg) = rx.recv()?;
        let mut world = World {
            system,
            repo,
            reg,
            storage_directory,
            system_pipelines: HashMap::new(),
            system_onramps: HashMap::new(),
            system_offramps: HashMap::new(),
        };
        world.register_system()?;
        Ok((world, system_t))
    }

    fn register_system(&mut self) -> Result<()> {
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
        let artefact = PipelineArtefact {
            pipeline: tremor_pipeline::build_pipeline(metric_config)?,
        };
        self.repo
            .publish_pipeline(METRICS_PIPELINE.clone(), true, artefact)?;
        self.bind_pipeline(METRICS_PIPELINE.clone())?;

        let addr = self
            .reg
            .find_pipeline(METRICS_PIPELINE.clone())?
            .ok_or_else(|| Error::from("Failed to initialize metrics pipeline."))?;
        self.system_pipelines.insert(METRICS_PIPELINE.clone(), addr);

        // Register stdout offramp
        let artefact: OfframpArtefact = serde_yaml::from_str(
            r#"
id: system::stdout
type: stdout
"#,
        )?;
        self.repo
            .publish_offramp(STDOUT_OFFRAMP.clone(), true, artefact)?;
        self.bind_offramp(STDOUT_OFFRAMP.clone())?;
        let addr = self
            .reg
            .find_offramp(STDOUT_OFFRAMP.clone())?
            .ok_or_else(|| Error::from("Failed to initialize stdout offramp."))?;
        self.system_offramps.insert(STDOUT_OFFRAMP.clone(), addr);

        // Register stderr offramp
        let artefact: OfframpArtefact = serde_yaml::from_str(
            r#"
id: system::stderr
type: stderr
"#,
        )?;
        self.repo
            .publish_offramp(STDERR_OFFRAMP.clone(), true, artefact)?;
        self.bind_offramp(STDERR_OFFRAMP.clone())?;
        let addr = self
            .reg
            .find_offramp(STDERR_OFFRAMP.clone())?
            .ok_or_else(|| Error::from("Failed to initialize stderr offramp."))?;
        self.system_offramps.insert(STDERR_OFFRAMP.clone(), addr);

        Ok(())
    }
    pub fn start_pipeline(&self, config: PipelineArtefact, id: ServantId) -> Result<PipelineAddr> {
        self.system.send(CreatePipeline { id, config }).wait()?
    }
}
