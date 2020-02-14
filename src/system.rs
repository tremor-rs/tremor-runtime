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
use actix;
use actix::prelude::*;
use actix::Addr as ActixAddr;
use crossbeam_channel::{bounded, Sender};
use futures::future::Future;
use hashbrown::HashMap;
use std::borrow::Cow;
use std::fmt;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::thread;
use std::thread::JoinHandle;
use tremor_pipeline;
use tremor_pipeline::Event;

pub(crate) use crate::offramp;
pub(crate) use crate::onramp;

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

pub(crate) type Addr = ActixAddr<Manager>;
pub(crate) type ActixHandle = JoinHandle<std::result::Result<(), std::io::Error>>;

#[derive(Debug)]
pub(crate) struct Manager {
    pub offramp: ActixAddr<offramp::Manager>,
    pub onramp: ActixAddr<onramp::Manager>,
    pub offramp_t: ActixHandle,
    pub onramp_t: ActixHandle,
    pub qsize: usize,
}

impl Actor for Manager {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!("Pipeline manager started");
    }
}

/// Address for a a pipeline
#[derive(Clone)]
pub struct PipelineAddr {
    pub(crate) addr: Sender<PipelineMsg>,
    pub(crate) id: ServantId,
}

impl fmt::Debug for PipelineAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Pipeline({})", self.id)
    }
}

pub(crate) struct CreatePipeline {
    pub config: PipelineArtefact,
    pub id: ServantId,
}

impl Message for CreatePipeline {
    type Result = Result<PipelineAddr>;
}

#[derive(Debug)]
pub(crate) enum PipelineMsg {
    Event {
        event: Event,
        input: Cow<'static, str>,
    },
    ConnectOfframp(Cow<'static, str>, TremorURL, offramp::Addr),
    ConnectPipeline(Cow<'static, str>, TremorURL, PipelineAddr),
    Disconnect(Cow<'static, str>, TremorURL),
    #[allow(dead_code)]
    Signal(Event),
    Insight(Event),
}

#[derive(Debug)]
enum PipelineDest {
    Offramp(offramp::Addr),
    Pipeline(PipelineAddr),
}

impl PipelineDest {
    pub fn send_event(&self, input: Cow<'static, str>, event: Event) -> Result<()> {
        match self {
            Self::Offramp(addr) => addr.send(offramp::Msg::Event { input, event })?,
            Self::Pipeline(addr) => addr.addr.send(PipelineMsg::Event { input, event })?,
        }
        Ok(())
    }
}

impl Handler<CreatePipeline> for Manager {
    type Result = Result<PipelineAddr>;
    #[allow(clippy::too_many_lines)]
    fn handle(&mut self, req: CreatePipeline, _ctx: &mut Self::Context) -> Self::Result {
        #[inline]
        fn send_events(
            eventset: &mut Vec<(Cow<'static, str>, Event)>,
            dests: &halfbrown::HashMap<Cow<'static, str>, Vec<(TremorURL, PipelineDest)>>,
        ) -> Result<()> {
            for (output, event) in eventset.drain(..) {
                if let Some(dest) = dests.get(&output) {
                    let len = dest.len();
                    //We know we have len, so grabbing len - 1 elementsis safe
                    for (id, offramp) in unsafe { dest.get_unchecked(..len - 1) } {
                        offramp.send_event(
                            id.instance_port()
                                .ok_or_else(|| {
                                    Error::from(format!("missing instance port in {}.", id))
                                })?
                                .clone()
                                .into(),
                            event.clone(),
                        )?;
                    }
                    //We know we have len, so grabbing the last elementsis safe
                    let (id, offramp) = unsafe { dest.get_unchecked(len - 1) };
                    offramp.send_event(
                        id.instance_port()
                            .ok_or_else(|| {
                                Error::from(format!("missing instance port in {}.", id))
                            })?
                            .clone()
                            .into(),
                        event,
                    )?;
                };
            }
            Ok(())
        }
        let config = req.config;
        let id = req.id.clone();
        let mut dests: halfbrown::HashMap<Cow<'static, str>, Vec<(TremorURL, PipelineDest)>> =
            halfbrown::HashMap::new();
        let mut eventset: Vec<(Cow<'static, str>, Event)> = Vec::new();
        let (tx, rx) = bounded::<PipelineMsg>(self.qsize);
        let mut pipeline = config.to_executable_graph(tremor_pipeline::buildin_ops)?;
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
                            match pipeline.enqueue(&input, event, &mut eventset) {
                                Ok(()) => {
                                    if let Err(e) = send_events(&mut eventset, &dests) {
                                        error!("Failed to send event: {}", e)
                                    }
                                }
                                Err(e) => error!("error: {:?}", e),
                            }
                        }
                        PipelineMsg::Insight(insight) => {
                            pipeline.contraflow(insight);
                        }
                        PipelineMsg::Signal(signal) => {
                            match pipeline.enqueue_signal(signal, &mut eventset) {
                                Ok(()) => {
                                    if let Err(e) = send_events(&mut eventset, &dests) {
                                        error!("Failed to send event: {}", e)
                                    }
                                }
                                Err(e) => error!("error: {:?}", e),
                            }
                        }

                        PipelineMsg::ConnectOfframp(output, offramp_id, offramp) => {
                            info!(
                                "[Pipeline:{}] connecting {} to offramp {}",
                                id, output, offramp_id
                            );
                            if let Some(offramps) = dests.get_mut(&output) {
                                offramps.push((offramp_id, PipelineDest::Offramp(offramp)));
                            } else {
                                dests.insert(
                                    output,
                                    vec![(offramp_id, PipelineDest::Offramp(offramp))],
                                );
                            }
                        }
                        PipelineMsg::ConnectPipeline(output, pipeline_id, pipeline) => {
                            info!(
                                "[Pipeline:{}] connecting {} to pipeline {}",
                                id, output, pipeline_id
                            );
                            if let Some(offramps) = dests.get_mut(&output) {
                                offramps.push((pipeline_id, PipelineDest::Pipeline(pipeline)));
                            } else {
                                dests.insert(
                                    output,
                                    vec![(pipeline_id, PipelineDest::Pipeline(pipeline))],
                                );
                            }
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

impl Handler<offramp::Create> for Manager {
    type Result = Result<offramp::Addr>;
    fn handle(&mut self, req: offramp::Create, _ctx: &mut Self::Context) -> Self::Result {
        self.offramp
            .send(req)
            .wait()
            .map_err(|e| Error::from(format!("Milbox error: {:?}", e)))?
    }
}

impl Handler<onramp::Create> for Manager {
    type Result = Result<onramp::Addr>;
    fn handle(&mut self, req: onramp::Create, _ctx: &mut Self::Context) -> Self::Result {
        self.onramp
            .send(req)
            .wait()
            .map_err(|e| Error::from(format!("Milbox error: {:?}", e)))?
    }
}

pub(crate) struct Stop {}

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

pub(crate) struct Count {}

impl Message for Count {
    type Result = usize;
}

/// Tremor runtime
#[derive(Clone, Debug)]
pub struct World {
    pub(crate) system: Addr,
    /// Repository
    pub repo: Repositories,
    /// Registry
    pub reg: Registries,
    system_pipelines: HashMap<ServantId, PipelineAddr>,
    system_onramps: HashMap<ServantId, onramp::Addr>,
    system_offramps: HashMap<ServantId, offramp::Addr>,
    storage_directory: Option<String>,
}

impl World {
    /// Bind a pipeline
    pub fn bind_pipeline(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Binding pipeline {}", id);
        match (&self.repo.find_pipeline(id)?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let servant = ActivatorLifecycleFsm::new(
                    self.clone(),
                    artefact.artefact.to_owned(),
                    id.clone(),
                )?;
                self.repo.bind_pipeline(id)?;
                // We link to the metrics pipeline
                let res = self.reg.publish_pipeline(id, servant)?;
                let mut id = id.clone();
                id.set_port("metrics".to_owned());
                let m = vec![("metrics".to_string(), METRICS_PIPELINE.clone())]
                    .into_iter()
                    .collect();
                self.link_pipeline(&id, m)?;
                Ok(res)
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    /// Unbind a pipeline
    pub fn unbind_pipeline(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Unbinding pipeline {}", id);
        match (&self.repo.find_pipeline(id)?, &id.instance()) {
            (Some(_artefact), Some(_instance_id)) => {
                let r = self.reg.unpublish_pipeline(id)?;
                self.repo.unbind_pipeline(id)?;
                Ok(r)
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {}", id).into()),
        }
    }

    /// Stop the runtime
    pub fn stop(&self) {
        self.system.do_send(Stop {});
    }
    /// Links a pipeline
    pub fn link_pipeline(
        &self,
        id: &TremorURL,
        mappings: HashMap<
            <PipelineArtefact as Artefact>::LinkLHS,
            <PipelineArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<PipelineArtefact as Artefact>::LinkResult> {
        info!("Linking pipeline {} to {:?}", id, mappings);
        if let Some(pipeline_a) = self.repo.find_pipeline(id)? {
            if self.reg.find_pipeline(id)?.is_none() {
                self.bind_pipeline(&id)?;
            };
            pipeline_a.artefact.link(self, id, mappings)
        } else {
            Err(format!("Pipeline {} not found.", id).into())
        }
    }
    /// Unlink a pipelein
    pub fn unlink_pipeline(
        &self,
        id: &TremorURL,
        mappings: HashMap<
            <PipelineArtefact as Artefact>::LinkLHS,
            <PipelineArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<PipelineArtefact as Artefact>::LinkResult> {
        if let Some(pipeline_a) = self.repo.find_pipeline(id)? {
            let r = pipeline_a.artefact.unlink(self, id, mappings);
            if self.reg.find_pipeline(id)?.is_some() {
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
        id: &TremorURL,
        artefact: PipelineArtefact,
    ) -> Result<ActivationState> {
        self.repo.publish_pipeline(id, false, artefact)?;
        self.bind_pipeline(id)
    }
    /// Bind an onramp
    pub fn bind_onramp(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Binding onramp {}", id);
        match (&self.repo.find_onramp(id)?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let servant = ActivatorLifecycleFsm::new(
                    self.clone(),
                    artefact.artefact.to_owned(),
                    id.clone(),
                )?;
                self.repo.bind_onramp(id)?;
                // We link to the metrics pipeline
                let res = self.reg.publish_onramp(id, servant)?;
                let mut id = id.clone();
                id.set_port("metrics".to_owned());
                let m = vec![("metrics".to_string(), METRICS_PIPELINE.clone())]
                    .into_iter()
                    .collect();
                self.link_onramp(&id, m)?;
                Ok(res)
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }
    /// Unbind an onramp
    pub fn unbind_onramp(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Unbinding onramp {}", id);
        match (&self.repo.find_onramp(id)?, &id.instance()) {
            (Some(_artefact), Some(_instsance_id)) => {
                let r = self.reg.unpublish_onramp(id);
                self.repo.unbind_onramp(id)?;
                r
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    /// Link an onramp
    pub fn link_onramp(
        &self,
        id: &TremorURL,
        mappings: HashMap<
            <OnrampArtefact as Artefact>::LinkLHS,
            <OnrampArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<OnrampArtefact as Artefact>::LinkResult> {
        if let Some(onramp_a) = self.repo.find_onramp(id)? {
            if self.reg.find_onramp(id)?.is_none() {
                self.bind_onramp(&id)?;
            };
            onramp_a.artefact.link(self, id, mappings)
        } else {
            Err(format!("Onramp {:?} not found.", id).into())
        }
    }

    /// Unlink an onramp
    pub fn unlink_onramp(
        &self,
        id: &TremorURL,
        mappings: HashMap<
            <OnrampArtefact as Artefact>::LinkLHS,
            <OnrampArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<OnrampArtefact as Artefact>::LinkResult> {
        if let Some(onramp_a) = self.repo.find_onramp(id)? {
            let r = onramp_a.artefact.unlink(self, id, mappings)?;
            if r {
                self.unbind_onramp(&id)?;
            };
            Ok(r)
        } else {
            Err(format!("Onramp {:?} not found.", id).into())
        }
    }

    /// Bind an offramp
    pub fn bind_offramp(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Binding offramp {}", id);
        match (&self.repo.find_offramp(id)?, &id.instance()) {
            (Some(artefact), Some(_instance_id)) => {
                let servant = ActivatorLifecycleFsm::new(
                    self.clone(),
                    artefact.artefact.to_owned(),
                    id.clone(),
                )?;
                self.repo.bind_offramp(id)?;
                // We link to the metrics pipeline
                let res = self.reg.publish_offramp(id, servant)?;
                // TODO remove
                //let mut id = id.clone();
                //id.set_port("metrics".to_owned());
                //let m = vec![("metrics".to_string(), METRICS_PIPELINE.clone())]
                let m = vec![(METRICS_PIPELINE.clone(), id.clone())]
                    .into_iter()
                    .collect();
                self.link_offramp(&id, m)?;
                Ok(res)
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    /// Unbind an offramp
    pub fn unbind_offramp(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Unbinding offramp {}", id);
        match (&self.repo.find_offramp(id)?, &id.instance()) {
            (Some(_artefact), Some(_instsance_id)) => {
                let r = self.reg.unpublish_offramp(id);
                self.repo.unbind_offramp(id)?;
                r
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    /// Link an offramp
    pub fn link_offramp(
        &self,
        id: &TremorURL,
        mappings: HashMap<
            <OfframpArtefact as Artefact>::LinkLHS,
            <OfframpArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<OfframpArtefact as Artefact>::LinkResult> {
        if let Some(offramp_a) = self.repo.find_offramp(id)? {
            if self.reg.find_offramp(id)?.is_none() {
                self.bind_offramp(&id)?;
            };
            offramp_a.artefact.link(self, id, mappings)
        } else {
            Err(format!("Offramp {:?} not found.", id).into())
        }
    }

    /// Unlink an offramp
    pub fn unlink_offramp(
        &self,
        id: &TremorURL,
        mappings: HashMap<
            <OfframpArtefact as Artefact>::LinkLHS,
            <OfframpArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<OfframpArtefact as Artefact>::LinkResult> {
        if let Some(offramp_a) = self.repo.find_offramp(id)? {
            let r = offramp_a.artefact.unlink(self, id, mappings)?;
            if r {
                self.unbind_offramp(id)?;
            };
            Ok(r)
        } else {
            Err(format!("Offramp {:?} not found.", id).into())
        }
    }

    pub(crate) fn bind_binding_a(
        &self,
        id: &TremorURL,
        artefact: &BindingArtefact,
    ) -> Result<ActivationState> {
        info!("Binding binding {}", id);
        match &id.instance() {
            Some(_instance_id) => {
                let servant =
                    ActivatorLifecycleFsm::new(self.clone(), artefact.to_owned(), id.clone())?;
                self.repo.bind_binding(id)?;
                self.reg.publish_binding(id, servant)
            }
            None => Err(format!("Invalid URI for instance {}", id).into()),
        }
    }

    pub(crate) fn unbind_binding_a(
        &self,
        id: &TremorURL,
        _artefact: &BindingArtefact,
    ) -> Result<ActivationState> {
        info!("Unbinding binding {}", id);
        match &id.instance() {
            Some(_instance_id) => {
                let servant = self.reg.unpublish_binding(id)?;
                self.repo.unbind_binding(id)?;
                Ok(servant)
            }
            None => Err(format!("Invalid URI for instance {}", id).into()),
        }
    }

    /// Links a binding
    pub fn link_binding(
        &self,
        id: &TremorURL,
        mappings: HashMap<
            <BindingArtefact as Artefact>::LinkLHS,
            <BindingArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<BindingArtefact as Artefact>::LinkResult> {
        if let Some(binding_a) = self.repo.find_binding(id)? {
            let r = binding_a.artefact.link(self, id, mappings)?;
            if self.reg.find_binding(id)?.is_none() {
                self.bind_binding_a(id, &r)?;
            };
            Ok(r)
        } else {
            Err(format!("Binding {:?} not found.", id).into())
        }
    }

    /// Turns the running system into a config
    pub fn to_config(&self) -> Result<Config> {
        let pipeline: PipelineVec = self
            .repo
            .serialize_pipelines()?
            .into_iter()
            .filter_map(|p| match p {
                PipelineArtefact::Pipeline(p) => Some(p.config),
                PipelineArtefact::Query(_q) => None, // FIXME
            })
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
    /// Saves the current config
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

    /// Unlinks a binding
    pub fn unlink_binding(
        &self,
        id: &TremorURL,
        mappings: HashMap<
            <BindingArtefact as Artefact>::LinkLHS,
            <BindingArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<BindingArtefact as Artefact>::LinkResult> {
        if let Some(mapping) = self.reg.find_binding(id)? {
            if mapping.unlink(self, id, mappings)? {
                self.unbind_binding_a(id, &mapping)?;
            }
            return Ok(mapping);
        }
        Err(format!("Binding {:?} not found.", id).into())
    }

    /// Starts the runtime system
    pub fn start(qsize: usize, storage_directory: Option<String>) -> Result<(Self, ActixHandle)> {
        use std::sync::mpsc;

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
        let mut world = Self {
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
        let artefact =
            PipelineArtefact::Pipeline(Box::new(tremor_pipeline::build_pipeline(metric_config)?));
        self.repo
            .publish_pipeline(&METRICS_PIPELINE, true, artefact)?;
        self.bind_pipeline(&METRICS_PIPELINE)?;

        let addr = self
            .reg
            .find_pipeline(&METRICS_PIPELINE)?
            .ok_or_else(|| Error::from("Failed to initialize metrics pipeline."))?;
        self.system_pipelines.insert(METRICS_PIPELINE.clone(), addr);

        // Register stdout offramp
        let artefact: OfframpArtefact = serde_yaml::from_str(
            r#"
id: system::stdout
type: stdout
"#,
        )?;
        self.repo.publish_offramp(&STDOUT_OFFRAMP, true, artefact)?;
        self.bind_offramp(&STDOUT_OFFRAMP)?;
        let addr = self
            .reg
            .find_offramp(&STDOUT_OFFRAMP)?
            .ok_or_else(|| Error::from("Failed to initialize stdout offramp."))?;
        self.system_offramps.insert(STDOUT_OFFRAMP.clone(), addr);

        // Register stderr offramp
        let artefact: OfframpArtefact = serde_yaml::from_str(
            r#"
id: system::stderr
type: stderr
"#,
        )?;
        self.repo.publish_offramp(&STDERR_OFFRAMP, true, artefact)?;
        self.bind_offramp(&STDERR_OFFRAMP)?;
        let addr = self
            .reg
            .find_offramp(&STDERR_OFFRAMP)?
            .ok_or_else(|| Error::from("Failed to initialize stderr offramp."))?;
        self.system_offramps.insert(STDERR_OFFRAMP.clone(), addr);

        Ok(())
    }

    pub(crate) fn start_pipeline(
        &self,
        config: PipelineArtefact,
        id: ServantId,
    ) -> Result<PipelineAddr> {
        self.system.send(CreatePipeline { id, config }).wait()?
    }
}
