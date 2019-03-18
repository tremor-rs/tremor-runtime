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

use crate::dynamic::offramp::{self, OfframpAddr, OfframpMsg};
use crate::dynamic::onramp::{self, OnrampAddr};
use crate::errors::*;
use crate::lifecycle::{ActivationState, ActivatorLifecycleFsm};
use crate::registry::{Registries, ServantId};
use crate::repository::{
    Artefact, BindingArtefact, OfframpArtefact, OnrampArtefact, PipelineArtefact, Repositories,
};
use crate::url::TremorURL;
use actix;
use actix::prelude::*;
use crossbeam_channel::bounded;
use crossbeam_channel::Sender;
use futures::future::Future;
use hashbrown::HashMap;
use std::fmt;
use std::thread;
use std::thread::JoinHandle;
use tremor_pipeline;
use tremor_pipeline::Event;

pub use crate::dynamic::offramp::CreateOfframp;
pub use crate::dynamic::onramp::CreateOnramp;

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
    pub config: tremor_pipeline::Pipeline,
    pub id: ServantId,
}

impl Message for CreatePipeline {
    type Result = Result<PipelineAddr>;
}

#[derive(Debug)]
pub enum PipelineMsg {
    Event { event: Event, input: String },
    ConnectOfframp(String, TremorURL, OfframpAddr),
    DisconnectOfframp(String, TremorURL),
    Signal(Event),
    Insight(Event),
}

impl Handler<CreatePipeline> for Manager {
    type Result = Result<PipelineAddr>;
    fn handle(&mut self, req: CreatePipeline, _ctx: &mut Self::Context) -> Self::Result {
        fn send_events(
            eventset: Vec<(String, Event)>,
            offramps: &HashMap<String, Vec<(TremorURL, OfframpAddr)>>,
        ) {
            for (input, event) in eventset {
                if let Some(offramps) = offramps.get(&input) {
                    let len = offramps.len();
                    for (_, offramp) in offramps.iter().take(len - 1) {
                        let _ = offramp.send(OfframpMsg::Event {
                            input: input.clone(),
                            event: event.clone(),
                        });
                    }
                    let (_, offramp) = &offramps[len - 1];
                    let _ = offramp.send(OfframpMsg::Event { input, event });
                };
            }
        }
        let config = req.config;
        let id = req.id.clone();
        let mut offramps: HashMap<String, Vec<(TremorURL, OfframpAddr)>> = HashMap::new();
        let (tx, rx) = bounded::<PipelineMsg>(self.qsize);
        let mut pipeline = config
            .to_executable_graph(tremor_pipeline::buildin_ops)
            .unwrap();
        thread::Builder::new()
            .name(format!("pipeline-{}", id.clone()))
            .spawn(move || {
                info!("[Pipeline:{}] starting thread.", id);
                for req in rx {
                    match req {
                        PipelineMsg::Event { input, event } => {
                            match pipeline.enqueue(&input, event) {
                                Ok(eventset) => send_events(eventset, &offramps),
                                Err(e) => error!("error: {:?}", e),
                            }
                        }
                        PipelineMsg::Insight(insight) => {
                            pipeline.contraflow(insight);
                        }
                        PipelineMsg::Signal(signal) => match pipeline.signalflow(signal) {
                            Ok(eventset) => send_events(eventset, &offramps),
                            Err(e) => error!("error: {:?}", e),
                        },

                        //                    PipelineMsg::Activate => (),
                        PipelineMsg::ConnectOfframp(output, offramp_id, offramp) => {
                            info!("[Pipeline:{}] connecting {} to {}", id, output, offramp_id);
                            offramps
                                .entry(output)
                                .or_insert(vec![])
                                .push((offramp_id, offramp));
                        }
                        PipelineMsg::DisconnectOfframp(output, to_delete) => {
                            let mut remove = false;
                            if let Some(offramp_vec) = offramps.get_mut(&output) {
                                offramp_vec.retain(|(this_id, _)| this_id != &to_delete);
                                remove = offramp_vec.is_empty();
                            }
                            if remove {
                                offramps.remove(&output);
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

#[derive(Clone)]
pub struct World {
    pub system: SystemAddr,
    pub repo: Repositories,
    pub reg: Registries,
}

impl World {
    pub fn bind_pipeline(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Binding pipeline {}", id);
        match (&self.repo.find_pipeline(id)?, &id.instance()) {
            (Some(artefact), Some(instance_id)) => {
                let servant = ActivatorLifecycleFsm::new(
                    self.clone(),
                    artefact.artefact.to_owned(),
                    instance_id.to_string(),
                )?;
                self.repo.bind_pipeline(id)?;
                self.reg.publish_pipeline(&id, servant)
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    pub fn unbind_pipeline(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Unbinding pipeline {}", id);
        match (&self.repo.find_pipeline(id)?, &id.instance()) {
            (Some(_artefact), Some(_instance_id)) => {
                let r = self.reg.unpublish_pipeline(&id);
                self.repo.unbind_pipeline(id)?;
                Ok(r?)
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
        id: &TremorURL,
        mappings: HashMap<
            <PipelineArtefact as Artefact>::LinkLHS,
            <PipelineArtefact as Artefact>::LinkRHS,
        >,
    ) -> Result<<PipelineArtefact as Artefact>::LinkResult> {
        if let Some(pipeline_a) = self.repo.find_pipeline(id)? {
            if self.reg.find_pipeline(id)?.is_none() {
                self.bind_pipeline(id)?;
            };
            pipeline_a.artefact.link(self, id, mappings)
        } else {
            Err(format!("Pipeline {} not found.", id).into())
        }
    }

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
        self.repo.publish_pipeline(id, artefact)?;
        self.bind_pipeline(id)
    }

    pub fn bind_onramp(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Binding onramp {}", id);
        match (&self.repo.find_onramp(id)?, &id.instance()) {
            (Some(artefact), Some(instance_id)) => {
                let servant = ActivatorLifecycleFsm::new(
                    self.clone(),
                    artefact.artefact.to_owned(),
                    instance_id.to_string(),
                )?;
                self.repo.bind_onramp(id)?;
                self.reg.publish_onramp(&id, servant)
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    pub fn unbind_onramp(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Unbinding onramp {}", id);
        match (&self.repo.find_onramp(id)?, &id.instance()) {
            (Some(_artefact), Some(_instsance_id)) => {
                let r = self.reg.unpublish_onramp(&id);
                self.repo.unbind_onramp(id)?;
                r
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

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
                self.bind_onramp(id)?;
            };
            onramp_a.artefact.link(self, id, mappings)
        } else {
            Err(format!("Onramp {:?} not found.", id).into())
        }
    }

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
                self.unbind_onramp(id)?;
            };
            Ok(r)
        } else {
            Err(format!("Onramp {:?} not found.", id).into())
        }
    }

    pub fn bind_offramp(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Binding offramp {}", id);
        match (&self.repo.find_offramp(id)?, &id.instance()) {
            (Some(artefact), Some(instance_id)) => {
                let servant = ActivatorLifecycleFsm::new(
                    self.clone(),
                    artefact.artefact.to_owned(),
                    instance_id.to_string(),
                )?;
                self.repo.bind_offramp(id)?;
                self.reg.publish_offramp(id, servant)
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

    pub fn unbind_offramp(&self, id: &TremorURL) -> Result<ActivationState> {
        info!("Unbinding offramp {}", id);
        match (&self.repo.find_offramp(id)?, &id.instance()) {
            (Some(_artefact), Some(_instsance_id)) => {
                let r = self.reg.unpublish_offramp(&id);
                self.repo.unbind_offramp(id)?;
                r
            }
            (None, _) => Err(format!("Artefact not found: {}", id).into()),
            (_, None) => Err(format!("Invalid URI for instance {} ", id).into()),
        }
    }

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
                self.bind_offramp(id)?;
            };
            offramp_a.artefact.link(self, id, mappings)
        } else {
            Err(format!("Offramp {:?} not found.", id).into())
        }
    }

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

    pub fn bind_binding_a(
        &self,
        id: &TremorURL,
        artefact: BindingArtefact,
    ) -> Result<ActivationState> {
        info!("Binding binding {}", id);
        match &id.instance() {
            Some(instance_id) => {
                let servant = ActivatorLifecycleFsm::new(
                    self.clone(),
                    artefact.to_owned(),
                    instance_id.to_string(),
                )?;
                self.repo.bind_binding(id)?;
                self.reg.publish_binding(&id, servant)
            }
            None => Err(format!("Invalid URI for instance {}", id).into()),
        }
    }

    pub fn unbind_binding_a(
        &self,
        id: &TremorURL,
        _artefact: BindingArtefact,
    ) -> Result<ActivationState> {
        info!("Unbinding binding {}", id);
        match &id.instance() {
            Some(_instance_id) => {
                let servant = self.reg.unpublish_binding(&id)?;
                self.repo.unbind_binding(id)?;
                Ok(servant)
            }
            None => Err(format!("Invalid URI for instance {}", id).into()),
        }
    }

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
                self.bind_binding_a(id, r.clone())?;
            };
            Ok(r)
        } else {
            Err(format!("Binding {:?} not found.", id).into())
        }
    }

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
                self.unbind_binding_a(id, mapping.clone())?;
            }
            return Ok(mapping);
        }
        Err(format!("Binding {:?} not found.", id).into())
    }

    pub fn start(qsize: usize) -> Result<(Self, JoinHandle<i32>)> {
        use crate::dynamic::{offramp, onramp};
        use std::sync::mpsc;
        use std::thread;

        let (tx, rx) = mpsc::channel();
        let onramp_t = thread::spawn(|| {
            info!("Onramp thread started");
            actix::System::run(move || {
                let manager = onramp::Manager::create(|_ctx| onramp::Manager::default());

                tx.send(manager).unwrap();
            })
        });
        let onramp = rx.recv()?;

        let (tx, rx) = mpsc::channel();
        let offramp_t = thread::spawn(|| {
            info!("Offramp thread started");
            actix::System::run(move || {
                let manager = offramp::Manager::create(|_ctx| offramp::Manager::default());

                tx.send(manager).unwrap();
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

                tx.send((system, Repositories::new(), Registries::new()))
                    .unwrap();
            })
        });
        let (system, repo, reg) = rx.recv()?;
        Ok((World { system, repo, reg }, system_t))
    }
    pub fn start_pipeline(&self, config: PipelineArtefact, id: ServantId) -> Result<PipelineAddr> {
        self.system.send(CreatePipeline { id, config }).wait()?
    }
}
