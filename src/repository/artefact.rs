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

use crate::dynamic::codec;
use crate::dynamic::offramp::{self, CreateOfframp, OfframpAddr, OfframpMsg};
use crate::dynamic::onramp::{self, CreateOnramp, OnrampAddr, OnrampMsg};
use crate::errors::*;
use crate::registry::ServantId;
use crate::system::{PipelineAddr, PipelineMsg, World};
use crate::url::{self, TremorURL};
use futures::future::Future;
use hashbrown::HashMap;
use maplit::hashmap;

pub type ArtefactId = String;
pub use crate::dynamic::Binding as BindingArtefact;
pub use crate::dynamic::OffRamp as OfframpArtefact;
pub use crate::dynamic::OnRamp as OnrampArtefact;
use crossbeam_channel::bounded;
pub use tremor_pipeline::Pipeline as PipelineArtefact;

pub trait Artefact: Clone {
    //    type Configuration;
    type SpawnResult: Clone;
    type LinkResult: Clone;
    type LinkLHS: Clone;
    type LinkRHS: Clone;
    /// Move from Repository to Registry
    fn spawn(&self, system: &World, servant_id: ServantId) -> Result<Self::SpawnResult>;
    /// Move from Registry(instanciated) to Registry(Active) or from one form of active to another
    /// This acts differently on bindings and the rest. Where the binding takers a mapping of string
    /// replacements, the others take a from and to id
    fn link(
        &self,
        system: &World,
        id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult>;

    fn unlink(
        &self,
        system: &World,
        id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<bool>;
    fn artefact_id(u: &TremorURL) -> Result<String>;
    fn servant_id(u: &TremorURL) -> Result<String>;
    //    fn new(Configuration) -> Result<Self>;
}

impl Artefact for PipelineArtefact {
    type SpawnResult = PipelineAddr;
    type LinkResult = bool;
    type LinkLHS = String;
    type LinkRHS = TremorURL;

    //    type Configuration = tremor_pipeline::Pipeline;
    fn spawn(&self, world: &World, servant_id: ServantId) -> Result<Self::SpawnResult> {
        world.start_pipeline(self.clone(), servant_id)
    }

    fn link(
        &self,
        system: &World,
        id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        if let Some(pipeline) = system.reg.find_pipeline(&id)? {
            // TODO: Make this a two step 'transactional' process where all pipelines are gathered and then send
            for (from, to) in mappings {
                match to.resource_type() {
                    //TODO: Check that we really have the right ramp!
                    Some(url::ResourceType::Offramp) => {
                        if let Some(offramp) = system.reg.find_offramp(&to)? {
                            pipeline
                                .addr
                                .clone()
                                .send(PipelineMsg::ConnectOfframp(from.clone(), to, offramp))
                                .map_err(|e| -> Error {
                                    format!("Could not send to pipeline: {:?}", e).into()
                                })?;
                        } else {
                            return Err(format!("Pipeline {:?} not found", to).into());
                        }
                    }
                    _ => {
                        return Err("Source isn't a Offramp".into());
                    }
                }
            }
            Ok(true)
        } else {
            Err(format!("Pipeline {:?} not found", id).into())
        }
    }

    fn unlink(
        &self,
        system: &World,
        id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        if let Some(pipeline) = system.reg.find_pipeline(&id)? {
            for (from, to) in mappings {
                match to.resource_type() {
                    Some(url::ResourceType::Offramp) => {
                        pipeline
                            .addr
                            .send(PipelineMsg::DisconnectOfframp(from.clone(), to))
                            .map_err(|_e| Error::from("Failed to unlink pipeline"))?;
                    }
                    _ => {
                        return Err("Source isn't an Offramp".into());
                    }
                }
            }
            Ok(true)
        } else {
            Err(format!("Pipeline {:?} not found", id).into())
        }
    }

    fn artefact_id(id: &TremorURL) -> Result<String> {
        match (&id.resource_type(), &id.artefact()) {
            (Some(url::ResourceType::Pipeline), Some(id)) => Ok(id.to_string()),
            _ => Err("URL does not contain a pipeline artifact id".into()),
        }
    }
    fn servant_id(id: &TremorURL) -> Result<String> {
        match (&id.resource_type(), &id.instance()) {
            (Some(url::ResourceType::Pipeline), Some(id)) => Ok(id.to_string()),
            _ => Err("URL does not contain a pipeline servant id".into()),
        }
    }
}

impl Artefact for OfframpArtefact {
    type SpawnResult = OfframpAddr;
    type LinkResult = bool;
    type LinkLHS = TremorURL;
    type LinkRHS = TremorURL;
    fn spawn(&self, world: &World, servant_id: ServantId) -> Result<Self::SpawnResult> {
        //TODO: define offramp by config!
        let offramp = offramp::lookup(self.binding_type.clone(), self.config.clone())?;
        let codec = if let Some(codec) = &self.codec {
            codec::lookup(&codec)?
        } else {
            codec::lookup(offramp.default_codec())?
        };
        let res = world
            .system
            .send(CreateOfframp {
                id: servant_id,
                codec,
                offramp,
            })
            .wait()??;
        Ok(res)
    }
    fn link(
        &self,
        system: &World,
        id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        info!("Linking offramp {} ..", id);
        if let Some(offramp) = system.reg.find_offramp(id)? {
            for (pipeline_id, _this) in mappings {
                info!("Linking offramp {} to {}", id, pipeline_id);
                if let Some(pipeline) = system.reg.find_pipeline(&pipeline_id)? {
                    offramp.send(OfframpMsg::Connect {
                        id: pipeline_id,
                        addr: pipeline,
                    })?;
                };
            }
            Ok(true)
        } else {
            Err(format!("Offramp {} not found for linking,", id).into())
        }
    }

    fn unlink(
        &self,
        system: &World,
        id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        info!("Linking offramp {} ..", id);
        if let Some(offramp) = system.reg.find_offramp(id)? {
            let (tx, rx) = bounded(mappings.len());
            for (_this, pipeline_id) in mappings {
                offramp.send(OfframpMsg::Disconnect {
                    id: pipeline_id,
                    tx: tx.clone(),
                })?;
            }
            for empty in rx {
                if empty {
                    return Ok(true);
                }
            }
            Ok(false)
        } else {
            Err(format!("Offramp {} not found for unlinking,", id).into())
        }
    }

    fn artefact_id(id: &TremorURL) -> Result<String> {
        match (&id.resource_type(), &id.artefact()) {
            (Some(url::ResourceType::Offramp), Some(id_s)) => Ok(id_s.to_string()),
            _ => Err("URL does not contain a offramp artifact id".into()),
        }
    }
    fn servant_id(id: &TremorURL) -> Result<String> {
        match (&id.resource_type(), &id.instance()) {
            (Some(url::ResourceType::Offramp), Some(id)) => Ok(id.to_string()),
            _ => Err("URL does not contain a offramp servant id".into()),
        }
    }
}
impl Artefact for OnrampArtefact {
    type SpawnResult = OnrampAddr;
    type LinkResult = bool;
    type LinkLHS = String;
    type LinkRHS = TremorURL;
    fn spawn(&self, world: &World, servant_id: ServantId) -> Result<Self::SpawnResult> {
        let stream = onramp::lookup(self.binding_type.clone(), self.config.clone())?;
        let codec = if let Some(codec) = &self.codec {
            codec.clone()
        } else {
            stream.default_codec().to_string()
        };
        let res = world
            .system
            .send(CreateOnramp {
                id: servant_id,
                codec,
                stream,
            })
            .wait()??;
        Ok(res)
    }

    fn link(
        &self,
        system: &World,
        id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        if let Some(onramp) = system.reg.find_onramp(&id)? {
            // TODO: Make this a two step 'transactional' process where all pipelines are gathered and then send
            for (_from, to) in mappings {
                match to.resource_type() {
                    //TODO: Check that we really have the right onramp!
                    Some(url::ResourceType::Pipeline) => {
                        if let Some(pipeline) = system.reg.find_pipeline(&to)? {
                            onramp.send(OnrampMsg::Connect(vec![(to.clone(), pipeline)]))?;
                        } else {
                            return Err(format!("Pipeline {:?} not found", to).into());
                        }
                    }
                    _ => {
                        return Err("Destination isn't a Pipeline".into());
                    }
                }
            }
            Ok(true)
        } else {
            Err(format!("Pipeline {:?} not found", id).into())
        }
    }

    fn unlink(
        &self,
        system: &World,
        id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<bool> {
        if let Some(onramp) = system.reg.find_onramp(&id)? {
            let mut links = Vec::new();
            let (tx, rx) = bounded(mappings.len());
            for to in mappings.values() {
                links.push(to.to_owned())
            }
            for (_port, pipeline_id) in mappings {
                onramp.send(OnrampMsg::Disconnect {
                    id: pipeline_id,
                    tx: tx.clone(),
                })?;
            }
            for empty in rx {
                if empty {
                    return Ok(true);
                }
            }
            Ok(false)
        } else {
            Err(format!("Unlinking failed Onramp {} not found ", id).into())
        }
    }

    fn artefact_id(id: &TremorURL) -> Result<String> {
        match (&id.resource_type(), &id.artefact()) {
            (Some(url::ResourceType::Onramp), Some(id)) => Ok(id.to_string()),
            _ => Err(format!("URL {} does not contain a onramp artifact id", id).into()),
        }
    }
    fn servant_id(id: &TremorURL) -> Result<String> {
        match (&id.resource_type(), &id.instance()) {
            (Some(url::ResourceType::Onramp), Some(id)) => Ok(id.to_string()),
            _ => Err(format!("URL {} does not contain a onramp servant id", id).into()),
        }
    }
}

impl Artefact for BindingArtefact {
    type SpawnResult = BindingArtefact;
    type LinkResult = BindingArtefact;
    type LinkLHS = String;
    type LinkRHS = String;
    fn spawn(&self, _world: &World, _servant_id: ServantId) -> Result<Self::SpawnResult> {
        //TODO: Validate
        Ok(self.clone())
    }

    fn link(
        &self,
        system: &World,
        _id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        let mut pipelines: Vec<(TremorURL, TremorURL)> = Vec::new();
        let mut onramps: Vec<(TremorURL, TremorURL)> = Vec::new();
        let mut res = self.clone();
        res.links.clear();
        for (src, dsts) in self.links.clone() {
            // TODO: It should be validated ahead of time that every mapping has an instance!
            // * is a port
            // *  is a combination of on and offramp
            if let Some(instance) = src.instance().clone() {
                let instance = instance.trim_start_matches("%7B").trim_end_matches("%7D");
                let from = if let Some(i) = mappings.get(instance) {
                    let mut from = src.clone();
                    from.set_instance(i.to_owned());
                    from
                } else {
                    return Err(format!("Instance {} not found for src {}", instance, src).into());
                };
                let mut tos: Vec<TremorURL> = Vec::new();
                for dst in dsts {
                    // TODO: It should be validated ahead of time that every mapping has an instance!
                    if let Some(instance) = dst.instance().clone() {
                        // Thisbecause it is an URL we have to use escape codes
                        let instance = instance.trim_start_matches("%7B").trim_end_matches("%7D");
                        if let Some(i) = mappings.get(instance) {
                            let mut to = dst.clone();
                            to.set_instance(i.to_owned());
                            tos.push(to.clone());
                            match (&from.resource_type(), &to.resource_type()) {
                                //TODO: Check that we really have the right onramp!
                                (
                                    Some(url::ResourceType::Onramp),
                                    Some(url::ResourceType::Pipeline),
                                ) => onramps.push((from.clone(), to)),
                                (
                                    Some(url::ResourceType::Pipeline),
                                    Some(url::ResourceType::Offramp),
                                ) => pipelines.push((from.clone(), to)),
                                (_, _) => {
                                    return Err(
                        "links require the from of onramp -> pipeline or pipelein -> offramp"
                                            .into(),
                                    );
                                }
                            }
                        } else {
                            return Err(
                                format!("Instance {} not found for dst {}", instance, dst).into()
                            );
                        };
                    }
                }
                res.links.insert(from, tos);
            }
        }

        for (from, to) in pipelines {
            if system.reg.find_offramp(&to)?.is_none() {
                info!("Offramp not found during binding process, binding {} to create a new instance.", &to);
                system.bind_offramp(&to)?;
            }
            if system.reg.find_pipeline(&from)?.is_none() {
                info!(
                    "Pipeline (src) not found during binding process, binding {} to create a new instance.",
                    &from
                );
                system.bind_pipeline(&from)?;
            }
            system.link_pipeline(
                &from,
                hashmap! {from.instance_port().unwrap().to_string() =>  to.clone()},
            )?;
            system.link_offramp(&to.clone(), hashmap! {from =>  to})?;
        }

        for (from, to) in onramps {
            if system.reg.find_pipeline(&to)?.is_none() {
                info!("Pipeline (dst) not found during binding process, binding {} to create a new instance.", &to);
                system.bind_pipeline(&to)?;
            }
            if system.reg.find_onramp(&from)?.is_none() {
                info!(
                    "Onramp not found during binding process, binding {} to create a new instance.",
                    &from
                );
                system.bind_onramp(&from)?;
            }
            system.link_onramp(
                &from,
                hashmap! {from.instance_port().unwrap().to_string() =>  to},
            )?;
        }
        Ok(res)
    }

    fn unlink(
        &self,
        system: &World,
        _id: &TremorURL,
        _mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<bool> {
        // TODO Quiescence Protocol ( termination correctness checks )
        //
        // We should ensure any in-flight events in a pipeline are flushed
        // to completion before unlinkining *OR* unlink should should block/wait
        // until the pipeline quiesces before returning
        //
        // For now, we let this hang wet - May require an FSM
        //
        // For example, once shutdown has been signalled via on_signal
        // we should follow through with a Quiesce signal, when all outputs
        // have signalled Quiesce we are guaranteed ( ordering ) that the Quiesce
        // signal has propagated through all branches in a pipeline. At this point
        // we can latch a quiescence condition in the pipeline which unlink or other
        // post-quiescence cleanup logic can hook off / block on etc...
        //

        for (from, to) in &self.links {
            if from.resource_type() == Some(url::ResourceType::Onramp) {
                let mut mappings = HashMap::new();
                for p in to {
                    mappings.insert(from.instance_port().unwrap(), p.clone());
                }
                system.unlink_onramp(from, mappings)?;
            }
        }
        for (from, tos) in &self.links {
            if from.resource_type() == Some(url::ResourceType::Pipeline) {
                for to in tos {
                    let mut mappings = HashMap::new();
                    mappings.insert(from.instance_port().unwrap(), to.clone());
                    system.unlink_pipeline(from, mappings)?;
                    let mut mappings = HashMap::new();
                    mappings.insert(to.clone(), from.clone());
                    system.unlink_offramp(to, mappings)?;
                }
            }
        }
        Ok(true)
    }

    fn artefact_id(id: &TremorURL) -> Result<String> {
        match (&id.resource_type(), &id.artefact()) {
            (Some(url::ResourceType::Binding), Some(id)) => Ok(id.to_string()),
            _ => Err(format!("URL {} does not contain a binding artifact id", id).into()),
        }
    }
    fn servant_id(id: &TremorURL) -> Result<String> {
        match (&id.resource_type(), &id.instance()) {
            (Some(url::ResourceType::Binding), Some(id)) => Ok(id.to_string()),
            _ => Err(format!("URL {} does not contain a binding servant id", id).into()),
        }
    }
}
