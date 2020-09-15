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

use crate::codec;
use crate::errors::{Error, Result};
use crate::metrics::RampReporter;
use crate::offramp;
use crate::onramp;
use crate::pipeline;
use crate::registry::ServantId;
use crate::system::{self, World};
use crate::url::{ResourceType, TremorURL};
use hashbrown::HashMap;
use tremor_pipeline::query;
pub(crate) type Id = TremorURL;
pub(crate) use crate::OffRamp as OfframpArtefact;
pub(crate) use crate::OnRamp as OnrampArtefact;
use async_std::sync::channel;
use async_trait::async_trait;
use crossbeam_channel::bounded;

/// A Binding
#[derive(Clone, Debug)]
pub struct Binding {
    /// The binding itself
    pub binding: crate::Binding,
    /// The mappings
    pub mapping: Option<crate::config::MappingMap>,
}

/// A Pipeline
#[derive(Clone)]
pub enum Pipeline {
    /// A normal pipeline
    Pipeline(Box<tremor_pipeline::Pipeline>),
    /// A query based pipeline
    Query(query::Query),
}

impl Pipeline {
    pub(crate) fn to_executable_graph(
        &self,
        resolver: tremor_pipeline::NodeLookupFn,
    ) -> Result<tremor_pipeline::ExecutableGraph> {
        let mut g = match self {
            Self::Pipeline(p) => p.to_executable_graph(resolver)?,
            Self::Query(q) => q.to_pipe()?,
        };
        g.optimize();
        Ok(g)
    }
}

impl From<tremor_pipeline::Pipeline> for Pipeline {
    fn from(pipeline: tremor_pipeline::Pipeline) -> Self {
        Self::Pipeline(Box::new(pipeline))
    }
}

impl From<query::Query> for Pipeline {
    fn from(query: query::Query) -> Self {
        Self::Query(query)
    }
}

#[async_trait]
pub trait Artefact: Clone {
    //    type Configuration;
    type SpawnResult: Clone;
    type LinkResult: Clone;
    type LinkLHS: Clone;
    type LinkRHS: Clone;
    /// Move from Repository to Registry
    async fn spawn(&self, system: &World, servant_id: ServantId) -> Result<Self::SpawnResult>;
    /// Move from Registry(instanciated) to Registry(Active) or from one form of active to another
    /// This acts differently on bindings and the rest. Where the binding takers a mapping of string
    /// replacements, the others take a from and to id
    async fn link(
        &self,
        system: &World,
        id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult>;

    async fn unlink(
        &self,
        system: &World,
        id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<bool>;
    fn artefact_id(u: &TremorURL) -> Result<Id>;
    fn servant_id(u: &TremorURL) -> Result<ServantId>;
}

#[async_trait]
impl Artefact for Pipeline {
    type SpawnResult = pipeline::Addr;
    type LinkResult = bool;
    type LinkLHS = String;
    type LinkRHS = TremorURL;

    //    type Configuration = tremor_pipeline::Pipeline;
    async fn spawn(&self, world: &World, servant_id: ServantId) -> Result<Self::SpawnResult> {
        world.start_pipeline(self.clone(), servant_id).await
    }

    async fn link(
        &self,
        system: &World,
        id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        if let Some(pipeline) = system.reg.find_pipeline(id).await? {
            // TODO: Make this a two step 'transactional' process where all pipelines are gathered and then send
            for (from, to) in mappings {
                match to.resource_type() {
                    //TODO: Check that we really have the right ramp!
                    Some(ResourceType::Offramp) => {
                        if let Some(offramp) = system.reg.find_offramp(&to).await? {
                            pipeline
                                .addr
                                .clone()
                                .send(pipeline::Msg::ConnectOfframp(
                                    from.clone().into(),
                                    to.clone(),
                                    offramp,
                                ))
                                .map_err(|e| -> Error {
                                    format!("Could not send to pipeline: {}", e).into()
                                })?;
                        } else {
                            return Err(format!("Offramp {} not found", to).into());
                        }
                    }
                    Some(ResourceType::Pipeline) => {
                        info!("[Pipeline:{}] Linking port {} to {}", id, from, to);
                        if let Some(p) = system.reg.find_pipeline(&to).await? {
                            pipeline
                                .addr
                                .clone()
                                .send(pipeline::Msg::ConnectPipeline(
                                    from.clone().into(),
                                    to.clone(),
                                    p,
                                ))
                                .map_err(|e| -> Error {
                                    format!("Could not send to pipeline: {:?}", e).into()
                                })?;
                        } else {
                            return Err(format!("Pipeline {:?} not found", to).into());
                        }
                    }
                    _ => {
                        return Err("Source isn't a Offramp or pipeline".into());
                    }
                }
            }
            Ok(true)
        } else {
            Err(format!("Pipeline {:?} not found", id).into())
        }
    }

    async fn unlink(
        &self,
        system: &World,
        id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        if let Some(pipeline) = system.reg.find_pipeline(id).await? {
            for (from, to) in mappings {
                match to.resource_type() {
                    Some(ResourceType::Offramp) => {
                        pipeline
                            .addr
                            .send(pipeline::Msg::DisconnectOutput(from.clone().into(), to))
                            .map_err(|_e| Error::from("Failed to unlink pipeline"))?;
                    }
                    Some(ResourceType::Pipeline) => {
                        pipeline
                            .addr
                            .send(pipeline::Msg::DisconnectOutput(from.clone().into(), to))
                            .map_err(|_e| Error::from("Failed to unlink pipeline"))?;
                    }
                    _ => {
                        return Err("Source isn't an Offramp or Pipeline".into());
                    }
                }
            }
            Ok(true)
        } else {
            Err(format!("Pipeline {:?} not found", id).into())
        }
    }

    fn artefact_id(id: &TremorURL) -> Result<Id> {
        let mut id = id.clone();
        id.trim_to_artefact();
        match (&id.resource_type(), &id.artefact()) {
            (Some(ResourceType::Pipeline), Some(_id)) => Ok(id),
            _ => Err("URL does not contain a pipeline artifact id".into()),
        }
    }
    fn servant_id(id: &TremorURL) -> Result<ServantId> {
        let mut id = id.clone();
        id.trim_to_instance();
        match (&id.resource_type(), &id.instance()) {
            (Some(ResourceType::Pipeline), Some(_id)) => Ok(id),
            _ => Err("URL does not contain a pipeline servant id".into()),
        }
    }
}

#[async_trait]
impl Artefact for OfframpArtefact {
    type SpawnResult = offramp::Addr;
    type LinkResult = bool;
    type LinkLHS = TremorURL;
    type LinkRHS = TremorURL;
    async fn spawn(&self, world: &World, servant_id: ServantId) -> Result<Self::SpawnResult> {
        //TODO: define offramp by config!
        let offramp = offramp::lookup(&self.binding_type, &self.config)?;
        let codec = if let Some(codec) = &self.codec {
            codec::lookup(&codec)?
        } else {
            codec::lookup(offramp.default_codec())?
        };
        let postprocessors = if let Some(postprocessors) = &self.postprocessors {
            postprocessors.clone()
        } else {
            vec![]
        };
        let metrics_reporter = RampReporter::new(servant_id.clone(), self.metrics_interval_s);

        let (tx, rx) = channel(1);

        world
            .system
            .send(system::ManagerMsg::CreateOfframp(
                tx,
                offramp::Create {
                    id: servant_id,
                    codec,
                    offramp,
                    postprocessors,
                    metrics_reporter,
                },
            ))
            .await;
        rx.recv().await?
    }
    async fn link(
        &self,
        system: &World,
        id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        info!("Linking offramp {} ..", id);
        if let Some(offramp) = system.reg.find_offramp(id).await? {
            for (pipeline_id, _this) in mappings {
                info!("Linking offramp {} to {}", id, pipeline_id);
                if let Some(pipeline) = system.reg.find_pipeline(&pipeline_id).await? {
                    offramp.send(offramp::Msg::Connect {
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

    async fn unlink(
        &self,
        system: &World,
        id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        info!("Linking offramp {} ..", id);
        if let Some(offramp) = system.reg.find_offramp(id).await? {
            let (tx, rx) = bounded(mappings.len());
            for (_this, pipeline_id) in mappings {
                offramp.send(offramp::Msg::Disconnect {
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

    fn artefact_id(id: &TremorURL) -> Result<Id> {
        let mut id = id.clone();
        id.trim_to_artefact();
        match (&id.resource_type(), &id.artefact()) {
            (Some(ResourceType::Offramp), Some(_)) => Ok(id),
            _ => Err(format!("URL does not contain an offramp artifact id: {}", id).into()),
        }
    }
    fn servant_id(id: &TremorURL) -> Result<ServantId> {
        let mut id = id.clone();
        id.trim_to_instance();
        match (&id.resource_type(), &id.instance()) {
            (Some(ResourceType::Offramp), Some(_)) => Ok(id),
            _ => Err("URL does not contain a offramp servant id".into()),
        }
    }
}
#[async_trait]
impl Artefact for OnrampArtefact {
    type SpawnResult = onramp::Addr;
    type LinkResult = bool;
    type LinkLHS = String;
    type LinkRHS = TremorURL;
    async fn spawn(&self, world: &World, servant_id: ServantId) -> Result<Self::SpawnResult> {
        let stream = onramp::lookup(&self.binding_type, &self.config)?;
        let codec = if let Some(codec) = &self.codec {
            codec.clone()
        } else {
            stream.default_codec().to_string()
        };
        let preprocessors = if let Some(preprocessors) = &self.preprocessors {
            preprocessors.clone()
        } else {
            vec![]
        };
        let metrics_reporter = RampReporter::new(servant_id.clone(), self.metrics_interval_s);
        let (tx, rx) = channel(1);

        world
            .system
            .send(system::ManagerMsg::CreateOnrampt(
                tx,
                onramp::Create {
                    id: servant_id,
                    preprocessors,
                    codec,
                    stream,
                    metrics_reporter,
                },
            ))
            .await;
        rx.recv().await?
    }

    async fn link(
        &self,
        system: &World,
        id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        if let Some(onramp) = system.reg.find_onramp(id).await? {
            // TODO: Make this a two step 'transactional' process where all pipelines are gathered and then send
            for (_from, to) in mappings {
                //TODO: Check that we really have the right onramp!
                if let Some(ResourceType::Pipeline) = to.resource_type() {
                    if let Some(pipeline) = system.reg.find_pipeline(&to).await? {
                        onramp
                            .send(onramp::Msg::Connect(vec![(to.clone(), pipeline)]))
                            .await;
                    } else {
                        return Err(format!("Pipeline {:?} not found", to).into());
                    }
                } else {
                    return Err("Destination isn't a Pipeline".into());
                }
            }
            Ok(true)
        } else {
            Err(format!("Pipeline {:?} not found", id).into())
        }
    }

    async fn unlink(
        &self,
        system: &World,
        id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<bool> {
        if let Some(onramp) = system.reg.find_onramp(id).await? {
            let mut links = Vec::new();
            let (tx, rx) = bounded(mappings.len());

            for to in mappings.values() {
                links.push(to.to_owned())
            }
            for (_port, pipeline_id) in mappings {
                onramp
                    .send(onramp::Msg::Disconnect {
                        id: pipeline_id,
                        tx: tx.clone(),
                    })
                    .await;
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

    fn artefact_id(id: &TremorURL) -> Result<Id> {
        let mut id = id.clone();
        id.trim_to_artefact();
        match (&id.resource_type(), &id.artefact()) {
            (Some(ResourceType::Onramp), Some(_)) => Ok(id),
            _ => Err(format!("URL {} does not contain a onramp artifact id", id).into()),
        }
    }
    fn servant_id(id: &TremorURL) -> Result<ServantId> {
        let mut id = id.clone();
        id.trim_to_instance();
        match (&id.resource_type(), &id.instance()) {
            (Some(ResourceType::Onramp), Some(_id)) => Ok(id),
            _ => Err(format!("URL {} does not contain a onramp servant id", id).into()),
        }
    }
}

#[async_trait]
impl Artefact for Binding {
    type SpawnResult = Self;
    type LinkResult = Self;
    type LinkLHS = String;
    type LinkRHS = String;
    async fn spawn(&self, _: &World, _: ServantId) -> Result<Self::SpawnResult> {
        //TODO: Validate
        Ok(self.clone())
    }

    #[allow(clippy::too_many_lines, clippy::cognitive_complexity)]
    async fn link(
        &self,
        system: &World,
        id: &TremorURL,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        let mut pipelines: Vec<(TremorURL, TremorURL)> = Vec::new();
        let mut onramps: Vec<(TremorURL, TremorURL)> = Vec::new();
        let mut res = self.clone();
        res.binding.links.clear();
        for (src, dsts) in self.binding.links.clone() {
            // TODO: It should be validated ahead of time that every mapping has an instance!
            // * is a port
            // *  is a combination of on and offramp
            if let Some(mut instance) = src.instance().clone() {
                for (map_name, map_replace) in &mappings {
                    let mut f = String::from("%7B");
                    f.push_str(map_name.as_str());
                    f.push_str("%7D");
                    instance = instance.as_str().replace(f.as_str(), map_replace.as_str());
                }
                let mut from = src.clone();
                from.set_instance(instance.to_owned());
                let mut tos: Vec<TremorURL> = Vec::new();
                for dst in dsts {
                    // TODO: It should be validated ahead of time that every mapping has an instance!
                    if let Some(mut instance) = dst.instance().clone() {
                        // Thisbecause it is an URL we have to use escape codes
                        for (map_name, map_replace) in &mappings {
                            let mut f = String::from("%7B");
                            f.push_str(map_name.as_str());
                            f.push_str("%7D");
                            instance = instance.as_str().replace(f.as_str(), map_replace.as_str());
                        }
                        let mut to = dst.clone();
                        to.set_instance(instance);
                        tos.push(to.clone());
                        match (&from.resource_type(), &to.resource_type()) {
                            //TODO: Check that we really have the right onramp!
                            (Some(ResourceType::Onramp), Some(ResourceType::Pipeline)) => {
                                onramps.push((from.clone(), to))
                            }
                            (Some(ResourceType::Pipeline), Some(ResourceType::Offramp))
                            | (Some(ResourceType::Pipeline), Some(ResourceType::Pipeline)) => {
                                pipelines.push((from.clone(), to))
                            }
                            (_, _) => {
                                return Err(
                                    "links require the form of onramp -> pipeline or pipeline -> offramp or pipeline -> pipeline"
                                        .into(),
                                );
                            }
                        };
                    }
                }
                res.binding.links.insert(from, tos);
            }
        }

        for (from, to) in pipelines {
            info!("Binding {} to {}", from, to);
            match to.resource_type() {
                Some(ResourceType::Offramp) => {
                    if system.reg.find_offramp(&to).await?.is_none() {
                        info!("Offramp not found during binding process, binding {} to create a new instance.", &to);
                        system.bind_offramp(&to).await?;
                    } else {
                        info!("Existing offramp {} found", to);
                    }
                }
                Some(ResourceType::Pipeline) => {
                    if system.reg.find_pipeline(&to).await?.is_none() {
                        info!("Pipeline not found during binding process, binding {} to create a new instance.", &to);
                        system.bind_pipeline(&to).await?;
                    } else {
                        info!("Existing pipeline {} found", to);
                    }
                }
                _ => (),
            };
            if system.reg.find_pipeline(&from).await?.is_none() {
                info!(
                    "Pipeline (src) not found during binding process, binding {} to create a new instance.",
                    from
                );
                system.bind_pipeline(&from).await?;
            }
            system
                .link_pipeline(
                    &from,
                    vec![(
                        from.instance_port().ok_or_else(|| {
                            Error::from(format!("{} is missing an instnace port", from))
                        })?,
                        to.clone(),
                    )]
                    .into_iter()
                    .collect(),
                )
                .await?;
            match to.resource_type() {
                Some(ResourceType::Offramp) => {
                    let to2 = to.clone();
                    system
                        .link_offramp(&to, vec![(from, to2)].into_iter().collect())
                        .await?;
                }
                Some(ResourceType::Pipeline) => {
                    //TODO: How to reverse link onramps
                    warn!("Linking pipelines is currently only supported for system pipelines!")
                }
                _ => (),
            }
        }

        for (from, to) in onramps {
            if system.reg.find_pipeline(&to).await?.is_none() {
                info!("Pipeline (dst) not found during binding process, binding {} to create a new instance.", to);
                system.bind_pipeline(&to).await?;
            }
            if system.reg.find_onramp(&from).await?.is_none() {
                info!(
                    "Onramp not found during binding process, binding {} to create a new instance.",
                    from
                );
                system.bind_onramp(&from).await?;
            }
            system
                .link_onramp(
                    &from,
                    vec![(
                        from.instance_port().ok_or_else(|| {
                            Error::from(format!("{} is missing an instnace port", from))
                        })?,
                        to,
                    )]
                    .into_iter()
                    .collect(),
                )
                .await?;
        }
        res.mapping = Some(vec![(id.clone(), mappings)].into_iter().collect());
        Ok(res)
    }

    async fn unlink(
        &self,
        system: &World,
        _: &TremorURL,
        _: HashMap<Self::LinkLHS, Self::LinkRHS>,
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

        for (from, to) in &self.binding.links {
            if from.resource_type() == Some(ResourceType::Onramp) {
                let mut mappings = HashMap::new();
                for p in to {
                    mappings.insert(
                        from.instance_port().ok_or_else(|| {
                            Error::from(format!("{} is missing an instnace port", from))
                        })?,
                        p.clone(),
                    );
                }
                system.unlink_onramp(&from, mappings).await?;
            }
        }
        for (from, tos) in &self.binding.links {
            if from.resource_type() == Some(ResourceType::Pipeline) {
                for to in tos {
                    let mut mappings = HashMap::new();
                    if let Some(port) = from.instance_port() {
                        mappings.insert(port, to.clone());
                    } else {
                        error!("{} is missing an instnace port", from)
                    }
                    system.unlink_pipeline(&from, mappings).await?;
                    let mut mappings = HashMap::new();
                    mappings.insert(to.clone(), from.clone());
                    system.unlink_offramp(&to, mappings).await?;
                }
            }
        }
        Ok(true)
    }

    fn artefact_id(id: &TremorURL) -> Result<Id> {
        let mut id = id.clone();
        id.trim_to_artefact();
        match (&id.resource_type(), &id.artefact()) {
            (Some(ResourceType::Binding), Some(_)) => Ok(id),
            _ => Err(format!("URL {} does not contain a binding artifact id", id).into()),
        }
    }
    fn servant_id(id: &TremorURL) -> Result<ServantId> {
        let mut id = id.clone();
        id.trim_to_instance();
        match (&id.resource_type(), &id.instance()) {
            (Some(ResourceType::Binding), Some(_id)) => Ok(id),
            _ => Err(format!("URL {} does not contain a binding servant id", id).into()),
        }
    }
}
