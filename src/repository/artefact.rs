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

use crate::binding;
use crate::pipeline;

use crate::system::World;
use crate::url::{ResourceType, TremorUrl};
use crate::{connectors, url};
use crate::{
    errors::{Error, ErrorKind, Result},
    pipeline::OutputTarget,
};
use beef::Cow;
use hashbrown::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use tremor_pipeline::query;
pub(crate) type Id = TremorUrl;
pub(crate) use crate::Connector as ConnectorArtefact;
use async_std::channel::bounded;
use async_std::prelude::FutureExt;
use async_trait::async_trait;

/// A Binding
#[derive(Clone, Debug)]
pub struct Binding {
    /// The binding itself
    pub binding: crate::Binding,
    /// The mappings
    pub mapping: Option<crate::config::MappingMap>,
}

impl Binding {
    /// Constructor
    #[must_use]
    pub fn new(binding: crate::Binding, mapping: Option<crate::config::MappingMap>) -> Self {
        Self { binding, mapping }
    }
}

/// A Pipeline
pub type Pipeline = query::Query;

#[async_trait]
pub trait Artefact: Clone {
    //    type Configuration;
    type SpawnResult: Clone + Send + core::fmt::Debug;
    type LinkResult: Clone;
    type LinkLHS: Clone;
    type LinkRHS: Clone;

    /// Move from Repository to Registry
    async fn spawn(&self, system: &World, instance_id: TremorUrl) -> Result<Self::SpawnResult>;
    /// Move from Registry(instantiated) to Registry(Active) or from one form of active to another
    /// This acts differently on bindings and the rest. Where the binding takes a mapping of string
    /// replacements, the others take a from and to id
    async fn link(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult>;

    async fn unlink(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<bool>;

    fn resource_type() -> url::ResourceType;

    fn artefact_id(id: &TremorUrl) -> Result<Id> {
        let mut id = id.clone();
        id.trim_to_artefact();
        let rt = Self::resource_type();
        match (id.resource_type(), id.artefact()) {
            (Some(id_rt), Some(_id)) if id_rt == rt => Ok(id),
            _ => Err(ErrorKind::InvalidTremorUrl(
                format!("Url does not contain a {} artefact id", rt),
                id.to_string(),
            )
            .into()),
        }
    }
    fn instance_id(id: &TremorUrl) -> Result<TremorUrl> {
        let id = id.to_instance();
        let rt = Self::resource_type();
        match (id.resource_type(), id.instance()) {
            (Some(id_rt), Some(_id)) if id_rt == rt => Ok(id),
            _ => Err(ErrorKind::InvalidTremorUrl(
                format!("Url does not contain a {} instance id", rt),
                id.to_string(),
            )
            .into()),
        }
    }
}

#[async_trait]
impl Artefact for Pipeline {
    type SpawnResult = pipeline::Addr;
    type LinkResult = bool;
    type LinkLHS = String;
    type LinkRHS = TremorUrl;

    fn resource_type() -> url::ResourceType {
        url::ResourceType::Pipeline
    }

    async fn spawn(&self, world: &World, instance_id: TremorUrl) -> Result<Self::SpawnResult> {
        world.spawn_pipeline(self.clone(), instance_id).await
    }

    async fn link(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        if let Some(pipeline) = system.reg.find_pipeline(id).await? {
            let mut msgs = Vec::with_capacity(mappings.len());
            for (from, to) in mappings {
                let target = match to.resource_type() {
                    Some(ResourceType::Pipeline) => {
                        // TODO: connect both ways?
                        if let Some(p) = system.reg.find_pipeline(&to).await? {
                            OutputTarget::Pipeline(Box::new(p))
                        } else {
                            return Err(ErrorKind::InstanceNotFound(
                                "pipeline".to_string(),
                                to.to_string(),
                            )
                            .into());
                        }
                    }
                    Some(ResourceType::Connector) => {
                        if let Some(connector) =
                            system.reg.find_connector(&to).await?.and_then(|c| c.sink)
                        {
                            OutputTarget::Sink(connector)
                        } else {
                            return Err(ErrorKind::InstanceNotFound(
                                "connector".to_string(),
                                to.to_string(),
                            )
                            .into());
                        }
                    }
                    _ => {
                        return Err(format!("Cannot link Pipeline to: {}.", to).into());
                    }
                };
                // link an output to this pipeline via outgoing port
                msgs.push(pipeline::MgmtMsg::ConnectOutput {
                    port: Cow::owned(from),
                    output_url: to,
                    target,
                });
            }
            for msg in msgs {
                pipeline.send_mgmt(msg).await.map_err(|e| -> Error {
                    format!("Could not send to pipeline: {}", e).into()
                })?;
            }
            Ok(true)
        } else {
            Err(format!("Pipeline {:?} not found", id).into())
        }
    }

    async fn unlink(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        info!("Unlinking pipeline {} ..", id);
        if let Some(pipeline) = system.reg.find_pipeline(id).await? {
            for (from, to) in mappings {
                match to.resource_type() {
                    Some(ResourceType::Pipeline | ResourceType::Connector) => {
                        pipeline
                            .send_mgmt(pipeline::MgmtMsg::DisconnectOutput(from.clone().into(), to))
                            .await
                            .map_err(|_e| Error::from("Failed to unlink pipeline"))?;
                    }
                    _ => {
                        return Err(format!("Cannot unlink {} from pipeline {}", to, id).into());
                    }
                }
            }
            info!("Pipeline {} unlinked.", id);
            Ok(true)
        } else {
            Err(format!("Pipeline {:?} not found", id).into())
        }
    }
}

#[async_trait]
impl Artefact for ConnectorArtefact {
    type SpawnResult = connectors::Addr;

    type LinkResult = bool;

    type LinkLHS = String;

    type LinkRHS = TremorUrl;

    fn resource_type() -> url::ResourceType {
        url::ResourceType::Connector
    }

    /// Here we only create an instance of the connector,
    /// we don't actually start it here, so it doesnt handle any events yet
    async fn spawn(&self, world: &World, instance_id: TremorUrl) -> Result<Self::SpawnResult> {
        world
            .spawn_connector(self.clone(), instance_id.clone())
            .await
    }

    /// wire up pipelines to this connector
    /// pipelines to connect need to be findable in the registry for this to work
    ///
    /// snot: badger
    async fn link(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        info!("Linking connector {}...", id);
        let timeout = Duration::from_secs(2);
        if let Some(connector) = system.reg.find_connector(id).await? {
            let (tx, rx) = bounded(mappings.len());
            let mut msgs = Vec::with_capacity(mappings.len());
            for (port, pipeline) in mappings {
                if let Some(ResourceType::Pipeline) = pipeline.resource_type() {
                    match system.reg.find_pipeline(&pipeline).await {
                        Ok(Some(pipeline_addr)) => {
                            msgs.push(connectors::Msg::Link {
                                port: port.into(),
                                pipelines: vec![(pipeline.clone(), pipeline_addr)],
                                result_tx: tx.clone(),
                            });
                        }
                        Ok(None) => {
                            return Err(ErrorKind::InstanceNotFound(
                                "pipeline".to_string(),
                                pipeline.to_string(),
                            )
                            .into());
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                } else {
                    return Err(format!(
                        "Can only link pipelines to connector {}. Not a pipeline: {}",
                        id, &pipeline
                    )
                    .into());
                }
            }
            // send connect messages
            let mut expect = msgs.len();
            for msg in msgs {
                connector.send(msg).await?;
            }
            // wait for answers with timeout
            while expect > 0 {
                // throw any error
                // TODO: roll back previous linkings from this call in case of error
                rx.recv().timeout(timeout).await???;
                expect -= 1;
            }
            Ok(true)
        } else {
            Err(ErrorKind::InstanceNotFound("connector".to_string(), id.to_string()).into())
        }
    }

    /// disconnect pipelines from this connector
    async fn unlink(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<bool> {
        let timeout = Duration::from_secs(2);
        if let Some(connector) = system.reg.find_connector(id).await? {
            let mut msgs = Vec::with_capacity(mappings.len());
            let (tx, rx) = bounded(mappings.len());
            for (port, pipeline_id) in mappings {
                let msg = connectors::Msg::Unlink {
                    port: port.into(),
                    id: pipeline_id,
                    tx: tx.clone(),
                };
                msgs.push(msg);
            }
            let mut expect = msgs.len();
            for msg in msgs {
                connector.send(msg).await?;
            }
            // wait for answers with timeout
            let mut now_empty = false;
            while expect > 0 {
                // throw any error
                now_empty |= async_std::future::timeout(timeout, rx.recv()).await???;
                expect -= 1;
            }
            Ok(now_empty)
        } else {
            Err(ErrorKind::InstanceNotFound("connector".to_string(), id.to_string()).into())
        }
    }
}

impl Binding {
    const LINKING_ERROR: &'static str = "links require the form of onramp -> pipeline or pipeline -> offramp or pipeline -> pipeline or pipeline -> onramp or offramp -> pipeline";
}

#[async_trait]
impl Artefact for Binding {
    type SpawnResult = binding::Addr;
    type LinkResult = Self;
    type LinkLHS = String;
    type LinkRHS = String;

    fn resource_type() -> url::ResourceType {
        url::ResourceType::Binding
    }

    async fn spawn(&self, world: &World, id: TremorUrl) -> Result<Self::SpawnResult> {
        world.spawn_binding(self.clone(), id).await
    }

    /// apply mapping to this binding - the result is a binding with the mappings applied
    #[allow(clippy::too_many_lines)]
    async fn link(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        use ResourceType::{Connector, Pipeline};
        let mut pipelines: Vec<(TremorUrl, TremorUrl)> = Vec::new(); // pipeline -> {onramp, offramp, pipeline, connector}
        let mut connectors: Vec<(TremorUrl, TremorUrl)> = Vec::new(); // connector -> pipeline

        let mut res = self.clone();
        res.binding.links.clear();
        for (src, dsts) in self.binding.links.clone() {
            // TODO: It should be validated ahead of time that every mapping has an instance!
            // * is a port
            // *  is a combination of on and offramp
            if let Some(inst) = src.instance() {
                let mut instance = String::new();
                for (map_name, map_replace) in &mappings {
                    instance = inst.replace(&format!("%7B{}%7D", map_name), map_replace);
                }
                let mut from = src.clone();
                from.set_instance(&instance);
                let mut tos: Vec<TremorUrl> = Vec::new();
                for dst in dsts {
                    // TODO: we should be able to replace any part of the tremor url with mapping values, not just the instance
                    // TODO: It should be validated ahead of time that every mapping has an instance!
                    if let Some(inst) = dst.instance() {
                        let mut instance = String::new();

                        // This is because it is an URL and we have to use escape codes
                        for (map_name, map_replace) in &mappings {
                            instance = inst.replace(&format!("%7B{}%7D", map_name), map_replace);
                        }
                        let mut to = dst.clone();
                        to.set_instance(&instance);
                        tos.push(to.clone());
                        match (from.resource_type(), to.resource_type()) {
                            (Some(Pipeline), Some(Pipeline | Connector)) => {
                                pipelines.push((from.clone(), to));
                            }

                            // handling connectors as source
                            (Some(Connector), Some(Pipeline)) => {
                                connectors.push((from.clone(), to));
                            }
                            (_, _) => return Err(Self::LINKING_ERROR.into()),
                        };
                    }
                }
                res.binding.links.insert(from, tos);
            }
        }

        for (from_pipeline, to) in &pipelines {
            info!("Binding {} to {}", from_pipeline, to);
            match to.resource_type() {
                Some(Pipeline) => system.ensure_pipeline(to).await?,
                Some(Connector) => system.ensure_connector(to).await?,
                _ => (),
            };
            system.ensure_pipeline(from_pipeline).await?;
            system
                .link_pipeline(
                    from_pipeline,
                    vec![(
                        from_pipeline.instance_port_required()?.to_string(),
                        to.clone(),
                    )]
                    .into_iter()
                    .collect(),
                )
                .await?;
            match to.resource_type() {
                Some(Pipeline) => {
                    // notify the pipeline we connect to that a pipeline has been connected to its 'in' port
                    warn!("Linking pipelines is highly experimental! You are on your own, watch your steps!");
                    // we do the reverse linking from within the pipeline
                }
                Some(Connector) => {
                    system
                        .link_connector(
                            to,
                            vec![(
                                to.instance_port_required()?.to_string(),
                                from_pipeline.clone(),
                            )]
                            .into_iter()
                            .collect(),
                        )
                        .await?;
                }
                _ => (),
            }
        }

        // link source connectors
        for (from_connector, to_pipeline) in &connectors {
            system.ensure_pipeline(to_pipeline).await?;
            system.ensure_connector(from_connector).await?;

            system
                .link_connector(
                    from_connector,
                    vec![(
                        from_connector.instance_port_required()?.to_string(),
                        to_pipeline.clone(),
                    )]
                    .into_iter()
                    .collect(),
                )
                .await?;
        }
        info!("[Binding::{}] Binding successfully linked.", id);

        res.mapping = Some(vec![(id.clone(), mappings)].into_iter().collect());
        Ok(res)
    }

    async fn unlink(
        &self,
        system: &World,
        _: &TremorUrl,
        _: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<bool> {
        // here we assume quiescence is done if the DRAIN mechanism was executed on all connectors
        info!("Unlinking Binding {}", self.binding.id);

        // keep track of already handled pipelines, so we don't unlink twice and run into errors
        let mut unlinked = HashSet::with_capacity(self.binding.links.len());
        for (from, tos) in &self.binding.links {
            let mut from_instance = from.clone();
            from_instance.trim_to_instance();

            if let Some(ResourceType::Pipeline) = from.resource_type() {
                if !unlinked.contains(&from_instance) {
                    for to in tos {
                        let mut mappings = HashMap::new();
                        mappings.insert(from.instance_port_required()?.to_string(), to.clone());
                        system.unlink_pipeline(from, mappings).await?;
                        if to.resource_type() == Some(ResourceType::Connector) {
                            let mut mappings = HashMap::new();
                            mappings.insert(to.instance_port_required()?.to_string(), from.clone());
                            system.unlink_connector(to, mappings).await?;
                        }
                    }
                    unlinked.insert(from_instance);
                }
            }
        }

        info!("Binding {} unlinked.", self.binding.id);
        Ok(true)
    }
}
