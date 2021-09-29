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

use crate::errors::{Error, ErrorKind, Result};
use crate::metrics::RampReporter;
use crate::onramp;
use crate::pipeline;
use crate::registry::Instance;
use crate::registry::ServantId;
use crate::system::{self, World};
use crate::url::ports::IN;
use crate::url::{ResourceType, TremorUrl};
use crate::{codec, pipeline::ConnectTarget};
use crate::{connectors, offramp, url};
use beef::Cow;
use hashbrown::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use tremor_pipeline::query;
pub(crate) type Id = TremorUrl;
pub(crate) use crate::Connector as ConnectorArtefact;
pub(crate) use crate::OffRamp as OfframpArtefact;
pub(crate) use crate::OnRamp as OnrampArtefact;
use async_std::channel::bounded;
use async_trait::async_trait;

/// A Binding
#[derive(Clone, Debug)]
pub struct Binding {
    /// The binding itself
    pub binding: crate::Binding,
    /// The mappings
    pub mapping: Option<crate::config::MappingMap>,

    /// track spawned instances for better quiescence
    spawned_instances: HashSet<TremorUrl>,
}

impl Binding {
    /// Constructor
    #[must_use]
    pub fn new(binding: crate::Binding, mapping: Option<crate::config::MappingMap>) -> Self {
        Self {
            binding,
            mapping,
            spawned_instances: HashSet::new(),
        }
    }
}

/// A Pipeline
pub type Pipeline = query::Query;

#[async_trait]
pub trait Artefact: Clone {
    //    type Configuration;
    type SpawnResult: Clone + Instance + Send;
    type LinkResult: Clone;
    type LinkLHS: Clone;
    type LinkRHS: Clone;

    /// Move from Repository to Registry
    async fn spawn(&self, system: &World, servant_id: ServantId) -> Result<Self::SpawnResult>;
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
    fn servant_id(id: &TremorUrl) -> Result<ServantId> {
        let mut id = id.clone();
        id.trim_to_instance();
        let rt = Self::resource_type();
        match (id.resource_type(), id.instance()) {
            (Some(id_rt), Some(_id)) if id_rt == rt => Ok(id),
            _ => Err(ErrorKind::InvalidTremorUrl(
                format!("Url does not contain a {} servant id", rt),
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

    async fn spawn(&self, world: &World, servant_id: ServantId) -> Result<Self::SpawnResult> {
        world.instantiate_pipeline(self.clone(), servant_id).await
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
                    Some(ResourceType::Offramp) => {
                        if let Some(offramp) = system.reg.find_offramp(&to).await? {
                            ConnectTarget::Offramp(offramp)
                        } else {
                            return Err(ErrorKind::InstanceNotFound(
                                "offramp".to_string(),
                                to.to_string(),
                            )
                            .into());
                        }
                    }
                    Some(ResourceType::Pipeline) => {
                        // TODO: connect both ways?
                        if let Some(p) = system.reg.find_pipeline(&to).await? {
                            ConnectTarget::Pipeline(Box::new(p))
                        } else {
                            return Err(ErrorKind::InstanceNotFound(
                                "pipeline".to_string(),
                                to.to_string(),
                            )
                            .into());
                        }
                    }
                    Some(ResourceType::Onramp) => {
                        if let Some(onramp) = system.reg.find_onramp(&to).await? {
                            ConnectTarget::Onramp(onramp)
                        } else {
                            return Err(ErrorKind::InstanceNotFound(
                                "onramp".to_string(),
                                to.to_string(),
                            )
                            .into());
                        }
                    }
                    Some(ResourceType::Connector) => {
                        if let Some(connector) = system.reg.find_connector(&to).await? {
                            ConnectTarget::Connector(connector)
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
                    Some(
                        ResourceType::Offramp
                        | ResourceType::Pipeline
                        | ResourceType::Onramp
                        | ResourceType::Connector,
                    ) => {
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
impl Artefact for OfframpArtefact {
    type SpawnResult = offramp::Addr;
    type LinkResult = bool;
    type LinkLHS = TremorUrl;
    type LinkRHS = TremorUrl;

    fn resource_type() -> url::ResourceType {
        url::ResourceType::Offramp
    }

    async fn spawn(&self, world: &World, servant_id: ServantId) -> Result<Self::SpawnResult> {
        // TODO: make duration configurable
        let timeout = Duration::from_secs(2);
        let offramp = world
            .instantiate_offramp(self.binding_type.clone(), self.config.clone(), timeout)
            .await?;
        // lookup codecs already here
        // this will bail out early if something is mistyped or so
        let codec = if let Some(codec) = &self.codec {
            codec::lookup(codec)?
        } else {
            codec::lookup(offramp.default_codec())?
        };
        let mut resolved_codec_map = codec::builtin_codec_map();
        // override the builtin map
        if let Some(codec_map) = &self.codec_map {
            for (k, v) in codec_map {
                resolved_codec_map.insert(k.to_string(), codec::lookup(v.as_str())?);
            }
        }

        let preprocessors = if let Some(preprocessors) = &self.preprocessors {
            preprocessors.clone()
        } else {
            vec![]
        };

        let postprocessors = if let Some(postprocessors) = &self.postprocessors {
            postprocessors.clone()
        } else {
            vec![]
        };
        let metrics_reporter = RampReporter::new(servant_id.clone(), self.metrics_interval_s);

        let (tx, rx) = bounded(1);

        // start the offramp
        // TODO: postpone to later
        world
            .system
            .send(system::ManagerMsg::Offramp(offramp::ManagerMsg::Create(
                tx,
                Box::new(offramp::Create {
                    id: servant_id,
                    codec,
                    codec_map: resolved_codec_map,
                    offramp,
                    preprocessors,
                    postprocessors,
                    metrics_reporter,
                    is_linked: self.is_linked,
                }),
            )))
            .await?;
        rx.recv().await?
    }

    async fn link(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        info!("Linking offramp {} ..", id);
        if let Some(offramp) = system.reg.find_offramp(id).await? {
            for (pipeline_id, this) in mappings {
                let port = Cow::from(this.instance_port_required()?.to_string());
                info!("Linking offramp {} to {}", this, pipeline_id);
                if let Some(pipeline) = system.reg.find_pipeline(&pipeline_id).await? {
                    offramp
                        .send(offramp::Msg::Connect {
                            port,
                            id: pipeline_id,
                            addr: Box::new(pipeline),
                        })
                        .await?;
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
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        info!("Unlinking offramp {} ..", id);
        if let Some(offramp) = system.reg.find_offramp(id).await? {
            let (tx, rx) = bounded(mappings.len());
            let mut expect_answers = mappings.len();
            for (from, to) in mappings {
                let (this, pipeline_id) = match (from.resource_type(), to.resource_type()) {
                    (Some(ResourceType::Offramp), Some(ResourceType::Pipeline)) => (from, to),
                    (Some(ResourceType::Pipeline), Some(ResourceType::Offramp)) => (to, from),
                    _ => return Err(format!("Invalid mapping for Offramp {}.", id).into()),
                };
                let port = Cow::from(this.instance_port_required()?.to_string());
                offramp
                    .send(offramp::Msg::Disconnect {
                        port,
                        id: pipeline_id,
                        tx: tx.clone(),
                    })
                    .await?;
            }
            let mut empty = false;
            while expect_answers > 0 {
                empty |= rx.recv().await?;
                expect_answers -= 1;
            }
            Ok(empty)
        } else {
            Err(format!("Offramp {} not found for unlinking,", id).into())
        }
    }
}
#[async_trait]
impl Artefact for OnrampArtefact {
    type SpawnResult = onramp::Addr;
    type LinkResult = bool;
    type LinkLHS = String;
    type LinkRHS = TremorUrl;

    fn resource_type() -> url::ResourceType {
        url::ResourceType::Onramp
    }

    async fn spawn(&self, world: &World, servant_id: ServantId) -> Result<Self::SpawnResult> {
        let timeout = Duration::from_secs(2); // TODO: make configurable
        let stream = world
            .instantiate_onramp(
                self.binding_type.clone(),
                servant_id.clone(),
                self.config.clone(),
                timeout,
            )
            .await?;
        let codec = self.codec.as_ref().map_or_else(
            || stream.default_codec().to_string(),
            std::clone::Clone::clone,
        );
        let codec_map = self
            .codec_map
            .clone()
            .unwrap_or_else(|| halfbrown::HashMap::with_capacity(0));
        let preprocessors = if let Some(preprocessors) = &self.preprocessors {
            preprocessors.clone()
        } else {
            vec![]
        };
        let postprocessors = if let Some(postprocessors) = &self.postprocessors {
            postprocessors.clone()
        } else {
            vec![]
        };

        let metrics_reporter = RampReporter::new(servant_id.clone(), self.metrics_interval_s);
        let (tx, rx) = bounded(1);

        world
            .system
            .send(system::ManagerMsg::Onramp(onramp::ManagerMsg::Create(
                tx,
                Box::new(onramp::Create {
                    id: servant_id,
                    preprocessors,
                    postprocessors,
                    codec,
                    codec_map,
                    stream,
                    metrics_reporter,
                    is_linked: self.is_linked,
                    err_required: self.err_required,
                }),
            )))
            .await?;
        rx.recv().await?
    }

    async fn link(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        // check if we have the right onramp
        if let Some(artefact) = id.artefact() {
            if self.id.as_str() != artefact {
                return Err(format!(
                    "Onramp for linking ({}) is not from this artifact: {}.",
                    id, self.id
                )
                .into());
            }
        }
        if let Some(onramp) = system.reg.find_onramp(id).await? {
            let mut msgs = Vec::with_capacity(mappings.len());
            for (from, to) in mappings {
                // TODO: validate that `from` - the port name - is valid (OUT, ERR, METRICS)
                if let Some(ResourceType::Pipeline) = to.resource_type() {
                    if let Some(pipeline) = system.reg.find_pipeline(&to).await? {
                        msgs.push(onramp::Msg::Connect(
                            from.into(),
                            vec![(to.clone(), pipeline)],
                        ));
                    } else {
                        return Err(format!("Pipeline {:?} not found", to).into());
                    }
                } else {
                    return Err("Destination isn't a Pipeline".into());
                }
            }
            for msg in msgs {
                onramp.send(msg).await?;
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
    ) -> Result<bool> {
        info!("Unlinking onramp {} ..", id);
        if let Some(onramp) = system.reg.find_onramp(id).await? {
            let (tx, rx) = bounded(mappings.len());
            let mut expect_answers = mappings.len();
            for (_port, pipeline_id) in mappings {
                onramp
                    .send(onramp::Msg::Disconnect {
                        id: pipeline_id,
                        tx: tx.clone(),
                    })
                    .await?;
            }
            let mut empty = false;
            while expect_answers > 0 {
                empty |= rx.recv().await?;
                expect_answers -= 1;
            }

            info!("Onramp {} unlinked.", id);
            Ok(empty)
        } else {
            Err(format!("Unlinking failed Onramp {} not found ", id).into())
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
    async fn spawn(&self, world: &World, servant_id: ServantId) -> Result<Self::SpawnResult> {
        let create = connectors::Create::new(servant_id.clone(), self.clone());
        let (tx, rx) = bounded(1);
        world
            .system
            .send(system::ManagerMsg::Connector(
                connectors::ManagerMsg::Create {
                    tx,
                    create: Box::new(create),
                },
            ))
            .await?;
        rx.recv().await?
    }

    /// wire up pipelines to this connector
    /// pipelines to connect need to be findable in the registry for this to work
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
                async_std::future::timeout(timeout, rx.recv()).await???;
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
    type SpawnResult = Self;
    type LinkResult = Self;
    type LinkLHS = String;
    type LinkRHS = String;

    fn resource_type() -> url::ResourceType {
        url::ResourceType::Binding
    }

    async fn spawn(&self, _: &World, _: ServantId) -> Result<Self::SpawnResult> {
        // do some basic verification:
        // - left side: IN port doesnt make sense
        // - right side: should have IN port
        for (from, tos) in &self.binding.links {
            let port = from.instance_port_required()?;
            if port.eq_ignore_ascii_case(IN.as_ref()) {
                return Err(format!(
                    "Invalid Binding {}. Cannot link from port {} in {}.",
                    &self.binding.id, port, &from
                )
                .into());
            }
            for to in tos {
                let port = to.instance_port_required()?;
                if !port.eq_ignore_ascii_case(IN.as_ref()) {
                    return Err(format!(
                        "Invalid Binding {}. Cannot link to port {} in {}.",
                        &self.binding.id, port, &to
                    )
                    .into());
                }
            }
        }
        Ok(self.clone())
    }

    /// apply mapping to this binding - the result is a binding with the mappings applied
    #[allow(clippy::too_many_lines)]
    async fn link(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        use ResourceType::{Connector, Offramp, Onramp, Pipeline};
        let mut pipelines: Vec<(TremorUrl, TremorUrl)> = Vec::new(); // pipeline -> {onramp, offramp, pipeline, connector}
        let mut onramps: Vec<(TremorUrl, TremorUrl)> = Vec::new(); // onramp -> pipeline
        let mut linked_offramps: Vec<(TremorUrl, TremorUrl)> = Vec::new(); // linked offramps -> pipeline
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
                            (Some(Onramp), Some(Pipeline)) => {
                                onramps.push((from.clone(), to));
                            }
                            (Some(Pipeline), Some(Offramp | Pipeline | Onramp | Connector)) => {
                                pipelines.push((from.clone(), to));
                            }
                            // for linked offramps
                            // TODO improve this process: this should really be treated as onramps,
                            // or as a separate resource
                            (Some(Offramp), Some(Pipeline)) => {
                                linked_offramps.push((from.clone(), to));
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

        // first link (and thus start) the linked offramps
        for (from, to) in linked_offramps {
            system.ensure_pipeline(&to).await?;
            system.ensure_offramp(&from).await?;
            system
                .link_offramp(&from, vec![(to, from.clone())].into_iter().collect())
                .await?;
        }

        for (from_pipeline, to) in &pipelines {
            info!("Binding {} to {}", from_pipeline, to);
            match to.resource_type() {
                Some(Offramp) => system.ensure_offramp(to).await?,
                Some(Pipeline) => system.ensure_pipeline(to).await?,
                Some(Onramp) => system.ensure_onramp(to).await?,
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
                Some(Offramp) => {
                    system
                        .link_offramp(
                            to,
                            vec![(from_pipeline.clone(), to.clone())]
                                .into_iter()
                                .collect(),
                        )
                        .await?;
                }
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

        for (from, to) in onramps {
            system.ensure_pipeline(&to).await?;
            system.ensure_onramp(&from).await?;
            system
                .link_onramp(
                    &from,
                    vec![(from.instance_port_required()?.to_string(), to)]
                        .into_iter()
                        .collect(),
                )
                .await?;
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

        res.mapping = Some(vec![(id.clone(), mappings)].into_iter().collect());
        Ok(res)
    }

    async fn unlink(
        &self,
        system: &World,
        _: &TremorUrl,
        _: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<bool> {
        // here we assume quiescence is done if the DRAIN mechanism was executed on all connectors as is
        // implemented in the Binding instance logic in `Instance::stop()`.
        info!("Unlinking Binding {}", self.binding.id);

        let links = self.binding.links.clone();

        // collect incoming connections from external instances - and unlink them
        for (from, tos) in links.iter() {
            if !self.spawned_instances.contains(from) {
                // unlink external resource
                match from.resource_type() {
                    Some(ResourceType::Connector) => {
                        let mut mappings = HashMap::new();
                        let port = from.instance_port_required()?.to_string();
                        for to in tos {
                            mappings.insert(port.clone(), to.clone());
                        }
                        system.unlink_connector(from, mappings).await?;
                    }
                    Some(ResourceType::Pipeline) => {
                        let mut mappings = HashMap::new();
                        let port = from.instance_port_required()?.to_string();
                        for to in tos {
                            mappings.insert(port.clone(), to.clone());
                        }
                        system.unlink_pipeline(from, mappings).await?;
                    }
                    Some(ResourceType::Onramp) => {
                        let mut mappings = HashMap::new();
                        let port = from.instance_port_required()?.to_string();
                        for to in tos {
                            mappings.insert(port.clone(), to.clone());
                        }
                        system.unlink_onramp(from, mappings).await?;
                    }
                    Some(ResourceType::Offramp) => {
                        let mut mappings = HashMap::new();
                        for to in tos {
                            mappings.insert(to.clone(), from.clone());
                        }
                        system.unlink_offramp(from, mappings).await?;
                    }
                    Some(_) => return Err(format!("Cannot unlink {}", from).into()),
                    None => {
                        return Err(ErrorKind::InvalidTremorUrl(
                            "Missing resource type".to_string(),
                            from.to_string(),
                        )
                        .into())
                    }
                }
            }
        }

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
                        match to.resource_type() {
                            Some(ResourceType::Offramp) => {
                                let mut mappings = HashMap::new();
                                mappings.insert(to.clone(), from.clone());
                                system.unlink_offramp(to, mappings).await?;
                            }
                            Some(ResourceType::Connector) => {
                                let mut mappings = HashMap::new();
                                mappings
                                    .insert(to.instance_port_required()?.to_string(), from.clone());
                                system.unlink_connector(to, mappings).await?;
                            }
                            Some(ResourceType::Onramp) => {
                                let mut mappings = HashMap::new();
                                mappings
                                    .insert(to.instance_port_required()?.to_string(), from.clone());
                                system.unlink_onramp(to, mappings).await?;
                            }
                            _ => {}
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
