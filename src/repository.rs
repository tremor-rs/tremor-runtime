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

// The repository workflow is tested through EQC as well as the cli tests,
// neither of them generate coverage :(
#![cfg(not(tarpaulin_include))]

mod artefact;

use crate::{
    errors::{Kind as ErrorKind, Result},
    QSIZE,
};
use async_std::channel::{bounded, Sender};
use async_std::task;
use hashbrown::{hash_map::Entry, HashMap};
use std::default::Default;
use std::fmt;
use std::sync::atomic::Ordering;
use tremor_common::url::TremorUrl;

/// A binding artefact
pub use artefact::Binding as BindingArtefact;
pub(crate) use artefact::ConnectorArtefact;
/// A pipeline artefact
pub use artefact::Pipeline as PipelineArtefact;
pub(crate) use artefact::{Artefact, Id as ArtefactId};

/// Wrapper around a repository
#[derive(Serialize, Clone, Debug)]
pub struct RepoWrapper<A: Artefact> {
    /// The artefact
    pub artefact: A,
    /// Instances of this artefact
    pub instances: Vec<TremorUrl>,
    /// If this is a protected system artefact
    pub system: bool,
}

/// Repository for artefacts
#[derive(Default, Debug)]
pub(crate) struct Repository<A: Artefact> {
    map: HashMap<ArtefactId, RepoWrapper<A>>,
}

impl<A: Artefact> Repository<A> {
    // Retrives the wraped artefacts
    fn values(&self) -> Vec<A> {
        self.map
            .values()
            .filter_map(|a| {
                if a.system {
                    None
                } else {
                    Some(a.artefact.clone())
                }
            })
            .collect()
    }
    /// New repository
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
    /// Retreives the artifact Id's
    fn keys(&self) -> Vec<ArtefactId> {
        self.map.keys().cloned().collect()
    }
    /// Finds an artefact by ID
    pub fn find(&self, mut id: ArtefactId) -> Option<&RepoWrapper<A>> {
        id.trim_to_artefact();
        self.map.get(&id)
    }

    /// Publishes an artefact
    pub fn publish(&mut self, mut id: ArtefactId, system: bool, artefact: A) -> Result<&A> {
        id.trim_to_artefact();
        match self.map.entry(id.clone()) {
            Entry::Occupied(_) => Err(ErrorKind::PublishFailedAlreadyExists(id.to_string()).into()),
            Entry::Vacant(e) => Ok(&e
                .insert(RepoWrapper {
                    instances: Vec::new(),
                    artefact,
                    system,
                })
                .artefact),
        }
    }
    /// Unpublishes an artefact
    pub fn unpublish(&mut self, mut id: ArtefactId) -> Result<A> {
        id.trim_to_artefact();
        match self.map.entry(id.clone()) {
            Entry::Vacant(_) => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
            Entry::Occupied(e) => {
                let wrapper = e.get();
                if wrapper.system {
                    Err(ErrorKind::UnpublishFailedSystemArtefact(id.to_string()).into())
                } else if wrapper.instances.is_empty() {
                    let (_, w) = e.remove_entry();
                    Ok(w.artefact)
                } else {
                    Err(ErrorKind::UnpublishFailedNonZeroInstances(id.to_string()).into())
                }
            }
        }
    }

    /// Binds an artefact to a given servant
    pub fn bind(&mut self, mut id: ArtefactId, mut sid: TremorUrl) -> Result<&A> {
        id.trim_to_artefact();
        sid.trim_to_instance();
        match self.map.get_mut(&id) {
            Some(w) => {
                w.instances.push(sid);
                Ok(&w.artefact)
            }
            None => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
        }
    }
    /// Unbinds an artefact with a given servant
    pub fn unbind(&mut self, mut id: ArtefactId, mut sid: TremorUrl) -> Result<&A> {
        id.trim_to_artefact();
        sid.trim_to_instance();
        match self.map.get_mut(&id) {
            Some(w) => {
                w.instances.retain(|x| x != &sid);
                Ok(&w.artefact)
            }
            None => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
        }
    }
}

pub(crate) enum Msg<A: Artefact> {
    ListArtefacts(Sender<Vec<ArtefactId>>),
    SerializeArtefacts(Sender<Vec<A>>),
    FindArtefact(Sender<Result<Option<RepoWrapper<A>>>>, ArtefactId),
    PublishArtefact(Sender<Result<A>>, ArtefactId, bool, A),
    UnpublishArtefact(Sender<Result<A>>, ArtefactId),
    RegisterInstance(Sender<Result<A>>, ArtefactId, TremorUrl),
    UnregisterInstance(Sender<Result<A>>, ArtefactId, TremorUrl),
}
impl<A: Artefact + Send + Sync + 'static> Repository<A> {
    fn start(mut self) -> Sender<Msg<A>> {
        let (tx, rx) = bounded(QSIZE.load(Ordering::Relaxed));

        task::spawn::<_, Result<()>>(async move {
            while let Ok(msg) = rx.recv().await {
                match msg {
                    Msg::ListArtefacts(r) => r.send(self.keys()).await?,
                    Msg::SerializeArtefacts(r) => r.send(self.values()).await?,
                    Msg::FindArtefact(r, id) => {
                        r.send(A::artefact_id(&id).map(|id| self.find(id).cloned()))
                            .await?;
                    }
                    Msg::PublishArtefact(r, id, sys, a) => {
                        r.send(
                            A::artefact_id(&id).and_then(|id| {
                                self.publish(id, sys, a).map(std::clone::Clone::clone)
                            }),
                        )
                        .await?;
                    }
                    Msg::UnpublishArtefact(r, id) => {
                        r.send(A::artefact_id(&id).and_then(|id| self.unpublish(id)))
                            .await?;
                    }
                    Msg::RegisterInstance(r, a_id, s_id) => {
                        r.send(
                            A::artefact_id(&a_id)
                                .and_then(|aid| Ok((aid, A::instance_id(&s_id)?)))
                                .and_then(|(aid, sid)| {
                                    self.bind(aid, sid).map(std::clone::Clone::clone)
                                }),
                        )
                        .await?;
                    }
                    Msg::UnregisterInstance(r, a_id, s_id) => {
                        r.send(
                            A::artefact_id(&a_id)
                                .and_then(|a_id| Ok((a_id, A::instance_id(&s_id)?)))
                                .and_then(|(a_id, s_id)| {
                                    self.unbind(a_id, s_id).map(std::clone::Clone::clone)
                                }),
                        )
                        .await?;
                    }
                }
            }
            Ok(())
        });
        tx
    }
}

/// Repositories
#[derive(Clone)]
pub struct Repositories {
    pipeline: Sender<Msg<PipelineArtefact>>,
    connector: Sender<Msg<ConnectorArtefact>>,
    binding: Sender<Msg<BindingArtefact>>,
}

#[cfg(not(tarpaulin_include))]
impl fmt::Debug for Repositories {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Repositories {{ ... }}")
    }
}

#[cfg(not(tarpaulin_include))]
impl Default for Repositories {
    fn default() -> Self {
        Self::new()
    }
}

impl Repositories {
    /// Creates an empty repository
    #[must_use]
    pub fn new() -> Self {
        Self {
            pipeline: Repository::new().start(),
            connector: Repository::new().start(),
            binding: Repository::new().start(),
        }
    }

    /// List the pipelines
    ///
    /// # Errors
    ///  * if we can't list the pipelines
    pub async fn list_pipelines(&self) -> Result<Vec<ArtefactId>> {
        let (tx, rx) = bounded(1);
        self.pipeline.send(Msg::ListArtefacts(tx)).await?;
        Ok(rx.recv().await?)
    }

    /// Serialises the pipelines
    ///
    /// # Errors
    ///  * if we can't serialize a pipeline
    pub async fn serialize_pipelines(&self) -> Result<Vec<PipelineArtefact>> {
        let (tx, rx) = bounded(1);
        self.pipeline.send(Msg::SerializeArtefacts(tx)).await?;
        Ok(rx.recv().await?)
    }

    /// Find a pipeline
    ///
    /// # Errors
    ///  * if we can't find a pipeline
    pub async fn find_pipeline(
        &self,
        id: &TremorUrl,
    ) -> Result<Option<RepoWrapper<PipelineArtefact>>> {
        let (tx, rx) = bounded(1);
        self.pipeline
            .send(Msg::FindArtefact(tx, id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Publish a pipeline artefact / config
    ///
    /// # Errors
    ///  * if we can't publish a pipeline artefact
    pub async fn publish_pipeline(
        &self,
        id: &TremorUrl,
        system: bool,
        artefact: PipelineArtefact,
    ) -> Result<PipelineArtefact> {
        let (tx, rx) = bounded(1);
        self.pipeline
            .send(Msg::PublishArtefact(tx, id.clone(), system, artefact))
            .await?;
        rx.recv().await?
    }

    /// Unpublish a pipeline
    ///
    /// # Errors
    ///  * if we can't unpublish a pipeline
    pub async fn unpublish_pipeline(&self, id: &TremorUrl) -> Result<PipelineArtefact> {
        let (tx, rx) = bounded(1);
        self.pipeline
            .send(Msg::UnpublishArtefact(tx, id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Register a pipeline instance
    ///
    /// # Errors
    ///  * if we can't register a pipeline instance
    pub async fn register_pipeline_instance(&self, id: &TremorUrl) -> Result<PipelineArtefact> {
        let (tx, rx) = bounded(1);
        self.pipeline
            .send(Msg::RegisterInstance(tx, id.clone(), id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Unregisters a pipeline instance
    ///
    /// # Errors
    ///  * if we can't unregister a pipeline instance
    pub async fn unregister_pipeline_instance(&self, id: &TremorUrl) -> Result<PipelineArtefact> {
        let (tx, rx) = bounded(1);
        self.pipeline
            .send(Msg::UnregisterInstance(tx, id.clone(), id.clone()))
            .await?;
        rx.recv().await?
    }

    /// List connectors
    ///
    /// # Errors
    ///  * if we can't list connectors
    pub async fn list_connectors(&self) -> Result<Vec<ArtefactId>> {
        let (tx, rx) = bounded(1);
        self.connector.send(Msg::ListArtefacts(tx)).await?;
        Ok(rx.recv().await?)
    }

    /// Serialises connectors
    ///
    /// # Errors
    ///  * if we cna't serialize a connector
    pub async fn serialize_connectors(&self) -> Result<Vec<ConnectorArtefact>> {
        let (tx, rx) = bounded(1);
        self.connector.send(Msg::SerializeArtefacts(tx)).await?;
        Ok(rx.recv().await?)
    }

    /// Find a connector
    ///
    /// # Errors
    ///  * if we can't find a connector
    pub async fn find_connector(
        &self,
        id: &TremorUrl,
    ) -> Result<Option<RepoWrapper<ConnectorArtefact>>> {
        let (tx, rx) = bounded(1);
        self.connector
            .send(Msg::FindArtefact(tx, id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Publishes a connector artefact
    ///
    /// # Errors
    ///  * if we can't publish a connector artefact
    pub async fn publish_connector(
        &self,
        id: &TremorUrl,
        system: bool,
        artefact: ConnectorArtefact,
    ) -> Result<ConnectorArtefact> {
        let (tx, rx) = bounded(1);
        self.connector
            .send(Msg::PublishArtefact(tx, id.clone(), system, artefact))
            .await?;
        rx.recv().await?
    }

    /// Unpublishes a connector artefact
    ///
    /// # Errors
    ///  * if we can't unpublish a connector artefact
    pub async fn unpublish_connector(&self, id: &TremorUrl) -> Result<ConnectorArtefact> {
        let (tx, rx) = bounded(1);
        self.connector
            .send(Msg::UnpublishArtefact(tx, id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Registers a connector instance
    ///
    /// # Errors
    ///  * if we can't register a connector instance
    pub async fn register_connector_instance(&self, id: &TremorUrl) -> Result<ConnectorArtefact> {
        let (tx, rx) = bounded(1);
        self.connector
            .send(Msg::RegisterInstance(tx, id.clone(), id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Unregisters a connector instance
    ///
    /// # Errors
    ///  * if we can't unregister the connector instance
    pub async fn unregister_connector_instance(&self, id: &TremorUrl) -> Result<ConnectorArtefact> {
        let (tx, rx) = bounded(1);
        self.connector
            .send(Msg::UnregisterInstance(tx, id.clone(), id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Lists bindings
    ///
    /// # Errors
    ///  * if we can't list all bindings
    pub async fn list_bindings(&self) -> Result<Vec<ArtefactId>> {
        let (tx, rx) = bounded(1);
        self.binding.send(Msg::ListArtefacts(tx)).await?;
        Ok(rx.recv().await?)
    }

    /// Serialises bindings
    ///
    /// # Errors
    ///  * if we can't serialize the binding
    pub async fn serialize_bindings(&self) -> Result<Vec<BindingArtefact>> {
        let (tx, rx) = bounded(1);
        self.binding.send(Msg::SerializeArtefacts(tx)).await?;
        Ok(rx.recv().await?)
    }

    /// Find a binding
    ///
    /// # Errors
    ///  * if finding the binding failed
    pub async fn find_binding(
        &self,
        id: &TremorUrl,
    ) -> Result<Option<RepoWrapper<BindingArtefact>>> {
        let (tx, rx) = bounded(1);
        self.binding.send(Msg::FindArtefact(tx, id.clone())).await?;
        rx.recv().await?
    }

    /// Publish a binding
    ///
    /// # Errors
    ///  * if we can't publish the binding
    pub async fn publish_binding(
        &self,
        id: &TremorUrl,
        system: bool,
        artefact: BindingArtefact,
    ) -> Result<BindingArtefact> {
        let (tx, rx) = bounded(1);
        self.binding
            .send(Msg::PublishArtefact(tx, id.clone(), system, artefact))
            .await?;
        rx.recv().await?
    }

    /// Unpublishes a binding
    ///
    /// # Errors
    ///  * if we can't unpublish the binding
    pub async fn unpublish_binding(&self, id: &TremorUrl) -> Result<BindingArtefact> {
        let (tx, rx) = bounded(1);
        self.binding
            .send(Msg::UnpublishArtefact(tx, id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Register the instance identified by `id` for its artefact entry in the repo.
    ///
    /// # Errors
    ///  * if we can't bind the binding
    pub async fn register_binding_instance(&self, id: &TremorUrl) -> Result<BindingArtefact> {
        let (tx, rx) = bounded(1);
        self.binding
            .send(Msg::RegisterInstance(tx, id.clone(), id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Unregisters a binding instance
    ///
    /// # Errors
    ///  * if we can't unrtegister the binding instance
    pub async fn unregister_binding_instance(&self, id: &TremorUrl) -> Result<BindingArtefact> {
        let (tx, rx) = bounded(1);
        self.binding
            .send(Msg::UnregisterInstance(tx, id.clone(), id.clone()))
            .await?;
        rx.recv().await?
    }
}
