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

mod artefact;

use crate::errors::*;
use crate::url::TremorURL;
use async_std::sync::{self, channel};
use async_std::task;
use hashbrown::{hash_map::Entry, HashMap};
use std::default::Default;
use std::fmt;

/// A Servant ID
pub use crate::registry::ServantId;
/// A binding artefact
pub use artefact::Binding as BindingArtefact;
pub(crate) use artefact::OfframpArtefact;
pub(crate) use artefact::OnrampArtefact;
/// A pipeline artefact
pub use artefact::Pipeline as PipelineArtefact;
pub(crate) use artefact::{Artefact, Id as ArtefactId};

/// Wrapper around a repository
#[derive(Serialize, Clone, Debug)]
pub struct RepoWrapper<A: Artefact> {
    /// The artefact
    pub artefact: A,
    /// Instances of this artefact
    pub instances: Vec<ServantId>,
    /// If this is a protected system artefact
    pub system: bool,
}

/// Repository for artefacts
#[derive(Default, Debug)]
pub(crate) struct Repository<A: Artefact> {
    map: HashMap<ArtefactId, RepoWrapper<A>>,
}

impl<A: Artefact> Repository<A> {
    /// Retrives the wraped artefacts
    pub fn values(&self) -> Vec<A> {
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
    pub fn keys(&self) -> Vec<ArtefactId> {
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
            Entry::Vacant(_) => Err(ErrorKind::ArtifactNotFound(id.to_string()).into()),
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
    pub fn bind(&mut self, mut id: ArtefactId, mut sid: ServantId) -> Result<&A> {
        id.trim_to_artefact();
        sid.trim_to_instance();
        match self.map.get_mut(&id) {
            Some(w) => {
                w.instances.push(sid);
                Ok(&w.artefact)
            }
            None => Err(ErrorKind::ArtifactNotFound(id.to_string()).into()),
        }
    }
    /// Unbinds an artefact with a given servant
    pub fn unbind(&mut self, mut id: ArtefactId, mut sid: ServantId) -> Result<&A> {
        id.trim_to_artefact();
        sid.trim_to_instance();
        match self.map.get_mut(&id) {
            Some(w) => {
                w.instances.retain(|x| x != &sid);
                Ok(&w.artefact)
            }
            None => Err(ErrorKind::ArtifactNotFound(id.to_string()).into()),
        }
    }
}

/// This is control plane
#[allow(clippy::large_enum_variant)]
pub(crate) enum Msg<A: Artefact> {
    ListArtefacts(sync::Sender<Vec<ArtefactId>>),
    SerializeArtefacts(sync::Sender<Vec<A>>),
    FindArtefact(sync::Sender<Result<Option<RepoWrapper<A>>>>, ArtefactId),
    PublishArtefact(sync::Sender<Result<A>>, ArtefactId, bool, A),
    UnpublishArtefact(sync::Sender<Result<A>>, ArtefactId),
    RegisterInstance(sync::Sender<Result<A>>, ArtefactId, ServantId),
    UnregisterInstance(sync::Sender<Result<A>>, ArtefactId, ServantId),
}
impl<A: Artefact + Send + Sync + 'static> Repository<A> {
    fn start(mut self) -> sync::Sender<Msg<A>> {
        let (tx, rx) = channel(64);

        task::spawn(async move {
            loop {
                match rx.recv().await {
                    Some(Msg::ListArtefacts(r)) => r.send(self.keys()).await,
                    Some(Msg::SerializeArtefacts(r)) => r.send(self.values()).await,
                    Some(Msg::FindArtefact(r, id)) => {
                        r.send(A::artefact_id(&id).map(|id| self.find(id).cloned()))
                            .await
                    }
                    Some(Msg::PublishArtefact(r, id, sys, a)) => {
                        r.send(
                            A::artefact_id(&id).and_then(|id| {
                                self.publish(id, sys, a).map(std::clone::Clone::clone)
                            }),
                        )
                        .await
                    }
                    Some(Msg::UnpublishArtefact(r, id)) => {
                        r.send(A::artefact_id(&id).and_then(|id| self.unpublish(id)))
                            .await
                    }
                    Some(Msg::RegisterInstance(r, a_id, s_id)) => {
                        r.send(
                            A::artefact_id(&a_id)
                                .and_then(|aid| Ok((aid, A::servant_id(&s_id)?)))
                                .and_then(|(aid, sid)| {
                                    self.bind(aid, sid).map(std::clone::Clone::clone)
                                }),
                        )
                        .await
                    }
                    Some(Msg::UnregisterInstance(r, a_id, s_id)) => {
                        r.send(
                            A::artefact_id(&a_id)
                                .and_then(|a_id| Ok((a_id, A::servant_id(&s_id)?)))
                                .and_then(|(a_id, s_id)| {
                                    self.unbind(a_id, s_id).map(std::clone::Clone::clone)
                                }),
                        )
                        .await
                    }
                    None => info!("Terminating repositry"),
                }
            }
        });
        tx
    }
}

/// Repositories
#[derive(Clone)]
pub struct Repositories {
    pipeline: sync::Sender<Msg<PipelineArtefact>>,
    onramp: sync::Sender<Msg<OnrampArtefact>>,
    offramp: sync::Sender<Msg<OfframpArtefact>>,
    binding: sync::Sender<Msg<BindingArtefact>>,
}

impl fmt::Debug for Repositories {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Repositories {{ ... }}")
    }
}

impl Default for Repositories {
    fn default() -> Self {
        Self::new()
    }
}

impl Repositories {
    /// Creates an empty repository
    pub fn new() -> Self {
        Self {
            pipeline: Repository::new().start(),
            onramp: Repository::new().start(),
            offramp: Repository::new().start(),
            binding: Repository::new().start(),
        }
    }

    /// List the pipelines
    pub async fn list_pipelines(&self) -> Vec<ArtefactId> {
        let (tx, rx) = channel(1);
        self.pipeline.send(Msg::ListArtefacts(tx)).await;
        rx.recv().await.unwrap_or_default()
    }

    /// Serialises the pipelines
    pub async fn serialize_pipelines(&self) -> Vec<PipelineArtefact> {
        let (tx, rx) = channel(1);
        self.pipeline.send(Msg::SerializeArtefacts(tx)).await;
        rx.recv().await.unwrap_or_default()
    }

    /// Find a pipeline
    pub async fn find_pipeline(
        &self,
        id: &TremorURL,
    ) -> Result<Option<RepoWrapper<PipelineArtefact>>> {
        let (tx, rx) = channel(1);
        self.pipeline.send(Msg::FindArtefact(tx, id.clone())).await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// Publish a pipeline
    pub async fn publish_pipeline(
        &self,
        id: &TremorURL,
        system: bool,
        artefact: PipelineArtefact,
    ) -> Result<PipelineArtefact> {
        let (tx, rx) = channel(1);
        self.pipeline
            .send(Msg::PublishArtefact(tx, id.clone(), system, artefact))
            .await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// Unpublish a pipeline
    pub async fn unpublish_pipeline(&self, id: &TremorURL) -> Result<PipelineArtefact> {
        let (tx, rx) = channel(1);
        self.pipeline
            .send(Msg::UnpublishArtefact(tx, id.clone()))
            .await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// Bind a pipeline
    pub async fn bind_pipeline(&self, id: &TremorURL) -> Result<PipelineArtefact> {
        let (tx, rx) = channel(1);
        self.pipeline
            .send(Msg::RegisterInstance(tx, id.clone(), id.clone()))
            .await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// Unbinds a pipeline
    pub async fn unbind_pipeline(&self, id: &TremorURL) -> Result<PipelineArtefact> {
        let (tx, rx) = channel(1);
        self.pipeline
            .send(Msg::UnregisterInstance(tx, id.clone(), id.clone()))
            .await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// List onramps
    pub async fn list_onramps(&self) -> Vec<ArtefactId> {
        let (tx, rx) = channel(1);
        self.onramp.send(Msg::ListArtefacts(tx)).await;
        rx.recv().await.unwrap_or_default()
    }

    /// serializes onramps
    pub async fn serialize_onramps(&self) -> Vec<OnrampArtefact> {
        let (tx, rx) = channel(1);
        self.onramp.send(Msg::SerializeArtefacts(tx)).await;
        rx.recv().await.unwrap_or_default()
    }

    /// find an onramp
    pub async fn find_onramp(&self, id: &TremorURL) -> Result<Option<RepoWrapper<OnrampArtefact>>> {
        let (tx, rx) = channel(1);
        self.onramp.send(Msg::FindArtefact(tx, id.clone())).await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// Publish onramp
    pub async fn publish_onramp(
        &self,
        id: &TremorURL,
        system: bool,
        artefact: OnrampArtefact,
    ) -> Result<OnrampArtefact> {
        let (tx, rx) = channel(1);
        self.onramp
            .send(Msg::PublishArtefact(tx, id.clone(), system, artefact))
            .await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// Unpublish an onramp
    pub async fn unpublish_onramp(&self, id: &TremorURL) -> Result<OnrampArtefact> {
        let (tx, rx) = channel(1);
        self.onramp
            .send(Msg::UnpublishArtefact(tx, id.clone()))
            .await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// Binds an onramp
    pub async fn bind_onramp(&self, id: &TremorURL) -> Result<OnrampArtefact> {
        let (tx, rx) = channel(1);
        self.onramp
            .send(Msg::RegisterInstance(tx, id.clone(), id.clone()))
            .await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// Unbinds an onramp
    pub async fn unbind_onramp(&self, id: &TremorURL) -> Result<OnrampArtefact> {
        let (tx, rx) = channel(1);
        self.onramp
            .send(Msg::UnregisterInstance(tx, id.clone(), id.clone()))
            .await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// List offramps
    pub async fn list_offramps(&self) -> Vec<ArtefactId> {
        let (tx, rx) = channel(1);
        self.offramp.send(Msg::ListArtefacts(tx)).await;
        rx.recv().await.unwrap_or_default()
    }

    /// Serialises offramps
    pub async fn serialize_offramps(&self) -> Vec<OfframpArtefact> {
        let (tx, rx) = channel(1);
        self.offramp.send(Msg::SerializeArtefacts(tx)).await;
        rx.recv().await.unwrap_or_default()
    }

    /// Find an offramp
    pub async fn find_offramp(
        &self,
        id: &TremorURL,
    ) -> Result<Option<RepoWrapper<OfframpArtefact>>> {
        let (tx, rx) = channel(1);
        self.offramp.send(Msg::FindArtefact(tx, id.clone())).await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// Publishes an offramp
    pub async fn publish_offramp(
        &self,
        id: &TremorURL,
        system: bool,
        artefact: OfframpArtefact,
    ) -> Result<OfframpArtefact> {
        let (tx, rx) = channel(1);
        self.offramp
            .send(Msg::PublishArtefact(tx, id.clone(), system, artefact))
            .await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// Unpublishes an offramp
    pub async fn unpublish_offramp(&self, id: &TremorURL) -> Result<OfframpArtefact> {
        let (tx, rx) = channel(1);
        self.offramp
            .send(Msg::UnpublishArtefact(tx, id.clone()))
            .await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// Binds an offramp
    pub async fn bind_offramp(&self, id: &TremorURL) -> Result<OfframpArtefact> {
        let (tx, rx) = channel(1);
        self.offramp
            .send(Msg::RegisterInstance(tx, id.clone(), id.clone()))
            .await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// Unbinds an offramp
    pub async fn unbind_offramp(&self, id: &TremorURL) -> Result<OfframpArtefact> {
        let (tx, rx) = channel(1);
        self.offramp
            .send(Msg::UnregisterInstance(tx, id.clone(), id.clone()))
            .await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// Lists bindings
    pub async fn list_bindings(&self) -> Vec<ArtefactId> {
        let (tx, rx) = channel(1);
        self.binding.send(Msg::ListArtefacts(tx)).await;
        rx.recv().await.unwrap_or_default()
    }

    /// Serialises bindings
    pub async fn serialize_bindings(&self) -> Vec<BindingArtefact> {
        let (tx, rx) = channel(1);
        self.binding.send(Msg::SerializeArtefacts(tx)).await;
        rx.recv().await.unwrap_or_default()
    }

    /// Find a binding
    pub async fn find_binding(
        &self,
        id: &TremorURL,
    ) -> Result<Option<RepoWrapper<BindingArtefact>>> {
        let (tx, rx) = channel(1);
        self.binding.send(Msg::FindArtefact(tx, id.clone())).await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// Publish a binding
    pub async fn publish_binding(
        &self,
        id: &TremorURL,
        system: bool,
        artefact: BindingArtefact,
    ) -> Result<BindingArtefact> {
        let (tx, rx) = channel(1);
        self.binding
            .send(Msg::PublishArtefact(tx, id.clone(), system, artefact))
            .await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// Unpublishes a binding
    pub async fn unpublish_binding(&self, id: &TremorURL) -> Result<BindingArtefact> {
        let (tx, rx) = channel(1);
        self.binding
            .send(Msg::UnpublishArtefact(tx, id.clone()))
            .await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// Binds a binding
    pub async fn bind_binding(&self, id: &TremorURL) -> Result<BindingArtefact> {
        let (tx, rx) = channel(1);
        self.binding
            .send(Msg::RegisterInstance(tx, id.clone(), id.clone()))
            .await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }

    /// Unbinds a binding
    pub async fn unbind_binding(&self, id: &TremorURL) -> Result<BindingArtefact> {
        let (tx, rx) = channel(1);
        self.binding
            .send(Msg::UnregisterInstance(tx, id.clone(), id.clone()))
            .await;
        rx.recv()
            .await
            .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config;
    use crate::url::TremorURL;
    use serde_yaml;

    use crate::incarnate;
    use matches::assert_matches;
    use std::fs::File;
    use std::io::BufReader;

    fn slurp(file: &str) -> config::Config {
        let file = File::open(file).expect("could not open file");
        let buffered_reader = BufReader::new(file);
        serde_yaml::from_reader(buffered_reader).expect("failed to parse config")
    }

    #[test]
    fn test_pipeline_repo_lifecycle() {
        let config = slurp("tests/configs/ut.passthrough.yaml");
        let mut runtime = incarnate(config).expect("failed to incarnate");
        let pipeline = runtime.pipes.pop().expect("failed to find artefact");
        let mut repo: Repository<PipelineArtefact> = Repository::new();
        let id = TremorURL::parse("/pipeline/test").expect("failed to parse id");
        assert!(repo.find(id.clone()).is_none());
        let receipt = repo.publish(id.clone(), false, pipeline.clone().into());
        assert!(receipt.is_ok());
        assert!(repo.find(id.clone()).is_some());
        let receipt = repo.publish(id.clone(), false, pipeline.into());

        assert_matches!(
            receipt.err(),
            Some(Error(ErrorKind::PublishFailedAlreadyExists { .. }, _))
        );
    }
}
