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

mod artefact;

use crate::errors::*;
use crate::system;
use crate::url::TremorURL;
use actix::prelude::*;
use futures::future::Future;
use hashbrown::{hash_map::Entry, HashMap};
use std::default::Default;
use std::fmt;
use std::marker::PhantomData;

pub use crate::registry::ServantId;
pub use artefact::Binding as BindingArtefact;
pub use artefact::OfframpArtefact;
pub use artefact::OnrampArtefact;
pub use artefact::Pipeline as PipelineArtefact;
pub use artefact::{Artefact, Id as ArtefactId};

#[derive(Serialize, Clone, Debug)]
pub struct RepoWrapper<A: Artefact> {
    pub artefact: A,
    pub instances: Vec<ServantId>,
    pub system: bool,
}

#[derive(Default, Debug)]
pub struct Repository<A: Artefact> {
    map: HashMap<ArtefactId, RepoWrapper<A>>,
}

impl<A: Artefact> Repository<A> {
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

    pub fn count(&self) -> usize {
        self.map.len()
    }

    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn keys(&self) -> Vec<ArtefactId> {
        self.map.keys().cloned().collect()
    }

    pub fn find(&self, mut id: ArtefactId) -> Option<&RepoWrapper<A>> {
        id.trim_to_artefact();
        self.map.get(&id)
    }

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

impl<A: 'static + Artefact> Actor for Repository<A> {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!("Pipeline repository");
    }
}

struct ListArtefacts<A: Artefact> {
    _a: PhantomData<A>,
}

impl<A: Artefact> ListArtefacts<A> {
    fn new() -> Self {
        Self {
            _a: std::marker::PhantomData,
        }
    }
}

impl<A: 'static + Artefact> Message for ListArtefacts<A> {
    type Result = Result<Vec<ArtefactId>>;
}

impl<A: 'static + Artefact> Handler<ListArtefacts<A>> for Repository<A> {
    type Result = Result<Vec<ArtefactId>>;
    fn handle(&mut self, _req: ListArtefacts<A>, _ctx: &mut Self::Context) -> Self::Result {
        Ok(self.keys())
    }
}

struct SerializeArtefacts<A: Artefact> {
    _a: PhantomData<A>,
}

impl<A: Artefact> SerializeArtefacts<A> {
    fn new() -> Self {
        Self {
            _a: std::marker::PhantomData,
        }
    }
}

impl<A: 'static + Artefact> Message for SerializeArtefacts<A> {
    type Result = Result<Vec<A>>;
}

impl<A: 'static + Artefact> Handler<SerializeArtefacts<A>> for Repository<A> {
    type Result = Result<Vec<A>>;
    fn handle(&mut self, _req: SerializeArtefacts<A>, _ctx: &mut Self::Context) -> Self::Result {
        Ok(self.values())
    }
}

struct FindArtefact<A: Artefact> {
    _a: PhantomData<A>,
    id: ArtefactId,
}

impl<A: Artefact> FindArtefact<A> {
    fn new(id: ArtefactId) -> Self {
        Self {
            _a: std::marker::PhantomData,
            id,
        }
    }
}

impl<A: 'static + Artefact> Message for FindArtefact<A> {
    type Result = Option<RepoWrapper<A>>;
}

impl<A: 'static + Artefact> Handler<FindArtefact<A>> for Repository<A> {
    type Result = Option<RepoWrapper<A>>;
    fn handle(&mut self, req: FindArtefact<A>, _ctx: &mut Self::Context) -> Self::Result {
        self.find(req.id).cloned()
    }
}

struct PublishArtefact<A: Artefact> {
    id: ArtefactId,
    artefact: A,
    system: bool,
}

impl<A: 'static + Artefact> Message for PublishArtefact<A> {
    type Result = Result<A>;
}

impl<A: 'static + Artefact> Handler<PublishArtefact<A>> for Repository<A> {
    type Result = Result<A>;
    fn handle(&mut self, req: PublishArtefact<A>, _ctx: &mut Self::Context) -> Self::Result {
        self.publish(req.id, req.system, req.artefact)
            .map(std::clone::Clone::clone)
    }
}

struct UnpublishArtefact<A: Artefact> {
    id: ArtefactId,
    _artefact: PhantomData<A>,
}

impl<A: 'static + Artefact> Message for UnpublishArtefact<A> {
    type Result = Result<A>;
}

impl<A: 'static + Artefact> Handler<UnpublishArtefact<A>> for Repository<A> {
    type Result = Result<A>;
    fn handle(&mut self, req: UnpublishArtefact<A>, _ctx: &mut Self::Context) -> Self::Result {
        self.unpublish(req.id)
    }
}

struct RegisterInstance<A: Artefact> {
    _a: std::marker::PhantomData<A>,
    id: ArtefactId,
    servant_id: ServantId,
}

impl<A: 'static + Artefact> Message for RegisterInstance<A> {
    type Result = Result<A>;
}

impl<A: 'static + Artefact> Handler<RegisterInstance<A>> for Repository<A> {
    type Result = Result<A>;
    fn handle(&mut self, req: RegisterInstance<A>, _ctx: &mut Self::Context) -> Self::Result {
        self.bind(req.id, req.servant_id)
            .map(std::clone::Clone::clone)
    }
}

impl<A: Artefact> RegisterInstance<A> {
    fn new(id: ArtefactId, servant_id: ServantId) -> Self {
        Self {
            _a: std::marker::PhantomData,
            id,
            servant_id,
        }
    }
}

struct UnregisterInstance<A: Artefact> {
    _a: std::marker::PhantomData<A>,
    id: ArtefactId,
    servant_id: ServantId,
}

impl<A: 'static + Artefact> Message for UnregisterInstance<A> {
    type Result = Result<A>;
}

impl<A: 'static + Artefact> Handler<UnregisterInstance<A>> for Repository<A> {
    type Result = Result<A>;
    fn handle(&mut self, req: UnregisterInstance<A>, _ctx: &mut Self::Context) -> Self::Result {
        self.unbind(req.id, req.servant_id)
            .map(std::clone::Clone::clone)
    }
}

impl<A: Artefact> UnregisterInstance<A> {
    fn new(id: ArtefactId, servant_id: ServantId) -> Self {
        Self {
            _a: std::marker::PhantomData,
            id,
            servant_id,
        }
    }
}

impl<A: 'static + Artefact> Handler<system::Count> for Repository<A> {
    type Result = usize;
    fn handle(&mut self, _req: system::Count, _ctx: &mut Self::Context) -> Self::Result {
        self.count()
    }
}

#[derive(Clone)]
pub struct Repositories {
    pipeline: Addr<Repository<PipelineArtefact>>,
    onramp: Addr<Repository<OnrampArtefact>>,
    offramp: Addr<Repository<OfframpArtefact>>,
    binding: Addr<Repository<BindingArtefact>>,
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
    pub fn new() -> Self {
        Self {
            pipeline: Repository::create(|_ctx| Repository::new()),
            onramp: Repository::create(|_ctx| Repository::new()),
            offramp: Repository::create(|_ctx| Repository::new()),
            binding: Repository::create(|_ctx| Repository::new()),
        }
    }

    pub fn list_pipelines(&self) -> Result<Vec<ArtefactId>> {
        self.pipeline.send(ListArtefacts::new()).wait()?
    }

    pub fn serialize_pipelines(&self) -> Result<Vec<PipelineArtefact>> {
        self.pipeline.send(SerializeArtefacts::new()).wait()?
    }

    pub fn find_pipeline(&self, id: TremorURL) -> Result<Option<RepoWrapper<PipelineArtefact>>> {
        Ok(self
            .pipeline
            .send(FindArtefact::new(PipelineArtefact::artefact_id(id)?))
            .wait()?)
    }

    pub fn publish_pipeline(
        &self,
        id: TremorURL,
        system: bool,
        artefact: PipelineArtefact,
    ) -> Result<PipelineArtefact> {
        self.pipeline
            .send(PublishArtefact {
                id: PipelineArtefact::artefact_id(id)?,
                artefact,
                system,
            })
            .wait()?
    }

    pub fn unpublish_pipeline(&self, id: TremorURL) -> Result<PipelineArtefact> {
        self.pipeline
            .send(UnpublishArtefact {
                id: PipelineArtefact::artefact_id(id)?,
                _artefact: PhantomData {},
            })
            .wait()?
    }

    pub fn bind_pipeline(&self, id: TremorURL) -> Result<PipelineArtefact> {
        self.pipeline
            .send(RegisterInstance::new(
                PipelineArtefact::artefact_id(id.clone())?,
                PipelineArtefact::servant_id(id)?,
            ))
            .wait()?
    }

    pub fn unbind_pipeline(&self, id: TremorURL) -> Result<PipelineArtefact> {
        self.pipeline
            .send(UnregisterInstance::new(
                PipelineArtefact::artefact_id(id.clone())?,
                PipelineArtefact::servant_id(id)?,
            ))
            .wait()?
    }

    pub fn list_onramps(&self) -> Result<Vec<ArtefactId>> {
        self.onramp.send(ListArtefacts::new()).wait()?
    }

    pub fn serialize_onramps(&self) -> Result<Vec<OnrampArtefact>> {
        self.onramp.send(SerializeArtefacts::new()).wait()?
    }

    pub fn find_onramp(&self, id: TremorURL) -> Result<Option<RepoWrapper<OnrampArtefact>>> {
        Ok(self
            .onramp
            .send(FindArtefact::new(OnrampArtefact::artefact_id(id)?))
            .wait()?)
    }

    pub fn publish_onramp(
        &self,
        id: TremorURL,
        system: bool,
        artefact: OnrampArtefact,
    ) -> Result<OnrampArtefact> {
        self.onramp
            .send(PublishArtefact {
                id: OnrampArtefact::artefact_id(id)?,
                system,
                artefact,
            })
            .wait()?
    }

    pub fn unpublish_onramp(&self, id: TremorURL) -> Result<OnrampArtefact> {
        self.onramp
            .send(UnpublishArtefact {
                id: OnrampArtefact::artefact_id(id)?,
                _artefact: PhantomData {},
            })
            .wait()?
    }

    pub fn bind_onramp(&self, id: TremorURL) -> Result<OnrampArtefact> {
        self.onramp
            .send(RegisterInstance::new(
                OnrampArtefact::artefact_id(id.clone())?,
                OnrampArtefact::servant_id(id)?,
            ))
            .wait()?
    }

    pub fn unbind_onramp(&self, id: TremorURL) -> Result<OnrampArtefact> {
        self.onramp
            .send(UnregisterInstance::new(
                OnrampArtefact::artefact_id(id.clone())?,
                OnrampArtefact::servant_id(id)?,
            ))
            .wait()?
    }

    pub fn list_offramps(&self) -> Result<Vec<ArtefactId>> {
        self.offramp.send(ListArtefacts::new()).wait()?
    }

    pub fn serialize_offramps(&self) -> Result<Vec<OfframpArtefact>> {
        self.offramp.send(SerializeArtefacts::new()).wait()?
    }

    pub fn find_offramp(&self, id: TremorURL) -> Result<Option<RepoWrapper<OfframpArtefact>>> {
        Ok(self
            .offramp
            .send(FindArtefact::new(OfframpArtefact::artefact_id(id)?))
            .wait()?)
    }

    pub fn publish_offramp(
        &self,
        id: TremorURL,
        system: bool,
        artefact: OfframpArtefact,
    ) -> Result<OfframpArtefact> {
        self.offramp
            .send(PublishArtefact {
                id: OfframpArtefact::artefact_id(id)?,
                artefact,
                system,
            })
            .wait()?
    }

    pub fn unpublish_offramp(&self, id: TremorURL) -> Result<OfframpArtefact> {
        self.offramp
            .send(UnpublishArtefact {
                id: OfframpArtefact::artefact_id(id)?,
                _artefact: PhantomData {},
            })
            .wait()?
    }

    pub fn bind_offramp(&self, id: TremorURL) -> Result<OfframpArtefact> {
        self.offramp
            .send(RegisterInstance::new(
                OfframpArtefact::artefact_id(id.clone())?,
                OfframpArtefact::servant_id(id)?,
            ))
            .wait()?
    }

    pub fn unbind_offramp(&self, id: TremorURL) -> Result<OfframpArtefact> {
        self.offramp
            .send(UnregisterInstance::new(
                OfframpArtefact::artefact_id(id.clone())?,
                OfframpArtefact::servant_id(id)?,
            ))
            .wait()?
    }

    pub fn list_bindings(&self) -> Result<Vec<ArtefactId>> {
        self.binding.send(ListArtefacts::new()).wait()?
    }

    pub fn serialize_bindings(&self) -> Result<Vec<BindingArtefact>> {
        self.binding.send(SerializeArtefacts::new()).wait()?
    }

    pub fn find_binding(&self, id: TremorURL) -> Result<Option<RepoWrapper<BindingArtefact>>> {
        Ok(self
            .binding
            .send(FindArtefact::new(BindingArtefact::artefact_id(id)?))
            .wait()?)
    }

    pub fn publish_binding(
        &self,
        id: TremorURL,
        system: bool,
        artefact: BindingArtefact,
    ) -> Result<BindingArtefact> {
        self.binding
            .send(PublishArtefact {
                id: BindingArtefact::artefact_id(id)?,
                system,
                artefact,
            })
            .wait()?
    }

    pub fn unpublish_binding(&self, id: TremorURL) -> Result<BindingArtefact> {
        self.binding
            .send(UnpublishArtefact {
                id: BindingArtefact::artefact_id(id)?,
                _artefact: PhantomData {},
            })
            .wait()?
    }

    pub fn bind_binding(&self, id: TremorURL) -> Result<BindingArtefact> {
        self.binding
            .send(RegisterInstance::new(
                BindingArtefact::artefact_id(id.clone())?,
                BindingArtefact::servant_id(id)?,
            ))
            .wait()?
    }

    pub fn unbind_binding(&self, id: TremorURL) -> Result<BindingArtefact> {
        self.binding
            .send(UnregisterInstance::new(
                BindingArtefact::artefact_id(id.clone())?,
                BindingArtefact::servant_id(id)?,
            ))
            .wait()?
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
