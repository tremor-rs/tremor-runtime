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

/// ┌─────────────────┐
/// │  Configuration  │
/// └─────────────────┘
///          │
///       publish
///          │
///          ▼
/// ┌─────────────────┐
/// │   Repository    │
/// └─────────────────┘
///          │
///        find
///          │
///          ▼
/// ┌─────────────────┐
/// │    Artefact     │
/// └─────────────────┘
///          │
///        bind
///          │
///          ▼
/// ┌─────────────────┐
/// │    Registry     │ (instance registry)
/// └─────────────────┘
use crate::errors::*;
#[cfg(test)]
use crate::lifecycle::Transition as TransitionTrait;
use crate::lifecycle::{ActivationState, ActivatorLifecycleFsm};
use crate::repository::{
    Artefact, ArtefactId, BindingArtefact, OfframpArtefact, OnrampArtefact, PipelineArtefact,
};
use crate::system::Count;
use crate::url::TremorURL;
use actix::prelude::*;
use futures::future::Future;
use hashbrown::HashMap;
use std::default::Default;
use std::fmt;
use std::marker::PhantomData;

mod servant;

pub use servant::BindingServant;
pub use servant::OfframpServant;
pub use servant::OnrampServant;
pub use servant::PipelineServant;
pub use servant::ServantId;

#[derive(Clone, Debug)]
pub struct Servant<A>
where
    A: Artefact,
{
    artefact: A,
    artefact_id: ArtefactId,
    id: ServantId,
}

#[derive(Default, Debug)]
pub struct Registry<A: Artefact> {
    map: HashMap<ServantId, ActivatorLifecycleFsm<A>>,
}

impl<A: Artefact> Registry<A> {
    pub fn new() -> Self {
        Registry {
            map: HashMap::new(),
        }
    }

    pub fn find(&self, mut id: ServantId) -> Option<&ActivatorLifecycleFsm<A>> {
        id.trim_to_instance();
        self.map.get(&id)
    }

    pub fn values(&self) -> Vec<A> {
        self.map.values().map(|a| a.artefact.clone()).collect()
    }

    #[cfg(test)]
    pub fn find_mut(&mut self, mut id: ServantId) -> Option<&mut ActivatorLifecycleFsm<A>> {
        id.trim_to_instance();
        self.map.get_mut(&id)
    }

    pub fn publish(
        &mut self,
        mut id: ServantId,
        servant: ActivatorLifecycleFsm<A>,
    ) -> Result<&ActivatorLifecycleFsm<A>> {
        id.trim_to_instance();
        match self.map.insert(id.clone(), servant) {
            Some(_old) => Err(ErrorKind::UnpublishFailedDoesNotExist(id.to_string()).into()),
            None => Ok(&self.map[&id]),
        }
    }

    pub fn unpublish(&mut self, mut id: ServantId) -> Result<ActivatorLifecycleFsm<A>> {
        id.trim_to_instance();
        match self.map.remove(&id) {
            Some(removed) => Ok(removed.to_owned()),
            None => Err(ErrorKind::PublishFailedAlreadyExists(id.to_string()).into()),
        }
    }

    pub fn count(&self) -> usize {
        self.map.len()
    }
}

impl<A: 'static + Artefact> Actor for Registry<A> {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!("Starting registry");
    }
}

pub struct SerializeServants<A: Artefact> {
    _a: PhantomData<A>,
}

impl<A: Artefact> SerializeServants<A> {
    fn new() -> Self {
        Self {
            _a: std::marker::PhantomData,
        }
    }
}

impl<A: 'static + Artefact> Message for SerializeServants<A> {
    type Result = Result<HashMap<ServantId, A>>;
}

impl<A: 'static + Artefact> Handler<SerializeServants<A>> for Registry<A> {
    type Result = Result<HashMap<ServantId, A>>;
    fn handle(&mut self, _req: SerializeServants<A>, _ctx: &mut Self::Context) -> Self::Result {
        Ok(self
            .map
            .iter()
            .map(|(k, v)| (k.clone(), v.artefact.clone()))
            .collect())
    }
}

struct FindServant<A: Artefact> {
    _a: PhantomData<A>,
    id: ServantId,
}

impl<A: Artefact> FindServant<A> {
    fn new(id: ServantId) -> Self {
        Self {
            id,
            _a: PhantomData,
        }
    }
}

impl<A: 'static + Artefact> Message for FindServant<A> {
    type Result = Option<A::SpawnResult>;
}

impl<A: 'static + Artefact> Handler<FindServant<A>> for Registry<A> {
    type Result = Option<A::SpawnResult>;
    fn handle(&mut self, req: FindServant<A>, _ctx: &mut Self::Context) -> Self::Result {
        let mut id = req.id;
        id.trim_to_instance();
        if let Some(Some(r)) = self.find(id).map(|p| p.resolution.clone()) {
            Some(r)
        } else {
            None
        }
    }
}

struct PublishServant<A: Artefact> {
    id: ServantId,
    servant: ActivatorLifecycleFsm<A>,
}

impl<A: 'static + Artefact> Message for PublishServant<A> {
    type Result = Result<ActivationState>;
}

impl<A: 'static + Artefact> Handler<PublishServant<A>> for Registry<A> {
    type Result = Result<ActivationState>;
    fn handle(&mut self, req: PublishServant<A>, _ctx: &mut Self::Context) -> Self::Result {
        let mut id = req.id;
        id.trim_to_instance();
        self.publish(id, req.servant).map(|p| p.state)
    }
}

impl<A: 'static + Artefact> Handler<Count> for Registry<A> {
    type Result = usize;
    fn handle(&mut self, _req: Count, _ctx: &mut Self::Context) -> Self::Result {
        self.count()
    }
}

struct UnpublishServant {
    id: ServantId,
}

impl Message for UnpublishServant {
    type Result = Result<ActivationState>;
}

impl<A: 'static + Artefact> Handler<UnpublishServant> for Registry<A> {
    type Result = Result<ActivationState>;
    fn handle(&mut self, req: UnpublishServant, _ctx: &mut Self::Context) -> Self::Result {
        let mut id = req.id;
        id.trim_to_instance();
        Ok(self.unpublish(id)?.state)
    }
}

#[cfg(test)]
struct Transition<A: Artefact> {
    _a: PhantomData<A>,
    id: ServantId,
    new_state: ActivationState,
}

#[cfg(test)]
impl<A: Artefact> Transition<A> {
    fn new(id: ServantId, new_state: ActivationState) -> Self {
        Self {
            id,
            new_state,
            _a: PhantomData,
        }
    }
}

#[cfg(test)]
impl<A: 'static + Artefact> Message for Transition<A> {
    type Result = Result<ActivationState>;
}

#[cfg(test)]
impl<A: 'static + Artefact> Handler<Transition<A>> for Registry<A> {
    type Result = Result<ActivationState>;
    fn handle(&mut self, req: Transition<A>, _ctx: &mut Self::Context) -> Self::Result {
        let mut id = req.id;
        id.trim_to_instance();
        match self.find_mut(id) {
            Some(s) => Ok(s.transition(req.new_state)?.state),
            None => Err("Servant not found".into()),
        }
    }
}

#[derive(Clone)]
pub struct Registries {
    pipeline: Addr<Registry<PipelineArtefact>>,
    onramp: Addr<Registry<OnrampArtefact>>,
    offramp: Addr<Registry<OfframpArtefact>>,
    binding: Addr<Registry<BindingArtefact>>,
}
impl fmt::Debug for Registries {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Registries {{ ... }}")
    }
}

impl Default for Registries {
    fn default() -> Self {
        Self::new()
    }
}

impl Registries {
    pub fn new() -> Self {
        Self {
            pipeline: Registry::create(|_ctx| Registry::new()),
            onramp: Registry::create(|_ctx| Registry::new()),
            offramp: Registry::create(|_ctx| Registry::new()),
            binding: Registry::create(|_ctx| Registry::new()),
        }
    }

    pub fn serialize_mappings(&self) -> Result<crate::config::MappingMap> {
        let r = self
            .binding
            .send(SerializeServants::new())
            .wait()??
            .into_iter()
            .filter_map(|(_k, v)| v.mapping)
            .fold(HashMap::new(), |mut acc, v| {
                acc.extend(v);
                acc
            });
        Ok(r)
    }
    pub fn find_pipeline(
        &self,
        id: TremorURL,
    ) -> Result<Option<<PipelineArtefact as Artefact>::SpawnResult>> {
        Ok(self
            .pipeline
            .send(FindServant::new(PipelineArtefact::servant_id(id)?))
            .wait()?)
    }

    pub fn publish_pipeline(
        &self,
        id: TremorURL,
        servant: PipelineServant,
    ) -> Result<ActivationState> {
        self.pipeline
            .send(PublishServant {
                id: PipelineArtefact::servant_id(id)?,
                servant,
            })
            .wait()?
    }

    pub fn unpublish_pipeline(&self, id: TremorURL) -> Result<ActivationState> {
        self.pipeline
            .send(UnpublishServant {
                id: PipelineArtefact::servant_id(id)?,
            })
            .wait()?
    }

    #[cfg(test)]
    pub fn transition_pipeline(
        &self,
        id: TremorURL,
        new_state: ActivationState,
    ) -> Result<ActivationState> {
        self.pipeline
            .send(Transition::new(
                PipelineArtefact::servant_id(id)?,
                new_state,
            ))
            .wait()?
    }

    pub fn find_onramp(
        &self,
        id: TremorURL,
    ) -> Result<Option<<OnrampArtefact as Artefact>::SpawnResult>> {
        Ok(self
            .onramp
            .send(FindServant::new(OnrampArtefact::servant_id(id)?))
            .wait()?)
    }

    pub fn publish_onramp(&self, id: TremorURL, servant: OnrampServant) -> Result<ActivationState> {
        self.onramp
            .send(PublishServant {
                id: OnrampArtefact::servant_id(id)?,
                servant,
            })
            .wait()?
    }

    pub fn unpublish_onramp(&self, id: TremorURL) -> Result<ActivationState> {
        self.onramp
            .send(UnpublishServant {
                id: OnrampArtefact::servant_id(id)?,
            })
            .wait()?
    }

    #[cfg(test)]
    pub fn transition_onramp(
        &self,
        id: TremorURL,
        new_state: ActivationState,
    ) -> Result<ActivationState> {
        self.onramp
            .send(Transition::new(OnrampArtefact::servant_id(id)?, new_state))
            .wait()?
    }

    pub fn find_offramp(
        &self,
        id: TremorURL,
    ) -> Result<Option<<OfframpArtefact as Artefact>::SpawnResult>> {
        Ok(self
            .offramp
            .send(FindServant::new(OfframpArtefact::servant_id(id)?))
            .wait()?)
    }

    pub fn publish_offramp(
        &self,
        id: TremorURL,
        servant: OfframpServant,
    ) -> Result<ActivationState> {
        self.offramp
            .send(PublishServant {
                id: OfframpArtefact::servant_id(id)?,
                servant,
            })
            .wait()?
    }

    pub fn unpublish_offramp(&self, id: TremorURL) -> Result<ActivationState> {
        self.offramp
            .send(UnpublishServant {
                id: OfframpArtefact::servant_id(id)?,
            })
            .wait()?
    }

    #[cfg(test)]
    pub fn transition_offramp(
        &self,
        id: TremorURL,
        new_state: ActivationState,
    ) -> Result<ActivationState> {
        self.offramp
            .send(Transition::new(OfframpArtefact::servant_id(id)?, new_state))
            .wait()?
    }

    pub fn find_binding(
        &self,
        id: TremorURL,
    ) -> Result<Option<<BindingArtefact as Artefact>::SpawnResult>> {
        Ok(self
            .binding
            .send(FindServant::new(BindingArtefact::servant_id(id)?))
            .wait()?)
    }

    pub fn publish_binding(
        &self,
        id: TremorURL,
        servant: BindingServant,
    ) -> Result<ActivationState> {
        self.binding
            .send(PublishServant {
                id: BindingArtefact::servant_id(id)?,
                servant,
            })
            .wait()?
    }

    pub fn unpublish_binding(&self, id: TremorURL) -> Result<ActivationState> {
        self.binding
            .send(UnpublishServant {
                id: BindingArtefact::servant_id(id)?,
            })
            .wait()?
    }

    #[cfg(test)]
    pub fn transition_binding(
        &self,
        id: TremorURL,
        new_state: ActivationState,
    ) -> Result<ActivationState> {
        self.binding
            .send(Transition::new(BindingArtefact::servant_id(id)?, new_state))
            .wait()?
    }
}
