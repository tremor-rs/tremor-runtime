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
use crate::errors::{ErrorKind, Result};
use crate::lifecycle::{ActivationState, ActivatorLifecycleFsm};
use crate::repository::{
    Artefact, ArtefactId, BindingArtefact, OfframpArtefact, OnrampArtefact, PipelineArtefact,
};
use crate::url::TremorURL;
use async_channel::bounded;
use async_std::task;
use hashbrown::HashMap;
use std::default::Default;
use std::fmt;

mod servant;

pub use servant::{
    Binding as BindingServant, Id as ServantId, Offramp as OfframpServant, Onramp as OnrampServant,
    Pipeline as PipelineServant,
};

#[derive(Clone, Debug)]
pub(crate) struct Servant<A>
where
    A: Artefact,
{
    artefact: A,
    artefact_id: ArtefactId,
    id: ServantId,
}

#[derive(Default, Debug)]
pub(crate) struct Registry<A: Artefact> {
    map: HashMap<ServantId, ActivatorLifecycleFsm<A>>,
}

impl<A: Artefact> Registry<A> {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn find(&self, mut id: ServantId) -> Option<&ActivatorLifecycleFsm<A>> {
        id.trim_to_instance();
        self.map.get(&id)
    }

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
            Some(removed) => Ok(removed),
            None => Err(ErrorKind::PublishFailedAlreadyExists(id.to_string()).into()),
        }
    }

    pub fn values(&self) -> Vec<A> {
        self.map.values().map(|v| v.artefact.clone()).collect()
    }
}
pub(crate) enum Msg<A: Artefact> {
    SerializeServants(async_channel::Sender<Vec<A>>),
    FindServant(
        async_channel::Sender<Result<Option<A::SpawnResult>>>,
        ServantId,
    ),
    PublishServant(
        async_channel::Sender<Result<ActivationState>>,
        ServantId,
        ActivatorLifecycleFsm<A>,
    ),
    UnpublishServant(async_channel::Sender<Result<ActivationState>>, ServantId),
    Transition(
        async_channel::Sender<Result<ActivationState>>,
        ServantId,
        ActivationState,
    ),
}

impl<A> Registry<A>
where
    A: Artefact + Send + Sync + 'static,
    A::SpawnResult: Send + Sync + 'static,
{
    fn start(mut self) -> async_channel::Sender<Msg<A>> {
        let (tx, rx) = bounded(64);

        task::spawn::<_, Result<()>>(async move {
            loop {
                match rx.recv().await? {
                    Msg::SerializeServants(r) => r.send(self.values()).await?,
                    Msg::FindServant(r, id) => {
                        r.send(
                            A::servant_id(&id)
                                .map(|id| self.find(id).and_then(|v| v.resolution.clone())),
                        )
                        .await?
                    }
                    Msg::PublishServant(r, id, s) => {
                        r.send(
                            A::servant_id(&id).and_then(|id| self.publish(id, s).map(|p| p.state)),
                        )
                        .await?
                    }

                    Msg::UnpublishServant(r, id) => {
                        r.send(
                            A::servant_id(&id).and_then(|id| self.unpublish(id).map(|p| p.state)),
                        )
                        .await?
                    }
                    Msg::Transition(r, mut id, new_state) => {
                        id.trim_to_instance();
                        let res = match self.find_mut(id) {
                            Some(s) => s.transition(new_state).map(|s| s.state),
                            None => Err("Servant not found".into()),
                        };

                        r.send(res).await?
                    }
                }
            }
        });
        tx
    }
}

/// The tremor registry holding running artifacts
#[derive(Clone)]
pub struct Registries {
    pipeline: async_channel::Sender<Msg<PipelineArtefact>>,
    onramp: async_channel::Sender<Msg<OnrampArtefact>>,
    offramp: async_channel::Sender<Msg<OfframpArtefact>>,
    binding: async_channel::Sender<Msg<BindingArtefact>>,
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
    /// Create a new Registry
    pub fn new() -> Self {
        Self {
            binding: Registry::new().start(),
            pipeline: Registry::new().start(),
            onramp: Registry::new().start(),
            offramp: Registry::new().start(),
        }
    }
    /// serialize the mappings of this registry
    pub async fn serialize_mappings(&self) -> Result<crate::config::MappingMap> {
        let (tx, rx) = bounded(1);
        self.binding.send(Msg::SerializeServants(tx)).await?;
        Ok(rx.recv().await?.into_iter().filter_map(|v| v.mapping).fold(
            HashMap::new(),
            |mut acc, v| {
                acc.extend(v);
                acc
            },
        ))
    }
    /// Finds a pipeline
    pub async fn find_pipeline(
        &self,
        id: &TremorURL,
    ) -> Result<Option<<PipelineArtefact as Artefact>::SpawnResult>> {
        let (tx, rx) = bounded(1);
        self.pipeline.send(Msg::FindServant(tx, id.clone())).await?;
        rx.recv().await?
    }
    /// Publishes a pipeline
    pub async fn publish_pipeline(
        &self,
        id: &TremorURL,
        servant: PipelineServant,
    ) -> Result<ActivationState> {
        let (tx, rx) = bounded(1);
        self.pipeline
            .send(Msg::PublishServant(tx, id.clone(), servant))
            .await?;
        rx.recv().await?
    }

    /// unpublishes a pipeline
    pub async fn unpublish_pipeline(&self, id: &TremorURL) -> Result<ActivationState> {
        let (tx, rx) = bounded(1);
        self.pipeline
            .send(Msg::UnpublishServant(tx, id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Transitions a pipeline
    pub async fn transition_pipeline(
        &self,
        id: &TremorURL,
        new_state: ActivationState,
    ) -> Result<ActivationState> {
        let (tx, rx) = bounded(1);
        self.pipeline
            .send(Msg::Transition(tx, id.clone(), new_state))
            .await?;
        rx.recv().await?
    }
    /// Finds an onramp
    pub async fn find_onramp(
        &self,
        id: &TremorURL,
    ) -> Result<Option<<OnrampArtefact as Artefact>::SpawnResult>> {
        let (tx, rx) = bounded(1);
        self.onramp.send(Msg::FindServant(tx, id.clone())).await?;
        rx.recv().await?
    }
    /// Publishes an onramp
    pub async fn publish_onramp(
        &self,
        id: &TremorURL,
        servant: OnrampServant,
    ) -> Result<ActivationState> {
        let (tx, rx) = bounded(1);
        self.onramp
            .send(Msg::PublishServant(tx, id.clone(), servant))
            .await?;
        rx.recv().await?
    }
    /// Usnpublishes an onramp
    pub async fn unpublish_onramp(&self, id: &TremorURL) -> Result<ActivationState> {
        let (tx, rx) = bounded(1);
        self.onramp
            .send(Msg::UnpublishServant(tx, id.clone()))
            .await?;
        rx.recv().await?
    }

    #[cfg(test)]
    pub async fn transition_onramp(
        &self,
        id: &TremorURL,
        new_state: ActivationState,
    ) -> Result<ActivationState> {
        let (tx, rx) = bounded(1);
        self.onramp
            .send(Msg::Transition(tx, id.clone(), new_state))
            .await?;
        rx.recv().await?
    }

    /// Finds an onramp
    pub async fn find_offramp(
        &self,
        id: &TremorURL,
    ) -> Result<Option<<OfframpArtefact as Artefact>::SpawnResult>> {
        let (tx, rx) = bounded(1);
        self.offramp.send(Msg::FindServant(tx, id.clone())).await?;
        rx.recv().await?
    }

    /// Publishes an offramp
    pub async fn publish_offramp(
        &self,
        id: &TremorURL,
        servant: OfframpServant,
    ) -> Result<ActivationState> {
        let (tx, rx) = bounded(1);
        self.offramp
            .send(Msg::PublishServant(tx, id.clone(), servant))
            .await?;
        rx.recv().await?
    }
    /// Unpublishes an offramp
    pub async fn unpublish_offramp(&self, id: &TremorURL) -> Result<ActivationState> {
        let (tx, rx) = bounded(1);
        self.offramp
            .send(Msg::UnpublishServant(tx, id.clone()))
            .await?;
        rx.recv().await?
    }

    #[cfg(test)]
    pub async fn transition_offramp(
        &self,
        id: &TremorURL,
        new_state: ActivationState,
    ) -> Result<ActivationState> {
        let (tx, rx) = bounded(1);
        self.offramp
            .send(Msg::Transition(tx, id.clone(), new_state))
            .await?;
        rx.recv().await?
    }
    /// Finds a binding
    pub async fn find_binding(
        &self,
        id: &TremorURL,
    ) -> Result<Option<<BindingArtefact as Artefact>::SpawnResult>> {
        let (tx, rx) = bounded(1);
        self.binding.send(Msg::FindServant(tx, id.clone())).await?;
        rx.recv().await?
    }
    /// Publishes a binding
    pub async fn publish_binding(
        &self,
        id: &TremorURL,
        servant: BindingServant,
    ) -> Result<ActivationState> {
        let (tx, rx) = bounded(1);
        self.binding
            .send(Msg::PublishServant(tx, id.clone(), servant))
            .await?;
        rx.recv().await?
    }

    /// Unpublishes a binding
    pub async fn unpublish_binding(&self, id: &TremorURL) -> Result<ActivationState> {
        let (tx, rx) = bounded(1);
        self.binding
            .send(Msg::UnpublishServant(tx, id.clone()))
            .await?;
        rx.recv().await?
    }

    #[cfg(test)]
    pub async fn transition_binding(
        &self,
        id: &TremorURL,
        new_state: ActivationState,
    ) -> Result<ActivationState> {
        let (tx, rx) = bounded(1);
        self.binding
            .send(Msg::Transition(tx, id.clone(), new_state))
            .await?;
        rx.recv().await?
    }
}
