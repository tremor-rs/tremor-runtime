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
use crate::errors::*;
use crate::lifecycle::{ActivationState, ActivatorLifecycleFsm};
use crate::repository::{
    Artefact, ArtefactId, BindingArtefact, OfframpArtefact, OnrampArtefact, PipelineArtefact,
};
use crate::url::TremorURL;
use async_std::sync::{self, channel};
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

    //pub fn count(&self) -> usize {
    //    self.map.len()
    //}
    pub fn values(&self) -> Vec<A> {
        self.map.values().map(|v| v.artefact.clone()).collect()
    }
}
pub(crate) enum Msg<A: Artefact> {
    //Count(sync::Sender<usize>),
    SerializeServants(sync::Sender<Vec<A>>),
    FindServant(sync::Sender<Result<Option<A::SpawnResult>>>, ServantId),
    PublishServant(
        sync::Sender<Result<ActivationState>>,
        ServantId,
        ActivatorLifecycleFsm<A>,
    ),
    UnpublishServant(sync::Sender<Result<ActivationState>>, ServantId),
    Transition(
        sync::Sender<Result<ActivationState>>,
        ServantId,
        ActivationState,
    ),
}

impl<A> Registry<A>
where
    A: Artefact + Send + Sync + 'static,
    A::SpawnResult: Send + Sync + 'static,
{
    fn start(mut self) -> sync::Sender<Msg<A>> {
        let (tx, rx) = channel(64);

        task::spawn(async move {
            loop {
                match rx.recv().await {
                    //Some(Msg::Count(r)) => r.send(self.count()).await,
                    Some(Msg::SerializeServants(r)) => r.send(self.values()).await,
                    Some(Msg::FindServant(r, id)) => {
                        r.send(
                            A::servant_id(&id)
                                .map(|id| self.find(id).and_then(|v| v.resolution.clone())),
                        )
                        .await
                    }
                    Some(Msg::PublishServant(r, id, s)) => {
                        r.send(
                            A::servant_id(&id).and_then(|id| self.publish(id, s).map(|p| p.state)),
                        )
                        .await
                    }

                    Some(Msg::UnpublishServant(r, id)) => {
                        r.send(
                            A::servant_id(&id).and_then(|id| self.unpublish(id).map(|p| p.state)),
                        )
                        .await
                    }
                    Some(Msg::Transition(r, mut id, new_state)) => {
                        id.trim_to_instance();
                        let res = match self.find_mut(id) {
                            Some(s) => s.transition(new_state).map(|s| s.state),
                            None => Err("Servant not found".into()),
                        };

                        r.send(res).await
                    }
                    None => info!("Terminating repositry"),
                }
            }
        });
        tx
    }
}

/// The tremor registry holding running artifacts
#[derive(Clone)]
pub struct Registries {
    pipeline: sync::Sender<Msg<PipelineArtefact>>,
    onramp: sync::Sender<Msg<OnrampArtefact>>,
    offramp: sync::Sender<Msg<OfframpArtefact>>,
    binding: sync::Sender<Msg<BindingArtefact>>,
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
    pub fn serialize_mappings(&self) -> crate::config::MappingMap {
        task::block_on(async {
            let (tx, rx) = channel(1);
            self.binding.send(Msg::SerializeServants(tx)).await;
            rx.recv()
                .await
                .unwrap_or_default()
                .into_iter()
                .filter_map(|v| v.mapping)
                .fold(HashMap::new(), |mut acc, v| {
                    acc.extend(v);
                    acc
                })
        })
    }
    /// Finds a pipeline
    pub fn find_pipeline(
        &self,
        id: &TremorURL,
    ) -> Result<Option<<PipelineArtefact as Artefact>::SpawnResult>> {
        task::block_on(async {
            let (tx, rx) = channel(1);
            self.pipeline.send(Msg::FindServant(tx, id.clone())).await;
            rx.recv()
                .await
                .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
        })
    }
    /// Publishes a pipeline
    pub fn publish_pipeline(
        &self,
        id: &TremorURL,
        servant: PipelineServant,
    ) -> Result<ActivationState> {
        task::block_on(async {
            let (tx, rx) = channel(1);
            self.pipeline
                .send(Msg::PublishServant(tx, id.clone(), servant))
                .await;
            rx.recv()
                .await
                .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
        })
    }

    /// unpublishes a pipeline
    pub fn unpublish_pipeline(&self, id: &TremorURL) -> Result<ActivationState> {
        task::block_on(async {
            let (tx, rx) = channel(1);
            self.pipeline
                .send(Msg::UnpublishServant(tx, id.clone()))
                .await;
            rx.recv()
                .await
                .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
        })
    }

    /// Transitions a pipeline
    pub fn transition_pipeline(
        &self,
        id: &TremorURL,
        new_state: ActivationState,
    ) -> Result<ActivationState> {
        task::block_on(async {
            let (tx, rx) = channel(1);
            self.pipeline
                .send(Msg::Transition(tx, id.clone(), new_state))
                .await;
            rx.recv()
                .await
                .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
        })
    }
    /// Finds an onramp
    pub fn find_onramp(
        &self,
        id: &TremorURL,
    ) -> Result<Option<<OnrampArtefact as Artefact>::SpawnResult>> {
        task::block_on(async {
            let (tx, rx) = channel(1);
            self.onramp.send(Msg::FindServant(tx, id.clone())).await;
            rx.recv()
                .await
                .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
        })
    }
    /// Publishes an onramp
    pub fn publish_onramp(
        &self,
        id: &TremorURL,
        servant: OnrampServant,
    ) -> Result<ActivationState> {
        task::block_on(async {
            let (tx, rx) = channel(1);
            self.onramp
                .send(Msg::PublishServant(tx, id.clone(), servant))
                .await;
            rx.recv()
                .await
                .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
        })
    }
    /// Usnpublishes an onramp
    pub fn unpublish_onramp(&self, id: &TremorURL) -> Result<ActivationState> {
        task::block_on(async {
            let (tx, rx) = channel(1);
            self.onramp
                .send(Msg::UnpublishServant(tx, id.clone()))
                .await;
            rx.recv()
                .await
                .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
        })
    }

    #[cfg(test)]
    pub fn transition_onramp(
        &self,
        id: &TremorURL,
        new_state: ActivationState,
    ) -> Result<ActivationState> {
        task::block_on(async {
            let (tx, rx) = channel(1);
            self.onramp
                .send(Msg::Transition(tx, id.clone(), new_state))
                .await;
            rx.recv()
                .await
                .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
        })
    }

    /// Finds an onramp
    pub fn find_offramp(
        &self,
        id: &TremorURL,
    ) -> Result<Option<<OfframpArtefact as Artefact>::SpawnResult>> {
        task::block_on(async {
            let (tx, rx) = channel(1);
            self.offramp.send(Msg::FindServant(tx, id.clone())).await;
            rx.recv()
                .await
                .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
        })
    }

    /// Publishes an offramp
    pub fn publish_offramp(
        &self,
        id: &TremorURL,
        servant: OfframpServant,
    ) -> Result<ActivationState> {
        task::block_on(async {
            let (tx, rx) = channel(1);
            self.offramp
                .send(Msg::PublishServant(tx, id.clone(), servant))
                .await;
            rx.recv()
                .await
                .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
        })
    }
    /// Unpublishes an offramp
    pub fn unpublish_offramp(&self, id: &TremorURL) -> Result<ActivationState> {
        task::block_on(async {
            let (tx, rx) = channel(1);
            self.offramp
                .send(Msg::UnpublishServant(tx, id.clone()))
                .await;
            rx.recv()
                .await
                .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
        })
    }

    #[cfg(test)]
    pub fn transition_offramp(
        &self,
        id: &TremorURL,
        new_state: ActivationState,
    ) -> Result<ActivationState> {
        task::block_on(async {
            let (tx, rx) = channel(1);
            self.offramp
                .send(Msg::Transition(tx, id.clone(), new_state))
                .await;
            rx.recv()
                .await
                .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
        })
    }
    /// Finds a binding
    pub fn find_binding(
        &self,
        id: &TremorURL,
    ) -> Result<Option<<BindingArtefact as Artefact>::SpawnResult>> {
        task::block_on(async {
            let (tx, rx) = channel(1);
            self.binding.send(Msg::FindServant(tx, id.clone())).await;
            rx.recv()
                .await
                .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
        })
    }
    /// Publishes a binding
    pub fn publish_binding(
        &self,
        id: &TremorURL,
        servant: BindingServant,
    ) -> Result<ActivationState> {
        task::block_on(async {
            let (tx, rx) = channel(1);
            self.binding
                .send(Msg::PublishServant(tx, id.clone(), servant))
                .await;
            rx.recv()
                .await
                .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
        })
    }

    /// Unpublishes a binding
    pub fn unpublish_binding(&self, id: &TremorURL) -> Result<ActivationState> {
        task::block_on(async {
            let (tx, rx) = channel(1);
            self.binding
                .send(Msg::UnpublishServant(tx, id.clone()))
                .await;
            rx.recv()
                .await
                .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
        })
    }

    #[cfg(test)]
    pub fn transition_binding(
        &self,
        id: &TremorURL,
        new_state: ActivationState,
    ) -> Result<ActivationState> {
        task::block_on(async {
            let (tx, rx) = channel(1);
            self.binding
                .send(Msg::Transition(tx, id.clone(), new_state))
                .await;
            rx.recv()
                .await
                .ok_or_else(|| Error::from(ErrorKind::AsyncRecvError))?
        })
    }
}
