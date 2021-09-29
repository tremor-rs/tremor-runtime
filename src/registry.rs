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
use crate::lifecycle::{InstanceLifecycleFsm, InstanceState};
use crate::repository::{
    Artefact, ArtefactId, BindingArtefact, ConnectorArtefact, OfframpArtefact, OnrampArtefact,
    PipelineArtefact,
};
use crate::url::{ResourceType, TremorUrl};
use crate::QSIZE;
use async_std::channel::{bounded, Sender};
use async_std::future::timeout;
use async_std::task;
use hashbrown::HashMap;
use std::default::Default;
use std::fmt;
use std::sync::atomic::Ordering;
use std::time::Duration;

mod instance;
mod servant;

pub use servant::{
    Binding as BindingServant, Connector as ConnectorServant, Id as ServantId,
    Offramp as OfframpServant, Onramp as OnrampServant, Pipeline as PipelineServant,
};

pub use instance::Instance;

#[derive(Clone, Debug)]
pub(crate) struct Servant<A>
where
    A: Artefact,
{
    artefact: A,
    artefact_id: ArtefactId,
    id: ServantId,
}

#[derive(Debug)]
pub(crate) struct Registry<A: Artefact> {
    resource_type: ResourceType,
    map: HashMap<ServantId, InstanceLifecycleFsm<A>>,
}

impl<A: Artefact> Registry<A> {
    pub fn new(resource_type: ResourceType) -> Self {
        Self {
            resource_type,
            map: HashMap::new(),
        }
    }

    pub fn find(&self, mut id: ServantId) -> Option<&InstanceLifecycleFsm<A>> {
        id.trim_to_instance();
        self.map.get(&id)
    }

    pub fn find_mut(&mut self, mut id: ServantId) -> Option<&mut InstanceLifecycleFsm<A>> {
        id.trim_to_instance();
        self.map.get_mut(&id)
    }

    pub fn publish(
        &mut self,
        mut id: ServantId,
        servant: InstanceLifecycleFsm<A>,
    ) -> Result<&InstanceLifecycleFsm<A>> {
        id.trim_to_instance();
        match self.map.insert(id.clone(), servant) {
            Some(_old) => Err(ErrorKind::PublishFailedAlreadyExists(id.to_string()).into()),
            None => self
                .map
                .get(&id)
                .ok_or_else(|| ErrorKind::PublishFailedAlreadyExists(id.to_string()).into()),
        }
    }

    pub fn unpublish(&mut self, mut id: ServantId) -> Result<InstanceLifecycleFsm<A>> {
        id.trim_to_instance();
        match self.map.remove(&id) {
            Some(removed) => Ok(removed),
            None => Err(ErrorKind::UnpublishFailedDoesNotExist(id.to_string()).into()),
        }
    }

    pub fn values(&self) -> Vec<A> {
        self.map.values().map(|v| v.artefact.clone()).collect()
    }

    /// returns a list of all ids of all instances currently registered
    pub fn servant_ids(&self) -> Vec<ServantId> {
        self.map.values().map(|v| v.id.clone()).collect()
    }
}
pub(crate) enum Msg<A: Artefact> {
    ListServants(Sender<Vec<ServantId>>),
    SerializeServants(Sender<Vec<A>>),
    FindServant(Sender<Result<Option<A::SpawnResult>>>, ServantId),
    PublishServant(
        Sender<Result<InstanceState>>,
        ServantId,
        InstanceLifecycleFsm<A>,
    ),
    UnpublishServant(Sender<Result<InstanceLifecycleFsm<A>>>, ServantId),
    Transition(Sender<Result<InstanceState>>, ServantId, InstanceState),
}

impl<A> Registry<A>
where
    A: Artefact + Send + Sync + 'static,
    A::SpawnResult: Send + Sync + 'static,
{
    fn start(mut self) -> Sender<Msg<A>> {
        let (tx, rx) = bounded(QSIZE.load(Ordering::Relaxed));

        task::spawn::<_, Result<()>>(async move {
            loop {
                match rx.recv().await? {
                    Msg::ListServants(r) => r.send(self.servant_ids()).await?,
                    Msg::SerializeServants(r) => r.send(self.values()).await?,
                    Msg::FindServant(r, id) => {
                        r.send(
                            A::servant_id(&id).map(|id| self.find(id).map(|v| v.instance.clone())),
                        )
                        .await?;
                    }
                    Msg::PublishServant(r, id, s) => {
                        r.send(
                            A::servant_id(&id).and_then(|id| self.publish(id, s).map(|p| p.state)),
                        )
                        .await?;
                    }

                    Msg::UnpublishServant(r, id) => {
                        let x =
                            A::servant_id(&id).and_then(|instance_id| self.unpublish(instance_id));
                        r.send(x).await?;
                    }
                    Msg::Transition(r, id, new_state) => {
                        let id_str = id.to_string();
                        let res = match self.find_mut(id) {
                            Some(s) => s.transition(new_state).await.map(|s| s.state),
                            None => Err(ErrorKind::InstanceNotFound(
                                self.resource_type.to_string(),
                                id_str,
                            )
                            .into()),
                        };

                        r.send(res).await?;
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
    pipeline: Sender<Msg<PipelineArtefact>>,
    onramp: Sender<Msg<OnrampArtefact>>,
    offramp: Sender<Msg<OfframpArtefact>>,
    connector: Sender<Msg<ConnectorArtefact>>,
    binding: Sender<Msg<BindingArtefact>>,
}

#[cfg(not(tarpaulin_include))]
impl fmt::Debug for Registries {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Registries {{ ... }}")
    }
}

#[cfg(not(tarpaulin_include))]
impl Default for Registries {
    fn default() -> Self {
        Self::new()
    }
}

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

/// apply a default timeout of 2 secs to each async op
async fn wait<F, T>(future: F) -> Result<T>
where
    F: std::future::Future<Output = T>,
{
    Ok(timeout(DEFAULT_TIMEOUT, future).await?)
}

macro_rules! transition_instance {
    ($(#[$meta:meta])* $registry:ident, $fn_name:ident, $new_state:expr) => {
        $(#[$meta])*
        pub async fn $fn_name(&self, id: &TremorUrl) -> Result<InstanceState> {
            let (tx, rx) = bounded(1);
            self.$registry
                .send(Msg::Transition(tx, id.clone(), $new_state))
                .await?;
            wait(rx.recv()).await??
        }
    };
}

impl Registries {
    /// Create a new Registry
    #[must_use]
    pub fn new() -> Self {
        Self {
            binding: Registry::new(ResourceType::Binding).start(),
            pipeline: Registry::new(ResourceType::Pipeline).start(),
            onramp: Registry::new(ResourceType::Onramp).start(),
            offramp: Registry::new(ResourceType::Offramp).start(),
            connector: Registry::new(ResourceType::Connector).start(),
        }
    }
    /// serialize the mappings of this registry
    ///
    /// # Errors
    ///  * if we can't serialize the mappings
    pub async fn serialize_mappings(&self) -> Result<crate::config::MappingMap> {
        let (tx, rx) = bounded(1);
        self.binding.send(Msg::SerializeServants(tx)).await?;
        Ok(wait(rx.recv()).await?.map(|bindings| {
            bindings
                .into_iter()
                .filter_map(|v| v.mapping)
                .fold(HashMap::new(), |mut acc, v| {
                    acc.extend(v);
                    acc
                })
        })?)
    }
    /// Finds a pipeline
    ///
    /// # Errors
    ///  * if we can't find a pipeline
    pub async fn find_pipeline(
        &self,
        id: &TremorUrl,
    ) -> Result<Option<<PipelineArtefact as Artefact>::SpawnResult>> {
        let (tx, rx) = bounded(1);
        self.pipeline.send(Msg::FindServant(tx, id.clone())).await?;
        wait(rx.recv()).await??
    }
    /// Publishes a pipeline
    ///
    /// # Errors
    ///  * if I can't publish a pipeline
    pub async fn publish_pipeline(
        &self,
        id: &TremorUrl,
        servant: PipelineServant,
    ) -> Result<InstanceState> {
        let (tx, rx) = bounded(1);
        self.pipeline
            .send(Msg::PublishServant(tx, id.clone(), servant))
            .await?;
        wait(rx.recv()).await??
    }

    /// unpublishes a pipeline
    ///
    /// # Errors
    ///  * if we can't unpublish a pipeline
    pub async fn unpublish_pipeline(&self, id: &TremorUrl) -> Result<PipelineServant> {
        let (tx, rx) = bounded(1);
        self.pipeline
            .send(Msg::UnpublishServant(tx, id.clone()))
            .await?;
        wait(rx.recv()).await??
    }

    transition_instance!(
        /// start a pipeline
        pipeline,
        start_pipeline,
        InstanceState::Running
    );
    transition_instance!(
        /// pause a pipeline
        pipeline,
        pause_pipeline,
        InstanceState::Paused
    );
    transition_instance!(
        /// resume a pipeline
        pipeline,
        resume_pipeline,
        InstanceState::Running
    );
    transition_instance!(
        /// stop a pipeline - the instance in the registry will be invalid afterwards
        pipeline,
        stop_pipeline,
        InstanceState::Stopped
    );

    /// Finds an onramp
    ///
    /// # Errors
    ///  * if we can't find a onramp
    pub async fn find_onramp(
        &self,
        id: &TremorUrl,
    ) -> Result<Option<<OnrampArtefact as Artefact>::SpawnResult>> {
        let (tx, rx) = bounded(1);
        self.onramp.send(Msg::FindServant(tx, id.clone())).await?;
        rx.recv().await?
    }
    /// Publishes an onramp
    ///
    /// # Errors
    ///  * if we can't publish the onramp
    pub async fn publish_onramp(
        &self,
        id: &TremorUrl,
        servant: OnrampServant,
    ) -> Result<InstanceState> {
        let (tx, rx) = bounded(1);
        self.onramp
            .send(Msg::PublishServant(tx, id.clone(), servant))
            .await?;
        wait(rx.recv()).await??
    }
    /// Usnpublishes an onramp
    ///
    /// # Errors
    ///  * if we can't unpublish an onramp
    pub async fn unpublish_onramp(&self, id: &TremorUrl) -> Result<OnrampServant> {
        let (tx, rx) = bounded(1);
        self.onramp
            .send(Msg::UnpublishServant(tx, id.clone()))
            .await?;
        wait(rx.recv()).await??
    }

    #[cfg(test)]
    pub async fn transition_onramp(
        &self,
        id: &TremorUrl,
        new_state: InstanceState,
    ) -> Result<InstanceState> {
        let (tx, rx) = bounded(1);
        self.onramp
            .send(Msg::Transition(tx, id.clone(), new_state))
            .await?;
        wait(rx.recv()).await??
    }

    /// Finds an onramp
    ///
    /// # Errors
    ///  * if we can't find an offramp
    pub async fn find_offramp(
        &self,
        id: &TremorUrl,
    ) -> Result<Option<<OfframpArtefact as Artefact>::SpawnResult>> {
        let (tx, rx) = bounded(1);
        self.offramp.send(Msg::FindServant(tx, id.clone())).await?;
        rx.recv().await?
    }

    /// Publishes an offramp
    ///
    /// # Errors
    ///  * if we can't pubish an offramp
    pub async fn publish_offramp(
        &self,
        id: &TremorUrl,
        servant: OfframpServant,
    ) -> Result<InstanceState> {
        let (tx, rx) = bounded(1);
        self.offramp
            .send(Msg::PublishServant(tx, id.clone(), servant))
            .await?;
        rx.recv().await?
    }
    /// Unpublishes an offramp - stops it first
    ///
    /// # Errors
    ///  * if we can't unpublish an offramp
    pub async fn unpublish_offramp(&self, id: &TremorUrl) -> Result<OfframpServant> {
        let (tx, rx) = bounded(1);
        self.offramp
            .send(Msg::UnpublishServant(tx, id.clone()))
            .await?;
        wait(rx.recv()).await??
    }

    #[cfg(test)]
    pub async fn transition_offramp(
        &self,
        id: &TremorUrl,
        new_state: InstanceState,
    ) -> Result<InstanceState> {
        let (tx, rx) = bounded(1);
        self.offramp
            .send(Msg::Transition(tx, id.clone(), new_state))
            .await?;
        rx.recv().await?
    }

    /// Finds a connector
    ///
    /// # Errors
    ///  * if we can't find the connector instance identified by `id`
    pub async fn find_connector(
        &self,
        id: &TremorUrl,
    ) -> Result<Option<<ConnectorArtefact as Artefact>::SpawnResult>> {
        let (tx, rx) = bounded(1);
        self.connector
            .send(Msg::FindServant(tx, id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Publishes a connector
    ///
    /// # Errors
    ///  * if we can't publish the connector identified by `id`
    pub async fn publish_connector(
        &self,
        id: &TremorUrl,
        servant: ConnectorServant,
    ) -> Result<InstanceState> {
        let (tx, rx) = bounded(1);
        self.connector
            .send(Msg::PublishServant(tx, id.clone(), servant))
            .await?;
        wait(rx.recv()).await??
    }
    /// Stops and removes a connector instance from this registry
    ///
    /// # Errors
    ///  * if we can't stop or unpublish the connector identified by `id`
    pub async fn unpublish_connector(&self, id: &TremorUrl) -> Result<ConnectorServant> {
        let (tx, rx) = bounded(1);
        self.connector
            .send(Msg::UnpublishServant(tx, id.clone()))
            .await?;
        wait(rx.recv()).await??
    }

    transition_instance!(
        /// start a connector
        connector,
        start_connector,
        InstanceState::Running
    );
    transition_instance!(
        /// pause a connector
        connector,
        pause_connector,
        InstanceState::Paused
    );
    transition_instance!(
        /// resume a connector
        connector,
        resume_connector,
        InstanceState::Running
    );
    transition_instance!(
        /// stop a connector - the instance in the registry will be invalid afterwards
        connector,
        stop_connector,
        InstanceState::Stopped
    );

    /// Finds a binding
    ///
    /// # Errors
    ///  * if we can't find a binding
    pub async fn find_binding(
        &self,
        id: &TremorUrl,
    ) -> Result<Option<<BindingArtefact as Artefact>::SpawnResult>> {
        let (tx, rx) = bounded(1);
        self.binding.send(Msg::FindServant(tx, id.clone())).await?;
        rx.recv().await?
    }
    /// Publishes a binding
    ///
    /// # Errors
    ///  * if we can't publish a binding
    pub async fn publish_binding(
        &self,
        id: &TremorUrl,
        servant: BindingServant,
    ) -> Result<InstanceState> {
        let (tx, rx) = bounded(1);
        self.binding
            .send(Msg::PublishServant(tx, id.clone(), servant))
            .await?;
        wait(rx.recv()).await??
    }

    /// Unpublishes a binding instance and returns it
    ///
    /// # Errors
    ///  * if we can't unpublish a binding
    pub async fn unpublish_binding(&self, id: &TremorUrl) -> Result<BindingServant> {
        let (tx, rx) = bounded(1);
        self.binding
            .send(Msg::UnpublishServant(tx, id.clone()))
            .await?;
        wait(rx.recv()).await??
    }

    transition_instance!(
        /// start a binding
        binding,
        start_binding,
        InstanceState::Running
    );
    transition_instance!(
        /// pause a binding
        binding,
        pause_binding,
        InstanceState::Paused
    );
    transition_instance!(
        /// resume a binding
        binding,
        resume_binding,
        InstanceState::Running
    );
    transition_instance!(
        /// stop a binding - the instance should be unpublished afterwards
        binding,
        stop_binding,
        InstanceState::Stopped
    );

    /// stop all bindings in the binding registry
    /// thereby starting the quiescence process
    pub async fn stop_all_bindings(&self) -> Result<()> {
        let (tx, rx) = bounded(1);
        self.binding.send(Msg::ListServants(tx)).await?;
        let ids = rx.recv().await?;
        if !ids.is_empty() {
            info!(
                "Stopping Bindings: {}",
                ids.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let res = futures::future::join_all(ids.iter().map(|id| self.stop_binding(id))).await;
            for r in res {
                r?;
            }
        }

        Ok(())
    }
}
