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

//! ┌─────────────────┐
//! │  Configuration  │
//! └─────────────────┘
//!          │
//!       publish
//!          │
//!          ▼
//! ┌─────────────────┐
//! │   Repository    │
//! └─────────────────┘
//!          │
//!        find
//!          │
//!          ▼
//! ┌─────────────────┐
//! │    Artefact     │
//! └─────────────────┘
//!          │
//!        bind
//!          │
//!          ▼
//! ┌─────────────────┐
//! │    Registry     │ (instance registry)
//! └─────────────────┘
//!

use crate::binding::{Addr as BindingAddr, Msg as BindingMsg};
use crate::connectors::{Addr as ConnectorAddr, ConnectorResult, Msg as ConnectorMsg};
use crate::errors::{ErrorKind, Result};
use crate::pipeline::MgmtMsg as PipelineMsg;
use crate::repository::{Artefact, BindingArtefact, ConnectorArtefact, PipelineArtefact};
use crate::{pipeline, QSIZE};
use async_std::channel::{bounded, unbounded, Sender};
use async_std::prelude::*;
use async_std::task;
use hashbrown::HashMap;
use std::default::Default;
use std::fmt;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tremor_common::url::{ResourceType, TremorUrl};

/// containing the InstanceState
pub mod instance;

#[derive(Debug, Clone)]
pub(crate) struct RegistryItem<A: Artefact> {
    /// only kept around for serializing it when saving the config
    artefact: A,
    instance: A::SpawnResult,
}
#[derive(Debug)]
pub(crate) struct Registry<A: Artefact> {
    resource_type: ResourceType,
    map: HashMap<TremorUrl, RegistryItem<A>>,
}

impl<A: Artefact> Registry<A> {
    pub fn new(resource_type: ResourceType) -> Self {
        Self {
            resource_type,
            map: HashMap::new(),
        }
    }

    pub fn find(&self, id: &TremorUrl) -> Option<&RegistryItem<A>> {
        self.map.get(&id.to_instance())
    }

    pub fn publish(&mut self, id: &TremorUrl, item: RegistryItem<A>) -> Result<&RegistryItem<A>> {
        let instance_id = id.to_instance();
        match self.map.insert(instance_id.clone(), item) {
            Some(_old) => {
                Err(ErrorKind::PublishFailedAlreadyExists(instance_id.to_string()).into())
            }
            None => self.map.get(&instance_id).ok_or_else(|| {
                ErrorKind::PublishFailedAlreadyExists(instance_id.to_string()).into()
            }),
        }
    }

    pub fn unpublish(&mut self, id: &TremorUrl) -> Result<RegistryItem<A>> {
        match self.map.remove(&id.to_instance()) {
            Some(removed) => Ok(removed),
            None => {
                Err(ErrorKind::UnpublishFailedDoesNotExist(id.to_instance().to_string()).into())
            }
        }
    }

    pub fn values(&self) -> Vec<A> {
        self.map.values().map(|v| v.artefact.clone()).collect()
    }

    /// returns a list of all ids of all instances currently registered
    pub fn instance_ids(&self) -> Vec<TremorUrl> {
        self.map.keys().cloned().collect()
    }
}
pub(crate) enum Msg<A: Artefact> {
    List(Sender<Vec<TremorUrl>>),
    Serialize(Sender<Vec<A>>),
    Find(Sender<Result<Option<RegistryItem<A>>>>, TremorUrl),
    Publish(Sender<Result<()>>, TremorUrl, RegistryItem<A>),
    Unpublish(Sender<Result<RegistryItem<A>>>, TremorUrl),
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
                    Msg::List(r) => r.send(self.instance_ids()).await?,
                    Msg::Serialize(r) => r.send(self.values()).await?,
                    Msg::Find(r, id) => {
                        let item = A::instance_id(&id)
                            .map(|id| self.find(&id).map(std::clone::Clone::clone));
                        r.send(item).await?;
                    }
                    Msg::Publish(r, id, s) => {
                        let res =
                            A::instance_id(&id).and_then(|id| self.publish(&id, s).map(|_| ()));
                        r.send(res).await?;
                    }

                    Msg::Unpublish(r, id) => {
                        let x = A::instance_id(&id)
                            .and_then(|instance_id| self.unpublish(&instance_id));
                        r.send(x).await?;
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
    Ok(future.timeout(DEFAULT_TIMEOUT).await?)
}

impl Registries {
    /// Create a new Registry
    #[must_use]
    pub fn new() -> Self {
        Self {
            binding: Registry::new(ResourceType::Binding).start(),
            pipeline: Registry::new(ResourceType::Pipeline).start(),
            connector: Registry::new(ResourceType::Connector).start(),
        }
    }
    /// serialize the mappings of this registry
    ///
    /// # Errors
    ///  * if we can't serialize the mappings
    pub async fn serialize_mappings(&self) -> Result<crate::config::MappingMap> {
        let (tx, rx) = bounded(1);
        self.binding.send(Msg::Serialize(tx)).await?;
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
    pub async fn find_pipeline(&self, id: &TremorUrl) -> Result<Option<pipeline::Addr>> {
        let (tx, rx) = bounded(1);
        self.pipeline.send(Msg::Find(tx, id.to_instance())).await?;
        wait(rx.recv()).await??.map(|item| item.map(|i| i.instance))
    }
    /// Publishes a pipeline
    ///
    /// # Errors
    ///  * if I can't publish a pipeline
    pub async fn publish_pipeline(
        &self,
        id: &TremorUrl,
        artefact: PipelineArtefact,
        instance: pipeline::Addr,
    ) -> Result<()> {
        let (tx, rx) = bounded(1);
        let item = RegistryItem { artefact, instance };
        self.pipeline
            .send(Msg::Publish(tx, id.to_instance(), item))
            .await?;
        wait(rx.recv()).await??
    }

    /// unpublishes a pipeline
    ///
    /// # Errors
    ///  * if we can't unpublish a pipeline
    pub async fn unpublish_pipeline(&self, id: &TremorUrl) -> Result<pipeline::Addr> {
        let (tx, rx) = bounded(1);
        self.pipeline
            .send(Msg::Unpublish(tx, id.to_instance()))
            .await?;
        wait(rx.recv()).await??.map(|item| item.instance)
    }

    /// start a pipeline
    ///
    /// # Errors
    ///   * if the pipeline could not be started
    pub async fn start_pipeline(&self, id: &TremorUrl) -> Result<()> {
        if let Some(addr) = self.find_pipeline(id).await? {
            addr.start().await
        } else {
            Err(
                ErrorKind::InstanceNotFound(ResourceType::Pipeline.to_string(), id.to_string())
                    .into(),
            )
        }
    }

    /// pause a pipeline
    ///
    /// # Errors
    ///   * if the pipeline could not be started
    pub async fn pause_pipeline(&self, id: &TremorUrl) -> Result<()> {
        if let Some(addr) = self.find_pipeline(id).await? {
            addr.send_mgmt(PipelineMsg::Pause).await
        } else {
            Err(
                ErrorKind::InstanceNotFound(ResourceType::Pipeline.to_string(), id.to_string())
                    .into(),
            )
        }
    }

    /// resume a pipeline
    ///
    /// # Errors
    ///   * if the given `id` is not found or if we couldn't communicate with it
    pub async fn resume_pipeline(&self, id: &TremorUrl) -> Result<()> {
        if let Some(addr) = self.find_pipeline(id).await? {
            addr.send_mgmt(PipelineMsg::Resume).await
        } else {
            Err(
                ErrorKind::InstanceNotFound(ResourceType::Pipeline.to_string(), id.to_string())
                    .into(),
            )
        }
    }

    /// stop a pipeline - the instance in the registry will be invalid afterwards
    ///
    /// # Errors
    ///   * if the pipeline could not be started
    pub async fn stop_pipeline(&self, id: &TremorUrl) -> Result<()> {
        if let Some(addr) = self.find_pipeline(id).await? {
            // TODO: should we wait for this?
            addr.stop().await
        } else {
            Err(
                ErrorKind::InstanceNotFound(ResourceType::Pipeline.to_string(), id.to_string())
                    .into(),
            )
        }
    }

    /// Finds a connector
    ///
    /// # Errors
    ///  * if we can't find the connector instance identified by `id`
    pub async fn find_connector(&self, id: &TremorUrl) -> Result<Option<ConnectorAddr>> {
        let (tx, rx) = bounded(1);
        self.connector.send(Msg::Find(tx, id.clone())).await?;
        wait(rx.recv()).await??.map(|item| item.map(|i| i.instance))
    }

    /// Publishes a connector
    ///
    /// # Errors
    ///  * if we can't publish the connector identified by `id`
    pub async fn publish_connector(
        &self,
        id: &TremorUrl,
        artefact: ConnectorArtefact,
        instance: ConnectorAddr,
    ) -> Result<()> {
        let (tx, rx) = bounded(1);
        let item = RegistryItem { artefact, instance };
        self.connector
            .send(Msg::Publish(tx, id.clone(), item))
            .await?;
        wait(rx.recv()).await??
    }
    /// Stops and removes a connector instance from this registry
    ///
    /// # Errors
    ///  * if we can't stop or unpublish the connector identified by `id`
    pub async fn unpublish_connector(&self, id: &TremorUrl) -> Result<ConnectorAddr> {
        let (tx, rx) = bounded(1);
        self.connector.send(Msg::Unpublish(tx, id.clone())).await?;
        wait(rx.recv()).await??.map(|item| item.instance)
    }

    /// start a connector
    pub async fn start_connector(&self, id: &TremorUrl) -> Result<()> {
        if let Some(addr) = self.find_connector(id).await? {
            // TODO: should we wait for this?
            addr.send(ConnectorMsg::Start).await
        } else {
            Err(
                ErrorKind::InstanceNotFound(ResourceType::Connector.to_string(), id.to_string())
                    .into(),
            )
        }
    }

    /// pause a connector - does not wait for the actual pausing process, just sends the msg
    pub async fn pause_connector(&self, id: &TremorUrl) -> Result<()> {
        if let Some(addr) = self.find_connector(id).await? {
            // TODO: should we wait for this?
            addr.send(ConnectorMsg::Pause).await
        } else {
            Err(
                ErrorKind::InstanceNotFound(ResourceType::Connector.to_string(), id.to_string())
                    .into(),
            )
        }
    }

    /// resume a connector - does not wait for the actual resuming process, just sends the msg
    pub async fn resume_connector(&self, id: &TremorUrl) -> Result<()> {
        if let Some(addr) = self.find_connector(id).await? {
            addr.send(ConnectorMsg::Resume).await
        } else {
            Err(
                ErrorKind::InstanceNotFound(ResourceType::Connector.to_string(), id.to_string())
                    .into(),
            )
        }
    }

    /// stop a connector - notification for the result of the stop process will be sent to `sender`
    pub async fn stop_connector(
        &self,
        id: &TremorUrl,
        sender: Sender<ConnectorResult<()>>,
    ) -> Result<()> {
        if let Some(addr) = self.find_connector(id).await? {
            addr.send(ConnectorMsg::Stop(sender)).await
        } else {
            Err(
                ErrorKind::InstanceNotFound(ResourceType::Connector.to_string(), id.to_string())
                    .into(),
            )
        }
    }

    /// drain a connector - notification for the result of the drain process will be sent to `sender`
    pub async fn drain_connector(
        &self,
        id: &TremorUrl,
        sender: Sender<ConnectorResult<()>>,
    ) -> Result<()> {
        if let Some(addr) = self.find_connector(id).await? {
            addr.send(ConnectorMsg::Drain(sender)).await
        } else {
            Err(
                ErrorKind::InstanceNotFound(ResourceType::Connector.to_string(), id.to_string())
                    .into(),
            )
        }
    }

    /// Finds a binding.
    /// Returns both the BindingAddr as well as the BindingArtefact that has been modified by applying mapping to the links.
    ///
    /// # Errors
    ///  * if we can't find a binding
    pub async fn find_binding(
        &self,
        id: &TremorUrl,
    ) -> Result<Option<(BindingAddr, BindingArtefact)>> {
        let (tx, rx) = bounded(1);
        self.binding.send(Msg::Find(tx, id.clone())).await?;
        wait(rx.recv())
            .await??
            .map(|item| item.map(|i| (i.instance, i.artefact)))
    }

    /// Lists all binding instances.
    ///
    /// # Errors
    ///   * if we cannot communicate with the registry
    pub async fn list_bindings(&self) -> Result<Vec<TremorUrl>> {
        let (tx, rx) = bounded(1);
        self.binding.send(Msg::List(tx)).await?;
        Ok(wait(rx.recv()).await??)
    }

    /// Publishes a binding
    ///
    /// # Errors
    ///  * if we can't publish a binding
    pub async fn publish_binding(
        &self,
        id: &TremorUrl,
        artefact: BindingArtefact,
        instance: BindingAddr,
    ) -> Result<()> {
        let (tx, rx) = bounded(1);
        let item = RegistryItem { artefact, instance };
        self.binding
            .send(Msg::Publish(tx, id.clone(), item))
            .await?;
        wait(rx.recv()).await??
    }

    /// Unpublishes a binding instance and returns it
    ///
    /// # Errors
    ///  * if we can't unpublish a binding
    pub async fn unpublish_binding(
        &self,
        id: &TremorUrl,
    ) -> Result<(BindingAddr, BindingArtefact)> {
        let (tx, rx) = bounded(1);
        self.binding.send(Msg::Unpublish(tx, id.clone())).await?;
        wait(rx.recv())
            .await??
            .map(|item| (item.instance, item.artefact))
    }

    /// start a binding
    pub async fn start_binding(&self, id: &TremorUrl) -> Result<()> {
        if let Some((addr, _)) = self.find_binding(id).await? {
            addr.send(BindingMsg::Start).await
        } else {
            Err(
                ErrorKind::InstanceNotFound(ResourceType::Binding.to_string(), id.to_string())
                    .into(),
            )
        }
    }

    /// pause a binding
    pub async fn pause_binding(&self, id: &TremorUrl) -> Result<()> {
        if let Some((addr, _)) = self.find_binding(id).await? {
            addr.send(BindingMsg::Pause).await
        } else {
            Err(
                ErrorKind::InstanceNotFound(ResourceType::Binding.to_string(), id.to_string())
                    .into(),
            )
        }
    }

    /// resume a binding
    pub async fn resume_binding(&self, id: &TremorUrl) -> Result<()> {
        if let Some((addr, _)) = self.find_binding(id).await? {
            addr.send(BindingMsg::Resume).await
        } else {
            Err(
                ErrorKind::InstanceNotFound(ResourceType::Binding.to_string(), id.to_string())
                    .into(),
            )
        }
    }

    /// stop a binding - notification for the result of the stop process will be sent to `sender`
    pub async fn stop_binding(&self, id: &TremorUrl, sender: Sender<Result<()>>) -> Result<()> {
        if let Some((addr, _)) = self.find_binding(id).await? {
            addr.send(BindingMsg::Stop(sender)).await
        } else {
            Err(
                ErrorKind::InstanceNotFound(ResourceType::Binding.to_string(), id.to_string())
                    .into(),
            )
        }
    }

    /// drain a binding - notification for the result of the drain process will be sent to `sender`
    pub async fn drain_binding(&self, id: &TremorUrl, sender: Sender<Result<()>>) -> Result<()> {
        if let Some((addr, _)) = self.find_binding(id).await? {
            addr.send(BindingMsg::Drain(sender)).await
        } else {
            Err(
                ErrorKind::InstanceNotFound(ResourceType::Binding.to_string(), id.to_string())
                    .into(),
            )
        }
    }

    /// drain all bindings in the binding registry
    /// thereby starting the quiescence process
    ///
    /// # Errors
    ///   * If we can't drain all the bindings
    pub async fn drain_all_bindings(&self, timeout: Duration) -> Result<()> {
        let ids = self.list_bindings().await?;
        if !ids.is_empty() {
            info!(
                "Draining Bindings: {}",
                ids.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let (tx, rx) = unbounded();
            for id in &ids {
                self.drain_binding(id, tx.clone()).await?;
            }

            let mut expected_drains = ids.len();
            // spawn a task for waiting on all the drain messages
            let h = task::spawn::<_, Result<()>>(async move {
                while expected_drains > 0 {
                    let res = rx.recv().await?;
                    if let Err(e) = res {
                        error!("Error during Draining of a Binding: {}", e);
                    }
                    expected_drains = expected_drains.saturating_sub(1);
                }
                Ok(())
            });
            h.timeout(timeout).await??;
        }

        Ok(())
    }

    /// stop all bindings in the binding registry
    ///
    /// # Errors
    ///   * If we can't stop all bindings
    pub async fn stop_all_bindings(&self, timeout: Duration) -> Result<()> {
        let ids = self.list_bindings().await?;
        if !ids.is_empty() {
            info!(
                "Stopping Bindings: {}",
                ids.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let (tx, rx) = unbounded();
            for id in &ids {
                self.stop_binding(id, tx.clone()).await?;
            }
            let mut expected_stops = ids.len();
            let h = task::spawn::<_, Result<()>>(async move {
                while expected_stops > 0 {
                    let res = rx.recv().await?;
                    if let Err(e) = res {
                        error!("Error during Stopping of a Binding: {}", e);
                    }
                    expected_stops = expected_stops.saturating_sub(1);
                }
                Ok(())
            });
            h.timeout(timeout).await??;
        }

        Ok(())
    }
}
