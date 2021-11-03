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

//
// Artefact instance lifecycle support and specializations for the
// different artefact types
//

use hashbrown::HashSet;

use crate::errors::Result;
use crate::repository::BindingArtefact;
use crate::system::World;
use crate::url::TremorUrl;
use crate::{connectors, pipeline};

/// Representing an artefact instance and
/// encapsulates specializations of state transitions
#[async_trait::async_trait]
pub trait Instance: Send {
    /// Initialized -> Running
    async fn start(&mut self, _world: &World, _id: &TremorUrl) -> Result<()> {
        Ok(())
    }

    /// * -> Draining
    async fn drain(&mut self, _world: &World, _id: &TremorUrl) -> Result<()> {
        Ok(())
    }
    /// * -> Stopped
    async fn stop(&mut self, _world: &World, _id: &TremorUrl) -> Result<()> {
        Ok(())
    }

    /// Running -> Paused
    async fn pause(&mut self, _world: &World, _id: &TremorUrl) -> Result<()> {
        Ok(())
    }
    /// Paused -> Running
    async fn resume(&mut self, _world: &World, _id: &TremorUrl) -> Result<()> {
        Ok(())
    }
}

/// connector instance
#[async_trait::async_trait()]
impl Instance for connectors::Addr {
    async fn start(&mut self, _world: &World, _id: &TremorUrl) -> Result<()> {
        self.send(connectors::Msg::Start).await
    }

    async fn drain(&mut self, _world: &World, _id: &TremorUrl) -> Result<()> {
        let (tx, rx) = async_std::channel::bounded(1);
        self.send(connectors::Msg::Drain(tx)).await?;
        rx.recv().await?
    }

    async fn stop(&mut self, _world: &World, _id: &TremorUrl) -> Result<()> {
        // we do not drain here, only in BindingArtefact::stop
        self.send(connectors::Msg::Stop).await
    }

    async fn pause(&mut self, _world: &World, _id: &TremorUrl) -> Result<()> {
        self.send(connectors::Msg::Pause).await
    }

    async fn resume(&mut self, _world: &World, _id: &TremorUrl) -> Result<()> {
        self.send(connectors::Msg::Resume).await
    }
}

/// binding instance
#[async_trait::async_trait()]
impl Instance for BindingArtefact {
    async fn start(&mut self, world: &World, _id: &TremorUrl) -> Result<()> {
        // old artefact types (onramp, offramp) don't (and never will) support starting
        // start all pipelines first - order doesnt matter as connectors aren't started yet
        let pipelines: HashSet<TremorUrl> = self
            .binding
            .links
            .keys()
            .chain(self.binding.links.values().flatten())
            .filter(|url| url.is_pipeline())
            .map(TremorUrl::to_instance)
            .collect();
        for pipe in &pipelines {
            world.reg.start_pipeline(pipe).await?;
        }
        // start connectors
        let sink_connectors: HashSet<TremorUrl> = self
            .binding
            .links
            .iter()
            .flat_map(|(_from, tos)| tos.iter())
            .map(TremorUrl::to_instance)
            .filter(TremorUrl::is_connector)
            .collect();
        let source_connectors: HashSet<TremorUrl> = self
            .binding
            .links
            .iter()
            .map(|(from, _tos)| from.to_instance())
            .filter(TremorUrl::is_connector)
            .collect();

        // starting connectors without source first, so they are ready when stuff arrives
        for conn in sink_connectors.difference(&source_connectors) {
            world.reg.start_connector(conn).await?;
        }
        // start source/sink connectors in random order
        for conn in sink_connectors.intersection(&source_connectors) {
            world.reg.start_connector(conn).await?;
        }
        // start source only connectors
        for conn in source_connectors.difference(&sink_connectors) {
            world.reg.start_connector(conn).await?;
        }
        Ok(())
    }

    async fn drain(&mut self, world: &World, id: &TremorUrl) -> Result<()> {
        // QUIESCENCE
        // - send drain msg to all connectors
        // - wait until
        //   a) all connectors are drained (means all pipelines in between are also drained) or
        //   b) we timed out
        info!("[Binding::{}] Draining...", id);
        let sinks: HashSet<TremorUrl> = self
            .binding
            .links
            .iter()
            .flat_map(|(_from, tos)| tos.iter())
            .filter(|url| (*url).is_connector())
            .map(TremorUrl::to_instance)
            .collect();
        let sources: HashSet<TremorUrl> = self
            .binding
            .links
            .iter()
            .map(|(from, _tos)| from.to_instance())
            .filter(TremorUrl::is_connector)
            .collect();

        let start_points = sources.difference(&sinks);
        let mixed_pickles = sinks.intersection(&sources);
        let end_points = sinks.difference(&sources);
        let mut drain_futures = Vec::with_capacity(sinks.len() + sources.len());

        // source only connectors
        for start_point in start_points {
            drain_futures.push(world.drain_connector(start_point));
        }
        // source/sink connectors
        for url in mixed_pickles {
            drain_futures.push(world.drain_connector(url));
        }
        // sink only connectors
        for url in end_points {
            drain_futures.push(world.drain_connector(url));
        }
        // wait for 5 secs for all drain futures
        // it might be this binding represents a topology that doesn't support proper quiescence
        let results = futures::future::join_all(drain_futures).await;
        // report some errors if any
        info!("[Binding::{}] Drained.", id);
        for r in results {
            if let Err(e) = r {
                error!("[Binding::{}] Error during Draining: {}", id, e);
            }
        }
        Ok(())
    }

    async fn stop(&mut self, world: &World, id: &TremorUrl) -> Result<()> {
        // - call stop on all instances
        // - we ignore onramps and offramps
        // - we try to go from source connectors to sink connectors, this is not always possible
        info!("[Binding::{}] Stopping...", id);
        let connectors: HashSet<TremorUrl> = self
            .binding
            .links
            .iter()
            .flat_map(|(from, tos)| tos.iter().chain(std::iter::once(from)))
            .filter(|url| (*url).is_connector())
            .map(TremorUrl::to_instance)
            .collect();
        // actually stop everything
        // connectors
        for connector_url in &connectors {
            if let Err(e) = world.reg.stop_connector(connector_url).await {
                error!(
                    "[Binding::{}] Error while stopping {}: {}",
                    id, connector_url, e
                );
            }
        }
        let pipelines: HashSet<TremorUrl> = self
            .binding
            .links
            .iter()
            .flat_map(|(from, tos)| std::iter::once(from).chain(tos.iter()))
            .filter(|url| (*url).is_pipeline())
            .map(TremorUrl::to_instance)
            .collect();
        // pipelines
        for pipeline_url in pipelines {
            if let Err(e) = world.reg.stop_pipeline(&pipeline_url).await {
                error!(
                    "[Binding::{}] Error while stopping {}: {}",
                    id, &pipeline_url, e
                );
            }
        }
        info!("[Binding::{}] Stopped.", id);
        Ok(())
    }

    async fn pause(&mut self, world: &World, id: &TremorUrl) -> Result<()> {
        let sinks: HashSet<TremorUrl> = self
            .binding
            .links
            .iter()
            .flat_map(|(_from, tos)| tos.iter())
            .map(TremorUrl::to_instance)
            .filter(TremorUrl::is_connector)
            .collect();
        let sources: HashSet<TremorUrl> = self
            .binding
            .links
            .iter()
            .map(|(from, _tos)| from.to_instance())
            .filter(TremorUrl::is_connector)
            .collect();
        let pipelines: HashSet<TremorUrl> = self
            .binding
            .links
            .iter()
            .flat_map(|(from, tos)| std::iter::once(from).chain(tos.iter()))
            .map(TremorUrl::to_instance)
            .filter(TremorUrl::is_pipeline)
            .collect();

        for source in sources.difference(&sinks) {
            world.reg.pause_connector(source).await?;
        }
        for source_n_sink in sources.intersection(&sinks) {
            world.reg.pause_connector(source_n_sink).await?;
        }
        for sink in sinks.difference(&sources) {
            world.reg.pause_connector(sink).await?;
        }

        for url in pipelines {
            world.reg.pause_pipeline(&url).await?;
        }
        info!("[Binding::{}] Paused.", id);
        Ok(())
    }

    async fn resume(&mut self, world: &World, id: &TremorUrl) -> Result<()> {
        let sinks: HashSet<TremorUrl> = self
            .binding
            .links
            .iter()
            .flat_map(|(_from, tos)| tos.iter())
            .map(TremorUrl::to_instance)
            .filter(TremorUrl::is_connector)
            .collect();
        let sources: HashSet<TremorUrl> = self
            .binding
            .links
            .iter()
            .map(|(from, _tos)| from.to_instance())
            .filter(TremorUrl::is_connector)
            .collect();
        let pipelines: HashSet<TremorUrl> = self
            .binding
            .links
            .iter()
            .flat_map(|(from, tos)| std::iter::once(from).chain(tos.iter()))
            .filter(|url| url.is_pipeline())
            .map(TremorUrl::to_instance)
            .collect();

        for url in pipelines {
            world.reg.resume_pipeline(&url).await?;
        }

        for sink in sinks.difference(&sources) {
            world.reg.resume_connector(sink).await?;
        }
        for source_n_sink in sources.intersection(&sinks) {
            world.reg.resume_connector(source_n_sink).await?;
        }
        for source in sources.difference(&sinks) {
            world.reg.resume_connector(source).await?;
        }

        info!("[Binding::{}] Paused.", id);
        Ok(())
    }
}

/// pipeline instance - no-op implementation
///
/// FIXME: at least specialize the stop transition
impl Instance for pipeline::Addr {}
