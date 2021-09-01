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
use crate::{connectors, offramp, onramp, pipeline};

/// Representing an artefact instance and
/// encapsulates specializations of state transitions
#[async_trait::async_trait]
pub trait Instance: Send {
    /// Initialized -> Running
    async fn start(&mut self, _world: &World, _id: &TremorUrl) -> Result<()> {
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

/// onramp instance
#[async_trait::async_trait()]
impl Instance for onramp::Addr {}

/// offramp instance
#[async_trait::async_trait()]
impl Instance for offramp::Addr {
    async fn stop(&mut self, _world: &World, _id: &TremorUrl) -> Result<()> {
        self.send(offramp::Msg::Terminate).await?;
        Ok(())
    }
}

/// connector instance
#[async_trait::async_trait()]
impl Instance for connectors::Addr {
    async fn start(&mut self, _world: &World, _id: &TremorUrl) -> Result<()> {
        self.send(connectors::Msg::Start).await
    }

    async fn stop(&mut self, _world: &World, _id: &TremorUrl) -> Result<()> {
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
            .filter(|c| c.is_connector())
            .cloned()
            .collect();
        let source_connectors: HashSet<TremorUrl> = self
            .binding
            .links
            .iter()
            .map(|(from, _tos)| from)
            .filter(|c| c.is_connector())
            .cloned()
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

    async fn stop(&mut self, _world: &World, _id: &TremorUrl) -> Result<()> {
        // FIXME: quiescence
        // stop all instances - traverse the links graph
        let _sinks: HashSet<TremorUrl> = self
            .binding
            .links
            .iter()
            .flat_map(|(_from, tos)| tos.iter())
            //.filter(|c| c.is_connector())
            .cloned()
            .collect();
        let _sources: HashSet<TremorUrl> = self
            .binding
            .links
            .iter()
            .map(|(from, _tos)| from)
            .filter(|c| c.is_connector())
            .cloned()
            .collect();

        //let start_points = sources.difference(&sinks);
        //let mut connected = vec![];
        //for start_point in start_points {
        // FIXME
        //}

        // TODO: maybe proper graph traversal makes sense here
        // TODO: proper event draining support at the connector level
        //       - so we don't lose in-flight events that have already been read but not fordwarded to any pipeline
        //       - it should suffice
        Ok(())
    }

    async fn pause(&mut self, _world: &World, _id: &TremorUrl) -> Result<()> {
        Ok(())
    }

    async fn resume(&mut self, _world: &World, _id: &TremorUrl) -> Result<()> {
        Ok(())
    }
}

/// pipeline instance - no-op implementation
///
/// FIXME: at least specialize the stop transition
impl Instance for pipeline::Addr {}
