// Copyright 2021, The Tremor Team
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

//! # Exit Connector
//!
//! Will stop the Tremor process gracefully when it receives an Event
//!
//! ## Expected Event format
//!
//! This Connector doesnt have any requirements on the event format.
//! But options can be provided within a record with the following fields:
//!
//! * delay: milliseconds to wait before stopping the process
use crate::connectors::prelude::*;
use crate::system::{ShutdownMode, World, DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT};
use std::time::Duration;

use simd_json::ValueAccess;

#[derive(Clone)]
pub struct Exit {
    world: World,
}

#[async_trait::async_trait()]
impl Connector for Exit {
    fn is_structured(&self) -> bool {
        true
    }
    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        Ok(true)
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: super::sink::SinkManagerBuilder,
    ) -> Result<Option<super::sink::SinkAddr>> {
        let sink = self.clone();

        Ok(Some(builder.spawn(sink, sink_context)?))
    }

    async fn on_start(&mut self, _ctx: &ConnectorContext) -> Result<ConnectorState> {
        Ok(ConnectorState::Running)
    }

    fn default_codec(&self) -> &str {
        "null"
    }
}

impl Exit {
    const DELAY: &'static str = "delay";
    const TIMEOUT: &'static str = "timeout";
    const GRACEFUL: &'static str = "graceful";
}

#[async_trait::async_trait()]
impl Sink for Exit {
    fn auto_ack(&self) -> bool {
        true
    }
    async fn on_event(
        &mut self,
        _input: &str,
        event: tremor_pipeline::Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        for (value, _meta) in event.value_meta_iter() {
            if let Some(delay) = value.get_u64(Self::DELAY) {
                async_std::task::sleep(Duration::from_millis(delay)).await;
            }
            let mode = if value.get_bool(Self::GRACEFUL).unwrap_or(true) {
                let timeout = value
                    .get_u64(Self::TIMEOUT)
                    .map_or(DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT, Duration::from_millis);
                ShutdownMode::Graceful { timeout }
            } else {
                ShutdownMode::Forceful
            };
            // this should stop the whole server process
            self.world.stop(mode).await?;
        }
        Ok(SinkReply::default())
    }
}

#[derive(Debug)]
pub(crate) struct Builder {
    world: World,
}
impl Builder {
    pub(crate) fn new(world: &World) -> Self {
        Self {
            world: world.clone(),
        }
    }
}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    async fn from_config(
        &self,
        _id: &TremorUrl,
        _config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        Ok(Box::new(Exit {
            world: self.world.clone(),
        }))
    }
}
