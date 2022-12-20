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
use crate::system::{KillSwitch, ShutdownMode};
use std::time::Duration;
use value_trait::ValueAccess;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    #[serde(default)]
    delay: Option<u64>,

    #[serde(default = "default_true")]
    graceful: bool,
}

impl ConfigImpl for Config {}

impl Default for Config {
    fn default() -> Self {
        Self {
            delay: None,
            graceful: default_true(),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "exit".into()
    }

    async fn build(
        &self,
        _id: &Alias,
        config: &ConnectorConfig,
        kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = match config.config.as_ref() {
            Some(raw_config) => Config::new(raw_config)?,
            None => Config::default(),
        };
        Ok(Box::new(Exit {
            kill_switch: kill_switch.clone(),
            config,
            done: false,
        }))
    }
}
#[derive(Clone)]
pub(crate) struct Exit {
    kill_switch: KillSwitch,
    config: Config,
    done: bool,
}

#[async_trait::async_trait]
impl Connector for Exit {
    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = self.clone();

        Ok(Some(builder.spawn(sink, ctx)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }
}

impl Exit {
    const DELAY: &'static str = "delay";
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
        ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        if self.done {
            debug!("{ctx} Already exited.");
        } else if let Some((value, _meta)) = event.value_meta_iter().next() {
            if let Some(delay) = value.get_u64(Self::DELAY).or(self.config.delay) {
                let delay = Duration::from_nanos(delay);
                info!("{ctx} Sleeping for {delay:?} before triggering shutdown.");
                tokio::time::sleep(delay).await;
            }
            let mode = if value
                .get_bool(Self::GRACEFUL)
                .unwrap_or(self.config.graceful)
            {
                ShutdownMode::Graceful
            } else {
                ShutdownMode::Forceful
            };
            let kill_switch = self.kill_switch.clone();
            let stop_ctx = ctx.clone();

            // this should stop the whole server process
            // we spawn this out into another task, so we don't block the sink loop handling control plane messages
            tokio::task::spawn(async move {
                info!("{stop_ctx} Exiting...");
                stop_ctx.swallow_err(kill_switch.stop(mode).await, "Error stopping the world");
            });

            self.done = true;
        }

        Ok(SinkReply::NONE)
    }
}
