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
#![allow(clippy::doc_markdown)]

//! :::info
//!
//! This connector is not intended for production use, but for testing the Tremor runtime itself. To enable it pass `--debug-connectors` to tremor.
//!
//! :::
//!
//!
//! The `exit` connector generates is a convenience designed for tremor based systems
//! that need to deterministically stop.
//!
//! The connector is particularly useful for integration tests to halt a process when
//! testing has completed. The connector is also useful for interactive testing when
//! console redirection is being used to test processing behaviour allowing an "exit"
//! command to halt the tremor server process.
//!
//! ## Configuration
//!
//! The connector needs no configuration
//!
//! ```tremor
//!   define connector stop from exit;
//! ```
//!
//! ## Illustrative example
//!
//! A simple tremor application that starts and shuts down when an event is
//! received.
//!
//! A high level summary of this flow's logic:
//!
//! ```mermaid
//! graph LR
//!     A{Console} -->|every line| B(triggered events)
//!     B -->|passthrough| C{Standard Output}
//!     B -->|stop server| D{Exit Runtime}
//! ```
//!
//! The application source for this example:
//!
//! ```tremor title="stopper.troy"
//! define flow stopper
//! flow
//!   use tremor::connectors; # Use predefined console and exit connector definitions
//!   use tremor::pipelines;  # Use predefined passthrough pipeline definition
//!
//!   create connector console from connectors::console;
//!   create connector stop from connectors::exit;
//!   create pipeline passthrough from pipelines::passthrough;
//!   
//!   connect /connector/console to /pipeline/passthrough;
//!   connect /pipeline/passthrough to /connector/console;
//!   connect /pipeline/passthrough to /connector/stop;
//! end;
//!
//! deploy flow stopper;
//! ```
//!
//! :::note
//! Tremor supports a `quiescence` protocol. As we connect our `console` to the
//! passthrough before the exit connector `stop` - this means our event from the
//! console's `stdin` flows through the passthrough to `stdout` in the same connector
//! before the event is passed to the `stop` exit connector instance.
//!
//! So we should see our event echo'd once.
//!
//! ```shell
//! $ tremor server run stopper.troy
//! tremor version: 0.12.0 (RELEASE)
//! tremor instance: tremor
//! rd_kafka version: 0x000002ff, 1.8.2
//! allocator: snmalloc
//! Listening at: http://0.0.0.0:9898
//! Beep!
//! Beep!
//! ```
//!
//! :::

use crate::sink::prelude::*;
use std::time::Duration;
use tremor_system::{killswitch::ShutdownMode, selector::PluginType};
use tremor_value::prelude::*;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    #[serde(default)]
    delay: Option<u64>,

    #[serde(default = "tremor_common::default_true")]
    graceful: bool,
}

impl tremor_config::Impl for Config {}

impl Default for Config {
    fn default() -> Self {
        Self {
            delay: None,
            graceful: true,
        }
    }
}

/// Exit connector
#[derive(Debug, Default)]
pub struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "exit".into()
    }

    fn plugin_type(&self) -> PluginType {
        PluginType::Debug
    }

    async fn build(
        &self,
        _id: &alias::Connector,
        config: &ConnectorConfig,
        kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
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
    ) -> anyhow::Result<Option<SinkAddr>> {
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
        event: tremor_system::event::Event,
        ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> anyhow::Result<SinkReply> {
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
