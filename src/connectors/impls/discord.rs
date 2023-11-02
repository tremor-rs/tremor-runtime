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

//! The discord connector provides integration with the `discord` service and is
//! used for the [Tremor Community Bot](https://github.com/tremor-rs/tremor-bot) which is
//! implemented in tremor.
//!
//! The connectors requires a bot application [token](https://discordjs.guide/preparations/setting-up-a-bot-application.html#your-token) and a set of [gateway intents](https://discordjs.guide/popular-topics/intents.html#privileged-intents).
//!
//! As a quick start, the liberally licensed `tremor-bot` can be used as a starting point for
//! a custom bot based on tremor
//!
//! ## Configuration
//!
//! ```tremor
//!   define connector discord from discord
//!   with
//!     config = {
//!       # required - A discord API token
//!       "token" = "some-token",
//!
//!       "intents" = [...]
//!       
//!     },
//!   end;
//!
//!

mod handler;
mod utils;

use crate::channel::{bounded, Receiver, Sender};
use crate::connectors::prelude::*;
use crate::connectors::Context as ContextTrait;
use handler::Handler;
use serenity::prelude::*;
use tokio::task::JoinHandle;
use utils::Intents;

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub token: String,
    #[serde(default)]
    pub intents: Vec<Intents>,
}

impl tremor_config::Impl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "discord".into()
    }
    async fn build_cfg(
        &self,
        _: &alias::Connector,
        _: &ConnectorConfig,
        config: &Value,
    ) -> Result<Box<dyn Connector>> {
        let config: Config = Config::new(config)?;

        let origin_uri = EventOriginUri {
            scheme: "tremor-discord".to_string(),
            host: "localhost".to_string(),
            port: None,
            path: vec![],
        };
        let message_channel = bounded(qsize());
        let (reply_tx, reply_rx) = bounded(qsize());
        Ok(Box::new(Discord {
            config,
            origin_uri,
            client_task: None,
            message_channel: (message_channel.0, Some(message_channel.1)),
            reply_tx,
            reply_rx: Some(reply_rx),
        }))
    }
}

pub(crate) struct Discord {
    config: Config,
    origin_uri: EventOriginUri,
    client_task: Option<JoinHandle<()>>,
    message_channel: (Sender<Value<'static>>, Option<Receiver<Value<'static>>>),
    reply_tx: Sender<Value<'static>>,
    reply_rx: Option<Receiver<Value<'static>>>,
}
impl std::fmt::Debug for Discord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Discord")
    }
}

#[async_trait::async_trait()]
impl Connector for Discord {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        // the source is listening for events formatted as `Value` from the discord client
        let rx = self
            .message_channel
            .1
            .take()
            .ok_or_else(|| ConnectorError::AlreadyCreated(ctx.alias().clone()))?;
        let source = DiscordSource {
            rx,
            origin_uri: self.origin_uri.clone(),
        };
        Ok(Some(builder.spawn(source, ctx)))
    }

    /// create sink if we have a stdout or stderr stream
    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        // the sink is forwarding events to the discord client where they are decoded
        // into discord events and sent out
        let sink = DiscordSink {
            tx: self.reply_tx.clone(),
        };
        Ok(Some(builder.spawn(sink, ctx)))
    }

    async fn connect(&mut self, ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        if let Some(rx) = self.reply_rx.take() {
            let token = self.config.token.clone();

            let intents = self
                .config
                .intents
                .iter()
                .copied()
                .map(Intents::into)
                .fold(GatewayIntents::default(), |a, b| a | b);

            let client = Client::builder(&token, intents).event_handler(Handler {
                tx: self.message_channel.0.clone(),
                rx: RwLock::new(Some(rx)),
            });

            let mut client = client.await?;

            // set up new client task
            self.client_task = Some(spawn_task(
                ctx.clone(),
                async move { Ok(client.start().await?) },
            ));
        }
        Ok(true)
    }
}

struct DiscordSink {
    tx: Sender<Value<'static>>,
}

#[async_trait::async_trait()]
impl Sink for DiscordSink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: tremor_pipeline::Event,
        ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> anyhow::Result<SinkReply> {
        for v in event.value_iter() {
            if let Err(e) = self.tx.send(v.clone_static()).await {
                error!(
                    "{} Discord Client unreachable. Initiating Reconnect...",
                    &ctx
                );
                ctx.notifier().connection_lost().await?;
                // return here to avoid notifying the notifier multiple times
                return Err(e.into());
            }
        }
        Ok(SinkReply::NONE)
    }

    fn auto_ack(&self) -> bool {
        true
    }
}

struct DiscordSource {
    rx: Receiver<Value<'static>>,
    origin_uri: EventOriginUri,
}
#[async_trait::async_trait()]
impl Source for DiscordSource {
    #[allow(clippy::option_if_let_else)]
    async fn pull_data(
        &mut self,
        _pull_id: &mut u64,
        _ctx: &SourceContext,
    ) -> anyhow::Result<SourceReply> {
        Ok(self
            .rx
            .recv()
            .await
            .map(|data| SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                payload: (data, Value::object()).into(),
                stream: DEFAULT_STREAM_ID,
                port: None,
            })
            .ok_or(ChannelError::Recv)?)
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        true
    }
}
