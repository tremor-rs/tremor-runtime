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

// #![cfg_attr(coverage, no_coverage)] // We need a life discord api for this

mod handler;
mod utils;

use crate::{
    connectors::{prelude::*, spawn_task, Context},
    system::KillSwitch,
};
use async_std::{
    channel::{bounded, Receiver, Sender},
    task::JoinHandle,
};
use handler::Handler;
use serenity::prelude::*;
use std::sync::atomic::AtomicBool;
use utils::Intents;

#[derive(Deserialize, Clone)]
pub(crate) struct Config {
    pub token: String,
    #[serde(default)]
    pub intents: Vec<Intents>,
}

impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "discord".into()
    }
    async fn build_cfg(
        &self,
        _: &Alias,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config: Config = Config::new(config)?;

        let origin_uri = EventOriginUri {
            scheme: "tremor-discord".to_string(),
            host: "localhost".to_string(),
            port: None,
            path: vec![],
        };
        let message_channel = bounded(crate::QSIZE.load(Ordering::Relaxed));
        let reply_channel = bounded(crate::QSIZE.load(Ordering::Relaxed));
        Ok(Box::new(Discord {
            config,
            origin_uri,
            client_task: None,
            message_channel,
            reply_channel,
        }))
    }
}

pub(crate) struct Discord {
    config: Config,
    origin_uri: EventOriginUri,
    client_task: Option<JoinHandle<()>>,
    message_channel: (Sender<Value<'static>>, Receiver<Value<'static>>),
    reply_channel: (Sender<Value<'static>>, Receiver<Value<'static>>),
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
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        // the source is listening for events formatted as `Value` from the discord client
        let source = DiscordSource {
            rx: self.message_channel.1.clone(),
            origin_uri: self.origin_uri.clone(),
        };
        builder.spawn(source, source_context).map(Some)
    }

    /// create sink if we have a stdout or stderr stream
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        // the sink is forwarding events to the discord client where they are decoded
        // into discord events and sent out
        let sink = DiscordSink {
            tx: self.reply_channel.0.clone(),
        };
        builder.spawn(sink, sink_context).map(Some)
    }

    async fn connect(&mut self, ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        // cancel and quit client task
        if let Some(client_task) = self.client_task.take() {
            client_task.cancel().await;
        }
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
            rx: self.reply_channel.1.clone(),
            is_loop_running: AtomicBool::from(false),
        });

        let mut client = client
            .await
            .map_err(|e| Error::from(format!("Err discord creating client: {}", e)))?;
        // set up new client task
        self.client_task = Some(spawn_task(
            ctx.clone(),
            async move { Ok(client.start().await?) },
        ));
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
    ) -> Result<SinkReply> {
        for v in event.value_iter() {
            if self.tx.send(v.clone_static()).await.is_err() {
                error!(
                    "{} Discord Client unreachable. Initiating Reconnect...",
                    &ctx
                );
                ctx.notifier().connection_lost().await?;
                // return here to avoid notifying the notifier multiple times
                return Err("Discord unreachable.".into());
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
    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        self.rx
            .recv()
            .await
            .map(|data| SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                payload: (data, Value::object()).into(),
                stream: DEFAULT_STREAM_ID,
                port: None,
            })
            .map_err(Error::from)
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        true
    }
}
