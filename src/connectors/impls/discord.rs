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

use std::sync::atomic::AtomicBool;

use crate::connectors::prelude::*;
use crate::QSIZE;

mod handler;
mod utils;

use async_std::{
    channel::{bounded, Receiver, Sender, TryRecvError},
    task,
};
use handler::*;
use serenity::{client::bridge::gateway::GatewayIntents, Client};
use utils::*;

#[derive(Deserialize, Clone)]
pub struct Config {
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
    async fn from_config(
        &self,
        id: &TremorUrl,
        config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;

            let origin_uri = EventOriginUri {
                scheme: "tremor-discord".to_string(),
                host: "localhost".to_string(),
                port: None,
                path: vec![],
            };
            Ok(Box::new(Discord {
                onramp_id: id.clone(),
                origin_uri,
                config,
                client: None,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(id.to_string()).into())
        }
    }
}

#[derive(Clone)]
pub struct Discord {
    pub config: Config,
    origin_uri: EventOriginUri,
    onramp_id: TremorUrl,
    client: Option<(Sender<Value<'static>>, Receiver<Value<'static>>)>,
}
impl std::fmt::Debug for Discord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Discord")
    }
}

#[async_trait::async_trait()]
impl Sink for Discord {
    async fn on_event(
        &mut self,
        _input: &str,
        event: tremor_pipeline::Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        if let Some((tx, _)) = self.client.as_mut() {
            for v in event.value_iter() {
                if let Err(e) = tx.send(v.clone_static()).await {
                    error!("Send error: {}", e);
                }
            }
        }
        Ok(SinkReply::default())
    }

    fn auto_ack(&self) -> bool {
        true
    }
}
#[async_trait::async_trait()]
impl Source for Discord {
    #[allow(clippy::option_if_let_else)]
    async fn pull_data(&mut self, _pull_id: u64, _ctx: &SourceContext) -> Result<SourceReply> {
        if let Some((_, rx)) = self.client.as_ref() {
            match rx.try_recv() {
                Ok(data) => Ok(SourceReply::Structured {
                    origin_uri: self.origin_uri.clone(),
                    payload: (data, ()).into(),
                    stream: DEFAULT_STREAM_ID,
                    port: None,
                }),
                Err(TryRecvError::Closed) => Err(TryRecvError::Closed.into()),
                Err(TryRecvError::Empty) => Ok(SourceReply::Empty(10)),
            }
        } else {
            Err("No client".into())
        }
    }

    fn is_transactional(&self) -> bool {
        false
    }
}
#[async_trait::async_trait()]
impl Connector for Discord {
    fn is_structured(&self) -> bool {
        true
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        builder.spawn(self.clone(), source_context).map(Some)
    }

    /// create sink if we have a stdout or stderr stream
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let addr = builder.spawn(self.clone(), sink_context)?;
        Ok(Some(addr))
    }

    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        let token = self.config.token.clone();
        let (tx, rx) = bounded(QSIZE.load(Ordering::Relaxed));
        let (reply_tx, reply_rx) = bounded(QSIZE.load(Ordering::Relaxed));
        self.client = Some((reply_tx, rx));
        let client = Client::builder(&token).event_handler(Handler {
            tx,
            rx: reply_rx,
            is_loop_running: AtomicBool::from(false),
        });

        let client = if self.config.intents.is_empty() {
            client
        } else {
            let intents = self
                .config
                .intents
                .iter()
                .copied()
                .map(Intents::into)
                .fold(GatewayIntents::default(), |a, b| a | b);
            client.intents(intents)
        };
        let mut client = client
            .await
            .map_err(|e| Error::from(format!("Err discord creating client: {}", e)))?;
        task::spawn(async move { client.start().await });
        Ok(true)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
