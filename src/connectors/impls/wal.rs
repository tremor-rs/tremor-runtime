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

#![cfg(not(tarpaulin_include))]
use std::{collections::HashMap, sync::Arc};

use crate::connectors::prelude::*;
use async_std::sync::Mutex;

use simd_json_derive::{Deserialize, Serialize};

#[derive(serde::Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    dir: String,
    chunk_size: u64,
    max_chunks: usize,
}

impl ConfigImpl for Config {}

struct Wal {
    event_origin_uri: EventOriginUri,
    wal: Arc<Mutex<qwal::Wal>>,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "wal".into()
    }
    async fn from_config(
        &self,
        _id: &str,
        config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;

            let event_origin_uri = EventOriginUri {
                scheme: "tremor-kv".to_string(),
                host: "localhost".to_string(),
                port: None,
                path: config.dir.split('/').map(ToString::to_string).collect(),
            };
            let wal = qwal::Wal::open(&config.dir, config.chunk_size, config.max_chunks).await?;

            Ok(Box::new(Wal {
                event_origin_uri,
                wal: Arc::new(Mutex::new(wal)),
            }))
        } else {
            Err("[WAL Offramp] Offramp requires a config".into())
        }
    }
}

struct WalSource {
    origin_uri: EventOriginUri,
    wal: Arc<Mutex<qwal::Wal>>,
}

struct Payload(Event);

impl qwal::Entry for Payload {
    type Output = Event;

    fn serialize(self) -> Vec<u8> {
        self.0.json_vec().unwrap()
    }

    fn deserialize(mut data: Vec<u8>) -> Self::Output {
        Event::from_slice(&mut data).unwrap()
    }
}

#[async_trait::async_trait]
impl Source for WalSource {
    async fn pull_data(&mut self, pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        if let Some((id, event)) = self.wal.lock().await.pop::<Payload>().await? {
            // the wal is creating its own ids, we take over here
            *pull_id = id;
            Ok(SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                payload: event.data,
                stream: DEFAULT_STREAM_ID,
                port: None,
            })
        } else {
            Ok(SourceReply::Empty(DEFAULT_POLL_INTERVAL))
        }
    }

    async fn ack(&mut self, _stream_id: u64, pull_id: u64) -> Result<()> {
        self.wal.lock().await.ack(pull_id).await?;
        Ok(())
    }

    async fn fail(&mut self, _stream_id: u64, _pull_id: u64) -> Result<()> {
        self.wal.lock().await.revert().await?;
        Ok(())
    }

    fn is_transactional(&self) -> bool {
        true
    }

    fn asynchronous(&self) -> bool {
        false
    }
}

struct WalSink {
    wal: Arc<Mutex<qwal::Wal>>,
}

#[async_trait::async_trait]
impl Sink for WalSink {
    fn auto_ack(&self) -> bool {
        false
    }

    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        self.wal.lock().await.push(Payload(event)).await?;
        Ok(SinkReply::ACK)
    }
}

#[async_trait::async_trait]
impl Connector for Wal {
    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let s = WalSource {
            wal: self.wal.clone(),
            origin_uri: self.event_origin_uri.clone(),
        };
        builder.spawn(s, source_context).map(Some)
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let s = WalSink {
            wal: self.wal.clone(),
        };
        builder.spawn(s, sink_context).map(Some)
    }

    fn is_structured(&self) -> bool {
        true
    }

    async fn on_stop(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        self.wal.lock().await.preserve_ack().await?;
        Ok(())
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
#[derive(Debug)]
struct ThingyBuilder();
#[async_trait::async_trait]
impl ConnectorBuilder for ThingyBuilder {
    fn connector_type(&self) -> ConnectorType {
        "wal_thingy".into()
    }
    async fn from_config(
        &self,
        _id: &str,
        config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;

            let origin_uri = EventOriginUri {
                scheme: "tremor-kv".to_string(),
                host: "localhost".to_string(),
                port: None,
                path: config.dir.split('/').map(ToString::to_string).collect(),
            };

            Ok(Box::new(Arc::new(Mutex::new(WalThingy {
                origin_uri,
                config,
                wal: None,
                pull_id_map: HashMap::new(),
            }))))
        } else {
            Err("[WAL Offramp] Offramp requires a config".into())
        }
    }
}

/// FIXME: This is just as test playgorund
struct WalThingy {
    config: Config,
    origin_uri: EventOriginUri,
    wal: Option<qwal::Wal>,
    pull_id_map: HashMap<u64, u64>, // FIXME: this is terrible :(
}

impl WalThingy {
    async fn on_stop(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        if let Some(wal) = self.wal.as_mut() {
            wal.preserve_ack().await?;
        }
        Ok(())
    }

    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        if self.wal.is_none() {
            self.wal = Some(
                qwal::Wal::open(
                    &self.config.dir,
                    self.config.chunk_size,
                    self.config.max_chunks,
                )
                .await?,
            );
        }
        Ok(true)
    }

    async fn pull_data(&mut self, pull_id: u64, _ctx: &SourceContext) -> Result<SourceReply> {
        if let Some(wal) = self.wal.as_mut() {
            if let Some((id, event)) = wal.pop::<Payload>().await? {
                // FIXME: this is a dirty hack untill we can define the pull_id / event ID
                // if this is the only place we might not want to change this however
                self.pull_id_map.insert(pull_id, id);
                Ok(SourceReply::Structured {
                    origin_uri: self.origin_uri.clone(),
                    payload: event.data,
                    stream: DEFAULT_STREAM_ID,
                    port: None,
                })
            } else {
                Ok(SourceReply::Empty(DEFAULT_POLL_INTERVAL))
            }
        } else {
            Ok(SourceReply::Empty(DEFAULT_POLL_INTERVAL))
        }
    }

    async fn ack(&mut self, _stream_id: u64, pull_id: u64) -> Result<()> {
        // FIXME: this is a dirty hack until we can define the pull_id for a connector
        if let Some((id, wal)) = self.pull_id_map.remove(&pull_id).zip(self.wal.as_mut()) {
            wal.ack(id).await?;
        }
        Ok(())
    }

    async fn fail(&mut self, _stream_id: u64, _pull_id: u64) -> Result<()> {
        if let Some(wal) = self.wal.as_mut() {
            wal.revert().await?;
        }
        Ok(())
    }

    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        if let Some(wal) = self.wal.as_mut() {
            wal.push(Payload(event)).await?;
        }
        Ok(SinkReply::ACK)
    }
}

#[async_trait::async_trait]
impl Source for Arc<Mutex<WalThingy>> {
    async fn pull_data(&mut self, pull_id: &mut u64, ctx: &SourceContext) -> Result<SourceReply> {
        self.lock().await.pull_data(*pull_id, ctx).await
    }

    async fn ack(&mut self, stream_id: u64, pull_id: u64) -> Result<()> {
        self.lock().await.ack(stream_id, pull_id).await
    }

    async fn fail(&mut self, stream_id: u64, pull_id: u64) -> Result<()> {
        self.lock().await.fail(stream_id, pull_id).await
    }

    fn is_transactional(&self) -> bool {
        true
    }

    fn asynchronous(&self) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl Sink for Arc<Mutex<WalThingy>> {
    fn auto_ack(&self) -> bool {
        false
    }

    async fn on_event(
        &mut self,
        input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        start: u64,
    ) -> Result<SinkReply> {
        self.lock()
            .await
            .on_event(input, event, ctx, serializer, start)
            .await
    }
}

#[async_trait::async_trait]
impl Connector for Arc<Mutex<WalThingy>> {
    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        builder.spawn(self.clone(), source_context).map(Some)
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        builder.spawn(self.clone(), sink_context).map(Some)
    }

    fn is_structured(&self) -> bool {
        true
    }

    async fn on_stop(&mut self, ctx: &ConnectorContext) -> Result<()> {
        self.lock().await.on_stop(ctx).await
    }

    async fn connect(&mut self, ctx: &ConnectorContext, attempt: &Attempt) -> Result<bool> {
        self.lock().await.connect(ctx, attempt).await
    }
    fn default_codec(&self) -> &str {
        "json"
    }
}
