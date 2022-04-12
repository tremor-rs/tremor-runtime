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
use std::sync::Arc;

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
