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

// #![cfg_attr(coverage, no_coverage)]
use std::{sync::Arc, time::Duration};

use crate::connectors::prelude::*;
use async_std::{sync::Mutex, task};

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
    async fn build_cfg(
        &self,
        _: &Alias,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
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
    }
}

struct WalSource {
    origin_uri: EventOriginUri,
    wal: Arc<Mutex<qwal::Wal>>,
}

struct Payload(Event);

impl qwal::Entry for Payload {
    type Output = Event;
    type Error = simd_json::Error;

    fn serialize(self) -> std::result::Result<Vec<u8>, Self::Error> {
        Ok(self.0.json_vec()?)
    }

    fn deserialize(mut data: Vec<u8>) -> std::result::Result<Self::Output, Self::Error> {
        Event::from_slice(&mut data)
    }
}

#[async_trait::async_trait]
impl Source for WalSource {
    async fn pull_data(&mut self, pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        // This is a busy loop until we get data to avoid hogging the cpu
        // TODO: improve this by adding  notifyer on write
        loop {
            if let Some((id, event)) = self.wal.lock().await.pop::<Payload>().await? {
                // the wal is creating its own ids, we take over here
                *pull_id = id;
                return Ok(SourceReply::Structured {
                    origin_uri: self.origin_uri.clone(),
                    payload: event.data,
                    stream: DEFAULT_STREAM_ID,
                    port: None,
                });
            }
            task::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn ack(&mut self, _stream_id: u64, pull_id: u64, _ctx: &SourceContext) -> Result<()> {
        self.wal.lock().await.ack(pull_id).await?;
        Ok(())
    }

    async fn fail(&mut self, _stream_id: u64, _pull_id: u64, _ctx: &SourceContext) -> Result<()> {
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
        true
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
        Ok(SinkReply::NONE)
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

    async fn on_stop(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        self.wal.lock().await.preserve_ack().await?;
        Ok(())
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }
}
