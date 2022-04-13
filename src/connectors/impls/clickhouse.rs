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

use std::fmt::{self, Display, Formatter};

use crate::connectors::prelude::*;

use clickhouse_rs::{Block, Pool};

#[derive(Default, Debug)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "clickhouse".into()
    }

    async fn build(&self, alias: &str, raw_config: &ConnectorConfig) -> Result<Box<dyn Connector>> {
        // So something bad happens here and nobody know what or why.
        let config = raw_config
            .config
            .as_ref()
            .ok_or_else(|| ErrorKind::MissingConfiguration(alias.to_string()))?;

        let config = ClickhouseConfig::new(config)?;

        Ok(Box::new(Clickhouse { config }))
    }
}

pub struct Clickhouse {
    config: ClickhouseConfig,
}

#[async_trait::async_trait]
impl Connector for Clickhouse {
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let db_url = self.connection_url();
        let sink = ClickhouseSink { db_url, pool: None };
        builder.spawn(sink, sink_context).map(Some)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }
}

impl Clickhouse {
    fn connection_url(&self) -> String {
        let ClickhouseConfig {
            host,
            port,
            database,
            compression,
            ..
        } = &self.config;
        let port = port.unwrap_or(ClickhouseConfig::DEFAULT_PORT);
        let compression = compression.unwrap_or_default();

        format!("tcp://{host}:{port}/{database}?compression={compression}")
    }
}

#[derive(Deserialize)]
struct ClickhouseConfig {
    host: String,
    // TODO: use #[serde(default)] for optional fields.
    port: Option<u16>,
    compression: Option<Compression>,
    database: String,
    columns: Vec<Column>,
}

impl ClickhouseConfig {
    const DEFAULT_PORT: u16 = 9000;
}

impl ConfigImpl for ClickhouseConfig {}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Compression {
    None,
    Lz4,
}

impl Default for Compression {
    fn default() -> Compression {
        Compression::None
    }
}

impl Display for Compression {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Compression::None => "none",
            Compression::Lz4 => "lz4",
        }
        .fmt(f)
    }
}

#[derive(Deserialize)]
struct DatabaseColumns {
    #[serde(flatten)]
    columns: Vec<Column>,
}

#[derive(Deserialize)]
struct Column {
    name: String,
    #[serde(rename = "type")]
    type_: ClickHouseType,
}

#[derive(Deserialize)]
enum ClickHouseType {
    UInt8,
}

// Assumptions for now:
//   - db_url is fetched in the `create_sink` method,
//   - the pool is created in the `connect` method,
//   - the actual client is created in on-event
pub(crate) struct ClickhouseSink {
    db_url: String,
    pool: Option<Pool>,
}

#[async_trait::async_trait]
impl Sink for ClickhouseSink {
    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let pool = Pool::new(self.db_url.as_str());
        self.pool = Some(pool);

        Ok(true)
    }

    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        debug!("on_event is getting called");
        // TODO: is this the correct ErrorKind variant?
        let mut client = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::from(ErrorKind::NoClickHouseClientAvailable))?
            .get_handle()
            .await?;

        // TODO: fetch column data from the context?
        let columns = get_column_data();

        let block = columns
            .into_iter()
            .fold(Block::new(), |block, (name, ty)| match ty {
                ClickHouseType::UInt8 => add_uint8_column(block, name, &event),
            });

        client.insert("people", block.clone()).await?;
        debug!("{:#?}", block);

        Ok(SinkReply::ACK)
    }

    fn auto_ack(&self) -> bool {
        true
    }
}

fn get_column_data() -> Vec<(String, ClickHouseType)> {
    vec![("age".to_string(), ClickHouseType::UInt8)]
}

fn add_uint8_column(block: Block, name: String, event: &Event) -> Block {
    // TODO: better name
    let values = event
        .value_iter()
        .map(|row| {
            row.as_object()
                .unwrap()
                .get(name.as_str())
                .unwrap()
                .as_u8()
                .unwrap()
        })
        .collect::<Vec<_>>();

    block.add_column(name.as_str(), values)
}
