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

use crate::connectors::{prelude::*, utils::url::ClickHouseDefaults};

use clickhouse_rs::{Block, Pool};
use simd_json::StaticNode;

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
        let columns = self
            .config
            .columns
            .iter()
            .map(|Column { name, type_ }| (name.clone(), type_.clone()))
            .collect();

        let sink = ClickhouseSink {
            db_url,
            pool: None,
            columns,
        };
        builder.spawn(sink, sink_context).map(Some)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }
}

impl Clickhouse {
    fn connection_url(&self) -> String {
        let url = self.config.url.as_str();
        let compression = self.config.compression.unwrap_or_default();

        format!("{url}?compression={compression}")
    }
}

#[derive(Deserialize)]
struct ClickhouseConfig {
    url: Url<ClickHouseDefaults>,
    compression: Option<Compression>,
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
struct Column {
    name: String,
    #[serde(rename = "type")]
    type_: DummySqlType,
}

pub(crate) struct ClickhouseSink {
    db_url: String,
    pool: Option<Pool>,
    columns: Vec<(String, DummySqlType)>,
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
        let mut client = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::from(ErrorKind::NoClickHouseClientAvailable))?
            .get_handle()
            .await?;

        // TODO: initialize with correct size to avoid reallocations.
        let mut block = Block::new();

        for value in event.value_iter() {
            let row = self.clickhouse_row_of(value)?;
            block.push(row)?;
        }

        debug!("Inserting block:{:#?}", block);
        client.insert("people", block).await?;

        Ok(SinkReply::NONE)
    }

    fn auto_ack(&self) -> bool {
        true
    }
}

impl ClickhouseSink {
    fn clickhouse_row_of(
        &self,
        input: &tremor_value::Value,
    ) -> Result<Vec<(String, clickhouse_rs::types::Value)>> {
        let mut rslt = Vec::new();

        let object = input.as_object().unwrap();

        for (column_name, expected_type) in self.columns.iter() {
            let cell = object.get(column_name.as_str()).unwrap();
            let cell = clickhouse_value_of(cell, expected_type)?;

            rslt.push((column_name.clone(), cell));
        }

        Ok(rslt)
    }
}

// This is just a subset of the types actually supported by clickhouse_rs.
// It targets only the types that can be emitted by Tremor.
//
// Note: null is in a weird half-supported/half-unsupported on the clickhouse_rs
// crate. Due to its special properties, it has nonetheless been added here, so
// that most of the logic is already written once it is available.
#[derive(Clone, Debug, Deserialize, PartialEq)]
enum DummySqlType {
    Array(Box<DummySqlType>),
    Nullable(Box<DummySqlType>),

    Int64,
    UInt64,
    String,
}

impl DummySqlType {
    fn as_non_nullable(&self) -> &DummySqlType {
        match self {
            DummySqlType::Nullable(inner_type) => inner_type.as_ref(),
            non_nullable => non_nullable,
        }
    }

    fn wrap_if_nullable(&self, value: clickhouse_rs::types::Value) -> clickhouse_rs::types::Value {
        match self {
            DummySqlType::Nullable(_) => panic!("Nullable values can not be constructed :("),
            _ => value,
        }
    }
}

fn clickhouse_value_of(
    cell: &Value,
    expected_type: &DummySqlType,
) -> Result<clickhouse_rs::types::Value> {
    use clickhouse_rs::types;

    match cell {
        Value::Static(value) => {
            if matches!(
                (value, expected_type),
                (StaticNode::Null, DummySqlType::Nullable(_))
            ) {
                // Null can be of any type, as long as it is allowed by the
                // schema.

                // The situation is quite sad. The problem is that
                // clickhouse_rs::types::Value::Nullable wraps an Either [1],
                // which defined in clickhouse itself and is not publicly
                // available [2]. As an interim solution, we will panic. A
                // solution could be to open a pull-request to clickhouse-rs,
                // changing its visibility from pub(crate) to pub.
                //
                // [1]: https://docs.rs/clickhouse-rs/1.0.0-alpha.1/clickhouse_rs/types/enum.Value.html#variant.Nullable
                // [2]: https://docs.rs/clickhouse-rs/0.1.21/src/clickhouse_rs/types/either.rs.html#4
                panic!("Null value can't be constructed for now :(");
            }

            let value_as_non_null = match (value, expected_type.as_non_nullable()) {
                (StaticNode::U64(v), DummySqlType::UInt64) => types::Value::UInt64(*v),

                _ => todo!(),
            };

            Ok(expected_type.wrap_if_nullable(value_as_non_null))
        }

        _ => todo!(),
    }
}
