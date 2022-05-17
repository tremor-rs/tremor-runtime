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

mod conversion;

use std::fmt::{self, Display, Formatter};

use crate::connectors::{prelude::*, utils::url::ClickHouseDefaults};

use clickhouse_rs::{types::SqlType, Block, Pool};
use either::Either;

#[derive(Default, Debug)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "clickhouse".into()
    }

    async fn build(&self, alias: &str, raw_config: &ConnectorConfig) -> Result<Box<dyn Connector>> {
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

        let (event_min_size, event_max_size) = event.value_iter().size_hint();
        let block_size_estimate = event_max_size.unwrap_or(event_min_size);

        let mut block = Block::with_capacity(block_size_estimate);

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

        let object = input
            .as_object()
            .ok_or_else(|| Error::from(ErrorKind::ExpectedObjectEvent(input.value_type())))?;

        for (column_name, expected_type) in self.columns.iter() {
            let cell = object.get(column_name.as_str()).ok_or_else(|| {
                Error::from(ErrorKind::MissingEventColumn(column_name.to_string()))
            })?;

            let cell = conversion::convert_value(column_name.as_str(), cell, expected_type)?;

            rslt.push((column_name.clone(), cell));
        }

        Ok(rslt)
    }
}

// This is just a subset of the types actually supported by clickhouse_rs.
// It targets only the types that can be emitted by Tremor.
#[derive(Clone, Debug, Deserialize, PartialEq)]
enum DummySqlType {
    Array(Box<DummySqlType>),
    Nullable(Box<DummySqlType>),

    Int64,
    UInt64,
    String,

    #[serde(rename = "IPv4")]
    Ipv4,
    #[serde(rename = "IPv6")]
    Ipv6,
    #[serde(rename = "UUID")]
    Uuid,
    // TODO: rename to Ipv4 (considered as Rust idiomatic) and use
    // #[serde(rename(...))]
    IPv4,
    IPv6,
    UUID,
}

impl fmt::Display for DummySqlType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            DummySqlType::Array(inner) => write!(f, "Array({inner})"),
            DummySqlType::Nullable(inner) => write!(f, "Nullable({inner})"),
            DummySqlType::Int64 => write!(f, "Int64"),
            DummySqlType::UInt64 => write!(f, "UInt64"),
            DummySqlType::String => write!(f, "String"),
            DummySqlType::Ipv4 => write!(f, "IPv4"),
            DummySqlType::Ipv6 => write!(f, "IPv6"),
            DummySqlType::Uuid => write!(f, "UUID"),
        }
    }
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
            DummySqlType::Nullable(_) => {
                clickhouse_rs::types::Value::Nullable(Either::Right(Box::new(value)))
            }
            _ => value,
        }
    }
}

impl Into<&'static SqlType> for &DummySqlType {
    fn into(self) -> &'static SqlType {
        let non_static_type = match self {
            DummySqlType::Array(inner) => SqlType::Array(inner.as_ref().into()),
            DummySqlType::Nullable(inner) => SqlType::Nullable(inner.as_ref().into()),
            DummySqlType::Int64 => SqlType::Int64,
            DummySqlType::UInt64 => SqlType::UInt64,
            DummySqlType::String => SqlType::String,
            DummySqlType::Ipv4 => SqlType::Ipv4,
            DummySqlType::Ipv6 => SqlType::Ipv6,
            DummySqlType::Uuid => SqlType::Uuid,
        };

        // This sounds like pure magic - and it actually is.
        //
        // There's an implementation of Into<&'static SqlType> for &SqlType.
        // Under the hood, it returns either constant values for non-nested,
        // or store them in a global type pool which lives for the whole program
        // lifetime.
        non_static_type.into()
    }
}
