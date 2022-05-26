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

use crate::connectors::prelude::*;

use clickhouse_rs::{
    types::{DateTimeType, SqlType},
    Block, ClientHandle, Pool,
};
use simd_json::StaticNode;

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
        let table = self.config.table.clone();

        let sink = ClickhouseSink {
            db_url,
            handle: None,
            table,
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
        let host = self.config.url.as_str();

        let path = self.config.database.as_deref().unwrap_or_default();

        let compression = self.config.compression.unwrap_or_default();

        format!("{host}/{path}?compression={compression}")
    }
}

#[derive(Deserialize)]
struct ClickhouseConfig {
    url: Url<ClickHouseDefaults>,
    compression: Option<Compression>,
    database: Option<String>,
    table: String,
    columns: Vec<Column>,
}

pub(crate) struct ClickHouseDefaults;
impl Defaults for ClickHouseDefaults {
    const SCHEME: &'static str = "tcp";
    const HOST: &'static str = "localhost";
    const PORT: u16 = 9000;
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
    handle: Option<ClientHandle>,
    table: String,
    columns: Vec<(String, DummySqlType)>,
}

#[async_trait::async_trait]
impl Sink for ClickhouseSink {
    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let pool = Pool::new(self.db_url.as_str());
        let handle = pool.get_handle().await?;

        self.handle = Some(handle);

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
        let handle = self
            .handle
            .as_mut()
            .ok_or_else(|| Error::from(ErrorKind::NoClickHouseClientAvailable))?;

        let mut block = Block::with_capacity(event.len());

        for value in event.value_iter() {
            let row = Self::clickhouse_row_of(&self.columns, value)?;
            block.push(row)?;
        }

        debug!("Inserting block:{:#?}", block);
        handle.insert(&self.table, block).await?;

        Ok(SinkReply::NONE)
    }

    fn auto_ack(&self) -> bool {
        true
    }
}

impl ClickhouseSink {
    fn clickhouse_row_of(
        columns: &[(String, DummySqlType)],
        input: &tremor_value::Value,
    ) -> Result<Vec<(String, clickhouse_rs::types::Value)>> {
        let mut rslt = Vec::new();

        let object = input
            .as_object()
            .ok_or_else(|| Error::from(ErrorKind::ExpectedObjectEvent(input.value_type())))?;

        for (column_name, expected_type) in columns.iter() {
            // If the value is not present, then we can replace it by null.
            const NULL: Value = Value::Static(StaticNode::Null);
            let cell = object.get(column_name.as_str()).unwrap_or(&NULL);

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

    UInt8,
    UInt16,
    UInt32,
    UInt64,

    Int8,
    Int16,
    Int32,
    Int64,

    String,

    #[serde(rename = "IPv4")]
    Ipv4,
    #[serde(rename = "IPv6")]
    Ipv6,
    #[serde(rename = "UUID")]
    Uuid,

    DateTime,

    // The following three variants allow us to get more control of how much
    // precision is needed.
    #[serde(rename = "DateTime64(0)")]
    DateTime64Secs,
    #[serde(rename = "DateTime64(3)")]
    DateTime64Millis,
    #[serde(rename = "DateTime64(6)")]
    DateTime64Micros,
    #[serde(rename = "DateTime64(9)")]
    DateTime64Nanos,
}

impl fmt::Display for DummySqlType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            DummySqlType::Array(inner) => write!(f, "Array({inner})"),
            DummySqlType::Nullable(inner) => write!(f, "Nullable({inner})"),

            DummySqlType::UInt8 => write!(f, "UInt8"),
            DummySqlType::UInt16 => write!(f, "UInt16"),
            DummySqlType::UInt32 => write!(f, "UInt32"),
            DummySqlType::UInt64 => write!(f, "UInt64"),

            DummySqlType::Int8 => write!(f, "Int8"),
            DummySqlType::Int16 => write!(f, "Int16"),
            DummySqlType::Int32 => write!(f, "Int32"),
            DummySqlType::Int64 => write!(f, "Int64"),

            DummySqlType::String => write!(f, "String"),
            DummySqlType::Ipv4 => write!(f, "IPv4"),
            DummySqlType::Ipv6 => write!(f, "IPv6"),
            DummySqlType::Uuid => write!(f, "UUID"),
            DummySqlType::DateTime => write!(f, "DateTime"),

            DummySqlType::DateTime64Secs => write!(f, "DateTime64(0)"),
            DummySqlType::DateTime64Millis => write!(f, "DateTime64(3)"),
            DummySqlType::DateTime64Micros => write!(f, "DateTime64(6)"),
            DummySqlType::DateTime64Nanos => write!(f, "DateTime64(9)"),
        }
    }
}

impl From<&DummySqlType> for &'static SqlType {
    fn from(ty: &DummySqlType) -> &'static SqlType {
        let non_static_type = match ty {
            DummySqlType::Array(inner) => SqlType::Array(inner.as_ref().into()),
            DummySqlType::Nullable(inner) => SqlType::Nullable(inner.as_ref().into()),

            DummySqlType::UInt8 => SqlType::UInt8,
            DummySqlType::UInt16 => SqlType::UInt16,
            DummySqlType::UInt32 => SqlType::UInt32,
            DummySqlType::UInt64 => SqlType::UInt64,

            DummySqlType::Int8 => SqlType::Int8,
            DummySqlType::Int16 => SqlType::Int16,
            DummySqlType::Int32 => SqlType::Int32,
            DummySqlType::Int64 => SqlType::Int64,

            DummySqlType::String => SqlType::String,
            DummySqlType::Ipv4 => SqlType::Ipv4,
            DummySqlType::Ipv6 => SqlType::Ipv6,
            DummySqlType::Uuid => SqlType::Uuid,
            DummySqlType::DateTime => SqlType::DateTime(DateTimeType::DateTime32),
            DummySqlType::DateTime64Secs => {
                SqlType::DateTime(DateTimeType::DateTime64(0, chrono_tz::Tz::UTC))
            }
            DummySqlType::DateTime64Millis => {
                SqlType::DateTime(DateTimeType::DateTime64(3, chrono_tz::Tz::UTC))
            }
            DummySqlType::DateTime64Micros => {
                SqlType::DateTime(DateTimeType::DateTime64(6, chrono_tz::Tz::UTC))
            }
            DummySqlType::DateTime64Nanos => {
                SqlType::DateTime(DateTimeType::DateTime64(9, chrono_tz::Tz::UTC))
            }
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
