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

//! > `ClickHouse` is an open-source [column-oriented DBMS][co-dbms] (columnar database management system) for [online analytical processing][olap] (OLAP) that allows users to generate analytical reports using SQL queries in real-time.
//!
//! Source: [Wikipedia][wikipedia-ch].
//!
//! [co-dbms]: https://en.wikipedia.org/wiki/Column-oriented_DBMS
//! [olap]: https://en.wikipedia.org/wiki/Online_analytical_processing
//! [wikipedia-ch]: https://en.wikipedia.org/wiki/ClickHouse
//!
//! # The `clickhouse` Connector
//!
//! The `clickhouse` collector aims integrate the [ClickHouse] database in Tremor. It has been tested with `ClickHouse` `v22.3-lts`.
//!
//! [Clickhouse]: https://clickhouse.com/
//!
//! This connector is a sink-only connector.
//!
//! ## Configuration
//!
//! ```troy
//!   # File: config.troy
//!   define connector clickhouse from clickhouse
//!   with
//!     config = {
//!       # The hostname of the database node
//!       "url": "localhost",
//!
//!       # Compression
//!       "compression": "lz4",
//!
//!       # The database to write data to
//!       #
//!       # This field is not mandatory.
//!       "database": "tremor_testing",
//!
//!       # The table to write data to
//!       "table": "people",
//!       "columns": [
//!         # All the table's columns
//!         {
//!           # The column name
//!           "name": "name",
//!           # Its type
//!           "type": "String",
//!         }
//!         {
//!           "name": "age",
//!           "type": "UInt8",
//!         },
//!       ]
//!     }
//!   end;
//! ```
//!
//! ### Compression
//!
//! Specifying a compression method is optional. This setting currently supports [`lz4`] and `none` (no compression). If no value is specified, then no compression is performed.
//!
//! [`lz4`]: https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)
//!
//!
//! ### Database
//!
//! Specifying a database is optional. If no database name is supplied, then the `default` database is used.
//!
//! ## Value conversion
//!
//! The following sections show how Tremor values are transformed into `ClickHouse` values. As numerous casts can be performed, the conversions are grouped by output type.
//!
//! Any type which is not documented in the following sections is considered as unsupported.
//!
//! ### [`String`][CString]
//!
//! [CString]: https://clickhouse.com/docs/en/sql-reference/data-types/string
//!
//! `ClickHouse` `String`s values can be created from any Tremor string.
//!
//! #### Example
//!
//! The following Tremor values represent a valid `ClickHouse` string:
//!
//! ```troy
//! "Hello, world!"
//! "Grace Hopper"
//! ```
//!
//!
//! ### Integers ([`UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32`, `Int64`][CNumerals])
//!
//! [CNumerals]: https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
//!
//! The following table shows the valid ranges where each numerical type can be created:
//!
//! | Type     | Lower Bound (inclusive) | Upper Bound (inclusive) |
//! | -------- | ----------------------- | ----------------------- |
//! | `UInt8`  | 0                       | 255                     |
//! | `UInt16` | 0                       | 65535                   |
//! | `UInt32` | 0                       | 4294967295              |
//! | `UInt64` | 0                       | 18446744073709551615    |
//! | `Int8`   | - 128                   | 127                     |
//! | `Int16`  | - 32768                 | 32767                   |
//! | `Int32`  | - 2147483648            | 2147483647              |
//! | `Int64`  | - 9223372036854775808   | 9223372036854775807     |
//!
//!
//! #### Example
//!
//! The following Tremor values can represent any integer type:
//!
//! ```troy
//! 42
//! 101
//! 13
//! 37
//! ```
//!
//! ### [`DateTime`][CDateTime]
//!
//! [CDateTime]: https://clickhouse.com/docs/en/sql-reference/data-types/datetime
//!
//! `DateTime`s can be created from any non-negative Tremor integer. It represents the number of seconds elapsed since January 1st of 1970 at 00:00:00, in UTC timezone. It is encoded as a 32 bit unsigned integer.
//!
//! Storing a `DateTime` in 32-bit format is likely to lead to a [Year 2038 problem][year-2038] problem. It is advised to use `DateTime64(0)`, as described below.
//!
//! [year-2038]: https://en.wikipedia.org/wiki/Year_2038_problem
//!
//!
//! #### Example
//!
//! The following Tremor values represent valid `ClickHouse` `DateTime`:
//!
//! ```troy
//! 1634400000
//! 954232020
//! ```
//!
//! ### [`DateTime64`][CDateTime64]
//!
//! [CDateTime64]: https://clickhouse.com/docs/en/sql-reference/data-types/datetime64
//!
//! `ClickHouse` `DateTime64` type offers various precisions. Tremor supports only four precisions:
//!   - `DateTime64(0)`, second-precise,
//!   - `DateTime64(3)`, millisecond-precise,
//!   - `DateTime64(6)`, microsecond-precise,
//!   - `DateTime64(9)`, nanosecond-precise.
//!
//! `DateTime64(0)` (respectively `DateTime64(3)`, `DateTime64(6)` and `DateTime64(9)`) can be created from any Tremor integer representing the number of seconds (respectively milliseconds, microseconds and nanoseconds) elapsed since January 1st of 1970 at 00:00:00, in UTC timezone.
//!
//!
//! #### Example
//!
//! The following Tremor values represent valid `ClickHouse` `DateTime64(0, Etc/UTC)` values.
//!
//! ```troy
//! 1634400000
//! 954232020
//! ```
//!
//! ### [`IPv4`][CIPv4]
//!
//! [CIPv4]: https://clickhouse.com/docs/en/sql-reference/data-types/domains/ipv4/
//!
//! `ClickHouse` `IPv4`s can be created from strings or from arrays of octets.
//!
//! A `ClickHouse` `IPv4` can be created from any 4-elements long array of integers in the [0 - 255] range.
//!
//! A `ClickHouse` `IPv4` can be created from any string composed of four octets written as decimal and separated by dots, as defined in the [RFC 6943, section 3.1.1][rfc6943-1-1-1]
//!
//! [rfc6943-1-1-1]: https://datatracker.ietf.org/doc/html/rfc6943#section-3.1.1
//!
//!
//! #### Example
//!
//! The following Tremor values represent valid `ClickHouse` `IPv4` values:
//!
//! ```troy
//! "192.168.1.1"
//! [192, 168, 1, 1]
//! ```
//!
//! ### [`IPv6`][CIPv6]
//!
//! [CIPv6]: https://clickhouse.com/docs/en/sql-reference/data-types/domains/ipv6/
//!
//! `ClickHouse` `IPv6` values can be created from strings or from arrays of octets.
//!
//! A `ClickHouse` `IPv6` can be created from any 16-elements long array of integers in the [0 - 255 range].
//!
//! A `ClickHouse` `IPv6` can be created from any [RFC 5952][rfc-5952]-compliant string.
//!
//! [rfc-5952]: https://datatracker.ietf.org/doc/html/rfc5952
//!
//!
//! #### Example
//!
//! The following Tremor values represent valid `ClickHouse` `IPv6` values:
//!
//! ```troy
//! "FE80:0000:0000:0000:0202:B3FF:FE1E:8329"
//! [254, 128, 0, 0, 0, 0, 0, 0, 2, 2, 179, 255, 254, 30, 131, 41]
//! ```
//!
//! ### Nullable
//!
//! Any column type can be marked as nullable. It allows to make optional the key-value pair for the events that are sent through the sink.
//!
//! A column whose type is a nullable `UInt8` can be declared as follows:
//!
//! ```json
//! {
//!     "name": "column_name",
//!     "type": { "Nullable": "UInt8" }
//! }
//! ```
//!
//!
//! #### Example
//!
//! The following Tremor values represent valid `ClickHouse` `UInt8` values:
//!
//! ```troy
//! 101
//! null
//! ```
//!
//! ### Array
//!
//! Arrays can store any number of elements.
//!
//! A column whose type is an array of `UInt8` can be declared as follows:
//!
//! ```json
//! {
//!     "name": "column_name",
//!     "type": { "Array": "UInt8" }
//! }
//! ```
//!
//! #### Example
//!
//! The following Tremor values represent valid `ClickHouse` `Array(UInt8)` values:
//!
//! ```troy
//! [101, 42, true]
//! [1, 2, false]
//! ```
//!
//! ### [`Uuid`][CUuid]
//!
//! [CUuid]: https://clickhouse.com/docs/en/sql-reference/data-types/uuid
//!
//! An UUID can be created either from an array of integers or from a string.
//!
//! In order to form a valid UUID, an array must have exactly 16 elements and every
//! element must be an integer in the [0, 255] range.
//!
//! An string respecting the [RFC 4122][rfc4122] also represents a valid UUID.
//!
//! [rfc4122]: https://datatracker.ietf.org/doc/html/rfc4122
//!
//! #### Example
//!
//! The following Tremor values represent valid `ClickHouse` `Uuid` values:
//!
//! ```troy
//! "123e4567-e89b-12d3-a456-426614174000"
//! [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
//! ```

mod conversion;

use std::fmt::{self, Display, Formatter};

use crate::connectors::prelude::*;

use clickhouse_rs::{
    errors::Error as CError,
    types::{DateTimeType, SqlType},
    Block, ClientHandle, Pool,
};

#[derive(Default, Debug)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "clickhouse".into()
    }

    async fn build_cfg(
        &self,
        _alias: &Alias,
        _config: &ConnectorConfig,
        connector_config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = ClickhouseConfig::new(connector_config)?;

        Ok(Box::new(Clickhouse { config }))
    }
}

pub(crate) struct Clickhouse {
    config: ClickhouseConfig,
}

#[async_trait::async_trait]
impl Connector for Clickhouse {
    async fn create_sink(
        &mut self,
        ctx: SinkContext,
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
        Ok(Some(builder.spawn(sink, ctx)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }
}

impl Clickhouse {
    fn connection_url(&self) -> String {
        let host = self.config.url.as_str();

        let path = self.config.database.as_deref().unwrap_or_default();

        let compression = self.config.compression;

        format!("{host}/{path}?compression={compression}")
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClickhouseConfig {
    url: Url<ClickHouseDefaults>,
    #[serde(default)]
    compression: Compression,
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

#[derive(Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum Compression {
    #[default]
    None,
    Lz4,
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
    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let pool = Pool::new(self.db_url.as_str());
        let handle = match pool.get_handle().await {
            Ok(handle) => handle,
            Err(e) => {
                return match e {
                    CError::Driver(_) | CError::Io(_) | CError::Connection(_) => {
                        ctx.notifier().connection_lost().await?;
                        Ok(false)
                    }
                    _ => Err(Error::from(e)),
                }
            }
        };

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
            const NULL: &Value = &Value::const_null();
            let cell = object.get(column_name.as_str()).unwrap_or(NULL);

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
                SqlType::DateTime(DateTimeType::DateTime64(0, clickhouse_chrono_tz::Tz::UTC))
            }
            DummySqlType::DateTime64Millis => {
                SqlType::DateTime(DateTimeType::DateTime64(3, clickhouse_chrono_tz::Tz::UTC))
            }
            DummySqlType::DateTime64Micros => {
                SqlType::DateTime(DateTimeType::DateTime64(6, clickhouse_chrono_tz::Tz::UTC))
            }
            DummySqlType::DateTime64Nanos => {
                SqlType::DateTime(DateTimeType::DateTime64(9, clickhouse_chrono_tz::Tz::UTC))
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

#[cfg(test)]
mod tests {
    use super::*;

    mod dummy_sql_type_display {
        use super::*;

        macro_rules! test_display {
            ($test_name:ident :: $value:expr => $displayed:literal $(,)? ) => {
                #[test]
                fn $test_name() {
                    let ty: DummySqlType = $value;
                    let left = ty.to_string();
                    let right: &str = $displayed;

                    assert_eq!(left, right);
                }
            };

            (
                $(
                    $test_name:ident :: $value:expr => $displayed:literal
                ),* $(,)?
            ) => {
                $(
                    test_display! { $test_name :: $value => $displayed }
                )*
            };
        }

        use DummySqlType::{
            Array, DateTime, DateTime64Micros, DateTime64Millis, DateTime64Nanos, DateTime64Secs,
            Int16, Int32, Int64, Int8, Ipv4, Ipv6, String, UInt16, UInt32, UInt64, UInt8,
        };

        test_display! {
            array :: Array(Box::new(UInt8)) => "Array(UInt8)",

            nullable :: DummySqlType::Nullable(Box::new(DummySqlType::UInt8)) => "Nullable(UInt8)",

            uint8 :: UInt8  => "UInt8",

            uint16 :: UInt16 => "UInt16",

            uint32 :: UInt32 => "UInt32",

            uint64 :: UInt64 => "UInt64",

            int8 :: Int8 => "Int8",

            int16 :: Int16 => "Int16",

            int32 :: Int32 => "Int32",

            int64 :: Int64 => "Int64",

            string :: String => "String",

            ipv4 :: Ipv4 => "IPv4",

            ipv6 :: Ipv6 => "IPv6",

            datetime :: DateTime => "DateTime",

            datetime64_secs :: DateTime64Secs => "DateTime64(0)",

            datetime64_millis :: DateTime64Millis => "DateTime64(3)",

            datetime64_micros :: DateTime64Micros => "DateTime64(6)",

            datetime64_nanos :: DateTime64Nanos => "DateTime64(9)",
        }
    }

    mod dummy_sql_type_into_sql_type {
        use clickhouse_chrono_tz::Tz::UTC;

        use super::*;

        macro_rules! test_conversion {
            ($test_name:ident :: $left:expr => $right:expr) => {
                #[test]
                fn $test_name() {
                    // We don't want the enum variant import to pollute each
                    // other. As such, the expansion of left and right go in two
                    // separate blocks.

                    let init: DummySqlType = {
                        use DummySqlType::*;
                        $left
                    };
                    let left: &'static SqlType = (&init).into();

                    let right = {
                        use SqlType::*;
                        $right
                    };

                    assert_eq!(left, right);
                }
            };

            ( $( $test_name:ident :: $left:expr => $right:expr ),* $(,)? ) => {
                $(
                    test_conversion! {
                        $test_name :: $left => $right
                    }
                )*
            }
        }

        test_conversion! {
            array :: Array(Box::new(UInt8)) => &Array(&UInt8),

            nullable :: Nullable(Box::new(UInt8)) => &Nullable(&UInt8),

            uint8 :: UInt8 => &UInt8,

            uint16 :: UInt16 => &UInt16,

            uint32 :: UInt32 => &UInt32,

            uint64 :: UInt64 => &UInt64,

            int8 :: Int8 => &Int8,

            int16 :: Int16 => &Int16,

            int32 :: Int32 => &Int32,

            int64 :: Int64 => &Int64,

            string :: String => &String,

            ipv4 :: Ipv4 => &Ipv4,

            ipv6 :: Ipv6 => &Ipv6,

            uuid :: Uuid => &Uuid,

            datetime :: DateTime => &DateTime(DateTimeType::DateTime32),

            datetime64_secs :: DateTime64Secs => &DateTime(DateTimeType::DateTime64(0, UTC)),

            datetime64_millis :: DateTime64Millis => &DateTime(DateTimeType::DateTime64(3, UTC)),

            datetime64_micros :: DateTime64Micros  => &DateTime(DateTimeType::DateTime64(6, UTC)),

            datetime64_nanos :: DateTime64Nanos  => &DateTime(DateTimeType::DateTime64(9, UTC)),
        }
    }
}
