// Copyright 2018-2020, Wayfair GmbH
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

//! # `PostgreSQL` Ramp
//!
//! Implements an intermediate representation
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use bytes;
use bytes::buf::{BufMut, BufMutExt};
use chrono::prelude::*;
use postgres::types::to_sql_checked;
use postgres_protocol;
use simd_json::prelude::*;
use std::time::SystemTime;

#[derive(Debug)]
pub struct Record<'a> {
    pub t: postgres::types::Type,
    pub value: &'a simd_json::BorrowedValue<'a>,
    pub name: String,
}

impl postgres::types::ToSql for Record<'_> {
    fn to_sql(
        &self,
        type_: &postgres::types::Type,
        w: &mut bytes::BytesMut,
    ) -> std::result::Result<postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    {
        if self.value.is_null() {
            return Ok(postgres::types::IsNull::Yes);
        }

        match self.t {
            postgres::types::Type::BOOL => {
                match self.value.as_bool() {
                    Some(v) => postgres_protocol::types::bool_to_sql(v, w),
                    None => {
                        return Err(
                            format!("Could not serialize {} into {}", self.name, "bool").into()
                        )
                    }
                };
            }
            postgres::types::Type::BPCHAR
            | postgres::types::Type::NAME
            | postgres::types::Type::TEXT
            | postgres::types::Type::UNKNOWN
            | postgres::types::Type::VARCHAR => {
                match self.value.as_str() {
                    Some(v) => postgres_protocol::types::text_to_sql(v, w),
                    None => {
                        return Err(
                            format!("Could not serialize {} into {}", self.name, "text").into()
                        )
                    }
                };
            }
            postgres::types::Type::INT2 => {
                let val = match self.value.as_i16() {
                    Some(v) => v,
                    None => {
                        return Err(
                            format!("Could not serialize {} into {}", self.name, "i16").into()
                        )
                    }
                };

                postgres_protocol::types::int2_to_sql(val, w)
            }
            postgres::types::Type::INT4 => {
                let val = match self.value.as_i32() {
                    Some(v) => v,
                    None => return Err("Could not serialize".into()),
                };

                postgres_protocol::types::int4_to_sql(val, w)
            }
            postgres::types::Type::INT8 => {
                let val = match self.value.as_i64() {
                    Some(v) => v,
                    None => return Err("Could not serialize".into()),
                };

                postgres_protocol::types::int8_to_sql(val, w)
            }
            postgres::types::Type::JSON => {
                let val = match self.value.as_str() {
                    Some(v) => v,
                    None => "",
                };

                serde_json::ser::to_writer(w.writer(), &val)?;
            }
            postgres::types::Type::JSONB => {
                let val = match self.value.as_str() {
                    Some(v) => v,
                    None => "",
                };
                w.put_u8(1);

                serde_json::ser::to_writer(w.writer(), &val)?;
            }
            postgres::types::Type::TIMESTAMPTZ => {
                let val = match self.value.as_str() {
                    Some(v) => v,
                    None => "",
                };

                let dt = match DateTime::parse_from_str(val, "%Y-%m-%d %H:%M:%S%.6f %:z") {
                    Ok(v) => v,
                    Err(e) => return Err(format!("error parsing timestamptz: {}", e).into()),
                };
                dt.with_timezone(&Utc).to_sql(type_, w)?;
            }
            postgres::types::Type::TIMESTAMP => {
                let val = match self.value.as_str() {
                    Some(v) => v,
                    None => "",
                };

                let base: NaiveDateTime = NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0);
                let dt = match NaiveDateTime::parse_from_str(val, "%Y-%m-%d %H:%M:%S%.6f %:z") {
                    Ok(v) => v,
                    Err(e) => return Err(format!("error parsing timestamptz: {}", e).into()),
                };
                let time = match dt.signed_duration_since(base).num_microseconds() {
                    Some(time) => time,
                    None => return Err("value too large".into()),
                };

                postgres_protocol::types::timestamp_to_sql(time, w)
            }
            _ => return Err(format!("Not supported type: {}", self.t).into()),
        };

        Ok(postgres::types::IsNull::No)
    }

    // NOTE: cannot easily tell which types are accepted due to trait not accepting self
    // which holds t context. This renders the function not as useful
    fn accepts(ty: &postgres::types::Type) -> bool {
        match ty {
            &postgres::types::Type::BOOL
            | &postgres::types::Type::CHAR
            | &postgres::types::Type::TEXT
            | &postgres::types::Type::NAME
            | &postgres::types::Type::INT2
            | &postgres::types::Type::INT4
            | &postgres::types::Type::INT8
            | &postgres::types::Type::JSON
            | &postgres::types::Type::JSONB
            | &postgres::types::Type::TIMESTAMP
            | &postgres::types::Type::TIMESTAMPTZ
            | &postgres::types::Type::UNKNOWN
            | &postgres::types::Type::VARCHAR => true,
            _ => false,
        }
    }

    to_sql_checked!();
}

pub fn json_to_record<'a>(
    json: &'a simd_json::BorrowedValue<'a>,
) -> std::result::Result<Record, Box<dyn std::error::Error + Sync + Send>> {
    let field_type = match json["fieldType"].as_str() {
        Some(v) => v,
        None => return Err("error getting fieldType".into()),
    };

    let t: postgres::types::Type = match field_type {
        "VARCHAR" => postgres::types::Type::VARCHAR,
        "UNKNOWN" => postgres::types::Type::UNKNOWN,
        "BOOL" => postgres::types::Type::BOOL,
        "CHAR" => postgres::types::Type::CHAR,
        "TEXT" => postgres::types::Type::TEXT,
        "NAME" => postgres::types::Type::NAME,
        "INT8" => postgres::types::Type::INT8,
        "INT2" => postgres::types::Type::INT2,
        "INT4" => postgres::types::Type::INT4,
        "JSON" => postgres::types::Type::JSON,
        "JSONB" => postgres::types::Type::JSONB,
        "TIMESTAMPTZ" => postgres::types::Type::TIMESTAMPTZ,
        "TIMESTAMP" => postgres::types::Type::TIMESTAMP,
        _ => return Err("intermediate representation does not support field type".into()),
    };

    let n: String = json["name"].to_string();

    Ok(Record {
        t,
        value: &json["value"],
        name: n,
    })
}

pub fn row_to_json(
    row: &postgres::row::Row,
) -> std::result::Result<serde_json::Value, Box<dyn std::error::Error + Sync + Send>> {
    let mut json = serde_json::Map::new();

    for (cid, col) in row.columns().iter().enumerate() {
        let t: &'static str;
        let mut obj = serde_json::Map::new();

        let v: serde_json::Value = match *col.type_() {
            postgres::types::Type::BOOL => {
                t = "BOOL";
                serde_json::Value::Bool(row.get(cid))
            }
            postgres::types::Type::CHAR => {
                t = "CHAR";
                serde_json::Value::String(row.get(cid))
            }
            postgres::types::Type::INT8 => {
                t = "INT8";
                let val: i64 = row.get(cid);
                serde_json::Value::Number(serde_json::Number::from(val))
            }
            postgres::types::Type::INT2 => {
                t = "INT2";
                let val: i16 = row.get(cid);
                serde_json::Value::Number(serde_json::Number::from(val))
            }
            postgres::types::Type::INT4 => {
                t = "INT4";
                let val: i32 = row.get(cid);
                serde_json::Value::Number(serde_json::Number::from(val))
            }
            postgres::types::Type::JSON => {
                t = "JSON";
                let val = row.get::<usize, serde_json::Value>(cid);
                serde_json::Value::String(val.to_string())
            }
            postgres::types::Type::JSONB => {
                t = "JSONB";
                let val = row.get::<usize, serde_json::Value>(cid);
                serde_json::Value::String(val.to_string())
            }
            postgres::types::Type::NAME => {
                t = "NAME";
                serde_json::Value::String(row.get(cid))
            }
            postgres::types::Type::TEXT => {
                t = "TEXT";
                serde_json::Value::String(row.get(cid))
            }
            // FIXME: We should specify timezone offset for data in config
            postgres::types::Type::TIMESTAMPTZ => {
                t = "TIMESTAMPTZ";
                let ts: DateTime<Utc> = row.get::<usize, SystemTime>(cid).into();
                serde_json::Value::String(ts.with_timezone(&FixedOffset::east(0)).to_string())
            }
            postgres::types::Type::TIMESTAMP => {
                t = "TIMESTAMP";
                let ts: DateTime<Utc> = row.get::<usize, SystemTime>(cid).into();
                serde_json::Value::String(ts.to_string())
            }
            postgres::types::Type::UNKNOWN => {
                t = "UNKNOWN";
                serde_json::Value::String(row.get(cid))
            }
            // FIXME: Encoding, see UTF-8
            postgres::types::Type::VARCHAR => {
                t = "VARCHAR";
                serde_json::Value::String(row.get(cid))
            }
            _ => return Err(format!("type not supported: {}", col.type_()).into()),
        };
        obj.insert(
            "fieldType".to_string(),
            serde_json::value::Value::String(String::from(t)),
        );
        obj.insert("value".to_string(), v);
        obj.insert(
            "name".to_string(),
            serde_json::value::Value::from(String::from(col.name())),
        );

        json.insert(
            String::from(col.name()),
            serde_json::value::Value::from(obj),
        );
    }

    Ok(serde_json::Value::Object(json))
}
