// Copyright 2018, Wayfair GmbH
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

//! # MSSQL Onramo
//!
//! The `mssql` onramp processes each line of a query as a event. It is possible
//! to re-run queries in a periodic basis.
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use crate::errors::*;
use crate::onramp::{EnterReturn, Onramp as OnrampT, PipelineOnramp};
use crate::pipeline::prelude::*;
use crate::utils;
use chrono;
use futures::future::{loop_fn, Future, Loop};
use futures::sync::mpsc::{channel, Receiver};
use futures::Stream;
use futures_state_stream::StateStream;
use serde_json;
use serde_yaml;
use std::collections::HashMap;
use std::{boxed, f64, str, thread, time};
use tiberius::ty::{ColumnData, FromColumnData};
use tiberius::{self, SqlConnection};
use tokio_current_thread::block_on_all;

pub struct Onramp {
    config: Config,
}
const DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";
const DATE_FORMAT: &str = "%Y-%m-%d";

fn dflt_false() -> bool {
    false
}

fn dflt_port() -> u32 {
    1433
}

#[derive(Deserialize, Debug, Clone)]
struct Config {
    /// Host to connect to
    pub host: String,
    /// Port to connect to
    #[serde(default = "dflt_port")]
    pub port: u32,
    /// Username to register with
    pub username: String,
    /// Password to register with
    pub password: String,
    /// Query to execute
    pub query: String,
    /// Interval in which the query is executed, if not provided the query
    /// will execute once and then terminate
    pub interval_ms: Option<u64>,
    /// Automatically trust the server certificate even if it can not be
    /// validated
    #[serde(default = "dflt_false")]
    pub trust_server_certificate: bool,
}

impl Onramp {
    pub fn create(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        Ok(Onramp { config })
    }
}

fn row_to_json(row: &tiberius::query::QueryRow) -> serde_json::Value {
    let mut json = serde_json::Map::new();
    for (meta, col) in row.iter() {
        let v: serde_json::Value = match col {
            ColumnData::I8(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
            ColumnData::I16(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
            ColumnData::I32(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
            ColumnData::I64(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
            ColumnData::F32(f) => {
                serde_json::Value::Number(serde_json::Number::from_f64(f64::from(*f)).unwrap())
            }
            ColumnData::F64(f) => {
                serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap())
            }
            ColumnData::Bit(b) => serde_json::Value::Bool(*b),

            ColumnData::None => serde_json::Value::Null,
            ColumnData::String(s) => serde_json::Value::String(s.to_string()),
            ColumnData::BString(s) => serde_json::Value::String(String::from(s.as_str())),
            ColumnData::Binary(s) => {
                serde_json::Value::String(String::from(str::from_utf8(s).unwrap()))
            }
            ColumnData::Guid(g) => serde_json::Value::String(format!("{}", g)),
            ColumnData::Time(t) => json!({
                "increments": t.increments,
                "scale": t.scale
            }),
            date @ ColumnData::Date(_) => {
                let date: chrono::NaiveDate = chrono::NaiveDate::from_column_data(date).unwrap();
                let s = format!("{}", date.format(DATE_FORMAT));

                serde_json::Value::String(s)
            }
            date @ ColumnData::DateTime(_) => {
                let date: chrono::NaiveDateTime =
                    chrono::NaiveDateTime::from_column_data(date).unwrap();
                let s = format!("{}", date.format(DATETIME_FORMAT));

                serde_json::Value::String(s)
            }
            date @ ColumnData::DateTime2(_) => {
                let date: chrono::NaiveDateTime =
                    chrono::NaiveDateTime::from_column_data(date).unwrap();
                let s = format!("{}", date.format(DATETIME_FORMAT));

                serde_json::Value::String(s)
            }
            date @ ColumnData::SmallDateTime(_) => {
                let date: chrono::NaiveDateTime =
                    chrono::NaiveDateTime::from_column_data(date).unwrap();
                let s = format!("{}", date.format(DATETIME_FORMAT));

                serde_json::Value::String(s)
            }
        };
        json.insert(String::from(meta.col_name.as_str()), v);
    }
    serde_json::Value::Object(json)
}

// passing in the vector as a ref is OK here.
#[cfg_attr(feature = "cargo-clippy", allow(clippy::ptr_arg))]
fn send_row(
    pipelines: &PipelineOnramp,
    row: &tiberius::query::QueryRow,
    idx: usize,
    len: usize,
) -> (usize, Receiver<Return>) {
    let mut idx = (idx + 1) % len;
    let json = row_to_json(&row);
    let json = serde_json::to_string(&json).unwrap();
    let (tx, rx) = channel(1);
    let msg = OnData {
        reply_channel: Some(tx),
        data: EventValue::Raw(json.into_bytes()),
        vars: HashMap::new(),
        ingest_ns: utils::nanotime(),
    };

    idx = (idx + 1) % len;
    pipelines[idx].do_send(msg);
    (idx, rx)
}
impl OnrampT for Onramp {
    fn enter_loop(&mut self, pipelines: PipelineOnramp) -> EnterReturn {
        let len = pipelines.len();
        let config = self.config.clone();
        thread::spawn(move || {
            let conn_str = format!(
                "server=tcp:{},{};uid={};password={};trustservercertificate={}",
                config.host,
                config.port,
                config.username,
                config.password,
                config.trust_server_certificate
            );
            let query = config.query.as_str();
            if let Some(ival) = config.interval_ms {
                let conn = SqlConnection::connect(conn_str.as_str());
                let ival = time::Duration::from_millis(ival);
                let f = conn.and_then(|conn| {
                    loop_fn::<_, SqlConnection<boxed::Box<tiberius::BoxableIo>>, _, _>(
                        conn,
                        |conn| {
                            let now = time::Instant::now();
                            conn.simple_query(query)
                                .for_each(|row| {
                                    send_row(&pipelines, &row, 0, len);
                                    Ok(())
                                })
                                .and_then(move |conn| {
                                    thread::sleep(ival - now.elapsed());
                                    Ok(Loop::Continue(conn))
                                })
                        },
                    )
                });
                block_on_all(f).unwrap();
            } else {
                let mut idx = 0;
                let mut i = 0;
                let conn = SqlConnection::connect(conn_str.as_str());
                let mut rxs = Vec::new();
                let future = conn.and_then(|conn| {
                    conn.simple_query(query).for_each(|row| {
                        i += 1;
                        let (idx1, rx) = send_row(&pipelines, &row, idx, len);
                        rxs.push(rx);
                        idx = idx1;
                        Ok(())
                    })
                });
                block_on_all(future).unwrap();
                for p in pipelines {
                    p.do_send(Shutdown);
                }
                for rx in rxs {
                    for _ in rx.wait() {}
                }
            };
        })
    }
}
