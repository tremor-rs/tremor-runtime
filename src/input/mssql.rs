use chrono;
use futures::future::{loop_fn, Future, Loop};
use futures_state_stream::StateStream;
use input::Input as InputT;
use pipeline::Msg;
use serde_json;
#[cfg(feature = "try_spmc")]
use spmc;
use std::sync::mpsc;
use std::{boxed, str, thread, time};
use tiberius::ty::{ColumnData, FromColumnData};
use tiberius::{self, SqlConnection};
use tokio_current_thread::block_on_all;
pub struct Input {
    config: Config,
}
const DATETIME_FORMAT: &'static str = "%Y-%m-%d %H:%M:%S";
const DATE_FORMAT: &'static str = "%Y-%m-%d";

fn dflt_false() -> bool {
    false
}

fn dflt_port() -> u32 {
    1433
}

#[derive(Deserialize, Debug, Clone)]
struct Config {
    host: String,
    #[serde(default = "dflt_port")]
    port: u32,
    username: String,
    password: String,
    query: String,
    interval_ms: Option<u64>,
    #[serde(default = "dflt_false")]
    trust_server_certificate: bool,
}

impl Input {
    pub fn new(opts: &str) -> Self {
        match serde_json::from_str(opts) {
            Ok(config @ Config { .. }) => {

                Input { config }
            }
            e => {
                panic!("Invalid options for Kafka input, use `{{\"host\": \"<host>\", \"port\": <port>, \"username\": \"<username>\", \"password\": \"<password>\", \"query\": \"<query>\"}}`\n{:?} ({})", e, opts)
            }
        }
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
                serde_json::Value::Number(serde_json::Number::from_f64(*f as f64).unwrap())
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

fn send_row(
    pipelines: &Vec<mpsc::SyncSender<Msg>>,
    row: tiberius::query::QueryRow,
    idx: usize,
    len: usize,
) -> usize {
    let mut idx = (idx + 1) % len;
    let json = row_to_json(&row);
    let json = serde_json::to_string(&json).unwrap();
    let mut sent = false;
    for i in 0..len {
        idx = (idx + i) % len;
        if pipelines[idx]
            .try_send(Msg::new(None, json.clone()))
            .is_ok()
        {
            sent = true;
            break;
        }
    }
    if !sent {
        if let Err(e) = pipelines[idx].send(Msg::new(None, json)) {
            error!("Error during handling message: {:?}", e)
        };
    }
    idx
}
impl InputT for Input {
    fn enter_loop(&mut self, pipelines: Vec<mpsc::SyncSender<Msg>>) {
        let conn_str = format!(
            "server=tcp:{},{};username={};password={};trustservercertificate={}",
            self.config.host,
            self.config.port,
            self.config.username,
            self.config.password,
            self.config.trust_server_certificate
        );
        let len = pipelines.len();
        let query = self.config.query.as_str();
        if let Some(ival) = self.config.interval_ms {
            let conn = SqlConnection::connect(conn_str.as_str());
            //let conn = block_on_all(conn).unwrap();
            let ival = time::Duration::from_millis(ival);
            let f = conn.and_then(|conn| {
                loop_fn::<_, SqlConnection<boxed::Box<tiberius::BoxableIo>>, _, _>(conn, |conn| {
                    let now = time::Instant::now();
                    conn.simple_query(query)
                        .for_each(|row| {
                            send_row(&pipelines, row, 0, len);
                            Ok(())
                        })
                        .and_then(move |conn| {
                            thread::sleep(ival - now.elapsed());
                            Ok(Loop::Continue(conn))
                        })
                })
            });
            block_on_all(f).unwrap();
        } else {
            let mut idx = 0;
            let conn = SqlConnection::connect(conn_str.as_str());
            let future = conn.and_then(|conn| {
                conn.simple_query(query).for_each(|row| {
                    idx = send_row(&pipelines, row, idx, len);
                    Ok(())
                })
            });
            block_on_all(future).unwrap();
        }
    }

    #[cfg(feature = "try_spmc")]
    fn enter_loop2(&mut self, pipelines: Vec<spmc::Sender<Msg>>) {
        panic!("Not implemented");
    }
}
