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

//! # `PostgreSQL` Offramp
//!
//! Writes events to a `PostgreSQL` and `TimescaleDB` database
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use crate::offramp::prelude::*;
use crate::ramp::postgres::{json_to_record, Record};
use halfbrown::HashMap;
use postgres::{Client, NoTls};

pub struct Postgres {
    pub config: Config,
    client: Option<postgres::Client>,
    pipelines: HashMap<TremorURL, pipeline::Addr>,
    postprocessors: Postprocessors,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u32,
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub table: String,
}

impl ConfigImpl for Config {}

impl offramp::Impl for Postgres {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;

            Ok(Box::new(Self {
                config,
                pipelines: HashMap::new(),
                postprocessors: vec![],
                client: None,
            }))
        } else {
            Err("Missing config for onramp".into())
        }
    }
}

fn init_cli(config: &Config) -> std::result::Result<postgres::Client, postgres::Error> {
    let conn_str = format!(
        "host={} user={} password={} port={} dbname={}",
        config.host, config.user, config.password, config.port, config.dbname
    );
    let cli = Client::connect(&conn_str, NoTls)?;

    Ok(cli)
}

impl Offramp for Postgres {
    fn on_event(&mut self, _codec: &dyn Codec, _input: &str, event: Event) -> Result<()> {
        for val in event.value_iter() {
            let obj = val.as_object();
            if let Some(kv) = obj {
                let mut fields: Vec<String> = Vec::new();
                let mut params: Vec<String> = Vec::new();
                let mut records: Vec<Record> = Vec::new();

                let mut ct: usize = 1;
                for (field, value) in kv {
                    fields.push(field.to_string());
                    params.push(format!("${}", ct));
                    ct += 1;
                    let record = match json_to_record(value) {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(format!("Could not convert json to record: {}", e).into())
                        }
                    };
                    records.push(record);
                }

                let fields = fields.join(",");
                let params = params.join(",");

                let q = format!(
                    "INSERT INTO {} ({}) VALUES ({});",
                    self.config.table, fields, params
                );
                let s_slice: &str = &q[..];

                if self.client.is_none() {
                    self.client = match init_cli(&self.config) {
                        Ok(v) => Some(v),
                        Err(e) => {
                            return Err(
                                format!("Could not initialize a Postgres client: {}", e).into()
                            )
                        }
                    }
                }

                let client = match self.client.as_mut() {
                    Some(v) => v,
                    None => return Err("could not move client value".into()),
                };

                match client.query_raw(
                    s_slice,
                    records
                        .iter()
                        .map(|p| p as &dyn postgres::types::ToSql)
                        .collect::<Vec<&dyn postgres::types::ToSql>>(),
                ) {
                    Ok(_) => return Ok(()),
                    Err(e) => return Err(format!("Failure while querying: {}", e).into()),
                }
            }
        }

        Ok(())
    }
    fn add_pipeline(&mut self, id: TremorURL, addr: pipeline::Addr) {
        self.pipelines.insert(id, addr);
    }
    fn remove_pipeline(&mut self, id: TremorURL) -> bool {
        self.pipelines.remove(&id);
        self.pipelines.is_empty()
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    fn start(&mut self, _codec: &dyn Codec, postprocessors: &[String]) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }
}
