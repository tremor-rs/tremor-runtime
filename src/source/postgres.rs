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

//! # Postgres Onramp
//!
//! See [Config](struct.Config.html) for details.

use crate::errors::Result;
use crate::onramp::prelude::*;
use crate::ramp;
use crate::ramp::postgres::row_to_json;
use crate::ramp::{Config as CacheConfig, KV};
use chrono::prelude::*;
use postgres::{Client, NoTls, Row, Statement};
use serde_yaml::Value;
use simd_json::{prelude::*, OwnedValue};
use std::fmt;
use tokio_postgres::error::SqlState;

const TIME_FMT: &str = "%Y-%m-%d %H:%M:%S%.6f %:z";

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u32,
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub query: String,
    pub interval_ms: u32,
    pub consume_from: String,
    pub cache: CacheConfig,
}

impl ConfigImpl for Config {}

pub struct Postgres {
    onramp_id: TremorURL,
    pub config: Config,
}

pub struct Int {
    pub config: Config,
    // onramp_uid: u64,
    onramp_id: TremorURL,
    origin_uri: EventOriginUri,
    cache: Box<dyn KV + Send>,
    cli: Option<Client>,
    stmt: Option<Statement>,
    rows: Vec<Row>,
}
impl fmt::Debug for Int {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Postgres")
    }
}

impl onramp::Impl for Postgres {
    fn from_config(id: &TremorURL, config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;

            Ok(Box::new(Self {
                config,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Missing config for onramp".into())
        }
    }
}

impl Int {
    async fn from_config(uid: u64, onramp_id: TremorURL, config: &Config) -> Result<Self> {
        let origin_uri = tremor_pipeline::EventOriginUri {
            uid,
            scheme: "tremor-file".to_string(),
            host: hostname(),
            port: None,
            path: vec![config.host.clone()],
        };
        let consume_from = DateTime::parse_from_str(&config.consume_from, TIME_FMT)?;
        let consume_from = consume_from.format(TIME_FMT).to_string();

        let consume_until: DateTime<Utc> = chrono::offset::Utc::now();
        let consume_until = consume_until.format(TIME_FMT).to_string();

        let mut obj = OwnedValue::object();

        obj.insert("consume_from", consume_from)?;

        obj.insert("consume_until", consume_until)?;
        let cache = match ramp::lookup("mmap_file", Some(config.cache.clone()), &obj) {
            Ok(v) => v,
            Err(e) => return Err(e),
        };

        Ok(Int {
            // onramp_uid,
            onramp_id,
            config: config.clone(),
            origin_uri,
            cache,
            cli: None,
            stmt: None,
            rows: Vec::new(),
        })
    }
    fn init_cli(&self) -> Result<Client> {
        let conn_str = format!(
            "host={} user={} password={} port={} dbname={}",
            self.config.host,
            self.config.user,
            self.config.password,
            self.config.port,
            self.config.dbname
        );
        Ok(Client::connect(&conn_str, NoTls)?)
    }
}

#[async_trait::async_trait]
impl Source for Int {
    #[allow(clippy::used_underscore_binding)]
    async fn read(&mut self, _id: u64) -> Result<SourceReply> {
        if let Some(row) = self.rows.pop() {
            return Ok(SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                data: row_to_json(&row)?.into(),
            });
        }
        if self.cli.is_none() {
            self.cli = Some(self.init_cli()?);
        };
        let client = self
            .cli
            .as_mut()
            .ok_or_else(|| Error::from("No CLI connection"))?;
        if self.stmt.is_none() {
            self.stmt = Some(client.prepare(&self.config.query)?);
        };
        let statement = self
            .stmt
            .as_mut()
            .ok_or_else(|| Error::from("No Statement connection"))?;

        let mut obj = self.cache.get()?;

        let consume_from = obj
            .get("consume_from")
            .and_then(ValueTrait::as_str)
            .ok_or_else(|| Error::from("Failed to fetching consume_from"))?;

        let consume_until = obj
            .get("consume_until")
            .and_then(ValueTrait::as_str)
            .ok_or_else(|| Error::from("Failed to fetching consume_until"))?;

        let cf = DateTime::parse_from_str(consume_from, TIME_FMT)?;
        let ct = DateTime::parse_from_str(consume_until, TIME_FMT)?;

        self.rows = match client.query(statement, &[&cf, &ct]) {
            Ok(v) => v,
            Err(e) => {
                let code = if let Some(v) = e.code() {
                    v
                } else {
                    &SqlState::CONNECTION_EXCEPTION
                };

                if code == &SqlState::CONNECTION_EXCEPTION {
                    self.cli = None;
                    self.stmt = None;
                }
                return Ok(SourceReply::Empty(10));
            }
        };
        self.rows.reverse();

        // prepare interval for the next query
        let cf = DateTime::parse_from_str(&consume_until.to_string(), TIME_FMT)?;
        let ct = cf + chrono::Duration::milliseconds(i64::from(self.config.interval_ms));
        let cf = cf.format(TIME_FMT);
        let ct = ct.format(TIME_FMT);
        obj.insert("consume_from", cf.to_string())?;
        obj.insert("consume_until", ct.to_string())?;

        self.cache.set(obj)?;

        if let Some(row) = self.rows.pop() {
            Ok(SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                data: row_to_json(&row)?.into(),
            })
        } else {
            Ok(SourceReply::Empty(100))
        }
    }
    async fn init(&mut self) -> Result<SourceState> {
        Ok(SourceState::Connected)
    }
    fn id(&self) -> &TremorURL {
        &self.onramp_id
    }
}

#[async_trait::async_trait]
impl Onramp for Postgres {
    async fn start(
        &mut self,
        onramp_uid: u64,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let source = Int::from_config(onramp_uid, self.onramp_id.clone(), &self.config).await?;

        let (manager, tx) =
            SourceManager::new(onramp_uid, source, preprocessors, codec, metrics_reporter).await?;
        task::Builder::new()
            .name(format!("on-pg-{}", self.config.dbname))
            .spawn(manager.run())?;
        Ok(tx)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
