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
use crate::ramp::Config as CacheConfig;
use chrono::prelude::*;
use postgres::{Client, NoTls};
use serde_yaml::Value;
use simd_json::prelude::*;
use std::time::Duration;
use tokio_postgres::error::SqlState;

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
    pub config: Config,
}

impl onramp::Impl for Postgres {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;

            Ok(Box::new(Self { config }))
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

#[allow(clippy::too_many_lines, clippy::cognitive_complexity)]
fn onramp_loop(
    rx: &Receiver<onramp::Msg>,
    config: &Config,
    mut preprocessors: Preprocessors,
    mut codec: Box<dyn Codec>,
    mut metrics_reporter: RampReporter,
) -> Result<()> {
    let mut pipelines: Vec<(TremorURL, pipeline::Addr)> = Vec::new();
    let mut id = 0;

    let origin_uri = tremor_pipeline::EventOriginUri {
        scheme: "tremor-file".to_string(),
        host: hostname(),
        port: None,
        path: vec![config.host.clone()],
    };

    let mut obj = simd_json::OwnedValue::object();

    // NOTE: strftime format obedience is a must unless we automagically try to guess
    let consume_from =
        match DateTime::parse_from_str(&config.consume_from, "%Y-%m-%d %H:%M:%S%.6f %:z") {
            Ok(v) => v,
            Err(e) => return Err(format!("Could not parse consume_from: {}", e).into()),
        };

    let consume_until: DateTime<Utc> = chrono::offset::Utc::now();
    let consume_from = consume_from.format("%Y-%m-%d %H:%M:%S%.6f %:z");
    let consume_until = consume_until.format("%Y-%m-%d %H:%M:%S%.6f %:z");

    if let Err(e) = obj.insert("consume_from", consume_from.to_string()) {
        return Err(format!("Could not insert into obj: {}", e).into());
    }

    if let Err(e) = obj.insert("consume_until", consume_until.to_string()) {
        return Err(format!("Could not insert into obj: {}", e).into());
    }

    let mut cache = match ramp::lookup("mmap_file", Some(config.cache.clone()), &obj) {
        Ok(v) => v,
        Err(e) => return Err(e),
    };

    let mut cli: Option<postgres::Client> = None;
    let mut stmt: Option<postgres::Statement> = None;

    loop {
        match task::block_on(handle_pipelines(
            false,
            &rx,
            &mut pipelines,
            &mut metrics_reporter,
        ))? {
            PipeHandlerResult::Retry => continue,
            PipeHandlerResult::Terminate => return Ok(()),
            _ => (), // fixme .unwrap()
        }
        if cli.is_none() {
            cli = loop {
                match init_cli(&config) {
                    Ok(v) => {
                        break Some(v);
                    }
                    Err(e) => {
                        warn!("Could not initialize a Postgres client: {}", e);
                        warn!("Attempting to reconnect to Postgres database in 2s...");
                        thread::sleep(Duration::from_millis(2000));
                        continue;
                    }
                }
            }
        }

        if stmt.is_none() {
            let client = match cli.as_mut() {
                Some(v) => v,
                None => return Err("could not move client value".into()),
            };

            stmt = match client.prepare(&config.query) {
                Ok(v) => Some(v),
                Err(e) => {
                    metrics_reporter.increment_error();
                    warn!("Failed to prepare a statement: {}", e);
                    continue;
                }
            }
        }

        let mut obj = match cache.get() {
            Ok(v) => v,
            Err(e) => {
                metrics_reporter.increment_error();
                warn!("Failed to fetching from cache: {}", e);
                continue;
            }
        };

        let consume_from = if let Some(v) = obj.get("consume_from") {
            v
        } else {
            metrics_reporter.increment_error();
            warn!("Failed to fetching consume_from");
            continue;
        };

        let consume_until = if let Some(v) = obj.get("consume_until") {
            v
        } else {
            metrics_reporter.increment_error();
            warn!("Failed to fetching consume_until");
            continue;
        };

        let cf = match DateTime::parse_from_str(
            &consume_from.to_string(),
            "%Y-%m-%d %H:%M:%S%.6f %:z",
        ) {
            Ok(v) => v,
            Err(e) => {
                metrics_reporter.increment_error();
                warn!("Could not parse consume_from: {}", e);
                continue;
            }
        };

        let ct =
            match DateTime::parse_from_str(&consume_until.to_string(), "%Y-%m-%d %H:%M:%S%.6f %:z")
            {
                Ok(v) => v,
                Err(e) => {
                    metrics_reporter.increment_error();
                    warn!("Could not parse consume_until: {}", e);
                    continue;
                }
            };

        let client = if let Some(v) = cli.as_mut() {
            v
        } else {
            metrics_reporter.increment_error();
            warn!("Could not move client value");
            continue;
        };

        let statement = if let Some(v) = stmt.as_mut() {
            v
        } else {
            metrics_reporter.increment_error();
            warn!("Could not move statement value");
            continue;
        };

        let result = match client.query(statement, &[&cf, &ct]) {
            Ok(v) => v,
            Err(e) => {
                metrics_reporter.increment_error();
                warn!("Error while querying: {}", e);

                let code = if let Some(v) = e.code() {
                    v
                } else {
                    cli = None;
                    stmt = None;
                    continue;
                };

                if code == &SqlState::CONNECTION_EXCEPTION {
                    cli = None;
                    stmt = None;
                }
                continue;
            }
        };

        let mut ingest_ns = nanotime();

        // prepare interval for the next query
        let cf =
            match DateTime::parse_from_str(&consume_until.to_string(), "%Y-%m-%d %H:%M:%S%.6f %:z")
            {
                Ok(v) => v,
                Err(e) => {
                    metrics_reporter.increment_error();
                    warn!("Could not parse consume_until: {}", e);
                    continue;
                }
            };
        let ct = cf + chrono::Duration::milliseconds(i64::from(config.interval_ms));
        let cf = cf.format("%Y-%m-%d %H:%M:%S%.6f %:z");
        let ct = ct.format("%Y-%m-%d %H:%M:%S%.6f %:z");
        if let Err(e) = obj.insert("consume_from", cf.to_string()) {
            metrics_reporter.increment_error();
            warn!("Could not insert into obj: {}", e);
            continue;
        }
        if let Err(e) = obj.insert("consume_until", ct.to_string()) {
            metrics_reporter.increment_error();
            warn!("Could not insert into obj: {}", e);
            continue;
        }

        if let Err(e) = cache.set(obj) {
            metrics_reporter.increment_error();
            warn!("Could not set interval in cache: {}", e);
            continue;
        }

        for row in result {
            let json = match row_to_json(&row) {
                Ok(v) => v,
                Err(e) => {
                    metrics_reporter.increment_error();
                    warn!("Could not convert row to json: {}", e);
                    continue;
                }
            };

            let json = match simd_json::to_string(&json) {
                Ok(v) => v,
                Err(e) => {
                    metrics_reporter.increment_error();
                    warn!("Could not convert json to string: {}", e);
                    continue;
                }
            };

            send_event(
                &pipelines,
                &mut preprocessors,
                &mut codec,
                &mut metrics_reporter,
                &mut ingest_ns,
                &origin_uri,
                id,
                json.into_bytes(),
            );
            id += 1;
        }
        thread::sleep(Duration::from_millis(u64::from(config.interval_ms)));
    }
}

#[async_trait::async_trait]
impl Onramp for Postgres {
    async fn start(
        &mut self,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let (tx, rx) = channel(1);
        let config = self.config.clone();
        let codec = codec::lookup(&codec)?;
        let preprocessors = make_preprocessors(&preprocessors)?;

        thread::Builder::new()
            .name(format!("onramp-postgres-{}", "???"))
            .spawn(move || {
                if let Err(e) = onramp_loop(&rx, &config, preprocessors, codec, metrics_reporter) {
                    error!("[Onramp] Error: {}", e)
                }
            })?;
        Ok(tx)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
