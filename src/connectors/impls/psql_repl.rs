use std::time::Duration;
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
use crate::connectors::prelude::*;
use tremor_common::time::nanotime;

// use anyhow::anyhow;
// use futures::{Stream, StreamExt};
// use mz_expr::MirScalarExpr;
// use mz_postgres_util::{desc::PostgresTableDesc, Config};
// use mz_repr::{Datum, DatumVec, Row};
// use once_cell::sync::Lazy;
// use postgres_protocol::message::backend::{
//     LogicalReplicationMessage, ReplicationMessage, XLogDataBody,
// };
// use std::{
//     collections::BTreeMap,
//     env,
//     str::FromStr,
//     sync::{
//         atomic::{AtomicU64, Ordering},
//         Arc,
//     },
//     time::{Duration, Instant, SystemTime, UNIX_EPOCH},
// };
// use simd_json::ValueAccess;
// use tokio_postgres::{replication::LogicalReplicationStream, types::PgLsn, Client, SimpleQueryMessage, GenericClient};
// mod serializer;
// use serializer::SerializedXLogDataBody;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// Interval in nanoseconds
    pub interval: u64,
    /// Host name
    pub host: String,
    /// Port number
    pub port: u16,
    /// Username
    pub username: String,
    /// Password string
    pub password: String,
    /// Database name
    pub dbname: String,
}

impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "psql_repl".into()
    }

    async fn build_cfg(
        &self,
        _: &Alias,
        _: &ConnectorConfig,
        raw: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(raw)?;
        let origin_uri = EventOriginUri {
            scheme: "tremor-psql-repl".to_string(),
            host: config.host,
            port: Option::from(config.port),
            path: vec![config.interval.to_string()],
        };
        let _database = config.dbname;
        let _username = config.username;
        let _password = config.password;

        Ok(Box::new(Metronome {
            interval: config.interval,
            origin_uri,
        }))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Metronome {
    interval: u64,
    origin_uri: EventOriginUri,
}

#[async_trait::async_trait()]
impl Connector for Metronome {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = MetronomeSource::new(self.interval, self.origin_uri.clone());
        Ok(Some(builder.spawn(source, ctx)))
    }
}

struct MetronomeSource {
    interval_ns: u64,
    next: u64,
    origin_uri: EventOriginUri,
    id: u64,
}

impl MetronomeSource {
    fn new(interval_ns: u64, origin_uri: EventOriginUri) -> Self {
        Self {
            interval_ns,
            next: nanotime() + interval_ns, // dummy placeholer
            origin_uri,
            id: 0,
        }
    }
}

#[async_trait::async_trait()]
impl Source for MetronomeSource {
    async fn connect(&mut self, _ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        self.next = nanotime() + self.interval_ns;
        Ok(true)
    }

    async fn pull_data(&mut self, pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        let now = nanotime();
        // we need to wait here before we continue to fulfill the interval conditions
        if now < self.next {
            tokio::time::sleep(Duration::from_nanos(self.next - now)).await;
        }
        self.next += self.interval_ns;
        *pull_id = self.id;
        self.id += 1;

////////////////
//         let pg_config = tokio_postgres::Config::from_str("host=127.0.0.1 port=5433 user=postgres password=password dbname=testdb")?;
//
//         let tunnel_config = mz_postgres_util::TunnelConfig::Direct;
//         let connection_config = tokio_postgres::Config::new(pg_config, tunnel_config)?;
//         let slot = "gamess";
//         let publication = "gamespub";
//         let source_id = "source_id";
//
//         println!("======== BEGIN SNAPSHOT ==========");
//         let publication_tables =
//             mz_postgres_util::publication_info(&connection_config, publication, None).await?;
//         // Validate publication tables against the state snapshot
//         dbg!(&publication_tables);

////////////////////
        let data = literal!({
            "connector": "psql-repl",
            "ingest_ns": now,
            "id": *pull_id
        });
        Ok(SourceReply::Structured {
            origin_uri: self.origin_uri.clone(),
            payload: data.into(),
            stream: DEFAULT_STREAM_ID,
            port: None,
        })
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {

    use crate::{config::Reconnect, connectors::prelude::*};

    #[tokio::test(flavor = "multi_thread")]
    async fn missing_config() -> Result<()> {
        let alias = Alias::new("flow", "connector");
        let builder = super::Builder::default();
        let connector_config = super::ConnectorConfig {
            connector_type: builder.connector_type(),
            codec: None,
            config: None,
            preprocessors: None,
            postprocessors: None,
            reconnect: Reconnect::None,
            metrics_interval_s: Some(5),
        };
        let kill_switch = KillSwitch::dummy();
        assert!(matches!(
            builder
                .build(&alias, &connector_config, &kill_switch)
                .await
                .err(),
            Some(Error(ErrorKind::MissingConfiguration(_), _))
        ));
        Ok(())
    }
}
