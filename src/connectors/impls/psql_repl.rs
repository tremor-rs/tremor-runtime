use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
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
use crate::{
    channel::{bounded, Receiver, Sender},
    errors::already_created_error,
};
use mz_postgres_util::Config as MzConfig;
use tokio::task;
use tokio_postgres::config::Config as TokioPgConfig;
mod postgres_replication;

pub(crate) struct PgSqlDefaults;
impl Defaults for PgSqlDefaults {
    const SCHEME: &'static str = "postgres";
    const HOST: &'static str = "localhost";
    const PORT: u16 = 5432;
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    url: Url<PgSqlDefaults>,
    /// Database name
    pub dbname: String,
    /// Publication name
    pub publication: String,
    /// Replication slot name
    pub replication_slot: String,
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
            host: config.url.host_or_local().to_string(),
            port: Some(config.url.port_or_dflt()),
            path: vec![config.url.host_or_local().to_string()],
        };
        let publication = config.publication;
        let replication_slot = config.replication_slot;
        let pg_config = TokioPgConfig::from_str(&format!(
            "host={} port={} user={} password={} dbname={}",
            config.url.host_or_local(),
            config.url.port_or_dflt(),
            config.url.username(),
            config.url.password().unwrap_or_default(),
            config.dbname
        ))?;
        let connection_config = MzConfig::new(pg_config, mz_postgres_util::TunnelConfig::Direct)?;
        let (tx, rx) = bounded(qsize());

        Ok(Box::new(PostgresReplication {
            connection_config,
            publication,
            replication_slot,
            origin_uri,
            rx: Some(rx),
            tx,
        }))
    }
}

#[derive(Debug)]
pub(crate) struct PostgresReplication {
    connection_config: MzConfig,
    publication: String,
    replication_slot: String,
    origin_uri: EventOriginUri,
    rx: Option<Receiver<Value<'static>>>,
    tx: Sender<Value<'static>>,
}

#[async_trait::async_trait()]
impl Connector for PostgresReplication {
    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = PostgresReplicationSource::new(
            self.connection_config.clone(),
            self.publication.clone(),
            self.replication_slot.clone(),
            self.rx.take().ok_or_else(already_created_error)?,
            self.tx.clone(),
            self.origin_uri.clone(),
        );
        Ok(Some(builder.spawn(source, ctx)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }
}

struct PostgresReplicationSource {
    connection_config: MzConfig,
    publication: String,
    replication_slot: String,
    rx: Receiver<Value<'static>>,
    tx: Sender<Value<'static>>,
    origin_uri: EventOriginUri,
}

impl PostgresReplicationSource {
    fn new(
        connection_config: MzConfig,
        publication: String,
        replication_slot: String,
        rx: Receiver<Value<'static>>,
        tx: Sender<Value<'static>>,
        origin_uri: EventOriginUri,
    ) -> Self {
        Self {
            connection_config,
            publication,
            replication_slot,
            rx,
            tx,
            origin_uri,
        }
    }
}

#[async_trait::async_trait()]
impl Source for PostgresReplicationSource {
    async fn connect(&mut self, ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        let conn_config = self.connection_config.clone();
        let publication = self.publication.clone();
        let replication_slot = self.replication_slot.clone();
        let tx = self.tx.clone();
        let cloned_ctx = ctx.clone();
        let committed_lsn = Arc::new(AtomicU64::new(0));
        task::spawn(async move {
            if let Err(e) = postgres_replication::replication(
                conn_config,
                &publication,
                &replication_slot,
                tx,
                committed_lsn,
            )
            .await
            {
                if (cloned_ctx.notifier().connection_lost().await).is_ok() {
                    error!("{cloned_ctx} Error in replication task: {e}. Initiating Reconnect...",);
                }
                return Err(e);
            }
            Ok(())
        });
        Ok(true)
    }

    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        self.rx
            .recv()
            .await
            .map(|data| SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                payload: (data, Value::object()).into(),
                stream: DEFAULT_STREAM_ID,
                port: None,
            })
            .ok_or_else(|| Error::from("channel closed"))
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
