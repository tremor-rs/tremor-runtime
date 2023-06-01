use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64};
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
use percent_encoding::percent_decode_str;
use postgres_replication::PgMessage;
use postgres_replication::PublicationTables;

use self::postgres_replication::serializer::SerializedXLogDataBody;

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
        let password = percent_decode_str(config.url.password().unwrap_or_default())
            .decode_utf8_lossy()
            .into_owned();
        let publication = config.publication;
        let replication_slot = config.replication_slot;
        let pg_config = TokioPgConfig::from_str(&format!(
            "host={} port={} user={} password={} dbname={}",
            config.url.host_or_local(),
            config.url.port_or_dflt(),
            config.url.username(),
            password,
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
            committed_lsn: Arc::new(AtomicU64::new(0)),
        }))
    }
}
#[derive(Debug)]
enum PullIdMapping {
    Begin(u64),
    Commit(u64),
    BeginAndCommit(u64),
}

#[derive(Debug)]
pub(crate) struct PostgresReplication {
    connection_config: MzConfig,
    publication: String,
    replication_slot: String,
    origin_uri: EventOriginUri,
    rx: Option<Receiver<PgMessage>>,
    tx: Sender<PgMessage>,
    committed_lsn: Arc<AtomicU64>,
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
            self.committed_lsn.clone(),
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
    rx: Receiver<PgMessage>,
    tx: Sender<PgMessage>,
    origin_uri: EventOriginUri,
    pull_lsn_mapping: BTreeMap<u64, PullIdMapping>,
    as_of: Arc<AtomicU64>,
    replay: Arc<AtomicBool>,
    committed_lsn: Arc<AtomicU64>,
    first_lsn: Option<u64>,
    last_pull_id: Option<u64>,
}

impl PostgresReplicationSource {
    fn new(
        connection_config: MzConfig,
        publication: String,
        replication_slot: String,
        rx: Receiver<PgMessage>,
        tx: Sender<PgMessage>,
        origin_uri: EventOriginUri,
        committed_lsn: Arc<AtomicU64>,
    ) -> Self {
        Self {
            connection_config,
            publication,
            replication_slot,
            rx,
            tx,
            origin_uri,
            pull_lsn_mapping: BTreeMap::new(),
            committed_lsn,
            as_of: Arc::new(AtomicU64::new(0)),
            replay: Arc::new(AtomicBool::new(false)),
            first_lsn: None,
            last_pull_id: None,
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
        let committed_lsn = self.committed_lsn.clone();
        let as_of = self.as_of.clone();
        let replay = self.replay.clone();
        task::spawn(async move {
            if let Err(e) = postgres_replication::replication(
                conn_config,
                &publication,
                &replication_slot,
                tx,
                as_of,
                committed_lsn,
                replay,
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

    async fn pull_data(&mut self, pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        loop {
            match self.rx.recv().await {
                Some(PgMessage::PublicationTables(publication)) => {
                    let publication_tables_json = serde_json::to_string(&PublicationTables {
                        publication_tables: publication,
                    })
                    .map_err(|_| "json error")?;
                    let json_obj =
                        serde_json::from_str::<tremor_value::Value>(&publication_tables_json)
                            .map_err(|_| "json error")?;
                    return Ok(SourceReply::Structured {
                        origin_uri: self.origin_uri.clone(),
                        payload: (json_obj.into_static(), Value::object()).into(),
                        stream: DEFAULT_STREAM_ID,
                        port: None,
                    });
                }
                None => return Err(Error::from("Channel closed")),
                Some(PgMessage::Begin(lsn)) => {
                    self.first_lsn = Some(lsn);
                }
                Some(PgMessage::Data(event)) => {
                    let serialized_event =
                        serde_json::to_string_pretty(&SerializedXLogDataBody(event))
                            .map_err(|_| "json error")?;
                    let json_obj = serde_json::from_str::<tremor_value::Value>(&serialized_event)
                        .map_err(|_| "json error")?;
                    self.last_pull_id = Some(*pull_id);
                    if let Some(lsn) = self.first_lsn.take() {
                        self.pull_lsn_mapping
                            .insert(*pull_id, PullIdMapping::Begin(lsn));
                    }
                    return Ok(SourceReply::Structured {
                        origin_uri: self.origin_uri.clone(),
                        payload: (json_obj.into_static(), Value::object()).into(),
                        stream: DEFAULT_STREAM_ID,
                        port: None,
                    });
                }
                Some(PgMessage::Commit(end_lsn)) => {
                    if let Some(last_pid) = self.last_pull_id.take() {
                        if self.pull_lsn_mapping.contains_key(&last_pid) {
                            self.pull_lsn_mapping
                                .insert(last_pid, PullIdMapping::BeginAndCommit(end_lsn));
                        } else {
                            self.pull_lsn_mapping
                                .insert(last_pid, PullIdMapping::Commit(end_lsn));
                        }
                    }
                }
            }
        }
    }

    async fn ack(&mut self, _stream_id: u64, pull_id: u64, _ctx: &SourceContext) -> Result<()> {
        let mut last_commit_lsn = None;

        for (pid, lsn) in self.pull_lsn_mapping.range(..=pull_id) {
            match lsn {
                PullIdMapping::Commit(end_lsn) => {
                    last_commit_lsn = Some((pid, end_lsn));
                }
                PullIdMapping::Begin(_) => {}
                PullIdMapping::BeginAndCommit(lsn) => {
                    last_commit_lsn = Some((pid, lsn));
                }
            }
        }
        if let Some((pid, lsn)) = last_commit_lsn {
            let next_pid = pid + 1;
            let lsn = *lsn;
            self.pull_lsn_mapping = self.pull_lsn_mapping.split_off(&next_pid);
            self.as_of.store(lsn, Ordering::SeqCst);
            self.committed_lsn.store(lsn, Ordering::SeqCst);
        }

        Ok(())
    }

    async fn fail(&mut self, _stream_id: u64, pull_id: u64, _ctx: &SourceContext) -> Result<()> {
        let mut last_begin_lsn = None;

        for (pid, lsn) in self.pull_lsn_mapping.range(..=pull_id) {
            match lsn {
                PullIdMapping::Commit(_) => {}
                PullIdMapping::Begin(begin_lsn) => last_begin_lsn = Some((*pid, *begin_lsn)),
                PullIdMapping::BeginAndCommit(_) => {}
            }
        }
        if let Some((pid, lsn)) = last_begin_lsn {
            self.pull_lsn_mapping.split_off(&pid);
            // the reason we store the as_of first is to guarantee that we set the replication thread
            // re-start point before we break the current replication with setting replay to true
            //             P1                         | P2                          | replay               | as_of  | commited_lsp
            //                                        |                             | false                | 42     | 10
            //             replay = true              |                             | true                 | 42     | 10
            //                                        | exit loop                   | true                 | 42     | 10
            //                                        | replay = false              | false                | 42     | 10
            //                                        | replay_from(as_of = 42)     | false                | 42     | 10
            //                                        | produce data                | false                | 42     | 10
            //              as_of = 23                |                             | false                | 23     | 10
            //              ack (43)                  |                             | false                | 43     | 43
            self.as_of.store(lsn, Ordering::SeqCst);
            self.replay.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    fn is_transactional(&self) -> bool {
        true
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
