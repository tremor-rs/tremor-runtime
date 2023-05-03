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

use crate::channel::Sender;
use anyhow::anyhow;
use futures::{Stream, StreamExt};
use mz_expr::MirScalarExpr;
use mz_postgres_util::{desc::PostgresTableDesc, Config as MzConfig};
use mz_repr::{Datum, DatumVec, Row};
use once_cell::sync::Lazy;
use postgres_protocol::message::backend::{
    LogicalReplicationMessage, ReplicationMessage, XLogDataBody,
};
use std::{
    collections::BTreeMap,
    // env,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use tokio_postgres::{
    replication::LogicalReplicationStream, types::PgLsn, Client, SimpleQueryMessage,
};
mod serializer;
use serializer::SerializedXLogDataBody;

// https://github.com/MaterializeInc/materialize/blob/main/src/storage/src/source/postgres.rs#L60
/// Postgres epoch is 2000-01-01T00:00:00Z
static PG_EPOCH: Lazy<SystemTime> = Lazy::new(|| UNIX_EPOCH + Duration::from_secs(946_684_800));

/// How often a status update message should be sent to the server
static FEEDBACK_INTERVAL: Duration = Duration::from_secs(30);

/// The amount of time we should wait after the last received message before worrying about WAL lag
static WAL_LAG_GRACE_PERIOD: Duration = Duration::from_secs(30);

// https://github.com/MaterializeInc/materialize/blob/main/src/storage/src/source/postgres.rs#L956
async fn produce_replication<'a>(
    client_config: mz_postgres_util::Config,
    slot: &'a str,
    publication: &'a str,
    as_of: PgLsn,
    committed_lsn: Arc<AtomicU64>,
) -> impl Stream<Item = Result<XLogDataBody<LogicalReplicationMessage>, ReplicationError>> + 'a {
    async_stream::try_stream!({
        let mut last_feedback = Instant::now();
        let mut last_commit_lsn = as_of;
        loop {
            let client = client_config.clone().connect_replication().await?;
            let query = format!(
                r#"START_REPLICATION SLOT "{name}" LOGICAL {lsn}
                  ("proto_version" '1', "publication_names" '{publication}')"#,
                name = &slot,
                lsn = last_commit_lsn,
                publication = publication
            );
            let copy_stream = client.copy_both_simple(&query).await?;
            let mut stream = Box::pin(LogicalReplicationStream::new(copy_stream));

            let mut last_data_message = Instant::now();

            // The inner loop
            loop {
                // The upstream will periodically request status updates by setting the keepalive's
                // reply field to 1. However, we cannot rely on these messages arriving on time. For
                // example, when the upstream is sending a big transaction its keepalive messages are
                // queued and can be delayed arbitrarily. Therefore, we also make sure to
                // send a proactive status update every 30 seconds There is an implicit requirement
                // that a new resumption frontier is converted into an lsn relatively soon after
                // startup.
                //
                // See: https://www.postgresql.org/message-id/CAMsr+YE2dSfHVr7iEv1GSPZihitWX-PMkD9QALEGcTYa+sdsgg@mail.gmail.com
                let mut needs_status_update = last_feedback.elapsed() > FEEDBACK_INTERVAL;

                match stream.as_mut().next().await {
                    Some(Ok(
                        postgres_protocol::message::backend::ReplicationMessage::XLogData(
                            xlog_data,
                        ),
                    )) => {
                        last_data_message = Instant::now();
                        match xlog_data.data() {
                            LogicalReplicationMessage::Origin(_origin) => {}

                            LogicalReplicationMessage::Commit(commit) => {
                                last_commit_lsn = PgLsn::from(commit.end_lsn());
                            }
                            LogicalReplicationMessage::Begin(_begin) => {}
                            _ => yield xlog_data,
                        }
                    }
                    Some(Ok(ReplicationMessage::PrimaryKeepAlive(keepalive))) => {
                        needs_status_update = needs_status_update || keepalive.reply() == 1;

                        if last_data_message.elapsed() > WAL_LAG_GRACE_PERIOD {
                            break;
                        }
                    }
                    Some(Err(err)) => {
                        return Err(ReplicationError::from(err))?;
                    }
                    None => {
                        break;
                    }
                    // The enum is marked non_exhaustive, better be conservative
                    _ => {
                        return Err(anyhow!("Unexpected replication message"))?;
                    }
                }
                if needs_status_update {
                    let ts: i64 = PG_EPOCH
                        .elapsed()
                        .expect("system clock set earlier than year 2000!")
                        .as_micros()
                        .try_into()
                        .expect("software more than 200k years old, consider updating");

                    let committed_lsn = PgLsn::from(committed_lsn.load(Ordering::SeqCst));
                    stream
                        .as_mut()
                        .standby_status_update(committed_lsn, committed_lsn, committed_lsn, ts, 0)
                        .await?;
                    last_feedback = Instant::now();
                }
            }
            // This may not be required, but as mentioned above in
            // `postgres_replication_loop_inner`, we drop clients aggressively out of caution.
            drop(stream);

            let client = client_config.clone().connect_replication().await?;

            // We reach this place if the consume loop above detected large WAL lag. This
            // section determines whether or not we can skip over that part of the WAL by
            // peeking into the replication slot using a normal SQL query and the
            // `pg_logical_slot_peek_binary_changes` administrative function.
            //
            // By doing so we can get a positive statement about existence or absence of
            // relevant data from the current LSN to the observed WAL end. If there are no
            // messages then it is safe to fast forward last_commit_lsn to the WAL end LSN and restart
            // the replication stream from there.
            let query = format!(
                "SELECT lsn FROM pg_logical_slot_peek_binary_changes(
                     '{name}', NULL, NULL,
                     'proto_version', '1',
                     'publication_names', '{publication}'
                )",
                name = &slot,
                publication = publication
            );

            let rows = client.simple_query(&query).await?;

            rows.into_iter()
                .filter(|row| match row {
                    SimpleQueryMessage::Row(row) => {
                        let change_lsn: PgLsn = row
                            .get("lsn")
                            .expect("missing expected column: `lsn`")
                            .parse()
                            .expect("invalid lsn");
                        // Keep all the changes that may exist after our last observed transaction
                        // commit
                        change_lsn > last_commit_lsn
                    }
                    SimpleQueryMessage::CommandComplete(_) => false,
                    _ => panic!("unexpected enum variant"),
                })
                .count();
        }
    })
}

// https://github.com/MaterializeInc/materialize/blob/main/src/storage/src/source/postgres.rs#L941
/// Casts a text row into the target types
fn cast_row(table_cast: &[MirScalarExpr], datums: &[Datum<'_>]) -> Result<Row, anyhow::Error> {
    let arena = mz_repr::RowArena::new();
    let mut row = Row::default();
    let mut packer = row.packer();
    for column_cast in table_cast {
        let datum = column_cast.eval(datums, &arena)?;
        packer.push(datum);
    }
    Ok(row)
}

type ReplicationError = anyhow::Error;
/// Parses SQL results that are expected to be a single row into a Rust type
fn parse_single_row<T: FromStr>(
    result: &[SimpleQueryMessage],
    column: &str,
) -> std::result::Result<T, anyhow::Error> {
    let mut rows = result.iter().filter_map(|msg| match msg {
        SimpleQueryMessage::Row(row) => Some(row),
        _ => None,
    });
    match (rows.next(), rows.next()) {
        (Some(row), None) => row
            .get(column)
            .ok_or_else(|| anyhow!("missing expected column: {column}"))
            .and_then(|col| col.parse().map_err(|_| anyhow!("invalid data"))),
        (None, None) => Err(anyhow!("empty result")),
        _ => Err(anyhow!("ambiguous result, more than one row")),
    }
}

//https://github.com/MaterializeInc/materialize/blob/main/src/storage/src/source/postgres.rs#L855
/// Produces the initial snapshot of the data by performing a `COPY` query for each of the provided
/// `source_tables`.
///
/// The return stream of data returned is not annotated with LSN numbers. It is up to the caller to
/// provide a client that is in a known LSN context in which the snapshot will be taken. For
/// example by calling this method while being in a transaction for which the LSN is known.
fn produce_snapshot<'a>(
    client: &'a Client,
    source_tables: &'a BTreeMap<u32, SourceTable>,
) -> impl Stream<Item = Result<(usize, Row), ReplicationError>> + 'a {
    async_stream::try_stream! {
        // Scratch space to use while evaluating casts
        let mut datum_vec = DatumVec::new();

        for info in source_tables.values() {
            let reader = client
                .copy_out_simple(
                    format!(
                        "COPY {:?}.{:?} TO STDOUT (FORMAT TEXT, DELIMITER '\t')",
                        info.desc.namespace, info.desc.name
                    )
                    .as_str(),
                )
                .await?;

            tokio::pin!(reader);
            let mut text_row = Row::default();
            while let Some(b) = tokio::time::timeout(Duration::from_secs(30), reader.next())
                .await?
                .transpose()?
            {
                let mut packer = text_row.packer();
                // Convert raw rows from COPY into repr:Row. Each Row is a relation_id
                // and list of string-encoded values, e.g. Row{ 16391 , ["1", "2"] }
                let parser = mz_pgcopy::CopyTextFormatParser::new(b.as_ref(), "\t", "\\N");

                let mut raw_values = parser.iter_raw(info.desc.columns.len());
                while let Some(raw_value) = raw_values.next() {
                    match raw_value? {
                        Some(value) => {
                            packer.push(Datum::String(std::str::from_utf8(value)?));
                        }
                        None => packer.push(Datum::Null),
                    }
                }

                let mut datums = datum_vec.borrow();
                datums.extend(text_row.iter());

                let row = cast_row(&info.casts, &datums)?;


                yield (info.output_index, row);
            }

        }
    }
}

/// Information about an ingested upstream table
struct SourceTable {
    /// The source output index of this table
    output_index: usize,
    /// The relational description of this table
    desc: PostgresTableDesc,
    // /// The scalar expressions required to cast the text encoded columns received from postgres
    // /// into the target relational types
    casts: Vec<MirScalarExpr>,
}

#[derive(Serialize, Deserialize)]
struct PublicationTables {
    publication_tables: Vec<PostgresTableDesc>,
}

pub(crate) async fn replication(
    connection_config: MzConfig,
    publication: &str,
    slot: &str,
    tx: Sender<tremor_value::Value<'static>>,
) -> Result<(), anyhow::Error> {
    let publication_tables =
        mz_postgres_util::publication_info(&connection_config, publication, None).await?;
    let source_id = "source_id";
    let mut _replication_lsn = PgLsn::from(0);
    let publication_tables_json = serde_json::to_string(&PublicationTables {
        publication_tables: publication_tables.clone(),
    })?;
    let json_obj = serde_json::from_str::<tremor_value::Value>(&publication_tables_json)?;
    tx.send(json_obj.into_static()).await?;

    let source_tables: BTreeMap<u32, SourceTable> = publication_tables
        .into_iter()
        .map(|t| {
            (
                t.oid,
                SourceTable {
                    output_index: t.oid as usize,
                    desc: t,
                    casts: vec![],
                },
            )
        })
        .collect();
    let client = connection_config.clone().connect_replication().await?;

    let res = client
        .simple_query(&format!(
            r#"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{slot}'"#
        ))
        .await?;

    let slot_lsn = parse_single_row(&res, "confirmed_flush_lsn");
    client
        .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
        .await?;
    let (slot_lsn, snapshot_lsn, temp_slot): (PgLsn, PgLsn, _) = match slot_lsn {
        Ok(slot_lsn) => {
            // The main slot already exists which means we can't use it for the snapshot. So
            // we'll create a temporary replication slot in order to both set the transaction's
            // snapshot to be a consistent point and also to find out the LSN that the snapshot
            // is going to run at.
            //
            // When this happens we'll most likely be snapshotting at a later LSN than the slot
            // which we will take care below by rewinding.
            let temp_slot = uuid::Uuid::new_v4().to_string().replace('-', "");
            let res = client
                .simple_query(&format!(
                    r#"CREATE_REPLICATION_SLOT {temp_slot} TEMPORARY LOGICAL "pgoutput" USE_SNAPSHOT"#
                ))
                .await?;
            let snapshot_lsn = parse_single_row(&res, "consistent_point")?;
            (slot_lsn, snapshot_lsn, Some(temp_slot))
        }
        Err(_e) => {
            let res = client
                .simple_query(&format!(
                    r#"CREATE_REPLICATION_SLOT {slot} LOGICAL "pgoutput" USE_SNAPSHOT"#
                ))
                .await?;
            let slot_lsn: PgLsn = parse_single_row(&res, "consistent_point")?;
            (slot_lsn, slot_lsn, None)
        }
    };

    let mut stream = Box::pin(produce_snapshot(&client, &source_tables).enumerate());

    while let Some((_i, event)) = stream.as_mut().next().await {
        let (_output, _row) = event?;
    }

    if let Some(temp_slot) = temp_slot {
        client
            .simple_query(&format!("DROP_REPLICATION_SLOT {temp_slot:?}"))
            .await?;
    }
    client.simple_query("COMMIT;").await?;

    // Drop the stream and the client, to ensure that the future `produce_replication` don't
    // conflict with the above processing.
    //
    // Its possible we can avoid dropping the `client` value here, but we do it out of an
    // abundance of caution, as rust-postgres has had curious bugs around this.
    drop(stream);
    drop(client);
    assert!(slot_lsn <= snapshot_lsn);
    if slot_lsn < snapshot_lsn {
        println!(
            "postgres snapshot was at {snapshot_lsn:?} but we need it at {slot_lsn:?}. Rewinding"
        );
        // Our snapshot was too far ahead so we must rewind it by reading the replication
        // stream until the snapshot lsn and emitting any rows that we find with negated diffs
        let replication_stream = produce_replication(
            connection_config.clone(),
            slot,
            publication,
            slot_lsn,
            Arc::new(0.into()),
        )
        .await;
        tokio::pin!(replication_stream);

        while let Some(event) = replication_stream.next().await {
            let serialized_event = serde_json::to_string_pretty(&SerializedXLogDataBody(event?))?;
            let json_obj: Result<tremor_value::Value, _> = serde_json::from_str(&serialized_event);
            match json_obj {
                Ok(value) => {
                    tx.send(value.into_static()).await?;
                }
                Err(e) => {
                    eprintln!("Error deserializing JSON object: {e}");
                    break;
                }
            }
        }
    }
    println!("replication snapshot for source {} succeeded", &source_id);
    _replication_lsn = slot_lsn;
    Ok(())
}
