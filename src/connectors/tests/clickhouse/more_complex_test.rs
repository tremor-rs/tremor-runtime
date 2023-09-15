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

// The test in this module is very similar to what is done in `simple_test`.
// The biggest difference is that we will try as much datatype as possible.

// The table will contain the following columns:
//
// ---------------------------------
// | Column name | Type            |
// |-------------|-----------------|
// | a           | UInt8           |
// | b           | UInt16          |
// | c           | UInt32          |
// | d           | UInt64          |
// | e           | Int8            |
// | f           | Int16           |
// | g           | Int32           |
// | h           | Int64           |
// | i           | String          |
// | j           | DateTime        |
// | k           | DateTime64(0)   |
// | l           | DateTime64(3)   |
// | m           | DateTime64(6)   |
// | n           | DateTime64(9)   |
// | o           | IPv4            |
// | p           | IPv6            |
// | q           | Uuid            |
// | r           | Nullable(UInt8) |
// | s           | Array(String)   |
// ---------------------------------
//
// As the `DateTime64` encoding is not implemented in `clickhouse-rs`, the
// columns `k`, `l`, `m` and `n` commented out everywhere in the file.

use std::{
    net::{Ipv4Addr, Ipv6Addr},
    time::{Duration, Instant},
};

use chrono::{DateTime, NaiveDateTime, Utc};
use chrono_tz::Tz;
use clickhouse_rs::Pool;
use testcontainers::{clients, core::Port, images::generic::GenericImage, RunnableImage};
use tremor_common::ports::IN;
use tremor_pipeline::{CbAction, Event, EventId};
use tremor_value::literal;

use super::utils;
use crate::{
    connectors::{
        impls::clickhouse,
        tests::{free_port, ConnectorHarness},
    },
    errors::{Error, Result},
};

macro_rules! assert_row_equals {
    (
        $row:expr,
        {
            $( $field_name:ident: $ty:ty = $value:expr ),* $(,)?
        }
    ) => {
        $({
            let $field_name = $row.get::<$ty, _>(stringify!($field_name))?;
            assert_eq!($field_name, $value);
        })*
    };
}

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread")]
async fn test() -> Result<()> {
    let _: std::result::Result<_, _> = env_logger::try_init();

    let docker = clients::Cli::docker();

    // The following lines spin up a regular ClickHouse container and wait for
    // the database to be up and running.

    let image = GenericImage::new(utils::CONTAINER_NAME, utils::CONTAINER_VERSION);
    // We want to access the container from the host, so we need to make the
    // corresponding port available.
    let local = free_port::find_free_tcp_port().await?;
    let port_to_expose = Port {
        internal: utils::SERVER_PORT,
        local,
    };
    let image = RunnableImage::from(image).with_mapped_port(port_to_expose);
    let container = docker.run(image);
    let port = container.get_host_port_ipv4(9000);
    utils::wait_for_ok(port).await?;

    // Once the database is available, we use the regular client to create the
    // database we're going to use

    create_table(port).await?;

    // Once the database side is ready, we can spin up our system and plug to
    // our database.

    let db_host = utils::DB_HOST;

    let connector_config = literal!({
        "config": {
            "url": format!("{db_host}:{port}"),
            "compression": "lz4",
            "table": "t",
            "columns": [
                {
                    "name": "a",
                    "type": "UInt8",
                },
                {
                    "name": "b",
                    "type": "UInt16",
                },
                {
                    "name": "c",
                    "type": "UInt32",
                },
                {
                    "name": "d",
                    "type": "UInt64",
                },
                {
                    "name": "e",
                    "type": "Int8",
                },
                {
                    "name": "f",
                    "type": "Int16",
                },
                {
                    "name": "g",
                    "type": "Int32",
                },
                {
                    "name": "h",
                    "type": "Int64",
                },
                {
                    "name": "i",
                    "type": "String",
                },
                {
                    "name": "j",
                    "type": "DateTime",
                },
                // {
                //     "name": "k",
                //     "type": "DateTime64(0)",
                // },
                // {
                //     "name": "l",
                //     "type": "DateTime64(3)",
                // },
                // {
                //     "name": "m",
                //     "type": "DateTime64(6)",
                // },
                // {
                //     "name": "n",
                //     "type": "DateTime64(9)",
                // },
                {
                    "name": "o",
                    "type": "IPv4",
                },
                {
                    "name": "p",
                    "type": "IPv6",
                },
                {
                    "name": "q",
                    "type": "UUID",
                },
                {
                    "name": "r",
                    "type": { "Nullable": "UInt8" },
                },
                {
                    "name": "s",
                    "type": { "Array": "String" },
                },
            ]
        },
    });
    let mut harness =
        ConnectorHarness::new("clickhouse", &clickhouse::Builder {}, &connector_config).await?;

    harness.start().await?;
    harness.wait_for_connected().await?;
    harness.consume_initial_sink_contraflow().await?;

    // Once everything is up and running, we can finally feed our system with
    // data that will be inserted in the database.

    let batched_data = literal!([
        {
            "data": {
                "value": {
                    "a": 42,
                    "b": 291,
                    "c": 64_000,
                    "d": 66_000,
                    "e": -1,
                    "f": -125,
                    "g": -32_000,
                    "h": -33_000,
                    "i": "hello",
                    "j": 1_634_400_000,
                    // "k": 1634400000,
                    // "l": 1634400000_000_u64,
                    // "m": 1634400000_000_000_u64,
                    // "n": 1634400000_000_000_000_u64,
                    "o": [192, 168, 1, 10],
                    "p": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                    "q": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                    "r": 42,
                    "s": ["hello", ",", "world"],
                }
            },
        },
        {
            "data": {
                "value": {
                    "a": 42,
                    "b": 291,
                    "c": 64_000,
                    "d": 66_000,
                    "e": -1,
                    "f": -125,
                    "g": -32_000,
                    "h": -33_000,
                    "i": "hello",
                    "j": 1_634_400_000,
                    // "k": 1634400000,
                    // "l": 1634400000_000_u64,
                    // "m": 1634400000_000_000_u64,
                    // "n": 1634400000_000_000_000_u64,
                    "o": "192.168.1.10",
                    "p": "0001:0203:0405:0607:0809:0A0B:0C0D:0E0F",
                    "q": "000102030405060708090A0B0C0D0E0F",
                    // We don't define r - it should be null
                    "s": ["hello", ",", "world"],
                }
            }
        }
    ]);

    let batched_meta = literal!({});

    let batched_id = EventId::new(0, 0, 1, 1);
    let event = Event {
        id: batched_id.clone(),
        is_batch: true,
        transactional: true,
        data: (batched_data, batched_meta).into(),
        ..Event::default()
    };

    harness.send_to_sink(event, IN).await?;

    // Once the data has been inserted, we wait for the "I handled everything"
    // signal and check its properties.

    let cf = harness.get_pipe(IN)?.get_contraflow().await?;
    assert_eq!(CbAction::Ack, cf.cb);
    assert_eq!(batched_id, cf.id);

    // Now that we finished everything on the Tremor side, we can just stop
    // the testing harness.

    harness.stop().await?;

    // We can now connect to the database on our own, and ensure that it has
    // all the data we expect it to have.

    // There are plenty of processes asynchronously running here. We need to
    // ensure that all the data has been inserted in the database before
    // checking its value.
    //
    // A good way to do that is to continuously fetch data from the database,
    // until we get the correct amount of data (or timeout with an error),
    // and then check that the data contains the correct values.
    let mut client = Pool::new(format!("tcp://{db_host}:{port}/"))
        .get_handle()
        .await?;
    let request = "select * from t";
    let delay = Duration::from_secs(1);
    let start = Instant::now();
    let wait_for = Duration::from_secs(60);

    // This is the wait loop we were talking about earlier.
    let block = loop {
        let block = client.query(request).fetch_all().await?;

        // Test if we have all the data we need.
        if block.row_count() == 2 {
            break block;
        }

        // Test for timeout.
        if start.elapsed() > wait_for {
            let max_time = wait_for.as_secs();
            error!("We waited for more than {max_time}");
            return Err(Error::from(
                "Timeout while waiting for all the data to be available",
            ));
        }

        tokio::time::sleep(delay).await;
    };

    // Now that we fetched everything we want, we can extract the meaningful
    // data.

    for (idx, row) in block.rows().enumerate() {
        assert_row_equals!(
            row,
            {
                a: u8 = 42,
                b: u16 = 291,
                c: u32 = 64_000,
                d: u64 = 66_000,
                e: i8 = -1,
                f: i16 = -125,
                g: i32 = -32_000,
                h: i64 = -33_000,
                i: &str = "hello",
                j: DateTime<Tz> = DateTime::<Utc>::from_naive_utc_and_offset(NaiveDateTime::from_timestamp_opt(1_634_400_000, 0).expect("valid timestamp literal"), Utc),
                // k: DateTime<Tz> = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(1_634_400_000, 0), Utc),
                // l: DateTime<Tz> = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(1_634_400_000, 0), Utc),
                // m: DateTime<Tz> = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(1_634_400_000, 0), Utc),
                // n: DateTime<Tz> = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(1_634_400_000, 0), Utc),
                o: Ipv4Addr = Ipv4Addr::new(192, 168, 1, 10),
                p: Ipv6Addr = Ipv6Addr::from([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
                // Any type here must implement the FromSql trait. The problem is
                // that clickhouse-rs uses an old version of Uuid. This means that
                // any reference to Uuid here will lead to the newer one, on which
                // FromSql is not implemented.
                //
                // A good solution would be either to re-export the uuid dependency
                // from the clickhouse-rs crate (non-breaking change), or to update
                // the uuid dependency of clickhouse-rs.
                //
                // q: Uuid = Uuid::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
                r: Option<u8> = if idx == 0 { Some(42) } else { None },
                s: Vec<String> = vec!["hello".to_string(), ",".to_string(), "world".to_string()],
            }
        );
    }

    container.stop();

    Ok(())
}

async fn create_table(port: u16) -> Result<()> {
    let db_host = utils::DB_HOST;
    let db_url = format!("tcp://{db_host}:{port}/");
    let request = "create table if not exists t (
            a UInt8,
            b UInt16,
            c UInt32,
            d UInt64,
            e Int8,
            f Int16,
            g Int32,
            h Int64,
            i String,
            j DateTime,
            -- k DateTime64(0, \"etc/UTC\"),
            -- l DateTime64(3, \"etc/UTC\"),
            -- m DateTime64(6, \"etc/UTC\"),
            -- n DateTime64(9, \"etc/UTC\"),
            o IPv4,
            p IPv6,
            q UUID,
            r Nullable(UInt8),
            s Array(String)
        ) Engine=Memory";

    let pool = Pool::new(db_url);

    let mut client = pool.get_handle().await?;
    client.execute(request).await.map_err(Into::into)
}
