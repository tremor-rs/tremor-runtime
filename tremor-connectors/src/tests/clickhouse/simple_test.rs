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

// As we only have a sink implementation, we'll put everything here. Everything
// will eventually follow a structure similar to the s3 connector.

use crate::{
    connectors::{
        impls::clickhouse,
        tests::{clickhouse::utils, free_port, ConnectorHarness},
    },
    errors::{Error, Result},
};

use std::time::{Duration, Instant};

use clickhouse_rs::Pool;
use testcontainers::{clients, core::Port, images::generic::GenericImage, RunnableImage};
use tremor_common::ports::IN;
use tremor_pipeline::{CbAction, Event, EventId};
use tremor_value::literal;

// In this test, we spin up an empty ClickHouse container, plug it to the
// ClickHouse sink and use that sink to save a bunch of super simple events.
// Once all these events are inserted, we use the regular ClickHouse client to
// ensure that all the data was actually written.
#[tokio::test(flavor = "multi_thread")]
async fn simple_insertion() -> Result<()> {
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

    create_table(port, "people").await?;

    // Once the database side is ready, we can spin up our system and plug to
    // our database.

    let db_host = utils::DB_HOST;

    let connector_config = literal!({
        "config": {
            "url": format!("{db_host}:{port}"),
            "compression": "lz4",
            "table": "people",
            "columns": [
                {
                    "name": "age",
                    "type": "UInt64",
                }
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
                    "age": 42u64,
                }
            },
        },
        {
            "data": {
                "value": {
                    "age": 101u64,
                }
            },
        },
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
    let request = "select * from people";
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

    let ages = block
        .rows()
        .map(|row| row.get::<u64, _>("age").map_err(Error::from))
        .collect::<Result<Vec<_>>>()?;

    assert_eq!(ages, [42, 101]);

    container.stop();

    Ok(())
}

async fn create_table(port: u16, table: &str) -> Result<()> {
    let db_host = utils::DB_HOST;
    let db_url = format!("tcp://{db_host}:{port}/");
    let request = format!("create table if not exists {table} ( age UInt64 ) Engine=Memory");

    let pool = Pool::new(db_url);

    let mut client = pool.get_handle().await?;
    client.execute(request).await.map_err(Into::into)
}
