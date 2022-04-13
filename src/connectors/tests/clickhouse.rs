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
    connectors::tests::free_port,
    errors::{Error, Result},
};

use std::time::Duration;

use async_std::stream::StreamExt;
use clickhouse_rs::Pool;
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_async_std::Signals;
use testcontainers::{clients, core::Port, images::generic::GenericImage, Docker, RunArgs};
use tremor_common::ports::IN;
use tremor_pipeline::{Event, EventId};
use tremor_script::literal;

use super::ConnectorHarness;

const CONTAINER_NAME: &str = "clickhouse/clickhouse-server";
const CONTAINER_VERSION: &str = "22.3.3.44";
const SERVER_PORT: u16 = 9000;

const DB_HOST: &str = "127.0.0.1";

#[async_std::test]
async fn simple_insertion() -> Result<()> {
    let _ = env_logger::try_init();

    let docker = clients::Cli::default();
    let image = GenericImage::new(format!("{CONTAINER_NAME}:{CONTAINER_VERSION}"));

    // We want to access the container from the host, so we need to make the
    // corresponding port available.
    let local = free_port::find_free_tcp_port().await?;
    let port_to_expose = Port {
        internal: SERVER_PORT,
        local,
    };
    let container =
        docker.run_with_args(image, RunArgs::default().with_mapped_port(port_to_expose));

    let container_id = container.id().to_string();
    let mut signals = Signals::new(&[SIGTERM, SIGINT, SIGQUIT])?;
    let signal_handle = signals.handle();
    let signal_handler_task = async_std::task::spawn(async move {
        let signal_docker = clients::Cli::default();
        while let Some(_signal) = signals.next().await {
            signal_docker.stop(container_id.as_str());
            signal_docker.rm(container_id.as_str());
        }
    });
    let port = container.get_host_port(9000).unwrap();

    wait_for_ok(port).await.unwrap();

    create_table(port, "people").await.unwrap();

    let connector_config = literal!({
        "config": {
            "host": DB_HOST,
            "port": port,
            "database": "",
        },
    });

    let harness = ConnectorHarness::new("clickhouse", &connector_config).await?;

    let in_pipe = harness.get_pipe(IN).expect("No pipe connected to port IN");

    harness.start().await?;
    harness.wait_for_connected().await?;

    let batched_data = literal!([
        {
            "data": {
                "value": {
                    "age": 42u8,
                }
            },
        },
        {
            "data": {
                "value": {
                    "age": 101u8,
                }
            },
        },
    ]);

    let batched_meta = literal!({});

    let event = Event {
        id: EventId::new(0, 0, 1, 1),
        is_batch: true,
        transactional: true,
        data: (batched_data, batched_meta).into(),
        ..Event::default()
    };

    harness.send_to_sink(event, IN).await?;
    // TODO: check for some kind of ack or something i guess?

    info!("SUCCESSFULLY INSERTED DATA");

    // Now that we have sent some events to the sink, let's check that
    // everything was properly inserted in the database.

    harness.stop().await?;

    // Please don't judge me too much - this is a yolo test.
    async_std::task::sleep(Duration::from_secs(5)).await;

    let mut client = Pool::new(format!("tcp://{DB_HOST}:{port}/"))
        .get_handle()
        .await?;

    let request = "select * from people";

    let block = client.query(request).fetch_all().await?;

    let ages = block
        .rows()
        .map(|row| row.get::<u8, _>("age").map_err(Error::from))
        .collect::<Result<Vec<_>>>()?;

    info!("CHECKING EQUALITY");
    assert_eq!(ages, [42, 101]);
    info!("IT WORKED SOMEHOW!");

    container.stop();

    Ok(())
}

// Blocks the task until calling GET on `url` returns an HTTP 200.
async fn wait_for_ok(port: u16) -> Result<()> {
    // Actually we don't do what you expect.
    //
    // Let's suppose it takes less than 30 seconds for the database to start.
    // We'll do more accurate in the future.
    async_std::task::sleep(Duration::from_secs(30)).await;

    Ok(())
}

async fn create_table(port: u16, table: &str) -> Result<()> {
    let db_url = format!("tcp://{DB_HOST}:{port}/");
    let request = format!("create table if not exists {table} ( age UInt8 ) Engine=Memory");

    let pool = Pool::new(db_url);

    let mut client = pool.get_handle().await?;
    client.execute(request).await.map_err(Into::into)
}
