// Copyright 2022, The Tremor Team
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

use super::ConnectorHarness;
use crate::connectors::impls::psql_repl;
use crate::errors::Result;
use testcontainers::core::WaitFor;
use testcontainers::{clients::Cli as DockerCli, images::generic::GenericImage, RunnableImage};
use tremor_value::literal;

const IMAGE: &str = "postgres";
const TAG: &str = "14-alpine";
fn default_image() -> GenericImage {
    GenericImage::new(IMAGE, TAG)
        // USER and PASSWORD
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "password")
}

#[tokio::test(flavor = "multi_thread")]
async fn connector_pg_repl() -> Result<()> {
    let port = super::free_port::find_free_tcp_port().await?;
    let args = vec!["postgres", "-c", "wal_level=logical"]
        .into_iter()
        .map(ToString::to_string)
        .collect();
    let image = RunnableImage::from((
        default_image()
            .with_wait_for(WaitFor::message_on_stdout(
                "PostgreSQL init process complete; ready for start up",
            ))
            .with_wait_for(WaitFor::message_on_stdout(
                "database system is ready to accept connections",
            )),
        args,
    ))
    .with_mapped_port((port, 5432_u16));
    let docker = DockerCli::default();
    let container = docker.run(image.with_container_name("postgres_test"));
    let port = container.get_host_port_ipv4(5432);

    // Connect to the PostgreSQL server
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=postgres password=password dbname=postgres"),
        tokio_postgres::NoTls,
    )
    .await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Prepare dependencies function (creating table
    // creating transactions
    // creating publication
    // replica identity

    client
        .execute(
            "CREATE OR REPLACE FUNCTION insert_test_pg_table()
        RETURNS void AS $$
        BEGIN
            CREATE TABLE IF NOT EXISTS public.test_pg_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR NOT NULL
            );
            ALTER TABLE IF EXISTS public.test_pg_table REPLICA IDENTITY DEFAULT;
            CREATE PUBLICATION pg_conn_test_publication FOR TABLE public.test_pg_table;

        END;
        $$ LANGUAGE plpgsql;",
            &[],
        )
        .await?;

    // executing the above postgres created function
    client.execute("SELECT insert_test_pg_table()", &[]).await?;

    // creating replication_slot
    client.execute(
        "SELECT PG_CREATE_LOGICAL_REPLICATION_SLOT ('pg_conn_test_resplication_slot', 'pgoutput');",
        &[],
    )
    .await?;

    let port = container.get_host_port_ipv4(5432);

    let connector_config = literal!({
        "reconnect": {
            "retry": {
                "interval_ms": 1000,
                "max_retries": 10
            }
        },
        "config": {
            "host":"127.0.0.1",
            "port": port,
            "username": "postgres",
            "password": "password",
            "dbname":"postgres",
            "publication":"pg_conn_test_publication",
            "replication_slot":"pg_conn_test_resplication_slot"
            }
        }
    );

    let mut harness = ConnectorHarness::new(
        function_name!(),
        &psql_repl::Builder::default(),
        &connector_config,
    )
    .await?;

    harness.start().await?;
    harness.wait_for_connected().await?;

    let publication_event = harness.out()?.get_event().await?;
    let expected_publication_event = literal!({
        "publication_tables": [
            {
                "columns": [
                    {
                        "type_oid": 23_u64,
                        "name": "id",
                        "col_num": 1_u64,
                        "type_mod": -1_i64,
                        "nullable": false
                    },
                    {
                        "type_oid": 1043_u64,
                        "name": "name",
                        "col_num": 2_u64,
                        "type_mod": -1_i64,
                        "nullable": false
                    }
                ],
                "namespace": "public",
                "name": "test_pg_table",
                "keys": [
                    {
                        "name": "test_pg_table_pkey",
                        "cols": [1_u64],
                        "is_primary": true,
                        "nulls_not_distinct": false,
                        "oid": 16393_u64
                    }
                ],
                "oid": 16386_u64
            }
        ]
    });

    let actual_publication_event = publication_event.data.suffix().value();
    // For publication message Assert each field individually, ignoring the oid field(s)
    assert_eq!(
        (
            expected_publication_event["publication_tables"][0]["columns"].clone(),
            expected_publication_event["publication_tables"][0]["namespace"].clone(),
            expected_publication_event["publication_tables"][0]["name"].clone(),
            expected_publication_event["publication_tables"][0]["keys"][0]["name"].clone(),
            expected_publication_event["publication_tables"][0]["keys"][0]["cols"].clone(),
            expected_publication_event["publication_tables"][0]["keys"][0]["is_primary"].clone(),
            expected_publication_event["publication_tables"][0]["keys"][0]["nulls_not_distinct"]
                .clone()
        ),
        (
            actual_publication_event["publication_tables"][0]["columns"].clone(),
            actual_publication_event["publication_tables"][0]["namespace"].clone(),
            actual_publication_event["publication_tables"][0]["name"].clone(),
            actual_publication_event["publication_tables"][0]["keys"][0]["name"].clone(),
            actual_publication_event["publication_tables"][0]["keys"][0]["cols"].clone(),
            actual_publication_event["publication_tables"][0]["keys"][0]["is_primary"].clone(),
            actual_publication_event["publication_tables"][0]["keys"][0]["nulls_not_distinct"]
                .clone()
        ),
        "Publication event fields mismatch"
    );

    client
        .execute(
            "INSERT INTO public.test_pg_table (name) VALUES ('test 1')",
            &[],
        )
        .await?;
    let relation_event = harness.out()?.get_event().await?;
    let expected_relation_event = literal!({
        "timestamp": 737456254721140_u64,
        "data": {
            "rel_id": 16386_u64,
            "namespace": "public",
            "replica_identity": "Default",
            "name": "test_pg_table",
            "columns": [
                {
                    "flags": 1_u64,
                    "name": "id",
                    "type_modifier": -1,
                    "type_id": 23_u64
                },
                {
                    "flags": 0_u64,
                    "name": "name",
                    "type_modifier": -1,
                    "type_id": 1043_u64
                }
            ],
            "type": "RELATION"
        },
        "wal_start": 0_u64,
        "wal_end": 0_u64
    });

    let actual_relation_event = relation_event.data.suffix().value();

    // For Relation message Assert each field individually, ignoring the timestamp, wal_start and wal_end fields
    assert_eq!(
        (
            expected_relation_event["data"]["namespace"].clone(),
            expected_relation_event["data"]["replica_identity"].clone(),
            expected_relation_event["data"]["name"].clone(),
            expected_relation_event["data"]["columns"].clone(),
            expected_relation_event["data"]["type"].clone(),
        ),
        (
            actual_relation_event["data"]["namespace"].clone(),
            actual_relation_event["data"]["replica_identity"].clone(),
            actual_relation_event["data"]["name"].clone(),
            actual_relation_event["data"]["columns"].clone(),
            actual_relation_event["data"]["type"].clone(),
        ),
        "Relation event fields mismatch"
    );

    let insert_event = harness.out()?.get_event().await?;
    let expected_insert_event = literal!({
        "timestamp": 737456254721267_u64,
        "data": {
            "rel_id": 16386_u64,
            "tuple": {
                "data": [
                    "1",
                    "test 1"
                ]
            },
            "type": "INSERT"
        },
        "wal_start": 24303816_u64,
        "wal_end": 24303816_u64
    });
    let actual_insert_event = insert_event.data.suffix().value();
    // For Insert message Assert each field individually, ignoring the timestamp field and rel_id , wal_start and wal_end
    assert_eq!(
        (
            expected_insert_event["data"]["tuple"].clone(),
            expected_insert_event["data"]["type"].clone(),
        ),
        (
            actual_insert_event["data"]["tuple"].clone(),
            actual_insert_event["data"]["type"].clone(),
        ),
        "Insert event fields mismatch"
    );

    client
        .execute(
            "UPDATE public.test_pg_table SET name = 'test 1 updated' WHERE id = 1",
            &[],
        )
        .await?;
    let update_event = harness.out()?.get_event().await?;

    let expected_update_event = literal!({
        "timestamp": 737456254728888_u64,
        "data": {
            "rel_id": 16386_u64,
            "new_tuple": {
                "data": [
                    "1",
                    "test 1 updated"
                ]
            },
            "type": "UPDATE"
        },
        "wal_start": 24304152_u64,
        "wal_end": 24304152_u64
    });
    let actual_update_event = update_event.data.suffix().value();
    // For Update message Assert each field individually, ignoring the timestamp field and rel_id , wal_start and wal_end
    assert_eq!(
        (
            expected_update_event["data"]["new_tuple"].clone(),
            expected_update_event["data"]["type"].clone(),
        ),
        (
            actual_update_event["data"]["new_tuple"].clone(),
            actual_update_event["data"]["type"].clone(),
        ),
        "Update event fields mismatch"
    );
    client
        .execute("DELETE FROM public.test_pg_table WHERE id = 1", &[])
        .await?;

    let delete_event = harness.out()?.get_event().await?;
    let expected_delete_event = literal!({
        "timestamp": 737456254734844_u64,
        "data": {
            "rel_id": 16386_u64,
            "key_tuple": {
                "data": [
                    "1",
                    null
                ]
            },
            "type": "DELETE"
        },
        "wal_start": 24304288_u64,
        "wal_end": 24304288_u64
    });
    let actual_delete_event = delete_event.data.suffix().value();
    // For Delete message Assert each field individually, ignoring the timestamp field
    assert_eq!(
        (
            expected_delete_event["data"]["key_tuple"].clone(),
            expected_delete_event["data"]["type"].clone(),
        ),
        (
            actual_delete_event["data"]["key_tuple"].clone(),
            actual_delete_event["data"]["type"].clone(),
        ),
        "Delete event fields mismatch"
    );
    container.stop();
    Ok(())
}
