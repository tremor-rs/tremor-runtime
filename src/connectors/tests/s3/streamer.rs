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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or imelied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Read;

use super::super::ConnectorHarness;
use super::{
    create_bucket, get_client, random_bucket_name, spawn_docker, wait_for_s3, MINIO_REGION,
    MINIO_ROOT_PASSWORD, MINIO_ROOT_USER,
};
use crate::connectors::impls::s3;
use crate::connectors::utils::EnvHelper;
use crate::errors::Result;
use async_std::prelude::FutureExt;
use aws_sdk_s3::Client;
use bytes::Buf;
use rand::{distributions::Alphanumeric, Rng};
use serial_test::serial;
use std::time::Duration;
use testcontainers::clients;
use tremor_common::ports::IN;
use tremor_pipeline::{CbAction, Event, EventId};
use tremor_value::{literal, value};
use value_trait::{Builder, Mutable, ValueAccess};

#[async_std::test]
#[serial(s3, timeout_ms = 600000)]
async fn no_connection() -> Result<()> {
    let _ = env_logger::try_init();
    let bucket_name = random_bucket_name("no-connection");
    let mut env = EnvHelper::new();
    env.set_var("AWS_ACCESS_KEY_ID", MINIO_ROOT_USER);
    env.set_var("AWS_SECRET_ACCESS_KEY", MINIO_ROOT_PASSWORD);
    env.set_var("AWS_DEFAULT_REGION", MINIO_REGION);
    let connector_yaml = literal!({
        "codec": "binary",
        "config":{
            "bucket": bucket_name.clone(),
            "url": "http://localhost:9090",
            "mode": "yolo"
        }
    });

    let harness = ConnectorHarness::new(
        function_name!(),
        &s3::streamer::Builder::default(),
        &connector_yaml,
    )
    .await?;
    assert!(harness.start().await.is_err());
    Ok(())
}

#[async_std::test]
#[serial(s3, timeout_ms = 600000)]
async fn no_credentials() -> Result<()> {
    let _ = env_logger::try_init();
    let bucket_name = random_bucket_name("no-credentials");

    let docker = clients::Cli::default();
    let (_container, http_port) = spawn_docker(&docker).await;

    wait_for_s3(http_port).await?;
    create_bucket(&bucket_name, http_port).await?;

    let mut env = EnvHelper::new();
    env.remove_var("AWS_ACCESS_KEY_ID");
    env.remove_var("AWS_SECRET_ACCESS_KEY");
    env.set_var("AWS_REGION", MINIO_REGION);
    let endpoint = format!("http://localhost:{http_port}");
    let connector_yaml = literal!({
        "codec": "binary",
        "config":{
            "bucket": bucket_name.clone(),
            "url": endpoint,
            "mode": "consistent"
        }
    });

    let harness = ConnectorHarness::new(
        function_name!(),
        &s3::streamer::Builder::default(),
        &connector_yaml,
    )
    .await?;
    assert!(harness.start().await.is_err());

    Ok(())
}

#[async_std::test]
#[serial(s3, timeout_ms = 600000)]
async fn no_region() -> Result<()> {
    let _ = env_logger::try_init();
    let bucket_name = random_bucket_name("no-region");

    let docker = clients::Cli::default();
    let (_container, http_port) = spawn_docker(&docker).await;

    wait_for_s3(http_port).await?;
    create_bucket(&bucket_name, http_port).await?;

    let mut env = EnvHelper::new();
    env.set_var("AWS_ACCESS_KEY_ID", "snot");
    env.set_var("AWS_SECRET_ACCESS_KEY", "badger");
    env.remove_var("AWS_REGION");
    env.remove_var("AWS_DEFAULT_REGION");

    let endpoint = format!("http://localhost:{http_port}");
    let connector_yaml = literal!({
        "codec": "binary",
        "config":{
            "bucket": bucket_name.clone(),
            "url": endpoint,
            "mode": "yolo"
        }
    });

    let harness = ConnectorHarness::new(
        function_name!(),
        &s3::streamer::Builder::default(),
        &connector_yaml,
    )
    .await?;
    assert!(harness.start().await.is_err());

    Ok(())
}

#[async_std::test]
#[serial(s3, timeout_ms = 600000)]
async fn no_bucket() -> Result<()> {
    let _ = env_logger::try_init();
    let bucket_name = random_bucket_name("no-bucket");

    let docker = clients::Cli::default();
    let (_container, http_port) = spawn_docker(&docker).await;

    wait_for_s3(http_port).await?;

    let mut env = EnvHelper::new();
    env.set_var("AWS_ACCESS_KEY_ID", MINIO_ROOT_USER);
    env.set_var("AWS_SECRET_ACCESS_KEY", MINIO_ROOT_PASSWORD);
    env.set_var("AWS_DEFAULT_REGION", MINIO_REGION);
    let endpoint = format!("http://localhost:{http_port}");
    let connector_yaml = literal!({
        "codec": "binary",
        "config": {
            "bucket": bucket_name.clone(),
            "url": endpoint,
            "mode": "consistent"
        }
    });
    let harness = ConnectorHarness::new(
        function_name!(),
        &s3::streamer::Builder::default(),
        &connector_yaml,
    )
    .await?;
    assert!(harness.start().await.is_err());

    Ok(())
}

#[async_std::test]
#[serial(s3, timeout_ms = 600000)]
async fn connector_s3_consistent() -> Result<()> {
    let _ = env_logger::try_init();

    let bucket_name = random_bucket_name("tremor");

    let docker = clients::Cli::default();
    let (_container, http_port) = spawn_docker(&docker).await;

    wait_for_s3(http_port).await?;
    create_bucket(&bucket_name, http_port).await?;

    let s3_client = get_client(http_port);

    let mut env = EnvHelper::new();
    env.set_var("AWS_ACCESS_KEY_ID", MINIO_ROOT_USER);
    env.set_var("AWS_SECRET_ACCESS_KEY", MINIO_ROOT_PASSWORD);
    env.set_var("AWS_REGION", MINIO_REGION);

    // connector setup
    let connector_yaml = literal!({
        "codec": "binary",
        "config":{
            "bucket": bucket_name.clone(),
            "url": format!("http://localhost:{http_port}"),
            "mode": "consistent"
        }
    });

    let harness = ConnectorHarness::new(
        function_name!(),
        &s3::streamer::Builder::default(),
        &connector_yaml,
    )
    .await?;
    let in_pipe = harness
        .get_pipe(IN)
        .expect("No pipe connectored to port IN");
    harness.start().await?;
    harness.wait_for_connected().await?;
    harness.consume_initial_sink_contraflow().await?;

    let (unbatched_event, unbatched_value) = get_unbatched_event();
    send_to_sink(&harness, &unbatched_event).await?;
    // upload not yet finished, hence no ack, also not transactional
    let res = in_pipe.get_contraflow_events()?;
    assert!(res.is_empty(), "Expected no contraflow, got: {:?}", &res);

    // batched event with 3 new keys, but not transactional
    let (batched_event, batched_value_0, batched_value_1, batched_value_2) = get_batched_event();
    send_to_sink(&harness, &batched_event).await?;

    let res = in_pipe.get_contraflow_events()?;
    assert!(res.is_empty(), "Expected no contraflow, got: {:?}", &res);

    // first transactional event
    let (large_unbatched_event, large_unbatched_bytes) = large_unbatched_event();
    send_to_sink(&harness, &large_unbatched_event).await?;

    // upload not yet finished, no ack yet
    let res = in_pipe.get_contraflow_events()?;
    assert!(res.is_empty(), "Expected no contraflow, got: {:?}", &res);

    let (large_batched_event, large_batched_value) = large_batched_event();
    send_to_sink(&harness, &large_batched_event).await?;

    let cf_event = in_pipe
        .get_contraflow()
        .timeout(Duration::from_secs(5))
        .await??;
    assert_eq!(CbAction::Ack, cf_event.cb);
    assert!(cf_event.id.is_tracking(&large_unbatched_event.id));
    assert!(!cf_event.id.is_tracking(&large_batched_event.id)); // not yet

    harness.stop().await?;

    // fetch the commited events from mock s3

    // verify a small unbatched event.
    let unbatched_value_recv = get_object_value(&s3_client, &bucket_name, "unbatched_key").await;
    assert_eq!(unbatched_value, unbatched_value_recv);

    // verify small and different batched events.
    let batched_value_0_recv = get_object_value(&s3_client, &bucket_name, "batched_key0").await;
    assert_eq!(batched_value_0, batched_value_0_recv);

    let batched_value_1_recv = get_object_value(&s3_client, &bucket_name, "batched_key1").await;
    assert_eq!(batched_value_1, batched_value_1_recv);

    let batched_value_2_recv = get_object_value(&s3_client, &bucket_name, "batched_key2").await;
    assert_eq!(batched_value_2, batched_value_2_recv);

    // verify a large unbatched_event. Checked directly against the bytes.
    let large_unbatched_bytes_recv =
        get_object(&s3_client, &bucket_name, "large_unbatched_event").await;
    assert_eq!(large_unbatched_bytes, large_unbatched_bytes_recv);

    // verify a large batched event having multiples keys for the same field.
    let large_batched_value_recv =
        get_object(&s3_client, &bucket_name, "large_batched_event").await;
    assert_eq!(large_batched_value, large_batched_value_recv);

    Ok(())
}

async fn send_to_sink(harness: &ConnectorHarness, event: &Event) -> Result<()> {
    harness.send_to_sink(event.clone(), IN).await?;
    Ok(())
}

async fn get_object(client: &Client, bucket: &str, key: &str) -> Vec<u8> {
    let resp = client
        .get_object()
        .bucket(bucket.clone())
        .key(key.clone())
        .send()
        .await
        .unwrap();

    let mut v = Vec::new();
    let read_bytes = resp
        .body
        .collect()
        .await
        .unwrap()
        .reader()
        .read_to_end(&mut v)
        .unwrap();
    v.truncate(read_bytes);
    v
}

async fn get_object_value(client: &Client, bucket: &str, key: &str) -> value::Value<'static> {
    let mut v = get_object(client, bucket, key).await;
    let obj = value::parse_to_value(&mut v).unwrap();
    return obj.into_static();
}

fn get_unbatched_event() -> (Event, value::Value<'static>) {
    let data = literal!({
        "numField": 12.5,
        "strField": "string",
        "listField": [true, false],
        "nestedField" : {
            "nested": true
        },
    });
    let meta = literal!({
        "s3_streamer": {
            "name": "unbatched_key"
        }
    });

    (
        Event {
            id: EventId::from_id(0, 0, 1),
            data: (data.clone(), meta).into(),
            transactional: false,
            ..Event::default()
        },
        data,
    )
}

// handle seperately because 3 seperate events.
fn get_batched_event() -> (
    Event,
    value::Value<'static>,
    value::Value<'static>,
    value::Value<'static>,
) {
    let batched_data = literal!([
        {
            "data": {
                "value": {
                    "field1": 0.1,
                    "field2": "another_string",
                    "field3": [],
                },
                "meta": {
                    "s3_streamer": {
                        "name": "batched_key0"
                    }
                }
            }
        },
        {
            "data": {
                "value": {
                    "field3": 12,
                    "field4": {
                        "nested": false,
                        "actually": "no"
                    }
                },
                "meta": {
                    "s3_streamer": {
                        "name": "batched_key1"
                    }
                }
            }
           },
        {
            "data": {
                "value": {
                    "some_more_fields" :1,
                    "vec_field": ["elem1", "elem2", "elem3"],
                },
                "meta": {
                    "s3_streamer": {
                        "name": "batched_key2"
                    }
                }
            }
        }
    ]);

    let batched_meta = literal!({});
    let batched_id = EventId::new(0, 0, 2, 2);
    (
        Event {
            id: batched_id,
            is_batch: true,
            transactional: false,
            data: (batched_data.clone(), batched_meta).into(),
            ..Event::default()
        },
        batched_data[0]
            .get("data")
            .unwrap()
            .get("value")
            .unwrap()
            .clone(),
        batched_data[1]
            .get("data")
            .unwrap()
            .get("value")
            .unwrap()
            .clone(),
        batched_data[2]
            .get("data")
            .unwrap()
            .get("value")
            .unwrap()
            .clone(),
    )
}

fn large_unbatched_event() -> (Event, Vec<u8>) {
    let ten_mbs = 10 * 1024 * 1024;
    let large_text = random_alphanum_string(ten_mbs).into_bytes();
    let large_data = value::Value::Bytes(large_text.clone().into());

    let large_meta = literal!({
        "s3_streamer": {
            "name": "large_unbatched_event"
        }
    });

    (
        Event {
            id: EventId::from_id(0, 0, 3),
            data: (large_data.clone(), large_meta).into(),
            transactional: true,
            ..Event::default()
        },
        large_text,
    )
}

fn large_batched_event() -> (Event, Vec<u8>) {
    let mut batched_data = value::Value::array_with_capacity(1000);
    let mut batched_value = Vec::new();

    let batched_meta = literal!({});

    for idx in 0..1000 {
        let field = format!("field{}", idx);
        let field_val = random_alphanum_string(10000);

        let lit = literal! ({field.clone():field_val.clone()});
        batched_value.append(&mut simd_json::to_vec(&lit).unwrap());

        let event = literal!({
            "data": {
                "value": lit,
                "meta" : {
                    "s3_streamer" : {
                        "name": "large_batched_event",
                    }
                }
            }
        });

        batched_data.push(event).unwrap();
    }

    (
        Event {
            id: EventId::from_id(0, 0, 4),
            data: (batched_data, batched_meta).into(),
            transactional: true,
            is_batch: true,
            ..Event::default()
        },
        batched_value,
    )
}

fn random_alphanum_string(str_size: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(str_size)
        .map(char::from)
        .collect()
}
