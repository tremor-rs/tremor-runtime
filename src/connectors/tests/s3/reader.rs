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

use super::super::ConnectorHarness;
use super::{random_bucket_name, spawn_docker, wait_for_s3mock, EnvHelper, IMAGE, TAG};
use crate::connectors::impls::s3;
use crate::connectors::tests::s3::get_client;
use crate::errors::Result;
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::Client;
use serial_test::serial;
use testcontainers::{clients, images::generic::GenericImage};
use tremor_value::{literal, Value};
use value_trait::ValueAccess;

#[async_std::test]
#[serial(s3)]
async fn connector_s3_no_connection() -> Result<()> {
    let _ = env_logger::try_init();
    let bucket_name = random_bucket_name("no-connection");
    let mut env = EnvHelper::new();
    env.set_var("AWS_ACCESS_KEY_ID", "KEY_NOT_REQD");
    env.set_var("AWS_SECRET_ACCESS_KEY", "KEY_NOT_REQD");
    let connector_yaml = literal!({
        "codec": "binary",
        "config":{
            "aws_region": "eu-central-1",
            "bucket": bucket_name.clone(),
            "endpoint": "http://localhost:9090",
        }
    });

    let harness = ConnectorHarness::new(function_name!(), "s3_reader", &connector_yaml).await?;
    assert!(harness.start().await.is_err());
    Ok(())
}

#[async_std::test]
#[serial(s3)]
async fn connector_s3_no_credentials() -> Result<()> {
    let _ = env_logger::try_init();
    let bucket_name = random_bucket_name("no-credentials");

    let docker = clients::Cli::default();
    let image = GenericImage::new(IMAGE, TAG).with_env_var("initialBuckets", &bucket_name);
    let (_container, http_port, _https_port) = spawn_docker(&docker, image).await;

    wait_for_s3mock(http_port).await?;

    let mut env = EnvHelper::new();
    env.remove_var("AWS_ACCESS_KEY_ID");
    env.remove_var("AWS_SECRET_ACCESS_KEY");
    env.set_var("AWS_REGION", "eu-central-1");
    let endpoint = format!("http://localhost:{http_port}");
    let connector_yaml = literal!({
        "codec": "binary",
        "config":{
            "bucket": bucket_name.clone(),
            "endpoint": endpoint,
        }
    });

    let harness = ConnectorHarness::new(function_name!(), "s3_reader", &connector_yaml).await?;
    assert!(harness.start().await.is_err());

    Ok(())
}

#[async_std::test]
#[serial(s3)]
async fn connector_s3_no_region() -> Result<()> {
    let _ = env_logger::try_init();
    let bucket_name = random_bucket_name("no-region");

    let docker = clients::Cli::default();
    let image = GenericImage::new(IMAGE, TAG).with_env_var("initialBuckets", &bucket_name);
    let (_container, http_port, _https_port) = spawn_docker(&docker, image).await;

    wait_for_s3mock(http_port).await?;

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
            "endpoint": endpoint,
        }
    });

    let harness = ConnectorHarness::new(function_name!(), "s3_reader", &connector_yaml).await?;
    assert!(harness.start().await.is_err());

    Ok(())
}

#[async_std::test]
#[serial(s3)]
async fn connector_s3_no_bucket() -> Result<()> {
    let _ = env_logger::try_init();
    let bucket_name = random_bucket_name("no-bucket");

    let docker = clients::Cli::default();
    let image = GenericImage::new(IMAGE, TAG);
    let (_container, http_port, _https_port) = spawn_docker(&docker, image).await;

    wait_for_s3mock(http_port).await?;

    let mut env = EnvHelper::new();
    env.set_var("AWS_ACCESS_KEY_ID", "KEY_NOT_REQD");
    env.set_var("AWS_SECRET_ACCESS_KEY", "KEY_NOT_REQD");
    let endpoint = format!("http://localhost:{http_port}");
    let connector_yaml = literal!({
        "codec": "binary",
        "config": {
            "aws_region": "eu-central-1",
            "bucket": bucket_name.clone(),
            "endpoint": endpoint
        }
    });
    let harness = ConnectorHarness::new(function_name!(), "s3_reader", &connector_yaml).await?;
    assert!(harness.start().await.is_err());

    Ok(())
}

#[async_std::test]
#[serial(s3)]
async fn connector_s3_reader() -> Result<()> {
    let _ = env_logger::try_init();
    let bucket_name = random_bucket_name("tremor");

    let docker = clients::Cli::default();
    let image = GenericImage::new(IMAGE, TAG).with_env_var("initialBuckets", &bucket_name);
    let (_container, http_port, _https_port) = spawn_docker(&docker, image).await;

    wait_for_s3mock(http_port).await?;

    // insert 100 small files
    let s3_client: Client = get_client(http_port);
    static SMALL_FILE: [u8; 256] = [b'A'; 256];
    for i in 0..100 {
        let _ = s3_client
            .put_object()
            .key(format!("small_{i}"))
            .bucket(bucket_name.as_str())
            .body(ByteStream::from_static(&SMALL_FILE))
            .send()
            .await?;
    }
    // and 10 big ones
    static HUGE_FILE: [u8; 4096] = [b'Z'; 4096];
    for i in 0..10 {
        let _ = s3_client
            .put_object()
            .key(format!("big_{i}"))
            .bucket(bucket_name.as_str())
            .body(ByteStream::from_static(&HUGE_FILE))
            .send()
            .await?;
    }

    let mut env = EnvHelper::new();
    env.set_var("AWS_ACCESS_KEY_ID", "KEY_NOT_REQD");
    env.set_var("AWS_SECRET_ACCESS_KEY", "KEY_NOT_REQD");
    let endpoint = format!("http://localhost:{http_port}");
    let connector_yaml = literal!({
        "codec": "binary",
        "config": {
            "aws_region": "eu-central-1",
            "bucket": bucket_name.clone(),
            "endpoint": endpoint,
            "multipart_threshold": 1000,
            "multipart_chunksize": 1000,
            "max_connections": 2
        }
    });

    let harness = ConnectorHarness::new(
        function_name!(),
        s3::reader::CONNECTOR_TYPE,
        &connector_yaml,
    )
    .await?;
    let out_pipe = harness
        .out()
        .expect("No pipelines connected to out port of s3-reader");
    harness.start().await?;

    for _ in 0..150 {
        let event = out_pipe.get_event().await?;
        let meta = event.data.suffix().meta();
        let s3_meta = meta.get("s3_reader");
        let bucket = s3_meta.get_str("bucket");
        assert_eq!(Some(bucket_name.as_str()), bucket);
        let key = s3_meta.get_str("key").unwrap();
        if key.starts_with("small_") {
            assert_eq!(Some(SMALL_FILE.len()), s3_meta.get_usize("size"));
            assert_eq!(
                Some(SMALL_FILE.as_slice()),
                event.data.suffix().value().as_bytes()
            );
            assert_eq!(Some(&Value::const_null()), s3_meta.get("range"));
        } else {
            assert_eq!(Some(HUGE_FILE.len()), s3_meta.get_usize("size"));
            assert!(s3_meta.get_object("range").is_some());
            let start = s3_meta.get("range").get_usize("start").unwrap();
            let end = s3_meta.get("range").get_usize("end").unwrap();
            assert_eq!(
                Some(&HUGE_FILE.as_slice()[start..=end]),
                event.data.suffix().value().as_bytes()
            );
        }
    }

    let (out, err) = harness.stop().await?;
    assert!(out.is_empty());
    assert!(err.is_empty());

    Ok(())
}
