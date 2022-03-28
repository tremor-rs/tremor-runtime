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
use super::{random_bucket_name, spawn_docker, wait_for_s3mock, EnvHelper, SignalHandler};
use crate::errors::Result;
use serial_test::serial;
use testcontainers::{clients, images::generic::GenericImage};
use tremor_value::literal;

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

    let harness = ConnectorHarness::new("s3-reader", &connector_yaml).await?;
    assert!(harness.start().await.is_err());
    Ok(())
}

#[async_std::test]
#[serial(s3)]
async fn connector_s3_no_credentials() -> Result<()> {
    let _ = env_logger::try_init();
    let bucket_name = random_bucket_name("no-credentials");

    let docker = clients::Cli::default();
    let image = GenericImage::new("adobe/s3mock").with_env_var("initialBuckets", &bucket_name);
    let (container, http_port, _https_port) = spawn_docker(&docker, image).await;

    // signal handling - stop and rm the container, even if we quit the test in the middle of everything
    let _signal_handler = SignalHandler::new(container.id().to_string())?;

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

    let harness = ConnectorHarness::new("s3-reader", &connector_yaml).await?;
    assert!(harness.start().await.is_err());

    Ok(())
}

#[async_std::test]
#[serial(s3)]
async fn connector_s3_no_region() -> Result<()> {
    let _ = env_logger::try_init();
    let bucket_name = random_bucket_name("no-region");

    let docker = clients::Cli::default();
    let image = GenericImage::new("adobe/s3mock").with_env_var("initialBuckets", &bucket_name);
    let (container, http_port, _https_port) = spawn_docker(&docker, image).await;

    // signal handling - stop and rm the container, even if we quit the test in the middle of everything
    let _signal_handler = SignalHandler::new(container.id().to_string())?;

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

    let harness = ConnectorHarness::new("s3-reader", &connector_yaml).await?;
    assert!(harness.start().await.is_err());

    Ok(())
}

#[async_std::test]
#[serial(s3)]
async fn connector_s3_no_bucket() -> Result<()> {
    let _ = env_logger::try_init();
    let bucket_name = random_bucket_name("no-bucket");

    let docker = clients::Cli::default();
    let image = GenericImage::new("adobe/s3mock");
    let (container, http_port, _https_port) = spawn_docker(&docker, image).await;

    // signal handling - stop and rm the container, even if we quit the test in the middle of everything
    let _signal_handler = SignalHandler::new(container.id().to_string())?;

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
    let harness = ConnectorHarness::new("s3-reader", &connector_yaml).await?;
    assert!(harness.start().await.is_err());

    Ok(())
}

#[async_std::test]
#[serial(s3)]
async fn connector_s3_reader() -> Result<()> {
    let _ = env_logger::try_init();
    let bucket_name = random_bucket_name("tremor");

    let docker = clients::Cli::default();
    let image = GenericImage::new("adobe/s3mock").with_env_var("initialBuckets", &bucket_name);
    let (container, http_port, _https_port) = spawn_docker(&docker, image).await;

    // signal handling - stop and rm the container, even if we quit the test in the middle of everything
    let _signal_handler = SignalHandler::new(container.id().to_string())?;

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
            "endpoint": endpoint,
            "prefix": "/snot",
            "multipart_threshold": 1000,
            "max_connections": 2
        }
    });

    let harness = ConnectorHarness::new("s3-reader", &connector_yaml).await?;
    let _out_pipe = harness
        .out()
        .expect("No pipelines connected to out port of s3-reader");
    harness.start().await?;

    // TODO: check for events from out pipeline
    let (out, err) = harness.stop().await?;
    assert!(out.is_empty());
    assert!(err.is_empty());

    Ok(())
}
