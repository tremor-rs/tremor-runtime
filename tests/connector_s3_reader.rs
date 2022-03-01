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

mod connectors;

#[cfg(feature = "integration")]
extern crate log;
#[cfg(feature = "integration")]
mod test {
    // use async_std::stream::StreamExt;
    use rand::{distributions::Alphanumeric, Rng};
    use std::io::Read;
    use std::time::{Duration, Instant};
    // use bytes::buf::buf_impl::Buf;
    use bytes::Buf;
    use serial_test::serial;

    use crate::connectors::{ConnectorHarness, EnvHelper, SignalHandler, TestPipeline};
    use testcontainers::images::generic::GenericImage;
    use testcontainers::{clients, Container};
    use testcontainers::{Docker, RunArgs};

    use tremor_common::url::ports::IN;
    use tremor_pipeline::{CbAction, Event, EventId};
    use tremor_runtime::errors::{Error, Result};
    use tremor_value::{literal, value};
    use value_trait::{Builder, Mutable, ValueAccess};

    use aws_sdk_s3 as s3;
    use s3::client::Client as S3Client;
    use s3::{Credentials, Endpoint, Region};

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

        let harness = ConnectorHarness::new("s3-reader", connector_yaml).await?;
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

        let harness = ConnectorHarness::new("s3-reader", connector_yaml).await?;
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

        let harness = ConnectorHarness::new("s3-reader", connector_yaml).await?;
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
        let harness = ConnectorHarness::new("s3-reader", connector_yaml).await?;
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

        let client = get_client(http_port);
        // FIXME: populate the bucket

        let harness = ConnectorHarness::new("s3-reader", connector_yaml).await?;
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

    fn get_client(http_port: u16) -> S3Client {
        let s3_config = s3::config::Config::builder()
            .credentials_provider(Credentials::new(
                "KEY_NOT_REQD",
                "KEY_NOT_REQD",
                None,
                None,
                "Environment",
            ))
            .region(Region::new("ap-south-1"))
            .endpoint_resolver(Endpoint::immutable(
                format!("http://localhost:{http_port}").parse().unwrap(),
            ))
            .build();

        S3Client::from_conf(s3_config)
    }

    async fn wait_for_s3mock(port: u16) -> Result<()> {
        let s3_client: S3Client = get_client(port);

        let wait_for = Duration::from_secs(30);
        let start = Instant::now();

        while let Err(e) = s3_client.list_buckets().send().await {
            if start.elapsed() > wait_for {
                return Err(Error::from(e).chain_err(|| "Waiting for mock-s3 container timed out"));
            }

            async_std::task::sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }

    async fn spawn_docker<'d, D: Docker>(
        docker: &'d D,
        image: GenericImage,
    ) -> (Container<'d, D, GenericImage>, u16, u16) {
        let http_port = ConnectorHarness::find_free_tcp_port().await;
        let https_port = ConnectorHarness::find_free_tcp_port().await;
        let container = docker.run_with_args(
            image,
            RunArgs::default()
                .with_mapped_port((http_port, 9090_u16))
                .with_mapped_port((https_port, 9191_u16)),
        );

        (container, http_port, https_port)
    }

    fn random_bucket_name(prefix: &str) -> String {
        format!(
            "{}-{}",
            prefix,
            rand::thread_rng()
                .sample_iter(Alphanumeric)
                .map(char::from)
                .take(10)
                .collect::<String>()
        )
    }
}
