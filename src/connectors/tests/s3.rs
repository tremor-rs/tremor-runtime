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
mod reader;
mod streamer;

use crate::errors::{Error, Result};
use aws_sdk_s3::{Client, Config, Credentials, Region};
use rand::{distributions::Alphanumeric, Rng};
use std::time::{Duration, Instant};
use testcontainers::{clients::Cli, images::generic::GenericImage, Container, RunnableImage};

use super::free_port::find_free_tcp_port;
const IMAGE: &str = "minio/minio";
const VERSION: &str = "RELEASE.2023-01-12T02-06-16Z";

const MINIO_ROOT_USER: &str = "tremor";
const MINIO_ROOT_PASSWORD: &str = "snot_badger";
const MINIO_REGION: &str = "eu-central-1";

async fn wait_for_s3(port: u16) -> Result<()> {
    let s3_client: Client = get_client(port);

    let wait_for = Duration::from_secs(60);
    let start = Instant::now();

    while let Err(e) = s3_client.list_buckets().send().await {
        if start.elapsed() > wait_for {
            return Err(Error::from(e).chain_err(|| "Waiting for mock-s3 container timed out"));
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(())
}

async fn wait_for_bucket(bucket: &str, client: Client) -> Result<()> {
    let wait_for = Duration::from_secs(10);
    let start = Instant::now();

    while let Err(e) = client.head_bucket().bucket(bucket).send().await {
        if start.elapsed() > wait_for {
            return Err(Error::from(e).chain_err(|| "Waiting for bucket to become alive timed out"));
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(())
}

async fn create_bucket(bucket: &str, http_port: u16) -> Result<()> {
    let client = get_client(http_port);
    client.create_bucket().bucket(bucket).send().await?;
    wait_for_bucket(bucket, client).await?;
    Ok(())
}

async fn spawn_docker(docker: &Cli) -> (Container<GenericImage>, u16) {
    let image = GenericImage::new(IMAGE, VERSION)
        .with_env_var("MINIO_ROOT_USER", MINIO_ROOT_USER)
        .with_env_var("MINIO_ROOT_PASSWORD", MINIO_ROOT_PASSWORD)
        .with_env_var("MINIO_REGION", MINIO_REGION);
    let http_port = find_free_tcp_port().await.unwrap_or(10080);
    let http_tls_port = find_free_tcp_port().await.unwrap_or(10443);
    let image = RunnableImage::from((
        image,
        vec![
            String::from("server"),
            String::from("/data"),
            String::from("--console-address"),
            String::from(":9001"),
        ],
    ))
    .with_mapped_port((http_port, 9000_u16))
    .with_mapped_port((http_tls_port, 9001_u16));
    let container = docker.run(image);
    let http_port = container.get_host_port_ipv4(9000);
    (container, http_port)
}

fn random_bucket_name(prefix: &str) -> String {
    format!(
        "{}-{}",
        prefix,
        rand::thread_rng()
            .sample_iter(Alphanumeric)
            .map(char::from)
            .filter(char::is_ascii_lowercase)
            .take(10)
            .collect::<String>()
    )
}

fn get_client(http_port: u16) -> Client {
    let s3_config = Config::builder()
        .credentials_provider(Credentials::new(
            MINIO_ROOT_USER,
            MINIO_ROOT_PASSWORD,
            None,
            None,
            "Environment",
        ))
        .region(Region::new(MINIO_REGION))
        .endpoint_url(format!("http://localhost:{http_port}"))
        .force_path_style(true)
        .build();

    Client::from_conf(s3_config)
}
