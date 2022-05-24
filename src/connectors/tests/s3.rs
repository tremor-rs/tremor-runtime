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
mod writer;

use crate::errors::{Error, Result};
use aws_sdk_s3::{Client, Config, Credentials, Endpoint, Region};
use rand::{distributions::Alphanumeric, Rng};
use std::time::{Duration, Instant};
use testcontainers::{clients::Cli, images::generic::GenericImage, Container, RunnableImage};

use super::free_port::find_free_tcp_port;
const IMAGE: &str = "adobe/s3mock";
const TAG: &str = "2.4.9";

async fn wait_for_s3mock(port: u16) -> Result<()> {
    let s3_client: Client = get_client(port);

    let wait_for = Duration::from_secs(60);
    let start = Instant::now();

    while let Err(e) = s3_client.list_buckets().send().await {
        if start.elapsed() > wait_for {
            return Err(Error::from(e).chain_err(|| "Waiting for mock-s3 container timed out"));
        }

        async_std::task::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

async fn spawn_docker<'d>(
    docker: &'d Cli,
    image: GenericImage,
) -> (Container<'d, GenericImage>, u16, u16) {
    let http_port = find_free_tcp_port().await.unwrap_or(10080);
    let https_port = find_free_tcp_port().await.unwrap_or(10443);
    let image = RunnableImage::from(image)
        .with_mapped_port((http_port, 9090_u16))
        .with_mapped_port((https_port, 9191_u16));
    let container = docker.run(image);
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

fn get_client(http_port: u16) -> Client {
    let s3_config = Config::builder()
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

    Client::from_conf(s3_config)
}
