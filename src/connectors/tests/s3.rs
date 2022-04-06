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
use async_std::stream::StreamExt;
use async_std::task::JoinHandle;
use aws_sdk_s3::{Client, Config, Credentials, Endpoint, Region};
use rand::{distributions::Alphanumeric, Rng};
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_async_std::{Handle, Signals};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use testcontainers::Docker;
use testcontainers::{clients, images::generic::GenericImage, Container, RunArgs};

use super::free_port::find_free_tcp_port;
/// Keeps track of process env manipulations and restores previous values upon drop
pub(crate) struct EnvHelper {
    restore: HashMap<String, String>,
}

impl EnvHelper {
    pub(crate) fn new() -> Self {
        Self {
            restore: HashMap::new(),
        }
    }

    #[cfg(feature = "s3-integration")]
    pub(crate) fn set_var(&mut self, key: &str, value: &str) {
        if let Ok(old_value) = std::env::var(key) {
            self.restore.insert(key.to_string(), old_value);
        }
        std::env::set_var(key, value);
    }

    #[cfg(feature = "s3-integration")]
    pub(crate) fn remove_var(&mut self, key: &str) {
        if let Ok(old_value) = std::env::var(key) {
            self.restore.insert(key.to_string(), old_value);
        }
        std::env::remove_var(key);
    }
}

impl Drop for EnvHelper {
    fn drop(&mut self) {
        for (k, v) in &self.restore {
            std::env::set_var(k, v);
        }
    }
}

pub(crate) struct SignalHandler {
    signal_handle: Handle,
    handle_task: Option<JoinHandle<()>>,
}

impl SignalHandler {
    pub(crate) fn new(container_id: String) -> Result<Self> {
        let mut signals = Signals::new(&[SIGTERM, SIGINT, SIGQUIT])?;
        let signal_handle = signals.handle();
        let handle_task = async_std::async_global_executor::spawn(async move {
            let signal_docker = clients::Cli::default();
            while let Some(signal) = signals.next().await {
                signal_docker.stop(container_id.as_str());
                signal_docker.rm(container_id.as_str());
                let _ = signal_hook::low_level::emulate_default_handler(signal);
            }
        });
        Ok(Self {
            signal_handle,
            handle_task: Some(handle_task),
        })
    }
}
impl Drop for SignalHandler {
    fn drop(&mut self) {
        self.signal_handle.close();
        if let Some(s) = self.handle_task.take() {
            async_std::task::block_on(s.cancel());
        }
    }
}

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

async fn spawn_docker<'d, D: Docker>(
    docker: &'d D,
    image: GenericImage,
) -> (Container<'d, D, GenericImage>, u16, u16) {
    let http_port = find_free_tcp_port().await.unwrap_or(10080);
    let https_port = find_free_tcp_port().await.unwrap_or(10443);
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
