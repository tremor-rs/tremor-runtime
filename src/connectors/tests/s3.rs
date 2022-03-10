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

use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_async_std::{Handle, Signals};
use testcontainers::clients;
use crate::errors::Result;
use async_std::task::JoinHandle;
use std::collections::HashMap;
use testcontainers::Docker;
use async_std::stream::StreamExt;
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
        let handle_task = async_std::task::spawn(async move {
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