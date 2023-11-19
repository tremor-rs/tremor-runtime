// Copyright 2023, The Tremor Team
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

use dashmap::DashMap;
use pulsar::{Pulsar, TokioExecutor};
use tokio::sync::Mutex;

use crate::connectors::prelude::*;

lazy_static! {
    static ref SINGLETON_LOCK: Mutex<()> = Mutex::new(());
    static ref PULSAR_CLIENT_SINGLETON: DashMap<String, Pulsar<TokioExecutor>> = DashMap::new();
}

// TODO: Add other possible configurations
pub(crate) async fn get_client(url: &str) -> Result<Pulsar<TokioExecutor>> {
    // Add some contention to unwrap errors properly and not create multiple clients to the same url.
    let _ = SINGLETON_LOCK.lock().await;

    if PULSAR_CLIENT_SINGLETON.contains_key(url) {
        return Ok(PULSAR_CLIENT_SINGLETON
            .get(url)
            .unwrap_or_else(|| panic!("Client not found for {url:?}"))
            .clone());
    }

    let client = Pulsar::builder(url, TokioExecutor).build().await?;
    PULSAR_CLIENT_SINGLETON.insert(url.to_string(), client.clone());
    Ok(client)
}
