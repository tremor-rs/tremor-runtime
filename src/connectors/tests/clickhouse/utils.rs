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

use clickhouse_rs::Pool;
use std::time::{Duration, Instant};

use crate::errors::{Error, Result};

pub(super) const DB_HOST: &str = "127.0.0.1";
pub(crate) const CONTAINER_NAME: &str = "clickhouse/clickhouse-server";
pub(crate) const CONTAINER_VERSION: &str = "22.3.3.44";
pub(crate) const SERVER_PORT: u16 = 9000;

// Blocks the task until calling GET on `url` returns an HTTP 200.
pub(super) async fn wait_for_ok(port: u16) -> Result<()> {
    let wait_for = Duration::from_secs(60);
    let start = Instant::now();

    while let Err(e) = test_status_endpoint(port).await {
        if start.elapsed() > wait_for {
            let max_time = wait_for.as_secs();
            error!("We waited for more than {max_time}");
            return Err(e.context("Waiting for the ClickHouse container timed out."));
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}

pub(super) async fn test_status_endpoint(port: u16) -> Result<()> {
    Pool::new(format!("tcp://{DB_HOST}:{port}/?connection_timeout=100ms&send_retries=1&retry_timeout=1s&ping_timeout=100ms"))
        .get_handle()
        .await
        .map(drop)
        .map_err(Error::from)
}
