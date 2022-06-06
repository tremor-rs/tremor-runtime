use clickhouse_rs::Pool;
use std::time::{Duration, Instant};

use crate::errors::{Error, Result};

pub(super) const DB_HOST: &str = "127.0.0.1";

// Blocks the task until calling GET on `url` returns an HTTP 200.
pub(super) async fn wait_for_ok(port: u16) -> Result<()> {
    let wait_for = Duration::from_secs(60);
    let start = Instant::now();

    while let Err(e) = test_status_endpoint(port).await {
        if start.elapsed() > wait_for {
            let max_time = wait_for.as_secs();
            error!("We waited for more than {max_time}");
            return Err(e.chain_err(|| "Waiting for the ClickHouse container timed out."));
        }

        async_std::task::sleep(Duration::from_secs(1)).await;
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
