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
