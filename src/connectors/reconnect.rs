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

/// reconnect logic and execution for connectors
use crate::config::Reconnect as ReconnectConfig;
use crate::connectors::{Addr, Connectivity, Connector, ConnectorContext, Msg};
use crate::errors::{ErrorKind, Result};
use async_std::channel::Sender;
use async_std::task;
use std::time::Duration;

/// describing the number of previous connection attempts
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Attempt {
    overall: u64,
    success: u64,
    since_last_success: u64,
}

impl Attempt {
    // reset to the state after a successful connection attempt
    fn on_success(&mut self) {
        self.overall += 1;
        self.success += 1;
        self.since_last_success = 0;
    }

    fn on_failure(&mut self) {
        self.overall += 1;
        self.since_last_success += 1;
    }

    /// Returns true if this is the very first attempt
    pub fn is_first(&self) -> bool {
        self.overall == 0
    }

    /// returns the number of previous successful connection attempts
    pub fn success(&self) -> u64 {
        self.success
    }

    /// returns the number of connection attempts since the last success
    pub fn since_last_success(&self) -> u64 {
        self.since_last_success
    }

    /// returns the overall connection attempts
    pub fn overall(&self) -> u64 {
        self.overall
    }
}

/// Entity that executes the reconnect logic based upon the given `ReconnectConfig`
pub(crate) struct Reconnect {
    /// attempt since last successful connect, only the very first attempt will be 0
    attempt: Attempt,
    interval_ms: u64,
    config: ReconnectConfig,
    addr: Addr,
}

/// Notifier that connector implementations
/// can use to asynchronously notify the runtime whenever their connection is lost
///
/// This will change the connector state properly and trigger a new reconnect attempt (according to the configured logic)
#[derive(Clone)]
pub struct ConnectionLostNotifier(Sender<Msg>);

impl ConnectionLostNotifier {
    /// notify the runtime that this connector lost its connection
    pub async fn notify(&self) -> Result<()> {
        self.0.send(Msg::ConnectionLost).await?;
        Ok(())
    }
}

impl Reconnect {
    pub(crate) fn notifier(&self) -> ConnectionLostNotifier {
        ConnectionLostNotifier(self.addr.sender.clone())
    }
    /// constructor
    pub(crate) fn new(connector_addr: &Addr, config: ReconnectConfig) -> Self {
        let interval_ms = config.interval_ms;
        Self {
            config,
            interval_ms,
            attempt: Attempt::default(),
            addr: connector_addr.clone(),
        }
    }

    /// Use the given connector to attempt to establish a connection.
    ///
    /// Will issue a retry after the configured interval (based upon the number of the attempt)
    /// asynchronously and send a `connector::Msg::Reconnect` to the connector identified by `addr`.
    pub(crate) async fn attempt(
        &mut self,
        connector: &mut dyn Connector,
        ctx: &ConnectorContext,
    ) -> Result<Connectivity> {
        match connector.connect(ctx, &self.attempt).await {
            Ok(true) => {
                self.reset();
                Ok(Connectivity::Connected)
            }
            Ok(false) => {
                self.update_and_retry()?;

                Ok(Connectivity::Disconnected)
            }

            Err(e) => {
                error!(
                    "[Connector::{}] Reconnect Error (Attempt {:?}): {}",
                    &ctx.url, self.attempt, e
                );
                self.update_and_retry()?;

                Ok(Connectivity::Disconnected)
            }
        }
    }

    /// update internal state for the current failed connect attempt
    /// and spawn a retry task
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation
    )]
    fn update_and_retry(&mut self) -> Result<()> {
        // update internal state
        if let Some(max_retries) = self.config.max_retry {
            if self.attempt.since_last_success() >= max_retries {
                return Err(ErrorKind::MaxRetriesExceeded(max_retries).into());
            }
        }
        self.attempt.on_failure();
        // TODO: trait out next interval computation, to support different strategies
        self.interval_ms = (self.interval_ms as f64 * self.config.growth_rate) as u64;

        // spawn retry
        // ALLOW: we are not interested in fractions here
        let duration = Duration::from_millis(self.interval_ms as u64);
        let sender = self.addr.sender.clone();
        let url = self.addr.url.clone();
        task::spawn(async move {
            task::sleep(duration).await;
            if sender.send(Msg::Reconnect).await.is_err() {
                error!(
                    "[Connector::{}] Error sending reconnect msg to connector.",
                    &url
                );
            }
        });
        Ok(())
    }

    /// reset internal state after successful connect attempt
    fn reset(&mut self) {
        self.attempt.on_success();

        self.interval_ms = self.config.interval_ms;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attempt() -> Result<()> {
        let mut attempt = Attempt::default();
        assert_eq!(0, attempt.since_last_success());
        assert_eq!(0, attempt.overall());
        assert_eq!(0, attempt.success());

        attempt.on_success();
        assert_eq!(0, attempt.since_last_success());
        assert_eq!(1, attempt.overall());
        assert_eq!(1, attempt.success());

        attempt.on_failure();
        assert_eq!(1, attempt.since_last_success());
        assert_eq!(2, attempt.overall());
        assert_eq!(1, attempt.success());

        Ok(())
    }
}
