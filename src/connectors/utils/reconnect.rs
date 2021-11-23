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
use crate::config::Reconnect;
use crate::connectors::{Addr, Connectivity, Connector, ConnectorContext, Msg};
use crate::errors::Result;
use async_std::channel::Sender;
use async_std::task;
use std::fmt::Display;
use std::time::Duration;
use tremor_common::url::TremorUrl;

#[derive(Debug, PartialEq, Clone)]
enum ShouldRetry {
    Yes,
    No(String),
}

trait ReconnectStrategy: std::marker::Send {
    fn next_interval(&mut self, current: Option<u64>, attempt: &Attempt) -> u64;
    /// should we actually do a reconnect attempt?
    ///
    /// will throw a `MaxRetriesExceeded` error
    fn should_retry(&mut self, attempt: &Attempt) -> ShouldRetry;
}

struct FailFast {}
impl ReconnectStrategy for FailFast {
    fn next_interval(&mut self, current: Option<u64>, _attempt: &Attempt) -> u64 {
        current.unwrap_or(0)
    }

    fn should_retry(&mut self, _attempt: &Attempt) -> ShouldRetry {
        ShouldRetry::No("`None` Reconnect strategy.".to_string())
    }
}

struct SimpleBackoff {
    start_interval: u64,
    growth_rate: f64,
    max_retries: u64,
}
impl ReconnectStrategy for SimpleBackoff {
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation
    )]
    fn next_interval(&mut self, current: Option<u64>, _attempt: &Attempt) -> u64 {
        match current {
            Some(interval) => (interval as f64 * self.growth_rate) as u64,
            None => self.start_interval,
        }
    }

    fn should_retry(&mut self, attempt: &Attempt) -> ShouldRetry {
        if attempt.since_last_success() < self.max_retries {
            ShouldRetry::Yes
        } else {
            ShouldRetry::No(format!("Max retries exceeded: {}", self.max_retries))
        }
    }
}

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

impl Display for Attempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Attempt #{} (overall: #{})",
            self.since_last_success, self.overall
        )
    }
}

/// Entity that executes the reconnect logic based upon the given `ReconnectConfig` -> `ReconnectStrategy`
pub(crate) struct ReconnectRuntime {
    /// attempt since last successful connect, only the very first attempt will be 0
    attempt: Attempt,
    interval_ms: Option<u64>,
    strategy: Box<dyn ReconnectStrategy>,
    sender: Sender<Msg>,
    connector_url: TremorUrl,
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

impl ReconnectRuntime {
    pub(crate) fn notifier(&self) -> ConnectionLostNotifier {
        ConnectionLostNotifier(self.sender.clone())
    }
    /// constructor
    pub(crate) fn new(connector_addr: &Addr, config: &Reconnect) -> Self {
        Self::inner(
            connector_addr.sender.clone(),
            connector_addr.url.clone(),
            config,
        )
    }
    fn inner(sender: Sender<Msg>, connector_url: TremorUrl, config: &Reconnect) -> Self {
        let strategy: Box<dyn ReconnectStrategy> = match config {
            Reconnect::None => Box::new(FailFast {}),
            Reconnect::Custom {
                interval_ms,
                growth_rate,
                max_retries,
            } => Box::new(SimpleBackoff {
                start_interval: *interval_ms,
                growth_rate: *growth_rate,
                max_retries: max_retries.unwrap_or(u64::MAX),
            }),
        };
        Self {
            attempt: Attempt::default(),
            interval_ms: None,
            strategy,
            sender,
            connector_url,
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
                self.update_and_retry();

                Ok(Connectivity::Disconnected)
            }

            Err(e) => {
                error!(
                    "[Connector::{}] Reconnect Error ({}): {}",
                    &ctx.url, self.attempt, e
                );
                self.update_and_retry();

                Ok(Connectivity::Disconnected)
            }
        }
    }

    /// update internal state for the current failed connect attempt
    /// and spawn a retry task
    fn update_and_retry(&mut self) {
        // update internal state
        self.attempt.on_failure();
        // check if we can retry according to strategy
        if let ShouldRetry::No(msg) = self.strategy.should_retry(&self.attempt) {
            error!(
                "[Connector::{}] Not reconnecting: {}",
                &self.connector_url, msg
            );
        } else {
            // compute next interval
            let interval = self.strategy.next_interval(self.interval_ms, &self.attempt);
            info!(
                "[Connector::{}] Reconnecting after {} ms",
                &self.connector_url, interval
            );
            self.interval_ms = Some(interval);

            // spawn retry
            // ALLOW: we are not interested in fractions here
            let duration = Duration::from_millis(interval);
            let sender = self.sender.clone();
            let url = self.connector_url.clone();
            task::spawn(async move {
                task::sleep(duration).await;
                if sender.send(Msg::Reconnect).await.is_err() {
                    error!(
                        "[Connector::{}] Error sending reconnect msg to connector.",
                        &url
                    );
                }
            });
        }
    }

    /// reset internal state after successful connect attempt
    fn reset(&mut self) {
        self.attempt.on_success();

        self.interval_ms = None;
    }
}

#[cfg(test)]
mod tests {
    // use crate::connectors::quiescence::QuiescenceBeacon;

    use crate::connectors::quiescence::QuiescenceBeacon;

    use super::*;

    /// does not connect
    struct FakeConnector {
        answer: Option<bool>,
    }
    #[async_trait::async_trait]
    impl Connector for FakeConnector {
        async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
            self.answer.ok_or("Blergh!".into())
        }

        fn default_codec(&self) -> &str {
            "json"
        }
    }

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

    #[test]
    fn failfast_strategy() -> Result<()> {
        let mut strategy = FailFast {};
        let mut attempt = Attempt::default();
        assert_eq!(
            ShouldRetry::No("`None` Reconnect strategy.".to_string()),
            strategy.should_retry(&attempt)
        );
        assert_eq!(0, strategy.next_interval(None, &attempt));
        attempt.on_failure();
        assert_eq!(0, strategy.next_interval(Some(0), &attempt));
        assert_eq!(
            ShouldRetry::No("`None` Reconnect strategy.".to_string()),
            strategy.should_retry(&attempt)
        );
        attempt.on_failure();
        assert_eq!(
            ShouldRetry::No("`None` Reconnect strategy.".to_string()),
            strategy.should_retry(&attempt)
        );
        attempt.on_success();
        Ok(())
    }

    #[async_std::test]
    async fn failfast_runtime() -> Result<()> {
        let (tx, rx) = async_std::channel::bounded(1);
        let url = TremorUrl::from_connector_instance("test", "test")?;
        let config = Reconnect::None;
        let mut runtime = ReconnectRuntime::inner(tx, url.clone(), &config);
        let mut connector = FakeConnector {
            answer: Some(false),
        };
        let qb = QuiescenceBeacon::default();
        let ctx = ConnectorContext {
            uid: 1,
            url,
            connector_type: "fake".into(),
            quiescence_beacon: qb,
            notifier: runtime.notifier(),
        };
        // failing attempt
        assert_eq!(
            Connectivity::Disconnected,
            runtime.attempt(&mut connector, &ctx).await?
        );
        async_std::task::sleep(Duration::from_millis(100)).await;
        assert!(rx.is_empty()); // no reconnect attempt has been made
        Ok(())
    }

    #[async_std::test]
    async fn backoff_runtime() -> Result<()> {
        let (tx, rx) = async_std::channel::bounded(1);
        let url = TremorUrl::from_connector_instance("test", "test")?;
        let config = Reconnect::Custom {
            interval_ms: 10,
            growth_rate: 2.0,
            max_retries: Some(3),
        };
        let mut runtime = ReconnectRuntime::inner(tx, url.clone(), &config);
        let mut connector = FakeConnector {
            answer: Some(false),
        };
        let qb = QuiescenceBeacon::default();
        let ctx = ConnectorContext {
            uid: 1,
            url,
            connector_type: "fake".into(),
            quiescence_beacon: qb,
            notifier: runtime.notifier(),
        };
        // 1st failing attempt
        assert!(matches!(
            runtime.attempt(&mut connector, &ctx).await?,
            Connectivity::Disconnected
        ));
        async_std::task::sleep(Duration::from_millis(20)).await;

        assert_eq!(1, rx.len()); // 1 reconnect attempt has been made
        assert!(matches!(rx.try_recv()?, Msg::Reconnect));

        // 2nd failing attempt
        assert!(matches!(
            runtime.attempt(&mut connector, &ctx).await?,
            Connectivity::Disconnected
        ));
        async_std::task::sleep(Duration::from_millis(30)).await;

        assert_eq!(1, rx.len()); // 1 reconnect attempt has been made
        assert!(matches!(rx.try_recv()?, Msg::Reconnect));

        // 3rd failing attempt
        assert!(matches!(
            runtime.attempt(&mut connector, &ctx).await?,
            Connectivity::Disconnected
        ));
        async_std::task::sleep(Duration::from_millis(50)).await;

        assert!(rx.is_empty()); // no reconnect attempt has been made

        Ok(())
    }
}
