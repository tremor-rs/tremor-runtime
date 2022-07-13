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
use crate::connectors::sink::SinkMsg;
use crate::connectors::source::SourceMsg;
use crate::connectors::{Addr, Connectivity, Connector, ConnectorContext, Context, Msg};
use crate::errors::{Error, Result};
use crate::system::flow::ConnectorAlias;
use async_std::channel::{bounded, Sender};
use async_std::task::{self, JoinHandle};
use futures::future::{join3, ready, FutureExt};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::convert::identity;
use std::fmt::Display;
use std::time::Duration;

#[derive(Debug, PartialEq, Clone)]
enum ShouldRetry {
    Yes,
    No(String),
}

trait ReconnectStrategy: std::marker::Send {
    /// Compute the next interval in milliseconds.
    ///
    /// Upon the first reconnect attempt or upon connection loss after a successful connect attempt, `current` is `None`.
    /// Further reconnect retries will have the last interval set.
    fn next_interval(&mut self, current: Option<u64>, attempt: &Attempt) -> u64;
    /// Should we actually do a reconnect attempt?
    fn should_reconnect(&mut self, attempt: &Attempt) -> ShouldRetry;
}

/// Strategy that only reconnects once when the connection was lost
/// and does no further reconnect attempts when the connection could not be re-established.
struct FailFast {}
impl ReconnectStrategy for FailFast {
    fn next_interval(&mut self, current: Option<u64>, attempt: &Attempt) -> u64 {
        if attempt.since_last_success() == 0 {
            // we lost the connection and this is the first reconnect attempt
            // lets wait for a second to avoid hot loops
            1000
        } else {
            // further reconnect retries are forbidden by the logic in `should_reconnect`
            current.unwrap_or(0)
        }
    }

    fn should_reconnect(&mut self, attempt: &Attempt) -> ShouldRetry {
        if attempt.since_last_success() == 0 {
            // we lost the connection and this is the first reconnect attempt
            // this one we let through
            ShouldRetry::Yes
        } else {
            // no further reconnect attempts
            ShouldRetry::No("`None` Reconnect strategy.".to_string())
        }
    }
}

/// Strategy that retries failed connection attempts up to a configurable `max_retries`,
/// and a configurable `start_interval` to wait, a `growth_rate` to grow the `start_interval`
/// between each attempt to reconnect and a `randomized` flag to randomize the growth interval,
/// so this can achieve randomized backoff.
struct RetryWithBackoff {
    start_interval: u64,
    growth_rate: f64,
    max_retries: u64,
    random: Option<SmallRng>,
}

impl RetryWithBackoff {
    fn new(start_interval: u64, growth_rate: f64, max_retries: u64, randomized: bool) -> Self {
        let random = if randomized {
            Some(SmallRng::from_entropy())
        } else {
            None
        };
        Self {
            start_interval,
            growth_rate,
            max_retries,
            random,
        }
    }
}

impl ReconnectStrategy for RetryWithBackoff {
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation
    )]
    fn next_interval(&mut self, current: Option<u64>, _attempt: &Attempt) -> u64 {
        match current {
            Some(interval) => {
                let growth_interval = (interval as f64 * self.growth_rate) as u64;
                if let Some(prng) = &mut self.random {
                    // apply randomness, interpreting the growth_interval as maximum growth
                    let range = if self.growth_rate >= 1.0 {
                        interval..=growth_interval
                    } else {
                        growth_interval..=interval
                    };
                    prng.gen_range(range)
                } else {
                    growth_interval
                }
            }
            None => self.start_interval,
        }
    }

    fn should_reconnect(&mut self, attempt: &Attempt) -> ShouldRetry {
        if attempt.since_last_success() < self.max_retries {
            ShouldRetry::Yes
        } else {
            ShouldRetry::No(format!("Max retries exceeded: {}", self.max_retries))
        }
    }
}

/// describing the number of previous connection attempts
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub(crate) struct Attempt {
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
    pub(crate) fn is_first(&self) -> bool {
        self.overall == 0
    }

    /// returns the number of previous successful connection attempts
    pub(crate) fn success(&self) -> u64 {
        self.success
    }

    /// returns the number of connection attempts since the last success
    pub(crate) fn since_last_success(&self) -> u64 {
        self.since_last_success
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
    addr: Addr,
    notifier: ConnectionLostNotifier,
    retry_task: Option<JoinHandle<()>>,
    alias: ConnectorAlias,
}

/// Notifier that connector implementations
/// can use to asynchronously notify the runtime whenever their connection is lost
///
/// This will change the connector state properly and trigger a new reconnect attempt (according to the configured logic)
#[derive(Clone)]
pub(crate) struct ConnectionLostNotifier(Sender<Msg>);

impl ConnectionLostNotifier {
    /// constructor
    pub(crate) fn new(tx: Sender<Msg>) -> Self {
        Self(tx)
    }
    /// notify the runtime that this connector lost its connection
    pub(crate) async fn connection_lost(&self) -> Result<()> {
        self.0.send(Msg::ConnectionLost).await?;
        Ok(())
    }
}

impl ReconnectRuntime {
    pub(crate) fn notifier(&self) -> ConnectionLostNotifier {
        self.notifier.clone()
    }
    /// constructor
    pub(crate) fn new(
        connector_addr: &Addr,
        notifier: ConnectionLostNotifier,
        config: &Reconnect,
    ) -> Self {
        Self::inner(
            connector_addr.clone(),
            connector_addr.alias.clone(),
            notifier,
            config,
        )
    }
    fn inner(
        addr: Addr,
        alias: ConnectorAlias,
        notifier: ConnectionLostNotifier,
        config: &Reconnect,
    ) -> Self {
        let strategy: Box<dyn ReconnectStrategy> = match config {
            Reconnect::None => Box::new(FailFast {}),
            Reconnect::Retry {
                interval_ms,
                growth_rate,
                max_retries,
                randomized,
            } => Box::new(RetryWithBackoff::new(
                *interval_ms,
                *growth_rate,
                max_retries.unwrap_or(u64::MAX),
                *randomized,
            )),
        };
        Self {
            attempt: Attempt::default(),
            interval_ms: None,
            strategy,
            addr,
            notifier,
            retry_task: None,
            alias,
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
    ) -> Result<(Connectivity, bool)> {
        let (tx, rx) = bounded(2);
        let source_fut = if self.addr.has_source() {
            self.addr
                .send_source(SourceMsg::Connect(tx.clone(), self.attempt.clone()))
                .await?;
            rx.recv()
                .map(|r| r.map_err(Error::from).and_then(identity))
                .boxed()
        } else {
            ready(Ok(true)).boxed()
        };
        let sink_fut = if self.addr.has_sink() {
            self.addr
                .send_sink(SinkMsg::Connect(tx, self.attempt.clone()))
                .await?;
            rx.recv()
                .map(|r| r.map_err(Error::from).and_then(identity))
                .boxed()
        } else {
            ready(Ok(true)).boxed()
        };
        let results = join3(source_fut, sink_fut, connector.connect(ctx, &self.attempt)).await;
        match results {
            (Ok(true), Ok(true), Ok(true)) => {
                self.reset();
                Ok((Connectivity::Connected, true))
            }
            (source, sink, conn) => {
                ctx.swallow_err(
                    source,
                    &format!("Error connecting the source part ({})", self.attempt),
                );
                ctx.swallow_err(
                    sink,
                    &format!("Error connecting the sink part ({})", self.attempt),
                );
                ctx.swallow_err(
                    conn,
                    &format!("Error connecting the connector ({})", self.attempt),
                );

                let will_retry = self.update_and_retry(ctx).await;
                Ok((Connectivity::Disconnected, will_retry))
            }
        }
    }

    /// update internal state for the current failed connect attempt
    /// and spawn a retry task
    async fn update_and_retry(&mut self, ctx: &ConnectorContext) -> bool {
        // update internal state
        self.attempt.on_failure();
        self.enqueue_retry(ctx).await
    }

    pub(crate) async fn enqueue_retry(&mut self, _ctx: &ConnectorContext) -> bool {
        if let ShouldRetry::No(msg) = self.strategy.should_reconnect(&self.attempt) {
            warn!("[Connector::{}] Not reconnecting: {}", &self.alias, msg);
            false
        } else {
            // compute next interval
            let interval = self.strategy.next_interval(self.interval_ms, &self.attempt);
            info!(
                "[Connector::{}] Reconnecting after {} ms",
                &self.alias, interval
            );
            self.interval_ms = Some(interval);

            // spawn a retry only if there is no retry currently pending
            let spawn_retry = if let Some(retry_handle) = self.retry_task.as_mut() {
                // poll once to check if the task is done
                futures::poll!(retry_handle).is_ready()
            } else {
                true
            };
            if spawn_retry {
                let duration = Duration::from_millis(interval);
                let sender = self.addr.sender.clone();
                let alias = self.alias.clone();
                self.retry_task = Some(task::spawn(async move {
                    task::sleep(duration).await;
                    if sender.send(Msg::Reconnect).await.is_err() {
                        error!(
                            "[Connector::{}] Error sending reconnect msg to connector.",
                            &alias
                        );
                    }
                }));
            }

            true
        }
    }

    #[cfg(test)]
    pub(crate) async fn await_retry(&mut self) {
        // take the task out to avoid having it be awaited from multiple tasks ?
        if let Some(retry_task) = self.retry_task.take() {
            retry_task.await;
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
    use super::*;
    use crate::{
        connectors::{utils::quiescence::QuiescenceBeacon, CodecReq},
        system::flow::FlowAlias,
    };

    /// does not connect
    struct FakeConnector {
        answer: Option<bool>,
    }
    #[async_trait::async_trait]
    impl Connector for FakeConnector {
        async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
            self.answer.ok_or("Blergh!".into())
        }

        fn codec_requirements(&self) -> CodecReq {
            CodecReq::Optional("json")
        }
    }

    #[test]
    fn attempt() -> Result<()> {
        let mut attempt = Attempt::default();
        assert_eq!(0, attempt.since_last_success());
        assert_eq!(0, attempt.success());

        attempt.on_success();
        assert_eq!(0, attempt.since_last_success());
        assert_eq!(1, attempt.success());

        attempt.on_failure();
        assert_eq!(1, attempt.since_last_success());
        assert_eq!(1, attempt.success());

        Ok(())
    }

    #[test]
    fn failfast_strategy() -> Result<()> {
        let mut strategy = FailFast {};
        let mut attempt = Attempt::default();
        // the first one we let through
        assert_eq!(ShouldRetry::Yes, strategy.should_reconnect(&attempt));
        // but we wait for a second
        assert_eq!(1000, strategy.next_interval(None, &attempt));
        attempt.on_failure();
        assert_eq!(0, strategy.next_interval(Some(0), &attempt));
        // no retries
        assert_eq!(
            ShouldRetry::No("`None` Reconnect strategy.".to_string()),
            strategy.should_reconnect(&attempt)
        );
        attempt.on_failure();
        assert_eq!(
            ShouldRetry::No("`None` Reconnect strategy.".to_string()),
            strategy.should_reconnect(&attempt)
        );
        attempt.on_success();
        Ok(())
    }

    #[async_std::test]
    async fn failfast_runtime() -> Result<()> {
        let (tx, rx) = async_std::channel::unbounded();
        let notifier = ConnectionLostNotifier::new(tx.clone());
        let alias = ConnectorAlias::new(FlowAlias::new("flow"), "test");
        let addr = Addr {
            alias: alias.clone(),
            source: None,
            sink: None,
            sender: tx.clone(),
        };
        let config = Reconnect::None;
        let mut runtime = ReconnectRuntime::inner(addr, alias.clone(), notifier, &config);
        let mut connector = FakeConnector {
            answer: Some(false),
        };
        let qb = QuiescenceBeacon::default();
        let ctx = ConnectorContext {
            alias,
            connector_type: "fake".into(),
            quiescence_beacon: qb,
            notifier: runtime.notifier(),
        };
        // failing attempt
        assert_eq!(
            (Connectivity::Disconnected, false),
            runtime.attempt(&mut connector, &ctx).await?
        );
        async_std::task::sleep(Duration::from_millis(100)).await;
        assert!(rx.is_empty()); // no reconnect attempt has been made
        Ok(())
    }

    #[async_std::test]
    async fn backoff_runtime() -> Result<()> {
        use async_std::prelude::FutureExt;
        let (tx, rx) = async_std::channel::unbounded();
        let notifier = ConnectionLostNotifier::new(tx.clone());
        let alias = ConnectorAlias::new(FlowAlias::new("flow"), "test");
        let addr = Addr {
            alias: alias.clone(),
            source: None,
            sink: None,
            sender: tx.clone(),
        };
        let config = Reconnect::Retry {
            interval_ms: 10,
            growth_rate: 2.0,
            max_retries: Some(3),
            randomized: true,
        };
        let mut runtime = ReconnectRuntime::inner(addr, alias.clone(), notifier, &config);
        let mut connector = FakeConnector {
            answer: Some(false),
        };
        let qb = QuiescenceBeacon::default();
        let ctx = ConnectorContext {
            alias,
            connector_type: "fake".into(),
            quiescence_beacon: qb,
            notifier: runtime.notifier(),
        };
        // 1st failing attempt
        assert!(matches!(
            runtime.attempt(&mut connector, &ctx).await?,
            (Connectivity::Disconnected, true)
        ));
        // we cannot test exact timings, but we can ensure it behaves as expected
        assert!(matches!(
            rx.recv().timeout(Duration::from_secs(5)).await??,
            Msg::Reconnect
        ));

        // 2nd failing attempt
        runtime.await_retry().await;
        assert!(matches!(
            runtime.attempt(&mut connector, &ctx).await?,
            (Connectivity::Disconnected, true)
        ));
        assert!(matches!(
            rx.recv().timeout(Duration::from_secs(5)).await??,
            Msg::Reconnect
        ));

        // 3rd failing attempt
        runtime.await_retry().await;
        assert!(matches!(
            runtime.attempt(&mut connector, &ctx).await?,
            (Connectivity::Disconnected, false)
        ));

        // assert we don't receive nothing, but run into a timeout
        assert!(rx.recv().timeout(Duration::from_millis(100)).await.is_err()); // no reconnect attempt has been made

        Ok(())
    }
}
