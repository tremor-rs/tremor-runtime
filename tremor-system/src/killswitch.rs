// Copyright 2022-2024, The Tremor Team
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

use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};

use crate::DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT;

#[derive(Debug, PartialEq, Eq)]
/// shutdown mode - controls how we shutdown Tremor
pub enum ShutdownMode {
    /// shut down by stopping all binding instances and wait for quiescence
    Graceful,
    /// Just stop everything and not wait
    Forceful,
}

#[derive(Debug)]

/// Shutdown commands
pub enum Msg {
    /// Stop the runtime
    Stop,
    /// Drain the runtime
    Drain(oneshot::Sender<anyhow::Result<()>>),
}

/// Killswitch errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error stopping all Flows
    #[error("Error stopping all Flows")]
    Send,
}
/// for draining and stopping
#[derive(Debug, Clone)]
pub struct KillSwitch(mpsc::Sender<Msg>);

impl KillSwitch {
    /// stop the runtime
    ///
    /// # Errors
    /// * if draining or stopping fails
    pub async fn stop(&self, mode: ShutdownMode) -> Result<(), Error> {
        if mode == ShutdownMode::Graceful {
            let (tx, rx) = oneshot::channel();
            self.0.send(Msg::Drain(tx)).await.map_err(|_| Error::Send)?;
            if let Ok(res) = timeout(DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT, rx).await {
                if res.is_err() {
                    log::error!("Error draining all Flows",);
                }
            } else {
                log::warn!(
                    "Timeout draining all Flows after {}s",
                    DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT.as_secs()
                );
            }
        }
        let res = self.0.send(Msg::Stop).await.map_err(|_| Error::Send);
        if let Err(e) = &res {
            log::error!("Error stopping all Flows: {e}");
        }
        res
    }

    /// Dummy kills switch
    #[must_use]
    pub fn dummy() -> Self {
        KillSwitch(mpsc::channel(1).0)
    }

    /// Creates a new kill switch
    #[must_use]
    pub fn new(sender: mpsc::Sender<Msg>) -> Self {
        KillSwitch(sender)
    }
}
