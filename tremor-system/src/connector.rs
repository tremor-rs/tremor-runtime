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

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};
use tokio::sync::mpsc::Sender;
use tremor_common::{alias, ports::Port};
use tremor_script::ast::DeployEndpoint;

use crate::{instance::State, pipeline};

/// surce messages
pub mod source;

/// sinks
pub mod sink;

#[derive(Debug)]
/// result of an async operation of the connector.
/// bears a `url` to identify the connector who finished the operation
pub struct ResultWrapper<T: std::fmt::Debug> {
    /// the connector alias
    pub alias: alias::Connector,
    /// the actual result
    pub res: Result<T, anyhow::Error>,
}

impl ResultWrapper<()> {
    /// create a new connector result ok
    #[must_use]
    pub fn ok(alias: &alias::Connector) -> Self {
        Self {
            alias: alias.clone(),
            res: Ok(()),
        }
    }
    /// create a new connector result error
    #[must_use]
    pub fn err<E: Into<anyhow::Error>>(alias: &alias::Connector, e: E) -> Self {
        Self {
            alias: alias.clone(),
            res: Err(e.into()),
        }
    }
}

/// connector errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error sending a message to connector
    #[error("{0} Error sending a message to connector")]
    SendError(alias::Connector),
    /// Error sending a message to source
    #[error("{0} Error sending a message to source")]
    SourceSendError(alias::Connector),
    /// Error sending a message to sink
    #[error("{0} Error sending a message to sink")]
    SinkSendError(alias::Connector),
    /// Status report error
    #[error("{0} Error receiving status report")]
    StatusReportError(alias::Connector),
    /// Connect failed Error
    #[error("{0} Connect failed")]
    ConnectFailed(alias::Connector),
}

impl Error {
    /// the alias of the connector
    #[must_use]
    pub fn alias(&self) -> &alias::Connector {
        match self {
            Self::SourceSendError(alias)
            | Self::SinkSendError(alias)
            | Self::StatusReportError(alias)
            | Self::ConnectFailed(alias)
            | Self::SendError(alias) => alias,
        }
    }
}

/// connector address
#[derive(Clone, Debug)]
pub struct Addr {
    /// connector instance alias
    alias: alias::Connector,
    /// the actual sender
    sender: Sender<Msg>,
    /// the source part of the connector
    source: Option<source::Addr>,
    /// the sink part of the connector
    pub(crate) sink: Option<sink::Addr>,
}

impl Display for Addr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.alias.fmt(f)
    }
}

impl Addr {
    /// create a new connector address
    #[must_use]
    pub fn new(
        alias: alias::Connector,
        sender: Sender<Msg>,
        source: Option<source::Addr>,
        sink: Option<sink::Addr>,
    ) -> Self {
        Self {
            alias,
            sender,
            source,
            sink,
        }
    }

    /// the connector alias
    #[must_use]
    pub fn alias(&self) -> &alias::Connector {
        &self.alias
    }

    /// the connector sink
    #[must_use]
    pub fn sink(&self) -> Option<&sink::Addr> {
        self.sink.as_ref()
    }

    /// the connector source
    #[must_use]
    pub fn source(&self) -> Option<&source::Addr> {
        self.source.as_ref()
    }

    /// The actual sender
    #[must_use]
    pub fn sender(&self) -> &Sender<Msg> {
        &self.sender
    }

    /// send a message
    ///
    /// # Errors
    ///  * If sending failed
    pub async fn send(&self, msg: Msg) -> Result<(), Error> {
        self.sender
            .send(msg)
            .await
            .map_err(|_| Error::SendError(self.alias.clone()))
    }

    /// send a message to the sink part of the connector.
    /// Results in a no-op if the connector has no sink part.
    ///
    /// # Errors
    ///   * if sending failed
    pub async fn send_sink(&self, msg: sink::Msg) -> Result<(), Error> {
        if let Some(sink) = self.sink.as_ref() {
            sink.send(msg)
                .await
                .map_err(|_| Error::SinkSendError(self.alias.clone()))?;
        }
        Ok(())
    }

    /// Send a message to the source part of the connector.
    /// Results in a no-op if the connector has no source part.
    ///
    /// # Errors
    ///   * if sending failed
    pub fn send_source(&self, msg: source::Msg) -> Result<(), Error> {
        if let Some(source) = self.source.as_ref() {
            source
                .send(msg)
                .map_err(|_| Error::SourceSendError(self.alias.clone()))?;
        }
        Ok(())
    }
    /// check if the connector has a source part
    #[must_use]
    pub fn has_source(&self) -> bool {
        self.source.is_some()
    }
    /// check if the connector has a sink part
    #[must_use]
    pub fn has_sink(&self) -> bool {
        self.sink.is_some()
    }

    /// stops the connector
    ///
    /// # Errors
    ///   * if sending failed
    pub async fn stop(&self, sender: Sender<ResultWrapper<()>>) -> Result<(), Error> {
        self.send(Msg::Stop(sender)).await
    }
    /// starts the connector
    ///
    /// # Errors
    ///   * if sending failed
    pub async fn start(&self, sender: Sender<ResultWrapper<()>>) -> Result<(), Error> {
        self.send(Msg::Start(sender)).await
    }
    /// drains the connector
    ///
    /// # Errors
    ///   * if sending failed
    pub async fn drain(&self, sender: Sender<ResultWrapper<()>>) -> Result<(), Error> {
        self.send(Msg::Drain(sender)).await
    }
    /// pauses the connector
    ///
    /// # Errors
    ///   * if sending failed
    pub async fn pause(&self) -> Result<(), Error> {
        self.send(Msg::Pause).await
    }
    /// resumes the connector
    ///
    /// # Errors
    ///   * if sending failed
    pub async fn resume(&self) -> Result<(), Error> {
        self.send(Msg::Resume).await
    }

    /// report status of the connector instance
    ///
    /// # Errors
    ///   * if sending or receiving failed
    pub async fn report_status(&self) -> Result<StatusReport, Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(Msg::Report(tx)).await?;
        rx.await
            .map_err(|_| Error::StatusReportError(self.alias.clone()))
    }
}

/// describing the number of previous connection attempts
#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
pub struct Attempt {
    overall: u64,
    success: u64,
    since_last_success: u64,
}

impl Attempt {
    /// indicate success to connect, reset to the state after a successful connection attempt
    pub fn on_success(&mut self) {
        self.overall += 1;
        self.success += 1;
        self.since_last_success = 0;
    }
    /// indicate failure to connect
    pub fn on_failure(&mut self) {
        self.overall += 1;
        self.since_last_success += 1;
    }

    /// Returns true if this is the very first attempt
    #[must_use]
    pub fn is_first(&self) -> bool {
        self.overall == 0
    }

    /// returns the number of previous successful connection attempts
    #[must_use]
    pub fn success(&self) -> u64 {
        self.success
    }

    /// returns the number of connection attempts since the last success
    #[must_use]
    pub fn since_last_success(&self) -> u64 {
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

/// describes connectivity state of the connector
#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Connectivity {
    /// connector is connected
    Connected,
    /// connector is disconnected
    Disconnected,
}

/// Connector instance status report
#[derive(Debug, Serialize)]
pub struct StatusReport {
    /// connector alias
    alias: alias::Connector,
    /// state of the connector
    status: State,
    /// current connectivity
    connectivity: Connectivity,
    /// connected pipelines
    pipelines: HashMap<Port<'static>, Vec<DeployEndpoint>>,
}

impl StatusReport {
    /// create a new status report
    #[must_use]
    pub fn new(
        alias: alias::Connector,
        status: State,
        connectivity: Connectivity,
        pipelines: HashMap<Port<'static>, Vec<DeployEndpoint>>,
    ) -> Self {
        Self {
            alias,
            status,
            connectivity,
            pipelines,
        }
    }
    /// the connector alias
    #[must_use]
    pub fn alias(&self) -> &alias::Connector {
        &self.alias
    }

    /// connector state
    #[must_use]
    pub fn status(&self) -> &State {
        &self.status
    }

    /// connector connectivity
    #[must_use]
    pub fn connectivity(&self) -> &Connectivity {
        &self.connectivity
    }

    /// connected pipelines
    #[must_use]
    pub fn pipelines(&self) -> &HashMap<Port<'static>, Vec<DeployEndpoint>> {
        &self.pipelines
    }
}

/// Messages a Connector instance receives and acts upon
#[derive(Debug)]
pub enum Msg {
    /// connect 1 or more pipelines to a port
    LinkInput {
        /// port to which to connect
        port: Port<'static>,
        /// pipelines to connect
        pipelines: Vec<(DeployEndpoint, pipeline::Addr)>,
        /// result receiver
        result_tx: Sender<Result<(), anyhow::Error>>,
    },
    /// connect 1 or more pipelines to a port
    LinkOutput {
        /// port to which to connect
        port: Port<'static>,
        /// pipeline to connect to
        pipeline: (DeployEndpoint, pipeline::Addr),
        /// result receiver
        result_tx: Sender<Result<(), anyhow::Error>>,
    },

    /// notification from the connector implementation that connectivity is lost and should be reestablished
    ConnectionLost,
    /// initiate a reconnect attempt
    Reconnect,
    // TODO: fill as needed
    /// start the connector
    Start(Sender<ResultWrapper<()>>),
    /// pause the connector
    ///
    /// source part is not polling for new data
    /// sink part issues a CB trigger
    /// until Resume is called, sink is restoring the CB again
    Pause,
    /// resume the connector after a pause
    Resume,
    /// Drain events from this connector
    ///
    /// - stop reading events from external connections
    /// - decline events received via the sink part
    /// - wait for drainage to be finished
    Drain(Sender<ResultWrapper<()>>),
    /// notify this connector that its source part has been drained
    SourceDrained,
    /// notify this connector that its sink part has been drained
    SinkDrained,
    /// stop the connector
    Stop(Sender<ResultWrapper<()>>),
    /// request a status report
    Report(tokio::sync::oneshot::Sender<StatusReport>),
}
