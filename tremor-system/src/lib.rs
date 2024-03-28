// Copyright 2020-2024, The Tremor Team
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

//! Tremor runtime

#![deny(warnings)]
#![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic,
    clippy::mod_module_files
)]

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

/// Instance management
pub mod instance;

/// default graceful shutdown timeout
pub const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Metrics instance name
pub static mut INSTANCE: &str = "tremor";

/// Default Q Size
static QSIZE: AtomicUsize = AtomicUsize::new(128);

/// Default Q Size
pub fn qsize() -> usize {
    QSIZE.load(Ordering::Relaxed)
}

/// Killswitch
pub mod killswitch {
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
    /// for draining and stopping
    #[derive(Debug, Clone)]
    pub struct KillSwitch(mpsc::Sender<Msg>);

    impl KillSwitch {
        /// stop the runtime
        ///
        /// # Errors
        /// * if draining or stopping fails
        pub async fn stop(&self, mode: ShutdownMode) -> anyhow::Result<()> {
            if mode == ShutdownMode::Graceful {
                let (tx, rx) = oneshot::channel();
                self.0.send(Msg::Drain(tx)).await?;
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
            let res = self.0.send(Msg::Stop).await;
            if let Err(e) = &res {
                log::error!("Error stopping all Flows: {e}");
            }
            Ok(res?)
        }

        /// FIXME: #[cfg(test)]
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
}
/// dataplane
pub mod dataplane {
    use tremor_common::ports::Port;
    use tremor_pipeline::Event;

    /// an input dataplane message for this pipeline
    #[derive(Debug)]
    pub enum Msg {
        /// an event
        Event {
            /// the event
            event: Event,
            /// the port the event came in from
            input: Port<'static>,
        },
        /// a signal
        Signal(Event),
    }
}

/// pipelines
pub mod pipeline {

    use std::fmt;

    use tokio::sync::mpsc::{error::SendError, Sender, UnboundedSender};
    use tremor_common::alias;
    use tremor_pipeline::Event;

    use super::{contraflow, controlplane, dataplane};

    /// Address for a pipeline
    #[derive(Clone)]
    pub struct Addr {
        addr: Sender<Box<dataplane::Msg>>,
        cf_addr: UnboundedSender<contraflow::Msg>,
        mgmt_addr: Sender<controlplane::Msg>,
        alias: alias::Pipeline,
    }

    impl Addr {
        /// creates a new address
        #[must_use]
        pub fn new(
            addr: Sender<Box<dataplane::Msg>>,
            cf_addr: UnboundedSender<contraflow::Msg>,
            mgmt_addr: Sender<controlplane::Msg>,
            alias: alias::Pipeline,
        ) -> Self {
            Self {
                addr,
                cf_addr,
                mgmt_addr,
                alias,
            }
        }

        /// send a contraflow insight message back down the pipeline
        ///
        /// # Errors
        /// * if sending failed
        pub fn send_insight(&self, event: Event) -> Result<(), SendError<contraflow::Msg>> {
            self.cf_addr.send(contraflow::Msg::Insight(event))
        }

        /// send a data-plane message to the pipeline
        ///
        /// # Errors
        /// * if sending failed
        pub async fn send(
            &self,
            msg: Box<dataplane::Msg>,
        ) -> Result<(), SendError<Box<dataplane::Msg>>> {
            self.addr.send(msg).await
        }

        /// send a control-plane message to the pipeline
        ///
        /// # Errors
        /// * if sending failed
        pub async fn send_mgmt(
            &self,
            msg: controlplane::Msg,
        ) -> Result<(), SendError<controlplane::Msg>> {
            self.mgmt_addr.send(msg).await
        }

        /// stop the pipeline
        ///     
        /// # Errors
        /// * if sending failed
        pub async fn stop(&self) -> Result<(), SendError<controlplane::Msg>> {
            self.send_mgmt(controlplane::Msg::Stop).await
        }

        /// start the pipeline
        ///     
        /// # Errors
        /// * if sending failed
        pub async fn start(&self) -> Result<(), SendError<controlplane::Msg>> {
            self.send_mgmt(controlplane::Msg::Start).await
        }

        /// pause the pipeline
        ///
        /// # Errors
        /// * if sending failed
        pub async fn pause(&self) -> Result<(), SendError<controlplane::Msg>> {
            self.send_mgmt(controlplane::Msg::Pause).await
        }

        /// resume the pipeline
        ///     
        /// # Errors
        /// * if sending failed
        pub async fn resume(&self) -> Result<(), SendError<controlplane::Msg>> {
            self.send_mgmt(controlplane::Msg::Resume).await
        }
    }

    impl fmt::Debug for Addr {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "Pipeline({})", self.alias)
        }
    }
}
/// connector messages
pub mod connector {
    use std::{collections::HashMap, fmt::Display};

    use serde::{Deserialize, Serialize};
    use tokio::sync::mpsc::{error::SendError, Sender};
    use tremor_common::{alias, ports::Port};
    use tremor_script::ast::DeployEndpoint;

    use crate::{instance::State, pipeline};

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
        pub fn err(alias: &alias::Connector, err_msg: &'static str) -> Self {
            Self {
                alias: alias.clone(),
                res: Err(anyhow::Error::msg(err_msg)),
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

        /// send a message
        ///
        /// # Errors
        ///  * If sending failed
        pub async fn send(&self, msg: Msg) -> Result<(), SendError<Msg>> {
            self.sender.send(msg).await
        }

        /// send a message to the sink part of the connector.
        /// Results in a no-op if the connector has no sink part.
        ///
        /// # Errors
        ///   * if sending failed
        pub async fn send_sink(&self, msg: sink::Msg) -> Result<(), SendError<sink::Msg>> {
            if let Some(sink) = self.sink.as_ref() {
                sink.send(msg).await?;
            }
            Ok(())
        }

        /// Send a message to the source part of the connector.
        /// Results in a no-op if the connector has no source part.
        ///
        /// # Errors
        ///   * if sending failed
        pub fn send_source(&self, msg: source::Msg) -> Result<(), SendError<source::Msg>> {
            if let Some(source) = self.source.as_ref() {
                source.addr.send(msg)?;
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
        pub async fn stop(&self, sender: Sender<ResultWrapper<()>>) -> Result<(), SendError<Msg>> {
            self.send(Msg::Stop(sender)).await
        }
        /// starts the connector
        ///
        /// # Errors
        ///   * if sending failed
        pub async fn start(&self, sender: Sender<ResultWrapper<()>>) -> Result<(), SendError<Msg>> {
            self.send(Msg::Start(sender)).await
        }
        /// drains the connector
        ///
        /// # Errors
        ///   * if sending failed
        pub async fn drain(&self, sender: Sender<ResultWrapper<()>>) -> Result<(), SendError<Msg>> {
            self.send(Msg::Drain(sender)).await
        }
        /// pauses the connector
        ///
        /// # Errors
        ///   * if sending failed
        pub async fn pause(&self) -> Result<(), SendError<Msg>> {
            self.send(Msg::Pause).await
        }
        /// resumes the connector
        ///
        /// # Errors
        ///   * if sending failed
        pub async fn resume(&self) -> Result<(), SendError<Msg>> {
            self.send(Msg::Resume).await
        }

        /// report status of the connector instance
        ///
        /// # Errors
        ///   * if sending or receiving failed
        pub async fn report_status(&self) -> Result<StatusReport, anyhow::Error> {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.send(Msg::Report(tx)).await?;
            Ok(rx.await?)
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

    /// surce messages
    pub mod source {
        use tokio::sync::mpsc::{error::SendError, Sender, UnboundedSender};
        use tremor_common::ports::Port;
        use tremor_pipeline::{CbAction, EventId};
        use tremor_script::ast::DeployEndpoint;

        use crate::pipeline;

        use super::Attempt;

        /// address of a source
        #[derive(Clone, Debug)]
        pub struct Addr {
            /// the actual address
            pub addr: UnboundedSender<Msg>,
        }

        impl Addr {
            /// send a message
            ///
            /// # Errors
            ///  * If sending failed
            pub fn send(&self, msg: Msg) -> Result<(), SendError<Msg>> {
                self.addr.send(msg)
            }
        }

        #[derive(Debug)]
        /// Messages a Source can receive
        pub enum Msg {
            /// connect a pipeline
            Link {
                /// port
                port: Port<'static>,
                /// sends the result
                tx: Sender<Result<(), anyhow::Error>>,
                /// pipeline to connect to
                pipeline: (DeployEndpoint, pipeline::Addr),
            },
            /// Connect to the outside world and send the result back
            Connect(Sender<Result<bool, anyhow::Error>>, Attempt),
            /// connectivity is lost in the connector
            ConnectionLost,
            /// connectivity is re-established
            ConnectionEstablished,
            /// Circuit Breaker Contraflow Event
            Cb(CbAction, EventId),
            /// start the source
            Start,
            /// pause the source
            Pause,
            /// resume the source
            Resume,
            /// stop the source
            Stop(Sender<Result<(), anyhow::Error>>),
            /// drain the source - bears a sender for sending out a SourceDrained status notification
            Drain(Sender<Msg>),
            /// FIXME: #[cfg(test)]
            Synchronize(tokio::sync::oneshot::Sender<()>),
        }
    }

    /// sinks
    pub mod sink {
        use tokio::sync::mpsc::Sender;
        use tremor_common::ports::Port;
        use tremor_pipeline::Event;
        use tremor_script::ast::DeployEndpoint;

        use crate::pipeline;

        use super::Attempt;

        /// address of a connector sink
        #[derive(Clone, Debug)]
        pub struct Addr {
            /// the actual sender
            addr: Sender<Msg>,
        }

        impl Addr {
            /// send a message
            ///
            /// # Errors
            ///  * If sending failed
            pub async fn send(
                &self,
                msg: Msg,
            ) -> Result<(), tokio::sync::mpsc::error::SendError<Msg>> {
                self.addr.send(msg).await
            }
        }

        /// messages a sink can receive
        #[derive(Debug)]
        pub enum Msg {
            /// receive an event to handle
            Event {
                /// the event
                event: Event,
                /// the port through which it came
                port: Port<'static>,
            },
            /// receive a signal
            Signal {
                /// the signal event
                signal: Event,
            },
            /// link some pipelines to the give port
            Link {
                /// the pipelines
                pipelines: Vec<(DeployEndpoint, pipeline::Addr)>,
            },
            /// Connect to the outside world and send the result back
            Connect(Sender<Result<bool, anyhow::Error>>, Attempt),
            /// the connection to the outside world wasl ost
            ConnectionLost,
            /// connection established
            ConnectionEstablished,
            // TODO: fill those
            /// start the sink
            Start,
            /// pause the sink
            Pause,
            /// resume the sink
            Resume,
            /// stop the sink
            Stop(Sender<Result<(), anyhow::Error>>),
            /// drain this sink and notify the connector via the provided sender
            Drain(Sender<super::Msg>),
        }
    }
}

/// control plane
pub mod controlplane {
    use crate::{
        connector,
        pipeline::{self, Addr},
    };
    use tokio::sync::mpsc::Sender;
    use tremor_common::ports::Port;
    use tremor_pipeline::Event;
    use tremor_script::ast::DeployEndpoint;

    /// Input targets
    #[derive(Debug)]
    pub enum InputTarget {
        /// another pipeline
        Pipeline(Box<super::pipeline::Addr>),
        /// a connector
        Source(connector::source::Addr),
    }

    impl InputTarget {
        /// send an insight to the target
        ///
        /// # Errors
        /// * if sending failed
        pub fn send_insight(&self, insight: Event) -> Result<(), anyhow::Error> {
            match self {
                InputTarget::Pipeline(addr) => Ok(addr.send_insight(insight)?),
                InputTarget::Source(addr) => {
                    Ok(addr.send(crate::connector::source::Msg::Cb(insight.cb, insight.id))?)
                }
            }
        }
    }

    /// Output targets
    #[derive(Debug)]
    pub enum OutputTarget {
        /// another pipeline
        Pipeline(Box<pipeline::Addr>),
        /// a connector
        Sink(connector::sink::Addr),
    }

    impl From<Addr> for OutputTarget {
        fn from(p: pipeline::Addr) -> Self {
            Self::Pipeline(Box::new(p))
        }
    }
    impl TryFrom<connector::Addr> for OutputTarget {
        type Error = anyhow::Error;

        fn try_from(addr: connector::Addr) -> Result<Self, anyhow::Error> {
            addr.sink
                .ok_or(anyhow::Error::msg("Connector has no sink"))
                .map(Self::Sink)
        }
    }

    /// control plane message
    #[derive(Debug)]
    pub enum Msg {
        /// connect a target to an input port
        ConnectInput {
            /// port to connect in
            port: Port<'static>,
            /// url of the input to connect
            endpoint: DeployEndpoint,
            /// sends the result
            tx: Sender<Result<(), anyhow::Error>>,
            /// the target that connects to the `in` port
            target: InputTarget,
            /// should we send insights to this input
            is_transactional: bool,
        },
        /// connect a target to an output port
        ConnectOutput {
            /// the port to connect to
            port: Port<'static>,
            /// the url of the output instance
            endpoint: DeployEndpoint,
            /// sends the result
            tx: Sender<Result<(), anyhow::Error>>,
            /// the actual target addr
            target: OutputTarget,
        },
        /// start the pipeline
        Start,
        /// pause the pipeline - currently a no-op
        Pause,
        /// resume from pause - currently a no-op
        Resume,
        /// stop the pipeline
        Stop,
        #[cfg(test)]
        Inspect(Sender<connector::StatusReport>),
    }
}

/// contraflow
pub mod contraflow {
    use tremor_pipeline::Event;

    /// contraflow message
    #[derive(Debug)]
    pub enum Msg {
        /// insight
        Insight(Event),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assert_qsize() {
        assert_eq!(qsize(), QSIZE.load(Ordering::Relaxed));
    }
}
