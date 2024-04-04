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

/// The Event that carries data through the system
pub mod event;
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
    use crate::event::Event;
    use crate::{
        connector::{self, sink, source},
        pipeline,
    };
    use tremor_common::{ids::SourceId, ports::Port};

    /// The kind of signal this is
    #[derive(
        Debug,
        Clone,
        Copy,
        PartialEq,
        simd_json_derive::Serialize,
        simd_json_derive::Deserialize,
        Eq,
    )]
    pub enum SignalKind {
        // Lifecycle
        /// Start signal, containing the source uid which just started
        Start(SourceId),
        /// Shutdown Signal
        Shutdown,
        // Pause, TODO debug trace
        // Resume, TODO debug trace
        // Step, TODO ( into, over, to next breakpoint )
        /// Control
        Control,
        /// Periodic Tick
        Tick,
        /// Drain Signal - this connection is being drained, there should be no events after this
        /// This signal must be answered with a Drain contraflow event containing the same uid (u64)
        /// this way a contraflow event will not be interpreted by connectors for which it isn't meant
        /// reception of such Drain contraflow event notifies the signal sender that the intermittent pipeline is drained and can be safely disconnected
        Drain(SourceId),
    }

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

    /// Input targets
    #[derive(Debug)]
    pub enum InputTarget {
        /// another pipeline
        Pipeline(Box<pipeline::Addr>),
        /// a connector
        Source(source::Addr),
    }

    /// Dataplane errors
    #[derive(Debug, thiserror::Error)]
    pub enum Error {
        /// Error sending an insight to source
        #[error("Error sending insight to source")]
        SourceSendInsightError,
        /// Error sending an insight to pipeline
        #[error("Error sending insight to pipeline")]
        PipelineSendInsightError,
        /// Error sending an event to pipeline
        #[error("Error sending event to pipeline")]
        PipelineSendEventError,
        /// Error sending an event to sink
        #[error("Error sending event to sink")]
        SinkSendEventError,
        /// Error sending a signal to pipeline
        #[error("Error sending signal to pipeline")]
        PipelineSendSignalError,
        /// Error sending a signal to sink
        #[error("Error sending signal to sink")]
        SinkSendEventErrorSendSignalError,
    }

    impl InputTarget {
        /// send an insight to the target
        ///
        /// # Errors
        /// * if sending failed
        pub fn send_insight(&self, insight: Event) -> Result<(), Error> {
            match self {
                InputTarget::Pipeline(addr) => addr
                    .send_insight(insight)
                    .map_err(|_| Error::PipelineSendInsightError),
                InputTarget::Source(addr) => addr
                    .send(source::Msg::Cb(insight.cb, insight.id))
                    .map_err(|_| Error::SourceSendInsightError),
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

    impl OutputTarget {
        /// send an event out to this destination
        ///
        /// # Errors
        ///  * when sending the event via the dest channel fails
        pub async fn send_event(
            &mut self,
            input: Port<'static>,
            event: Event,
        ) -> Result<(), Error> {
            match self {
                Self::Pipeline(addr) => addr
                    .send(Box::new(Msg::Event { input, event }))
                    .await
                    .map_err(|_| Error::PipelineSendInsightError),
                Self::Sink(addr) => addr
                    .send(sink::Msg::Event { event, port: input })
                    .await
                    .map_err(|_| Error::PipelineSendInsightError),
            }
        }

        /// send a signal out to this destination
        ///
        /// # Errors
        ///  * when sending the signal via the dest channel fails
        pub async fn send_signal(&mut self, signal: Event) -> Result<(), Error> {
            match self {
                Self::Pipeline(addr) => {
                    // Each pipeline has their own ticks, we don't
                    // want to propagate them
                    if signal.kind != Some(SignalKind::Tick) {
                        addr.send(Box::new(Msg::Signal(signal)))
                            .await
                            .map_err(|_| Error::PipelineSendSignalError)?;
                    }
                }
                Self::Sink(addr) => addr
                    .send(sink::Msg::Signal { signal })
                    .await
                    .map_err(|_| Error::SinkSendEventErrorSendSignalError)?,
            }
            Ok(())
        }
    }

    impl From<pipeline::Addr> for OutputTarget {
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
}

/// pipelines
pub mod pipeline {

    use std::{
        collections::BTreeMap,
        fmt::{self, Display},
        str::FromStr,
    };

    use simd_json::OwnedValue;
    use tokio::sync::mpsc::{error::SendError, Sender, UnboundedSender};
    use tremor_common::{alias, ids::OperatorId};

    use crate::event::Event;

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

    /// Stringified numeric key
    /// from <https://github.com/serde-rs/json-benchmark/blob/master/src/prim_str.rs>
    #[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Debug)]
    pub struct PrimStr<T>(pub T)
    where
        T: Copy + Ord + Display + FromStr;

    impl<T> simd_json_derive::SerializeAsKey for PrimStr<T>
    where
        T: Copy + Ord + Display + FromStr,
    {
        fn json_write<W>(&self, writer: &mut W) -> std::io::Result<()>
        where
            W: std::io::Write,
        {
            write!(writer, "\"{}\"", self.0)
        }
    }

    impl<T> simd_json_derive::Serialize for PrimStr<T>
    where
        T: Copy + Ord + Display + FromStr,
    {
        fn json_write<W>(&self, writer: &mut W) -> std::io::Result<()>
        where
            W: std::io::Write,
        {
            write!(writer, "\"{}\"", self.0)
        }
    }

    impl<'input, T> simd_json_derive::Deserialize<'input> for PrimStr<T>
    where
        T: Copy + Ord + Display + FromStr,
    {
        #[inline]
        fn from_tape(tape: &mut simd_json_derive::Tape<'input>) -> simd_json::Result<Self>
        where
            Self: std::marker::Sized + 'input,
        {
            if let Some(simd_json::Node::String(s)) = tape.next() {
                Ok(PrimStr(FromStr::from_str(s).map_err(|_e| {
                    simd_json::Error::generic(simd_json::ErrorType::Serde("not a number".into()))
                })?))
            } else {
                Err(simd_json::Error::generic(
                    simd_json::ErrorType::ExpectedNull,
                ))
            }
        }
    }

    /// Operator metadata
    #[derive(
        Clone, Debug, Default, PartialEq, simd_json_derive::Serialize, simd_json_derive::Deserialize,
    )]
    // TODO: optimization: - use two Vecs, one for operator ids, one for operator metadata (Values)
    //                     - make it possible to trace operators with and without metadata
    //                     - insert with bisect (numbers of operators tracked will be low single digit numbers most of the time)
    pub struct OpMeta(BTreeMap<PrimStr<OperatorId>, OwnedValue>);

    impl OpMeta {
        /// inserts a value
        pub fn insert<V>(&mut self, key: OperatorId, value: V) -> Option<OwnedValue>
        where
            OwnedValue: From<V>,
        {
            self.0.insert(PrimStr(key), OwnedValue::from(value))
        }
        /// reads a value
        pub fn get(&mut self, key: OperatorId) -> Option<&OwnedValue> {
            self.0.get(&PrimStr(key))
        }
        /// checks existance of a key
        #[must_use]
        pub fn contains_key(&self, key: OperatorId) -> bool {
            self.0.contains_key(&PrimStr(key))
        }

        /// Merges two op meta maps, overwriting values with `other` on duplicates
        pub fn merge(&mut self, mut other: Self) {
            self.0.append(&mut other.0);
        }

        /// Returns `true` if this instance contains no values
        #[must_use]
        pub fn is_empty(&self) -> bool {
            self.0.is_empty()
        }
    }

    /// Pipeline status report
    pub mod report {
        use std::collections::HashMap;

        use crate::{
            dataplane::{InputTarget, OutputTarget},
            instance::State,
        };
        use tremor_common::ports::Port;
        use tremor_script::ast::DeployEndpoint;

        /// Status report for a pipeline
        #[derive(Debug, Clone)]
        pub struct Status {
            pub(crate) state: State,
            pub(crate) inputs: Vec<Input>,
            pub(crate) outputs: HashMap<String, Vec<Output>>,
        }

        impl Status {
            /// create a new status report
            #[must_use]

            pub fn new(
                state: State,
                inputs: Vec<Input>,
                outputs: HashMap<String, Vec<Output>>,
            ) -> Self {
                Self {
                    state,
                    inputs,
                    outputs,
                }
            }
            /// state of the pipeline   
            #[must_use]
            pub fn state(&self) -> State {
                self.state
            }
            /// inputs of the pipeline
            #[must_use]
            pub fn inputs(&self) -> &[Input] {
                &self.inputs
            }
            /// outputs of the pipeline
            #[must_use]
            pub fn outputs(&self) -> &HashMap<String, Vec<Output>> {
                &self.outputs
            }

            /// outputs of the pipeline
            #[must_use]
            pub fn outputs_mut(&mut self) -> &mut HashMap<String, Vec<Output>> {
                &mut self.outputs
            }
        }

        /// Input report
        #[derive(Debug, Clone, PartialEq)]
        pub enum Input {
            /// Pipeline input report
            Pipeline {
                /// alias of the pipeline
                alias: String,

                /// port of the pipeline
                port: Port<'static>,
            },
            /// Source input report
            Source {
                /// alias of the source
                alias: String,
                /// port of the source
                port: Port<'static>,
            },
        }

        impl Input {
            /// create a new pipeline input report
            #[must_use]
            pub fn pipeline(alias: &str, port: Port<'static>) -> Self {
                Self::Pipeline {
                    alias: alias.to_string(),
                    port,
                }
            }
            /// create a new source input report
            #[must_use]
            pub fn source(alias: &str, port: Port<'static>) -> Self {
                Self::Source {
                    alias: alias.to_string(),
                    port,
                }
            }
            /// create a new input report
            #[must_use]
            pub fn new(endpoint: &DeployEndpoint, target: &InputTarget) -> Self {
                match target {
                    InputTarget::Pipeline(_addr) => {
                        Input::pipeline(endpoint.alias(), endpoint.port().clone())
                    }
                    InputTarget::Source(_addr) => {
                        Input::source(endpoint.alias(), endpoint.port().clone())
                    }
                }
            }
        }

        /// Output report
        #[derive(Debug, Clone, PartialEq)]
        pub enum Output {
            /// Pipeline output report
            Pipeline {
                /// alias of the pipeline
                alias: String,
                /// port of the pipeline
                port: Port<'static>,
            },
            /// Sink output report
            Sink {
                /// alias of the sink
                alias: String,
                /// port of the sink
                port: Port<'static>,
            },
        }

        impl Output {
            /// create a new pipeline output report
            #[must_use]
            pub fn pipeline(alias: &str, port: Port<'static>) -> Self {
                Self::Pipeline {
                    alias: alias.to_string(),
                    port,
                }
            }
            /// create a new sink output report
            #[must_use]
            pub fn sink(alias: &str, port: Port<'static>) -> Self {
                Self::Sink {
                    alias: alias.to_string(),
                    port,
                }
            }
        }
        impl From<&(DeployEndpoint, OutputTarget)> for Output {
            fn from(target: &(DeployEndpoint, OutputTarget)) -> Self {
                match target {
                    (endpoint, OutputTarget::Pipeline(_)) => {
                        Output::pipeline(endpoint.alias(), endpoint.port().clone())
                    }
                    (endpoint, OutputTarget::Sink(_)) => {
                        Output::sink(endpoint.alias(), endpoint.port().clone())
                    }
                }
            }
        }
    }

    #[cfg(test)]
    mod test {
        use super::OpMeta;
        use simd_json::prelude::ValueAsScalar;
        use tremor_common::ids::{Id, OperatorId};

        #[test]
        fn op_meta_merge() {
            let op_id1 = OperatorId::new(1);
            let op_id2 = OperatorId::new(2);
            let op_id3 = OperatorId::new(3);
            let mut m1 = OpMeta::default();
            let mut m2 = OpMeta::default();
            m1.insert(op_id1, 1);
            m1.insert(op_id2, 1);
            m2.insert(op_id1, 2);
            m2.insert(op_id3, 2);
            m1.merge(m2);

            assert!(m1.contains_key(op_id1));
            assert!(m1.contains_key(op_id2));
            assert!(m1.contains_key(op_id3));

            assert_eq!(m1.get(op_id1).as_u64(), Some(2));
            assert_eq!(m1.get(op_id2).as_u64(), Some(1));
            assert_eq!(m1.get(op_id3).as_u64(), Some(2));
        }
    }
}
/// connector messages
pub mod connector {
    use std::{collections::HashMap, fmt::Display};

    use serde::{Deserialize, Serialize};
    use tokio::sync::mpsc::Sender;
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

    /// surce messages
    pub mod source {
        use tokio::sync::mpsc::{Sender, UnboundedSender};
        use tremor_common::ports::Port;
        use tremor_script::ast::DeployEndpoint;

        use crate::{connector, controlplane::CbAction, event::EventId, pipeline};

        use super::Attempt;

        /// address of a source
        #[derive(Clone, Debug)]
        pub struct Addr {
            /// the actual address
            addr: UnboundedSender<Msg>,
        }

        impl Addr {
            /// create a new source address
            #[must_use]
            pub fn new(addr: UnboundedSender<Msg>) -> Self {
                Self { addr }
            }
            /// send a message
            ///
            /// # Errors
            ///  * If sending failed
            pub fn send(&self, msg: Msg) -> Result<(), Error> {
                self.addr.send(msg).map_err(|_| Error::SendError)
            }
        }
        /// Source Addr errors
        #[derive(Debug, thiserror::Error)]
        pub enum Error {
            /// Error sending a message to source
            #[error("Error sending a message to source")]
            SendError,
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
            Drain(Sender<connector::Msg>),
            /// FIXME: #[cfg(test)]
            Synchronize(tokio::sync::oneshot::Sender<()>),
        }
    }

    /// sinks
    pub mod sink {
        use tokio::sync::mpsc::Sender;
        use tremor_common::ports::Port;
        use tremor_script::ast::DeployEndpoint;

        use crate::{event::Event, pipeline};

        use super::Attempt;

        /// Sink Addr errors
        #[derive(Debug, thiserror::Error)]
        pub enum Error {
            /// Error sending a message to source
            #[error("Error sending a message to sink")]
            SendError,
        }

        /// address of a connector sink
        #[derive(Clone, Debug)]
        pub struct Addr {
            /// the actual sender
            addr: Sender<Msg>,
        }
        impl Addr {
            /// create a new sink address
            #[must_use]
            pub fn new(addr: Sender<Msg>) -> Self {
                Self { addr }
            }
            /// send a message
            ///
            /// # Errors
            ///  * If sending failed
            pub async fn send(&self, msg: Msg) -> Result<(), Error> {
                self.addr.send(msg).await.map_err(|_| Error::SendError)
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
        dataplane::{InputTarget, OutputTarget},
        pipeline,
    };
    use tokio::sync::mpsc::Sender;
    use tremor_common::{
        ids::{SinkId, SourceId},
        ports::Port,
    };
    use tremor_script::ast::DeployEndpoint;

    /// A circuit breaker action
    #[derive(
        Debug,
        Clone,
        Copy,
        PartialEq,
        simd_json_derive::Serialize,
        simd_json_derive::Deserialize,
        Eq,
    )]
    pub enum CbAction {
        /// Nothing of note
        None,
        /// The circuit breaker is triggerd and should break
        Trigger,
        /// The circuit breaker is restored and should work again
        Restore,
        // TODO: add stream based CbAction variants once their use manifests
        /// Acknowledge delivery of messages up to a given ID.
        /// All messages prior to and including  this will be considered delivered.
        Ack,
        /// Fail backwards to a given ID
        /// All messages after and including this will be considered non delivered
        Fail,
        /// Notify all upstream sources that this sink has started, notifying them of its existence.
        /// Will be used for tracking for which sinks to wait during Drain.
        SinkStart(SinkId),
        /// answer to a `SignalKind::Drain(uid)` signal from a connector with the same uid
        Drained(SourceId, SinkId),
    }
    impl Default for CbAction {
        fn default() -> Self {
            Self::None
        }
    }

    impl From<bool> for CbAction {
        fn from(success: bool) -> Self {
            if success {
                CbAction::Ack
            } else {
                CbAction::Fail
            }
        }
    }

    impl CbAction {
        /// This message should always be delivered and not filtered out
        #[must_use]
        pub fn always_deliver(self) -> bool {
            self.is_cb() || matches!(self, CbAction::Drained(_, _) | CbAction::SinkStart(_))
        }
        /// This is a Circuit Breaker related message
        #[must_use]
        pub fn is_cb(self) -> bool {
            matches!(self, CbAction::Trigger | CbAction::Restore)
        }
        /// This is a Guaranteed Delivery related message
        #[must_use]
        pub fn is_gd(self) -> bool {
            matches!(self, CbAction::Ack | CbAction::Fail)
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
        /// Fetches status of the pipeline
        Inspect(Sender<pipeline::report::Status>),
    }

    #[cfg(test)]
    mod test {
        use crate::pipeline::PrimStr;

        use super::*;
        use simd_json_derive::{Deserialize, Serialize};

        #[test]
        fn prim_str() {
            let p = PrimStr(42);
            let fourtytwo = r#""42""#;
            let mut fourtytwo_s = fourtytwo.to_string();
            let mut fourtytwo_i = "42".to_string();
            assert_eq!(fourtytwo, p.json_string().unwrap_or_default());
            assert_eq!(
                PrimStr::from_slice(unsafe { fourtytwo_s.as_bytes_mut() }),
                Ok(p)
            );
            assert!(PrimStr::<i32>::from_slice(unsafe { fourtytwo_i.as_bytes_mut() }).is_err());
        }

        #[test]
        fn cbaction_creation() {
            assert_eq!(CbAction::default(), CbAction::None);
            assert_eq!(CbAction::from(true), CbAction::Ack);
            assert_eq!(CbAction::from(false), CbAction::Fail);
        }

        #[test]
        fn cbaction_is_gd() {
            assert!(!CbAction::None.is_gd());

            assert!(CbAction::Fail.is_gd());
            assert!(CbAction::Ack.is_gd());

            assert!(!CbAction::Restore.is_gd());
            assert!(!CbAction::Trigger.is_gd());
        }

        #[test]
        fn cbaction_is_cb() {
            assert!(!CbAction::None.is_cb());

            assert!(!CbAction::Fail.is_cb());
            assert!(!CbAction::Ack.is_cb());

            assert!(CbAction::Restore.is_cb());
            assert!(CbAction::Trigger.is_cb());
        }
    }
}

/// contraflow
pub mod contraflow {
    use crate::event::Event;

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
