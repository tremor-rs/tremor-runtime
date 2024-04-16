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

use crate::event::Event;
use crate::{
    connector::{self, sink, source},
    pipeline,
};
use tremor_common::{ids::SourceId, ports::Port};

/// contraflow
pub mod contraflow;

/// The kind of signal this is
#[derive(
    Debug, Clone, Copy, PartialEq, simd_json_derive::Serialize, simd_json_derive::Deserialize, Eq,
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
    pub async fn send_event(&mut self, input: Port<'static>, event: Event) -> Result<(), Error> {
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
