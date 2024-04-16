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
    /// Forces syncronization with the source
    Synchronize(tokio::sync::oneshot::Sender<()>),
}
