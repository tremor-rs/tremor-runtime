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
