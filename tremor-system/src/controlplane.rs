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
    Debug, Clone, Copy, PartialEq, simd_json_derive::Serialize, simd_json_derive::Deserialize, Eq,
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
