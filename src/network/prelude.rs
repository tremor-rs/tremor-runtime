// Copyright 2020-2021, The Tremor Team
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

#![cfg(not(tarpaulin_include))]

use super::{
    control::{fsm::ControlLifecycleFsm, ControlState},
    NetworkCont,
};
pub(crate) use crate::errors::Result;
use crate::{network::control::ControlProtocol, source::tnt::SerializedResponse};
use async_channel::Sender;
use simd_json::json;
pub(crate) use tremor_pipeline::Event;
use tremor_script::{LineValue, ValueAndMeta};

/// Macro that creates an event based on a json type template
#[macro_export]
macro_rules! event {
    ($json:tt) => {{
        let value: Value = json!($json).into();
        let mut event = Event::default();
        event.data = LineValue::new(vec![], |_| {
            ValueAndMeta::from_parts(value, Value::Object(Box::new(hashmap! {})))
        });
        event as Event
    }};
}

/// Macro that creates a continuation for network protocol event actions
#[macro_export]
macro_rules! wsc_reply {
    ( $event:ident, $v:tt) => {
        Ok(NetworkCont::SourceReply(Event {
            id: $event.id.clone(),
            data: LineValue::new(vec![], |_| {
                ValueAndMeta::from_parts(json!($v).into(), json!({}).into())
            }),
            ingest_ns: $event.ingest_ns,
            origin_uri: $event.origin_uri.clone(),
            kind: None,
            is_batch: false,
            cb: $event.cb.clone(),
            op_meta: $event.op_meta.clone(),
            transactional: false,
        })) as Result<NetworkCont>
    };
}

// A stream represents a distinct connection such as a tcp client
// connection using a monotonic locally unique id
//
pub(crate) type StreamId = usize;

pub(crate) trait NetworkProtocol: NetworkProtocolClone {
    /// A new protocol session has been initiated by a connected participant
    fn on_init(&mut self) -> Result<()>;
    /// A data plane event for this protocol has been received
    fn on_event(&mut self, event: &Event) -> Result<NetworkCont>;
}

pub(crate) trait NetworkProtocolClone {
    fn clone_box(&self) -> Box<dyn NetworkProtocol>;
}

impl<P> NetworkProtocolClone for P
where
    P: 'static + NetworkProtocol + Clone,
{
    fn clone_box(&self) -> Box<dyn NetworkProtocol> {
        Box::new(self.clone())
    }
}

#[derive(Debug)]
pub(crate) struct NetworkSession {
    pub(crate) sid: StreamId,
    pub(crate) is_connected: bool,
    pub(crate) fsm: ControlLifecycleFsm,
}

unsafe impl Sync for NetworkSession {}
unsafe impl Send for NetworkSession {}

impl NetworkSession {
    pub(crate) fn new(sid: StreamId, control: ControlProtocol) -> Result<Self> {
        Ok(Self {
            sid,
            is_connected: true,
            fsm: ControlLifecycleFsm::new(sid, control)?,
        })
    }

    pub(crate) fn on_event(
        &mut self,
        _origin: &Sender<SerializedResponse>,
        event: &Event,
    ) -> Result<NetworkCont> {
        match self.fsm.on_event(event) {
            // We are connecting and activating a protocol session channel
            Ok(NetworkCont::ConnectProtocol(protocol, alias, next_state)) => {
                if self.fsm.control.active_protocols.len() > 1 {
                    self.fsm.transition(next_state)?;
                }
                return Ok(NetworkCont::ConnectProtocol(protocol, alias, next_state));
            }

            // We are disconnecting an active protocol session channel
            Ok(NetworkCont::DisconnectProtocol(alias)) => {
                if self.fsm.control.active_protocols.len() == 1 {
                    self.fsm.transition(ControlState::Connecting)?;
                }
                return Ok(NetworkCont::DisconnectProtocol(alias));
            }

            // Transmit data from tremor to the network connected participant
            Ok(NetworkCont::SourceReply(r)) => return Ok(NetworkCont::SourceReply(r)),

            // A known error was detected, propagate
            on_close @ Ok(NetworkCont::Close(_)) => return on_close,

            // By construction, we should really never get an error here
            Err(_) => return wsc_reply!(event, { "tremor": { "close": "unknown server error" }}),
        }
    }
}
