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
    nana::StatusCode,
    NetworkCont,
};
pub(crate) use crate::errors::Result;
use crate::{network::control::ControlProtocol, source::tnt::SerializedResponse};
use async_channel::Sender;
pub(crate) use tremor_pipeline::Event;
use tremor_value::json;
use tremor_value::Value;

/// Given a serializable rust struct, produces a tremor value
pub(crate) fn destructurize<T>(value: &T) -> Result<Value>
where
    T: serde::ser::Serialize,
{
    let mut value: String = simd_json::to_string(&value)?;
    let value = unsafe { value.as_mut_vec() };
    let value: Value = tremor_value::to_value(value)?.into_static();
    Ok(value)
}

// Given a tremor value, produces a rust struct via deserialization
pub(crate) fn structurize<'de, T>(value: Value<'de>) -> Result<T>
where
    T: serde::de::Deserialize<'de>,
{
    Ok(T::deserialize(value)?)
}

/// Macro that creates an event based on a json type template
#[macro_export]
macro_rules! event {
    ($json:tt) => {{
        // Convenience for macro call-sites to avoid needing to define
        // these deps
        use halfbrown::HashMap;
        use tremor_script::LineValue;
        use tremor_script::ValueAndMeta;
        use tremor_value::json;

        let value: Value = json!($json).into();
        let mut event = Event::default();
        event.data = LineValue::new(vec![], |_| {
            ValueAndMeta::from_parts(value, Value::Object(Box::new(HashMap::new())))
        });
        event as Event
    }};
}

/// Macro that creates a continuation for network protocol event actions
#[macro_export]
macro_rules! wsc_reply {
    ( $event:ident, $v:tt) => {{
        use tremor_script::LineValue;
        use tremor_script::ValueAndMeta;
        use tremor_value::json;
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
    }};
}

pub(crate) fn close(reason: StatusCode) -> Result<NetworkCont> {
    Ok(NetworkCont::Close(event!(
        {"tremor":
        { "close":
            &format!("{}.{}: {}", reason.base(), reason.code(), reason.canonical_reason())
        }
    }
    )))
}

// A stream represents a distinct connection such as a tcp client
// connection using a monotonic locally unique id
//
pub(crate) type StreamId = usize;

#[async_trait::async_trait()]
pub(crate) trait NetworkProtocol: NetworkProtocolClone + Send + Sync {
    /// A new protocol session has been initiated by a connected participant
    fn on_init(&mut self) -> Result<()>;
    /// A data plane event for this protocol has been received
    async fn on_event(&mut self, sid: StreamId, event: &Event) -> Result<NetworkCont>;

    async fn on_data(&mut self) -> Result<Option<Vec<Event>>> {
        Ok(None) // Do nothing by default
    } // FIXME exploration REMOVE/DELETE when done
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

impl NetworkSession {
    pub(crate) fn new(sid: StreamId, control: ControlProtocol) -> Result<Self> {
        Ok(Self {
            sid,
            is_connected: true,
            fsm: ControlLifecycleFsm::new(sid, control)?,
        })
    }

    pub(crate) async fn on_data(&mut self) -> Result<Option<Vec<Event>>> {
        self.fsm.on_data().await
    }

    pub(crate) async fn on_event(
        &mut self,
        _origin: &Sender<SerializedResponse>,
        event: &Event,
    ) -> Result<NetworkCont> {
        match self.fsm.on_event(event).await {
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

            // Propagate to caller - loopback transitions so no state change
            on_reply @ Ok(NetworkCont::SourceReply(_)) => return on_reply,
            on_close @ Ok(NetworkCont::Close(_)) => return on_close,
            on_none @ Ok(NetworkCont::None) => return on_none,

            // By construction, this is a protocol bug and unexpected
            Err(_e) => {
                self.fsm.transition(ControlState::Invalid)?;
                return wsc_reply!(event, { "tremor": { "close": "unknown server error" }});
            }
        }
    }
}

#[cfg(test)]
macro_rules! assert_cont {
    ($actual:expr, $expected:tt) => {{
        match $actual {
            NetworkCont::SourceReply(Event { data, .. }) => {
                let value: Value = json!($expected).into();
                assert_eq!(&value, data.parts().0);
            }
            NetworkCont::Close(Event { data, .. }) => {
                let value: Value = json!($expected).into();
                assert_eq!(&value, data.parts().0);
            }
            _ => {
                assert!(false, "nope not supported in assert");
            }
        };
    }};
}

#[cfg(test)]
macro_rules! assert_err {
    ($actual:expr, $expected:expr) => {{
        assert!($actual.is_err());
        assert_eq!($expected, $actual.unwrap_err().to_string());
    }};
}
