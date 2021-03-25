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

// FIXME TODO - Add timed-wait from Connecting->Active
// FIXME TODO - Add timed-wait from Disconnecting->Zombie->Inactive(Done)
// FIMXE TODO - formalize close error reasons

use crate::{errors::Result, system::Conductor};

use super::{
    api::ApiProtocol, echo::EchoProtocol, microring::MicroRingProtocol, nana::StatusCode,
    pubsub::PubSubProtocol, NetworkCont,
};
use crate::network::prelude::*;
use halfbrown::HashMap as HalfMap;
use std::collections::HashMap;
pub(crate) use tremor_pipeline::Event;
use tremor_script::Value;

pub(crate) mod fsm;

#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) enum ControlState {
    Connecting,
    Active,
    Disconnecting,
    #[allow(dead_code)] // FIXME implement timed states in control protocol
    Zombie,
    Invalid,
}

impl std::fmt::Display for ControlState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match *self {
            ControlState::Connecting => "connecting",
            ControlState::Active => "active",
            ControlState::Disconnecting => "disconnecting",
            ControlState::Zombie => "zombie",
            ControlState::Invalid => "invalid",
        };
        write!(f, "{}", str)
    }
}

type ProtocolAlias = String;
#[derive(Clone)]
pub(crate) struct ControlProtocol {
    pub(crate) conductor: Conductor,
    // Protocol multiplexing micro-state
    pub(crate) active_protocols: HashMap<ProtocolAlias, String>,
    pub(crate) mux: HashMap<String, Box<dyn NetworkProtocol>>,
}

const CONTROL_ALIAS: &str = "tremor";

impl Clone for Box<dyn NetworkProtocol> {
    fn clone(&self) -> Self {
        self.clone_box() // Snot!
    }
}

impl std::fmt::Debug for ControlProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ControlProtocol")
    }
}

impl ControlProtocol {
    pub(crate) fn new(conductor: &Conductor) -> Self {
        let mut instance = Self {
            conductor: conductor.clone(),
            active_protocols: HashMap::new(),
            mux: HashMap::new(),
        };

        // Self-register consuming the `control` alias
        instance
            .active_protocols
            .insert(CONTROL_ALIAS.to_string(), "control".to_string());
        instance
            .mux
            .insert(CONTROL_ALIAS.to_string(), instance.clone_box());
        instance.on_init().unwrap();

        instance
    }

    pub(crate) fn num_active_protocols(&self) -> usize {
        self.mux.len()
    }
}

#[async_trait::async_trait]
impl NetworkProtocol for ControlProtocol {
    fn on_init(&mut self) -> Result<()> {
        trace!("Initializing control for network protocol");
        Ok(())
    }

    async fn on_event(&mut self, sid: StreamId, event: &Event) -> Result<NetworkCont> {
        trace!("Received control protocol event");
        if let (Value::Object(value), ..) = event.data.parts() {
            // CONTROL_ALIAS is the first initial "tremor"
            if let Some(Value::Object(control)) = value.get(CONTROL_ALIAS) {
                if let Some(Value::Object(connect)) = control.get("connect") {
                    if let Some(Value::String(protocol)) = connect.get("protocol") {
                        let protocol: &str = &protocol.to_string();
                        let headers: Value = if let Some(headers) = connect.get("headers") {
                            headers.to_owned()
                        } else {
                            Value::Object(Box::new(HalfMap::new()))
                        };
                        let alias = if let Some(Value::String(alias)) = connect.get("alias") {
                            alias.to_string()
                        } else {
                            protocol.to_string()
                        };

                        if "control" == protocol || self.active_protocols.contains_key(&alias) {
                            return close(StatusCode::PROTOCOL_ALREADY_REGISTERED);
                        }

                        // If we get here we have a new protocol alias to register
                        self.active_protocols
                            .insert(alias.to_string(), protocol.to_string());
                        match protocol {
                            "echo" => {
                                //dbg!("registering echo");
                                let proto = Box::new(EchoProtocol::new(self, headers));
                                self.mux.insert(alias.to_string(), proto);
                            }
                            "pubsub" => {
                                let proto =
                                    Box::new(PubSubProtocol::new(self, alias.to_string(), headers));
                                self.mux.insert(alias.to_string(), proto);
                            }
                            "api" => {
                                let proto =
                                    Box::new(ApiProtocol::new(self, alias.to_string(), headers));
                                self.mux.insert(alias.to_string(), proto);
                            }
                            "microring" => {
                                let proto = Box::new(MicroRingProtocol::new(
                                    self,
                                    alias.to_string(),
                                    headers,
                                ));
                                self.mux.insert(alias.to_string(), proto);
                            }
                            _unknown_protocol => {
                                return close(StatusCode::PROTOCOL_NOT_FOUND);
                            }
                        }

                        return Ok(NetworkCont::ConnectProtocol(
                            protocol.to_string(),
                            alias.to_string(),
                            ControlState::Active,
                        ));
                    } else {
                        return close(StatusCode::PROTOCOL_NOT_SPECIFIED);
                    }
                } else if let Some(Value::Object(disconnect)) = control.get("disconnect") {
                    let alias = disconnect.get("alias");
                    match alias {
                        Some(Value::String(alias)) => {
                            let alias = alias.to_string();
                            self.active_protocols.remove(&alias);
                            self.mux.remove(&alias);
                            return Ok(NetworkCont::DisconnectProtocol(alias.to_string()));
                        }
                        _otherwise => {
                            return close(StatusCode::PROTOCOL_SESSION_NOT_FOUND);
                        }
                    }
                } else {
                    return close(StatusCode::PROTOCOL_OPERATION_INVALID);
                }
            } else if let Some(Value::String(maybe_close)) = value.get(CONTROL_ALIAS) {
                let maybe_close = maybe_close.to_string();
                if "close" == &maybe_close {
                    return Ok(NetworkCont::Close(
                        event!({"tremor": { "close-ack": "ok".to_string() } }),
                    ));
                } else {
                    return close(StatusCode::PROTOCOL_RECORD_EXPECTED);
                }
            }

            // Each well-formed message record MUST have only 1 field
            if value.len() != 1 {
                return close(StatusCode::PROTOCOL_RECORD_ONE_FIELD_EXPECTED);
            } else {
                //dbg!("choosing protocol to respond with");
                match value.keys().next() {
                    Some(key) => match self.mux.get_mut(&key.to_string()) {
                        Some(active_protocol) => {
                            let resp = active_protocol.on_event(sid, event).await?;
                            return Ok(resp);
                        }
                        None => {
                            return close(StatusCode::PROTOCOL_SESSION_NOT_FOUND);
                        }
                    },
                    None => {
                        return close(StatusCode::PROTOCOL_RECORD_ONE_FIELD_EXPECTED);
                    }
                }
            }
        }

        return close(StatusCode::PROTOCOL_RECORD_EXPECTED);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::network::prelude::NetworkSession;
    use crate::system;
    use crate::{errors::Result, event, system::Conductor};
    use async_channel::bounded;
    use tremor_script::Value;

    #[async_std::test]
    async fn control_connect_bad_protocol() -> Result<()> {
        use crate::temp_network::ws::UrMsg;
        let (uring_tx, _) = bounded::<UrMsg>(1);
        let (tx, _rx) = bounded::<system::ManagerMsg>(1);
        let conductor = Conductor::new(tx, uring_tx);
        let mut control = ControlProtocol::new(&conductor);

        let actual = control
            .on_event(
                0,
                &event!({
                    "tremor": {
                    "connect": {
                        "protocol_malformed": "control"
                    }
                }}),
            )
            .await?;

        assert_eq!(
            actual,
            NetworkCont::Close(event!({
                "tremor": {
                    "close": "0.201: Protocol not specified error"
                }
            }))
        );

        let actual = control
            .on_event(
                0,
                &event!({
                    "tremor": {
                    "connect": {
                        "protocol": "flork"
                    }
                }}),
            )
            .await?;

        assert_eq!(
            actual,
            NetworkCont::Close(event!({
                "tremor": {
                    "close": "0.200: Protocol not found error"
                }
            }))
        );

        Ok(())
    }

    #[async_std::test]
    async fn control_alias_connect_bad_protocol() -> Result<()> {
        use crate::temp_network::ws::UrMsg;
        let (uring_tx, _) = bounded::<UrMsg>(1);
        let (tx, _rx) = bounded::<system::ManagerMsg>(1);
        let conductor = Conductor::new(tx, uring_tx);
        let mut control = ControlProtocol::new(&conductor);

        let actual = control
            .on_event(
                0,
                &event!({
                    "tremor": {
                    "connect": {
                        "protocol_malformed": "control",
                        "alias": "snot"
                    }
                }}),
            )
            .await?;

        assert_eq!(
            actual,
            NetworkCont::Close(event!({
                "tremor": {
                    "close": "0.201: Protocol not specified error",
                }
            }))
        );

        let actual = control
            .on_event(
                0,
                &event!({
                    "tremor": {
                    "connect": {
                        "protocol": "flork",
                        "alias": "snot"
                    }
                }}),
            )
            .await?;

        assert_eq!(
            actual,
            NetworkCont::Close(event!({
                "tremor": {
                    "close": "0.200: Protocol not found error"
                }
            }))
        );

        Ok(())
    }

    #[async_std::test]
    async fn control_bad_operation() -> Result<()> {
        use crate::temp_network::ws::UrMsg;
        let (uring_tx, _) = bounded::<UrMsg>(1);
        let (tx, _rx) = bounded::<system::ManagerMsg>(1);
        let conductor = Conductor::new(tx, uring_tx);
        let mut control = ControlProtocol::new(&conductor);

        let actual = control
            .on_event(
                0,
                &event!({
                    "tremor": {
                    "snot": { }
                }}),
            )
            .await?;

        assert_eq!(
            actual,
            NetworkCont::Close(event!({
                "tremor": {
                    "close": "0.202: Protocol operation not supported",
                }
            }))
        );

        Ok(())
    }

    #[async_std::test]
    async fn network_session_cannot_override_control() -> Result<()> {
        use crate::temp_network::ws::UrMsg;
        let (uring_tx, _) = bounded::<UrMsg>(1);
        let stream_id = 0 as StreamId;
        let (tx, _rx) = bounded::<system::ManagerMsg>(1);
        let conductor = Conductor::new(tx, uring_tx);
        let control = ControlProtocol::new(&conductor);
        let mut session = NetworkSession::new(stream_id, control)?;
        let (sender, _receiver) = async_channel::bounded(1);
        assert_eq!(ControlState::Connecting, session.fsm.state);

        session
            .on_event(
                &sender,
                &event!({
                    "tremor": {
                    "connect": {
                        "protocol": "control"
                    }
                }}),
            )
            .await?;
        assert_eq!(ControlState::Connecting, session.fsm.state);
        Ok(())
    }

    #[async_std::test]
    async fn network_session_control_connect_disconnect_lifecycle() -> Result<()> {
        use crate::temp_network::ws::UrMsg;
        let (uring_tx, _) = bounded::<UrMsg>(1);
        let stream_id = 0 as StreamId;
        let (tx, _rx) = bounded::<system::ManagerMsg>(1);
        let conductor = Conductor::new(tx, uring_tx);
        let control = ControlProtocol::new(&conductor);
        let mut session = NetworkSession::new(stream_id, control)?;

        let (sender, _receiver) = async_channel::bounded(1);

        // Initial state
        assert_eq!(1, session.fsm.control.active_protocols.len());
        assert_eq!(1, session.fsm.control.mux.len());
        assert_eq!(ControlState::Connecting, session.fsm.state);

        session
            .on_event(
                &sender,
                &event!({
                    "tremor": {
                    "connect": {
                        "protocol": "echo"
                    }
                }}),
            )
            .await?;
        assert_eq!(2, session.fsm.control.active_protocols.len());
        assert_eq!(2, session.fsm.control.mux.len());
        assert_eq!(ControlState::Active, session.fsm.state);
        session
            .on_event(
                &sender,
                &event!({
                    "tremor": {
                    "disconnect": {
                        "alias": "echo"
                    }
                }}),
            )
            .await?;
        assert_eq!(1, session.fsm.control.active_protocols.len());
        assert_eq!(1, session.fsm.control.mux.len());
        assert_eq!(ControlState::Active, session.fsm.state);

        session.fsm.transition(ControlState::Active)?;
        session.fsm.transition(ControlState::Disconnecting)?;
        assert_eq!(ControlState::Disconnecting, session.fsm.state);
        session.fsm.transition(ControlState::Zombie)?;
        assert_eq!(ControlState::Zombie, session.fsm.state);

        Ok(())
    }

    #[async_std::test]
    async fn data_plane_mediation() -> Result<()> {
        use crate::temp_network::ws::UrMsg;
        let (uring_tx, _) = bounded::<UrMsg>(1);
        let stream_id = 0 as StreamId;
        let (tx, _rx) = bounded::<system::ManagerMsg>(1);
        let conductor = Conductor::new(tx, uring_tx);
        let control = ControlProtocol::new(&conductor);
        let mut session = NetworkSession::new(stream_id, control)?;

        let (sender, _receiver) = async_channel::bounded(1);

        // Initial state
        assert_eq!(1, session.fsm.control.active_protocols.len());
        assert_eq!(1, session.fsm.control.mux.len());
        assert_eq!(ControlState::Connecting, session.fsm.state);

        session
            .on_event(
                &sender,
                &event!({
                    "tremor": {
                    "connect": {
                        "protocol": "echo"
                    }
                }}),
            )
            .await?;
        assert_eq!(2, session.fsm.control.active_protocols.len());
        assert_eq!(2, session.fsm.control.mux.len());
        assert_eq!(ControlState::Active, session.fsm.state);

        session
            .on_event(
                &sender,
                &event!({
                    "tremor": {
                    "connect": {
                        "protocol": "echo",
                        "alias": "snot",
                    }
                }}),
            )
            .await?;
        assert_eq!(3, session.fsm.control.active_protocols.len());
        assert_eq!(3, session.fsm.control.mux.len());
        assert_eq!(ControlState::Active, session.fsm.state);

        let actual = session
            .on_event(&sender, &event!({"echo": "badger"}))
            .await?;
        assert_eq!(NetworkCont::SourceReply(event!({"echo": "badger"})), actual);

        let actual = session
            .on_event(&sender, &event!({"snot": "badger"}))
            .await?;
        assert_eq!(NetworkCont::SourceReply(event!({"snot": "badger"})), actual);

        let actual = session
            .on_event(&sender, &event!({"badger": "badger"}))
            .await?;
        assert_eq!(
            NetworkCont::Close(
                event!({"tremor": {"close": "0.207: Protocol session not connected error"}})
            ),
            actual,
        );

        let actual = session
            .on_event(&sender, &event!({"fleek": 1, "flook": 2}))
            .await?;
        assert_eq!(
            NetworkCont::Close(
                event!({"tremor": {"close": "0.205: Protocol operation - record requires one field only"}})
            ),
            actual,
        );

        let actual = session.on_event(&sender, &event!({})).await?;
        assert_eq!(
            NetworkCont::Close(
                event!({"tremor": {"close": "0.205: Protocol operation - record requires one field only"}})
            ),
            actual,
        );

        let actual = session.on_event(&sender, &event!("snot")).await?;
        assert_eq!(
            NetworkCont::Close(
                event!({"tremor": {"close": "0.203: Protocol operation expected a record"}})
            ),
            actual,
        );

        let actual = session
            .on_event(
                &sender,
                &event!({
                    "tremor": {
                    "connect": {
                        "protocol": "control",
                    }
                }}),
            )
            .await?;
        assert_eq!(
            NetworkCont::Close(
                event!({"tremor": {"close": "0.206: Protocol already registered error"}})
            ),
            actual,
        );

        let actual = session
            .on_event(
                &sender,
                &event!({
                    "tremor": {
                    "connect": {
                        "protocol": "control",
                        "alias": "control",
                    }
                }}),
            )
            .await?;
        assert_eq!(
            NetworkCont::Close(
                event!({"tremor": {"close": "0.206: Protocol already registered error"}})
            ),
            actual,
        );

        let actual = session
            .on_event(
                &sender,
                &event!({
                    "tremor": {
                    "connect": {
                        "protocol": "snot",
                        "alias": "control",
                    }
                }}),
            )
            .await?;
        assert_eq!(
            NetworkCont::Close(event!({"tremor": {"close": "0.200: Protocol not found error"}})),
            actual,
        );

        session.fsm.transition(ControlState::Active)?;
        session
            .on_event(
                &sender,
                &event!({ "tremor": { "disconnect": { "alias": "snot"}}}),
            )
            .await?;
        session.fsm.transition(ControlState::Active)?;
        session
            .on_event(
                &sender,
                &event!({ "tremor": { "disconnect": { "alias": "echo"}}}),
            )
            .await?;
        session.fsm.transition(ControlState::Disconnecting)?;
        assert_eq!(ControlState::Disconnecting, session.fsm.state);
        session.fsm.transition(ControlState::Zombie)?;
        assert_eq!(ControlState::Zombie, session.fsm.state);

        Ok(())
    }
}
