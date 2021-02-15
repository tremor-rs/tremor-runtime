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

use super::{api::ApiProtocol, echo::EchoProtocol, pubsub::PubSubProtocol, NetworkCont};
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
                            return protocol_error("control already connected");
                        }

                        // If we get here we have a new protocol alias to register
                        self.active_protocols
                            .insert(alias.to_string(), protocol.to_string());
                        match protocol {
                            "echo" => {
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
                            _unknown_protocol => {
                                return protocol_error("protocol not found error");
                            }
                        }

                        return Ok(NetworkCont::ConnectProtocol(
                            protocol.to_string(),
                            alias.to_string(),
                            ControlState::Active,
                        ));
                    } else {
                        return protocol_error("protocol not specified error");
                    }
                } else if let Some(Value::Object(disconnect)) = control.get("disconnect") {
                    let alias = disconnect.get("protocol");
                    match alias {
                        Some(Value::String(alias)) => {
                            let alias = alias.to_string();
                            self.active_protocols.remove(&alias);
                            self.mux.remove(&alias);
                            return Ok(NetworkCont::DisconnectProtocol(alias.to_string()));
                        }
                        _otherwise => {
                            return protocol_error("protocol not found error");
                        }
                    }
                } else {
                    return protocol_error("unsupported control operation");
                }
            }

            // Each well-formed message record MUST have only 1 field
            if value.len() != 1 {
                return protocol_error("expected one record field");
            } else {
                match value.keys().next() {
                    Some(key) => match self.mux.get_mut(&key.to_string()) {
                        Some(active_protocol) => {
                            let resp = active_protocol.on_event(sid, event).await?;
                            return Ok(resp);
                        }
                        None => {
                            return protocol_error(&format!(
                                "protocol session not found for {}",
                                &key.to_string()
                            ));
                        }
                    },
                    None => {
                        return protocol_error("expected one record field");
                    }
                }
            }
        } else {
            return protocol_error("control record expected");
        }
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
        let (tx, _rx) = bounded::<system::ManagerMsg>(1);
        let conductor = Conductor::new(tx);
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
                    "close": "protocol not specified error"
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
                    "close": "protocol not found error"
                }
            }))
        );

        Ok(())
    }

    #[async_std::test]
    async fn control_alias_connect_bad_protocol() -> Result<()> {
        let (tx, _rx) = bounded::<system::ManagerMsg>(1);
        let conductor = Conductor::new(tx);
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
                    "close": "protocol not specified error",
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
                    "close": "protocol not found error"
                }
            }))
        );

        Ok(())
    }

    #[async_std::test]
    async fn control_bad_operation() -> Result<()> {
        let (tx, _rx) = bounded::<system::ManagerMsg>(1);
        let conductor = Conductor::new(tx);
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
                    "close": "unsupported control operation",
                }
            }))
        );

        Ok(())
    }

    #[async_std::test]
    async fn network_session_cannot_override_control() -> Result<()> {
        let stream_id = 0 as StreamId;
        let (tx, _rx) = bounded::<system::ManagerMsg>(1);
        let conductor = Conductor::new(tx);
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
        let stream_id = 0 as StreamId;
        let (tx, _rx) = bounded::<system::ManagerMsg>(1);
        let conductor = Conductor::new(tx);
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
                        "protocol": "echo"
                    }
                }}),
            )
            .await?;
        assert_eq!(1, session.fsm.control.active_protocols.len());
        assert_eq!(1, session.fsm.control.mux.len());
        assert_eq!(ControlState::Connecting, session.fsm.state);

        session.fsm.transition(ControlState::Active)?;
        session.fsm.transition(ControlState::Disconnecting)?;
        assert_eq!(ControlState::Disconnecting, session.fsm.state);
        session.fsm.transition(ControlState::Zombie)?;
        assert_eq!(ControlState::Zombie, session.fsm.state);

        Ok(())
    }

    #[async_std::test]
    async fn data_plane_mediation() -> Result<()> {
        let stream_id = 0 as StreamId;
        let (tx, _rx) = bounded::<system::ManagerMsg>(1);
        let conductor = Conductor::new(tx);
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
                event!({"tremor": {"close": "protocol session not found for badger"}})
            ),
            actual,
        );

        let actual = session
            .on_event(&sender, &event!({"fleek": 1, "flook": 2}))
            .await?;
        assert_eq!(
            NetworkCont::Close(event!({"tremor": {"close": "expected one record field"}})),
            actual,
        );

        let actual = session.on_event(&sender, &event!({})).await?;
        assert_eq!(
            NetworkCont::Close(event!({"tremor": {"close": "expected one record field"}})),
            actual,
        );

        let actual = session.on_event(&sender, &event!("snot")).await?;
        assert_eq!(
            NetworkCont::Close(event!({"tremor": {"close": "control record expected"}})),
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
            NetworkCont::Close(event!({"tremor": {"close": "control already connected"}})),
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
            NetworkCont::Close(event!({"tremor": {"close": "control already connected"}})),
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
            NetworkCont::Close(event!({"tremor": {"close": "protocol not found error"}})),
            actual,
        );

        session.fsm.transition(ControlState::Active)?;
        session.fsm.transition(ControlState::Disconnecting)?;
        assert_eq!(ControlState::Disconnecting, session.fsm.state);
        session.fsm.transition(ControlState::Zombie)?;
        assert_eq!(ControlState::Zombie, session.fsm.state);

        Ok(())
    }
}
