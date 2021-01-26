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

use crate::errors::Result;

use super::{echo::EchoProtocol, pubsub::PubSubProtocol, NetworkCont};
use crate::network::prelude::*;
use crate::offramp;
use crate::onramp;
use crate::pipeline;
use async_channel::Sender;
use halfbrown::{hashmap, HashMap as HalfMap};
use simd_json::json;
use std::collections::HashMap;
pub(crate) use tremor_pipeline::Event;
use tremor_script::{LineValue, Value, ValueAndMeta};

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
    pub(crate) onramp: Sender<onramp::ManagerMsg>,
    pub(crate) offramp: Sender<offramp::ManagerMsg>,
    pub(crate) pipeline: Sender<pipeline::ManagerMsg>,
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
    pub(crate) fn new(
        onramp: Sender<onramp::ManagerMsg>,
        offramp: Sender<offramp::ManagerMsg>,
        pipeline: Sender<pipeline::ManagerMsg>,
    ) -> Self {
        let mut instance = Self {
            onramp: onramp.clone(),
            offramp: offramp.clone(),
            pipeline: pipeline.clone(),
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

unsafe impl Send for ControlProtocol {}
unsafe impl Sync for ControlProtocol {}

impl NetworkProtocol for ControlProtocol {
    fn on_init(&mut self) -> Result<()> {
        info!("Initializing control for network protocol");
        Ok(())
    }

    fn on_event(&mut self, event: &Event) -> Result<NetworkCont> {
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
                            return Ok(NetworkCont::Close(
                                event!({ CONTROL_ALIAS: { "close": "already connected" }}),
                            ));
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
                                let proto = Box::new(PubSubProtocol::new(self, headers));
                                self.mux.insert(alias.to_string(), proto);
                            }
                            _unknown_protocol => {
                                return Ok(NetworkCont::Close(event!({
                                    CONTROL_ALIAS: {
                                        "close": "protocol not found error"
                                    }
                                })));
                            }
                        }

                        return Ok(NetworkCont::ConnectProtocol(
                            protocol.to_string(),
                            alias.to_string(),
                            ControlState::Active,
                        ));
                    } else {
                        return Ok(NetworkCont::Close(event!({
                            CONTROL_ALIAS: {
                                "close": "protocol not specified error"
                            }
                        })));
                    }
                } else if let Some(Value::Object(disconnect)) = control.get("disconnect") {
                    let alias = disconnect.get("protocol");
                    dbg!(&alias);
                    match alias {
                        Some(Value::String(alias)) => {
                            let alias = alias.to_string();
                            self.active_protocols.remove(&alias);
                            self.mux.remove(&alias);
                            return Ok(NetworkCont::DisconnectProtocol(alias.to_string()));
                        }
                        _otherwise => {
                            return Ok(NetworkCont::Close(event!({
                                CONTROL_ALIAS: {
                                    "close": "protocol not found error"
                                }
                            })));
                        }
                    }
                } else {
                    return Ok(NetworkCont::Close(event!({
                        CONTROL_ALIAS: {
                            "close": "unsupported control operation"
                        }
                    })));
                }
            }

            // Each well-formed message record MUST have only 1 field
            if value.len() != 1 {
                return Ok(NetworkCont::Close(
                    event!({CONTROL_ALIAS: {"close": "expected 1 record field"}}),
                ));
            } else {
                match value.keys().next() {
                    Some(key) => match self.mux.get_mut(&key.to_string()) {
                        Some(active_protocol) => {
                            let resp = active_protocol.on_event(event)?;
                            return Ok(resp);
                        }
                        None => {
                            return Ok(NetworkCont::Close(event!({
                                CONTROL_ALIAS: {
                                    "close": format!("protocol session not found for {}", &key.to_string())
                                }
                            })));
                        }
                    },
                    None => {
                        return Ok(NetworkCont::Close(event!({
                            CONTROL_ALIAS: {
                                "close": format!("expected record with 1 field")
                            }
                        })));
                    }
                }
            }
        } else {
            return Ok(NetworkCont::Close(event!({
                CONTROL_ALIAS: {
                    "close": format!("record expected")
                }
            })));
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::network::prelude::NetworkSession;
    use crate::{errors::Result, event};
    use halfbrown::hashmap;
    use simd_json::json;
    use tremor_script::LineValue;
    use tremor_script::Value;
    use tremor_script::ValueAndMeta;

    #[async_std::test]
    async fn control_connect_bad_protocol() -> Result<()> {
        let onrampq = async_channel::bounded(1);
        let offrampq = async_channel::bounded(1);
        let pipelineq = async_channel::bounded(1);
        let mut control = ControlProtocol::new(onrampq.0, offrampq.0, pipelineq.0);

        let actual = control.on_event(&event!({
            "tremor": {
            "connect": {
                "protocol_malformed": "control"
            }
        }}))?;

        assert_eq!(
            actual,
            NetworkCont::Close(event!({
                "tremor": {
                    "close": "protocol not specified error"
                }
            }))
        );

        let actual = control.on_event(&event!({
            "tremor": {
            "connect": {
                "protocol": "flork"
            }
        }}))?;

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
        let onrampq = async_channel::bounded(1);
        let offrampq = async_channel::bounded(1);
        let pipelineq = async_channel::bounded(1);
        let mut control = ControlProtocol::new(onrampq.0, offrampq.0, pipelineq.0);

        let actual = control.on_event(&event!({
            "tremor": {
            "connect": {
                "protocol_malformed": "control",
                "alias": "snot"
            }
        }}))?;

        assert_eq!(
            actual,
            NetworkCont::Close(event!({
                "tremor": {
                    "close": "protocol not specified error",
                }
            }))
        );

        let actual = control.on_event(&event!({
            "tremor": {
            "connect": {
                "protocol": "flork",
                "alias": "snot"
            }
        }}))?;

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
        let onrampq = async_channel::bounded(1);
        let offrampq = async_channel::bounded(1);
        let pipelineq = async_channel::bounded(1);
        let mut control = ControlProtocol::new(onrampq.0, offrampq.0, pipelineq.0);

        let actual = control.on_event(&event!({
            "tremor": {
            "snot": { }
        }}))?;

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
        let onrampq = async_channel::bounded(1);
        let offrampq = async_channel::bounded(1);
        let pipelineq = async_channel::bounded(1);
        let control = ControlProtocol::new(onrampq.0, offrampq.0, pipelineq.0);
        let mut session = NetworkSession::new(stream_id, control)?;
        let (sender, _receiver) = async_channel::bounded(1);
        assert_eq!(ControlState::Connecting, session.fsm.state);

        session.on_event(
            &sender,
            &event!({
                "tremor": {
                "connect": {
                    "protocol": "control"
                }
            }}),
        )?;
        assert_eq!(ControlState::Connecting, session.fsm.state);
        Ok(())
    }

    #[async_std::test]
    async fn network_session_control_connect_disconnect_lifecycle() -> Result<()> {
        let stream_id = 0 as StreamId;
        let onrampq = async_channel::bounded(1);
        let offrampq = async_channel::bounded(1);
        let pipelineq = async_channel::bounded(1);
        let control = ControlProtocol::new(onrampq.0, offrampq.0, pipelineq.0);
        let mut session = NetworkSession::new(stream_id, control)?;

        let (sender, _receiver) = async_channel::bounded(1);

        // Initial state
        assert_eq!(1, session.fsm.control.active_protocols.len());
        assert_eq!(1, session.fsm.control.mux.len());
        assert_eq!(ControlState::Connecting, session.fsm.state);

        session.on_event(
            &sender,
            &event!({
                "tremor": {
                "connect": {
                    "protocol": "echo"
                }
            }}),
        )?;
        assert_eq!(2, session.fsm.control.active_protocols.len());
        assert_eq!(2, session.fsm.control.mux.len());
        assert_eq!(ControlState::Active, session.fsm.state);
        session.on_event(
            &sender,
            &event!({
                "tremor": {
                "disconnect": {
                    "protocol": "echo"
                }
            }}),
        )?;
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
        let onrampq = async_channel::bounded(1);
        let offrampq = async_channel::bounded(1);
        let pipelineq = async_channel::bounded(1);
        let control = ControlProtocol::new(onrampq.0, offrampq.0, pipelineq.0);
        let mut session = NetworkSession::new(stream_id, control)?;

        let (sender, _receiver) = async_channel::bounded(1);

        // Initial state
        assert_eq!(1, session.fsm.control.active_protocols.len());
        assert_eq!(1, session.fsm.control.mux.len());
        assert_eq!(ControlState::Connecting, session.fsm.state);

        session.on_event(
            &sender,
            &event!({
                "tremor": {
                "connect": {
                    "protocol": "echo"
                }
            }}),
        )?;
        assert_eq!(2, session.fsm.control.active_protocols.len());
        assert_eq!(2, session.fsm.control.mux.len());
        assert_eq!(ControlState::Active, session.fsm.state);

        session.on_event(
            &sender,
            &event!({
                "tremor": {
                "connect": {
                    "protocol": "echo",
                    "alias": "snot",
                }
            }}),
        )?;
        assert_eq!(3, session.fsm.control.active_protocols.len());
        assert_eq!(3, session.fsm.control.mux.len());
        assert_eq!(ControlState::Active, session.fsm.state);

        let actual = session.on_event(&sender, &event!({"echo": "badger"}))?;
        assert_eq!(NetworkCont::SourceReply(event!({"echo": "badger"})), actual);

        let actual = session.on_event(&sender, &event!({"snot": "badger"}))?;
        assert_eq!(NetworkCont::SourceReply(event!({"snot": "badger"})), actual);

        let actual = session.on_event(&sender, &event!({"badger": "badger"}))?;
        assert_eq!(
            NetworkCont::Close(
                event!({"tremor": {"close": "protocol session not found for badger"}})
            ),
            actual,
        );

        let actual = session.on_event(&sender, &event!({"fleek": 1, "flook": 2}))?;
        assert_eq!(
            NetworkCont::Close(event!({"tremor": {"close": "expected 1 record field"}})),
            actual,
        );

        let actual = session.on_event(&sender, &event!({}))?;
        assert_eq!(
            NetworkCont::Close(event!({"tremor": {"close": "expected 1 record field"}})),
            actual,
        );

        let actual = session.on_event(&sender, &event!("snot"))?;
        assert_eq!(
            NetworkCont::Close(event!({"tremor": {"close": "record expected"}})),
            actual,
        );

        let actual = session.on_event(
            &sender,
            &event!({
                "tremor": {
                "connect": {
                    "protocol": "control",
                }
            }}),
        )?;
        assert_eq!(
            NetworkCont::Close(event!({"tremor": {"close": "already connected"}})),
            actual,
        );

        let actual = session.on_event(
            &sender,
            &event!({
                "tremor": {
                "connect": {
                    "protocol": "control",
                    "alias": "control",
                }
            }}),
        )?;
        assert_eq!(
            NetworkCont::Close(event!({"tremor": {"close": "already connected"}})),
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
