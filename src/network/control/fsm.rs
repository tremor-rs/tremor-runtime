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

use tremor_pipeline::Event;

use super::{ControlProtocol, ControlState};
use crate::network::prelude::{NetworkProtocol, StreamId};
use crate::{errors::Result, network::NetworkCont};
use std::fmt;

pub(crate) struct ControlLifecycleFsm {
    pub state: ControlState,
    pub sid: StreamId,
    pub control: ControlProtocol,
}

impl fmt::Debug for ControlLifecycleFsm {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ControlLifecycleFsm {{ sid: {}, state: {:?} }}",
            self.sid, self.state
        )
    }
}

impl ControlLifecycleFsm {
    pub fn new(sid: StreamId, control: ControlProtocol) -> Result<Self> {
        let instance = Self {
            state: ControlState::Connecting,
            sid,
            control,
        };
        Ok(instance)
    }

    pub fn on_event(&mut self, event: &Event) -> Result<NetworkCont> {
        self.control.on_event(event)
    }
}

impl ControlLifecycleFsm {
    pub(crate) fn transition(&mut self, to: ControlState) -> Result<&mut Self> {
        loop {
            match (&self.state, &to) {
                (ControlState::Connecting, ControlState::Active) => {
                    self.state = ControlState::Active;
                    break;
                }
                (ControlState::Active, ControlState::Active) => {
                    break;
                }
                (ControlState::Active, ControlState::Connecting) => {
                    self.state = ControlState::Connecting;
                    break;
                }
                (ControlState::Active, ControlState::Disconnecting) => {
                    self.state = ControlState::Disconnecting;
                    break;
                }
                (ControlState::Disconnecting, ControlState::Zombie) => {
                    self.state = ControlState::Zombie;
                    continue;
                }
                (ControlState::Zombie, _) => break,
                _ => {
                    self.state = ControlState::Invalid;
                    return Err("Illegel State Transition".into());
                }
            };
        }

        Ok(self)
    }
}
