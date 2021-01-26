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

use super::{NetworkCont, NetworkProtocol};
use crate::errors::Result;
use crate::network::control::ControlProtocol;
use crate::url::TremorURL;
use crate::{offramp, onramp, pipeline};
use async_channel::Sender;
use halfbrown::hashmap;
use simd_json::json;
use tremor_pipeline::Event;
use tremor_script::LineValue;
use tremor_script::Value;
use tremor_script::ValueAndMeta;

#[derive(Clone)]
pub(crate) struct PubSubProtocol {
    _onramp: Sender<onramp::ManagerMsg>,
    _offramp: Sender<offramp::ManagerMsg>,
    _pipeline: Sender<pipeline::ManagerMsg>,
}

impl PubSubProtocol {
    pub(crate) fn new(ns: &ControlProtocol, _headers: Value) -> Self {
        Self {
            _onramp: ns.onramp.clone(),
            _offramp: ns.offramp.clone(),
            _pipeline: ns.pipeline.clone(),
        }
    }
}

unsafe impl Send for PubSubProtocol {}
unsafe impl Sync for PubSubProtocol {}

// FIXME implement `pubsub`

impl PubSubProtocol {
    fn subscribe(&mut self, _url: TremorURL) -> Result<NetworkCont> {
        dbg!("GOT HERE IN PUBSUB SUBSCRIBE");
        Ok(NetworkCont::Close(
            event!({ "control": "close", "reason": "not-implemented" }),
        ))
    }
}

impl NetworkProtocol for PubSubProtocol {
    fn on_init(&mut self) -> Result<()> {
        trace!("Initializing pubsub for network protocol");
        Ok(())
    }

    fn on_event(&mut self, _event: &Event) -> Result<NetworkCont> {
        trace!("Received pubsub network protocol event");
        self.subscribe(TremorURL::parse("/network").unwrap())
    }
}
