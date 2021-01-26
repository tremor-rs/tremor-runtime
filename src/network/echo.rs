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

use super::{prelude::NetworkProtocol, NetworkCont};
use crate::errors::Result;
use crate::network::control::ControlProtocol;
use crate::offramp;
use crate::onramp;
use crate::pipeline;
use async_channel::Sender;
use tremor_pipeline::Event;
use tremor_value::Value;

#[derive(Clone)]
pub(crate) struct EchoProtocol {
    _onramp: Sender<onramp::ManagerMsg>,
    _offramp: Sender<offramp::ManagerMsg>,
    _pipeline: Sender<pipeline::ManagerMsg>,
}

impl EchoProtocol {
    pub(crate) fn new(ns: &mut ControlProtocol, _headers: Value) -> Self {
        Self {
            _onramp: ns.onramp.clone(),
            _offramp: ns.offramp.clone(),
            _pipeline: ns.pipeline.clone(),
        }
    }
}

unsafe impl Send for EchoProtocol {}
unsafe impl Sync for EchoProtocol {}

impl NetworkProtocol for EchoProtocol {
    fn on_init(&mut self) -> Result<()> {
        trace!("Initializing Echo network protocol");
        Ok(())
    }
    fn on_event(&mut self, event: &Event) -> Result<NetworkCont> {
        trace!("Received echo network protocol event");
        Ok(NetworkCont::SourceReply(event.clone()))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{errors::Result, event};
    use halfbrown::hashmap;
    use simd_json::json;
    use tremor_script::LineValue;
    use tremor_script::Value;
    use tremor_script::ValueAndMeta;

    #[async_std::test]
    async fn network_session_cannot_override_control() -> Result<()> {
        let onrampq = async_channel::bounded(1);
        let offrampq = async_channel::bounded(1);
        let pipelineq = async_channel::bounded(1);
        let mut control = ControlProtocol::new(onrampq.0, offrampq.0, pipelineq.0);
        let mut echo = EchoProtocol::new(&mut control, Value::Object(Box::new(hashmap! {})));

        assert_eq!(Ok(()), echo.on_init());

        let actual = echo.on_event(&event!({
            "snot": "badger"
        }))?;
        assert_eq!(
            NetworkCont::SourceReply(event!({
                "snot": "badger",
            })),
            actual
        );

        let actual = echo.on_event(&event!([1, 2, 3, 4]))?;
        assert_eq!(NetworkCont::SourceReply(event!([1, 2, 3, 4])), actual);

        let actual = echo.on_event(&event!("snot"))?;
        assert_eq!(NetworkCont::SourceReply(event!("snot")), actual);

        Ok(())
    }
}
