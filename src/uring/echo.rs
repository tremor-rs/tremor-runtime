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

use super::{
    prelude::{NetworkProtocol, StreamId},
    NetworkCont,
};
use crate::errors::Result;
use crate::uring::control::ControlProtocol;
use tremor_pipeline::Event;
use tremor_value::Value;

#[derive(Clone)]
pub(crate) struct EchoProtocol {}

impl EchoProtocol {
    pub(crate) fn new(_unused: &mut ControlProtocol, _headers: Value) -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl NetworkProtocol for EchoProtocol {
    fn on_init(&mut self) -> Result<()> {
        trace!("Initializing Echo network protocol");
        Ok(())
    }
    async fn on_event(&mut self, _sid: StreamId, event: &Event) -> Result<NetworkCont> {
        trace!("Received echo network protocol event");
        Ok(NetworkCont::SourceReply(event.clone()))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{errors::Result, event, system, system::Conductor};
    use async_channel::bounded;
    use halfbrown::hashmap;
    use simd_json::json;
    use tremor_script::Value;

    #[async_std::test]
    async fn network_session_cannot_override_control() -> Result<()> {
        use crate::temp_network::ws::UrMsg;
        let (uring_tx, _) = bounded::<UrMsg>(1);
        let (tx, _rx) = bounded::<system::ManagerMsg>(1);
        let conductor = Conductor::new(tx, uring_tx);
        let mut control = ControlProtocol::new(&conductor);
        let mut echo = EchoProtocol::new(&mut control, Value::Object(Box::new(hashmap! {})));

        assert_eq!(Ok(()), echo.on_init());

        let actual = echo
            .on_event(
                0,
                &event!({
                    "snot": "badger"
                }),
            )
            .await?;
        assert_eq!(
            NetworkCont::SourceReply(event!({
                "snot": "badger",
            })),
            actual
        );

        let actual = echo.on_event(0, &event!([1, 2, 3, 4])).await?;
        assert_eq!(NetworkCont::SourceReply(event!([1, 2, 3, 4])), actual);

        let actual = echo.on_event(0, &event!("snot")).await?;
        assert_eq!(NetworkCont::SourceReply(event!("snot")), actual);

        Ok(())
    }
}
