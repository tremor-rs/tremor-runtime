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
    prelude::{destructurize, NetworkProtocol, StreamId},
    NetworkCont,
};
use crate::errors::Result;
use crate::network::control::ControlProtocol;
use crate::raft::node::{RaftNetworkMsg, RaftReply, RaftSender};
use futures::StreamExt;
use tremor_pipeline::Event;
use tremor_value::Value;
// TODO eliminate this import
use crate::temp_network::ws::{RequestId, UrMsg};
use simd_json::{Builder, Mutable};

#[derive(Clone)]
pub(crate) struct UringProtocol {
    // TODO add other
    alias: String,
    uring: RaftSender,
    temp_uring: async_channel::Sender<UrMsg>,
}

#[derive(Debug, PartialEq)]
enum UringOperationKind {
    // TODO add other
    Status,
}

#[derive(Debug)]
struct UringOperation {
    // TODO add other
    op: UringOperationKind,
}

impl UringProtocol {
    pub(crate) fn new(ns: &mut ControlProtocol, alias: String, _headers: Value) -> Self {
        Self {
            alias,
            uring: ns.conductor.uring.clone(),
            temp_uring: ns.conductor.temp_uring.clone(),
        }
    }

    async fn on_status(&mut self, _request: &UringOperation) -> Result<NetworkCont> {
        // FIXME dummy implementation for now
        // TODO send a raftnetworkmsg from here for status, with reply sender that will be used to
        // send the response back
        dbg!(&self.uring);

        let (tx, mut rx) = async_channel::bounded(64);
        self.uring
            .try_send(RaftNetworkMsg::Status(RequestId(42), tx))
            .unwrap();

        match rx.next().await {
            Some(RaftReply(.., data)) => {
                let value = destructurize(&data).unwrap().into_static();
                //dbg!(&value);

                let mut status = Value::object_with_capacity(1);
                status.insert("status".to_string(), value)?;
                let mut res = Value::object_with_capacity(1);
                res.insert(self.alias.to_string(), status)?;
                let event = Event {
                    data: res.into(),
                    ..Event::default()
                };
                //dbg!(&event);

                return Ok(NetworkCont::SourceReply(event));
                //return Ok(NetworkCont::SourceReply(
                //    event!({ self.alias.to_string(): { "snot": "badger" } }),
                //));
            }
            // FIXME
            None => unimplemented!(),
            //_ => unreachable!(),
        }
    }
}

fn parse_operation<'a>(alias: &str, request: &'a mut Value<'a>) -> Result<UringOperation> {
    if let Value::Object(c) = request {
        if let Some(cmd) = c.get(alias) {
            if let Value::Object(o) = cmd {
                // Required: `op`
                let op = if let Some(Value::String(s)) = o.get("op") {
                    let s: &str = &s.to_string();
                    match s {
                        "status" => UringOperationKind::Status,
                        // TODO this does not seem to bubble up
                        unsupported_operation => {
                            let reason: &str = &format!(
                                "Invalid Uring operation - unsupported operation type `{}` - must be one of status",
                                unsupported_operation
                            );
                            return Err(reason.into());
                        }
                    }
                } else {
                    return Err(
                        "Required Uring command record field `op` (string) not found error".into(),
                    );
                };

                Ok(UringOperation { op })
            } else {
                return Err("Expect Uring command value to be a record".into());
            }
        } else {
            return Err("Expected protocol alias outer record not found".into());
        }
    } else {
        return Err("Expected Uring request to be a record, it was not".into());
    }
}

#[async_trait::async_trait]
impl NetworkProtocol for UringProtocol {
    fn on_init(&mut self) -> Result<()> {
        trace!("Initializing Uring network protocol");
        Ok(())
    }
    async fn on_event(&mut self, _sid: StreamId, event: &Event) -> Result<NetworkCont> {
        trace!("Received Uring network protocol event");
        let cmd = event.data.parts().0;
        let alias: &str = self.alias.as_str();
        let request = parse_operation(alias, cmd)?;
        Ok(match request.op {
            UringOperationKind::Status => self.on_status(&request).await?,
        })
    }
}
