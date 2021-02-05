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

use crate::api::prelude::*;
use async_channel::bounded;
use futures::StreamExt;
use tremor_runtime::raft_node::{RaftNetworkMsg, RequestId, WsMessage};

pub async fn get(req: Request) -> Result<Response> {
    let raft = &req.state().world.raft;

    let (tx, mut rx) = bounded(1);
    raft.send(RaftNetworkMsg::Status(RequestId(42), tx))
        .await
        .unwrap();

    match rx.next().await {
        Some(WsMessage::Reply { code, data, .. }) => {
            // TODO use this
            dbg!(&code);
            reply(req, data, false, StatusCode::Ok).await
        }
        // FIXME
        Some(_) => unimplemented!(),
        None => unimplemented!(),
        //_ => unreachable!(),
    }
}
