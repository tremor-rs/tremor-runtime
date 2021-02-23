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
use tremor_runtime::temp_network::ws::{NodeId, UrMsg};

pub async fn add_node(req: Request) -> Result<Response> {
    let uring = &req.state().world.uring;

    let nid: u64 = req.param("nid").unwrap().parse::<u64>().unwrap();
    let (tx, mut rx) = bounded(64);
    uring.try_send(UrMsg::AddNode(NodeId(nid), tx)).unwrap();

    match rx.next().await {
        Some(success) => reply(req, success, false, StatusCode::Ok).await,
        None => unimplemented!(),
        //_ => unreachable!(),
    }
}
