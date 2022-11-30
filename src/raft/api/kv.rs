// Copyright 2022, The Tremor Team
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

use crate::raft::api::{wrapp, APIError, APIRequest, APIResult};
use crate::raft::app;
use crate::raft::store::{TremorResponse, TremorSet};
use std::sync::Arc;
use tide::Route;

use super::ToAPIResult;

pub(crate) fn install_rest_endpoints(parent: &mut Route<Arc<app::Tremor>>) {
    let mut kv_route = parent.at("/kv");
    kv_route.at("/write").post(wrapp(write));
    kv_route.at("/read").post(wrapp(read));
    kv_route.at("/consistent_read").post(wrapp(consistent_read));
}

async fn write(mut req: APIRequest) -> APIResult<String> {
    let body: TremorSet = req.body_json().await?;
    let tremor_res = req
        .state()
        .raft
        .client_write(body.into())
        .await
        .to_api_result(&req)
        .await?;
    debug_assert!(
        tremor_res.value.is_some(),
        "state machine didn't return the stored value upon write"
    );
    if let Some(value) = tremor_res.value {
        Ok(value)
    } else {
        Err(APIError::Store(
            "State machine didn't return the stored value upon write".to_string(),
        ))
    }
}

/// read a value from the current node, not necessarily the leader, thus this value can be stale
async fn read(mut req: APIRequest) -> APIResult<TremorResponse> {
    let key: String = req.body_json().await?;
    let state_machine = req.state().store.state_machine.read().await;
    let value = state_machine.get(&key)?;
    Ok(TremorResponse { value })
}

/// read a value from the leader. If this request is received by another node, it will return a redirect
async fn consistent_read(mut req: APIRequest) -> APIResult<TremorResponse> {
    let key: String = req.body_json().await?;
    let state = req.state();
    // this will fail if we are not a leader
    state.raft.client_read().await.to_api_result(&req).await?;
    // here we are safe to read
    let value = state.store.state_machine.read().await.get(key.as_str())?;
    Ok(TremorResponse { value })
}
