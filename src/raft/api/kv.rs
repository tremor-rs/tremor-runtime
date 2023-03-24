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

use crate::{
    channel::oneshot,
    raft::{
        api::{APIError, APIRequest, APIResult, ToAPIResult},
        store::{TremorResponse, TremorSet},
    },
};
use axum::{extract, routing::post, Router};
use tokio::time::timeout;

use super::API_WORKER_TIMEOUT;

pub(crate) fn endpoints() -> Router<APIRequest> {
    Router::<APIRequest>::new()
        .route("/write", post(write))
        .route("/read", post(read))
        .route("/consistent_read", post(consistent_read))
}

async fn write(
    extract::State(state): extract::State<APIRequest>,
    extract::OriginalUri(uri): extract::OriginalUri,
    extract::Json(body): extract::Json<TremorSet>,
) -> APIResult<String> {
    let tremor_res = state
        .raft
        .client_write(body.into())
        .await
        .to_api_result(&uri, &state)
        .await?;
    if let Some(value) = tremor_res.value {
        Ok(value)
    } else {
        Err(APIError::Store(
            "State machine didn't return the stored value upon write".to_string(),
        ))
    }
}

/// read a value from the current node, not necessarily the leader, thus this value can be stale
async fn read(
    extract::State(state): extract::State<APIRequest>,
    extract::Json(key): extract::Json<String>,
) -> APIResult<TremorResponse> {
    let (tx, rx) = oneshot();
    state
        .store_tx
        .send(super::APIStoreReq::KVGet(key, tx))
        .await?;
    let value = timeout(API_WORKER_TIMEOUT, rx).await??;
    Ok(TremorResponse { value })
}

/// read a value from the leader. If this request is received by another node, it will return a redirect
async fn consistent_read(
    extract::State(state): extract::State<APIRequest>,
    extract::OriginalUri(uri): extract::OriginalUri,
    extract::Json(key): extract::Json<String>,
) -> APIResult<TremorResponse> {
    // this will fail if we are not a leader
    state
        .raft
        .client_read()
        .await
        .to_api_result(&uri, &state)
        .await?;
    // here we are safe to read
    let (tx, rx) = oneshot();
    state
        .store_tx
        .send(super::APIStoreReq::KVGet(key, tx))
        .await?;
    let value = timeout(API_WORKER_TIMEOUT, rx).await??;
    // Ensure that we are still the leader at the end of the read so we can guarantee freshness
    state
        .raft
        .client_read()
        .await
        .to_api_result(&uri, &state)
        .await?;
    Ok(TremorResponse { value })
}
