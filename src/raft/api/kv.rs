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

use crate::raft::{
    api::{APIError, APIRequest, APIResult, ToAPIResult},
    store::TremorSet,
};
use axum::{extract, routing::post, Json, Router};
use http::StatusCode;
use simd_json::OwnedValue;
use tokio::time::timeout;

use super::API_WORKER_TIMEOUT;

pub(crate) fn endpoints() -> Router<APIRequest> {
    Router::<APIRequest>::new()
        .route("/write", post(write))
        .route("/read", post(read))
        .route("/consistent_read", post(consistent_read))
}

#[derive(Deserialize)]
struct KVSet {
    key: String,
    value: OwnedValue,
}

async fn write(
    extract::State(state): extract::State<APIRequest>,
    extract::OriginalUri(uri): extract::OriginalUri,
    extract::Json(body): extract::Json<KVSet>,
) -> APIResult<Vec<u8>> {
    let tremor_set = TremorSet {
        key: body.key,
        value: simd_json::to_vec(&body.value)?,
    };
    let res = state
        .raft
        .client_write(tremor_set.into())
        .await
        .to_api_result(&uri, &state)
        .await?;

    Ok(res.data.into_kv_value()?)
}

/// read a value from the current node, not necessarily the leader, thus this value can be stale
async fn read(
    extract::State(state): extract::State<APIRequest>,
    extract::Json(key): extract::Json<String>,
) -> APIResult<Json<OwnedValue>> {
    let value = timeout(API_WORKER_TIMEOUT, state.raft_manager.kv_get_local(key)).await??;
    if let Some(mut value) = value {
        Ok(Json(simd_json::from_slice(&mut value)?))
    } else {
        Err(APIError::HTTP {
            status: StatusCode::NOT_FOUND,
            message: "Key not found".to_string(),
        })
    }
}

/// read a value from the leader. If this request is received by another node, it will return a redirect
async fn consistent_read(
    extract::State(state): extract::State<APIRequest>,
    extract::OriginalUri(uri): extract::OriginalUri,
    extract::Json(key): extract::Json<String>,
) -> APIResult<Json<OwnedValue>> {
    // this will fail if we are not a leader
    state.ensure_leader(Some(uri.clone())).await?;
    // here we are safe to read
    let value = timeout(API_WORKER_TIMEOUT, state.raft_manager.kv_get_local(key)).await??;
    // Ensure that we are still the leader at the end of the read so we can guarantee freshness
    state.ensure_leader(Some(uri)).await?;
    if let Some(mut value) = value {
        Ok(Json(simd_json::from_slice(&mut value)?))
    } else {
        Err(APIError::HTTP {
            status: StatusCode::NOT_FOUND,
            message: "Key not found".to_string(),
        })
    }
}
