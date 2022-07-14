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

use crate::raft::api::{APIError, APIRequest, APIResult};
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

/// KV Write request
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct KVSet {
    /// they key
    pub key: String,
    /// the value
    pub value: OwnedValue,
}

async fn write(
    extract::State(state): extract::State<APIRequest>,
    extract::Json(body): extract::Json<KVSet>,
) -> APIResult<Vec<u8>> {
    Ok(state
        .raft_manager
        .kv_set(body.key, simd_json::to_vec(&body.value)?)
        .await?)
}

/// read a value from the current node, not necessarily the leader, thus this value can be stale
async fn read(
    extract::State(state): extract::State<APIRequest>,
    extract::Json(key): extract::Json<String>,
) -> APIResult<Json<OwnedValue>> {
    let value = timeout(API_WORKER_TIMEOUT, state.raft_manager.kv_get_local(key)).await??;
    if let Some(value) = value {
        Ok(Json(value))
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
    extract::Json(key): extract::Json<String>,
) -> APIResult<Json<OwnedValue>> {
    let value = timeout(API_WORKER_TIMEOUT, state.raft_manager.kv_get(key)).await??;
    if let Some(value) = value {
        Ok(Json(value))
    } else {
        Err(APIError::HTTP {
            status: StatusCode::NOT_FOUND,
            message: "Key not found".to_string(),
        })
    }
}
