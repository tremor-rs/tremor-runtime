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

use http_types::{headers, StatusCode};
use serde::Serialize;
use std::sync::{MutexGuard, PoisonError};
use tide::Response;
use tremor_runtime::errors::{Error as TremorError, Kind as ErrorKind};

/// Tremor API error
#[derive(Debug, Serialize)]
pub struct Error {
    pub code: StatusCode,
    pub error: String,
}

impl Error {
    /// Create new error from the given status code and error string
    #[must_use]
    pub fn new(code: StatusCode, error: String) -> Self {
        Self { code, error }
    }

    /// Convenience function for creating artefact not found errors
    pub fn artefact_not_found() -> Self {
        Self::new(StatusCode::NotFound, "Artefact not found".into())
    }

    /// Convenience function for creating instance not found errors
    pub fn instance_not_found() -> Self {
        Self::new(StatusCode::NotFound, "Instance not found".into())
    }
    /// Convenience function for denoting bad requests
    pub fn bad_request(msg: String) -> Self {
        Self::new(StatusCode::BadRequest, msg)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}
impl std::error::Error for Error {}

impl From<Error> for Response {
    fn from(err: Error) -> Response {
        Response::builder(err.code)
            .header(headers::CONTENT_TYPE, crate::api::ResourceType::Json)
            .body(format!(
                r#"{{"code":{},"error":"{}"}}"#,
                err.code, err.error
            ))
            .build()
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::new(StatusCode::InternalServerError, format!("IO error: {e}"))
    }
}

impl From<tremor_common::Error> for Error {
    fn from(e: tremor_common::Error) -> Self {
        Self::new(StatusCode::InternalServerError, format!("{e}"))
    }
}

impl From<tokio::time::error::Elapsed> for Error {
    fn from(_e: tokio::time::error::Elapsed) -> Self {
        Self::new(StatusCode::InternalServerError, "Request timed out".into())
    }
}

impl From<simd_json::Error> for Error {
    fn from(e: simd_json::Error) -> Self {
        Self::new(StatusCode::BadRequest, format!("JSON error: {e}"))
    }
}

impl From<serde_yaml::Error> for Error {
    fn from(e: serde_yaml::Error) -> Self {
        Self::new(StatusCode::BadRequest, format!("YAML error: {e}"))
    }
}

impl From<http_types::Error> for Error {
    fn from(e: http_types::Error) -> Self {
        Self::new(
            StatusCode::InternalServerError,
            format!("HTTP type error: {e}"),
        )
    }
}

impl From<tremor_pipeline::errors::Error> for Error {
    fn from(e: tremor_pipeline::errors::Error) -> Self {
        Self::new(StatusCode::BadRequest, format!("Pipeline error: {e}"))
    }
}

impl From<PoisonError<MutexGuard<'_, tremor_script::Registry>>> for Error {
    fn from(e: PoisonError<MutexGuard<tremor_script::Registry>>) -> Self {
        Self::new(
            StatusCode::InternalServerError,
            format!("Locking error: {e}"),
        )
    }
}

impl From<tremor_system::connector::Error> for Error {
    fn from(e: tremor_system::connector::Error) -> Self {
        Self::new(
            StatusCode::InternalServerError,
            format!("Connector error: {e}"),
        )
    }
}

impl From<TremorError> for Error {
    fn from(e: TremorError) -> Self {
        match e.0 {
            ErrorKind::FlowNotFound(id) => {
                Error::new(StatusCode::NotFound, format!("Flow {id} not found"))
            }
            ErrorKind::ConnectorNotFound(flow_id, id) => Error::new(
                StatusCode::NotFound,
                format!("Connector {id} not found in Flow {flow_id}"),
            ),
            _e => Error::new(
                StatusCode::InternalServerError,
                "Internal server error".into(),
            ),
        }
    }
}
