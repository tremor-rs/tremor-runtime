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

use http_types::{headers, StatusCode};
use serde::Serialize;
use std::sync::{MutexGuard, PoisonError};
use tide::Response;
use tremor_runtime::errors::{Error as TremorError, ErrorKind};

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
            .header(
                headers::CONTENT_TYPE,
                crate::api::ResourceType::Json.as_str(),
            )
            .body(format!(
                r#"{{"code":{},"error":"{}"}}"#,
                err.code, err.error
            ))
            .build()
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::new(StatusCode::InternalServerError, format!("IO error: {}", e))
    }
}

impl From<async_channel::RecvError> for Error {
    fn from(e: async_channel::RecvError) -> Self {
        Self::new(
            StatusCode::InternalServerError,
            format!("Error receiving from channel: {}", e),
        )
    }
}

impl From<async_std::future::TimeoutError> for Error {
    fn from(_e: async_std::future::TimeoutError) -> Self {
        Self::new(StatusCode::InternalServerError, "Request timed out".into())
    }
}

impl From<simd_json::Error> for Error {
    fn from(e: simd_json::Error) -> Self {
        Self::new(StatusCode::BadRequest, format!("JSON error: {}", e))
    }
}

impl From<serde_yaml::Error> for Error {
    fn from(e: serde_yaml::Error) -> Self {
        Self::new(StatusCode::BadRequest, format!("YAML error: {}", e))
    }
}

impl From<http_types::Error> for Error {
    fn from(e: http_types::Error) -> Self {
        Self::new(
            StatusCode::InternalServerError,
            format!("HTTP type error: {}", e),
        )
    }
}

impl From<tremor_pipeline::errors::Error> for Error {
    fn from(e: tremor_pipeline::errors::Error) -> Self {
        Self::new(StatusCode::BadRequest, format!("Pipeline error: {}", e))
    }
}

impl From<tremor_script::prelude::CompilerError> for Error {
    fn from(e: tremor_script::prelude::CompilerError) -> Self {
        Self::new(
            StatusCode::InternalServerError,
            format!("Compiler error: {:?}", e),
        )
    }
}
impl From<PoisonError<MutexGuard<'_, tremor_script::Registry>>> for Error {
    fn from(e: PoisonError<MutexGuard<tremor_script::Registry>>) -> Self {
        Self::new(
            StatusCode::InternalServerError,
            format!("Locking error: {}", e),
        )
    }
}

impl From<TremorError> for Error {
    fn from(e: TremorError) -> Self {
        match e.0 {
            ErrorKind::UnpublishFailedNonZeroInstances(_) => Error::new(
                StatusCode::Conflict,
                "Resource still has active instances".into(),
            ),
            ErrorKind::ArtefactNotFound(url) => {
                Error::new(StatusCode::NotFound, format!("Artefact not found {}", url))
            }
            ErrorKind::InstanceNotFound(url) => {
                Error::new(StatusCode::NotFound, format!("Instance not found {}", url))
            }
            ErrorKind::InvalidTremorUrl(msg, url) => Error::new(
                StatusCode::BadRequest,
                format!("Invalid Tremor Url: {} : {}", url, msg),
            ),
            ErrorKind::InvalidInstanceUrl(url) => Error::new(
                StatusCode::BadRequest,
                format!("Invalid instance Url: {}", url),
            ),
            ErrorKind::PublishFailedAlreadyExists(_) => Error::new(
                StatusCode::Conflict,
                "A resource with the requested ID already exists".into(),
            ),
            ErrorKind::UnpublishFailedSystemArtefact(_) => Error::new(
                StatusCode::Forbidden,
                "System artefacts cannot be unpublished".into(),
            ),
            _e => Error::new(
                StatusCode::InternalServerError,
                "Internal server error".into(),
            ),
        }
    }
}
