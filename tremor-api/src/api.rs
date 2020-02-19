// Copyright 2018-2020, Wayfair GmbH
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

use http::status::StatusCode;
use serde::{Deserialize, Serialize};
pub(crate) use tide::Response;
use tremor_runtime::errors::{Error as TremorError, ErrorKind};
use tremor_runtime::system::World;
use tremor_runtime::url::TremorURL;

pub mod binding;
pub mod offramp;
pub mod onramp;
pub mod pipeline;
pub mod version;

pub type Request = tide::Request<State>;
pub type Result<T> = std::result::Result<T, Error>;

impl tide::IntoResponse for Error {
    fn into_response(self) -> Response {
        self.into()
    }
}
pub struct State {
    pub world: World,
}

pub enum Error {
    Generic(StatusCode, String),
    JSON(StatusCode, String),
}

impl Error {
    fn not_found() -> Self {
        Self::json(StatusCode::NOT_FOUND, &r#"{"error": "Artefact not found"}"#)
    }
    fn generic<S: ToString>(c: StatusCode, s: &S) -> Self {
        Error::Generic(c, s.to_string())
    }
    fn json<S: ToString>(c: StatusCode, s: &S) -> Self {
        Error::JSON(c, s.to_string())
    }
}

impl Into<Response> for Error {
    fn into(self) -> Response {
        match self {
            Error::Generic(c, d) => Response::new(c.into()).body_string(d),
            Error::JSON(c, d) => Response::new(c.into())
                .body_string(d)
                .set_header("Content-Type", ResourceType::Json.to_string()),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::generic(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("IO Error: {}", e),
        )
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::generic(
            StatusCode::BAD_REQUEST,
            &format!("json encoder failed: {}", e),
        )
    }
}

impl From<serde_yaml::Error> for Error {
    fn from(e: serde_yaml::Error) -> Self {
        Self::generic(
            StatusCode::BAD_REQUEST,
            &format!("yaml encoder failed: {}", e),
        )
    }
}

impl From<tremor_pipeline::errors::Error> for Error {
    fn from(e: tremor_pipeline::errors::Error) -> Self {
        Self::generic(StatusCode::BAD_REQUEST, &format!("Pipeline error: {}", e))
    }
}

#[derive(Clone, Copy)]
pub enum ResourceType {
    Json,
    Yaml,
}

impl ToString for ResourceType {
    fn to_string(&self) -> String {
        match self {
            Self::Yaml => "application/yaml".to_string(),
            Self::Json => "application/json".to_string(),
        }
    }
}

pub fn content_type(req: &Request) -> Option<ResourceType> {
    match req.header("Content-Type") {
        Some("application/yaml") => Some(ResourceType::Yaml),
        Some("application/json") => Some(ResourceType::Json),
        _ => None,
    }
}

pub fn accept(req: &Request) -> ResourceType {
    // TODO implement correctly / RFC compliance
    match req.header("Accept") {
        Some("application/yaml") => ResourceType::Yaml,
        Some("application/json") | _ => ResourceType::Json,
    }
}

pub fn c(c: u16) -> StatusCode {
    StatusCode::from_u16(c).unwrap_or_default()
}

impl From<TremorError> for Error {
    fn from(e: TremorError) -> Self {
        match e.0 {
            ErrorKind::UnpublishFailedNonZeroInstances(_) => Error::JSON(
                StatusCode::CONFLICT,
                r#"{"error": "Resource still has active instances"}"#.into(),
            ),
            ErrorKind::ArtifactNotFound(_) => Error::JSON(
                StatusCode::NOT_FOUND,
                r#"{"error": "Artefact not found"}"#.into(),
            ),
            ErrorKind::PublishFailedAlreadyExists(_) => Error::JSON(
                StatusCode::CONFLICT,
                r#"{"error": "An resouce with the requested ID already exists"}"#.into(),
            ),
            ErrorKind::UnpublishFailedSystemArtefact(_) => Error::JSON(
                StatusCode::FORBIDDEN,
                r#"{"error": "System Artefacts can not be unpublished"}"#.into(),
            ),
            _e => Error::JSON(
                StatusCode::INTERNAL_SERVER_ERROR,
                r#"{"error": "Internal server error"}"#.into(),
            ),
        }
    }
}

pub fn serialize<T: Serialize>(t: ResourceType, d: &T, ok_code: u16) -> Result<Response> {
    Ok(match t {
        ResourceType::Yaml => Response::new(ok_code)
            .body_string(serde_yaml::to_string(d)?)
            .set_header("Content-Type", t.to_string()),

        ResourceType::Json => Response::new(ok_code)
            .body_string(serde_json::to_string(d)?)
            .set_header("Content-Type", t.to_string()),
    })
}

pub async fn reply<T: Serialize + Send + Sync + 'static>(
    req: Request,
    result_in: T,
    persist: bool,
    ok_code: u16,
) -> Result<Response> {
    if persist {
        let world = &req.state().world;
        world.save_config()?;
    }
    serialize(accept(&req), &result_in, ok_code)
}

async fn decode<T>(mut req: Request) -> Result<(Request, T)>
where
    for<'de> T: Deserialize<'de>,
{
    let body = req.body_bytes().await?;
    match content_type(&req) {
        Some(ResourceType::Yaml) => serde_yaml::from_slice(body.as_slice())
            .map_err(|e| {
                Error::Generic(
                    StatusCode::BAD_REQUEST,
                    format!("Could not decode YAML: {}", e),
                )
            })
            .map(|data| (req, data)),
        Some(ResourceType::Json) => serde_json::from_slice(body.as_slice())
            .map_err(|e| {
                Error::Generic(
                    StatusCode::BAD_REQUEST,
                    format!("Could not decode JSON: {}", e),
                )
            })
            .map(|data| (req, data)),
        None => Err(Error::Generic(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "No content type provided".into(),
        )),
    }
}

pub fn build_url(path: &[&str]) -> Result<TremorURL> {
    let url = format!("/{}", path.join("/"));
    TremorURL::parse(&url).map_err(|_e| {
        Error::Generic(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Could not decode Tremor URL: {}", url),
        )
    })
}
