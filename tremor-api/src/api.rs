// Copyright 2020, The Tremor Team
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

pub use http_types::headers;
pub use http_types::StatusCode;
use serde::{Deserialize, Serialize};
pub use tide::Response;
use tremor_runtime::errors::{Error as TremorError, ErrorKind};
use tremor_runtime::system::World;
use tremor_runtime::url::TremorURL;

pub mod binding;
pub mod offramp;
pub mod onramp;
pub mod pipeline;
pub mod prelude;
pub mod version;

pub type Request = tide::Request<State>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
pub struct State {
    pub world: World,
}

#[derive(Debug)]
pub enum Error {
    Generic(StatusCode, String),
    JSON(StatusCode, String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Generic(_c, d) | Error::JSON(_c, d) => write!(f, "{}", d),
        }
    }
}
impl std::error::Error for Error {}

impl Error {
    pub fn into_http_error(self) -> http_types::Error {
        match &self {
            Error::Generic(c, _d) | Error::JSON(c, _d) => http_types::Error::new(*c, self),
        }
    }

    fn not_found() -> Self {
        Self::json(StatusCode::NotFound, &r#"{"error": "Artefact not found"}"#)
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
            Error::Generic(c, d) => {
                let mut r = Response::new(c);
                r.set_body(d);
                r
            }
            Error::JSON(c, d) => {
                let mut r = Response::new(c);
                r.insert_header(headers::CONTENT_TYPE, ResourceType::Json.to_string());
                r.set_body(d);
                r
            }
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::generic(StatusCode::InternalServerError, &format!("IO Error: {}", e))
    }
}

impl From<simd_json::Error> for Error {
    fn from(e: simd_json::Error) -> Self {
        Self::generic(
            StatusCode::BadRequest,
            &format!("json encoder failed: {}", e),
        )
    }
}

impl From<serde_yaml::Error> for Error {
    fn from(e: serde_yaml::Error) -> Self {
        Self::generic(
            StatusCode::BadRequest,
            &format!("yaml encoder failed: {}", e),
        )
    }
}

impl From<tremor_pipeline::errors::Error> for Error {
    fn from(e: tremor_pipeline::errors::Error) -> Self {
        Self::generic(StatusCode::BadRequest, &format!("Pipeline error: {}", e))
    }
}
impl From<http_types::Error> for Error {
    fn from(e: http_types::Error) -> Self {
        Self::generic(
            StatusCode::InternalServerError,
            &format!("http type error: {}", e),
        )
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
    match req
        .header(&headers::CONTENT_TYPE)
        .map(headers::HeaderValues::last)
        .map(headers::HeaderValue::as_str)
    {
        Some("application/yaml") => Some(ResourceType::Yaml),
        Some("application/json") => Some(ResourceType::Json),
        _ => None,
    }
}

pub fn accept(req: &Request) -> ResourceType {
    // TODO implement correctly / RFC compliance
    match req
        .header(headers::ACCEPT)
        .map(headers::HeaderValues::last)
        .map(headers::HeaderValue::as_str)
    {
        Some("application/yaml") => ResourceType::Yaml,
        _ => ResourceType::Json,
    }
}

impl From<TremorError> for Error {
    fn from(e: TremorError) -> Self {
        match e.0 {
            ErrorKind::UnpublishFailedNonZeroInstances(_) => Error::JSON(
                StatusCode::Conflict,
                r#"{"error": "Resource still has active instances"}"#.into(),
            ),
            ErrorKind::ArtifactNotFound(_) => Error::JSON(
                StatusCode::NotFound,
                r#"{"error": "Artefact not found"}"#.into(),
            ),
            ErrorKind::PublishFailedAlreadyExists(_) => Error::JSON(
                StatusCode::Conflict,
                r#"{"error": "An resouce with the requested ID already exists"}"#.into(),
            ),
            ErrorKind::UnpublishFailedSystemArtefact(_) => Error::JSON(
                StatusCode::Forbidden,
                r#"{"error": "System Artefacts can not be unpublished"}"#.into(),
            ),
            _e => Error::JSON(
                StatusCode::InternalServerError,
                r#"{"error": "Internal server error"}"#.into(),
            ),
        }
    }
}

pub fn serialize<T: Serialize>(
    t: ResourceType,
    d: &T,
    ok_code: StatusCode,
) -> std::result::Result<Response, crate::Error> {
    match t {
        ResourceType::Yaml => {
            let mut r = Response::new(ok_code);
            r.insert_header(headers::CONTENT_TYPE, t.to_string());
            r.set_body(serde_yaml::to_string(d)?);
            Ok(r)
        }

        ResourceType::Json => {
            let mut r = Response::new(ok_code);
            r.insert_header(headers::CONTENT_TYPE, t.to_string());
            r.set_body(simd_json::to_string(d)?);
            Ok(r)
        }
    }
}

pub async fn reply<T: Serialize + Send + Sync + 'static>(
    req: Request,
    result_in: T,
    persist: bool,
    ok_code: StatusCode,
) -> std::result::Result<Response, crate::Error> {
    if persist {
        let world = &req.state().world;
        world.save_config().await?;
    }
    serialize(accept(&req), &result_in, ok_code)
}

async fn decode<T>(mut req: Request) -> Result<(Request, T)>
where
    for<'de> T: Deserialize<'de>,
{
    let mut body = req.body_bytes().await?;
    match content_type(&req) {
        Some(ResourceType::Yaml) => serde_yaml::from_slice(body.as_slice())
            .map_err(|e| {
                Error::Generic(
                    StatusCode::BadRequest,
                    format!("Could not decode YAML: {}", e),
                )
            })
            .map(|data| (req, data)),
        Some(ResourceType::Json) => simd_json::from_slice(body.as_mut_slice())
            .map_err(|e| {
                Error::Generic(
                    StatusCode::BadRequest,
                    format!("Could not decode JSON: {}", e),
                )
            })
            .map(|data| (req, data)),
        None => Err(Error::Generic(
            StatusCode::UnsupportedMediaType,
            "No content type provided".into(),
        )),
    }
}

pub fn build_url(path: &[&str]) -> Result<TremorURL> {
    let url = format!("/{}", path.join("/"));
    TremorURL::parse(&url).map_err(|_e| {
        Error::Generic(
            StatusCode::InternalServerError,
            format!("Could not decode Tremor URL: {}", url),
        )
    })
}
