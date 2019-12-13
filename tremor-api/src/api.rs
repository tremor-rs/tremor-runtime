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

use actix_web::http::StatusCode;
use actix_web::{error, web::Data, HttpMessage, HttpRequest, HttpResponse};
use log::error;
use serde::{Deserialize, Serialize};
use tremor_runtime::errors::{Error as TremorError, ErrorKind, Result as TremmorResult};
use tremor_runtime::system::World;
use tremor_runtime::url::TremorURL;

pub mod binding;
pub mod offramp;
pub mod onramp;
pub mod pipeline;
pub mod version;

pub type HTTPResult = Result<HttpResponse, error::Error>;

#[derive(Clone)]
pub struct State {
    pub world: World,
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

pub fn content_type(req: &HttpRequest) -> Option<ResourceType> {
    match req.content_type() {
        "application/yaml" => Some(ResourceType::Yaml),
        "application/json" => Some(ResourceType::Json),
        _ => None,
    }
}

pub fn accept(req: &HttpRequest) -> ResourceType {
    // TODO implement correctly / RFC compliance
    let accept: Option<&str> = match req.headers().get("Accept") {
        Some(x) => x.to_str().ok(),
        None => Some(req.content_type()),
    };
    match accept {
        Some("application/yaml") => ResourceType::Yaml,
        Some("application/json") | _ => ResourceType::Json,
    }
}

pub fn c(c: u16) -> StatusCode {
    StatusCode::from_u16(c).unwrap_or_default()
}

pub fn handle_errors(e: TremorError) -> error::Error {
    match e.0 {
        ErrorKind::UnpublishFailedNonZeroInstances(_) => {
            error::ErrorConflict(r#"{"error": "Resource still has active instances"}"#)
        }
        ErrorKind::ArtifactNotFound(_) => {
            error::ErrorNotFound(r#"{"error": "Artefact not found"}"#)
        }
        ErrorKind::PublishFailedAlreadyExists(_) => {
            error::ErrorConflict(r#"{"error": "An resouce with the requested ID already exists"}"#)
        }

        ErrorKind::UnpublishFailedSystemArtefact(_) => {
            error::ErrorForbidden(r#"{"error": "System Artefacts can not be unpublished"}"#)
        }

        e => {
            error!("Unhandled error: {}", e);
            error::ErrorInternalServerError(r#"{"error": "Internal server error"}"#)
        }
    }
}
pub fn serialize<T: Serialize>(t: ResourceType, d: &T, ok_code: u16) -> HTTPResult {
    let body = match t {
        ResourceType::Yaml => serde_yaml::to_string(d)
            .map_err(|e| error::ErrorInternalServerError(format!("yaml encoder failed: {}", e)))?,

        ResourceType::Json => serde_json::to_string(d)
            .map_err(|e| error::ErrorInternalServerError(format!("json encoder failed: {}", e)))?,
    };
    Ok(HttpResponse::build(c(ok_code))
        .content_type(t.to_string())
        .body(body))
}

pub fn reply<T: Serialize>(
    req: &HttpRequest,
    data: &Data<State>,
    result_in: TremmorResult<T>,
    persist: bool,
    ok_code: u16,
) -> HTTPResult {
    let r = result_in.map_err(handle_errors)?;
    if persist && data.world.save_config().is_err() {
        return Err(error::ErrorInternalServerError("failed to save state"));
    };
    serialize(accept(&req), &r, ok_code)
}

fn decode<T>(req: &HttpRequest, data_raw: &str) -> Result<T, error::Error>
where
    for<'de> T: Deserialize<'de>,
{
    match content_type(&req) {
        Some(ResourceType::Yaml) => serde_yaml::from_str(data_raw)
            .map_err(|e| error::ErrorBadRequest(format!("Could not decode YAML: {}", e))),
        Some(ResourceType::Json) => serde_json::from_str(data_raw)
            .map_err(|e| error::ErrorBadRequest(format!("Could not decode JSON: {}", e))),
        None => Err(error::ErrorBadRequest("No content type provided")),
    }
}

pub fn build_url(path: &[&str]) -> Result<TremorURL, error::Error> {
    let url = format!("/{}", path.join("/"));
    TremorURL::parse(&url)
        .map_err(|_e| error::ErrorBadRequest(format!("Could not decode Tremor URL: {}", url)))
}
