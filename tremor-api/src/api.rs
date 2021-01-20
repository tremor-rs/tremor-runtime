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

use crate::errors::Error;
use http_types::{headers, StatusCode};
use serde::{Deserialize, Serialize};
use tide::Response;
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

#[derive(Clone, Copy, Debug)]
pub enum ResourceType {
    Json,
    Yaml,
    Trickle,
}
impl ResourceType {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Yaml => "application/yaml",
            Self::Json => "application/json",
            Self::Trickle => "application/vnd.trickle",
        }
    }
}

#[must_use]
pub fn content_type(req: &Request) -> Option<ResourceType> {
    match req
        .header(&headers::CONTENT_TYPE)
        .map(headers::HeaderValues::last)
        .map(headers::HeaderValue::as_str)
    {
        Some("application/yaml") => Some(ResourceType::Yaml),
        Some("application/json") => Some(ResourceType::Json),
        Some("application/vnd.trickle") => Some(ResourceType::Trickle),
        _ => None,
    }
}

#[must_use]
pub fn accept(req: &Request) -> ResourceType {
    // TODO implement correctly / RFC compliance
    match req
        .header(headers::ACCEPT)
        .map(headers::HeaderValues::last)
        .map(headers::HeaderValue::as_str)
    {
        Some("application/yaml") => ResourceType::Yaml,
        Some("application/vnd.trickle") => ResourceType::Trickle,
        _ => ResourceType::Json,
    }
}

pub fn serialize<T: Serialize>(t: ResourceType, d: &T, code: StatusCode) -> Result<Response> {
    match t {
        ResourceType::Yaml => Ok(Response::builder(code)
            .header(headers::CONTENT_TYPE, t.as_str())
            .body(serde_yaml::to_string(d)?)
            .build()),
        ResourceType::Json => Ok(Response::builder(code)
            .header(headers::CONTENT_TYPE, t.as_str())
            .body(simd_json::to_string(d)?)
            .build()),
        ResourceType::Trickle => Err(Error::new(
            StatusCode::InternalServerError,
            "Unsuported formatting as trickle".into(),
        )),
    }
}

pub fn serialize_error(t: ResourceType, d: Error) -> Result<Response> {
    match t {
        ResourceType::Json | ResourceType::Yaml => serialize(t, &d, d.code),
        // formatting errors as trickle does not make sense so for this
        // fall back to the error's conversion into tide response
        ResourceType::Trickle => Ok(d.into()),
    }
}

pub async fn reply<T: Serialize + Send + Sync + 'static>(
    req: Request,
    result_in: T,
    persist: bool,
    ok_code: StatusCode,
) -> Result<Response> {
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
                Error::new(
                    StatusCode::BadRequest,
                    format!("Could not decode YAML: {}", e),
                )
            })
            .map(|data| (req, data)),
        Some(ResourceType::Json) => simd_json::from_slice(body.as_mut_slice())
            .map_err(|e| {
                Error::new(
                    StatusCode::BadRequest,
                    format!("Could not decode JSON: {}", e),
                )
            })
            .map(|data| (req, data)),
        Some(ResourceType::Trickle) | None => Err(Error::new(
            StatusCode::UnsupportedMediaType,
            "No content type provided".into(),
        )),
    }
}

fn build_url(path: &[&str]) -> Result<TremorURL> {
    let url = format!("/{}", path.join("/"));
    TremorURL::parse(&url).map_err(|_e| {
        Error::new(
            StatusCode::InternalServerError,
            format!("Could not decode Tremor URL: {}", url),
        )
    })
}
