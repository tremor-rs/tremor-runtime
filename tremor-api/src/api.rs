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

use std::time::Duration;

use crate::errors::Error;
use async_std::{prelude::*, task::JoinHandle};
use http_types::{
    headers::{self, HeaderValue, ToHeaderValues},
    StatusCode,
};
use serde::{Deserialize, Serialize};
use tide::Response;
use tremor_runtime::system::World;

pub mod flow;
pub mod prelude;
pub mod status;
pub mod version;

pub type Request = tide::Request<State>;
pub type Result<T> = std::result::Result<T, Error>;

/// Default API timeout applied to operations triggered by the API. E.g. get flow status
pub const DEFAULT_API_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct State {
    pub world: World,
}

#[derive(Clone, Copy, Debug)]
pub enum ResourceType {
    Json,
    Yaml,
    Trickle,
    Troy,
}
impl ResourceType {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Yaml => "application/yaml",
            Self::Json => "application/json",
            Self::Trickle => "application/vnd.trickle",
            Self::Troy => "application/vnd.troy",
        }
    }
}

impl ToHeaderValues for ResourceType {
    type Iter = std::iter::Once<HeaderValue>;

    fn to_header_values(&self) -> http_types::Result<Self::Iter> {
        HeaderValue::from_bytes(self.as_str().as_bytes().to_vec()).map(std::iter::once)
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
        Some("application/vnd.troy") => Some(ResourceType::Troy),
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
        Some("application/vnd.troy") => ResourceType::Troy,
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
        // FIXME: we should have troy serialization, maybe just use the file from the Arena?
        ResourceType::Troy => Err(Error::new(
            StatusCode::InternalServerError,
            "Unsuported formatting as troy".into(),
        )),
    }
}

pub fn serialize_error(t: ResourceType, d: Error) -> Result<Response> {
    match t {
        ResourceType::Json | ResourceType::Yaml => serialize(t, &d, d.code),
        // formatting errors as trickle does not make sense so for this
        // fall back to the error's conversion into tide response
        ResourceType::Trickle | ResourceType::Troy => Ok(d.into()),
    }
}

pub fn reply<T: Serialize + Send + Sync + 'static>(
    req: &Request,
    result_in: T,
    ok_code: StatusCode,
) -> Result<Response> {
    serialize(accept(req), &result_in, ok_code)
}

// async fn decode<T>(mut req: Request) -> Result<(Request, T)>
// where
//     for<'de> T: Deserialize<'de>,
// {
//     let mut body = req.body_bytes().await?;
//     match content_type(&req) {
//         Some(ResourceType::Yaml) => serde_yaml::from_slice(body.as_slice())
//             .map_err(|e| {
//                 Error::new(
//                     StatusCode::BadRequest,
//                     format!("Could not decode YAML: {}", e),
//                 )
//             })
//             .map(|data| (req, data)),
//         Some(ResourceType::Json) => simd_json::from_slice(body.as_mut_slice())
//             .map_err(|e| {
//                 Error::new(
//                     StatusCode::BadRequest,
//                     format!("Could not decode JSON: {}", e),
//                 )
//             })
//             .map(|data| (req, data)),
//         Some(ResourceType::Trickle) | None => Err(Error::new(
//             StatusCode::UnsupportedMediaType,
//             "No content type provided".into(),
//         )),
//     }
// }

async fn handle_api_request<
    G: std::future::Future<Output = Result<tide::Response>>,
    F: Fn(Request) -> G,
>(
    req: Request,
    handler_func: F,
) -> tide::Result {
    let resource_type = accept(&req);
    let path = req.url().path().to_string();
    let method = req.method();

    // Handle request. If any api error is returned, serialize it into a tide response
    // as well, respecting the requested resource type. (and if there's error during
    // this serialization, fall back to the error's conversion into tide response)
    let r = match handler_func(req).timeout(DEFAULT_API_TIMEOUT).await {
        Err(e) => {
            error!("[API {method} {path}] Timeout");
            Err(e.into())
        },
        Ok(Err(e)) => {
            error!("[API {method} {path}] Error: {e}");
            Err(e)
        },
        Ok(Ok(r)) => Ok(r)
    };
    r.or_else(|api_error| {
        serialize_error(resource_type, api_error)
            .or_else(|e| Ok(Into::<tide::Response>::into(e)))
    })
}

/// server the tremor API in a separately spawned task
pub fn serve_api(host: String, world: &World) -> JoinHandle<Result<()>> {
    let mut app = tide::Server::with_state(State {
        world: world.clone(),
    });
    app.at("/version")
        .get(|r| handle_api_request(r, version::get));
    app.at("/status")
        .get(|r| handle_api_request(r, status::get_runtime_status));
    app.at("/flows")
        .get(|r| handle_api_request(r, flow::list_flows));
    app.at("/flows/:id")
        .get(|r| handle_api_request(r, flow::get_flow))
        .patch(|r| handle_api_request(r, flow::patch_flow_status));
    app.at("/flows/:id/connectors")
        .get(|r| handle_api_request(r, flow::get_flow_connectors));
    app.at("/flows/:id/connectors/:connector")
        .get(|r| handle_api_request(r, flow::get_flow_connector_status))
        .patch(|r| handle_api_request(r, flow::patch_flow_connector_status));
    // spawn API listener
    async_std::task::spawn(async move {
        let res = app.listen(host).await;
        warn!("API stopped.");
        if let Err(e) = res {
            error!("API Error: {}", e);
            Err(e.into())
        } else {
            Ok(())
        }
    })
}
