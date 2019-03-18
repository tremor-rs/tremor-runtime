// Copyright 2018-2019, Wayfair GmbH
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
use actix_web::{HttpMessage, HttpRequest, HttpResponse};
use log::error;
use serde::Serialize;
use tremor_runtime::errors::*;
use tremor_runtime::system::World;

pub mod binding;
pub mod offramp;
pub mod onramp;
pub mod pipeline;
pub mod version;

mod resource_models;

#[derive(Clone)]
pub struct State {
    pub world: World,
}

pub enum ResourceType {
    Json,
    Yaml,
}

pub fn content_type(req: &HttpRequest<State>) -> Option<ResourceType> {
    let ct: &str = match req.headers().get("Content-type") {
        Some(x) => x.to_str().unwrap(),
        None => req.content_type(),
    };
    match ct {
        "application/yaml" => Some(ResourceType::Yaml),
        "application/json" => Some(ResourceType::Json),
        _ => None,
    }
}

pub fn accept(req: &HttpRequest<State>) -> ResourceType {
    // TODO implement correctly / RFC compliance
    let accept: &str = match req.headers().get("Accept") {
        Some(x) => x.to_str().unwrap(),
        None => req.content_type(),
    };
    match accept {
        "application/yaml" => ResourceType::Yaml,
        "application/json" => ResourceType::Json,
        _ => ResourceType::Json,
    }
}

pub fn c(c: u16) -> StatusCode {
    StatusCode::from_u16(c).unwrap()
}

pub fn handle_errors(e: Error) -> HttpResponse {
    match e.0 {
        ErrorKind::UnpublishFailedNonZeroInstances(_) => HttpResponse::build(c(409)).finish(),
        ErrorKind::ArtifactNotFound(_) => HttpResponse::build(c(404)).finish(),
        ErrorKind::PublishFailedAlreadyExists(_) => HttpResponse::build(c(409)).finish(),

        e => {
            error!("Unhandled error: {}", e);
            HttpResponse::build(StatusCode::from_u16(500).unwrap()).finish()
        }
    }
}
pub fn serialize<T: Serialize>(t: ResourceType, d: &T, ok_code: u16) -> HttpResponse {
    match t {
        ResourceType::Yaml => HttpResponse::build(c(ok_code))
            .content_type("application/yaml")
            .body(serde_yaml::to_string(d).unwrap()),
        ResourceType::Json => HttpResponse::build(c(ok_code))
            .content_type("application/json")
            .body(serde_json::to_string(d).unwrap()),
    }
}

pub fn reply<T: Serialize>(req: HttpRequest<State>, res: Result<T>, ok_code: u16) -> HttpResponse {
    match res {
        Ok(res) => serialize(accept(&req), &res, ok_code),
        Err(e) => handle_errors(e),
    }
}
