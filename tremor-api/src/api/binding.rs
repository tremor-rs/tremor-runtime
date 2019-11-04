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

// Screw actix web, it's not our fault!
#![allow(clippy::type_complexity)]

use crate::api::*;
use actix_web::{
    error,
    web::{Data, Path},
    Responder,
};
use hashbrown::HashMap;
use tremor_runtime::errors::*;
use tremor_runtime::repository::BindingArtefact;

#[derive(Serialize)]
struct BindingWrap {
    artefact: tremor_runtime::config::Binding,
    instances: Vec<String>,
}

pub fn list_artefact((req, data): (HttpRequest, Data<State>)) -> impl Responder {
    let result: Result<Vec<String>> = data.world.repo.list_bindings().map(|l| {
        l.iter()
            .filter_map(tremor_runtime::url::TremorURL::artefact)
            .collect()
    });
    reply(&req, &data, result, false, 200)
}

pub fn publish_artefact((req, data, data_raw): (HttpRequest, Data<State>, String)) -> HTTPResult {
    let binding: tremor_runtime::config::Binding = decode(&req, &data_raw)?;
    let url = build_url(&["binding", &binding.id])?;
    let result = data.world.repo.publish_binding(
        &url,
        false,
        BindingArtefact {
            binding,
            mapping: None,
        },
    );
    reply(&req, &data, result.map(|a| a.binding), true, 201)
}

pub fn unpublish_artefact((req, data, id): (HttpRequest, Data<State>, Path<String>)) -> HTTPResult {
    let url = build_url(&["binding", &id])?;
    let result = data.world.repo.unpublish_binding(&url);
    reply(&req, &data, result.map(|a| a.binding), true, 200)
}

pub fn get_artefact((req, data, id): (HttpRequest, Data<State>, Path<String>)) -> HTTPResult {
    let url = build_url(&["binding", &id])?;
    data.world
        .repo
        .find_binding(&url)
        .map_err(|_e| error::ErrorInternalServerError("lookup failed"))
        .and_then(|result| {
            result.ok_or_else(|| error::ErrorNotFound(r#"{"error": "Artefact not found"}"#))
        })
        .map(|result| {
            Ok(BindingWrap {
                artefact: result.artefact.binding,
                instances: result
                    .instances
                    .iter()
                    .filter_map(tremor_runtime::url::TremorURL::instance)
                    .collect(),
            })
        })
        .and_then(|result| reply(&req, &data, result, false, 200))
}

pub fn get_servant(
    (req, data, path): (HttpRequest, Data<State>, Path<(String, String)>),
) -> HTTPResult {
    let url = build_url(&["binding", &path.0, &path.1])?;
    data.world
        .reg
        .find_binding(&url)
        .map_err(|e| error::ErrorInternalServerError(format!("Internal server error: {}", e)))
        .and_then(|result| result.ok_or_else(|| error::ErrorNotFound("Binding not found")))
        .and_then(|result| reply(&req, &data, Ok(result.binding), false, 200))
}

// We really don't want to deal with that!
#[allow(clippy::implicit_hasher)]
pub fn link_servant(
    (req, data, path, data_raw): (HttpRequest, Data<State>, Path<(String, String)>, String),
) -> HTTPResult {
    let decoded_data: HashMap<String, String> = decode(&req, &data_raw)?;
    let url = build_url(&["binding", &path.0, &path.1])?;
    let result = data.world.link_binding(&url, decoded_data);
    reply(&req, &data, result.map(|a| a.binding), true, 201)
}

#[allow(clippy::implicit_hasher)]
pub fn unlink_servant(
    (req, data, path): (HttpRequest, Data<State>, Path<(String, String)>),
) -> HTTPResult {
    let url = build_url(&["binding", &path.0, &path.1])?;
    let result = data.world.unlink_binding(&url, HashMap::new());
    reply(&req, &data, result.map(|a| a.binding), true, 200)
}
