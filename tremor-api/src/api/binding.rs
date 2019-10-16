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
    let res: Result<Vec<String>> = data.world.repo.list_bindings().map(|l| {
        l.iter()
            .filter_map(tremor_runtime::url::TremorURL::artefact)
            .collect()
    });
    reply(req, data, res, false, 200)
}

pub fn publish_artefact((req, data, data_raw): (HttpRequest, Data<State>, String)) -> ApiResult {
    let binding: tremor_runtime::config::Binding = decode(&req, &data_raw)?;
    let url = build_url(&["binding", &binding.id])?;
    let res = data.world.repo.publish_binding(
        &url,
        false,
        BindingArtefact {
            binding,
            mapping: None,
        },
    );
    reply(req, data, res.map(|a| a.binding), true, 201)
}

pub fn unpublish_artefact(
    (req, data, id): (HttpRequest, Data<State>, Path<(String)>),
) -> ApiResult {
    let url = build_url(&["binding", &id])?;
    let res = data.world.repo.unpublish_binding(&url);
    reply(req, data, res.map(|a| a.binding), true, 200)
}

pub fn get_artefact((req, data, id): (HttpRequest, Data<State>, Path<String>)) -> ApiResult {
    let url = build_url(&["binding", &id])?;
    let res = data
        .world
        .repo
        .find_binding(&url)
        .map_err(|_e| error::ErrorInternalServerError("lookup failed"))?;
    match res {
        Some(res) => {
            let res: Result<BindingWrap> = Ok(BindingWrap {
                artefact: res.artefact.binding,
                instances: res
                    .instances
                    .iter()
                    .filter_map(tremor_runtime::url::TremorURL::instance)
                    .collect(),
            });
            reply(req, data, res, false, 200)
        }
        None => Err(error::ErrorNotFound(r#"{"error": "Artefact not found"}"#)),
    }
}

pub fn get_servant(
    (req, data, path): (HttpRequest, Data<State>, Path<(String, String)>),
) -> ApiResult {
    let url = build_url(&["binding", &path.0, &path.1])?;
    let res0 = data.world.reg.find_binding(&url);
    match res0 {
        Ok(res) => match res {
            Some(res) => reply(req, data, Ok(res.binding), false, 200),
            None => Err(error::ErrorNotFound("Binding not found")),
        },
        Err(e) => Err(error::ErrorInternalServerError(format!(
            "Internal server error: {}",
            e
        ))),
    }
}

// We really don't want to deal with that!
#[allow(clippy::implicit_hasher)]
pub fn link_servant(
    (req, data, path, data_raw): (HttpRequest, Data<State>, Path<(String, String)>, String),
) -> ApiResult {
    let decoded_data: HashMap<String, String> = decode(&req, &data_raw)?;
    let url = build_url(&["binding", &path.0, &path.1])?;
    let res = data.world.link_binding(&url, decoded_data);
    reply(req, data, res.map(|a| a.binding), true, 201)
}

#[allow(clippy::implicit_hasher)]
pub fn unlink_servant(
    (req, data, path): (HttpRequest, Data<State>, Path<(String, String)>),
) -> ApiResult {
    let url = build_url(&["binding", &path.0, &path.1])?;
    let res = data.world.unlink_binding(&url, HashMap::new());
    reply(req, data, res.map(|a| a.binding), true, 200)
}
