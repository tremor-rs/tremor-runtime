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

use crate::api::resource_models::BindingList;
use crate::api::{accept, encode, ResourceType, State};
use crate::repository::BindingArtefact;
use actix_web::http::StatusCode;
use actix_web::{error, HttpRequest, HttpResponse, Json, Path, Responder};
use std::collections::HashMap;
use tremor_runtime::url::TremorURL;

pub fn list_artefact(req: HttpRequest<State>) -> impl Responder {
    let res: Result<BindingList, error::Error> = req
        .state()
        .world
        .repo
        .list_bindings()
        .map_err(|_e| error::ErrorInternalServerError("lookup failed"));
    api_get!(req, res)
}

pub fn publish_artefact(
    (req, data): (HttpRequest<State>, Json<BindingArtefact>),
) -> impl Responder {
    let url = TremorURL::parse(&format!("/binding/{}", data.id))
        .map_err(|_e| error::ErrorBadRequest("bad url"))
        .unwrap();
    let res = req
        .state()
        .world
        .repo
        .publish_binding(&url, data.0)
        .map_err(|_e| error::ErrorInternalServerError("lookup failed"));

    match res {
        Ok(res) => match accept(&req) {
            Some(ResourceType::Yaml) => HttpResponse::build(StatusCode::from_u16(201).unwrap())
                .content_type("application/yaml")
                .body(serde_yaml::to_string(&res).unwrap()),
            Some(ResourceType::Json) => HttpResponse::build(StatusCode::from_u16(201).unwrap())
                .content_type("application/json")
                .body(serde_json::to_string(&res).unwrap()),
            None => HttpResponse::build(StatusCode::from_u16(400).unwrap()).finish(),
        },
        Err(_e) => HttpResponse::InternalServerError().finish(),
    }
}

pub fn get_artefact((req, id): (HttpRequest<State>, Path<String>)) -> impl Responder {
    let url = TremorURL::parse(&format!("/binding/{}", id))
        .map_err(|_e| error::ErrorBadRequest("bad url"))
        .unwrap();

    let res = req
        .state()
        .world
        .repo
        .find_binding(&url)
        .map_err(|_e| error::ErrorInternalServerError("lookup failed"))
        .unwrap();

    api_maybe_get!(req, res)
}

pub fn get_servant((req, path): (HttpRequest<State>, Path<(String, String)>)) -> impl Responder {
    // TODO
    let url = TremorURL::parse(&format!("/binding/{}/{}", path.0, path.1))
        .map_err(|_e| error::ErrorBadRequest("bad url"))
        .unwrap();
    let res = req
        .state()
        .world
        .reg
        .find_binding(&url)
        .map_err(|_e| error::ErrorInternalServerError("lookup failed"))
        .unwrap();

    api_maybe_get!(req, res)
}

pub fn link_servant(
    (req, path, mapping): (
        HttpRequest<State>,
        Path<(String, String)>,
        Json<HashMap<String, String>>,
    ),
) -> impl Responder {
    // TODO
    let url = TremorURL::parse(&format!("/binding/{}/{}", path.0, path.1))
        .map_err(|_e| error::ErrorBadRequest("bad url"))?;
    let res = req
        .state()
        .world
        .link_binding(&url, mapping.0)
        .map_err(|_e| error::ErrorInternalServerError("linking"));
    match res {
        Ok(res) => {
            let body = encode(req, &res)?;
            Ok(HttpResponse::Ok()
                .content_type("application/json")
                .body(body))
        }
        Err(e) => Err(e),
    }
}

pub fn unlink_servant(
    (req, path, mapping): (
        HttpRequest<State>,
        Path<(String, String)>,
        Json<HashMap<String, String>>,
    ),
) -> impl Responder {
    let url = TremorURL::parse(&format!("/binding/{}/{}", path.0, path.1))
        .map_err(|_e| error::ErrorBadRequest("bad url"))
        .unwrap();
    let res = req
        .state()
        .world
        .unlink_binding(&url, mapping.0)
        .map_err(|_e| error::ErrorInternalServerError("unlinking"));
    match res {
        Ok(res) => {
            let body = encode(req, &res).unwrap();
            Ok(HttpResponse::Ok()
                .content_type("application/json")
                .body(body))
        }
        Err(e) => Err(e),
    }
}
