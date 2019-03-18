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

use crate::api::{reply, State};
use crate::{content_type, ResourceType};
use actix_web::http::StatusCode;
use actix_web::{error, HttpRequest, HttpResponse, Path, Responder};
use tremor_runtime::errors::*;
use tremor_runtime::url::TremorURL;

#[derive(Serialize)]
struct OnRampWrap {
    artefact: tremor_runtime::config::OnRamp,
    instances: Vec<String>,
}

pub fn list_artefact(req: HttpRequest<State>) -> impl Responder {
    let res = req.state().world.repo.list_onramps();
    reply(req, res, 200)
}

pub fn publish_artefact((req, data_raw): (HttpRequest<State>, String)) -> impl Responder {
    let data: tremor_runtime::config::OnRamp = match content_type(&req) {
        Some(ResourceType::Yaml) => serde_yaml::from_str(&data_raw).unwrap(),
        Some(ResourceType::Json) => serde_json::from_str(&data_raw).unwrap(),
        None => return HttpResponse::InternalServerError().finish(),
    };
    let url = TremorURL::parse(&format!("/onramp/{}", data.id))
        .map_err(|_e| error::ErrorBadRequest("bad url"))
        .unwrap();
    let res = req.state().world.repo.publish_onramp(&url, data);
    reply(req, res, 201)
}

pub fn unpublish_artefact((req, path): (HttpRequest<State>, Path<(String)>)) -> impl Responder {
    let url = match TremorURL::parse(&format!("/onramp/{}", path)) {
        Ok(url) => url,
        Err(_) => return HttpResponse::build(StatusCode::from_u16(400).unwrap()).finish(),
    };
    let res = req.state().world.repo.unpublish_onramp(&url);
    reply(req, res, 200)
}

pub fn get_artefact((req, id): (HttpRequest<State>, Path<String>)) -> impl Responder {
    let url = TremorURL::parse(&format!("/onramp/{}", id))
        .map_err(|_e| error::ErrorBadRequest("bad url"))
        .unwrap();
    let res = req
        .state()
        .world
        .repo
        .find_onramp(&url)
        .map_err(|_e| error::ErrorInternalServerError("lookup failed"));
    match res {
        Ok(res) => match res {
            Some(res) => {
                let res: Result<OnRampWrap> = Ok(OnRampWrap {
                    artefact: res.artefact,
                    instances: res.instances,
                });
                reply(req, res, 200)
            }
            None => HttpResponse::build(StatusCode::from_u16(404).unwrap()).finish(),
        },
        Err(_) => HttpResponse::build(StatusCode::from_u16(404).unwrap()).finish(),
    }
}
