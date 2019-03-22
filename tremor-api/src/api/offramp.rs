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

use crate::api::*;
use actix_web::{error, HttpRequest, Path};
use tremor_runtime::errors::*;

#[derive(Serialize)]
struct OffRampWrap {
    artefact: tremor_runtime::config::OffRamp,
    instances: Vec<String>,
}

pub fn list_artefact(req: HttpRequest<State>) -> ApiResult {
    let res: Result<Vec<String>> = req
        .state()
        .world
        .repo
        .list_offramps()
        .map(|l| l.iter().filter_map(|v| v.artefact()).collect());
    reply(req, res, false, 200)
}

pub fn publish_artefact((req, data_raw): (HttpRequest<State>, String)) -> ApiResult {
    let data: tremor_runtime::config::OffRamp = decode(&req, &data_raw)?;
    let url = build_url(&["offramp", &data.id])?;
    let res = req.state().world.repo.publish_offramp(url, false, data);
    reply(req, res, true, 201)
}

pub fn unpublish_artefact((req, id): (HttpRequest<State>, Path<String>)) -> ApiResult {
    let url = build_url(&["offramp", &id])?;
    let res = req.state().world.repo.unpublish_offramp(url);
    reply(req, res, true, 200)
}

pub fn get_artefact((req, id): (HttpRequest<State>, Path<String>)) -> ApiResult {
    let url = build_url(&["offramp", &id])?;
    let res = req
        .state()
        .world
        .repo
        .find_offramp(url)
        .map_err(|_e| error::ErrorInternalServerError("lookup failed"))?;
    match res {
        Some(res) => {
            let res: Result<OffRampWrap> = Ok(OffRampWrap {
                artefact: res.artefact,
                instances: res.instances.iter().filter_map(|v| v.instance()).collect(),
            });
            reply(req, res, false, 200)
        }
        None => Err(error::ErrorNotFound("Artefact not found")),
    }
}
