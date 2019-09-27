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
use actix_web::{
    error,
    web::{Data, Path},
    HttpRequest,
};
use tremor_runtime::errors::*;
use tremor_runtime::repository::PipelineArtefact;

#[derive(Serialize)]
struct PipelineWrap {
    artefact: tremor_pipeline::config::Pipeline,
    instances: Vec<String>,
}

pub fn list_artefact((req, data): (HttpRequest, Data<State>)) -> ApiResult {
    let res: Result<Vec<String>> = data.world.repo.list_pipelines().map(|l| {
        l.iter()
            .filter_map(tremor_runtime::url::TremorURL::artefact)
            .collect()
    });
    reply(req, data, res, false, 200)
}

pub fn publish_artefact((req, data, data_raw): (HttpRequest, Data<State>, String)) -> ApiResult {
    let decoded_data: tremor_pipeline::config::Pipeline = decode(&req, &data_raw)?;
    let url = build_url(&["pipeline", &decoded_data.id])?;
    let pipeline = tremor_pipeline::build_pipeline(decoded_data.clone())
        .map_err(|e| error::ErrorBadRequest(format!("Bad pipeline: {}", e)))?;
    let res = data
        .world
        .repo
        .publish_pipeline(url, false, PipelineArtefact::Pipeline(pipeline))
        .map(|res| match res {
            PipelineArtefact::Pipeline(p) => p.config,
            //NOTE:  We publish a pipeline we can't ever get anything else back
            _ => unreachable!(),
        });
    reply(req, data, res, true, 201)
}

pub fn unpublish_artefact(
    (req, data, id): (HttpRequest, Data<State>, Path<(String)>),
) -> ApiResult {
    let url = build_url(&["pipeline", &id])?;
    let res = data
        .world
        .repo
        .unpublish_pipeline(url)
        .and_then(|res| match res {
            PipelineArtefact::Pipeline(p) => Ok(p.config),
            _ => Err("This is a query".into()), // FIXME
        });
    reply(req, data, res, true, 200)
}

pub fn get_artefact((req, data, id): (HttpRequest, Data<State>, Path<String>)) -> ApiResult {
    let url = build_url(&["pipeline", &id])?;
    let res = data
        .world
        .repo
        .find_pipeline(url)
        .map_err(|_e| error::ErrorInternalServerError("lookup failed"))?;
    match res {
        Some(res) => match res.artefact {
            PipelineArtefact::Pipeline(p) => reply(
                req,
                data,
                Ok(PipelineWrap {
                    artefact: p.config,
                    instances: res
                        .instances
                        .iter()
                        .filter_map(tremor_runtime::url::TremorURL::instance)
                        .collect(),
                }),
                false,
                200,
            ),
            _ => Err(error::ErrorNotFound(
                r#"{"error": "Artefact is a pipeline"}"#,
            )),
        },
        None => Err(error::ErrorNotFound(r#"{"error": "Artefact not found"}"#)),
    }
}
