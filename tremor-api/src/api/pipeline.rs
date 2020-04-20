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

use crate::api::*;
use tremor_runtime::repository::PipelineArtefact;

#[derive(Serialize)]
struct PipelineWrap {
    pub artefact: tremor_pipeline::config::Pipeline,
    instances: Vec<String>,
}

pub async fn list_artefact(req: Request) -> Result<Response> {
    let repo = &req.state().world.repo;

    let result: Vec<String> = repo
        .list_pipelines()
        .await
        .iter()
        .filter_map(tremor_runtime::url::TremorURL::artefact)
        .collect();
    reply(req, result, false, StatusCode::Ok).await
}

pub async fn publish_artefact(req: Request) -> Result<Response> {
    let (req, decoded_data): (_, tremor_pipeline::config::Pipeline) = decode(req).await?;

    let url = build_url(&["pipeline", &decoded_data.id])?;
    let pipeline = tremor_pipeline::build_pipeline(decoded_data)?;

    let repo = &req.state().world.repo;
    let result = repo
        .publish_pipeline(&url, false, PipelineArtefact::Pipeline(Box::new(pipeline)))
        .await
        .map(|result| match result {
            PipelineArtefact::Pipeline(p) => p.config,
            //ALLOW:  We publish a pipeline we can't ever get anything else back
            _ => unreachable!(),
        })?;
    reply(req, result, true, StatusCode::NoContent).await
}

pub async fn unpublish_artefact(req: Request) -> Result<Response> {
    let id: String = req.param("aid").unwrap_or_default();
    let url = build_url(&["pipeline", &id])?;
    let repo = &req.state().world.repo;
    let result = repo
        .unpublish_pipeline(&url)
        .await
        .and_then(|result| match result {
            PipelineArtefact::Pipeline(p) => Ok(p.config),
            _ => Err("This is a query".into()), // FIXME
        })?;
    reply(req, result, true, StatusCode::Ok).await
}

pub async fn get_artefact(req: Request) -> Result<Response> {
    let id: String = req.param("aid").unwrap_or_default();
    let url = build_url(&["pipeline", &id])?;
    let repo = &req.state().world.repo;
    let result = repo
        .find_pipeline(&url)
        .await?
        .ok_or_else(Error::not_found)?;
    match result.artefact {
        PipelineArtefact::Pipeline(p) => {
            reply(
                req,
                PipelineWrap {
                    artefact: p.config,
                    instances: result
                        .instances
                        .iter()
                        .filter_map(tremor_runtime::url::TremorURL::instance)
                        .collect(),
                },
                false,
                StatusCode::Ok,
            )
            .await
        }
        _ => Err(Error::json(
            StatusCode::BadRequest,
            &r#"{"error": "Artefact is a query"}"#,
        )),
    }
}
