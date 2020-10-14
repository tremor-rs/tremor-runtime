// Copyright 2020, The Tremor Team
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

use crate::api::prelude::*;

#[derive(Serialize)]
struct PipelineWrap {
    pub query: String,
    instances: Vec<String>,
}

pub async fn list_artefact(req: Request) -> Result<Response> {
    let repo = &req.state().world.repo;

    let result: Vec<_> = repo
        .list_pipelines()
        .await?
        .iter()
        .filter_map(|v| v.artefact().map(String::from))
        .collect();
    reply(req, result, false, StatusCode::Ok).await
}

pub async fn publish_artefact(_req: Request) -> Result<Response> {
    panic!("FIXME .unwrap()")
    //     let (req, decoded_data): (_, tremor_pipeline::config::Pipeline) = decode(req).await?;

    //     let url = build_url(&["pipeline", &decoded_data.id])?;
    //     let pipeline = tremor_pipeline::build_pipeline(decoded_data)?;

    //     let repo = &req.state().world.repo;
    //     let result = repo
    //         .publish_pipeline(&url, false, PipelineArtefact::Pipeline(Box::new(pipeline)))
    //         .await
    //         .map(|result| match result {
    //             PipelineArtefact::Pipeline(p) => p.config,
    //             //ALLOW:  We publish a pipeline we can't ever get anything else back
    //             PipelineArtefact::Query(_) => unreachable!(),
    //         })?;
    //     reply(req, result, true, StatusCode::Created).await
}

pub async fn unpublish_artefact(req: Request) -> Result<Response> {
    let id: String = req.param("aid").unwrap_or_default();
    let url = build_url(&["pipeline", &id])?;
    let repo = &req.state().world.repo;
    let result = repo
        .unpublish_pipeline(&url)
        .await
        .and_then(|result| Ok(result.source().to_string()))?;
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
    reply(
        req,
        PipelineWrap {
            query: result.artefact.source().to_string(),
            instances: result
                .instances
                .iter()
                .filter_map(|v| v.instance().map(String::from))
                .collect(),
        },
        false,
        StatusCode::Ok,
    )
    .await
}
