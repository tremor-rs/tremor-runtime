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

use tremor_pipeline::{query::Query, FN_REGISTRY};

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

pub async fn publish_artefact(mut req: Request) -> Result<Response> {
    match content_type(&req) {
        Some(ResourceType::Trickle) => {
            let body = req.body_string().await?;
            let aggr_reg = tremor_script::registry::aggr();
            let module_path = tremor_script::path::load();
            println!("{}", body);
            let query = Query::parse(
                &module_path,
                &body,
                "<API>",
                vec![],
                &*FN_REGISTRY.lock()?,
                &aggr_reg,
            )?;

            let id = query.id().ok_or_else(|| {
                Error::new(
                    StatusCode::UnprocessableEntity,
                    r#"no `#!config id = "trickle-id"` directive provided"#.into(),
                )
            })?;

            let url = build_url(&["pipeline", &id])?;
            let repo = &req.state().world.repo;
            let result = repo
                .publish_pipeline(&url, false, query)
                .await
                .map(|result| result.source().to_string())?;
            reply_trickle_flat(req, result, true, StatusCode::Created).await
        }
        Some(_) | None => Err(Error::new(
            StatusCode::UnsupportedMediaType,
            "No content type provided".into(),
        )),
    }
}

pub async fn reply_trickle_flat(
    req: Request,
    result_in: String,
    persist: bool,
    ok_code: StatusCode,
) -> Result<Response> {
    if persist {
        let world = &req.state().world;
        world.save_config().await?;
    }
    match accept(&req) {
        ResourceType::Json | ResourceType::Yaml => serialize(accept(&req), &result_in, ok_code),
        ResourceType::Trickle => {
            let mut r = Response::new(ok_code);
            r.insert_header(headers::CONTENT_TYPE, ResourceType::Trickle.as_str());
            r.set_body(result_in);
            Ok(r)
        }
    }
}

pub async fn reply_trickle_instanced(
    req: Request,
    mut result_in: String,
    instances: Vec<String>,
    persist: bool,
    ok_code: StatusCode,
) -> Result<Response> {
    if persist {
        let world = &req.state().world;
        world.save_config().await?;
    }
    match accept(&req) {
        ResourceType::Json | ResourceType::Yaml => serialize(accept(&req), &result_in, ok_code),
        ResourceType::Trickle => {
            let mut r = Response::new(ok_code);
            r.insert_header(headers::CONTENT_TYPE, ResourceType::Trickle.as_str());
            let instances = instances.join("#  * ");
            result_in.push_str("\n# Instances:\n#  * ");
            result_in.push_str(&instances);
            r.set_body(result_in);
            Ok(r)
        }
    }
}

pub async fn unpublish_artefact(req: Request) -> Result<Response> {
    let id = req.param("aid").unwrap_or_default();
    let url = build_url(&["pipeline", id])?;
    let repo = &req.state().world.repo;
    let result = repo
        .unpublish_pipeline(&url)
        .await
        .map(|result| result.source().to_string())?;
    reply_trickle_flat(req, result, true, StatusCode::Ok).await
}

pub async fn get_artefact(req: Request) -> Result<Response> {
    let id = req.param("aid").unwrap_or_default();
    let url = build_url(&["pipeline", id])?;
    let repo = &req.state().world.repo;
    let result = repo
        .find_pipeline(&url)
        .await?
        .ok_or_else(Error::not_found)?;

    reply_trickle_instanced(
        req,
        result.artefact.source().to_string(),
        result
            .instances
            .iter()
            .filter_map(|v| v.instance().map(String::from))
            .collect(),
        false,
        StatusCode::Ok,
    )
    .await
}
