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

#[derive(Serialize)]
struct OnRampWrap {
    artefact: tremor_runtime::config::OnRamp,
    instances: Vec<String>,
}

pub async fn list_artefact(req: Request) -> Result<Response> {
    let repo = &req.state().world.repo;

    let result: Vec<String> = repo
        .list_onramps()
        .await
        .iter()
        .filter_map(tremor_runtime::url::TremorURL::artefact)
        .collect();
    reply(req, result, false, StatusCode::Ok).await
}

pub async fn publish_artefact(req: Request) -> Result<Response> {
    let (req, data): (_, tremor_runtime::config::OnRamp) = decode(req).await?;
    let url = build_url(&["onramp", &data.id])?;
    let repo = &req.state().world.repo;
    let result = repo.publish_onramp(&url, false, data).await?;
    reply(req, result, true, StatusCode::NoContent).await
}

pub async fn unpublish_artefact(req: Request) -> Result<Response> {
    let id: String = req.param("aid").unwrap_or_default();
    let url = build_url(&["onramp", &id])?;
    let repo = &req.state().world.repo;
    let result = repo.unpublish_onramp(&url).await?;
    reply(req, result, true, StatusCode::Ok).await
}

pub async fn get_artefact(req: Request) -> Result<Response> {
    let id: String = req.param("aid").unwrap_or_default();
    let url = build_url(&["onramp", &id])?;
    let repo = &req.state().world.repo;
    let result = repo.find_onramp(&url).await?.ok_or_else(Error::not_found)?;
    let result = OnRampWrap {
        artefact: result.artefact,
        instances: result
            .instances
            .iter()
            .filter_map(TremorURL::instance)
            .collect(),
    };

    reply(req, result, false, StatusCode::Ok).await
}
