// Copyright 2020-2021, The Tremor Team
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
use hashbrown::HashMap;
use tremor_runtime::repository::BindingArtefact;

#[derive(Serialize)]
struct BindingWrap {
    artefact: tremor_runtime::config::Binding,
    instances: Vec<String>,
}

pub async fn list_artefact(req: Request) -> Result<Response> {
    let repo = &req.state().world.repo;

    let result: Vec<_> = repo
        .list_bindings()
        .await?
        .iter()
        .filter_map(|v| v.artefact().map(String::from))
        .collect();
    reply(req, result, false, StatusCode::Ok).await
}

pub async fn publish_artefact(req: Request) -> Result<Response> {
    let (req, binding): (_, tremor_runtime::config::Binding) = decode(req).await?;
    let url = build_url(&["binding", &binding.id])?;

    let repo = &req.state().world.repo;
    let result = repo
        .publish_binding(
            &url,
            false,
            BindingArtefact {
                binding,
                mapping: None,
            },
        )
        .await?;

    reply(req, result.binding, true, StatusCode::Created).await
}

pub async fn unpublish_artefact(req: Request) -> Result<Response> {
    let id = req.param("aid").unwrap_or_default();
    let url = build_url(&["binding", id])?;
    let repo = &req.state().world.repo;
    let result = repo.unpublish_binding(&url).await?;
    reply(req, result.binding, true, StatusCode::Ok).await
}

pub async fn get_artefact(req: Request) -> Result<Response> {
    let id = req.param("aid").unwrap_or_default();
    let url = build_url(&["binding", id])?;

    let repo = &req.state().world.repo;
    let result = repo
        .find_binding(&url)
        .await?
        .ok_or_else(Error::not_found)?;

    let result = BindingWrap {
        artefact: result.artefact.binding,
        instances: result
            .instances
            .iter()
            .filter_map(|v| v.instance().map(String::from))
            .collect(),
    };

    reply(req, result, false, StatusCode::Ok).await
}

pub async fn get_servant(req: Request) -> Result<Response> {
    let a_id = req.param("aid").unwrap_or_default();
    let s_id = req.param("sid").unwrap_or_default();
    let url = build_url(&["binding", a_id, s_id])?;

    let registry = &req.state().world.reg;
    let result = registry
        .find_binding(&url)
        .await?
        .ok_or_else(Error::not_found)?
        .binding;

    reply(req, result, false, StatusCode::Ok).await
}

// We really don't want to deal with that!
pub async fn link_servant(req: Request) -> Result<Response> {
    let (req, decoded_data): (_, HashMap<String, String, _>) = decode(req).await?;

    let a_id = req.param("aid").unwrap_or_default();
    let s_id = req.param("sid").unwrap_or_default();
    let url = build_url(&["binding", a_id, s_id])?;
    let world = &req.state().world;

    let result = world.link_binding(&url, decoded_data).await?.binding;

    reply(req, result, true, StatusCode::Created).await
}

#[allow(clippy::implicit_hasher)]
pub async fn unlink_servant(req: Request) -> Result<Response> {
    let a_id = req.param("aid").unwrap_or_default();
    let s_id = req.param("sid").unwrap_or_default();
    let url = build_url(&["binding", a_id, s_id])?;

    let world = &req.state().world;
    let result = world.unlink_binding(&url, HashMap::new()).await?.binding;

    reply(req, result, true, StatusCode::NoContent).await
}
