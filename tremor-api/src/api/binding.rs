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
use tremor_common::url::TremorUrl;
use tremor_runtime::repository::{BindingArtefact, RepoWrapper};

#[derive(Serialize)]
struct BindingWrap {
    artefact: tremor_runtime::config::Binding,
    instances: Vec<String>,
}

impl From<RepoWrapper<BindingArtefact>> for BindingWrap {
    fn from(wrapper: RepoWrapper<BindingArtefact>) -> Self {
        Self {
            artefact: wrapper.artefact.binding,
            instances: wrapper
                .instances
                .iter()
                .filter_map(|url| url.instance().map(ToString::to_string))
                .collect(),
        }
    }
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

pub async fn publish_artefact(_req: Request) -> Result<Response> {
    // FIXME: remove api for piepleins
    unimplemented!()
    // let (req, binding): (_, tremor_runtime::config::Binding) = decode(req).await?;
    // let url = TremorUrl::from_binding_id(&binding.id)?;

    // let repo = &req.state().world.repo;
    // let result = repo
    //     .publish_binding(&url, false, BindingArtefact::new(binding, None))
    //     .await?;

    // reply(req, result.binding, true, StatusCode::Created).await
}

pub async fn unpublish_artefact(req: Request) -> Result<Response> {
    let id = req.param("aid").unwrap_or_default();
    let url = TremorUrl::from_binding_id(id)?;
    let repo = &req.state().world.repo;
    // TODO: unbind all instances first
    let result = repo.unpublish_binding(&url).await?;
    reply(req, result.binding, true, StatusCode::Ok).await
}

pub async fn get_artefact(req: Request) -> Result<Response> {
    let id = req.param("aid").unwrap_or_default();
    let url = TremorUrl::from_binding_id(id)?;

    let repo = &req.state().world.repo;
    let result = repo
        .find_binding(&url)
        .await?
        .ok_or_else(Error::artefact_not_found)?;

    let result = BindingWrap::from(result);
    reply(req, result, false, StatusCode::Ok).await
}

pub async fn get_instance(req: Request) -> Result<Response> {
    let a_id = req.param("aid").unwrap_or_default();
    let s_id = req.param("sid").unwrap_or_default();
    let url = TremorUrl::from_binding_instance(a_id, s_id);

    let registry = &req.state().world.reg;
    let (_addr, artefact) = registry
        .find_binding(&url)
        .await?
        .ok_or_else(Error::instance_not_found)?;

    reply(req, artefact.binding, false, StatusCode::Ok).await
}

pub async fn spawn_instance(req: Request) -> Result<Response> {
    let (req, decoded_data): (_, HashMap<String, String, _>) = decode(req).await?;

    let a_id = req.param("aid").unwrap_or_default();
    let s_id = req.param("sid").unwrap_or_default();
    let url = TremorUrl::from_binding_instance(a_id, s_id);
    let world = &req.state().world;

    // link and start binding
    let result = world.launch_binding(&url, decoded_data).await?;
    world.reg.start_binding(&url).await?;

    reply(req, result.binding, true, StatusCode::Created).await
}

pub async fn shutdown_instance(req: Request) -> Result<Response> {
    let a_id = req.param("aid").unwrap_or_default();
    let s_id = req.param("sid").unwrap_or_default();
    let url = TremorUrl::from_binding_instance(a_id, s_id);

    let world = &req.state().world;
    let result = world.unlink_binding(&url, HashMap::new()).await?;

    reply(req, result.binding, true, StatusCode::NoContent).await
}
