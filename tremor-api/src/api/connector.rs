// Copyright 2021, The Tremor Team
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

use std::time::Duration;

use crate::api::prelude::*;
use async_std::channel::bounded;
use tremor_runtime::connectors::{Msg, StatusReport};
use tremor_runtime::registry::instance::InstanceState;

#[derive(Serialize)]
struct ConnectorAndInstances {
    artefact: tremor_runtime::config::Connector,
    instances: Vec<String>,
}

pub async fn list_artefacts(req: Request) -> Result<Response> {
    let repo = &req.state().world.repo;
    let result: Vec<_> = repo
        .list_connectors()
        .await?
        .iter()
        .filter_map(|url| url.artefact().map(String::from))
        .collect();
    reply(&req, result, StatusCode::Ok)
}

pub async fn publish_artefact(req: Request) -> Result<Response> {
    let (req, data): (_, tremor_runtime::config::Connector) =
        decode::<tremor_runtime::config::Connector>(req).await?;
    let url = TremorUrl::from_connector_id(&data.id)?;
    let repo = &req.state().world.repo;
    let result = repo.publish_connector(&url, false, data).await?;
    reply(&req, result, StatusCode::Created)
}

pub async fn unpublish_artefact(req: Request) -> Result<Response> {
    let id = req.param("aid")?;
    let url = TremorUrl::from_connector_id(id)?;
    let repo = &req.state().world.repo;
    let result = repo.unpublish_connector(&url).await?;
    reply(&req, result, StatusCode::Ok)
}

pub async fn get_artefact(req: Request) -> Result<Response> {
    let id = req.param("aid")?;
    let url = TremorUrl::from_connector_id(id)?;
    let repo = &req.state().world.repo;
    let result = repo
        .find_connector(&url)
        .await?
        .ok_or_else(Error::artefact_not_found)?;
    let conn = ConnectorAndInstances {
        artefact: result.artefact,
        instances: result
            .instances
            .iter()
            .filter_map(|instance_url| instance_url.instance().map(String::from))
            .collect(),
    };
    reply(&req, conn, StatusCode::Ok)
}

pub async fn get_instance(req: Request) -> Result<Response> {
    let a_id = req.param("aid")?;
    let s_id = req.param("sid")?;
    let instance_url = TremorUrl::from_connector_instance(a_id, s_id)?;
    let registry = &req.state().world.reg;
    let instance = registry
        .find_connector(&instance_url)
        .await?
        .ok_or_else(Error::instance_not_found)?;
    let (tx, rx) = bounded(1);
    instance.send(Msg::Report(tx)).await?;
    let report = rx.recv().await?;

    reply(&req, report, StatusCode::Ok)
}

#[derive(Deserialize)]
struct InstancePatch {
    status: InstanceState,
}

/// this boils down to pause/resume/start/stop
pub async fn patch_instance(req: Request) -> Result<Response> {
    use InstanceState::{Initialized, Paused, Running, Stopped};
    let (req, patch): (_, InstancePatch) = decode(req).await?;
    let a_id = req.param("aid")?;
    let s_id = req.param("sid")?;
    let instance_url = TremorUrl::from_connector_instance(a_id, s_id)?;
    let system = &req.state().world;
    let registry = &system.reg;
    let instance = registry
        .find_connector(&instance_url)
        .await?
        .ok_or_else(Error::instance_not_found)?;
    if instance
        .url
        .artefact()
        .map_or(false, |a| a.starts_with("system::"))
    {
        return Err(Error::new(
            StatusCode::BadRequest,
            "Cannot patch a system connector".into(),
        ));
    }
    // fetch current status
    let (tx, rx) = bounded(1);
    instance.send(Msg::Report(tx.clone())).await?;
    let current_state = rx.recv().await?;
    // dispatch to intended action
    match (current_state.status, patch.status) {
        (Running, Paused) => {
            // pause
            registry.pause_connector(&instance.url).await?;
        }
        (_, Stopped) => {
            // stop - we need to return immediately here, as the
            // connector will be stopped and the addr is now invalid
            system.destroy_connector_instance(&instance.url).await?; // unbind will properly remove the instance and such
            let report = StatusReport {
                status: Stopped,
                ..current_state
            };
            return reply(&req, report, StatusCode::Ok);
        }
        (Paused, Running) => {
            // resume
            registry.resume_connector(&instance.url).await?;
        }
        (Initialized, Running) => {
            // start
            registry.start_connector(&instance.url).await?;
        }
        (current, wanted) => {
            // cannot transition
            return Err(Error::new(
                StatusCode::Conflict,
                format!(
                    "Cannot patch {} state from {} to {}",
                    &instance_url, current, wanted
                ),
            ));
        }
    };

    // fetch new status
    instance.send(Msg::Report(tx)).await?;
    let new_status = async_std::future::timeout(Duration::from_secs(2), rx.recv()).await??;
    reply(&req, new_status, StatusCode::Ok)
}
