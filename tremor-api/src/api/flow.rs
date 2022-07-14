// Copyright 2022, The Tremor Team
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

//! Flow API

use crate::{
    api::prelude::*,
    model::{ApiConnectorStatusReport, ApiFlowStatusReport, PatchStatus},
};

pub(crate) async fn list_flows(req: Request) -> Result<Response> {
    let world = &req.state().world;
    let flows = world.get_flows().await?;
    let mut result: Vec<ApiFlowStatusReport> = Vec::with_capacity(flows.len());
    for flow in flows {
        let status = flow.report_status().await?;
        result.push(status.into());
    }
    reply(&req, result, StatusCode::Ok)
}

pub(crate) async fn get_flow(req: Request) -> Result<Response> {
    let world = &req.state().world;
    let flow_id = req.param("id")?.to_string();
    let flow = world.get_flow(flow_id).await?;
    let report = flow.report_status().await?;
    reply(&req, ApiFlowStatusReport::from(report), StatusCode::Ok)
}

pub(crate) async fn patch_flow_status(mut req: Request) -> Result<Response> {
    let patch_status_payload: PatchStatus = req.body_json().await?;
    let world = &req.state().world;
    let flow_id = req.param("id")?.to_string();
    let flow = world.get_flow(flow_id.clone()).await?;
    let current_status = flow.report_status().await?;
    let report = match (current_status.status, patch_status_payload.status) {
        (state1, state2) if state1 == state2 => {
            // desired status == current status
            current_status
        }
        (InstanceState::Running, InstanceState::Paused) => {
            flow.pause().await?;
            flow.report_status().await?
        }

        (InstanceState::Paused, InstanceState::Running) => {
            flow.resume().await?;
            flow.report_status().await?
        }
        // TODO: we could stop a flow by patching its status to `Stopped`
        (current, desired) => {
            // throw error
            return Err(Error::bad_request(format!(
                "Cannot patch status of {flow_id} to from {current} to {desired}"
            )));
        }
    };
    reply(&req, ApiFlowStatusReport::from(report), StatusCode::Ok)
}

pub(crate) async fn get_flow_connectors(req: Request) -> Result<Response> {
    let world = &req.state().world;
    let flow_id = req.param("id")?.to_string();
    let flow = world.get_flow(flow_id).await?;
    let connectors = flow.get_connectors().await?;
    let mut result: Vec<ApiConnectorStatusReport> = Vec::with_capacity(connectors.len());
    for connector in connectors {
        let status = connector.report_status().await?;
        result.push(status.into());
    }
    reply(&req, result, StatusCode::Ok)
}

pub(crate) async fn get_flow_connector_status(req: Request) -> Result<Response> {
    let world = &req.state().world;
    let flow_id = req.param("id")?.to_string();
    let connector_id = req.param("connector")?.to_string();
    let flow = world.get_flow(flow_id).await?;

    let connector = flow.get_connector(connector_id).await?;
    let report = connector.report_status().await?;
    reply(&req, ApiConnectorStatusReport::from(report), StatusCode::Ok)
}

pub(crate) async fn patch_flow_connector_status(mut req: Request) -> Result<Response> {
    let patch_status_payload: PatchStatus = req.body_json().await?;
    let flow_id = req.param("id")?.to_string();
    let connector_id = req.param("connector")?.to_string();

    let world = &req.state().world;
    let flow = world.get_flow(flow_id.clone()).await?;
    let connector = flow.get_connector(connector_id.clone()).await?;
    let current_status = connector.report_status().await?;
    let report = match (current_status.status(), patch_status_payload.status) {
        (state1, state2) if *state1 == state2 => {
            // desired status == current status
            current_status
        }
        (InstanceState::Running, InstanceState::Paused) => {
            connector.pause().await?;
            connector.report_status().await?
        }

        (InstanceState::Paused, InstanceState::Running) => {
            connector.resume().await?;
            connector.report_status().await?
        }
        // TODO: we could stop a deployment by patching its status to `Stopped`
        (current, desired) => {
            // throw error
            return Err(Error::bad_request(format!("Cannot patch status of connector {connector_id} in flow {flow_id} from {current} to {desired}")));
        }
    };
    reply(&req, ApiConnectorStatusReport::from(report), StatusCode::Ok)
}
