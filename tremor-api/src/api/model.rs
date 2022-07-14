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

use crate::api::prelude::*;
use halfbrown::HashMap;
use tremor_runtime::{
    connectors::{Connectivity, StatusReport as ConnectorStatusReport},
    instance::State,
    system::flow::{Alias as FlowAlias, StatusReport as FlowStatusReport},
};
use tremor_script::ast::DeployEndpoint;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ApiFlowStatusReport {
    pub(crate) alias: FlowAlias,
    pub(crate) status: State,
    pub(crate) connectors: Vec<String>,
}

impl From<FlowStatusReport> for ApiFlowStatusReport {
    fn from(sr: FlowStatusReport) -> Self {
        Self {
            alias: sr.alias,
            status: sr.status,
            connectors: sr
                .connectors
                .into_iter()
                .map(|ca| ca.connector_alias().to_string())
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Pipeline {
    alias: String,
    port: String,
}

impl From<&DeployEndpoint> for Pipeline {
    fn from(de: &DeployEndpoint) -> Self {
        Self {
            alias: de.alias().to_string(),
            port: de.port().to_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ApiConnectorStatusReport {
    pub(crate) alias: String,
    pub(crate) status: State,
    pub(crate) connectivity: Connectivity,
    pub(crate) pipelines: HashMap<String, Vec<Pipeline>>,
}

impl From<ConnectorStatusReport> for ApiConnectorStatusReport {
    fn from(csr: ConnectorStatusReport) -> Self {
        Self {
            alias: csr.alias().connector_alias().to_string(),
            status: *csr.status(),
            connectivity: *csr.connectivity(),
            pipelines: csr
                .pipelines()
                .iter()
                .map(|(k, v)| {
                    (
                        k.to_string(),
                        v.iter().map(Pipeline::from).collect::<Vec<_>>(),
                    )
                })
                .collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct PatchStatus {
    pub(crate) status: InstanceState,
}
