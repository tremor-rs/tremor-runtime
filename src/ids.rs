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

use std::fmt::{Display, Formatter};

use tremor_script::ast::DeployFlow;

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq, PartialOrd)]
pub struct InstanceId(pub String);
impl Display for InstanceId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for InstanceId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for InstanceId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// An `App` is an isolated container that is defined by
/// a troy file with possibly multiple flow definitions.
/// An `App` needs to have a unique name inside a tremor cluster.
/// Flow instances (and thus connector and pipeline instances) are spawned in the context
/// of an app and thus can have similar aliases/ids
#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq, PartialOrd)]
pub struct AppId(pub String);
impl Display for AppId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for AppId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for AppId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// This default implementation should not be used in the clustered context
impl Default for AppId {
    fn default() -> Self {
        Self("default".to_string())
    }
}

/// Identifier of a Flow definition
#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
pub struct FlowDefinitionId(pub String);
impl Display for FlowDefinitionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for FlowDefinitionId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for FlowDefinitionId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// Unique identifier of a `Flow` instance within a tremor cluster
/// A flow instance is always part of an `App` and thus always needs an `AppId` to be fully qualified.
/// The `Flow` id needs to be unique within the App, regardless of the flow definition this instance is based upon.
/// An actual running instance of a flow
#[derive(Debug, PartialEq, PartialOrd, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct AppFlowInstanceId {
    app_id: AppId,
    instance_id: InstanceId,
}

impl AppFlowInstanceId {
    /// construct a new flow if from some stringy thingy
    pub fn new(app_id: impl Into<AppId>, alias: impl Into<InstanceId>) -> Self {
        Self {
            app_id: app_id.into(),
            instance_id: alias.into(),
        }
    }

    pub fn from_deploy(app_id: impl Into<AppId>, deploy: &DeployFlow) -> Self {
        Self {
            app_id: app_id.into(),
            instance_id: deploy.instance_alias.clone().into(),
        }
    }

    #[must_use]
    pub fn app_id(&self) -> &AppId {
        &self.app_id
    }

    #[must_use]
    pub fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }
}

impl std::fmt::Display for AppFlowInstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", &self.app_id, &self.instance_id)
    }
}

impl From<&str> for AppFlowInstanceId {
    fn from(value: &str) -> Self {
        Self::new(AppId::default(), value)
    }
}

/// fixed node id used for root cluster nodes that have been bootstrapping the cluster
pub const BOOTSTRAP_NODE_ID: openraft::NodeId = 0;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub(crate) enum AliasType {
    Connector,
}

impl std::fmt::Display for AliasType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connector => write!(f, "connector"),
        }
    }
}

pub(crate) trait GenericAlias {
    fn app_id(&self) -> &AppId;
    fn app_instance(&self) -> &InstanceId;
    fn alias_type(&self) -> AliasType;
    fn alias(&self) -> &str;
}
