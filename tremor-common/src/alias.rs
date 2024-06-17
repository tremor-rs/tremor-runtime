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

use serde::{Deserialize, Serialize};

/// An `App` is an isolated container that is defined by
/// a troy file with possibly multiple flow definitions.
/// An `App` needs to have a unique name inside a tremor cluster.
/// Flow instances (and thus connector and pipeline instances) are spawned in the context
/// of an app and thus can have similar aliases/ids
#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq, PartialOrd)]
pub struct App(pub String);
impl App {
    /// construct a new `ConnectorId` from the id of the containing flow and the connector instance id
    pub fn new(alias: impl Into<String>) -> Self {
        Self(alias.into())
    }
}
impl std::fmt::Display for App {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for App {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for App {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// An instance
#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq, PartialOrd, Default)]
pub struct Instance(pub String);

impl std::fmt::Display for Instance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for Instance {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Instance {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<&String> for Instance {
    fn from(value: &String) -> Self {
        Self(value.to_string())
    }
}
impl From<&Instance> for Instance {
    fn from(value: &Instance) -> Self {
        value.clone()
    }
}

/// This default implementation should not be used in the clustered context
impl Default for App {
    fn default() -> Self {
        Self("default".to_string())
    }
}

/// unique identifier of a flow instance within a tremor instance
#[derive(Debug, PartialEq, PartialOrd, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Flow(String);

impl Flow {
    /// construct a new flow if from some stringy thingy
    pub fn new(alias: impl Into<String>) -> Self {
        Self(alias.into())
    }

    /// reference this id as a stringy thing again
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for Flow {
    fn from(e: &str) -> Self {
        Self(e.to_string())
    }
}

impl From<String> for Flow {
    fn from(alias: String) -> Self {
        Self(alias)
    }
}

impl std::fmt::Display for Flow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// unique instance alias/id of a connector within a deployment
#[derive(Debug, PartialEq, PartialOrd, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Connector {
    flow_alias: Flow,
    connector_alias: String,
}

impl Connector {
    /// construct a new `ConnectorId` from the id of the containing flow and the connector instance id
    pub fn new(flow_alias: impl Into<Flow>, connector_alias: impl Into<String>) -> Self {
        Self {
            flow_alias: flow_alias.into(),
            connector_alias: connector_alias.into(),
        }
    }

    /// get a reference to the flow alias
    #[must_use]
    pub fn flow_alias(&self) -> &Flow {
        &self.flow_alias
    }

    /// get a reference to the connector alias
    #[must_use]
    pub fn connector_alias(&self) -> &str {
        self.connector_alias.as_str()
    }
}

impl std::fmt::Display for Connector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}::{}", self.flow_alias, self.connector_alias)
    }
}

/// unique instance alias/id of a pipeline within a deployment
#[derive(Debug, PartialEq, PartialOrd, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    flow_alias: Flow,
    pipeline_alias: String,
}

impl Pipeline {
    /// construct a new `Pipeline` from the id of the containing flow and the pipeline instance id
    pub fn new(flow_alias: impl Into<Flow>, pipeline_alias: impl Into<String>) -> Self {
        Self {
            flow_alias: flow_alias.into(),
            pipeline_alias: pipeline_alias.into(),
        }
    }

    /// get a reference to the Pipeline alias
    #[must_use]
    pub fn pipeline_alias(&self) -> &str {
        self.pipeline_alias.as_str()
    }
}

impl std::fmt::Display for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}::{}", self.flow_alias, self.pipeline_alias)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn app() {
        let app = App::new("test");
        assert_eq!(app.to_string(), "test");

        let app = App::from("test2".to_string());
        assert_eq!(app.to_string(), "test2");

        let app = App::from("test3");
        assert_eq!(app.to_string(), "test3");
    }

    #[test]
    fn instance() {
        let instance = Instance::from("test");
        assert_eq!(instance.to_string(), "test");

        let instance = Instance::from("test2".to_string());
        assert_eq!(instance.to_string(), "test2");

        let instance = Instance::from("test3");
        assert_eq!(instance.to_string(), "test3");
    }

    #[test]
    fn flow() {
        let flow = Flow::new("test");
        assert_eq!(flow.to_string(), "test");
    }

    #[test]
    fn connector() {
        let flow = Flow::new("test");
        let connector = Connector::new(flow, "test");
        assert_eq!(connector.to_string(), "test::test");
    }

    #[test]
    fn pipeline() {
        let flow = Flow::new("test");
        let pipeline = Pipeline::new(flow, "test");
        assert_eq!(pipeline.to_string(), "test::test");
    }
}
