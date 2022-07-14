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

/// Types of aliases
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum Type {
    /// a connector
    Connector,
    /// a pipeline
    Pipeline,
}

impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connector => write!(f, "connector"),
            Self::Pipeline => write!(f, "pipeline"),
        }
    }
}

/// generic behaviour of an alias
pub trait Generic {
    /// the type if the alias
    fn alias_type(&self) -> Type;

    /// the alias itself
    fn alias(&self) -> &str;
}

#[derive(Clone, Debug)]
/// Precomputed prefix for logging for Soiurce context
pub struct SourceContext(String);

impl SourceContext {
    /// construct a new `ConnectorId` from the id of the containing flow and the connector instance id
    pub fn new(alias: impl Into<String>) -> Self {
        Self(alias.into())
    }
}
impl std::fmt::Display for SourceContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// An instance
#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq, PartialOrd)]
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

impl From<&FlowDefinition> for Instance {
    fn from(value: &FlowDefinition) -> Self {
        Self(value.0.clone())
    }
}
impl From<FlowDefinition> for Instance {
    fn from(value: FlowDefinition) -> Self {
        Self(value.0)
    }
}

/// An `App` is an isolated container that is defined by
/// a troy file with possibly multiple flow definitions.
/// An `App` needs to have a unique name inside a tremor cluster.
/// Flow instances (and thus connector and pipeline instances) are spawned in the context
/// of an app and thus can have similar aliases/ids
#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq, PartialOrd)]
pub struct App(pub String);
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

/// This default implementation should not be used in the clustered context
impl Default for App {
    fn default() -> Self {
        Self("default".to_string())
    }
}

/// Identifier of a Flow definition
#[derive(Debug, PartialEq, PartialOrd, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct FlowDefinition(pub String);
impl std::fmt::Display for FlowDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for FlowDefinition {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<FlowDefinition> for String {
    fn from(value: FlowDefinition) -> Self {
        value.0
    }
}

impl From<&FlowDefinition> for String {
    fn from(value: &FlowDefinition) -> Self {
        value.0.clone()
    }
}
impl From<&str> for FlowDefinition {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<&String> for FlowDefinition {
    fn from(value: &String) -> Self {
        Self(value.to_string())
    }
}

impl From<&Instance> for FlowDefinition {
    fn from(value: &Instance) -> Self {
        FlowDefinition(value.0.clone())
    }
}
impl From<Instance> for FlowDefinition {
    fn from(value: Instance) -> Self {
        FlowDefinition(value.0)
    }
}

/// Unique identifier of a `Flow` instance within a tremor cluster
/// A flow instance is always part of an `App` and thus always needs an `alias::App` to be fully qualified.
/// The `Flow` id needs to be unique within the App, regardless of the flow definition this instance is based upon.
/// An actual running instance of a flow
#[derive(Debug, PartialEq, PartialOrd, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Flow {
    app_id: App,
    instance_id: Instance,
}

impl Flow {
    /// construct a new flow if from some stringy thingy
    pub fn new(app_id: impl Into<App>, instance_id: impl Into<Instance>) -> Self {
        Self {
            app_id: app_id.into(),
            instance_id: instance_id.into(),
        }
    }

    /// Returns a reference to the appid
    #[must_use]
    pub fn app_id(&self) -> &App {
        &self.app_id
    }

    /// returns a reference to the alias
    #[must_use]
    pub fn instance_id(&self) -> &Instance {
        &self.instance_id
    }
}

impl From<&str> for Flow {
    fn from(alias: &str) -> Self {
        Self::new(App::default(), alias)
    }
}

impl From<String> for Flow {
    fn from(alias: String) -> Self {
        Self::new(App::default(), alias)
    }
}

impl std::fmt::Display for Flow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", &self.app_id, &self.instance_id)
    }
}

/// unique instance alias/id of a connector within a deployment
#[derive(Debug, PartialEq, PartialOrd, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Connector {
    connector_alias: String,
}

impl Connector {
    /// construct a new `ConnectorId` from the id of the containing flow and the connector instance id
    pub fn new(connector_alias: impl Into<String>) -> Self {
        Self {
            connector_alias: connector_alias.into(),
        }
    }

    /// get a reference to the connector alias
    #[must_use]
    pub fn connector_alias(&self) -> &str {
        self.connector_alias.as_str()
    }
}

impl std::fmt::Display for Connector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.connector_alias.fmt(f)
    }
}

impl<T> From<&T> for Connector
where
    T: ToString + ?Sized,
{
    fn from(s: &T) -> Self {
        Self {
            connector_alias: s.to_string(),
        }
    }
}

impl Generic for Connector {
    fn alias_type(&self) -> Type {
        Type::Connector
    }

    fn alias(&self) -> &str {
        &self.connector_alias
    }
}

/// unique instance alias/id of a pipeline within a deployment
#[derive(Debug, PartialEq, PartialOrd, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pipeline_alias: String,
}

impl Pipeline {
    /// construct a new `Pipeline` from the id of the containing flow and the pipeline instance id
    pub fn new(pipeline_alias: impl Into<String>) -> Self {
        Self {
            pipeline_alias: pipeline_alias.into(),
        }
    }
}
impl Generic for Pipeline {
    fn alias_type(&self) -> Type {
        Type::Pipeline
    }

    fn alias(&self) -> &str {
        &self.pipeline_alias
    }
}

impl std::fmt::Display for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.alias().fmt(f)
    }
}
