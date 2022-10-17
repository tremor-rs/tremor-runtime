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

// We want to keep the names here
#![allow(clippy::module_name_repetitions)]

use tremor_common::ports::Port;

use super::{
    docs::Docs,
    helper::Scope,
    node_id::{BaseRef, NodeId},
    raw::BaseExpr,
    CreationalWith, DefinitionalArgs, DefinitionalArgsWith, HashMap, NodeMeta, PipelineDefinition,
    Value,
};
use crate::{impl_expr, impl_expr_no_lt};
pub(crate) mod raw;

/// A Tremor deployment
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Deploy<'script> {
    // TODO handle configuration directives for troy definitions
    /// Configuration directives
    pub config: HashMap<String, Value<'script>>,
    /// Statements
    pub stmts: DeployStmts<'script>,
    /// Scope
    pub scope: Scope<'script>,
    #[serde(skip)]
    /// Documentation comments
    pub docs: Docs,
    #[serde(skip)]
    mid: Box<NodeMeta>,
}

impl<'script> BaseExpr for Deploy<'script> {
    fn meta(&self) -> &crate::NodeMeta {
        self.mid.meta()
    }
}

impl<'script> Deploy<'script> {
    /// Provides a `GraphViz` dot file representation of the deployment graph
    #[must_use]
    #[allow(clippy::unused_self)]
    pub fn dot(&self) -> String {
        "todo".to_string() // TODO convert to graphviz dot file
    }
}

/// A tremor deployment language ( troy ) statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum DeployStmt<'script> {
    /// A flow definition
    FlowDefinition(Box<FlowDefinition<'script>>),
    /// A pipeline definition
    PipelineDefinition(Box<PipelineDefinition<'script>>),
    /// A connector definition
    ConnectorDefinition(Box<ConnectorDefinition<'script>>),
    /// The create instance constructor
    DeployFlowStmt(Box<DeployFlow<'script>>),
}

impl<'script> BaseRef for DeployStmt<'script> {
    /// Returns the user provided `fqn` of this statement
    #[must_use]
    fn fqn(&self) -> String {
        match self {
            DeployStmt::FlowDefinition(stmt) => stmt.id.clone(),
            DeployStmt::PipelineDefinition(stmt) => stmt.id.clone(),
            DeployStmt::ConnectorDefinition(stmt) => stmt.id.clone(),
            DeployStmt::DeployFlowStmt(stmt) => stmt.fqn(),
        }
    }
}

// #[cfg_attr(coverage, no_coverage)] // this is a simple passthrough
impl<'script> BaseExpr for DeployStmt<'script> {
    fn meta(&self) -> &NodeMeta {
        match self {
            DeployStmt::PipelineDefinition(s) => s.meta(),
            DeployStmt::ConnectorDefinition(s) => s.meta(),
            DeployStmt::FlowDefinition(s) => s.meta(),
            DeployStmt::DeployFlowStmt(s) => s.meta(),
        }
    }
}

/// A connector definition
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ConnectorDefinition<'script> {
    pub(crate) mid: Box<NodeMeta>,
    /// Identifer for the connector
    pub id: String,
    /// Resolved argument defaults
    pub params: DefinitionalArgsWith<'script>,
    /// Internal / intrinsic builtin name
    pub builtin_kind: String,
    /// The rendered config of this connector
    pub config: Value<'script>,
    /// Documentation comments
    #[serde(skip)]
    pub docs: Option<String>,
}
impl_expr!(ConnectorDefinition);

impl<'script> ConnectorDefinition<'script> {
    // check with definition of `Connector` in tremor-runtime/src/config.rs
    /// param name for connector type
    pub const CODEC: &'static str = "codec";
    /// param name for connector config
    pub const CONFIG: &'static str = "config";
    /// param name for preprocessors
    pub const PREPROCESSORS: &'static str = "preprocessors";
    /// param name for postprocessors
    pub const POSTPROCESSORS: &'static str = "postprocessors";
    /// param name for `metrics_interval_s`
    pub const METRICS_INTERVAL_S: &'static str = "metrics_interval_s";
    /// param name for reconnect configuration
    pub const RECONNECT: &'static str = "reconnect";
    /// param name for initial command
    pub const INITIAL_COMMANDS: &'static str = "initial_commands";

    const AVAILABLE_PARAMS: [&'static str; 7] = [
        Self::CODEC,
        Self::CONFIG,
        Self::METRICS_INTERVAL_S,
        Self::POSTPROCESSORS,
        Self::PREPROCESSORS,
        Self::RECONNECT,
        Self::INITIAL_COMMANDS,
    ];
}

type DeployStmts<'script> = Vec<DeployStmt<'script>>;

/// A deployment link
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum ConnectStmt {
    /// Connector to Pipeline connection
    ConnectorToPipeline {
        /// Metadata ID
        mid: Box<NodeMeta>,
        /// The instance we're connecting to
        from: DeployEndpoint,
        /// The instance being connected
        to: DeployEndpoint,
    },
    /// Pipeline to connector connection
    PipelineToConnector {
        /// Metadata ID
        mid: Box<NodeMeta>,
        /// The instance we're connecting to
        from: DeployEndpoint,
        /// The instance being connected
        to: DeployEndpoint,
    },
    /// Pipeline to Pipeline connection
    PipelineToPipeline {
        /// Metadata ID
        mid: Box<NodeMeta>,
        /// The instance we're connecting to
        from: DeployEndpoint,
        /// The instance being connected
        to: DeployEndpoint,
    },
}
impl BaseExpr for ConnectStmt {
    fn meta(&self) -> &NodeMeta {
        match self {
            ConnectStmt::ConnectorToPipeline { mid, .. }
            | ConnectStmt::PipelineToConnector { mid, .. }
            | ConnectStmt::PipelineToPipeline { mid, .. } => mid,
        }
    }
}

impl ConnectStmt {
    // we get the field called 'from'  as muttable
    #[allow(clippy::wrong_self_convention)]
    pub(crate) fn from_mut(&mut self) -> &mut DeployEndpoint {
        match self {
            ConnectStmt::ConnectorToPipeline { from, .. }
            | ConnectStmt::PipelineToConnector { from, .. }
            | ConnectStmt::PipelineToPipeline { from, .. } => from,
        }
    }
    pub(crate) fn to_mut(&mut self) -> &mut DeployEndpoint {
        match self {
            ConnectStmt::ConnectorToPipeline { to, .. }
            | ConnectStmt::PipelineToConnector { to, .. }
            | ConnectStmt::PipelineToPipeline { to, .. } => to,
        }
    }
}

/// A deployment endpoint
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
pub struct DeployEndpoint {
    /// This alias is the instance-id of the instance targeted without the containing flow id
    /// as we currently do not allow inter-flow connection, this needs to change if we do
    alias: String,
    /// Refers to a local artefact being deployed in a troy definition
    port: Port<'static>,
    #[serde(skip_serializing)]
    mid: Box<NodeMeta>,
}
impl_expr_no_lt!(DeployEndpoint);

impl std::fmt::Display for DeployEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.alias, self.port)
    }
}

impl DeployEndpoint {
    /// Creates a new endpoint
    pub fn new<A>(alias: &A, port: Port<'static>, mid: &NodeMeta) -> Self
    where
        A: ToString + ?Sized,
    {
        Self {
            alias: alias.to_string(),
            port,
            mid: Box::new(mid.clone()),
        }
    }
    /// The artefact
    #[must_use]
    pub fn alias(&self) -> &str {
        &self.alias
    }
    /// The port
    #[must_use]
    pub fn port(&self) -> &Port<'static> {
        &self.port
    }
}

/// A flow definition
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FlowDefinition<'script> {
    pub(crate) mid: Box<NodeMeta>,
    /// Identifer for the flow
    pub id: String,
    /// Resolved argument defaults
    pub params: DefinitionalArgs<'script>,
    /// Links between artefacts in the flow
    pub connections: Vec<ConnectStmt>,
    /// Deployment atoms
    pub creates: Vec<CreateStmt<'script>>,
    /// Documentation comments
    #[serde(skip)]
    pub docs: Option<String>,
}
impl_expr!(FlowDefinition);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// A connect target
pub enum CreateTargetDefinition<'script> {
    /// A connector
    Connector(ConnectorDefinition<'script>),
    /// A Pipeline
    Pipeline(Box<PipelineDefinition<'script>>),
}
/// A create statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CreateStmt<'script> {
    pub(crate) mid: Box<NodeMeta>,
    /// Target of the artefact definition being deployed
    pub from_target: NodeId,
    /// The name of the created entity (aka local alias)
    pub instance_alias: String,
    /// creational args
    pub with: CreationalWith<'script>,
    /// Atomic unit of deployment
    pub defn: CreateTargetDefinition<'script>,
}
impl_expr!(CreateStmt);
impl crate::ast::node_id::BaseRef for CreateStmt<'_> {
    fn fqn(&self) -> String {
        self.instance_alias.clone()
    }
}

/// A create statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DeployFlow<'script> {
    /// metadata id
    pub mid: Box<NodeMeta>,
    /// Target of the artefact definition being deployed
    pub from_target: NodeId,
    /// Target for creation
    pub instance_alias: String,
    /// Atomic unit of deployment
    pub defn: FlowDefinition<'script>,
    /// Documentation comments
    #[serde(skip)]
    pub docs: Option<String>,
}
impl_expr!(DeployFlow);
impl crate::ast::node_id::BaseRef for DeployFlow<'_> {
    fn fqn(&self) -> String {
        self.instance_alias.clone()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn connect_stmt_mid() {
        let mid = NodeMeta::dummy();
        let stmt = ConnectStmt::ConnectorToPipeline {
            mid: mid.clone(),
            from: DeployEndpoint::new("from", "from".into(), &mid),
            to: DeployEndpoint::new("to", "to".into(), &mid),
        };
        assert_eq!(stmt.meta(), &*mid);
        let stmt = ConnectStmt::PipelineToConnector {
            mid: mid.clone(),
            from: DeployEndpoint::new("from", "from".into(), &mid),
            to: DeployEndpoint::new("to", "to".into(), &mid),
        };
        assert_eq!(stmt.meta(), &*mid);
        let stmt = ConnectStmt::PipelineToPipeline {
            mid: mid.clone(),
            from: DeployEndpoint::new("from", "from".into(), &mid),
            to: DeployEndpoint::new("to", "to".into(), &mid),
        };
        assert_eq!(stmt.meta(), &*mid);
    }
}
