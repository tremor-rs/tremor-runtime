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

use super::{
    node_id::BaseRef, raw::BaseExpr, visitors::ConstFolder, CreationalWith, DefinitioalArgs,
    DefinitioalArgsWith,
};
use super::{node_id::NodeId, PipelineDefinition};
use super::{Docs, HashMap, Value};
use crate::ast::walkers::DeployWalker;
use crate::{errors::Result, impl_expr_mid, impl_fqn};
pub(crate) mod raw;

/// A Tremor deployment
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Deploy<'script> {
    // TODO handle configuration directives for troy definitions
    /// Configuration directives
    pub config: HashMap<String, Value<'script>>,
    /// Statements
    pub stmts: DeployStmts<'script>,
    /// Flow Definitions
    pub flow_decls: HashMap<NodeId, FlowDefinition<'script>>,
    /// Connector Definitions
    pub connector_decls: HashMap<NodeId, ConnectorDefinition<'script>>,
    /// Pipeline Definitions
    pub pipeline_decls: HashMap<NodeId, PipelineDefinition<'script>>,
    #[serde(skip)]
    /// Documentation comments
    pub docs: Docs,
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
    /// A flow declaration
    FlowDefinition(Box<FlowDefinition<'script>>),
    /// A pipeline declaration
    PipelineDefinition(Box<PipelineDefinition<'script>>),
    /// A connector declaration
    ConnectorDefinition(Box<ConnectorDefinition<'script>>),
    /// The create instance constructor
    DeployFlowStmt(Box<DeployFlow<'script>>),
}

impl<'script> BaseRef for DeployStmt<'script> {
    /// Returns the user provided `fqn` of this statement
    #[must_use]
    fn fqn(&self) -> String {
        match self {
            DeployStmt::FlowDefinition(stmt) => stmt.fqn(),
            DeployStmt::PipelineDefinition(stmt) => stmt.fqn(),
            DeployStmt::ConnectorDefinition(stmt) => stmt.fqn(),
            DeployStmt::DeployFlowStmt(stmt) => stmt.fqn(),
        }
    }
}

#[cfg(not(tarpaulin_include))] // this is a simple passthrough
impl<'script> BaseExpr for DeployStmt<'script> {
    fn mid(&self) -> usize {
        match self {
            DeployStmt::PipelineDefinition(s) => s.mid(),
            DeployStmt::ConnectorDefinition(s) => s.mid(),
            DeployStmt::FlowDefinition(s) => s.mid(),
            DeployStmt::DeployFlowStmt(s) => s.mid(),
        }
    }
}

/// A connector declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ConnectorDefinition<'script> {
    pub(crate) mid: usize,
    /// Identifer for the connector
    pub node_id: NodeId,
    /// Resolved argument defaults
    pub params: DefinitioalArgsWith<'script>,
    /// Internal / intrinsic builtin name
    pub builtin_kind: String,
    /// The rendered config of this connector
    pub config: Value<'script>,
    /// Documentation comments
    #[serde(skip)]
    pub docs: Option<String>,
}
impl_expr_mid!(ConnectorDefinition);
impl_fqn!(ConnectorDefinition);

type DeployStmts<'script> = Vec<DeployStmt<'script>>;

/// A deployment link
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum ConnectStmt {
    /// Connector to Pipeline connection
    ConnectorToPipeline {
        /// Metadata ID
        mid: usize,
        /// The instance we're connecting to
        from: DeployEndpoint,
        /// The instance being connected
        to: DeployEndpoint,
    },
    /// Pipeline to connector connection
    PipelineToConnector {
        /// Metadata ID
        mid: usize,
        /// The instance we're connecting to
        from: DeployEndpoint,
        /// The instance being connected
        to: DeployEndpoint,
    },
    /// Pipeline to Pipeline connection
    PipelineToPipeline {
        /// Metadata ID
        mid: usize,
        /// The instance we're connecting to
        from: DeployEndpoint,
        /// The instance being connected
        to: DeployEndpoint,
    },
}

impl ConnectStmt {
    pub(crate) fn from_mut(&mut self) -> &mut DeployEndpoint {
        match self {
            ConnectStmt::ConnectorToPipeline { from, .. } => from,
            ConnectStmt::PipelineToConnector { from, .. } => from,
            ConnectStmt::PipelineToPipeline { from, .. } => from,
        }
    }
    pub(crate) fn to_mut(&mut self) -> &mut DeployEndpoint {
        match self {
            ConnectStmt::ConnectorToPipeline { to, .. } => to,
            ConnectStmt::PipelineToConnector { to, .. } => to,
            ConnectStmt::PipelineToPipeline { to, .. } => to,
        }
    }
}

/// A deployment endpoint
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
pub struct DeployEndpoint {
    alias: String,
    /// Refers to a local artefact being deployed in a troy definition
    port: String,
}

impl std::fmt::Display for DeployEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.alias, self.port)
    }
}

impl DeployEndpoint {
    /// Creates a new endpoint
    pub fn new<A, P>(alias: A, port: P) -> Self
    where
        A: ToString,
        P: ToString,
    {
        Self {
            alias: alias.to_string(),
            port: port.to_string(),
        }
    }
    /// The artefact
    pub fn alias(&self) -> &str {
        &self.alias
    }
    /// The port
    pub fn port(&self) -> &str {
        &self.port
    }
}

/// A flow declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FlowDefinition<'script> {
    pub(crate) mid: usize,
    /// Identifer for the flow
    pub node_id: NodeId,
    /// Resolved argument defaults
    pub params: DefinitioalArgs<'script>,
    /// Links between artefacts in the flow
    pub connections: Vec<ConnectStmt>,
    /// Deployment atoms
    pub creates: Vec<CreateStmt<'script>>,
    /// Documentation comments
    #[serde(skip)]
    pub docs: Option<String>,
}
impl_expr_mid!(FlowDefinition);
impl_fqn!(FlowDefinition);

impl<'script> FlowDefinition<'script> {
    /// Take the parameters of the `define flow` statement, which at this point should have been
    /// combined with the ones from the `deploy flow` statement and propagate them down into each
    /// `create *` statement inside this flow so that the `args` section of the `create` has
    /// all occurences place and can resolve it's with satement
    ///
    /// ```text
    ///                      v
    /// deploy flow -> define flow -> create * -> define * -> <body>
    /// ```
    fn apply_args<'registry>(
        &mut self,
        helper: &mut super::Helper<'script, 'registry>,
    ) -> Result<()> {
        ConstFolder::new(helper).walk_flow_definition(self)?;
        let args = self.params.render(&helper.meta)?;
        for create in &mut self.creates {
            create.apply_args(&args, helper)?;
        }
        ConstFolder::new(helper).walk_flow_definition(self)?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// A connect target
pub enum CreateTargetDefinition<'script> {
    /// A connector
    Connector(ConnectorDefinition<'script>),
    /// A Pipeline
    Pipeline(PipelineDefinition<'script>),
}
/// A create statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CreateStmt<'script> {
    pub(crate) mid: usize,
    /// Target of the artefact definition being deployed
    pub target: NodeId,
    /// The name of the created entity
    pub node_id: NodeId,
    /// creational args
    pub with: CreationalWith<'script>,
    /// Atomic unit of deployment
    pub defn: CreateTargetDefinition<'script>,
}
impl_expr_mid!(CreateStmt);
impl_fqn!(CreateStmt);
impl<'script> CreateStmt<'script> {
    /// First we apply the passed args to the creational with, substituting any use
    /// of `args` in with with the value.
    ///
    /// Second we takle this newly substutated with and ingest it into the definitional
    /// args of the definitions this create statment reffeences./
    /// ```text
    ///                                v
    /// deploy flow -> define flow -> create * -> define * -> <body>
    /// ```

    fn apply_args<'registry>(
        &mut self,
        args: &Value<'script>,
        helper: &mut super::Helper<'script, 'registry>,
    ) -> Result<()> {
        // replace any reference to the `args` path in the `with` statement of the receate
        // with the value provided by the outer statement
        self.with.substitute_args(args, helper)?;
        // Since the `with` statement is now purely literals, we ingest the statement into
        // the definitions `args` statment next by using `ingest_creational_with`.
        // Finally we create the combined new `args` of the ingested creational with into the
        // definitional args and definiitional with to apply it to all sub statements
        match &mut self.defn {
            CreateTargetDefinition::Connector(c) => {
                // include creational with into definitional args
                c.params.ingest_creational_with(&self.with)?;
                c.config = c.params.generate_config(helper)?;
                // We are done now since there are no statements inside of a connector
                // the rendering will be handled once we deploy the pipeline and spawn
                // the connector
                Ok(())
            }
            CreateTargetDefinition::Pipeline(p) => {
                p.params.ingest_creational_with(&self.with)?;
                let args = p.params.render(&helper.meta)?;
                p.apply_args(&args, helper)
            }
        }
    }
}
/// A create statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DeployFlow<'script> {
    pub(crate) mid: usize,
    /// Target of the artefact definition being deployed
    pub target: NodeId,
    /// Target for creation
    pub node_id: NodeId,
    /// Atomic unit of deployment
    pub decl: FlowDefinition<'script>,
    /// Documentation comments
    #[serde(skip)]
    pub docs: Option<String>,
}
impl_expr_mid!(DeployFlow);
impl_fqn!(DeployFlow);
