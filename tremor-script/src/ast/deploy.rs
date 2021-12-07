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

use tremor_common::url::TremorUrl;

use super::{node_id::BaseRef, raw::BaseExpr, DefinitioalArgs, DefinitioalArgsWith};
use super::{node_id::NodeId, PipelineDecl};
use super::{Docs, HashMap, Value};
use crate::{impl_expr_mid, impl_fqn};

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
    pub flow_decls: HashMap<NodeId, FlowDecl<'script>>,
    /// Connector Definitions
    pub connector_decls: HashMap<NodeId, ConnectorDecl<'script>>,
    /// Pipeline Definitions
    pub pipeline_decls: HashMap<NodeId, PipelineDecl<'script>>,
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
    FlowDecl(Box<FlowDecl<'script>>),
    /// A pipeline declaration
    PipelineDecl(Box<PipelineDecl<'script>>),
    /// A connector declaration
    ConnectorDecl(Box<ConnectorDecl<'script>>),
    /// The create instance constructor
    DeployFlowStmt(Box<DeployFlow<'script>>),
}

impl<'script> BaseRef for DeployStmt<'script> {
    /// Returns the user provided `fqn` of this statement
    #[must_use]
    fn fqn(&self) -> String {
        match self {
            DeployStmt::FlowDecl(stmt) => stmt.fqn(),
            DeployStmt::PipelineDecl(stmt) => stmt.fqn(),
            DeployStmt::ConnectorDecl(stmt) => stmt.fqn(),
            DeployStmt::DeployFlowStmt(stmt) => stmt.fqn(),
        }
    }
}

#[cfg(not(tarpaulin_include))] // this is a simple passthrough
impl<'script> BaseExpr for DeployStmt<'script> {
    fn mid(&self) -> usize {
        match self {
            DeployStmt::PipelineDecl(s) => s.mid(),
            DeployStmt::ConnectorDecl(s) => s.mid(),
            DeployStmt::FlowDecl(s) => s.mid(),
            DeployStmt::DeployFlowStmt(s) => s.mid(),
        }
    }
}

/// A connector declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ConnectorDecl<'script> {
    pub(crate) mid: usize,
    /// Identifer for the connector
    pub node_id: NodeId,
    /// Resolved argument defaults
    pub params: DefinitioalArgsWith<'script>,
    /// Internal / intrinsic builtin name
    pub builtin_kind: String,
    /// Documentation comments
    #[serde(skip)]
    pub docs: Option<String>,
}
impl_expr_mid!(ConnectorDecl);
impl_fqn!(ConnectorDecl);

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
    /// Tries to cast the connect statement into a sink connector
    pub fn as_sink_connector_url(&self) -> Option<TremorUrl> {
        if let ConnectStmt::PipelineToConnector { to, .. } = self {
            Some(TremorUrl::from_connector_instance(
                &to.artefact,
                &to.instance,
            ))
        } else {
            None
        }
    }
    /// Tries to cast the connect statement into a source connector
    pub fn as_source_connector_url(&self) -> Option<TremorUrl> {
        if let ConnectStmt::ConnectorToPipeline { from, .. } = self {
            Some(TremorUrl::from_connector_instance(
                &from.artefact,
                &from.instance,
            ))
        } else {
            None
        }
    }
    /// Tries to cast the connect statement into a vector of pipeline urls
    pub fn as_pipeline_urls(&self) -> Vec<TremorUrl> {
        match self {
            ConnectStmt::ConnectorToPipeline { to, .. } => vec![TremorUrl::from_pipeline_instance(
                &to.artefact,
                &to.instance,
            )],
            ConnectStmt::PipelineToConnector { from, .. } => {
                vec![TremorUrl::from_pipeline_instance(
                    &from.artefact,
                    &from.instance,
                )]
            }
            ConnectStmt::PipelineToPipeline { from, to, .. } => vec![
                TremorUrl::from_pipeline_instance(&from.artefact, &from.instance),
                TremorUrl::from_pipeline_instance(&to.artefact, &to.instance),
            ],
        }
    }
}

/// A deployment endpoint
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
pub struct DeployEndpoint {
    artefact: String,
    instance: String,
    /// Refers to a local artefact being deployed in a troy definition
    port: String,
}

impl DeployEndpoint {
    /// The artefact
    pub fn artefact(&self) -> &str {
        &self.artefact
    }
    /// The instance
    pub fn instance(&self) -> &str {
        &self.instance
    }
    /// The port
    pub fn port(&self) -> &str {
        &self.port
    }
    /// creates a connector instance url
    fn to_connector_instance(&self) -> TremorUrl {
        TremorUrl::from_connector_instance(&self.artefact, &self.instance)
    }
    /// creates a connector instance url
    pub fn to_connector_instance_and_port(&self) -> TremorUrl {
        let mut r = self.to_connector_instance();
        r.set_port(&self.port);
        r
    }
    /// creates a connector instance url
    fn to_pipeline_instance(&self) -> TremorUrl {
        TremorUrl::from_pipeline_instance(&self.artefact, &self.instance)
    }
    /// creates a connector instance url
    pub fn to_pipeline_instance_and_port(&self) -> TremorUrl {
        let mut r = self.to_pipeline_instance();
        r.set_port(&self.port);
        r
    }
}

/// A flow declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FlowDecl<'script> {
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
impl_expr_mid!(FlowDecl);
impl_fqn!(FlowDecl);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// A connect target
pub enum CreateTargetDecl<'script> {
    /// A connector
    Connector(ConnectorDecl<'script>),
    /// A Pipeline
    Pipeline(PipelineDecl<'script>),
}
/// A create statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CreateStmt<'script> {
    pub(crate) mid: usize,
    /// Target of the artefact definition being deployed
    pub target: NodeId,
    /// The name of the created entity
    pub node_id: NodeId,
    /// Atomic unit of deployment
    pub decl: CreateTargetDecl<'script>,
}
impl_expr_mid!(CreateStmt);
impl_fqn!(CreateStmt);

/// A create statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DeployFlow<'script> {
    pub(crate) mid: usize,
    /// Target of the artefact definition being deployed
    pub target: NodeId,
    /// Target for creation
    pub node_id: NodeId,
    /// Atomic unit of deployment
    pub decl: FlowDecl<'script>,
    /// Documentation comments
    #[serde(skip)]
    pub docs: Option<String>,
}
impl_expr_mid!(DeployFlow);
impl_fqn!(DeployFlow);
