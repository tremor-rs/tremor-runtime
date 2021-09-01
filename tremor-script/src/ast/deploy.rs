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

use super::query::raw::QueryRaw;
use super::raw::BaseExpr;
use super::{HashMap, Value};
pub use crate::ast::deploy::raw::AtomOfDeployment;
use crate::ast::Query;
use crate::ast::Script;
use crate::impl_expr_mid;
use beef::Cow;
use tremor_common::url::TremorUrl;

pub(crate) mod raw;

/// A Tremor deployment
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Deploy<'script> {
    /// Statements
    pub stmts: DeployStmts<'script>,
    /// Flow Definitions
    pub flows: HashMap<String, FlowDecl<'script>>,
    /// Connector Definitions
    pub connectors: HashMap<String, ConnectorDecl<'script>>,
    /// Pipeline Definitions
    pub pipelines: HashMap<String, PipelineDecl<'script>>,
}

impl<'script> Deploy<'script> {
    /// Provides a `GraphViz` dot file representation of the deployment graph
    #[must_use]
    #[allow(clippy::unused_self)]
    pub fn dot(&self) -> String {
        "todo".to_string() // TODO FIXME convert to graphviz dot file
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
    CreateStmt(Box<CreateStmt<'script>>),
}

#[cfg(not(tarpaulin_include))] // this is a simple passthrough
impl<'script> BaseExpr for DeployStmt<'script> {
    fn mid(&self) -> usize {
        match self {
            DeployStmt::PipelineDecl(s) => s.mid(),
            DeployStmt::ConnectorDecl(s) => s.mid(),
            DeployStmt::FlowDecl(s) => s.mid(),
            DeployStmt::CreateStmt(s) => s.mid(),
        }
    }
}

/// A connector declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ConnectorDecl<'script> {
    pub(crate) mid: usize,
    /// Module of the connector
    pub module: Vec<String>,
    /// Identifer for the connector
    pub id: String,
    /// Resolved argument specification
    pub spec: Vec<Cow<'script, str>>,
    /// Resolved argument defaults
    pub args: Value<'script>,
}
impl_expr_mid!(ConnectorDecl);

impl<'script> ConnectorDecl<'script> {
    /// Calculate the fully qualified name
    #[must_use]
    pub fn fqsn(&self, module: &[String]) -> String {
        if module.is_empty() {
            self.id.clone()
        } else {
            format!("{}::{}", module.join("::"), self.id)
        }
    }
}

type DeployStmts<'script> = Vec<DeployStmt<'script>>;

/// A pipeline query declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PipelineDecl<'script> {
    pub(crate) mid: usize,
    /// Module of the query
    pub module: Vec<String>,
    /// ID of the query
    pub id: String,
    /// Resolved argument specification
    pub spec: Vec<Cow<'script, str>>,
    /// Resolved argument defaults
    pub args: Value<'script>,
    /// The pipeline query ( before arg injection )
    pub query_raw: QueryRaw<'script>,
    /// The pipeline query ( runnable with args injected )
    pub query: Query<'script>,
}
impl_expr_mid!(PipelineDecl);

impl<'script> PipelineDecl<'script> {
    /// Calculate the fully qualified name
    #[must_use]
    pub fn fqsn(&self, module: &[String]) -> String {
        if module.is_empty() {
            self.id.clone()
        } else {
            format!("{}::{}", module.join("::"), self.id)
        }
    }
}

/// A flow declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FlowDecl<'script> {
    pub(crate) mid: usize,
    /// Module of the connector
    pub module: Vec<String>,
    /// Identifer for the connector
    pub id: String,
    /// Resolved argument specification
    pub spec: Vec<Cow<'script, str>>,
    /// Resolved argument defaults
    pub args: Value<'script>,
    /// Links between artefacts in the flow
    pub links: HashMap<TremorUrl, TremorUrl>,
}
impl_expr_mid!(FlowDecl);

impl<'script> FlowDecl<'script> {
    /// Calculate the fully qualified name
    #[must_use]
    pub fn fqsn(&self, module: &[String]) -> String {
        if module.is_empty() {
            self.id.clone()
        } else {
            format!("{}::{}", module.join("::"), self.id)
        }
    }
}

/// A create statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CreateStmt<'script> {
    pub(crate) mid: usize,
    /// Module of the artefact definition being deployed
    pub module: Vec<String>,
    /// Target of the artefact definition being deployed
    pub target: String,
    /// Identifer for the creation
    pub id: String,
    /// Script block for the instance connector
    pub script: Option<Script<'script>>,
    /// Atomic unit of deployment
    pub atom: AtomOfDeployment<'script>,
}
impl_expr_mid!(CreateStmt);

impl<'script> CreateStmt<'script> {
    /// Calculate the fully qualified name
    #[must_use]
    pub fn fqsn(&self, module: &[String]) -> String {
        if module.is_empty() {
            self.id.clone()
        } else {
            format!("{}::{}", module.join("::"), self.id)
        }
    }
}
