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

use super::raw::BaseExpr;
use super::PipelineDecl;
use super::{Docs, HashMap, Value};
use crate::ast::BaseRef;
use crate::{impl_expr_mid, impl_fqsn};
use tremor_common::url::TremorUrl;

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
    pub flows: HashMap<String, FlowDecl<'script>>,
    /// Connector Definitions
    pub connectors: HashMap<String, ConnectorDecl<'script>>,
    /// Pipeline Definitions
    pub pipelines: HashMap<String, PipelineDecl<'script>>,
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
    CreateStmt(Box<CreateStmt<'script>>),
}

impl<'script> DeployStmt<'script> {
    /// Returns the user provided `id` of this statement
    #[must_use]
    pub fn id(&self) -> String {
        match self {
            DeployStmt::FlowDecl(stmt) => stmt.id.clone(),
            DeployStmt::PipelineDecl(stmt) => stmt.id.clone(),
            DeployStmt::ConnectorDecl(stmt) => stmt.id.clone(),
            DeployStmt::CreateStmt(stmt) => stmt.id.clone(),
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
    /// Resolved argument defaults
    pub params: Option<HashMap<String, Value<'script>>>,
    /// Internal / intrinsic builtin name
    pub builtin_kind: String,
    /// Documentation comments
    #[serde(skip)]
    pub docs: Option<String>,
}
impl_expr_mid!(ConnectorDecl);
impl_fqsn!(ConnectorDecl);

type DeployStmts<'script> = Vec<DeployStmt<'script>>;

/// A flow declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FlowDecl<'script> {
    pub(crate) mid: usize,
    /// Module of the connector
    pub module: Vec<String>,
    /// Identifer for the connector
    pub id: String,
    /// Resolved argument defaults
    pub params: Option<HashMap<String, Value<'script>>>,
    /// Links between artefacts in the flow
    pub links: HashMap<TremorUrl, TremorUrl>,
    /// Documentation comments
    #[serde(skip)]
    pub docs: Option<String>,
}
impl_expr_mid!(FlowDecl);
impl_fqsn!(FlowDecl);

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
    /// Atomic unit of deployment
    pub atom: FlowDecl<'script>,
    /// Documentation comments
    #[serde(skip)]
    pub docs: Option<String>,
}
impl_expr_mid!(CreateStmt);
impl_fqsn!(CreateStmt);
