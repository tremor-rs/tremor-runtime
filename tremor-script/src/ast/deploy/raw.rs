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

use super::ConnectorDecl;
use super::CreateStmt;
use super::FlowDecl;
use super::Value;
use super::{BaseExpr, DeployFlow};
use super::{ConnectStmt, DeployEndpoint};
use crate::ast::{
    error_generic, node_id::NodeId, query::raw::ConfigRaw, AggrRegistry, CreateTargetDecl, Deploy,
    DeployStmt, Helper, ModDoc, NodeMetas, PipelineDecl, Registry, Script, Upable,
};
use crate::ast::{
    query::raw::{CreationalWithRaw, DefinitioalArgsRaw, DefinitioalArgsWithRaw, PipelineDeclRaw},
    visitors::ConstFolder,
};
use crate::ast::{
    raw::{ExprRaw, IdentRaw, ModuleRaw},
    walkers::ImutExprWalker,
};
use crate::errors::ErrorKind;
use crate::errors::Result;
use crate::impl_expr;
use crate::pos::Location;
use crate::AggrType;
use crate::EventContext;
use beef::Cow;
use halfbrown::HashMap;
use tremor_common::time::nanotime;
use tremor_value::literal;

use crate::Return;

/// Evaluate a script expression at compile time with an empty state context
/// for use during compile time reduction
/// # Errors
/// If evaluation of the script fails, or a legal value cannot be evaluated by result
pub fn run_script<'script, 'registry>(
    helper: &Helper<'script, 'registry>,
    expr: &Script<'script>,
) -> Result<Value<'script>> {
    // We duplicate these here as it simplifies use of the macro externally
    let ctx = EventContext::new(nanotime(), None);
    let mut event = literal!({}).into_static();
    let mut state = literal!({}).into_static();
    let mut meta = literal!({}).into_static();

    match expr.run(&ctx, AggrType::Emit, &mut event, &mut state, &mut meta) {
        Ok(Return::Emit { value, .. }) => Ok(value),
        _otherwise => error_generic(
            expr,
            expr,
            &"Failed to evaluate script at compile time".to_string(),
            &helper.meta,
        ),
    }
}

#[derive(Debug, PartialEq, Serialize)]
pub struct DeployRaw<'script> {
    pub(crate) config: ConfigRaw<'script>,
    pub(crate) stmts: DeployStmtsRaw<'script>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
}
impl<'script> DeployRaw<'script> {
    pub(crate) fn up_script<'registry>(
        self,
        mut helper: &mut Helper<'script, 'registry>,
    ) -> Result<Deploy<'script>> {
        let mut stmts: Vec<DeployStmt<'script>> = vec![];
        for (_i, e) in self.stmts.into_iter().enumerate() {
            match e {
                DeployStmtRaw::ModuleStmt(m) => {
                    m.define(helper.reg, helper.aggr_reg, &mut vec![], &mut helper)?;
                }
                other => {
                    stmts.push(other.up(&mut helper)?);
                }
            }
        }

        helper.docs.module = Some(ModDoc {
            name: "self".into(),
            doc: self
                .doc
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        });

        let mut config = HashMap::new();
        for (k, mut v) in self.config.up(helper)? {
            ConstFolder::new(helper).walk_expr(&mut v)?;
            config.insert(k.to_string(), v.try_into_lit(&helper.meta)?);
        }
        Ok(Deploy {
            config,
            stmts,
            connector_decls: helper.connector_decls.clone(),
            pipeline_decls: helper.pipeline_decls.clone(),
            flow_decls: helper.flow_decls.clone(),
            docs: helper.docs.clone(),
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum DeployStmtRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    DeployFlowStmt(DeployFlowRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    FlowDecl(FlowDeclRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    ConnectorDecl(ConnectorDeclRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    PipelineDecl(PipelineDeclRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    ModuleStmt(DeployModuleStmtRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Expr(Box<ExprRaw<'script>>),
}
impl<'script> DeployStmtRaw<'script> {
    const BAD_MODULE: &'static str = "Module in wrong place error";
    const BAD_EXPR: &'static str = "Expression in wrong place error";
}

impl<'script> Upable<'script> for DeployStmtRaw<'script> {
    type Target = DeployStmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match self {
            DeployStmtRaw::PipelineDecl(stmt) => {
                let stmt: PipelineDecl<'script> = stmt.up(helper)?;
                helper
                    .pipeline_decls
                    .insert(stmt.node_id.clone(), stmt.clone());
                Ok(DeployStmt::PipelineDecl(Box::new(stmt)))
            }
            DeployStmtRaw::ConnectorDecl(stmt) => {
                let stmt: ConnectorDecl<'script> = stmt.up(helper)?;
                helper
                    .connector_decls
                    .insert(stmt.node_id.clone(), stmt.clone());
                Ok(DeployStmt::ConnectorDecl(Box::new(stmt)))
            }
            DeployStmtRaw::FlowDecl(stmt) => {
                let stmt: FlowDecl<'script> = stmt.up(helper)?;
                helper.flow_decls.insert(stmt.node_id.clone(), stmt.clone());
                Ok(DeployStmt::FlowDecl(Box::new(stmt)))
            }
            DeployStmtRaw::DeployFlowStmt(stmt) => {
                // FIXME TODO constrain to flow create's for top level
                let stmt: DeployFlow = stmt.up(helper)?;
                helper.instances.insert(stmt.node_id.clone(), stmt.clone());
                Ok(DeployStmt::DeployFlowStmt(Box::new(stmt)))
            }
            DeployStmtRaw::ModuleStmt(ref m) => {
                error_generic(m, m, &Self::BAD_MODULE, &helper.meta)
            }
            DeployStmtRaw::Expr(m) => error_generic(&*m, &*m, &Self::BAD_EXPR, &helper.meta),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct DeployModuleStmtRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub name: IdentRaw<'script>,
    pub stmts: DeployStmtsRaw<'script>,
    pub docs: Option<Vec<Cow<'script, str>>>,
}
impl_expr!(DeployModuleStmtRaw);

impl<'script> DeployModuleStmtRaw<'script> {
    pub(crate) fn define<'registry>(
        self,
        reg: &'registry Registry,
        aggr_reg: &'registry AggrRegistry,
        consts: &mut Vec<Value<'script>>,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<()> {
        helper.module.push(self.name.to_string());
        for e in self.stmts {
            match e {
                DeployStmtRaw::ModuleStmt(m) => {
                    m.define(reg, aggr_reg, consts, helper)?;
                }
                DeployStmtRaw::Expr(e) => {
                    // We create a 'fake' tremor script module to define
                    // expressions inside this module
                    let expr_m = ModuleRaw {
                        name: self.name.clone(),
                        start: self.start,
                        end: self.end,
                        doc: self.docs.clone(),
                        exprs: vec![*e],
                    };
                    // since `ModuleRaw::define` also prepends the module
                    // name we got to remove it prior to calling `define` and
                    // add it back later
                    helper.module.pop();
                    expr_m.define(helper)?;
                    helper.module.push(self.name.to_string());
                }
                DeployStmtRaw::ConnectorDecl(stmt) => {
                    let stmt: ConnectorDecl<'script> = stmt.up(helper)?;
                    helper
                        .connector_decls
                        .insert(stmt.node_id.clone(), stmt.clone());
                }
                DeployStmtRaw::FlowDecl(stmt) => {
                    let stmt: FlowDecl<'script> = stmt.up(helper)?;
                    helper.flow_decls.insert(stmt.node_id.clone(), stmt.clone());
                }
                DeployStmtRaw::PipelineDecl(stmt) => {
                    let stmt: PipelineDecl<'script> = stmt.up(helper)?;
                    helper
                        .pipeline_decls
                        .insert(stmt.node_id.clone(), stmt.clone());
                }
                DeployStmtRaw::DeployFlowStmt(stmt) => {
                    let stmt: DeployFlow = stmt.up(helper)?;
                    helper.instances.insert(stmt.node_id.clone(), stmt.clone());
                }
            }
        }
        helper.module.pop();
        Ok(())
    }
}

pub type DeployStmtsRaw<'script> = Vec<DeployStmtRaw<'script>>;

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ConnectorDeclRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) kind: IdentRaw<'script>,
    pub(crate) params: DefinitioalArgsWithRaw<'script>,
    pub(crate) docs: Option<Vec<Cow<'script, str>>>,
}

impl<'script> Upable<'script> for ConnectorDeclRaw<'script> {
    type Target = ConnectorDecl<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let query_decl = ConnectorDecl {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            params: self.params.up(helper)?,
            builtin_kind: self.kind.to_string(),
            node_id: NodeId::new(self.id, helper.module.clone()),
            docs: self
                .docs
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        };

        Ok(query_decl)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// we're forced to make this pub because of lalrpop
pub struct DeployEndpointRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    pub artefact: IdentRaw<'script>,
    /// we're forced to make this pub because of lalrpop
    pub instance: IdentRaw<'script>,
    /// we're forced to make this pub because of lalrpop
    pub port: IdentRaw<'script>,
}

impl<'script> Upable<'script> for DeployEndpointRaw<'script> {
    type Target = DeployEndpoint;
    fn up<'registry>(self, _helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(DeployEndpoint {
            artefact: self.artefact.to_string(),
            instance: self.instance.to_string(),
            port: self.port.to_string(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// we're forced to make this pub because of lalrpop
pub enum ConnectStmtRaw<'script> {
    ConnectorToPipeline {
        /// The instance we're connecting to
        start: Location,
        /// The instance we're connecting to
        end: Location,
        /// The instance we're connecting to
        from: DeployEndpointRaw<'script>,
        /// The instance being connected
        to: DeployEndpointRaw<'script>,
    },
    PipelineToConnector {
        /// The instance we're connecting to
        start: Location,
        /// The instance we're connecting to
        end: Location,
        /// The instance we're connecting to
        from: DeployEndpointRaw<'script>,
        /// The instance being connected
        to: DeployEndpointRaw<'script>,
    },
    PipelineToPipeline {
        /// The instance we're connecting to
        start: Location,
        /// The instance we're connecting to
        end: Location,
        /// The instance we're connecting to
        from: DeployEndpointRaw<'script>,
        /// The instance being connected
        to: DeployEndpointRaw<'script>,
    },
}
impl<'script> Upable<'script> for ConnectStmtRaw<'script> {
    type Target = ConnectStmt;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match self {
            ConnectStmtRaw::ConnectorToPipeline {
                start,
                end,
                from,
                to,
            } => Ok(ConnectStmt::ConnectorToPipeline {
                mid: helper.add_meta(start, end),
                from: from.up(helper)?,
                to: to.up(helper)?,
            }),
            ConnectStmtRaw::PipelineToConnector {
                start,
                end,
                from,
                to,
            } => Ok(ConnectStmt::PipelineToConnector {
                mid: helper.add_meta(start, end),
                from: from.up(helper)?,
                to: to.up(helper)?,
            }),
            ConnectStmtRaw::PipelineToPipeline {
                start,
                end,
                from,
                to,
            } => Ok(ConnectStmt::PipelineToPipeline {
                mid: helper.add_meta(start, end),
                from: from.up(helper)?,
                to: to.up(helper)?,
            }),
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FlowDeclRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) params: DefinitioalArgsRaw<'script>,
    pub(crate) docs: Option<Vec<Cow<'script, str>>>,
    pub(crate) atoms: Vec<FlowStmtRaw<'script>>,
}

impl<'script> Upable<'script> for FlowDeclRaw<'script> {
    type Target = FlowDecl<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // NOTE As we can have module aliases and/or nested modules within script definitions
        // that are private to or inline with the script - multiple script definitions in the
        // same module scope can share the same relative function/const module paths.
        //
        // We add the script name to the scope as a means to distinguish these orthogonal
        // definitions. This is achieved with the push/pop pointcut around the up() call
        // below. The actual function registration occurs in the up() call in the usual way.
        //
        helper.module.push(self.id.clone());

        let mut connections = Vec::new();
        let mut creates = Vec::new();
        for link in self.atoms {
            match link {
                FlowStmtRaw::Connect(connect) => {
                    connections.push(connect.up(helper)?);
                }
                FlowStmtRaw::Create(stmt) => {
                    creates.push(stmt.up(helper)?);
                }
            }
        }
        let mid = helper.add_meta_w_name(self.start, self.end, &self.id);
        let docs = self
            .docs
            .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n"));
        let params = self.params.up(helper)?;
        helper.module.pop();
        let node_id = NodeId::new(self.id.clone(), helper.module.clone());

        let flow_decl = FlowDecl {
            mid,
            node_id,
            params,
            connections,
            creates,
            docs,
        };
        Ok(flow_decl)
    }
}

pub type FlowStmtsRaw<'script> = Vec<FlowStmtRaw<'script>>;

/// we're forced to make this pub because of lalrpop

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum FlowStmtRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Connect(ConnectStmtRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Create(CreateStmtRaw<'script>),
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum CreateKind {
    /// Reference to a connector definition
    Connector,
    /// Reference to a pipeline definition
    Pipeline,
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CreateStmtRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: IdentRaw<'script>,
    pub(crate) params: CreationalWithRaw<'script>,
    /// Id of the definition
    pub target: IdentRaw<'script>,
    /// Module of the definition
    pub(crate) kind: CreateKind,
    pub(crate) module: Vec<IdentRaw<'script>>,
}
impl_expr!(CreateStmtRaw);

impl<'script> Upable<'script> for CreateStmtRaw<'script> {
    type Target = CreateStmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // TODO check that names across pipeline/flow/connector definitions are unique or else hygienic error

        let target_module = self
            .module
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>();
        let node_id = NodeId::new(self.target.to_string(), target_module);
        let outer = self.extent(&helper.meta);
        let inner = self.id.extent(&helper.meta);
        let params = self.params.up(helper)?;
        let decl = match self.kind {
            CreateKind::Connector => {
                if let Some(artefact) = helper.connector_decls.get(&node_id) {
                    let mut artefact = artefact.clone();
                    dbg!(&params);
                    dbg!(&artefact.params);
                    artefact.params.ingest_creational_with(&params)?;
                    dbg!(&artefact.params);
                    CreateTargetDecl::Connector(artefact)
                } else {
                    return Err(ErrorKind::DeployArtefactNotDefined(
                        outer,
                        inner,
                        node_id.to_string(),
                        helper
                            .connector_decls
                            .keys()
                            .map(ToString::to_string)
                            .collect(),
                    )
                    .into());
                }
            }
            CreateKind::Pipeline => {
                if let Some(artefact) = helper.pipeline_decls.get(&node_id) {
                    // FIXME: do we need to ingest args?
                    let artefact = artefact.clone();
                    // artefact.params.ingest_creational_with(&params)?;
                    CreateTargetDecl::Pipeline(artefact)
                } else {
                    return Err(ErrorKind::DeployArtefactNotDefined(
                        outer,
                        inner,
                        node_id.to_string(),
                        helper
                            .connector_decls
                            .keys()
                            .map(ToString::to_string)
                            .collect(),
                    )
                    .into());
                }
            }
        };

        let create_stmt = CreateStmt {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            node_id: node_id.clone(),
            alias: self.id.to_string(),
            target: self.target.to_string(),
            decl,
        };
        // helper.instances.insert(node_id, create_stmt.clone());

        Ok(create_stmt)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DeployFlowRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: IdentRaw<'script>,
    pub(crate) params: CreationalWithRaw<'script>,
    /// Id of the definition
    pub target: IdentRaw<'script>,
    /// Module of the definition - FIXME: we don't need the deploy kind here once it's merged
    pub(crate) module: Vec<IdentRaw<'script>>,
    pub(crate) docs: Option<Vec<Cow<'script, str>>>,
}
impl_expr!(DeployFlowRaw);

impl<'script> Upable<'script> for DeployFlowRaw<'script> {
    type Target = DeployFlow<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // TODO check that names across pipeline/flow/connector definitions are unique or else hygienic error

        let target_module = self
            .module
            .iter()
            .map(|x| format!("{}", x.id))
            .collect::<Vec<String>>();
        let node_id = NodeId::new(self.target.to_string(), target_module);

        let decl = if let Some(artefact) = helper.flow_decls.get(&node_id) {
            artefact.clone()
        } else {
            return Err(ErrorKind::DeployArtefactNotDefined(
                self.extent(&helper.meta),
                self.id.extent(&helper.meta),
                node_id.to_string(),
                helper.flow_decls.keys().map(ToString::to_string).collect(),
            )
            .into());
        };

        let create_stmt = DeployFlow {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            node_id: node_id.clone(),
            alias: self.id.to_string(),
            target: self.target.to_string(),
            decl,
            docs: self
                .docs
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        };

        Ok(create_stmt)
    }
}
