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

use super::BaseExpr;
use super::ConnectorDecl;
use super::CreateStmt;
use super::DeployEndpoint;
use super::DeployLink;
use super::FlowDecl;
use super::Value;
use crate::ast::{
    error_generic, node_id::NodeId, query::raw::PipelineDeclRaw, raw::WithExprsRaw, AggrRegistry,
    Deploy, DeployStmt, Helper, ModDoc, NodeMetas, PipelineDecl, Registry, Script, StringLit,
    Upable,
};
use crate::ast::{
    node_id::BaseRef,
    raw::{ExprRaw, IdentRaw, ModuleRaw, StringLitRaw},
};
use crate::errors::ErrorKind;
use crate::errors::Result;
use crate::impl_expr;
use crate::pos::Location;
use crate::AggrType;
use crate::EventContext;
use beef::Cow;
use tremor_common::time::nanotime;
use tremor_common::url::TremorUrl;
use tremor_value::literal;

// For compile time interpretation support
use crate::interpreter::Env;
use crate::interpreter::ExecOpts;
use crate::interpreter::LocalStack;
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

/// Evaluate an interpolated literal string at compile time with an empty state context
/// for use during compile time reduction
/// # Errors
/// If evaluation of the expression fails, or a legal value cannot be evaluated by result
pub(crate) fn run_lit_str<'script, 'registry>(
    helper: &Helper<'script, 'registry>,
    literal: &StringLit<'script>,
) -> Result<Cow<'script, str>> {
    let eo = ExecOpts {
        aggr: AggrType::Emit,
        result_needed: true,
    };
    let ctx = EventContext::new(nanotime(), None);
    let event = literal!({}).into_static();
    let state = literal!({}).into_static();
    let meta = literal!({}).into_static();

    let local = LocalStack::with_size(0);

    let run_consts = helper.consts.clone();
    let run_consts = run_consts.run();
    let env = Env {
        context: &ctx,
        consts: run_consts,
        aggrs: &helper.aggregates.clone(),
        meta: &helper.meta.clone(),
        recursion_limit: crate::recursion_limit(),
    };
    literal.run(eo, &env, &event, &state, &meta, &local)
}

#[derive(Debug, PartialEq, Serialize)]
pub struct DeployRaw<'script> {
    pub(crate) config: WithExprsRaw<'script>,
    pub(crate) stmts: DeployStmtsRaw<'script>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
}
impl<'script> DeployRaw<'script> {
    pub(crate) fn up_script<'registry>(
        self,
        mut helper: &mut Helper<'script, 'registry>,
    ) -> Result<Deploy<'script>> {
        let mut stmts = vec![];
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

        Ok(Deploy {
            config: self.config.up(helper)?,
            stmts,
            connectors: helper.connector_defns.clone(),
            pipelines: helper.pipeline_defns.clone(),
            flows: helper.flow_defns.clone(),
            docs: helper.docs.clone(),
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum DeployStmtRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    CreateStmt(CreateStmtRaw<'script>),
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
                helper.pipeline_defns.insert(stmt.fqn(), stmt.clone());
                Ok(DeployStmt::PipelineDecl(Box::new(stmt)))
            }
            DeployStmtRaw::ConnectorDecl(stmt) => {
                let stmt: ConnectorDecl<'script> = stmt.up(helper)?;
                helper.connector_defns.insert(stmt.fqn(), stmt.clone());
                Ok(DeployStmt::ConnectorDecl(Box::new(stmt)))
            }
            DeployStmtRaw::FlowDecl(stmt) => {
                let stmt: FlowDecl<'script> = stmt.up(helper)?;
                helper.flow_defns.insert(stmt.fqn(), stmt.clone());
                Ok(DeployStmt::FlowDecl(Box::new(stmt)))
            }
            DeployStmtRaw::CreateStmt(stmt) => {
                // FIXME TODO constrain to flow create's for top level
                let stmt: CreateStmt = stmt.up(helper)?;
                helper.instances.insert(stmt.fqn(), stmt.clone());
                Ok(DeployStmt::CreateStmt(Box::new(stmt)))
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
                    helper.connector_defns.insert(stmt.fqn(), stmt.clone());
                }
                DeployStmtRaw::FlowDecl(stmt) => {
                    let stmt: FlowDecl<'script> = stmt.up(helper)?;
                    helper.flow_defns.insert(stmt.fqn(), stmt.clone());
                }
                DeployStmtRaw::PipelineDecl(stmt) => {
                    let stmt: PipelineDecl<'script> = stmt.up(helper)?;
                    helper.pipeline_defns.insert(stmt.fqn(), stmt.clone());
                }
                DeployStmtRaw::CreateStmt(stmt) => {
                    let stmt: CreateStmt = stmt.up(helper)?;
                    helper.instances.insert(stmt.fqn(), stmt.clone());
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
    pub(crate) params: Option<WithExprsRaw<'script>>,
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

        let script_name = query_decl.fqn();
        helper
            .connector_defns
            .insert(script_name, query_decl.clone());
        Ok(query_decl)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FlowDeclRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) params: Option<WithExprsRaw<'script>>,
    pub(crate) docs: Option<Vec<Cow<'script, str>>>,
    pub(crate) atoms: Vec<DeployLinkRaw<'script>>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// we're forced to make this pub because of lalrpop
pub enum DeployEndpointRaw<'script> {
    Legacy(StringLitRaw<'script>),
    // TODO modular target with optional port specification - await connectors before revising
    Troy(IdentRaw<'script>, Option<IdentRaw<'script>>),
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

        let mut links = Vec::new();
        let mut atoms = Vec::new();
        for link in self.atoms {
            match link {
                DeployLinkRaw::Link(from, to) => {
                    let from = match from {
                        DeployEndpointRaw::Legacy(string_url) => {
                            let literal = string_url.up(helper)?;
                            let raw_url = run_lit_str(helper, &literal);
                            DeployEndpoint::System(TremorUrl::parse(&raw_url?.to_string())?)
                        }
                        DeployEndpointRaw::Troy(id, port) => {
                            DeployEndpoint::Troy(id.to_string(), port.map(|port| port.to_string()))
                        }
                    };
                    let to = match to {
                        DeployEndpointRaw::Legacy(string_url) => {
                            let literal = string_url.up(helper)?;
                            let raw_url = run_lit_str(helper, &literal);
                            DeployEndpoint::System(TremorUrl::parse(&raw_url?.to_string())?)
                        }
                        DeployEndpointRaw::Troy(id, port) => {
                            DeployEndpoint::Troy(id.to_string(), port.map(|port| port.to_string()))
                        }
                    };
                    links.push(DeployLink { from, to });
                }
                DeployLinkRaw::Atom(stmt) => {
                    let stmt: CreateStmt = stmt.up(helper)?;
                    atoms.push(DeployStmt::CreateStmt(Box::new(stmt)));
                }
            }
        }

        let flow_decl = FlowDecl {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            node_id: NodeId::new(self.id, helper.module.clone()),
            params: self.params.up(helper)?,
            links,
            atoms,
            docs: self
                .docs
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        };
        helper.module.pop();
        let flow_name = flow_decl.fqn();
        helper.flow_defns.insert(flow_name, flow_decl.clone());
        Ok(flow_decl)
    }
}

pub type DeployLinksRaw<'script> = Vec<DeployLinkRaw<'script>>;

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum DeployLinkRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Link(DeployEndpointRaw<'script>, DeployEndpointRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Atom(CreateStmtRaw<'script>),
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CreateStmtRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: IdentRaw<'script>,
    pub(crate) params: Option<WithExprsRaw<'script>>,
    /// Id of the definition
    pub target: IdentRaw<'script>,
    /// Module of the definition
    pub(crate) module: Vec<IdentRaw<'script>>,
    pub(crate) docs: Option<Vec<Cow<'script, str>>>,
}
impl_expr!(CreateStmtRaw);

impl<'script> Upable<'script> for CreateStmtRaw<'script> {
    type Target = CreateStmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // TODO check that names across pipeline/flow/connector definitions are unique or else hygienic error

        let target_module = self
            .module
            .iter()
            .map(|x| format!("{}::", x.to_string()))
            .collect::<Vec<String>>()
            .join("");
        let fqn = format!("{}{}", target_module, self.target.to_string());

        let atom = if let Some(artefact) = helper.flow_defns.get(&fqn) {
            artefact.clone()
        } else {
            let inner = if target_module.is_empty() {
                self.id.extent(&helper.meta)
            } else {
                crate::pos::Range(self.module[0].s(&helper.meta), self.target.e(&helper.meta))
            };
            return Err(ErrorKind::DeployArtefactNotDefined(
                self.extent(&helper.meta),
                inner.extent(&helper.meta),
                fqn,
            )
            .into());
        };

        let create_stmt = CreateStmt {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            node_id: NodeId::new(self.id.to_string(), helper.module.clone()),
            target: self.target.to_string(),
            atom: DeployStmt::FlowDecl(Box::new(atom)),
            docs: self
                .docs
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        };
        let script_name = create_stmt.fqn();
        helper.instances.insert(script_name, create_stmt.clone());

        Ok(create_stmt)
    }
}
