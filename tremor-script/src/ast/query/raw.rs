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

use std::collections::HashSet;

use super::super::raw::{ExprRaw, IdentRaw, ImutExprRaw, ModuleRaw, ScriptRaw, WithExprsRaw};
use super::{
    error_generic, error_no_consts, error_no_locals, AggrRegistry, BaseExpr, GroupBy, GroupByInt,
    HashMap, Helper, ImutExpr, Location, NodeMetas, OperatorDecl, OperatorKind, OperatorStmt,
    Query, Registry, Result, ScriptDecl, ScriptStmt, Select, SelectStmt, Serialize, Stmt,
    StreamStmt, SubqueryDecl, SubqueryStmt, Upable, Value, WindowDecl, WindowKind,
};
use crate::ast::{
    node_id::NodeId,
    visitors::{ArgsRewriter, ExprReducer, GroupByExprExtractor, TargetEventRef},
    Ident,
};
use crate::{ast::InvokeAggrFn, impl_expr};
use beef::Cow;
use std::iter::FromIterator;
use crate::ast::aggregate_fn::{AggregateDeclRaw, AggrFnDeclRaw};

#[derive(Clone, Debug, PartialEq, Serialize)]
/// we're forced to make this pub because of lalrpop
pub enum Params<'script> {
    Raw(WithExprsRaw<'script>),
    Processed(HashMap<String, Value<'script>>),
}
impl<'script> From<WithExprsRaw<'script>> for Params<'script> {
    fn from(raw: WithExprsRaw<'script>) -> Self {
        Params::Raw(raw)
    }
}
impl<'script> From<HashMap<String, Value<'script>>> for Params<'script> {
    fn from(processed: HashMap<String, Value<'script>>) -> Self {
        Params::Processed(processed)
    }
}
impl<'script> Upable<'script> for Params<'script> {
    type Target = HashMap<String, Value<'script>>;
    fn up<'registry>(
        self,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<HashMap<String, Value<'script>>> {
        match self {
            Params::Raw(params) => params
                .into_iter()
                .map(|(name, value)| {
                    Ok((name.to_string(), value.up(helper)?.try_into_value(helper)?))
                })
                .collect(),
            Params::Processed(params) => Ok(params),
        }
    }
}

#[derive(Debug, PartialEq, Serialize)]
#[allow(clippy::module_name_repetitions)]
pub struct QueryRaw<'script> {
    pub(crate) config: WithExprsRaw<'script>,
    pub(crate) stmts: StmtsRaw<'script>,
}
impl<'script> QueryRaw<'script> {
    pub(crate) fn up_script<'registry>(
        self,
        mut helper: &mut Helper<'script, 'registry>,
    ) -> Result<Query<'script>> {
        let mut stmts = vec![];

        for stmt_raw in self.stmts {
            match stmt_raw {
                StmtRaw::ModuleStmt(m) => {
                    m.define(helper.reg, helper.aggr_reg, &mut vec![], &mut helper)?;
                }
                StmtRaw::SubqueryStmt(sq_stmt_raw) => {
                    let create_stmt_index = stmts.len();
                    // Inlines all statements inside the subq inside `stmts`
                    // and returns the subquery stmt
                    let sq_stmt = sq_stmt_raw.inline(&mut stmts, &mut helper)?;

                    // Insert the subquery stmt *before* the inlined stmts.
                    // In case of duplicate subqs, this makes sure dupe subq err
                    // is triggered before dupe err from any of the inlined stmt
                    stmts.insert(create_stmt_index, Stmt::SubqueryStmt(sq_stmt));
                }
                StmtRaw::AggregateFnDecl(f) => {
                    let upped = f.up(&mut helper)?;
                    helper.register_aggregate_fun(upped.clone());
                    stmts.push(Stmt::AggregateFnDecl(upped));
                },
                other => {
                    stmts.push(other.up(&mut helper)?);
                }
            }
        }

        Ok(Query {
            config: Params::Raw(self.config).up(helper)?,
            stmts,
            node_meta: helper.meta.clone(),
            windows: helper.windows.clone(),
            scripts: helper.scripts.clone(),
            operators: helper.operators.clone(),
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum StmtRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    WindowDecl(WindowDeclRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    OperatorDecl(Box<OperatorDeclRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    ScriptDecl(ScriptDeclRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    SubqueryDecl(SubqueryDeclRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    SubqueryStmt(SubqueryStmtRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Stream(StreamStmtRaw),
    /// we're forced to make this pub because of lalrpop
    Operator(OperatorStmtRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Script(ScriptStmtRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Select(Box<SelectRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    ModuleStmt(ModuleStmtRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    AggregateFnDecl(AggrFnDeclRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Expr(Box<ExprRaw<'script>>),
}
impl<'script> StmtRaw<'script> {
    const BAD_MODULE: &'static str = "Module in wrong place error";
    const BAD_EXPR: &'static str = "Expression in wrong place error";
    const BAD_SUBQ: &'static str = "Subquery Stmt in wrong place error";
}

impl<'script> Upable<'script> for StmtRaw<'script> {
    type Target = Stmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match self {
            StmtRaw::Select(stmt) => {
                let mut aggregates = Vec::new();
                let mut locals = HashMap::new();
                helper.swap(&mut aggregates, &mut locals);
                let stmt: Select<'script> = stmt.up(helper)?;
                helper.swap(&mut aggregates, &mut locals);
                let aggregates: Vec<_> = aggregates
                    .into_iter()
                    .map(InvokeAggrFn::into_static)
                    .collect();
                // only allocate scratches if they are really needed - when we have multiple windows

                Ok(Stmt::Select(SelectStmt {
                    stmt: Box::new(stmt),
                    aggregates,
                    consts: helper.consts.clone(),
                    locals: locals.len(),
                    node_meta: helper.meta.clone(),
                }))
            }
            StmtRaw::Stream(stmt) => Ok(Stmt::Stream(stmt.up(helper)?)),
            StmtRaw::OperatorDecl(stmt) => Ok(Stmt::OperatorDecl(stmt.up(helper)?)),
            StmtRaw::Operator(stmt) => Ok(Stmt::Operator(stmt.up(helper)?)),
            StmtRaw::ScriptDecl(stmt) => Ok(Stmt::ScriptDecl(Box::new(stmt.up(helper)?))),
            StmtRaw::Script(stmt) => Ok(Stmt::Script(stmt.up(helper)?)),
            StmtRaw::SubqueryDecl(stmt) => Ok(Stmt::SubqueryDecl(stmt.up(helper)?)),
            StmtRaw::WindowDecl(stmt) => Ok(Stmt::WindowDecl(Box::new(stmt.up(helper)?))),
            StmtRaw::AggregateFnDecl(stmt) => Ok(Stmt::AggregateFnDecl(stmt.up(helper)?)),
            StmtRaw::ModuleStmt(ref m) => error_generic(m, m, &Self::BAD_MODULE, &helper.meta),
            StmtRaw::SubqueryStmt(ref sq) => error_generic(sq, sq, &Self::BAD_SUBQ, &helper.meta),
            StmtRaw::Expr(m) => error_generic(&*m, &*m, &Self::BAD_EXPR, &helper.meta),
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorDeclRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) kind: OperatorKindRaw,
    pub(crate) id: String,
    pub(crate) params: Option<Params<'script>>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
}

impl<'script> Upable<'script> for OperatorDeclRaw<'script> {
    type Target = OperatorDecl<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let operator_decl = OperatorDecl {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            node_id: NodeId::new(self.id, helper.module.clone()),
            kind: self.kind.up(helper)?,
            params: self.params.map(|raw| raw.up(helper)).transpose()?,
        };
        helper
            .operators
            .insert(operator_decl.fqn(), operator_decl.clone());
        helper.add_query_decl_doc(&operator_decl.node_id.id(), self.doc);
        Ok(operator_decl)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct SubqueryDeclRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) params: Option<Params<'script>>,
    pub(crate) subquery: StmtsRaw<'script>,
    pub(crate) from: Option<Vec<IdentRaw<'script>>>,
    pub(crate) into: Option<Vec<IdentRaw<'script>>>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
}
impl_expr!(SubqueryDeclRaw);

impl<'script> SubqueryDeclRaw<'script> {
    fn dflt_in_ports<'ident>() -> Vec<Ident<'ident>> {
        vec!["in".into()]
    }
    fn dflt_out_ports<'ident>() -> Vec<Ident<'ident>> {
        vec!["out".into(), "err".into()]
    }
}

impl<'script> Upable<'script> for SubqueryDeclRaw<'script> {
    type Target = SubqueryDecl<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let from = self.from.up(helper)?.unwrap_or_else(Self::dflt_in_ports);
        let into = self.into.up(helper)?.unwrap_or_else(Self::dflt_out_ports);

        let ports_set: HashSet<_> = from
            .iter()
            .chain(into.iter())
            .map(ToString::to_string)
            .collect();
        for stmt in &self.subquery {
            if let StmtRaw::Stream(stream_raw) = stmt {
                if ports_set.contains(&stream_raw.id) {
                    let stream = stream_raw.clone().up(helper)?;
                    let err_str = &"Streams cannot share names with from/into ports";
                    return error_generic(&stream, &stream, &err_str, &helper.meta);
                }
            }
        }

        let subquery_decl = SubqueryDecl {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            node_id: NodeId::new(self.id, helper.module.clone()),
            params: self.params.map(|raw| raw.up(helper)).transpose()?,
            raw_stmts: self.subquery,
            from,
            into,
        };
        let subquery_name = subquery_decl.fqn();
        if helper.subquery_defns.contains_key(&subquery_name) {
            let err_str = format! {"Can't define the query `{}` twice", subquery_name};
            return error_generic(&subquery_decl, &subquery_decl, &err_str, &helper.meta);
        }

        helper
            .subquery_defns
            .insert(subquery_name, subquery_decl.clone());
        helper.add_query_decl_doc(&subquery_decl.node_id.id(), self.doc);
        Ok(subquery_decl)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SubqueryStmtRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) target: String,
    pub(crate) module: Vec<IdentRaw<'script>>,
    pub(crate) params: Option<Params<'script>>,
}
impl_expr!(SubqueryStmtRaw);

impl<'script> SubqueryStmtRaw<'script> {
    fn mangle_id(&self, id: &str) -> String {
        format! {"__SUBQ__{}_{}",self.id, id}
    }

    fn get_args_map<'registry>(
        &self,
        decl_params: &Option<HashMap<String, Value<'script>>>,
        stmt_params: Option<HashMap<String, Value<'script>>>,
        helper: &Helper<'script, 'registry>,
    ) -> Result<Value<'script>> {
        let args = match &decl_params {
            // No params in the declaration
            None => Ok(HashMap::new()),
            Some(decl_params) => {
                match stmt_params {
                    // No params in the creation
                    None => Ok(decl_params.clone()),
                    // Merge elided params from the declaration.
                    Some(mut stmt_params) => {
                        for (param, value) in decl_params {
                            if !stmt_params.contains_key(param) {
                                stmt_params.insert(param.clone(), value.clone());
                            }
                        }

                        // Make sure no new params were introduced in creation
                        let stmt_param_set: HashSet<&String> =
                            stmt_params.keys().into_iter().collect();
                        let decl_param_set: HashSet<&String> =
                            decl_params.keys().into_iter().collect();
                        let mut unknown_params: Vec<&&String> =
                            stmt_param_set.difference(&decl_param_set).collect();

                        if let Some(param) = unknown_params.pop() {
                            let err_str = format! {"Unknown parameter `{}`", param};
                            error_generic(self, self, &err_str, &helper.meta)
                        } else {
                            Ok(stmt_params)
                        }
                    }
                }
            }
        }?;
        Ok(Value::from_iter(args))
    }

    fn inline_params<'registry>(
        params: Params<'script>,
        sq_args: &Value<'script>,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<HashMap<String, Value<'script>>> {
        match params {
            // Subqueries inline params before up()-ing them, the `params` here must be Raw.
            Params::Processed(_) => Err("Can't inline processed params.".into()),
            Params::Raw(params) => params
                .into_iter()
                .map(|(name, value_expr)| {
                    let mut value_expr = value_expr.up(helper)?;
                    ArgsRewriter::new(sq_args.clone(), helper).rewrite_expr(&mut value_expr)?;
                    ExprReducer::new(helper).reduce(&mut value_expr)?;
                    let value = value_expr.try_into_value(helper)?;
                    Ok((name.to_string(), value))
                })
                .collect(),
        }
    }

    #[allow(clippy::too_many_lines)]
    pub fn inline<'registry>(
        self,
        mut query_stmts: &mut Vec<Stmt<'script>>,
        mut helper: &mut Helper<'script, 'registry>,
    ) -> Result<SubqueryStmt> {
        // Calculate the fully qualified name for the subquery declaration.
        let fq_subquery_defn = if self.module.is_empty() {
            self.target.clone()
        } else {
            let module: Vec<String> = self.module.iter().map(ToString::to_string).collect();
            format!("{}::{}", module.join("::"), self.target.clone())
        };

        match helper.subquery_defns.get(&fq_subquery_defn) {
            None => error_generic(
                &self,
                &self,
                &format!("query `{}` not found", fq_subquery_defn),
                &helper.meta,
            ),
            Some(subquery_decl) => {
                let mut subquery_stmts = vec![];

                let mut ports_set: HashSet<_> = HashSet::new();
                // Map of subquery -> {ports->internal_stream}
                let mut subquery_stream_map: HashMap<String, String> = HashMap::new();

                let ports = subquery_decl.from.iter().chain(subquery_decl.into.iter());
                for port in ports {
                    let stream_stmt = StmtRaw::Stream(StreamStmtRaw {
                        start: port.s(&helper.meta),
                        end: port.e(&helper.meta),
                        id: port.id.to_string(),
                    });
                    ports_set.insert(port.id.to_string());
                    subquery_stmts.push(stream_stmt);
                }

                subquery_stmts.extend(subquery_decl.raw_stmts.clone());
                let subq_module = &self.mangle_id(&self.id);
                helper.module.push(subq_module.clone());

                let decl_params = subquery_decl.params.clone();
                let stmt_params = self.params.clone().map(|raw| raw.up(helper)).transpose()?;
                let subq_args = self.get_args_map(&decl_params, stmt_params, helper)?;

                for stmt in subquery_stmts {
                    match stmt {
                        StmtRaw::ModuleStmt(m) => {
                            m.define(helper.reg, helper.aggr_reg, &mut vec![], &mut helper)?;
                        }
                        StmtRaw::SubqueryStmt(mut s) => {
                            let unmangled_id = s.id.clone();
                            s.id = self.mangle_id(&s.id);
                            s.params = s
                                .params
                                .map(|raw| {
                                    SubqueryStmtRaw::inline_params(raw, &subq_args, &mut helper)
                                })
                                .transpose()?
                                .map(|hash_map| hash_map.into());
                            // Create a IdentRaw because we need to insert it
                            // before .inline()-ing the stmt.
                            let subq_module = IdentRaw {
                                start: s.s(&helper.meta),
                                end: s.e(&helper.meta),
                                id: subq_module.clone().into(),
                            };
                            s.module.insert(0, subq_module);
                            let s_up = s.inline(&mut query_stmts, &mut helper)?;
                            if let Some(meta) = helper.meta.nodes.get_mut(s_up.mid) {
                                meta.name = Some(unmangled_id);
                            }
                            query_stmts.push(Stmt::SubqueryStmt(s_up));
                        }
                        StmtRaw::Operator(mut o) => {
                            let unmangled_id = o.id.clone();
                            o.id = self.mangle_id(&o.id);
                            o.params = o
                                .params
                                .map(|raw| {
                                    SubqueryStmtRaw::inline_params(raw, &subq_args, &mut helper)
                                })
                                .transpose()?
                                .map(|hash_map| hash_map.into());
                            let mut o = o.up(helper)?;
                            if let Some(meta) = helper.meta.nodes.get_mut(o.mid) {
                                meta.name = Some(unmangled_id);
                            }
                            // All `define`s inside the subq are inside `subq_module`
                            o.node_id.module_mut().insert(0, subq_module.clone());
                            query_stmts.push(Stmt::Operator(o));
                        }
                        StmtRaw::Script(mut s) => {
                            let unmangled_id = s.id.clone();
                            s.id = self.mangle_id(&s.id);
                            s.params = s
                                .params
                                .map(|raw| {
                                    SubqueryStmtRaw::inline_params(raw, &subq_args, &mut helper)
                                })
                                .transpose()?
                                .map(|hash_map| hash_map.into());
                            let mut s = s.up(helper)?;
                            if let Some(meta) = helper.meta.nodes.get_mut(s.mid) {
                                meta.name = Some(unmangled_id);
                            }
                            s.node_id.module_mut().insert(0, subq_module.clone());
                            query_stmts.push(Stmt::Script(s));
                        }
                        StmtRaw::Stream(mut s) => {
                            let unmangled_id = s.id.clone();
                            s.id = self.mangle_id(&s.id);
                            let s = s.up(helper)?;
                            // Add the internal stream.id to the map of port->stream_id
                            // if the stream.id matches a port_name.
                            if let Some(port_name) = ports_set.get(&unmangled_id) {
                                subquery_stream_map.insert(port_name.clone(), s.id.clone());
                            }

                            // Store the unmangled name in the meta
                            if let Some(meta) = helper.meta.nodes.get_mut(s.mid) {
                                meta.name = Some(unmangled_id);
                            }

                            query_stmts.push(Stmt::Stream(s));
                        }
                        StmtRaw::Select(mut s) => {
                            let unmangled_from = s.from.0.id.to_string();
                            let unmangled_into = s.into.0.id.to_string();
                            s.from.0.id = Cow::owned(self.mangle_id(&s.from.0.id.to_string()));
                            s.into.0.id = Cow::owned(self.mangle_id(&s.into.0.id.to_string()));

                            if let Some(windows) = &mut s.windows {
                                for window in windows {
                                    let subq_module = IdentRaw {
                                        start: window.start,
                                        end: window.end,
                                        id: subq_module.clone().into(),
                                    };
                                    window.module.insert(0, subq_module);
                                }
                            }

                            let mut select_up = StmtRaw::Select(s).up(&mut helper)?;

                            if let Stmt::Select(s) = &mut select_up {
                                // Inline the args in target, where, having and group-by clauses
                                ArgsRewriter::new(subq_args.clone(), helper)
                                    .rewrite_expr(&mut s.stmt.target.0)?;

                                if let Some(expr) = &mut s.stmt.maybe_where {
                                    ArgsRewriter::new(subq_args.clone(), helper)
                                        .rewrite_expr(&mut expr.0)?;
                                }
                                if let Some(expr) = &mut s.stmt.maybe_having {
                                    ArgsRewriter::new(subq_args.clone(), helper)
                                        .rewrite_expr(&mut expr.0)?;
                                }
                                if let Some(group_by) = &mut s.stmt.maybe_group_by {
                                    ArgsRewriter::new(subq_args.clone(), helper)
                                        .rewrite_group_by(&mut group_by.0)?;
                                }

                                // Store the unmangled name in meta for error reports
                                if let Some(meta) = helper.meta.nodes.get_mut(s.stmt.from.0.mid()) {
                                    meta.name = Some(unmangled_from);
                                }
                                if let Some(meta) = helper.meta.nodes.get_mut(s.stmt.into.0.mid()) {
                                    meta.name = Some(unmangled_into);
                                }
                            }
                            query_stmts.push(select_up);
                        }
                        StmtRaw::ScriptDecl(mut s) => {
                            s.params = s
                                .params
                                .map(|raw| {
                                    SubqueryStmtRaw::inline_params(raw, &subq_args, &mut helper)
                                })
                                .transpose()?
                                .map(|hash_map| hash_map.into());
                            query_stmts.push(StmtRaw::ScriptDecl(s).up(&mut helper)?);
                        }
                        StmtRaw::OperatorDecl(mut o) => {
                            o.params = o
                                .params
                                .map(|raw| {
                                    SubqueryStmtRaw::inline_params(raw, &subq_args, &mut helper)
                                })
                                .transpose()?
                                .map(|hash_map| hash_map.into());
                            query_stmts.push(StmtRaw::OperatorDecl(o).up(&mut helper)?);
                        }
                        StmtRaw::SubqueryDecl(mut s) => {
                            // Inline the parent subquery's `args` in the child's `with` clause, if any.
                            s.params = s
                                .params
                                .map(|raw| {
                                    SubqueryStmtRaw::inline_params(raw, &subq_args, &mut helper)
                                })
                                .transpose()?
                                .map(|hash_map| hash_map.into());
                            query_stmts.push(StmtRaw::SubqueryDecl(s).up(&mut helper)?);
                        }
                        StmtRaw::WindowDecl(mut w) => {
                            w.params =
                                SubqueryStmtRaw::inline_params(w.params, &subq_args, &mut helper)?
                                    .into();
                            query_stmts.push(StmtRaw::WindowDecl(w).up(&mut helper)?);
                        }
                        StmtRaw::AggregateFnDecl(mut f) => {
                            query_stmts.push(StmtRaw::AggregateFnDecl(f).up(&mut helper)?)
                        }
                        StmtRaw::Expr(e) => {
                            query_stmts.push(StmtRaw::Expr(e).up(&mut helper)?);
                        }
                    }
                }
                helper.module.pop();

                let subquery_stmt = SubqueryStmt {
                    mid: helper.add_meta_w_name(self.start, self.end, &self.id),
                    module: helper.module.clone(),
                    id: self.id,
                    port_stream_map: subquery_stream_map,
                };
                Ok(subquery_stmt)
            }
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct ModuleStmtRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub name: IdentRaw<'script>,
    pub stmts: StmtsRaw<'script>,
    pub doc: Option<Vec<Cow<'script, str>>>,
}
impl_expr!(ModuleStmtRaw);

impl<'script> ModuleStmtRaw<'script> {
    const BAD_STMT: &'static str = "Can't have statements inside of query modules";
    pub(crate) fn define<'registry>(
        self,
        reg: &'registry Registry,
        aggr_reg: &'registry AggrRegistry,
        consts: &mut Vec<Value<'script>>,
        mut helper: &mut Helper<'script, 'registry>,
    ) -> Result<()> {
        helper.module.push(self.name.to_string());
        for e in self.stmts {
            match e {
                StmtRaw::ModuleStmt(m) => {
                    m.define(reg, aggr_reg, consts, helper)?;
                }
                StmtRaw::Expr(e) => {
                    // We create a 'fake' tremor script module to define
                    // expressions inside this module
                    let expr_m = ModuleRaw {
                        name: self.name.clone(),
                        start: self.start,
                        end: self.end,
                        doc: None,
                        exprs: vec![*e],
                    };
                    // since `ModuleRaw::define` also prepends the module
                    // name we got to remove it prior to calling `define` and
                    // add it back later
                    helper.module.pop();
                    expr_m.define(helper)?;
                    helper.module.push(self.name.to_string());
                }
                StmtRaw::WindowDecl(stmt) => {
                    stmt.up(&mut helper)?;
                }
                StmtRaw::ScriptDecl(stmt) => {
                    stmt.up(&mut helper)?;
                }
                StmtRaw::OperatorDecl(stmt) => {
                    stmt.up(&mut helper)?;
                }
                StmtRaw::SubqueryDecl(stmt) => {
                    stmt.up(helper)?;
                }
                ref e => {
                    return error_generic(e, e, &Self::BAD_STMT, &helper.meta);
                }
            }
        }
        helper.module.pop();
        Ok(())
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorStmtRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) module: Vec<IdentRaw<'script>>,
    pub(crate) id: String,
    pub(crate) target: String,
    pub(crate) params: Option<Params<'script>>,
}
impl_expr!(OperatorStmtRaw);

impl<'script> Upable<'script> for OperatorStmtRaw<'script> {
    type Target = OperatorStmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let module = self.module.iter().map(ToString::to_string).collect();
        Ok(OperatorStmt {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            node_id: NodeId::new(self.id, module),
            target: self.target,
            params: self.params.map(|raw| raw.up(helper)).transpose()?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptDeclRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) params: Option<Params<'script>>,
    pub(crate) script: ScriptRaw<'script>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
}

impl<'script> Upable<'script> for ScriptDeclRaw<'script> {
    type Target = ScriptDecl<'script>;
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
        let script = self.script.up_script(helper)?;

        let script_decl = ScriptDecl {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            node_id: NodeId::new(self.id, helper.module.clone()),
            params: self.params.map(|raw| raw.up(helper)).transpose()?,
            script,
        };
        helper.module.pop();

        let script_name = if helper.module.is_empty() {
            script_decl.node_id.id().to_string()
        } else {
            format!("{}::{}", helper.module.join("::"), script_decl.node_id.id())
        };

        helper.scripts.insert(script_name, script_decl.clone());
        helper.add_query_decl_doc(&script_decl.node_id.id(), self.doc);
        Ok(script_decl)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptStmtRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) target: String,
    pub(crate) module: Vec<IdentRaw<'script>>,
    pub(crate) params: Option<Params<'script>>,
}

impl<'script> Upable<'script> for ScriptStmtRaw<'script> {
    type Target = ScriptStmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let module = self.module.iter().map(ToString::to_string).collect();
        Ok(ScriptStmt {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            node_id: NodeId::new(self.id, module),
            params: self.params.map(|raw| raw.up(helper)).transpose()?,
            target: self.target,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowDeclRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) kind: WindowKind,
    pub(crate) params: Params<'script>,
    pub(crate) script: Option<ScriptRaw<'script>>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
}

impl<'script> Upable<'script> for WindowDeclRaw<'script> {
    type Target = WindowDecl<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let maybe_script = self.script.map(|s| s.up_script(helper)).transpose()?;

        // warn params if `emit_empty_windows` is defined, but neither `max_groups` nor `evicition_period` is defined

        let window_decl = WindowDecl {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            node_id: NodeId::new(self.id, helper.module.clone()),
            kind: self.kind,
            params: self.params.up(helper)?,
            script: maybe_script,
        };

        helper
            .windows
            .insert(window_decl.fqn(), window_decl.clone());
        helper.add_query_decl_doc(&window_decl.node_id.id(), self.doc);
        Ok(window_decl)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowDefnRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    /// Module of the window definition
    pub module: Vec<IdentRaw<'script>>,
    /// Identity of the window definition
    pub id: String,
}

impl<'script> WindowDefnRaw<'script> {
    /// Calculate fully qualified module name
    pub fn fqwn(&self) -> String {
        if self.module.is_empty() {
            self.id.clone()
        } else {
            let parts: Vec<_> = self.module.iter().map(ToString::to_string).collect();
            format!("{}::{}", parts.join("::"), self.id)
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SelectRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) from: (IdentRaw<'script>, Option<IdentRaw<'script>>),
    pub(crate) into: (IdentRaw<'script>, Option<IdentRaw<'script>>),
    pub(crate) target: ImutExprRaw<'script>,
    pub(crate) maybe_where: Option<ImutExprRaw<'script>>,
    pub(crate) maybe_having: Option<ImutExprRaw<'script>>,
    pub(crate) maybe_group_by: Option<GroupByRaw<'script>>,
    pub(crate) windows: Option<Vec<WindowDefnRaw<'script>>>,
}
impl_expr!(SelectRaw);

impl<'script> Upable<'script> for SelectRaw<'script> {
    type Target = Select<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let const_count = helper.consts.len();

        let mut target = self.target.up(helper)?;

        if helper.has_locals() {
            return error_no_locals(&(self.start, self.end), &target, &helper.meta);
        };

        let maybe_having = self.maybe_having.up(helper)?;
        if helper.has_locals() {
            if let Some(definitely) = maybe_having {
                return error_no_locals(&(self.start, self.end), &definitely, &helper.meta);
            }
        };
        // We ensure that no new constatns were added
        if const_count != helper.consts.len() {
            return error_no_consts(&(self.start, self.end), &target, &helper.meta);
        }

        let maybe_where = self.maybe_where.up(helper)?;
        if helper.has_locals() {
            if let Some(definitely) = maybe_where {
                return error_no_locals(&(self.start, self.end), &definitely, &helper.meta);
            }
        };
        let maybe_group_by = self.maybe_group_by.up(helper)?;
        if helper.has_locals() {
            if let Some(definitely) = maybe_group_by {
                return error_no_locals(&(self.start, self.end), &definitely, &helper.meta);
            }
        };

        // check if target has references to event that are not inside an aggregate function.
        // if so, we need to clone the event and keep it around to evaluate those expressions
        // as during signal ticks we might not have an event around (don't ask, also this is only temporary)
        let group_by_expressions = if let Some(group_by) = maybe_group_by.as_ref() {
            let mut extractor = GroupByExprExtractor::new();
            extractor.extract_expressions(group_by);
            extractor.expressions
        } else {
            vec![]
        };
        let windows = self.windows.unwrap_or_default();
        if !windows.is_empty() {
            // if we have windows we need to forbid free event references in the target if they are not
            // inside an aggregate function or can be rewritten to a group reference
            TargetEventRef::new(group_by_expressions, &helper.meta).rewrite_target(&mut target)?;
        }

        let from = match self.from {
            (stream, None) => {
                let mut port = stream.clone();
                port.id = Cow::from("out");
                (stream, port)
            }
            (stream, Some(port)) => (stream, port),
        };
        let into = match self.into {
            (stream, None) => {
                let mut port = stream.clone();
                port.id = Cow::from("in");
                (stream, port)
            }
            (stream, Some(port)) => (stream, port),
        };
        Ok(Select {
            mid: helper.add_meta(self.start, self.end),
            from: (from.0.up(helper)?, from.1.up(helper)?),
            into: (into.0.up(helper)?, into.1.up(helper)?),
            target: ImutExpr(target),
            maybe_where: maybe_where.map(ImutExpr),
            maybe_having: maybe_having.map(ImutExpr),
            maybe_group_by,
            windows,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum GroupByRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Expr {
        /// we're forced to make this pub because of lalrpop
        start: Location,
        /// we're forced to make this pub because of lalrpop
        end: Location,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    Set {
        /// we're forced to make this pub because of lalrpop
        start: Location,
        /// we're forced to make this pub because of lalrpop
        end: Location,
        /// we're forced to make this pub because of lalrpop
        items: Vec<GroupByRaw<'script>>,
    },
    /// we're forced to make this pub because of lalrpop
    Each {
        /// we're forced to make this pub because of lalrpop
        start: Location,
        /// we're forced to make this pub because of lalrpop
        end: Location,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
    },
}

impl<'script> Upable<'script> for GroupByRaw<'script> {
    type Target = GroupBy<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(match self {
            GroupByRaw::Expr { start, end, expr } => GroupBy(GroupByInt::Expr {
                mid: helper.add_meta(start, end),
                expr: expr.up(helper)?,
            }),
            GroupByRaw::Each { start, end, expr } => GroupBy(GroupByInt::Each {
                mid: helper.add_meta(start, end),
                expr: expr.up(helper)?,
            }),
            GroupByRaw::Set { start, end, items } => GroupBy(GroupByInt::Set {
                mid: helper.add_meta(start, end),
                items: items.up(helper)?,
            }),
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorKindRaw {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) module: String,
    pub(crate) operation: String,
}

impl<'script> Upable<'script> for OperatorKindRaw {
    type Target = OperatorKind;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(OperatorKind {
            mid: helper.add_meta_w_name(
                self.start,
                self.end,
                &format!("{}::{}", self.module, self.operation),
            ),
            module: self.module,
            operation: self.operation,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StreamStmtRaw {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
}
impl<'script> Upable<'script> for StreamStmtRaw {
    type Target = StreamStmt;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(StreamStmt {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            id: self.id,
        })
    }
}

pub type StmtsRaw<'script> = Vec<StmtRaw<'script>>;
