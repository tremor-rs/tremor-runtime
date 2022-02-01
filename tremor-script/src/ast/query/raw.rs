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

use super::{
    super::raw::{IdentRaw, ImutExprRaw, ScriptRaw},
    ArgsExprs, CreationalWith, DefinitioalArgs, DefinitioalArgsWith, WithExprs,
};
use super::{
    error_generic, error_no_locals, BaseExpr, GroupBy, HashMap, Helper, Location, OperatorCreate,
    OperatorDefinition, OperatorKind, PipelineCreate, PipelineDefinition, Query, Result,
    ScriptCreate, ScriptDefinition, Select, SelectStmt, Serialize, Stmt, StreamStmt, Upable, Value,
    WindowDefinition, WindowKind,
};
use crate::{
    ast::{
        node_id::{BaseRef, NodeId},
        visitors::{ArgsRewriter, ConstFolder, GroupByExprExtractor, TargetEventRef},
        walkers::{ImutExprWalker, QueryWalker},
        Consts, Ident,
    },
    errors::err_generic,
};
use crate::{
    ast::{raw::TopLevelExprRaw, InvokeAggrFn},
    impl_expr,
};
use beef::Cow;
use std::iter::FromIterator;

#[derive(Clone, Debug, PartialEq, Serialize)]
#[allow(clippy::module_name_repetitions)]
pub struct QueryRaw<'script> {
    pub(crate) config: ConfigRaw<'script>,
    pub(crate) stmts: StmtsRaw<'script>,
    pub(crate) params: DefinitioalArgsRaw<'script>,
}
impl<'script> QueryRaw<'script> {
    pub(crate) fn up_script<'registry>(
        self,
        mut helper: &mut Helper<'script, 'registry>,
    ) -> Result<Query<'script>> {
        let mut stmts = vec![];
        let params = self.params.up(helper)?;
        let args = params.render(&helper.meta)?;

        for stmt_raw in self.stmts {
            match stmt_raw {
                // A subquert that is going to be inlined
                StmtRaw::PipelineCreate(sq_stmt_raw) => {
                    let create_stmt_index = stmts.len();
                    // Inlines all statements inside the subq inside `stmts`
                    // and returns the pipeline stmt
                    let sq_stmt = sq_stmt_raw.inline(&mut stmts, &mut helper, Some(&args))?;

                    // Insert the pipeline stmt *before* the inlined stmts.
                    // In case of duplicate subqs, this makes sure dupe subq err
                    // is triggered before dupe err from any of the inlined stmt
                    stmts.insert(create_stmt_index, Stmt::PipelineCreate(sq_stmt));
                }

                other => {
                    stmts.push(other.up(&mut helper)?);
                }
            }
        }
        for stmt in &mut stmts {
            ConstFolder::new(helper).walk_stmt(stmt)?;
        }
        let mut config = HashMap::new();
        for (k, mut v) in self.config.up(helper)? {
            ConstFolder::new(helper).walk_expr(&mut v)?;
            config.insert(k.to_string(), v.try_into_lit(&helper.meta)?);
        }
        todo!()
        // Ok(Query {
        //     params,
        //     config,
        //     stmts,
        //     node_meta: helper.meta.clone(),
        //     // windows: helper.windows.clone(),
        //     // scripts: helper.scripts.clone(),
        //     // operators: helper.operators.clone(),
        //     consts: helper.consts.clone(),
        //     windows: todo!(),
        //     scripts: todo!(),
        //     operators: todo!(),
        // })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum StmtRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    WindowDefinition(WindowDefinitionRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    OperatorDefinition(Box<OperatorDefinitionRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    ScriptDefinition(ScriptDefinitionRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    PipelineDefinition(PipelineDefinitionRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    PipelineCreate(PipelineInlineRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    StreamStmt(StreamStmtRaw),
    /// we're forced to make this pub because of lalrpop
    OperatorCreate(OperatorCreateRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    ScriptCreate(ScriptCreateRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    SelectStmt(Box<SelectRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Expr(Box<TopLevelExprRaw<'script>>),
}
impl<'script> StmtRaw<'script> {
    const BAD_EXPR: &'static str = "Expression in wrong place error";
    const BAD_SUBQ: &'static str = "Pipeline Stmt in wrong place error";
}

impl<'script> Upable<'script> for StmtRaw<'script> {
    type Target = Stmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match self {
            StmtRaw::SelectStmt(stmt) => {
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

                Ok(Stmt::SelectStmt(SelectStmt {
                    stmt: Box::new(stmt),
                    aggregates,
                    consts: Consts::new(),
                    locals: locals.len(),
                    node_meta: helper.meta.clone(),
                }))
            }
            StmtRaw::StreamStmt(stmt) => Ok(Stmt::StreamStmt(stmt.up(helper)?)),
            StmtRaw::OperatorDefinition(stmt) => {
                let _stmt: OperatorDefinition<'script> = stmt.up(helper)?;
                todo!();
                //FIXME: helper.operators.insert(stmt.fqn(), stmt.clone());
                // Ok(Stmt::OperatorDefinition(stmt))
            }
            StmtRaw::OperatorCreate(stmt) => Ok(Stmt::OperatorCreate(stmt.up(helper)?)),
            StmtRaw::ScriptDefinition(stmt) => {
                let _stmt: ScriptDefinition<'script> = stmt.up(helper)?;
                todo!();
                //FIXME: helper.scripts.insert(stmt.fqn(), stmt.clone());
                // Ok(Stmt::ScriptDefinition(Box::new(stmt)))
            }
            StmtRaw::ScriptCreate(stmt) => Ok(Stmt::ScriptCreate(stmt.up(helper)?)),
            StmtRaw::PipelineDefinition(stmt) => {
                Ok(Stmt::PipelineDefinition(Box::new(stmt.up(helper)?)))
            }
            StmtRaw::WindowDefinition(stmt) => {
                let _stmt: WindowDefinition<'script> = stmt.up(helper)?;
                todo!();
                //FIXME: helper.windows.insert(stmt.fqn(), stmt.clone());
                // Ok(Stmt::WindowDefinition(Box::new(stmt)))
            }
            StmtRaw::PipelineCreate(ref sq) => error_generic(sq, sq, &Self::BAD_SUBQ, &helper.meta),
            StmtRaw::Expr(m) => error_generic(&*m, &*m, &Self::BAD_EXPR, &helper.meta),
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorDefinitionRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) kind: OperatorKindRaw,
    pub(crate) id: String,
    pub(crate) params: DefinitioalArgsWithRaw<'script>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
}
impl_expr!(OperatorDefinitionRaw);

impl<'script> Upable<'script> for OperatorDefinitionRaw<'script> {
    type Target = OperatorDefinition<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let _operator_decl = OperatorDefinition {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            node_id: NodeId::new(self.id, &[]),
            kind: self.kind.up(helper)?,
            params: self.params.up(helper)?,
        };
        todo!();
        //FIXME: helper
        // .operators
        // .insert(operator_decl.fqn(), operator_decl.clone());
        // helper.add_query_decl_doc(&operator_decl.node_id.id(), self.doc);
        // Ok(operator_decl)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct PipelineDefinitionRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) config: ConfigRaw<'script>,
    pub(crate) params: DefinitioalArgsRaw<'script>,
    pub(crate) pipeline: StmtsRaw<'script>,
    pub(crate) from: Option<Vec<IdentRaw<'script>>>,
    pub(crate) into: Option<Vec<IdentRaw<'script>>>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
}
impl_expr!(PipelineDefinitionRaw);

impl<'script> PipelineDefinitionRaw<'script> {
    const STREAM_PORT_CONFILCT: &'static str = "Streams cannot share names with from/into ports";
    fn dflt_in_ports<'ident>() -> Vec<Ident<'ident>> {
        vec!["in".into()]
    }
    fn dflt_out_ports<'ident>() -> Vec<Ident<'ident>> {
        vec!["out".into(), "err".into()]
    }
}

impl<'script> Upable<'script> for PipelineDefinitionRaw<'script> {
    type Target = PipelineDefinition<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        helper.enter_scope();

        let query = None;

        let from = self.from.up(helper)?.unwrap_or_else(Self::dflt_in_ports);
        let into = self.into.up(helper)?.unwrap_or_else(Self::dflt_out_ports);

        let ports_set: HashSet<_> = from
            .iter()
            .chain(into.iter())
            .map(ToString::to_string)
            .collect();
        for stmt in &self.pipeline {
            if let StmtRaw::StreamStmt(stream_raw) = stmt {
                if ports_set.contains(&stream_raw.id) {
                    let stream = stream_raw.clone().up(helper)?;
                    return error_generic(
                        &stream,
                        &stream,
                        &Self::STREAM_PORT_CONFILCT,
                        &helper.meta,
                    );
                }
            }
        }

        let mid = helper.add_meta_w_name(self.start, self.end, &self.id);
        helper.leave_scope()?;
        let params = self.params.up(helper)?;

        let pipeline_decl = PipelineDefinition {
            raw_config: self.config,
            mid,
            node_id: NodeId::new(self.id, &[]),
            params,
            raw_stmts: self.pipeline,
            from,
            into,
            query,
        };

        let _pipeline_name = pipeline_decl.fqn();
        // FIXME:
        // if helper.queries.contains_key(&pipeline_name) {
        //     let err_str = format!("Can't define the pipeline `{}` twice", pipeline_name);
        //     return error_generic(&pipeline_decl, &pipeline_decl, &err_str, &helper.meta);
        // }

        // FIXME: helper.queries.insert(pipeline_name, pipeline_decl.clone());
        helper.add_query_decl_doc(&pipeline_decl.fqn(), self.doc);
        Ok(pipeline_decl)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PipelineInlineRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) target: NodeId,
    pub(crate) params: CreationalWithRaw<'script>,
}
impl_expr!(PipelineInlineRaw);

impl<'script> PipelineInlineRaw<'script> {
    fn mangle_id(&self, id: &str) -> String {
        format!("__PIP__id:{}_alias:{}", self.id, id)
    }

    fn get_args_map<'registry>(
        &self,
        decl_params: &DefinitioalArgs<'script>,
        stmt_params: CreationalWith<'script>,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<Value<'script>> {
        let mut stmt_params: HashMap<_, _> = stmt_params
            .with
            .0
            .into_iter()
            .map(|(k, v)| {
                let v = ConstFolder::reduce_to_val(helper, v)?;
                Ok((k.id, v))
            })
            .collect::<Result<_>>()?;
        for (param, value) in &decl_params.args.0 {
            if !stmt_params.contains_key(&param.id) {
                let value = value
                    .clone()
                    .map(|v| ConstFolder::reduce_to_val(helper, v))
                    .transpose()?;
                if let Some(value) = value {
                    stmt_params.insert(param.id.clone(), value);
                } else {
                    return Err("missing parameter".into()); // FIXME: good error
                }
            }
        }

        // Make sure no new params were introduced in creation
        let stmt_param_set: HashSet<_> = stmt_params.keys().into_iter().collect();
        let decl_param_set: HashSet<_> = decl_params.args.0.iter().map(|(k, _)| &k.id).collect();
        let mut unknown_params: Vec<_> = stmt_param_set.difference(&decl_param_set).collect();

        if let Some(param) = unknown_params.pop() {
            let err_str = format!("Unknown parameter `{}`", param);
            error_generic(self, self, &err_str, &helper.meta)
        } else {
            Ok(Value::from_iter(stmt_params))
        }
    }

    #[allow(clippy::too_many_lines)]
    pub fn inline<'registry>(
        self,
        mut query_stmts: &mut Vec<Stmt<'script>>,
        mut helper: &mut Helper<'script, 'registry>,
        args: Option<&Value<'script>>,
    ) -> Result<PipelineCreate> {
        let target = self.target.clone().with_prefix(&[]); // FIXME
                                                           // Calculate the fully qualified name for the pipeline declaration.
                                                           // let fq_pipeline_defn = target.fqn();

        let pipeline_decl = helper.get_pipeline(&target).ok_or_else(|| {
            err_generic(
                &self,
                &self,
                &format!(
                    "pipeline `{}` not found ",
                    target,
                    //helper.queries.keys().cloned().collect::<Vec<_>>().join(",")
                ),
                &helper.meta,
            )
        })?;

        let mut pipeline_stmts = vec![];

        let mut ports_set: HashSet<_> = HashSet::new();
        // Map of pipeline -> {ports->internal_stream}
        let mut pipeline_stream_map: HashMap<String, String> = HashMap::new();

        let ports = pipeline_decl.from.iter().chain(pipeline_decl.into.iter());
        for port in ports {
            let stream_stmt = StmtRaw::StreamStmt(StreamStmtRaw {
                start: port.s(&helper.meta),
                end: port.e(&helper.meta),
                id: port.id.to_string(),
            });
            ports_set.insert(port.id.to_string());
            pipeline_stmts.push(stream_stmt);
        }

        pipeline_stmts.extend(pipeline_decl.raw_stmts.clone());
        let subq_module = &self.mangle_id(&self.id);

        let mut decl_params = pipeline_decl.params.clone();
        let mut stmt_params = self.params.clone().up(helper)?;

        if let Some(args) = args {
            ArgsRewriter::new(args.clone(), helper).walk_definitinal_args(&mut decl_params)?;
            ArgsRewriter::new(args.clone(), helper).walk_creational_with(&mut stmt_params)?;
        }
        ConstFolder::new(helper).walk_definitinal_args(&mut decl_params)?;
        ConstFolder::new(helper).walk_creational_with(&mut stmt_params)?;

        let subq_args = self.get_args_map(&decl_params, stmt_params, helper)?;

        // we need to resolve the constant folder outside of this module
        helper.enter_scope();
        for stmt in pipeline_stmts {
            match stmt {
                StmtRaw::PipelineCreate(mut s) => {
                    let unmangled_id = s.id.clone();
                    s.id = self.mangle_id(&s.id);

                    let s_up = s.inline(&mut query_stmts, &mut helper, Some(&subq_args))?;
                    if let Some(meta) = helper.meta.nodes.get_mut(s_up.mid) {
                        meta.name = Some(unmangled_id);
                    }
                    query_stmts.push(Stmt::PipelineCreate(s_up));
                }
                StmtRaw::OperatorCreate(mut o) => {
                    let unmangled_id = o.id.clone();
                    o.id = self.mangle_id(&o.id);
                    let mut o = o.up(helper)?;
                    o.params.substitute_args(&subq_args, &mut helper)?;
                    if let Some(meta) = helper.meta.nodes.get_mut(o.mid) {
                        meta.name = Some(unmangled_id);
                    }
                    // All `define`s inside the subq are inside `subq_module`
                    o.node_id.module_mut().insert(0, subq_module.clone());
                    query_stmts.push(Stmt::OperatorCreate(o));
                }
                StmtRaw::ScriptCreate(mut s) => {
                    let unmangled_id = s.id.clone();
                    s.id = self.mangle_id(&s.id);
                    let mut s = s.up(helper)?;
                    s.params.substitute_args(&subq_args, &mut helper)?;
                    if let Some(meta) = helper.meta.nodes.get_mut(s.mid) {
                        meta.name = Some(unmangled_id);
                    }
                    s.node_id.module_mut().insert(0, subq_module.clone());
                    query_stmts.push(Stmt::ScriptCreate(s));
                }
                StmtRaw::StreamStmt(mut s) => {
                    let unmangled_id = s.id.clone();
                    s.id = self.mangle_id(&s.id);
                    let s = s.up(helper)?;
                    // Add the internal stream.id to the map of port->stream_id
                    // if the stream.id matches a port_name.
                    if let Some(port_name) = ports_set.get(&unmangled_id) {
                        pipeline_stream_map.insert(port_name.clone(), s.id.clone());
                    }

                    // Store the unmangled name in the meta
                    if let Some(meta) = helper.meta.nodes.get_mut(s.mid) {
                        meta.name = Some(unmangled_id);
                    }
                    query_stmts.push(Stmt::StreamStmt(s));
                }
                StmtRaw::SelectStmt(mut s) => {
                    let unmangled_from = s.from.0.id.to_string();
                    let unmangled_into = s.into.0.id.to_string();
                    s.from.0.id = Cow::owned(self.mangle_id(&s.from.0.id.to_string()));
                    s.into.0.id = Cow::owned(self.mangle_id(&s.into.0.id.to_string()));

                    let mut select_up = StmtRaw::SelectStmt(s).up(&mut helper)?;

                    if let Stmt::SelectStmt(s) = &mut select_up {
                        // Inline the args in target, where, having and group-by clauses
                        ArgsRewriter::new(subq_args.clone(), helper)
                            .rewrite_expr(&mut s.stmt.target)?;

                        if let Some(expr) = &mut s.stmt.maybe_where {
                            ArgsRewriter::new(subq_args.clone(), helper).rewrite_expr(expr)?;
                        }
                        if let Some(expr) = &mut s.stmt.maybe_having {
                            ArgsRewriter::new(subq_args.clone(), helper).rewrite_expr(expr)?;
                        }
                        if let Some(group_by) = &mut s.stmt.maybe_group_by {
                            ArgsRewriter::new(subq_args.clone(), helper)
                                .rewrite_group_by(group_by)?;
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
                StmtRaw::ScriptDefinition(d) => {
                    let mut d = d.up(&mut helper)?;
                    d.params.substitute_args(&subq_args, helper)?;
                    // We overwrite the original script with the substituted one
                    // FIXME: helper.scripts.insert(d.node_id.fqn(), d.clone());
                    query_stmts.push(Stmt::ScriptDefinition(Box::new(d)));
                }
                StmtRaw::OperatorDefinition(d) => {
                    let mut d = d.up(&mut helper)?;
                    d.params.substitute_args(&subq_args, &mut helper)?;
                    // We overwrite the original script with the substituted one
                    // FIXME: helper.operators.insert(d.node_id.fqn(), d.clone());
                    query_stmts.push(Stmt::OperatorDefinition(d));
                }
                StmtRaw::PipelineDefinition(d) => {
                    let mut d = d.up(&mut helper)?;
                    d.params.substitute_args(&subq_args, &mut helper)?;
                    query_stmts.push(Stmt::PipelineDefinition(Box::new(d)));
                }
                StmtRaw::WindowDefinition(d) => {
                    let mut d = d.up(&mut helper)?;
                    d.params.substitute_args(&subq_args, &mut helper)?;
                    query_stmts.push(Stmt::WindowDefinition(Box::new(d)));
                }
                StmtRaw::Expr(e) => {
                    query_stmts.push(StmtRaw::Expr(e).up(&mut helper)?);
                }
            }
        }
        helper.leave_scope()?;

        let pipeline_stmt = PipelineCreate {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            module: vec![], // FIXME
            id: self.id,
            port_stream_map: pipeline_stream_map,
        };
        Ok(pipeline_stmt)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorCreateRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) target: NodeId,
    pub(crate) params: CreationalWithRaw<'script>,
}
impl_expr!(OperatorCreateRaw);

impl<'script> Upable<'script> for OperatorCreateRaw<'script> {
    type Target = OperatorCreate<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let params = self.params.up(helper)?;
        Ok(OperatorCreate {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            node_id: NodeId::new(self.id, &[]),
            target: self.target.with_prefix(&[]), // FIXME
            params,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptDefinitionRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) params: DefinitioalArgsRaw<'script>,
    pub(crate) script: ScriptRaw<'script>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
}
impl_expr!(ScriptDefinitionRaw);

impl<'script> Upable<'script> for ScriptDefinitionRaw<'script> {
    type Target = ScriptDefinition<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // NOTE As we can have module aliases and/or nested modules within script definitions
        // that are private to or inline with the script - multiple script definitions in the
        // same module scope can share the same relative function/const module paths.
        //
        // We add the script name to the scope as a means to distinguish these orthogonal
        // definitions. This is achieved with the push/pop pointcut around the up() call
        // below. The actual function registration occurs in the up() call in the usual way.
        //

        // Handle the content of the script in it's own module
        helper.enter_scope();
        let script = self.script.up_script(helper)?;
        let mid = helper.add_meta_w_name(self.start, self.end, &self.id);
        helper.leave_scope()?;
        // Handle the params in the outside module
        let params = self.params;
        let params = params.up(helper)?;
        let node_id = NodeId::new(&self.id, &[]); // FIXME

        let script_decl = ScriptDefinition {
            mid,
            node_id,
            params,
            script,
        };

        // FIXME: helper
        // .scripts
        // .insert(script_decl.node_id.fqn(), script_decl.clone());
        helper.add_query_decl_doc(&script_decl.node_id.id(), self.doc);
        Ok(script_decl)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptCreateRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) target: NodeId,
    pub(crate) params: CreationalWithRaw<'script>,
}

impl<'script> Upable<'script> for ScriptCreateRaw<'script> {
    type Target = ScriptCreate<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let params = self.params.up(helper)?;
        Ok(ScriptCreate {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            node_id: NodeId::new(self.id, &[]),
            params,
            target: self.target.with_prefix(&[]), // FIXME
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowDefinitionRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) kind: WindowKind,
    // Yes it is odd that we use the `creational` here but windows are not crates
    // and defined like other constructs - perhaps we should revisit this?
    pub(crate) params: CreationalWithRaw<'script>,
    pub(crate) script: Option<ScriptRaw<'script>>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
}
impl_expr!(WindowDefinitionRaw);

impl<'script> Upable<'script> for WindowDefinitionRaw<'script> {
    type Target = WindowDefinition<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let maybe_script = self.script.map(|s| s.up_script(helper)).transpose()?;

        // warn params if `emit_empty_windows` is defined, but neither `max_groups` nor `evicition_period` is defined

        let window_decl = WindowDefinition {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            node_id: NodeId::new(self.id, &[]),
            kind: self.kind,
            params: self.params.up(helper)?,
            script: maybe_script,
        };

        // FIXME: helper
        // .windows
        // .insert(window_decl.fqn(), window_decl.clone());
        helper.add_query_decl_doc(&window_decl.node_id.id(), self.doc);
        Ok(window_decl)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowDefnRaw {
    pub(crate) start: Location,
    pub(crate) end: Location,
    /// Identity of the window definition
    pub id: NodeId,
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
    pub(crate) windows: Option<Vec<WindowDefnRaw>>,
}
impl_expr!(SelectRaw);

impl<'script> Upable<'script> for SelectRaw<'script> {
    type Target = Select<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
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
        let windows: Vec<_> = self
            .windows
            .unwrap_or_default()
            .into_iter()
            // .map(|mut w| {
            //     w.id = w.id.with_prefix(&helper.module);
            //     w
            // })
            .collect();
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
            target,
            maybe_where,
            maybe_having,
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
            GroupByRaw::Expr { start, end, expr } => GroupBy::Expr {
                mid: helper.add_meta(start, end),
                expr: expr.up(helper)?,
            },
            GroupByRaw::Each { start, end, expr } => GroupBy::Each {
                mid: helper.add_meta(start, end),
                expr: expr.up(helper)?,
            },
            GroupByRaw::Set { start, end, items } => GroupBy::Set {
                mid: helper.add_meta(start, end),
                items: items.up(helper)?,
            },
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

#[derive(Clone, Debug, PartialEq, Serialize, Default)]
pub struct DefinitioalArgsRaw<'script> {
    pub args: ArgsExprsRaw<'script>,
}

impl<'script> Upable<'script> for DefinitioalArgsRaw<'script> {
    type Target = DefinitioalArgs<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(DefinitioalArgs {
            args: self.args.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Default)]
pub struct DefinitioalArgsWithRaw<'script> {
    pub args: ArgsExprsRaw<'script>,
    pub with: WithExprsRaw<'script>,
}

impl<'script> Upable<'script> for DefinitioalArgsWithRaw<'script> {
    type Target = DefinitioalArgsWith<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(DefinitioalArgsWith {
            args: self.args.up(helper)?,
            with: self.with.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CreationalWithRaw<'script> {
    pub with: WithExprsRaw<'script>,
}

impl<'script> Upable<'script> for CreationalWithRaw<'script> {
    type Target = CreationalWith<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(CreationalWith {
            with: self.with.up(helper)?,
        })
    }
}

pub type ConfigRaw<'script> = Vec<(IdentRaw<'script>, ImutExprRaw<'script>)>;

#[derive(Clone, Debug, PartialEq, Serialize, Default)]
pub struct WithExprsRaw<'script>(pub Vec<(IdentRaw<'script>, ImutExprRaw<'script>)>);
impl<'script> Upable<'script> for WithExprsRaw<'script> {
    type Target = WithExprs<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(WithExprs(self.0.up(helper)?))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Default)]
pub struct ArgsExprsRaw<'script>(pub Vec<(IdentRaw<'script>, Option<ImutExprRaw<'script>>)>);
impl<'script> Upable<'script> for ArgsExprsRaw<'script> {
    type Target = ArgsExprs<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(ArgsExprs(self.0.up(helper)?))
    }
}
