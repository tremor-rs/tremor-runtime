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
    ArgsExprs, CreationalWith, DefinitionalArgs, DefinitionalArgsWith, WithExprs,
};
use super::{
    error_generic, error_no_locals, BaseExpr, GroupBy, HashMap, Helper, OperatorCreate,
    OperatorDefinition, OperatorKind, PipelineCreate, PipelineDefinition, Query, Result,
    ScriptCreate, ScriptDefinition, Select, SelectStmt, Serialize, Stmt, StreamStmt, Upable,
    WindowDefinition, WindowKind,
};
use crate::{ast::NodeMeta, impl_expr};
use crate::{
    ast::{
        node_id::NodeId,
        raw::UseRaw,
        visitors::{ConstFolder, GroupByExprExtractor, TargetEventRef},
        walkers::{ImutExprWalker, QueryWalker},
        Consts, Ident,
    },
    errors::{Error, Kind as ErrorKind},
    impl_expr_no_lt,
    module::Manager,
};
use beef::Cow;
use simd_json::ValueAccess;

#[derive(Clone, Debug, PartialEq, Serialize)]
#[allow(clippy::module_name_repetitions)]
pub struct QueryRaw<'script> {
    pub(crate) mid: Box<NodeMeta>,
    pub(crate) config: ConfigRaw<'script>,
    pub(crate) stmts: StmtsRaw<'script>,
    pub(crate) params: DefinitionalArgsRaw<'script>,
}
impl<'script> QueryRaw<'script> {
    pub(crate) fn up_script<'registry>(
        self,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<Query<'script>> {
        let params = self.params.up(helper)?;
        let mut stmts: Vec<_> = self
            .stmts
            .into_iter()
            .filter_map(|stmt| stmt.up(helper).transpose())
            .collect::<Result<_>>()?;
        for stmt in &mut stmts {
            ConstFolder::new(helper).walk_stmt(stmt)?;
        }
        let mut from = Vec::new();
        let mut into = Vec::new();
        let mut config = HashMap::new();
        for (k, mut v) in self.config.up(helper)? {
            ConstFolder::new(helper).walk_expr(&mut v)?;
            let mid = v.meta().clone();
            let v = v.try_into_value(helper)?;
            match (k.as_str(), v.as_str()) {
                ("from", Some(v)) => {
                    for v in v.split(',') {
                        from.push(Ident::new(
                            v.trim().to_string().into(),
                            Box::new(mid.clone()),
                        ));
                    }
                }
                ("into", Some(v)) => {
                    for v in v.split(',') {
                        into.push(Ident::new(
                            v.trim().to_string().into(),
                            Box::new(mid.clone()),
                        ));
                    }
                }
                _ => {}
            };
            config.insert(k.to_string(), v);
        }
        if from.is_empty() {
            from.push(Ident::new(Cow::const_str("in"), self.mid.clone()));
        }
        if into.is_empty() {
            into.push(Ident::new(Cow::const_str("out"), self.mid.clone()));
            into.push(Ident::new(Cow::const_str("err"), self.mid.clone()));
        }
        Ok(Query {
            mid: self.mid,
            params,
            from,
            into,
            config,
            stmts,
            scope: helper.scope.clone(),
        })
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
    PipelineCreate(PipelineCreateRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    StreamStmt(StreamStmtRaw),
    /// we're forced to make this pub because of lalrpop
    OperatorCreate(OperatorCreateRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    ScriptCreate(ScriptCreateRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    SelectStmt(Box<SelectRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Use(UseRaw),
}

impl<'script> Upable<'script> for StmtRaw<'script> {
    type Target = Option<Stmt<'script>>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match self {
            StmtRaw::SelectStmt(stmt) => {
                let mut aggregates = Vec::new();
                let mut locals = HashMap::new();
                helper.swap(&mut aggregates, &mut locals);
                let stmt: Select<'script> = stmt.up(helper)?;
                helper.swap(&mut aggregates, &mut locals);

                Ok(Some(Stmt::SelectStmt(SelectStmt {
                    stmt: Box::new(stmt),
                    aggregates,
                    consts: Consts::new(),
                    locals: locals.len(),
                })))
            }
            StmtRaw::StreamStmt(stmt) => Ok(Some(Stmt::StreamStmt(stmt.up(helper)?))),
            StmtRaw::OperatorDefinition(stmt) => {
                let stmt: OperatorDefinition<'script> = stmt.up(helper)?;
                helper.scope.insert_operator(stmt)?;
                Ok(None)
            }
            StmtRaw::OperatorCreate(stmt) => Ok(Some(Stmt::OperatorCreate(stmt.up(helper)?))),
            StmtRaw::ScriptDefinition(stmt) => {
                let stmt: ScriptDefinition<'script> = stmt.up(helper)?;
                helper.scope.insert_script(stmt)?;
                Ok(None)
            }
            StmtRaw::ScriptCreate(stmt) => Ok(Some(Stmt::ScriptCreate(stmt.up(helper)?))),
            StmtRaw::PipelineDefinition(stmt) => {
                let stmt = stmt.up(helper)?;
                helper.scope.insert_pipeline(stmt)?;
                Ok(None)
            }
            StmtRaw::WindowDefinition(stmt) => {
                let stmt: WindowDefinition<'script> = stmt.up(helper)?;
                helper.scope.insert_window(stmt)?;
                Ok(None)
            }
            StmtRaw::PipelineCreate(stmt) => Ok(Some(Stmt::PipelineCreate(stmt.up(helper)?))),
            StmtRaw::Use(UseRaw { alias, module, mid }) => {
                let range = mid.range;
                let module_id = Manager::load(&module).map_err(|err| match err {
                    Error(ErrorKind::ModuleNotFound(_, _, p, exp), state) => Error(
                        ErrorKind::ModuleNotFound(range.expand_lines(2), range, p, exp),
                        state,
                    ),
                    _ => err,
                })?;
                let alias = alias.unwrap_or_else(|| module.id.clone());
                helper.scope().add_module_alias(alias, module_id);
                Ok(None)
            }
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorDefinitionRaw<'script> {
    pub(crate) kind: OperatorKindRaw,
    pub(crate) id: String,
    pub(crate) params: DefinitionalArgsWithRaw<'script>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(OperatorDefinitionRaw);

impl<'script> Upable<'script> for OperatorDefinitionRaw<'script> {
    type Target = OperatorDefinition<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let operator_defn = OperatorDefinition {
            mid: self.mid.box_with_name(&self.id),
            id: self.id.clone(),
            kind: self.kind.up(helper)?,
            params: self.params.up(helper)?,
        };
        helper.add_query_doc(&operator_defn.id, self.doc);
        Ok(operator_defn)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct PipelineDefinitionRaw<'script> {
    pub(crate) id: String,
    pub(crate) config: ConfigRaw<'script>,
    pub(crate) params: DefinitionalArgsRaw<'script>,
    pub(crate) pipeline: StmtsRaw<'script>,
    pub(crate) from: Option<Vec<IdentRaw<'script>>>,
    pub(crate) into: Option<Vec<IdentRaw<'script>>>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(PipelineDefinitionRaw);

impl<'script> PipelineDefinitionRaw<'script> {
    const STREAM_PORT_CONFILCT: &'static str = "Streams cannot share names with from/into ports";
    fn dflt_in_ports<'ident>(&self) -> Vec<Ident<'ident>> {
        vec![Ident {
            mid: self.mid.clone().box_with_name("in"),
            id: "in".into(),
        }]
    }
    fn dflt_out_ports<'ident>(&self) -> Vec<Ident<'ident>> {
        vec![
            Ident {
                mid: self.mid.clone().box_with_name(&"out"),
                id: "out".into(),
            },
            Ident {
                mid: self.mid.clone().box_with_name(&"err"),
                id: "err".into(),
            },
        ]
    }
}

impl<'script> Upable<'script> for PipelineDefinitionRaw<'script> {
    type Target = PipelineDefinition<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        helper.enter_scope();

        let dflt_in_ports = self.dflt_in_ports();
        let dflt_out_ports = self.dflt_out_ports();
        let from = self.from.up(helper)?.unwrap_or(dflt_in_ports);
        let into = self.into.up(helper)?.unwrap_or(dflt_out_ports);

        let ports_set: HashSet<_> = from
            .iter()
            .chain(into.iter())
            .map(ToString::to_string)
            .collect();
        for stmt in &self.pipeline {
            if let StmtRaw::StreamStmt(stream_raw) = stmt {
                if ports_set.contains(&stream_raw.id) {
                    let stream = stream_raw.clone().up(helper)?;
                    return error_generic(&stream, &stream, &Self::STREAM_PORT_CONFILCT);
                }
            }
        }

        let mid = self.mid.box_with_name(&self.id);
        let stmts = self.pipeline.up(helper)?.into_iter().flatten().collect();
        let scope = helper.leave_scope()?;
        let params = self.params.up(helper)?;
        let config = self
            .config
            .into_iter()
            .map(|(k, v)| Ok((k.up(helper)?.to_string(), v.up(helper)?)))
            .collect::<Result<_>>()?;

        let pipeline_defn = PipelineDefinition {
            config,
            mid,
            id: self.id,
            params,
            stmts,
            from,
            into,
            scope,
        };

        helper.add_query_doc(&pipeline_defn.id, self.doc);
        Ok(pipeline_defn)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PipelineCreateRaw<'script> {
    pub(crate) alias: String,
    pub(crate) target: NodeId,
    pub(crate) params: CreationalWithRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(PipelineCreateRaw);

impl<'script> Upable<'script> for PipelineCreateRaw<'script> {
    type Target = PipelineCreate<'script>;

    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(PipelineCreate {
            mid: self.mid.box_with_name(&self.alias),
            target: self.target,
            alias: self.alias,
            port_stream_map: HashMap::new(),
            params: self.params.up(helper)?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorCreateRaw<'script> {
    pub(crate) id: String,
    pub(crate) target: NodeId,
    pub(crate) params: CreationalWithRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(OperatorCreateRaw);

impl<'script> Upable<'script> for OperatorCreateRaw<'script> {
    type Target = OperatorCreate<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(OperatorCreate {
            mid: self.mid.box_with_name(&self.id),
            id: self.id,
            target: self.target,
            params: self.params.up(helper)?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptDefinitionRaw<'script> {
    pub(crate) id: String,
    pub(crate) params: DefinitionalArgsRaw<'script>,
    pub(crate) script: ScriptRaw<'script>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
    pub(crate) mid: Box<NodeMeta>,
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
        let mid = self.mid.box_with_name(&self.id);
        helper.leave_scope()?;
        // Handle the params in the outside module
        let params = self.params;
        let params = params.up(helper)?;

        let script_defn = ScriptDefinition {
            mid,
            id: self.id,
            params,
            script,
        };

        helper.add_query_doc(&script_defn.id, self.doc);
        Ok(script_defn)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptCreateRaw<'script> {
    pub(crate) id: String,
    pub(crate) target: NodeId,
    pub(crate) params: CreationalWithRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(ScriptCreateRaw);

impl<'script> Upable<'script> for ScriptCreateRaw<'script> {
    type Target = ScriptCreate<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(ScriptCreate {
            mid: self.mid.box_with_name(&self.id),
            id: self.id,
            params: self.params.up(helper)?,
            target: self.target,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowDefinitionRaw<'script> {
    pub(crate) id: String,
    pub(crate) kind: WindowKind,
    // Yes it is odd that we use the `creational` here but windows are not crates
    // and defined like other constructs - perhaps we should revisit this?
    pub(crate) params: CreationalWithRaw<'script>,
    pub(crate) script: Option<ScriptRaw<'script>>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(WindowDefinitionRaw);

impl<'script> Upable<'script> for WindowDefinitionRaw<'script> {
    type Target = WindowDefinition<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let maybe_script = self.script.map(|s| s.up_script(helper)).transpose()?;

        // warn params if `emit_empty_windows` is defined, but neither `max_groups` nor `evicition_period` is defined

        let window_defn = WindowDefinition {
            mid: self.mid.box_with_name(&self.id),
            id: self.id,
            kind: self.kind,
            params: self.params.up(helper)?,
            script: maybe_script,
        };

        helper.add_query_doc(&window_defn.id, self.doc);
        Ok(window_defn)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize, Eq)]
pub struct WindowName {
    /// Identity of the window definition
    pub id: NodeId,
    pub(crate) mid: Box<NodeMeta>,
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SelectRaw<'script> {
    pub(crate) from: (IdentRaw<'script>, Option<IdentRaw<'script>>),
    pub(crate) into: (IdentRaw<'script>, Option<IdentRaw<'script>>),
    pub(crate) target: ImutExprRaw<'script>,
    pub(crate) maybe_where: Option<ImutExprRaw<'script>>,
    pub(crate) maybe_having: Option<ImutExprRaw<'script>>,
    pub(crate) maybe_group_by: Option<GroupByRaw<'script>>,
    pub(crate) windows: Option<Vec<WindowName>>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(SelectRaw);

impl<'script> Upable<'script> for SelectRaw<'script> {
    type Target = Select<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let mut target = self.target.up(helper)?;

        if helper.has_locals() {
            return error_no_locals(&self.mid.range, &target);
        };

        let maybe_having = self.maybe_having.up(helper)?;
        if helper.has_locals() {
            if let Some(definitely) = maybe_having {
                return error_no_locals(&self.mid.range, &definitely);
            }
        };

        let maybe_where = self.maybe_where.up(helper)?;
        if helper.has_locals() {
            if let Some(definitely) = maybe_where {
                return error_no_locals(&self.mid.range, &definitely);
            }
        };
        let maybe_group_by = self.maybe_group_by.up(helper)?;
        if helper.has_locals() {
            if let Some(definitely) = maybe_group_by {
                return error_no_locals(&self.mid.range, &definitely);
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
        let windows: Vec<_> = self.windows.unwrap_or_default().into_iter().collect();
        if !windows.is_empty() {
            // if we have windows we need to forbid free event references in the target if they are not
            // inside an aggregate function or can be rewritten to a group reference
            TargetEventRef::new(group_by_expressions).rewrite_target(&mut target)?;
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
            mid: self.mid,
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
        expr: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        mid: Box<NodeMeta>,
    },
    /// we're forced to make this pub because of lalrpop
    Set {
        /// we're forced to make this pub because of lalrpop
        items: Vec<GroupByRaw<'script>>,
        /// we're forced to make this pub because of lalrpop
        mid: Box<NodeMeta>,
    },
    /// we're forced to make this pub because of lalrpop
    Each {
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        mid: Box<NodeMeta>,
    },
}

impl<'script> Upable<'script> for GroupByRaw<'script> {
    type Target = GroupBy<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(match self {
            GroupByRaw::Expr { mid, expr } => GroupBy::Expr {
                mid,
                expr: expr.up(helper)?,
            },
            GroupByRaw::Each { mid, expr } => GroupBy::Each {
                mid,
                expr: expr.up(helper)?,
            },
            GroupByRaw::Set { mid, items } => GroupBy::Set {
                mid,
                items: items.up(helper)?,
            },
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize, Eq)]
pub struct OperatorKindRaw {
    pub(crate) mid: Box<NodeMeta>,
    pub(crate) module: String,
    pub(crate) operation: String,
}
impl_expr_no_lt!(OperatorKindRaw);
impl<'script> Upable<'script> for OperatorKindRaw {
    type Target = OperatorKind;
    fn up<'registry>(self, _helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(OperatorKind {
            mid: self
                .mid
                .box_with_name(&format!("{}::{}", self.module, self.operation)),
            module: self.module,
            operation: self.operation,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize, Eq)]
pub struct StreamStmtRaw {
    pub(crate) mid: Box<NodeMeta>,
    pub(crate) id: String,
}
impl_expr_no_lt!(StreamStmtRaw);

impl<'script> Upable<'script> for StreamStmtRaw {
    type Target = StreamStmt;
    fn up<'registry>(self, _helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(StreamStmt {
            mid: self.mid.box_with_name(&self.id),
            id: self.id,
        })
    }
}

pub type StmtsRaw<'script> = Vec<StmtRaw<'script>>;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DefinitionalArgsRaw<'script> {
    pub args: ArgsExprsRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}

impl<'script> DefinitionalArgsRaw<'script> {
    /// no args
    pub(crate) fn none(mid: Box<NodeMeta>) -> Self {
        Self {
            args: ArgsExprsRaw::default(),
            mid,
        }
    }
}

impl<'script> Upable<'script> for DefinitionalArgsRaw<'script> {
    type Target = DefinitionalArgs<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(DefinitionalArgs {
            args: self.args.up(helper)?,
            mid: self.mid,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DefinitionalArgsWithRaw<'script> {
    pub mid: Box<NodeMeta>,
    pub args: ArgsExprsRaw<'script>,
    pub with: WithExprsRaw<'script>,
}

impl<'script> DefinitionalArgsWithRaw<'script> {
    /// no args
    pub(crate) fn none(mid: Box<NodeMeta>) -> Self {
        Self {
            args: ArgsExprsRaw::default(),
            with: WithExprsRaw {
                mid: mid.clone(),
                exprs: Vec::default(),
            },
            mid,
        }
    }
}

impl<'script> Upable<'script> for DefinitionalArgsWithRaw<'script> {
    type Target = DefinitionalArgsWith<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(DefinitionalArgsWith {
            mid: self.mid,
            args: self.args.up(helper)?,
            with: self.with.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CreationalWithRaw<'script> {
    pub mid: Box<NodeMeta>,
    pub with: WithExprsRaw<'script>,
}

impl<'script> CreationalWithRaw<'script> {
    /// empty with
    pub(crate) fn none(mid: Box<NodeMeta>) -> Self {
        Self {
            mid: mid.clone(),
            with: WithExprsRaw {
                mid,
                exprs: Vec::default(),
            },
        }
    }
}

impl<'script> From<WithExprsRaw<'script>> for CreationalWithRaw<'script> {
    fn from(with_exprs: WithExprsRaw<'script>) -> Self {
        Self {
            mid: with_exprs.mid.clone(),
            with: with_exprs,
        }
    }
}

impl<'script> Upable<'script> for CreationalWithRaw<'script> {
    type Target = CreationalWith<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let mid = self.with.mid.clone();
        let with = self.with.up(helper)?;
        Ok(CreationalWith { with, mid })
    }
}

pub type ConfigRaw<'script> = Vec<(IdentRaw<'script>, ImutExprRaw<'script>)>;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WithExprsRaw<'script> {
    pub(crate) mid: Box<NodeMeta>,
    pub exprs: Vec<(IdentRaw<'script>, ImutExprRaw<'script>)>,
}
impl<'script> Upable<'script> for WithExprsRaw<'script> {
    type Target = WithExprs<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(WithExprs(self.exprs.up(helper)?))
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
