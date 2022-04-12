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
    error_generic, error_no_locals, GroupBy, HashMap, Helper, Location, OperatorCreate,
    OperatorDefinition, OperatorKind, PipelineCreate, PipelineDefinition, Query, Result,
    ScriptCreate, ScriptDefinition, Select, SelectStmt, Serialize, Stmt, StreamStmt, Upable,
    WindowDefinition, WindowKind,
};
use crate::{
    ast::{
        node_id::{BaseRef, NodeId},
        raw::UseRaw,
        visitors::{ConstFolder, GroupByExprExtractor, TargetEventRef},
        walkers::{ImutExprWalker, QueryWalker},
        Consts, Ident,
    },
    ModuleManager,
};
use crate::{
    ast::{InvokeAggrFn, NodeMeta},
    impl_expr_raw,
};
use beef::Cow;

#[derive(Clone, Debug, PartialEq, Serialize)]
#[allow(clippy::module_name_repetitions)]
pub struct QueryRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) config: ConfigRaw<'script>,
    pub(crate) stmts: StmtsRaw<'script>,
    pub(crate) params: DefinitioalArgsRaw<'script>,
}
impl<'script> QueryRaw<'script> {
    pub(crate) fn up_script<'registry>(
        self,
        mut helper: &mut Helper<'script, 'registry>,
    ) -> Result<Query<'script>> {
        let params = self.params.up(helper)?;
        // let args = params.render()?; FIXME

        let mut stmts: Vec<_> = self
            .stmts
            .into_iter()
            .filter_map(|stmt| stmt.up(&mut helper).transpose())
            .collect::<Result<_>>()?;

        for stmt in &mut stmts {
            ConstFolder::new(helper).walk_stmt(stmt)?;
        }
        let mut config = HashMap::new();
        for (k, mut v) in self.config.up(helper)? {
            ConstFolder::new(helper).walk_expr(&mut v)?;
            config.insert(k.to_string(), v.try_into_lit()?);
        }
        Ok(Query {
            mid: NodeMeta::new_box(self.start, self.end),
            params,
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
    Use(UseRaw),
    // /// we're forced to make this pub because of lalrpop
    // Const(ConstRaw<'script>),
    // /// we're forced to make this pub because of lalrpop
    // FnDecl(FnDeclRaw<'script>),
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
                let aggregates: Vec<_> = aggregates
                    .into_iter()
                    .map(InvokeAggrFn::into_static)
                    .collect();

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
            StmtRaw::Use(UseRaw { alias, module, .. }) => {
                let mid = ModuleManager::load(&module)?;
                let alias = alias.unwrap_or_else(|| module.id.clone());
                helper.scope().add_module_alias(alias, mid);
                Ok(None)
            }
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
impl_expr_raw!(OperatorDefinitionRaw);

impl<'script> Upable<'script> for OperatorDefinitionRaw<'script> {
    type Target = OperatorDefinition<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let operator_decl = OperatorDefinition {
            mid: NodeMeta::new_box_with_name(self.start, self.end, &self.id),
            node_id: NodeId::new(self.id, &[]),
            kind: self.kind.up(helper)?,
            params: self.params.up(helper)?,
        };
        helper.add_query_decl_doc(&operator_decl.node_id.id(), self.doc);
        Ok(operator_decl)
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
impl_expr_raw!(PipelineDefinitionRaw);

impl<'script> PipelineDefinitionRaw<'script> {
    const STREAM_PORT_CONFILCT: &'static str = "Streams cannot share names with from/into ports";
    fn dflt_in_ports<'ident>(&self) -> Vec<Ident<'ident>> {
        vec![Ident {
            mid: NodeMeta::new_box_with_name(self.start, self.end, "in"),
            id: "in".into(),
        }]
    }
    fn dflt_out_ports<'ident>(&self) -> Vec<Ident<'ident>> {
        vec![
            Ident {
                mid: NodeMeta::new_box_with_name(self.start, self.end, "out"),
                id: "in".into(),
            },
            Ident {
                mid: NodeMeta::new_box_with_name(self.start, self.end, "err"),
                id: "in".into(),
            },
        ]
    }
}

impl<'script> Upable<'script> for PipelineDefinitionRaw<'script> {
    type Target = PipelineDefinition<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        helper.enter_scope();

        let query = None;
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

        let mid = NodeMeta::new_box_with_name(self.start, self.end, &self.id);
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
impl_expr_raw!(PipelineInlineRaw);

impl<'script> Upable<'script> for PipelineInlineRaw<'script> {
    type Target = PipelineCreate<'script>;

    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // FIXME are those correct?!?
        Ok(PipelineCreate {
            mid: NodeMeta::new_box_with_name(self.start, self.end, &self.id),
            node_id: self.target,
            port_stream_map: HashMap::new(),
            params: self.params.up(helper)?,
        })
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
impl_expr_raw!(OperatorCreateRaw);

impl<'script> Upable<'script> for OperatorCreateRaw<'script> {
    type Target = OperatorCreate<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let params = self.params.up(helper)?;
        Ok(OperatorCreate {
            mid: NodeMeta::new_box_with_name(self.start, self.end, &self.id),
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
impl_expr_raw!(ScriptDefinitionRaw);

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
        let mid = NodeMeta::new_box_with_name(self.start, self.end, &self.id);
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
            mid: NodeMeta::new_box_with_name(self.start, self.end, &self.id),
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
impl_expr_raw!(WindowDefinitionRaw);

impl<'script> Upable<'script> for WindowDefinitionRaw<'script> {
    type Target = WindowDefinition<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let maybe_script = self.script.map(|s| s.up_script(helper)).transpose()?;

        // warn params if `emit_empty_windows` is defined, but neither `max_groups` nor `evicition_period` is defined

        let window_decl = WindowDefinition {
            mid: NodeMeta::new_box_with_name(self.start, self.end, &self.id),
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
impl_expr_raw!(SelectRaw);

impl<'script> Upable<'script> for SelectRaw<'script> {
    type Target = Select<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let mut target = self.target.up(helper)?;

        if helper.has_locals() {
            return error_no_locals(&(self.start, self.end), &target);
        };

        let maybe_having = self.maybe_having.up(helper)?;
        if helper.has_locals() {
            if let Some(definitely) = maybe_having {
                return error_no_locals(&(self.start, self.end), &definitely);
            }
        };

        let maybe_where = self.maybe_where.up(helper)?;
        if helper.has_locals() {
            if let Some(definitely) = maybe_where {
                return error_no_locals(&(self.start, self.end), &definitely);
            }
        };
        let maybe_group_by = self.maybe_group_by.up(helper)?;
        if helper.has_locals() {
            if let Some(definitely) = maybe_group_by {
                return error_no_locals(&(self.start, self.end), &definitely);
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
            mid: NodeMeta::new_box(self.start, self.end),
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
                mid: NodeMeta::new_box(start, end),
                expr: expr.up(helper)?,
            },
            GroupByRaw::Each { start, end, expr } => GroupBy::Each {
                mid: NodeMeta::new_box(start, end),
                expr: expr.up(helper)?,
            },
            GroupByRaw::Set { start, end, items } => GroupBy::Set {
                mid: NodeMeta::new_box(start, end),
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
    fn up<'registry>(self, _helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(OperatorKind {
            mid: NodeMeta::new_box_with_name(
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
    fn up<'registry>(self, _helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(StreamStmt {
            mid: NodeMeta::new_box_with_name(self.start, self.end, &self.id),
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
