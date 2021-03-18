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

use super::super::raw::{
    reduce2, BaseExpr, ExprRaw, IdentRaw, ImutExprRaw, ModuleRaw, ScriptRaw, WithExprsRaw,
};
use super::{
    error_generic, error_no_consts, error_no_locals, AggrRegistry, GroupBy, GroupByInt, HashMap,
    Helper, ImutExpr, Location, NodeMetas, OperatorDecl, OperatorKind, OperatorStmt, Query,
    Registry, Result, ScriptDecl, ScriptStmt, Select, SelectStmt, Serialize, Stmt, StreamStmt,
    Upable, Value, Warning, WindowDecl, WindowKind,
};
use crate::impl_expr;
use crate::{
    ast::visitors::{GroupByExprExtractor, TargetEventRefVisitor},
    lexer::Range,
};
use beef::Cow;
use value_trait::Value as ValueTrait;

fn up_params<'script, 'registry>(
    params: WithExprsRaw<'script>,
    helper: &mut Helper<'script, 'registry>,
) -> Result<HashMap<String, Value<'script>>> {
    params
        .into_iter()
        .map(|(name, value)| Ok((name.id.to_string(), reduce2(value.up(helper)?, &helper)?)))
        .collect()
}

fn up_maybe_params<'script, 'registry>(
    params: Option<WithExprsRaw<'script>>,
    helper: &mut Helper<'script, 'registry>,
) -> Result<Option<HashMap<String, Value<'script>>>> {
    params.map(|params| up_params(params, helper)).transpose()
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
        for (_i, e) in self.stmts.into_iter().enumerate() {
            match e {
                StmtRaw::ModuleStmt(m) => {
                    m.define(helper.reg, helper.aggr_reg, &mut vec![], &mut helper)?;
                }
                other => {
                    stmts.push(other.up(&mut helper)?);
                }
            }
        }

        Ok(Query {
            config: up_params(self.config, helper)?,
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
    Expr(Box<ExprRaw<'script>>),
}

impl<'script> BaseExpr for StmtRaw<'script> {
    fn mid(&self) -> usize {
        0
    }
    fn s(&self, meta: &NodeMetas) -> Location {
        match self {
            StmtRaw::ModuleStmt(s) => s.start,
            StmtRaw::Operator(s) => s.start,
            StmtRaw::OperatorDecl(s) => s.start,
            StmtRaw::Script(s) => s.start,
            StmtRaw::ScriptDecl(s) => s.start,
            StmtRaw::Select(s) => s.start,
            StmtRaw::Stream(s) => s.start,
            StmtRaw::WindowDecl(s) => s.start,
            StmtRaw::Expr(s) => s.s(meta),
        }
    }
    fn e(&self, meta: &NodeMetas) -> Location {
        match self {
            StmtRaw::ModuleStmt(e) => e.end,
            StmtRaw::Operator(e) => e.end,
            StmtRaw::OperatorDecl(e) => e.end,
            StmtRaw::Script(e) => e.end,
            StmtRaw::ScriptDecl(e) => e.end,
            StmtRaw::Select(e) => e.end,
            StmtRaw::Stream(e) => e.end,
            StmtRaw::WindowDecl(e) => e.end,
            StmtRaw::Expr(e) => e.e(meta),
        }
    }
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
                // only allocate scratches if they are really needed - when we have multiple windows
                let aggregate_scratches = if stmt.windows.len() > 1 {
                    Some((aggregates.clone(), aggregates.clone()))
                } else {
                    None
                };

                Ok(Stmt::Select(SelectStmt {
                    stmt: Box::new(stmt),
                    aggregates,
                    aggregate_scratches,
                    consts: helper.consts.clone(),
                    locals: locals.len(),
                    node_meta: helper.meta.clone(),
                }))
            }
            StmtRaw::Stream(stmt) => Ok(Stmt::Stream(stmt.up(helper)?)),
            StmtRaw::OperatorDecl(stmt) => {
                let stmt: OperatorDecl<'script> = stmt.up(helper)?;
                helper
                    .operators
                    .insert(stmt.fqon(&stmt.module), stmt.clone());
                Ok(Stmt::OperatorDecl(stmt))
            }
            StmtRaw::Operator(stmt) => Ok(Stmt::Operator(stmt.up(helper)?)),
            StmtRaw::ScriptDecl(stmt) => {
                let stmt: ScriptDecl<'script> = stmt.up(helper)?;
                helper.scripts.insert(stmt.fqsn(&stmt.module), stmt.clone());
                Ok(Stmt::ScriptDecl(Box::new(stmt)))
            }
            StmtRaw::Script(stmt) => Ok(Stmt::Script(stmt.up(helper)?)),
            StmtRaw::WindowDecl(stmt) => {
                let stmt: WindowDecl<'script> = stmt.up(helper)?;
                helper.windows.insert(stmt.fqwn(&stmt.module), stmt.clone());
                Ok(Stmt::WindowDecl(Box::new(stmt)))
            }
            StmtRaw::ModuleStmt(m) => {
                error_generic(&m, &m, &"Module in wrong place error", &helper.meta)
            }
            StmtRaw::Expr(m) => {
                error_generic(&*m, &*m, &"Expression in wrong place error", &helper.meta)
            }
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
    pub(crate) params: Option<WithExprsRaw<'script>>,
}

impl<'script> Upable<'script> for OperatorDeclRaw<'script> {
    type Target = OperatorDecl<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let operator_decl = OperatorDecl {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            module: helper.module.clone(),
            id: self.id,
            kind: self.kind.up(helper)?,
            params: up_maybe_params(self.params, helper)?,
        };
        helper
            .operators
            .insert(operator_decl.fqon(&helper.module), operator_decl.clone());
        Ok(operator_decl)
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
    pub(crate) fn define<'registry>(
        self,
        reg: &'registry Registry,
        aggr_reg: &'registry AggrRegistry,
        consts: &mut Vec<Value<'script>>,
        mut helper: &mut Helper<'script, 'registry>,
    ) -> Result<()> {
        helper.module.push(self.name.id.to_string());
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
                    helper.module.push(self.name.id.to_string());
                }
                StmtRaw::WindowDecl(stmt) => {
                    let w = stmt.up(&mut helper)?;
                    helper.windows.insert(w.fqwn(&helper.module), w);
                }
                StmtRaw::ScriptDecl(stmt) => {
                    let s = stmt.up(&mut helper)?;
                    helper.scripts.insert(s.fqsn(&helper.module), s);
                }
                StmtRaw::OperatorDecl(stmt) => {
                    let o = stmt.up(&mut helper)?;
                    helper.operators.insert(o.fqon(&helper.module), o);
                }
                e => {
                    return error_generic(
                        &e,
                        &e,
                        &"Can't have statements inside of query modules",
                        &helper.meta,
                    )
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
    pub(crate) params: Option<WithExprsRaw<'script>>,
}
impl_expr!(OperatorStmtRaw);

impl<'script> Upable<'script> for OperatorStmtRaw<'script> {
    type Target = OperatorStmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(OperatorStmt {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            id: self.id,
            module: self
                .module
                .into_iter()
                .map(|x| x.id.to_string())
                .collect::<Vec<String>>(),

            target: self.target,
            params: up_maybe_params(self.params, helper)?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptDeclRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) params: Option<WithExprsRaw<'script>>,
    pub(crate) script: ScriptRaw<'script>,
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
        helper.module.push(self.id.to_string());
        let script = self.script.up_script(helper)?;

        let script_decl = ScriptDecl {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            module: helper.module.clone(),
            id: self.id,
            params: up_maybe_params(self.params, helper)?,
            script,
        };
        helper.module.pop();
        helper
            .scripts
            .insert(script_decl.fqsn(&helper.module), script_decl.clone());
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
    pub(crate) params: Option<WithExprsRaw<'script>>,
}

impl<'script> Upable<'script> for ScriptStmtRaw<'script> {
    type Target = ScriptStmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // let (script, mut warnings) = self.script.up_script(helper.reg, helper.aggr_reg)?;
        // helper.warnings.append(&mut warnings);
        Ok(ScriptStmt {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            id: self.id,
            params: up_maybe_params(self.params, helper)?,
            target: self.target,
            module: self
                .module
                .into_iter()
                .map(|x| x.id.to_string())
                .collect::<Vec<String>>(),
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
    pub(crate) params: WithExprsRaw<'script>,
    pub(crate) script: Option<ScriptRaw<'script>>,
}

impl<'script> Upable<'script> for WindowDeclRaw<'script> {
    type Target = WindowDecl<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let maybe_script = self.script.map(|s| s.up_script(helper)).transpose()?;

        // warn params if `emit_empty_windows` is defined, but neither `max_groups` nor `evicition_period` is defined
        let params = up_params(self.params, helper)?;
        let custom_max_groups = params
            .get(WindowDecl::MAX_GROUPS)
            .and_then(Value::as_u64)
            .map(|x| x < u64::MAX)
            .unwrap_or_default();
        let emit_empty_windows = params
            .get(WindowDecl::EMIT_EMPTY_WINDOWS)
            .and_then(Value::as_bool)
            .unwrap_or_default();
        if emit_empty_windows
            && !(custom_max_groups || params.contains_key(WindowDecl::EVICTION_PERIOD))
        {
            let range: Range = (self.start, self.end).into();
            helper.warn(Warning::new_with_scope(
                range,
                "Using `emit_empty_windows` without guard is potentially dangerous. Consider limiting the amount of groups maintained internally by using `max_groups` and/or `eviction_period`.".to_owned(),
            ));
        }

        Ok(WindowDecl {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            module: helper.module.clone(),
            id: self.id,
            kind: self.kind,
            params,
            script: maybe_script,
        })
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
            format!(
                "{}::{}",
                self.module
                    .iter()
                    .map(|x| x.id.to_string())
                    .collect::<Vec<String>>()
                    .join("::"),
                self.id
            )
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
            TargetEventRefVisitor::new(group_by_expressions, &helper.meta)
                .rewrite_target(&mut target)?;
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
impl BaseExpr for OperatorKindRaw {
    fn s(&self, _meta: &NodeMetas) -> Location {
        self.start
    }
    fn e(&self, _meta: &NodeMetas) -> Location {
        self.end
    }
    fn mid(&self) -> usize {
        0
    }
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
