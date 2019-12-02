// Copyright 2018-2019, Wayfair GmbH
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

use super::super::raw::*;
use super::*;
use crate::impl_expr;
#[derive(Debug, PartialEq, Serialize)]
#[allow(clippy::module_name_repetitions)]
pub struct QueryRaw<'script> {
    pub stmts: StmtsRaw<'script>,
}
impl<'script> QueryRaw<'script> {
    pub fn up_script<'registry>(
        self,
        reg: &'registry Registry,
        aggr_reg: &'registry AggrRegistry,
    ) -> Result<(Query<'script>, usize, Vec<Warning>)> {
        let mut helper = Helper::new(reg, aggr_reg);
        Ok((
            Query {
                stmts: self.stmts.up(&mut helper)?,
                node_meta: helper.meta,
            },
            helper.locals.len(),
            helper.warnings,
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum StmtRaw<'script> {
    WindowDecl(WindowDeclRaw<'script>),
    OperatorDecl(OperatorDeclRaw<'script>),
    ScriptDecl(ScriptDeclRaw<'script>),
    Stream(StreamStmtRaw),
    Operator(OperatorStmtRaw<'script>),
    Script(ScriptStmtRaw<'script>),
    Select(Box<SelectRaw<'script>>),
}

impl<'script> Upable<'script> for StmtRaw<'script> {
    type Target = Stmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match self {
            StmtRaw::Select(stmt) => {
                let mut aggregates = Vec::new();
                let mut consts = HashMap::new();
                let mut locals = HashMap::new();
                helper.swap(&mut aggregates, &mut consts, &mut locals);
                let stmt: Select<'script> = stmt.up(helper)?;
                helper.swap(&mut aggregates, &mut consts, &mut locals);
                // We know that select statements have exactly three consts
                let consts = vec![Value::null(), Value::null(), Value::null()];

                Ok(Stmt::Select(SelectStmt {
                    stmt: Box::new(stmt),
                    aggregates,
                    consts,
                    locals: locals.len(),
                    node_meta: helper.meta.clone(),
                }))
            }
            StmtRaw::Stream(stmt) => Ok(Stmt::Stream(stmt.up(helper)?)),
            StmtRaw::OperatorDecl(stmt) => Ok(Stmt::OperatorDecl(stmt.up(helper)?)),
            StmtRaw::Operator(stmt) => Ok(Stmt::Operator(stmt.up(helper)?)),
            StmtRaw::ScriptDecl(stmt) => {
                let stmt: ScriptDecl<'script> = stmt.up(helper)?;
                Ok(Stmt::ScriptDecl(stmt))
            }
            StmtRaw::Script(stmt) => Ok(Stmt::Script(stmt.up(helper)?)),
            StmtRaw::WindowDecl(stmt) => {
                let stmt: WindowDecl<'script> = stmt.up(helper)?;
                Ok(Stmt::WindowDecl(stmt))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorDeclRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub kind: OperatorKindRaw,
    pub id: String,
    pub params: Option<WithExprsRaw<'script>>,
}

impl<'script> Upable<'script> for OperatorDeclRaw<'script> {
    type Target = OperatorDecl<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let operator_decl = OperatorDecl {
            mid: helper.add_meta(self.start, self.end),
            id: self.id,
            kind: self.kind.up(helper)?,
            params: match self.params {
                Some(p) => {
                    let mut pup = HashMap::new();
                    for (name, value) in p {
                        pup.insert(name.id.to_string(), reduce2(value.up(helper)?, &helper)?);
                    }
                    Some(pup)
                }
                None => None,
            },
        };
        helper.operators.push(operator_decl.clone());
        Ok(operator_decl)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorStmtRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub target: String,
    pub params: Option<WithExprsRaw<'script>>,
}
impl_expr!(OperatorStmtRaw);

impl<'script> Upable<'script> for OperatorStmtRaw<'script> {
    type Target = OperatorStmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(OperatorStmt {
            mid: helper.add_meta(self.start, self.end),
            id: self.id,
            target: self.target,
            params: match self.params {
                Some(p) => {
                    let mut pup = HashMap::new();
                    for (name, value) in p {
                        pup.insert(name.id.to_string(), reduce2(value.up(helper)?, &helper)?);
                    }
                    Some(pup)
                }
                None => None,
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptDeclRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub params: Option<WithExprsRaw<'script>>,
    pub script: ScriptRaw<'script>,
}

impl<'script> Upable<'script> for ScriptDeclRaw<'script> {
    type Target = ScriptDecl<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // Inject consts
        let (script, mut warnings) = self.script.up_script(helper.reg, helper.aggr_reg)?;
        helper.warnings.append(&mut warnings);
        helper.warnings.sort();
        helper.warnings.dedup();
        let script_decl = ScriptDecl {
            mid: helper.add_meta(self.start, self.end),
            id: self.id,
            params: match self.params {
                Some(p) => {
                    let mut pup = HashMap::new();
                    for (name, value) in p {
                        pup.insert(name.id.to_string(), reduce2(value.up(helper)?, &helper)?);
                    }
                    Some(pup)
                }
                None => None,
            },
            script,
        };
        helper.scripts.push(script_decl.clone());
        Ok(script_decl)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptStmtRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub target: String,
    pub params: Option<WithExprsRaw<'script>>,
}

impl<'script> Upable<'script> for ScriptStmtRaw<'script> {
    type Target = ScriptStmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // let (script, mut warnings) = self.script.up_script(helper.reg, helper.aggr_reg)?;
        // helper.warnings.append(&mut warnings);
        Ok(ScriptStmt {
            mid: helper.add_meta(self.start, self.end),
            id: self.id,
            params: match self.params {
                Some(p) => {
                    let mut pup = HashMap::new();
                    for (name, value) in p {
                        pup.insert(name.id.to_string(), reduce2(value.up(helper)?, &helper)?);
                    }
                    Some(pup)
                }
                None => None,
            },
            target: self.target,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowDeclRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub kind: WindowKind,
    pub params: WithExprsRaw<'script>,
    pub script: Option<ScriptRaw<'script>>,
}

impl<'script> Upable<'script> for WindowDeclRaw<'script> {
    type Target = WindowDecl<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let mut maybe_script = self
            .script
            .map(|s| s.up_script(helper.reg, helper.aggr_reg))
            .transpose()?;
        if let Some((_, ref mut warnings)) = maybe_script {
            helper.warnings.append(warnings);
            helper.warnings.sort();
            helper.warnings.dedup();
        };
        Ok(WindowDecl {
            mid: helper.add_meta(self.start, self.end),
            id: self.id,
            kind: self.kind,
            params: {
                let mut pup = HashMap::new();
                for (name, value) in self.params {
                    pup.insert(name.id.to_string(), reduce2(value.up(helper)?, &helper)?);
                }
                pup
            },
            script: maybe_script.map(|s| s.0),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowDefnRaw {
    pub start: Location,
    pub end: Location,
    pub id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SelectRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub from: (IdentRaw<'script>, Option<IdentRaw<'script>>),
    pub into: (IdentRaw<'script>, Option<IdentRaw<'script>>),
    pub target: ImutExprRaw<'script>,
    pub maybe_where: Option<ImutExprRaw<'script>>,
    pub maybe_having: Option<ImutExprRaw<'script>>,
    pub maybe_group_by: Option<GroupByRaw<'script>>,
    pub windows: Option<Vec<WindowDefnRaw>>,
}
impl_expr!(SelectRaw);

impl<'script> Upable<'script> for SelectRaw<'script> {
    type Target = Select<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        if !helper.consts.is_empty() {
            return error_no_consts(&(self.start, self.end), &self.target, &helper.meta);
        }
        // reserve const ids for builtin const
        helper.consts.insert("window".to_owned(), WINDOW_CONST_ID);
        helper.consts.insert("group".to_owned(), GROUP_CONST_ID);
        helper.consts.insert("args".to_owned(), ARGS_CONST_ID);
        let target = self.target.up(helper)?;

        if helper.has_locals() {
            return error_no_locals(&(self.start, self.end), &target, &helper.meta);
        };

        let maybe_having = self.maybe_having.up(helper)?;
        if helper.has_locals() {
            if let Some(definitely) = maybe_having {
                return error_no_locals(&(self.start, self.end), &definitely, &helper.meta);
            }
        };
        if helper.consts.remove("window") != Some(WINDOW_CONST_ID)
            || helper.consts.remove("group") != Some(GROUP_CONST_ID)
            || helper.consts.remove("args") != Some(ARGS_CONST_ID)
            || !helper.consts.is_empty()
        {
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

        let windows = self.windows.unwrap_or_default();

        let from = match self.from {
            (stream, None) => {
                let mut port = stream.clone();
                port.id = Cow::Borrowed("out");
                (stream, port)
            }
            (stream, Some(port)) => (stream, port),
        };
        let into = match self.into {
            (stream, None) => {
                let mut port = stream.clone();
                port.id = Cow::Borrowed("in");
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum GroupByRaw<'script> {
    Expr {
        start: Location,
        end: Location,
        expr: ImutExprRaw<'script>,
    },
    Set {
        start: Location,
        end: Location,
        items: Vec<GroupByRaw<'script>>,
    },
    Each {
        start: Location,
        end: Location,
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorKindRaw {
    pub start: Location,
    pub end: Location,
    pub module: String,
    pub operation: String,
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
            mid: helper.add_meta(self.start, self.end),
            module: self.module,
            operation: self.operation,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StreamStmtRaw {
    pub start: Location,
    pub end: Location,
    pub id: String,
}
impl<'script> Upable<'script> for StreamStmtRaw {
    type Target = StreamStmt;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(StreamStmt {
            mid: helper.add_meta(self.start, self.end),
            id: self.id,
        })
    }
}

pub type StmtsRaw<'script> = Vec<StmtRaw<'script>>;
