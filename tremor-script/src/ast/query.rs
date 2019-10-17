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

use super::*;
use crate::{impl_expr, impl_stmt, impl_stmt1};
#[derive(Debug, PartialEq, Serialize)]
#[allow(dead_code)]
#[allow(clippy::module_name_repetitions)]
pub struct Query1<'script> {
    pub stmts: Stmts1<'script>,
}

pub const WINDOW_CONST_ID: usize = 0;
pub const GROUP_CONST_ID: usize = 1;
pub const ARGS_CONST_ID: usize = 2;

impl<'script> Query1<'script> {
    #[allow(dead_code)]
    pub fn up_script<'registry>(
        self,
        reg: &'registry Registry,
        aggr_reg: &'registry AggrRegistry,
    ) -> Result<(Query<'script>, usize, Vec<Warning>)> {
        let mut helper = Helper::new(reg, aggr_reg);
        Ok((
            Query {
                stmts: self.stmts.up(&mut helper)?,
            },
            helper.locals.len(),
            helper.warnings,
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Query<'script> {
    pub stmts: Stmts<'script>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Stmt1<'script> {
    WindowDecl(WindowDecl1<'script>),
    OperatorDecl(OperatorDecl1<'script>),
    ScriptDecl(ScriptDecl1<'script>),
    Stream(StreamStmt),
    Operator(OperatorStmt1<'script>),
    Script(ScriptStmt1<'script>),
    Select(Box<MutSelect1<'script>>),
}

impl<'script> Upable<'script> for Stmt1<'script> {
    type Target = Stmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match self {
            Stmt1::Select(stmt) => {
                let mut aggregates = Vec::new();
                let mut consts = HashMap::new();
                let mut locals = HashMap::new();
                helper.swap(&mut aggregates, &mut consts, &mut locals);
                let stmt: MutSelect<'script> = stmt.up(helper)?;
                helper.swap(&mut aggregates, &mut consts, &mut locals);
                // We know that select statements have exactly three consts
                let consts = vec![Value::Null, Value::Null, Value::Null];

                Ok(Stmt::Select(SelectStmt {
                    stmt: Box::new(stmt),
                    aggregates,
                    consts,
                    locals: locals.len(),
                }))
            }
            Stmt1::Stream(stmt) => Ok(Stmt::Stream(stmt)),
            Stmt1::OperatorDecl(stmt) => Ok(Stmt::OperatorDecl(stmt.up(helper)?)),
            Stmt1::Operator(stmt) => Ok(Stmt::Operator(stmt.up(helper)?)),
            Stmt1::ScriptDecl(stmt) => {
                let stmt: ScriptDecl<'script> = stmt.up(helper)?;
                Ok(Stmt::ScriptDecl(stmt))
            }
            Stmt1::Script(stmt) => Ok(Stmt::Script(stmt.up(helper)?)),
            Stmt1::WindowDecl(stmt) => {
                let stmt: WindowDecl<'script> = stmt.up(helper)?;
                Ok(Stmt::WindowDecl(stmt))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Stmt<'script> {
    WindowDecl(WindowDecl<'script>),
    Stream(StreamStmt),
    OperatorDecl(OperatorDecl<'script>),
    ScriptDecl(ScriptDecl<'script>),
    Operator(OperatorStmt<'script>),
    Script(ScriptStmt<'script>),
    Select(SelectStmt<'script>),
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SelectStmt<'script> {
    pub stmt: Box<MutSelect<'script>>,
    pub aggregates: Vec<InvokeAggrFn<'script>>,
    pub consts: Vec<Value<'script>>,
    pub locals: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorKind {
    pub start: Location,
    pub end: Location,
    pub module: String,
    pub operation: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorDecl1<'script> {
    pub start: Location,
    pub end: Location,
    pub kind: OperatorKind,
    pub id: String,
    pub params: Option<WithExprs1<'script>>,
}

impl<'script> Upable<'script> for OperatorDecl1<'script> {
    type Target = OperatorDecl<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let operator_decl = OperatorDecl {
            start: self.start,
            end: self.end,
            id: self.id,
            kind: self.kind,
            params: match self.params {
                Some(p) => {
                    let mut pup = HashMap::new();
                    for (name, value) in p {
                        pup.insert(name.id.to_string(), reduce2(value.up(helper)?)?);
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
pub struct OperatorStmt1<'script> {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub target: String,
    pub params: Option<WithExprs1<'script>>,
}
impl_stmt1!(OperatorStmt1);

impl<'script> Upable<'script> for OperatorStmt1<'script> {
    type Target = OperatorStmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(OperatorStmt {
            start: self.start,
            end: self.end,
            id: self.id,
            target: self.target,
            params: match self.params {
                Some(p) => {
                    let mut pup = HashMap::new();
                    for (name, value) in p {
                        pup.insert(name.id.to_string(), reduce2(value.up(helper)?)?);
                    }
                    Some(pup)
                }
                None => None,
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorDecl<'script> {
    pub start: Location,
    pub end: Location,
    pub kind: OperatorKind,
    pub id: String,
    pub params: Option<HashMap<String, Value<'script>>>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorStmt<'script> {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub target: String,
    pub params: Option<HashMap<String, Value<'script>>>,
}
impl_stmt!(OperatorStmt);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptDecl1<'script> {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub params: Option<WithExprs1<'script>>,
    pub script: Script1<'script>,
}

impl<'script> ScriptDecl1<'script> {
    #[allow(dead_code)]
    pub fn up<'registry>(
        self,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<ScriptDecl<'script>> {
        // Inject consts
        let (script, mut warnings) = self.script.up_script(helper.reg, helper.aggr_reg)?;
        helper.warnings.append(&mut warnings);
        helper.warnings.sort();
        helper.warnings.dedup();
        let script_decl = ScriptDecl {
            start: self.start,
            end: self.end,
            id: self.id,
            params: match self.params {
                Some(p) => {
                    let mut pup = HashMap::new();
                    for (name, value) in p {
                        pup.insert(name.id.to_string(), reduce2(value.up(helper)?)?);
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
pub struct ScriptStmt1<'script> {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub target: String,
    pub params: Option<WithExprs1<'script>>,
}

impl<'script> ScriptStmt1<'script> {
    #[allow(dead_code)]
    pub fn up<'registry>(
        self,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<ScriptStmt<'script>> {
        // let (script, mut warnings) = self.script.up_script(helper.reg, helper.aggr_reg)?;
        // helper.warnings.append(&mut warnings);
        Ok(ScriptStmt {
            start: self.start,
            end: self.end,
            id: self.id,
            params: match self.params {
                Some(p) => {
                    let mut pup = HashMap::new();
                    for (name, value) in p {
                        pup.insert(name.id.to_string(), reduce2(value.up(helper)?)?);
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
pub struct ScriptDecl<'script> {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub params: Option<HashMap<String, Value<'script>>>,
    pub script: Script<'script>,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptStmt<'script> {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub target: String,
    pub params: Option<HashMap<String, Value<'script>>>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum WindowKind {
    Sliding,
    Tumbling,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowDecl1<'script> {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub kind: WindowKind,
    pub params: WithExprs1<'script>,
    pub script: Option<Script1<'script>>,
}

impl<'script> WindowDecl1<'script> {
    pub fn up<'registry>(
        self,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<WindowDecl<'script>> {
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
            start: self.start,
            end: self.end,
            id: self.id,
            kind: self.kind,
            params: {
                let mut pup = HashMap::new();
                for (name, value) in self.params {
                    pup.insert(name.id.to_string(), reduce2(value.up(helper)?)?);
                }
                pup
            },
            script: maybe_script.map(|s| s.0),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowDecl<'script> {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub kind: WindowKind,
    pub params: HashMap<String, Value<'script>>,
    pub script: Option<Script<'script>>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowDefn1 {
    pub start: Location,
    pub end: Location,
    pub id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MutSelect1<'script> {
    pub start: Location,
    pub end: Location,
    pub from: (Ident<'script>, Option<Ident<'script>>),
    pub into: (Ident<'script>, Option<Ident<'script>>),
    pub target: ImutExpr1<'script>,
    pub maybe_where: Option<ImutExpr1<'script>>,
    pub maybe_having: Option<ImutExpr1<'script>>,
    pub maybe_group_by: Option<GroupBy1<'script>>,
    pub windows: Option<Vec<WindowDefn1>>,
}
impl_expr!(MutSelect1);
impl_stmt1!(MutSelect1);

impl<'script> MutSelect1<'script> {
    #[allow(dead_code)]
    pub fn up<'registry>(
        self,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<MutSelect<'script>> {
        if !helper.consts.is_empty() {
            return error_no_consts(&(self.start, self.end), &self.target);
        }
        // reserve const ids for builtin const
        helper.consts.insert("window".to_owned(), WINDOW_CONST_ID);
        helper.consts.insert("group".to_owned(), GROUP_CONST_ID);
        helper.consts.insert("args".to_owned(), ARGS_CONST_ID);
        let target = self.target.up(helper)?;

        if helper.has_locals() {
            return error_no_locals(&(self.start, self.end), &target);
        };

        let maybe_having = self.maybe_having.up(helper)?;
        if helper.has_locals() {
            if let Some(definitely) = maybe_having {
                return error_no_locals(&(self.start, self.end), &definitely);
            }
        };
        if helper.consts.remove("window") != Some(WINDOW_CONST_ID)
            || helper.consts.remove("group") != Some(GROUP_CONST_ID)
            || helper.consts.remove("args") != Some(ARGS_CONST_ID)
            || !helper.consts.is_empty()
        {
            return error_no_consts(&(self.start, self.end), &target);
        }

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
        Ok(MutSelect {
            start: self.start,
            end: self.end,
            from,
            into,
            target,
            maybe_where,
            maybe_having,
            maybe_group_by,
            windows,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MutSelect<'script> {
    pub start: Location,
    pub end: Location,
    pub from: (Ident<'script>, Ident<'script>),
    pub into: (Ident<'script>, Ident<'script>),
    pub target: ImutExpr<'script>,
    pub maybe_where: Option<ImutExpr<'script>>,
    pub maybe_having: Option<ImutExpr<'script>>,
    pub maybe_group_by: Option<GroupBy<'script>>,
    pub windows: Vec<WindowDefn1>,
}
impl_expr!(MutSelect);
impl_stmt!(MutSelect);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum GroupBy1<'script> {
    Expr {
        start: Location,
        end: Location,
        expr: ImutExpr1<'script>,
    },
    Set {
        start: Location,
        end: Location,
        items: Vec<GroupBy1<'script>>,
    },
    Each {
        start: Location,
        end: Location,
        expr: ImutExpr1<'script>,
    },
}

impl<'script> Upable<'script> for GroupBy1<'script> {
    type Target = GroupBy<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(match self {
            GroupBy1::Expr { start, end, expr } => GroupBy::Expr {
                start,
                end,
                expr: expr.up(helper)?,
            },
            GroupBy1::Each { start, end, expr } => GroupBy::Each {
                start,
                end,
                expr: expr.up(helper)?,
            },
            GroupBy1::Set { start, end, items } => {
                let items = items.up(helper)?;
                GroupBy::Set { start, end, items }
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum GroupBy<'script> {
    Expr {
        start: Location,
        end: Location,
        expr: ImutExpr<'script>,
    },
    Set {
        start: Location,
        end: Location,
        items: Vec<GroupBy<'script>>,
    },
    Each {
        start: Location,
        end: Location,
        expr: ImutExpr<'script>,
    },
}
impl<'script> BaseExpr for GroupBy<'script> {
    fn s(&self) -> Location {
        match self {
            GroupBy::Expr { start, .. }
            | GroupBy::Set { start, .. }
            | GroupBy::Each { start, .. } => *start,
        }
    }
    fn e(&self) -> Location {
        match self {
            GroupBy::Expr { end, .. } | GroupBy::Set { end, .. } | GroupBy::Each { end, .. } => {
                *end
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StreamStmt {
    pub start: Location,
    pub end: Location,
    pub id: String,
}
