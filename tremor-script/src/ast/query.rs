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
use crate::{impl_expr, impl_expr1, impl_stmt, impl_stmt1};
#[derive(Debug, PartialEq, Serialize)]
#[allow(dead_code)]
pub struct Query1<'script> {
    pub stmts: Stmts1<'script>,
}

impl<'script> Query1<'script> {
    #[allow(dead_code)]
    pub fn up_script<'registry>(
        self,
        reg: &'registry Registry,
        aggr_reg: &'registry AggrRegistry,
    ) -> Result<(Query<'script>, usize, Vec<Warning>)> {
        let mut helper = Helper::new(reg, aggr_reg);
        let mut stmts = vec![];
        let _len = self.stmts.len();
        for (_i, e) in self.stmts.into_iter().enumerate() {
            stmts.push(e.up(&mut helper)?);
        }
        Ok((Query { stmts }, helper.locals.len(), helper.warnings))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Query<'script> {
    pub stmts: Stmts<'script>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Stmt1<'script> {
    WindowDecl(WindowDecl1<'script>),
    StreamDecl(StreamDecl),
    OperatorDecl(OperatorDecl1<'script>),
    ScriptDecl(ScriptDecl1<'script>),
    SelectStmt(Box<MutSelect1<'script>>),
}

impl<'script> Stmt1<'script> {
    #[allow(dead_code)]
    pub fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Stmt<'script>> {
        match self {
            Stmt1::SelectStmt(stmt) => {
                let mut aggregates = Vec::new();
                let mut consts = HashMap::new();
                helper.swap(&mut aggregates, &mut consts);
                let stmt: MutSelect<'script> = stmt.up(helper)?;
                helper.swap(&mut aggregates, &mut consts);
                // FIXME: iou a const
                let consts = Vec::new();
                Ok(Stmt::SelectStmt {
                    stmt: Box::new(stmt),
                    aggregates,
                    consts,
                })
            }
            Stmt1::StreamDecl(stmt) => Ok(Stmt::StreamDecl(stmt)),
            Stmt1::OperatorDecl(stmt) => Ok(Stmt::OperatorDecl(stmt.up(helper)?)),
            Stmt1::ScriptDecl(stmt) => {
                let stmt: ScriptDecl<'script> = stmt.up(helper)?;
                Ok(Stmt::ScriptDecl(stmt))
            }
            Stmt1::WindowDecl(stmt) => {
                let stmt: WindowDecl<'script> = stmt.up(helper)?;
                Ok(Stmt::WindowDecl(stmt))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[allow(dead_code)]
pub enum Stmt<'script> {
    WindowDecl(WindowDecl<'script>),
    StreamDecl(StreamDecl),
    OperatorDecl(OperatorDecl<'script>),
    ScriptDecl(ScriptDecl<'script>),
    SelectStmt {
        stmt: Box<MutSelect<'script>>,
        aggregates: Vec<InvokeAggrFn<'script>>,
        consts: Vec<Value<'script>>,
    },
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

impl<'script> OperatorDecl1<'script> {
    #[allow(dead_code)]
    pub fn up<'registry>(
        self,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<OperatorDecl<'script>> {
        Ok(OperatorDecl {
            start: self.start,
            end: self.end,
            id: self.id,
            kind: self.kind,
            params: match self.params {
                Some(p) => {
                    let mut pup = HashMap::new();
                    for (name, value) in p.into_iter() {
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
        Ok(ScriptDecl {
            start: self.start,
            end: self.end,
            id: self.id,
            params: match self.params {
                Some(p) => {
                    let mut pup = HashMap::new();
                    for (name, value) in p.into_iter() {
                        pup.insert(name.id.to_string(), reduce2(value.up(helper)?)?);
                    }
                    Some(pup)
                }
                None => None,
            },
            // FIXME side-effect of integrating two separate lines of code - we should really fuse/merge/join warnings and consts .unwrap()
            script: self.script.up_script(helper.reg, helper.aggr_reg)?.0,
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
pub enum WindowKind {
    Tumbling,
    Sliding,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowDecl1<'script> {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub kind: WindowKind,
    pub params: Option<WithExprs1<'script>>,
}

impl<'script> WindowDecl1<'script> {
    pub fn up<'registry>(
        self,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<WindowDecl<'script>> {
        Ok(WindowDecl {
            start: self.start,
            end: self.end,
            id: self.id,
            kind: self.kind,
            params: match self.params {
                Some(p) => {
                    let mut pup = HashMap::new();
                    for (name, value) in p.into_iter() {
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
pub struct WindowDecl<'script> {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub kind: WindowKind,
    pub params: Option<HashMap<String, Value<'script>>>,
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
    pub from: Ident<'script>,
    pub into: Ident<'script>,
    pub target: ImutExpr1<'script>,
    pub maybe_where: Option<ImutExpr1<'script>>,
    pub maybe_having: Option<ImutExpr1<'script>>,
    pub maybe_group_by: Option<GroupBy1<'script>>,
    pub maybe_window: Option<WindowDefn1>,
}
impl_expr1!(MutSelect1);
impl_stmt1!(MutSelect1);

impl<'script> MutSelect1<'script> {
    #[allow(dead_code)]
    pub fn up<'registry>(
        self,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<MutSelect<'script>> {
        Ok(MutSelect {
            start: self.start,
            end: self.end,
            from: self.from,
            into: self.into,
            target: self.target.up(helper)?,
            maybe_where: self.maybe_where.map(|h| h.up(helper)).transpose()?,
            maybe_having: self.maybe_having.map(|h| h.up(helper)).transpose()?,
            maybe_group_by: self.maybe_group_by.map(|h| h.up(helper)).transpose()?,
            maybe_window: self.maybe_window,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MutSelect<'script> {
    pub start: Location,
    pub end: Location,
    pub from: Ident<'script>,
    pub into: Ident<'script>,
    pub target: ImutExpr<'script>,
    pub maybe_where: Option<ImutExpr<'script>>,
    pub maybe_having: Option<ImutExpr<'script>>,
    pub maybe_group_by: Option<GroupBy<'script>>,
    pub maybe_window: Option<WindowDefn1>,
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
    #[allow(dead_code)]
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
            GroupBy1::Set {
                start,
                end,
                items: _items,
            } => GroupBy::Set {
                start,
                end,
                items: vec![],
            },
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StreamDecl {
    pub start: Location,
    pub end: Location,
    pub id: String,
}
