// Copyright 2018-2020, Wayfair GmbH
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

pub mod raw;
use super::raw::*;
use super::*;
use crate::impl_expr2;
use raw::*;

pub const WINDOW_CONST_ID: usize = 0;
pub const GROUP_CONST_ID: usize = 1;
pub const ARGS_CONST_ID: usize = 2;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Query<'script> {
    pub stmts: Stmts<'script>,
    pub node_meta: NodeMetas<'script>,
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
    pub stmt: Box<Select<'script>>,
    pub aggregates: Vec<InvokeAggrFn<'script>>,
    pub consts: Vec<Value<'script>>,
    pub locals: usize,
    pub node_meta: NodeMetas<'script>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorKind {
    pub mid: usize,
    pub module: String,
    pub operation: String,
}

impl BaseExpr for OperatorKind {
    fn mid(&self) -> usize {
        self.mid
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorDecl<'script> {
    pub mid: usize,
    pub kind: OperatorKind,
    pub id: String,
    pub params: Option<HashMap<String, Value<'script>>>,
}
impl_expr2!(OperatorDecl);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorStmt<'script> {
    pub mid: usize,
    pub id: String,
    pub target: String,
    pub params: Option<HashMap<String, Value<'script>>>,
}
impl_expr2!(OperatorStmt);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptDecl<'script> {
    pub mid: usize,
    pub id: String,
    pub params: Option<HashMap<String, Value<'script>>>,
    pub script: Script<'script>,
}
impl_expr2!(ScriptDecl);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptStmt<'script> {
    pub mid: usize,
    pub id: String,
    pub target: String,
    pub params: Option<HashMap<String, Value<'script>>>,
}
impl_expr2!(ScriptStmt);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum WindowKind {
    Sliding,
    Tumbling,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowDecl<'script> {
    pub mid: usize,
    pub id: String,
    pub kind: WindowKind,
    pub params: HashMap<String, Value<'script>>,
    pub script: Option<Script<'script>>,
}
impl_expr2!(WindowDecl);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Select<'script> {
    pub mid: usize,
    pub from: (Ident<'script>, Ident<'script>),
    pub into: (Ident<'script>, Ident<'script>),
    pub target: ImutExpr<'script>,
    pub maybe_where: Option<ImutExpr<'script>>,
    pub maybe_having: Option<ImutExpr<'script>>,
    pub maybe_group_by: Option<GroupBy<'script>>,
    pub windows: Vec<WindowDefnRaw>,
}
impl_expr2!(Select);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum GroupBy<'script> {
    Expr {
        mid: usize,
        expr: ImutExpr<'script>,
    },
    Set {
        mid: usize,
        items: Vec<GroupBy<'script>>,
    },
    Each {
        mid: usize,
        expr: ImutExpr<'script>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StreamStmt {
    pub mid: usize,
    pub id: String,
}

impl BaseExpr for StreamStmt {
    fn mid(&self) -> usize {
        self.mid
    }
}
