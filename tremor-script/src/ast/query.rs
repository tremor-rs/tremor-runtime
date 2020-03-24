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

pub(crate) mod raw;
use super::raw::*;
use super::*;
use crate::impl_expr2;
use raw::*;

/// The Constant ID of the `window` constant
pub const WINDOW_CONST_ID: usize = 0;
/// The Constant ID of the `group` constant
pub const GROUP_CONST_ID: usize = 1;
/// The Constant ID of the `args` constant
pub const ARGS_CONST_ID: usize = 2;

/// A Tremor query
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Query<'script> {
    /// Statements
    pub stmts: Stmts<'script>,
    /// Query Node Metadata
    pub node_meta: NodeMetas,
}

/// Query statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Stmt<'script> {
    /// A window declaration
    WindowDecl(WindowDecl<'script>),
    /// A stream
    Stream(StreamStmt),
    /// An operator declaration
    OperatorDecl(OperatorDecl<'script>),
    /// A script declaration
    ScriptDecl(ScriptDecl<'script>),
    /// An operator createion
    Operator(OperatorStmt<'script>),
    /// A script creation
    Script(ScriptStmt<'script>),
    /// A selelect statement
    Select(SelectStmt<'script>),
}

/// A Select statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SelectStmt<'script> {
    /// The select statement
    pub stmt: Box<Select<'script>>,
    /// Aggregates
    pub aggregates: Vec<InvokeAggrFn<'script>>,
    /// Constants
    pub consts: Vec<Value<'script>>,
    /// Number of locals
    pub locals: usize,
    /// Node metadata nodes
    pub node_meta: NodeMetas,
}

/// The type of a select statment
pub enum SelectType {
    /// This select statement can be turned
    /// into a passthrough node
    Passthrough,
    /// This is a simple statement without grouping
    /// or windowing
    Simple,
    /// This is a full fledged select statment
    Normal,
}

impl SelectStmt<'_> {
    /// Determin how complex a select statment is
    pub fn complexity(&self) -> SelectType {
        if self.stmt.target.0
            == ImutExprInt::Path(Path::Event(EventPath {
                mid: 0,
                segments: vec![],
            }))
            && self.stmt.maybe_group_by.is_none()
            && self.stmt.windows.is_empty()
        {
            if self.stmt.maybe_having.is_none() && self.stmt.maybe_where.is_none() {
                SelectType::Passthrough
            } else {
                SelectType::Simple
            }
        } else {
            SelectType::Normal
        }
    }
}

/// Operator kind identifyiner
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorKind {
    pub(crate) mid: usize,
    /// Modeule of the operator
    pub module: String,
    /// Operator name
    pub operation: String,
}

impl BaseExpr for OperatorKind {
    fn mid(&self) -> usize {
        self.mid
    }
}

/// An operator declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorDecl<'script> {
    pub(crate) mid: usize,
    /// Type of the operator
    pub kind: OperatorKind,
    /// Identifyer for the operator
    pub id: String,
    /// Parameters for the operator
    pub params: Option<HashMap<String, Value<'script>>>,
}
impl_expr2!(OperatorDecl);

/// An operator creaton
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorStmt<'script> {
    pub(crate) mid: usize,
    /// Id of the operator
    pub id: String,
    /// Target of the operator
    pub target: String,
    pub(crate) params: Option<HashMap<String, Value<'script>>>,
}
impl_expr2!(OperatorStmt);

/// A script declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptDecl<'script> {
    pub(crate) mid: usize,
    /// ID of the script
    pub id: String,
    /// Parameters of a script declaration
    pub params: Option<HashMap<String, Value<'script>>>,
    /// The script itself
    pub script: Script<'script>,
}
impl_expr2!(ScriptDecl);

/// A script creation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptStmt<'script> {
    pub(crate) mid: usize,
    /// ID of the script
    pub id: String,
    /// Target of the script
    pub target: String,
    /// Parameters of the script statement
    pub params: Option<HashMap<String, Value<'script>>>,
}
impl_expr2!(ScriptStmt);

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum WindowKind {
    /// we're forced to make this pub because of lalrpop
    Sliding,
    /// we're forced to make this pub because of lalrpop
    Tumbling,
}

/// A window declration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowDecl<'script> {
    pub(crate) mid: usize,
    /// Name of the window declaration
    pub id: String,
    /// The type of window
    pub kind: WindowKind,
    /// Parameters passed to the window
    pub params: HashMap<String, Value<'script>>,
    /// The script of the window
    pub script: Option<Script<'script>>,
}
impl_expr2!(WindowDecl);

/// A select statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Select<'script> {
    /// MetadataID of the statement
    pub mid: usize,
    /// The from clause
    pub from: (Ident<'script>, Ident<'script>),
    /// The into claus
    pub into: (Ident<'script>, Ident<'script>),
    /// The target (select part)
    pub target: ImutExpr<'script>,
    /// Where claus
    pub maybe_where: Option<ImutExpr<'script>>,

    /// Having clause
    pub maybe_having: Option<ImutExpr<'script>>,
    /// Group-By clause
    pub maybe_group_by: Option<GroupBy<'script>>,
    /// Window
    pub windows: Vec<WindowDefnRaw>,
}
impl_expr2!(Select);

/// A group by clause
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct GroupBy<'script>(pub(crate) GroupByInt<'script>);
#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) enum GroupByInt<'script> {
    /// Expression based group by
    Expr {
        mid: usize,
        expr: ImutExprInt<'script>,
    },
    /// `set` based group by
    Set {
        mid: usize,
        items: Vec<GroupBy<'script>>,
    },
    /// `each` based group by
    Each {
        mid: usize,
        expr: ImutExprInt<'script>,
    },
}

/// A stream statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StreamStmt {
    pub(crate) mid: usize,
    /// ID if the stream
    pub id: String,
}

impl BaseExpr for StreamStmt {
    fn mid(&self) -> usize {
        self.mid
    }
}
