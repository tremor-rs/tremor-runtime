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

pub(crate) mod raw;
use super::{
    error_generic, error_no_consts, error_no_locals, node_id::NodeId, AggrRegistry, EventPath,
    HashMap, Helper, Ident, ImutExpr, ImutExprInt, InvokeAggrFn, Location, NodeMetas, Path,
    Registry, Result, Script, Serialize, Stmts, Upable, Value,
};
use super::{raw::BaseExpr, Consts};
use crate::ast::base_ref::BaseRef;
use crate::ast::eq::AstEq;
use crate::{impl_expr_mid, impl_fqn};
use raw::WindowDefnRaw;

/// A Tremor query
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Query<'script> {
    /// Config for the query
    pub config: HashMap<String, Value<'script>>,
    /// Statements
    pub stmts: Stmts<'script>,
    /// Query Node Metadata
    pub node_meta: NodeMetas,
    /// Window declarations
    pub windows: HashMap<String, WindowDecl<'script>>,
    /// Script declarations
    pub scripts: HashMap<String, ScriptDecl<'script>>,
    /// Operators declarations
    pub operators: HashMap<String, OperatorDecl<'script>>,
    /// Query arguments
    pub args: Value<'script>,
}

/// Query statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Stmt<'script> {
    /// A window declaration
    WindowDecl(Box<WindowDecl<'script>>),
    /// A stream
    Stream(StreamStmt),
    /// An operator declaration
    OperatorDecl(OperatorDecl<'script>),
    /// A script declaration
    ScriptDecl(Box<ScriptDecl<'script>>),
    /// An operator creation
    Operator(OperatorStmt<'script>),
    /// A script creation
    Script(ScriptStmt<'script>),
    /// An pipeline declaration
    PipelineDecl(PipelineDecl<'script>),
    /// An pipeline creation
    PipelineStmt(PipelineStmt),
    /// A select statement
    Select(SelectStmt<'script>),
}

#[cfg(not(tarpaulin_include))] // this is a simple passthrough
impl<'script> BaseExpr for Stmt<'script> {
    fn mid(&self) -> usize {
        match self {
            Stmt::WindowDecl(s) => s.mid(),
            Stmt::Stream(s) => s.mid(),
            Stmt::OperatorDecl(s) => s.mid(),
            Stmt::ScriptDecl(s) => s.mid(),
            Stmt::PipelineDecl(s) => s.mid(),
            Stmt::PipelineStmt(s) => s.mid(),
            Stmt::Operator(s) => s.mid(),
            Stmt::Script(s) => s.mid(),
            Stmt::Select(s) => s.mid(),
        }
    }
}

/// array of aggregate functions
pub type Aggregates<'f> = Vec<InvokeAggrFn<'f>>;
/// array of aggregate functions (as slice)
pub type AggrSlice<'f> = [InvokeAggrFn<'f>];

///
/// A Select statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SelectStmt<'script> {
    /// The select statement
    pub stmt: Box<Select<'script>>,
    /// Aggregates
    pub aggregates: Aggregates<'static>,
    /// Constants
    pub consts: Consts<'script>,
    /// Number of locals
    pub locals: usize,
    /// Node metadata nodes
    pub node_meta: NodeMetas,
}
#[cfg(not(tarpaulin_include))] // this is a simple passthrough
impl<'script> BaseExpr for SelectStmt<'script> {
    fn mid(&self) -> usize {
        self.stmt.mid()
    }
}

/// The type of a select statement
pub enum SelectType {
    /// This select statement can be turned
    /// into a passthrough node
    Passthrough,
    /// This is a simple statement without grouping
    /// or windowing
    Simple,
    /// This is a full fledged select statement
    Normal,
}

impl SelectStmt<'_> {
    /// Determine how complex a select statement is
    #[must_use]
    pub fn complexity(&self) -> SelectType {
        if self
            .stmt
            .target
            .0
            .ast_eq(&ImutExprInt::Path(Path::Event(EventPath {
                mid: 0,
                segments: vec![],
            })))
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

/// Operator kind identifier
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorKind {
    pub(crate) mid: usize,
    /// Module of the operator
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
    /// The ID and Module of the Operator
    pub node_id: NodeId,
    /// metadata id
    pub(crate) mid: usize,
    /// Type of the operator
    pub kind: OperatorKind,
    /// Parameters for the operator
    pub params: Option<HashMap<String, Value<'script>>>,
}
impl_expr_mid!(OperatorDecl);
impl_fqn!(OperatorDecl);

/// An operator creation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorStmt<'script> {
    /// The ID and Module of the Operator
    pub node_id: NodeId,
    /// metadata id
    pub(crate) mid: usize,
    /// Target of the operator
    pub target: String,
    /// parameters of the instance
    pub params: Option<HashMap<String, Value<'script>>>,
}
impl_expr_mid!(OperatorStmt);

/// A script declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptDecl<'script> {
    pub(crate) mid: usize,
    /// The ID and Module of the Script
    pub node_id: NodeId,
    /// Parameters of a script declaration
    pub params: Option<HashMap<String, Value<'script>>>,
    /// The script itself
    pub script: Script<'script>,
}
impl_expr_mid!(ScriptDecl);

/// A script creation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptStmt<'script> {
    /// The ID and Module of the Script
    pub node_id: NodeId,
    /// metadata id
    pub(crate) mid: usize,
    /// Target of the script
    pub target: String,
    /// Parameters of the script statement
    pub params: Option<HashMap<String, Value<'script>>>,
}
impl_expr_mid!(ScriptStmt);

/// A pipeline declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PipelineDecl<'script> {
    /// The ID and Module of the SubqueryDecl
    pub node_id: NodeId,
    /// metadata id
    pub(crate) mid: usize,
    /// Parameters of a subquery declaration
    pub params: Option<HashMap<String, Value<'script>>>,
    /// Input Ports
    pub from: Vec<Ident<'script>>,
    /// Output Ports
    pub into: Vec<Ident<'script>>,
    /// The raw pipeline statements
    pub raw_stmts: raw::StmtsRaw<'script>,
    /// The query in it's runnable form
    pub query: Query<'script>,
}
impl_expr_mid!(PipelineDecl);
impl_fqn!(PipelineDecl);

/// A pipeline creation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PipelineStmt {
    pub(crate) mid: usize,
    /// Module of the pipeline
    pub module: Vec<String>,
    /// ID of the pipeline
    pub id: String,
    /// Map of pipeline ports and internal stream id
    pub port_stream_map: HashMap<String, String>,
}
impl BaseExpr for PipelineStmt {
    fn mid(&self) -> usize {
        self.mid
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum WindowKind {
    /// we're forced to make this pub because of lalrpop
    Sliding,
    /// we're forced to make this pub because of lalrpop
    Tumbling,
}

/// A window declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowDecl<'script> {
    /// ID and Module of the Window
    pub node_id: NodeId,
    /// metadata id
    pub(crate) mid: usize,
    /// The type of window
    pub kind: WindowKind,
    /// Parameters passed to the window
    pub params: HashMap<String, Value<'script>>,
    /// The script of the window
    pub script: Option<Script<'script>>,
}
impl_expr_mid!(WindowDecl);
impl_fqn!(WindowDecl);

impl<'script> WindowDecl<'script> {
    /// `emit_empty_windows` setting
    pub const EMIT_EMPTY_WINDOWS: &'static str = "emit_empty_windows";
    /// `max_groups` setting
    pub const MAX_GROUPS: &'static str = "max_groups";
    /// `interval` setting
    pub const INTERVAL: &'static str = "interval";
    /// `size` setting
    pub const SIZE: &'static str = "size";
}

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
    /// Where clause
    pub maybe_where: Option<ImutExpr<'script>>,
    /// Having clause
    pub maybe_having: Option<ImutExpr<'script>>,
    /// Group-By clause
    pub maybe_group_by: Option<GroupBy<'script>>,
    /// Window
    pub windows: Vec<WindowDefnRaw<'script>>,
}
impl_expr_mid!(Select);

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
