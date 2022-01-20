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

// Don't cover this file it's only getters
#![cfg(not(tarpaulin_include))]

use crate::{
    ast::{
        query::raw::{OperatorKindRaw, StmtRaw},
        raw::{AnyFnRaw, ExprRaw, GroupBy, ImutExprRaw, PathRaw, ReservedPathRaw, TestExprRaw},
        Expr, ImutExpr, InvokeAggr, NodeMetas, Path, Segment, TestExpr,
    },
    pos::{Location, Range},
};

use super::raw::TopLevelExprRaw;

#[doc(hidden)]
/// Implements the BaseExpr trait for a given expression
#[macro_export]
macro_rules! impl_expr {
    ($name:ident) => {
        impl<'script> crate::ast::base_expr::BaseExpr for $name<'script> {
            fn s(&self, _meta: &crate::ast::NodeMetas) -> Location {
                self.start
            }
            fn e(&self, _meta: &crate::ast::NodeMetas) -> Location {
                self.end
            }
            fn mid(&self) -> usize {
                0
            }
        }
    };
}
#[doc(hidden)]
/// Implements the BaseExpr trait for a given expression
#[macro_export]
macro_rules! impl_expr_exraw {
    ($name:ident) => {
        impl<'script, Ex> BaseExpr for $name<'script, Ex>
        where
            <Ex as Upable<'script>>::Target: Expression + 'script,
            Ex: ExpressionRaw<'script> + 'script,
        {
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
    };
}

#[doc(hidden)]
/// Implements the BaseExpr trait for a given expression
#[macro_export]
macro_rules! impl_expr_no_lt {
    ($name:ident) => {
        impl BaseExpr for $name {
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
    };
}

impl BaseExpr for Range {
    fn s(&self, _meta: &NodeMetas) -> Location {
        self.0
    }
    fn e(&self, _meta: &NodeMetas) -> Location {
        self.1
    }
    fn mid(&self) -> usize {
        0
    }
}

#[doc(hidden)]
/// Implements the BaseExpr trait for a given expression
#[macro_export]
macro_rules! impl_expr_mid {
    ($name:ident) => {
        impl<'script> BaseExpr for $name<'script> {
            fn mid(&self) -> usize {
                self.mid
            }
        }
    };
}

#[doc(hidden)]
/// Implements the BaseExpr trait for a given expression that generalizes over Expression
#[macro_export]
macro_rules! impl_expr_ex_mid {
    ($name:ident) => {
        impl<'script, Ex: Expression + 'script> BaseExpr for $name<'script, Ex> {
            fn mid(&self) -> usize {
                self.mid
            }
        }
    };
}

/// A Basic expression that can be turned into a location
pub trait BaseExpr: Clone {
    /// Obtain the metadata id of the expression
    fn mid(&self) -> usize;

    /// The start location of the expression
    fn s(&self, meta: &NodeMetas) -> Location {
        meta.start(self.mid()).unwrap_or_default()
    }
    /// The end location of the expression
    fn e(&self, meta: &NodeMetas) -> Location {
        meta.end(self.mid()).unwrap_or_default()
    }
    /// The span (range) of the expression
    fn extent(&self, meta: &NodeMetas) -> Range {
        Range(self.s(meta), self.e(meta))
    }
}

impl BaseExpr for (Location, Location) {
    fn s(&self, _meta: &NodeMetas) -> Location {
        self.0
    }
    fn e(&self, _meta: &NodeMetas) -> Location {
        self.1
    }
    fn mid(&self) -> usize {
        0
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl<'script> BaseExpr for ImutExpr<'script> {
    fn s(&self, meta: &NodeMetas) -> Location {
        match self {
            ImutExpr::Binary(e) => e.s(meta),
            ImutExpr::Comprehension(e) => e.s(meta),
            ImutExpr::Invoke(e)
            | ImutExpr::Invoke1(e)
            | ImutExpr::Invoke2(e)
            | ImutExpr::Invoke3(e) => e.s(meta),
            ImutExpr::InvokeAggr(e) => e.s(meta),
            ImutExpr::List(e) => e.s(meta),
            ImutExpr::Literal(e) => e.s(meta),
            ImutExpr::Recur(e) => e.s(meta),
            ImutExpr::Local { mid, .. } | ImutExpr::Present { mid, .. } => {
                meta.start(*mid).unwrap_or_default()
            }
            ImutExpr::Match(e) => e.s(meta),
            ImutExpr::Merge(e) => e.s(meta),
            ImutExpr::Patch(e) => e.s(meta),
            ImutExpr::Path(e) => e.s(meta),
            ImutExpr::Record(e) => e.s(meta),
            ImutExpr::Unary(e) => e.s(meta),
            ImutExpr::Bytes(e) => e.s(meta),
            ImutExpr::String(e) => e.s(meta),
        }
    }

    fn e(&self, meta: &NodeMetas) -> Location {
        match self {
            ImutExpr::Binary(e) => e.e(meta),
            ImutExpr::Comprehension(e) => e.e(meta),
            ImutExpr::Invoke(e)
            | ImutExpr::Invoke1(e)
            | ImutExpr::Invoke2(e)
            | ImutExpr::Invoke3(e) => e.e(meta),
            ImutExpr::InvokeAggr(e) => e.e(meta),
            ImutExpr::List(e) => e.e(meta),
            ImutExpr::Literal(e) => e.e(meta),
            ImutExpr::Match(e) => e.e(meta),
            ImutExpr::Merge(e) => e.e(meta),
            ImutExpr::Patch(e) => e.e(meta),
            ImutExpr::Path(e) => e.e(meta),
            ImutExpr::Recur(e) => e.e(meta),
            ImutExpr::Local { mid, .. } | ImutExpr::Present { mid, .. } => {
                meta.end(*mid).unwrap_or_default()
            }
            ImutExpr::Record(e) => e.e(meta),
            ImutExpr::Unary(e) => e.e(meta),
            ImutExpr::Bytes(e) => e.e(meta),
            ImutExpr::String(e) => e.e(meta),
        }
    }
    fn mid(&self) -> usize {
        match self {
            ImutExpr::Binary(e) => e.mid(),
            ImutExpr::Comprehension(e) => e.mid(),
            ImutExpr::Invoke(e)
            | ImutExpr::Invoke1(e)
            | ImutExpr::Invoke2(e)
            | ImutExpr::Invoke3(e) => e.mid(),
            ImutExpr::InvokeAggr(e) => e.mid(),
            ImutExpr::List(e) => e.mid(),
            ImutExpr::Literal(e) => e.mid(),
            ImutExpr::Match(e) => e.mid(),
            ImutExpr::Merge(e) => e.mid(),
            ImutExpr::Patch(e) => e.mid(),
            ImutExpr::Path(e) => e.mid(),
            ImutExpr::Recur(e) => e.mid(),
            ImutExpr::Local { mid, .. } | ImutExpr::Present { mid, .. } => *mid,
            ImutExpr::Record(e) => e.mid(),
            ImutExpr::Unary(e) => e.mid(),
            ImutExpr::Bytes(e) => e.mid(),
            ImutExpr::String(e) => e.mid(),
        }
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl<'script> BaseExpr for Expr<'script> {
    fn mid(&self) -> usize {
        match self {
            Expr::Assign { mid, .. }
            | Expr::AssignMoveLocal { mid, .. }
            | Expr::Drop { mid, .. } => *mid,
            Expr::Comprehension(e) => e.mid(),
            Expr::Emit(e) => e.mid(),
            Expr::Imut(e) => e.mid(),
            Expr::Match(e) => e.mid(),
            Expr::IfElse(e) => e.mid(),
        }
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl<'script> BaseExpr for PathRaw<'script> {
    fn s(&self, meta: &NodeMetas) -> Location {
        match self {
            PathRaw::Local(e) => e.s(meta),
            PathRaw::Const(e) => e.s(meta),
            PathRaw::Meta(e) => e.start,
            PathRaw::Event(e) => e.start,
            PathRaw::State(e) => e.start,
            PathRaw::Expr(e) => e.start,
            PathRaw::Reserved(e) => e.s(meta),
        }
    }
    fn e(&self, meta: &NodeMetas) -> Location {
        match self {
            PathRaw::Local(e) => e.e(meta),
            PathRaw::Const(e) => e.e(meta),
            PathRaw::Meta(e) => e.end,
            PathRaw::Event(e) => e.end,
            PathRaw::State(e) => e.end,
            PathRaw::Expr(e) => e.end,
            PathRaw::Reserved(e) => e.e(meta),
        }
    }
    fn mid(&self) -> usize {
        0
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl<'script> BaseExpr for Path<'script> {
    fn mid(&self) -> usize {
        match self {
            Path::Const(e) | Path::Local(e) => e.mid(),
            Path::Meta(e) => e.mid(),
            Path::Event(e) => e.mid(),
            Path::State(e) => e.mid(),
            Path::Expr(e) => e.mid(),
            Path::Reserved(e) => e.mid(),
        }
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl<'script> BaseExpr for Segment<'script> {
    fn mid(&self) -> usize {
        match self {
            Self::Id { mid, .. }
            | Self::Idx { mid, .. }
            | Self::Element { mid, .. }
            | Self::Range { mid, .. }
            | Self::RangeExpr { mid, .. } => *mid,
        }
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl<'script> BaseExpr for ImutExprRaw<'script> {
    fn mid(&self) -> usize {
        0
    }
    fn s(&self, meta: &NodeMetas) -> Location {
        match self {
            ImutExprRaw::Binary(e) => e.start,
            ImutExprRaw::Comprehension(e) => e.start,
            ImutExprRaw::Invoke(e) => e.s(meta),
            ImutExprRaw::List(e) => e.s(meta),
            ImutExprRaw::Literal(e) => e.s(meta),
            ImutExprRaw::Match(e) => e.start,
            ImutExprRaw::Merge(e) => e.start,
            ImutExprRaw::Patch(e) => e.start,
            ImutExprRaw::Path(e) => e.s(meta),
            ImutExprRaw::Present { start, .. } => *start,
            ImutExprRaw::Record(e) => e.s(meta),
            ImutExprRaw::Recur(e) => e.s(meta),
            ImutExprRaw::String(e) => e.start,
            ImutExprRaw::Unary(e) => e.start,
            ImutExprRaw::Bytes(e) => e.start,
        }
    }
    fn e(&self, meta: &NodeMetas) -> Location {
        match self {
            ImutExprRaw::Binary(e) => e.end,
            ImutExprRaw::Comprehension(e) => e.end,
            ImutExprRaw::Invoke(e) => e.e(meta),
            ImutExprRaw::List(e) => e.e(meta),
            ImutExprRaw::Literal(e) => e.e(meta),
            ImutExprRaw::Match(e) => e.end,
            ImutExprRaw::Merge(e) => e.end,
            ImutExprRaw::Patch(e) => e.end,
            ImutExprRaw::Path(e) => e.e(meta),
            ImutExprRaw::Present { end, .. } => *end,
            ImutExprRaw::Record(e) => e.e(meta),
            ImutExprRaw::Recur(e) => e.e(meta),
            ImutExprRaw::String(e) => e.end,
            ImutExprRaw::Unary(e) => e.end,
            ImutExprRaw::Bytes(e) => e.end,
        }
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl BaseExpr for TestExpr {
    fn mid(&self) -> usize {
        self.mid
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl BaseExpr for TestExprRaw {
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

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl BaseExpr for InvokeAggr {
    fn mid(&self) -> usize {
        self.mid
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl<'script> BaseExpr for GroupBy<'script> {
    fn mid(&self) -> usize {
        match self {
            GroupBy::Expr { mid, .. } | GroupBy::Set { mid, .. } | GroupBy::Each { mid, .. } => {
                *mid
            }
        }
    }
}

impl<'script> BaseExpr for ReservedPathRaw<'script> {
    fn s(&self, _meta: &NodeMetas) -> Location {
        match self {
            ReservedPathRaw::Args { start, .. }
            | ReservedPathRaw::Window { start, .. }
            | ReservedPathRaw::Group { start, .. } => *start,
        }
    }

    fn e(&self, _meta: &NodeMetas) -> Location {
        match self {
            ReservedPathRaw::Args { end, .. }
            | ReservedPathRaw::Window { end, .. }
            | ReservedPathRaw::Group { end, .. } => *end,
        }
    }

    fn mid(&self) -> usize {
        0
    }
}

impl<'script> BaseExpr for TopLevelExprRaw<'script> {
    fn mid(&self) -> usize {
        0
    }

    fn s(&self, meta: &NodeMetas) -> Location {
        match self {
            TopLevelExprRaw::Const(c) => c.s(meta),
            TopLevelExprRaw::Module(e) => e.s(meta),
            TopLevelExprRaw::FnDecl(e) => e.s(meta),
            TopLevelExprRaw::Use(e) => e.s(meta),
            TopLevelExprRaw::Expr(e) => e.s(meta),
        }
    }

    fn e(&self, meta: &NodeMetas) -> Location {
        match self {
            TopLevelExprRaw::Const(c) => c.e(meta),
            TopLevelExprRaw::Module(e) => e.e(meta),
            TopLevelExprRaw::FnDecl(e) => e.e(meta),
            TopLevelExprRaw::Use(e) => e.e(meta),
            TopLevelExprRaw::Expr(e) => e.e(meta),
        }
    }
}

impl<'script> BaseExpr for ExprRaw<'script> {
    fn mid(&self) -> usize {
        0
    }

    fn s(&self, meta: &NodeMetas) -> Location {
        match self {
            ExprRaw::Drop { start, .. } => *start,
            ExprRaw::MatchExpr(e) => e.s(meta),
            ExprRaw::Assign(e) => e.s(meta),
            ExprRaw::Comprehension(e) => e.s(meta),
            ExprRaw::Emit(e) => e.s(meta),
            ExprRaw::Imut(e) => e.s(meta),
        }
    }

    fn e(&self, meta: &NodeMetas) -> Location {
        match self {
            ExprRaw::Drop { end, .. } => *end,
            ExprRaw::MatchExpr(e) => e.e(meta),
            ExprRaw::Assign(e) => e.e(meta),
            ExprRaw::Comprehension(e) => e.e(meta),
            ExprRaw::Emit(e) => e.e(meta),
            ExprRaw::Imut(e) => e.e(meta),
        }
    }
}

impl<'script> BaseExpr for AnyFnRaw<'script> {
    fn mid(&self) -> usize {
        0
    }
    fn s(&self, _meta: &NodeMetas) -> Location {
        match self {
            AnyFnRaw::Match(m) => m.start,
            AnyFnRaw::Normal(m) => m.start,
        }
    }
    fn e(&self, _meta: &NodeMetas) -> Location {
        match self {
            AnyFnRaw::Match(m) => m.end,
            AnyFnRaw::Normal(m) => m.end,
        }
    }
}

impl<'script> BaseExpr for StmtRaw<'script> {
    fn mid(&self) -> usize {
        0
    }
    fn s(&self, meta: &NodeMetas) -> Location {
        match self {
            StmtRaw::ModuleStmt(s) => s.s(meta),
            StmtRaw::OperatorCreate(s) => s.start,
            StmtRaw::OperatorDefinition(s) => s.start,
            StmtRaw::ScriptCreate(s) => s.start,
            StmtRaw::ScriptDefinition(s) => s.start,
            StmtRaw::PipelineDefinition(s) => s.start,
            StmtRaw::PipelineCreate(s) => s.start,
            StmtRaw::SelectStmt(s) => s.start,
            StmtRaw::StreamStmt(s) => s.start,
            StmtRaw::WindowDefinition(s) => s.start,
            StmtRaw::Expr(s) => s.s(meta),
        }
    }
    fn e(&self, meta: &NodeMetas) -> Location {
        match self {
            StmtRaw::ModuleStmt(e) => e.e(meta),
            StmtRaw::OperatorCreate(e) => e.end,
            StmtRaw::OperatorDefinition(e) => e.end,
            StmtRaw::ScriptCreate(e) => e.end,
            StmtRaw::ScriptDefinition(e) => e.end,
            StmtRaw::PipelineDefinition(e) => e.end,
            StmtRaw::PipelineCreate(e) => e.end,
            StmtRaw::SelectStmt(e) => e.end,
            StmtRaw::StreamStmt(e) => e.end,
            StmtRaw::WindowDefinition(e) => e.end,
            StmtRaw::Expr(e) => e.e(meta),
        }
    }
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
