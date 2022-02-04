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
    arena,
    ast::{
        query::raw::{OperatorKindRaw, StmtRaw},
        raw::{AnyFnRaw, ExprRaw, GroupBy, ImutExprRaw, PathRaw, ReservedPathRaw, TestExprRaw},
        Expr, ImutExpr, InvokeAggr, NodeMeta, Path, Segment, TestExpr,
    },
    pos::{Location, Span},
};

use super::raw::TopLevelExprRaw;

#[doc(hidden)]
/// Implements the BaseExpr trait for a given expression
#[macro_export]
macro_rules! impl_expr_raw {
    ($name:ident) => {
        impl<'script> crate::ast::base_expr::BaseExpr for $name<'script> {
            fn s(&self) -> Location {
                self.start
            }
            fn e(&self) -> Location {
                self.end
            }
            fn meta(&self) -> &crate::ast::NodeMeta {
                todo!()
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
            fn s(&self) -> Location {
                self.start
            }

            fn e(&self) -> Location {
                self.end
            }
            fn meta(&self) -> &crate::ast::NodeMeta {
                todo!()
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
            fn s(&self) -> Location {
                self.start
            }
            fn e(&self) -> Location {
                self.end
            }
            fn meta(&self) -> &crate::ast::NodeMeta {
                todo!()
            }
        }
    };
}

impl BaseExpr for Span {
    fn s(&self) -> Location {
        self.start()
    }
    fn e(&self) -> Location {
        self.end()
    }
    fn aid(&self) -> arena::Index {
        self.start().aid
    }
    fn meta(&self) -> &crate::ast::NodeMeta {
        todo!()
    }
}

#[doc(hidden)]
/// Implements the BaseExpr trait for a given expression
#[macro_export]
macro_rules! impl_expr_mid {
    ($name:ident) => {
        impl<'script> BaseExpr for $name<'script> {
            fn meta(&self) -> &NodeMeta {
                &self.mid
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
            fn meta(&self) -> &NodeMeta {
                &self.mid
            }
        }
    };
}

/// A Basic expression that can be turned into a location
pub trait BaseExpr: Clone {
    /// Fetches the node meta
    fn meta(&self) -> &NodeMeta;

    /// The start location of the expression
    fn s(&self) -> Location {
        self.meta().start()
    }
    /// The end location of the expression
    fn e(&self) -> Location {
        self.meta().end()
    }

    /// The end location of the expression
    fn aid(&self) -> arena::Index {
        self.meta().aid()
    }

    /// The span (range) of the expression
    fn extent(&self) -> Span {
        Span::new(self.s(), self.e())
    }
    /// Name of the element
    fn name(&self) -> Option<&str> {
        self.meta().name.as_deref()
    }
}

impl BaseExpr for (Location, Location) {
    fn s(&self) -> Location {
        self.0
    }
    fn e(&self) -> Location {
        self.1
    }
    fn meta(&self) -> &NodeMeta {
        todo!()
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl<'script> BaseExpr for ImutExpr<'script> {
    fn s(&self) -> Location {
        match self {
            ImutExpr::Binary(e) => e.s(),
            ImutExpr::Comprehension(e) => e.s(),
            ImutExpr::Invoke(e)
            | ImutExpr::Invoke1(e)
            | ImutExpr::Invoke2(e)
            | ImutExpr::Invoke3(e) => e.s(),
            ImutExpr::InvokeAggr(e) => e.s(),
            ImutExpr::List(e) => e.s(),
            ImutExpr::Literal(e) => e.s(),
            ImutExpr::Recur(e) => e.s(),
            ImutExpr::Local { mid, .. } | ImutExpr::Present { mid, .. } => mid.start(),
            ImutExpr::Match(e) => e.s(),
            ImutExpr::Merge(e) => e.s(),
            ImutExpr::Patch(e) => e.s(),
            ImutExpr::Path(e) => e.s(),
            ImutExpr::Record(e) => e.s(),
            ImutExpr::Unary(e) => e.s(),
            ImutExpr::Bytes(e) => e.s(),
            ImutExpr::String(e) => e.s(),
        }
    }

    fn e(&self) -> Location {
        match self {
            ImutExpr::Binary(e) => e.e(),
            ImutExpr::Comprehension(e) => e.e(),
            ImutExpr::Invoke(e)
            | ImutExpr::Invoke1(e)
            | ImutExpr::Invoke2(e)
            | ImutExpr::Invoke3(e) => e.e(),
            ImutExpr::InvokeAggr(e) => e.e(),
            ImutExpr::List(e) => e.e(),
            ImutExpr::Literal(e) => e.e(),
            ImutExpr::Match(e) => e.e(),
            ImutExpr::Merge(e) => e.e(),
            ImutExpr::Patch(e) => e.e(),
            ImutExpr::Path(e) => e.e(),
            ImutExpr::Recur(e) => e.e(),
            ImutExpr::Local { mid, .. } | ImutExpr::Present { mid, .. } => mid.end(),
            ImutExpr::Record(e) => e.e(),
            ImutExpr::Unary(e) => e.e(),
            ImutExpr::Bytes(e) => e.e(),
            ImutExpr::String(e) => e.e(),
        }
    }
    fn meta(&self) -> &NodeMeta {
        match self {
            ImutExpr::Binary(e) => e.meta(),
            ImutExpr::Comprehension(e) => e.meta(),
            ImutExpr::Invoke(e)
            | ImutExpr::Invoke1(e)
            | ImutExpr::Invoke2(e)
            | ImutExpr::Invoke3(e) => e.meta(),
            ImutExpr::InvokeAggr(e) => e.meta(),
            ImutExpr::List(e) => e.meta(),
            ImutExpr::Literal(e) => e.meta(),
            ImutExpr::Match(e) => e.meta(),
            ImutExpr::Merge(e) => e.meta(),
            ImutExpr::Patch(e) => e.meta(),
            ImutExpr::Path(e) => e.meta(),
            ImutExpr::Recur(e) => e.meta(),
            ImutExpr::Local { mid, .. } | ImutExpr::Present { mid, .. } => &mid,
            ImutExpr::Record(e) => e.meta(),
            ImutExpr::Unary(e) => e.meta(),
            ImutExpr::Bytes(e) => e.meta(),
            ImutExpr::String(e) => e.meta(),
        }
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl<'script> BaseExpr for Expr<'script> {
    fn meta(&self) -> &NodeMeta {
        match self {
            Expr::Assign { mid, .. }
            | Expr::AssignMoveLocal { mid, .. }
            | Expr::Drop { mid, .. } => &mid,
            Expr::Comprehension(e) => e.meta(),
            Expr::Emit(e) => e.meta(),
            Expr::Imut(e) => e.meta(),
            Expr::Match(e) => e.meta(),
            Expr::IfElse(e) => e.meta(),
        }
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl<'script> BaseExpr for PathRaw<'script> {
    fn s(&self) -> Location {
        match self {
            PathRaw::Local(e) => e.s(),
            PathRaw::Const(e) => e.s(),
            PathRaw::Meta(e) => e.start,
            PathRaw::Event(e) => e.start,
            PathRaw::State(e) => e.start,
            PathRaw::Expr(e) => e.start,
            PathRaw::Reserved(e) => e.s(),
        }
    }
    fn e(&self) -> Location {
        match self {
            PathRaw::Local(e) => e.e(),
            PathRaw::Const(e) => e.e(),
            PathRaw::Meta(e) => e.end,
            PathRaw::Event(e) => e.end,
            PathRaw::State(e) => e.end,
            PathRaw::Expr(e) => e.end,
            PathRaw::Reserved(e) => e.e(),
        }
    }
    fn meta(&self) -> &NodeMeta {
        todo!()
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl<'script> BaseExpr for Path<'script> {
    fn meta(&self) -> &NodeMeta {
        match self {
            Path::Local(e) => e.meta(),
            Path::Meta(e) => e.meta(),
            Path::Event(e) => e.meta(),
            Path::State(e) => e.meta(),
            Path::Expr(e) => e.meta(),
            Path::Reserved(e) => e.meta(),
        }
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl<'script> BaseExpr for Segment<'script> {
    fn meta(&self) -> &NodeMeta {
        match self {
            Self::Id { mid, .. }
            | Self::Idx { mid, .. }
            | Self::Element { mid, .. }
            | Self::Range { mid, .. }
            | Self::RangeExpr { mid, .. } => &mid,
        }
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl<'script> BaseExpr for ImutExprRaw<'script> {
    fn meta(&self) -> &NodeMeta {
        todo!()
    }
    fn s(&self) -> Location {
        match self {
            ImutExprRaw::Binary(e) => e.start,
            ImutExprRaw::Comprehension(e) => e.start,
            ImutExprRaw::Invoke(e) => e.s(),
            ImutExprRaw::List(e) => e.s(),
            ImutExprRaw::Literal(e) => e.s(),
            ImutExprRaw::Match(e) => e.start,
            ImutExprRaw::Merge(e) => e.start,
            ImutExprRaw::Patch(e) => e.start,
            ImutExprRaw::Path(e) => e.s(),
            ImutExprRaw::Present { start, .. } => *start,
            ImutExprRaw::Record(e) => e.s(),
            ImutExprRaw::Recur(e) => e.s(),
            ImutExprRaw::String(e) => e.start,
            ImutExprRaw::Unary(e) => e.start,
            ImutExprRaw::Bytes(e) => e.start,
        }
    }
    fn e(&self) -> Location {
        match self {
            ImutExprRaw::Binary(e) => e.end,
            ImutExprRaw::Comprehension(e) => e.end,
            ImutExprRaw::Invoke(e) => e.e(),
            ImutExprRaw::List(e) => e.e(),
            ImutExprRaw::Literal(e) => e.e(),
            ImutExprRaw::Match(e) => e.end,
            ImutExprRaw::Merge(e) => e.end,
            ImutExprRaw::Patch(e) => e.end,
            ImutExprRaw::Path(e) => e.e(),
            ImutExprRaw::Present { end, .. } => *end,
            ImutExprRaw::Record(e) => e.e(),
            ImutExprRaw::Recur(e) => e.e(),
            ImutExprRaw::String(e) => e.end,
            ImutExprRaw::Unary(e) => e.end,
            ImutExprRaw::Bytes(e) => e.end,
        }
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl BaseExpr for TestExpr {
    fn meta(&self) -> &NodeMeta {
        &self.mid
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl BaseExpr for TestExprRaw {
    fn s(&self) -> Location {
        self.start
    }

    fn e(&self) -> Location {
        self.end
    }
    fn meta(&self) -> &NodeMeta {
        todo!()
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl BaseExpr for InvokeAggr {
    fn meta(&self) -> &NodeMeta {
        &self.mid
    }
}

// This is a simple accessor
#[cfg(not(tarpaulin_include))]
impl<'script> BaseExpr for GroupBy<'script> {
    fn meta(&self) -> &NodeMeta {
        match self {
            GroupBy::Expr { mid, .. } | GroupBy::Set { mid, .. } | GroupBy::Each { mid, .. } => {
                &mid
            }
        }
    }
}

impl<'script> BaseExpr for ReservedPathRaw<'script> {
    fn s(&self) -> Location {
        match self {
            ReservedPathRaw::Args { start, .. }
            | ReservedPathRaw::Window { start, .. }
            | ReservedPathRaw::Group { start, .. } => *start,
        }
    }

    fn e(&self) -> Location {
        match self {
            ReservedPathRaw::Args { end, .. }
            | ReservedPathRaw::Window { end, .. }
            | ReservedPathRaw::Group { end, .. } => *end,
        }
    }

    fn meta(&self) -> &NodeMeta {
        todo!()
    }
}

impl<'script> BaseExpr for TopLevelExprRaw<'script> {
    fn meta(&self) -> &NodeMeta {
        todo!()
    }

    fn s(&self) -> Location {
        match self {
            TopLevelExprRaw::Const(c) => c.s(),
            TopLevelExprRaw::FnDecl(e) => e.s(),
            TopLevelExprRaw::Use(e) => e.s(),
            TopLevelExprRaw::Expr(e) => e.s(),
        }
    }

    fn e(&self) -> Location {
        match self {
            TopLevelExprRaw::Const(c) => c.e(),
            TopLevelExprRaw::FnDecl(e) => e.e(),
            TopLevelExprRaw::Use(e) => e.e(),
            TopLevelExprRaw::Expr(e) => e.e(),
        }
    }
}

impl<'script> BaseExpr for ExprRaw<'script> {
    fn meta(&self) -> &NodeMeta {
        todo!()
    }

    fn s(&self) -> Location {
        match self {
            ExprRaw::Drop { start, .. } => *start,
            ExprRaw::MatchExpr(e) => e.s(),
            ExprRaw::Assign(e) => e.s(),
            ExprRaw::Comprehension(e) => e.s(),
            ExprRaw::Emit(e) => e.s(),
            ExprRaw::Imut(e) => e.s(),
        }
    }

    fn e(&self) -> Location {
        match self {
            ExprRaw::Drop { end, .. } => *end,
            ExprRaw::MatchExpr(e) => e.e(),
            ExprRaw::Assign(e) => e.e(),
            ExprRaw::Comprehension(e) => e.e(),
            ExprRaw::Emit(e) => e.e(),
            ExprRaw::Imut(e) => e.e(),
        }
    }
}

impl<'script> BaseExpr for AnyFnRaw<'script> {
    fn meta(&self) -> &NodeMeta {
        todo!()
    }
    fn s(&self) -> Location {
        match self {
            AnyFnRaw::Match(m) => m.start,
            AnyFnRaw::Normal(m) => m.start,
        }
    }
    fn e(&self) -> Location {
        match self {
            AnyFnRaw::Match(m) => m.end,
            AnyFnRaw::Normal(m) => m.end,
        }
    }
}

impl<'script> BaseExpr for StmtRaw<'script> {
    fn meta(&self) -> &NodeMeta {
        todo!()
    }
    fn s(&self) -> Location {
        match self {
            StmtRaw::OperatorCreate(s) => s.start,
            StmtRaw::OperatorDefinition(s) => s.start,
            StmtRaw::ScriptCreate(s) => s.start,
            StmtRaw::ScriptDefinition(s) => s.start,
            StmtRaw::PipelineDefinition(s) => s.start,
            StmtRaw::PipelineCreate(s) => s.start,
            StmtRaw::SelectStmt(s) => s.start,
            StmtRaw::StreamStmt(s) => s.start,
            StmtRaw::WindowDefinition(s) => s.start,
            StmtRaw::Use(s) => s.s(),
        }
    }
    fn e(&self) -> Location {
        match self {
            StmtRaw::OperatorCreate(e) => e.end,
            StmtRaw::OperatorDefinition(e) => e.end,
            StmtRaw::ScriptCreate(e) => e.end,
            StmtRaw::ScriptDefinition(e) => e.end,
            StmtRaw::PipelineDefinition(e) => e.end,
            StmtRaw::PipelineCreate(e) => e.end,
            StmtRaw::SelectStmt(e) => e.end,
            StmtRaw::StreamStmt(e) => e.end,
            StmtRaw::WindowDefinition(e) => e.end,
            StmtRaw::Use(e) => e.e(),
        }
    }
}

impl BaseExpr for OperatorKindRaw {
    fn s(&self) -> Location {
        self.start
    }
    fn e(&self) -> Location {
        self.end
    }
    fn meta(&self) -> &NodeMeta {
        todo!()
    }
}
