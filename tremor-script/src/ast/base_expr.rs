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
// #![cfg_attr(coverage, no_coverage)]
use crate::{
    arena,
    ast::{
        query::raw::StmtRaw,
        raw::{AnyFnRaw, ExprRaw, GroupBy, ImutExprRaw, PathRaw, ReservedPathRaw, TopLevelExprRaw},
        Expr, ImutExpr, NodeMeta, Path, Segment,
    },
    pos::{Location, Span},
};

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
            fn meta(&self) -> &$crate::ast::NodeMeta {
                &self.mid
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
            fn meta(&self) -> &$crate::ast::NodeMeta {
                &self.mid
            }
        }
    };
}

#[doc(hidden)]
/// Implements the BaseExpr trait for a given expression
#[macro_export]
macro_rules! impl_expr {
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
macro_rules! impl_expr_ex {
    ($name:ident) => {
        impl<'script, Ex: Expression + 'script> BaseExpr for $name<'script, Ex> {
            fn meta(&self) -> &NodeMeta {
                &self.mid
            }
        }
    };
}

/// Something that has a start, end and a arena ID
pub trait Ranged {
    /// The start location of the expression
    fn s(&self) -> Location;
    /// The end location of the expression
    fn e(&self) -> Location;
    /// The end location of the expression
    fn aid(&self) -> arena::Index;
    /// The span (range) of the expression
    fn extent(&self) -> Span {
        Span::new(self.s(), self.e())
    }
}

impl Ranged for Span {
    fn s(&self) -> Location {
        self.start()
    }
    fn e(&self) -> Location {
        self.end()
    }
    fn aid(&self) -> arena::Index {
        self.start().aid
    }
}

impl<T> Ranged for T
where
    T: BaseExpr,
{
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
}

/// A Basic expression that can be turned into a location
pub trait BaseExpr: Clone {
    /// Fetches the node meta
    fn meta(&self) -> &NodeMeta;
    /// Name of the element
    fn name(&self) -> Option<&str> {
        self.meta().name.as_deref()
    }
    /// The name or an empty string
    fn name_dflt(&self) -> &str {
        self.name().unwrap_or_default()
    }
}

impl BaseExpr for NodeMeta {
    fn meta(&self) -> &NodeMeta {
        self
    }
}

impl Ranged for (Location, Location) {
    fn s(&self) -> Location {
        self.0
    }
    fn e(&self) -> Location {
        self.1
    }

    fn aid(&self) -> arena::Index {
        self.0.aid
    }
}

// This is a simple accessor
impl<'script> BaseExpr for ImutExpr<'script> {
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
            ImutExpr::Local { mid, .. } | ImutExpr::Present { mid, .. } => mid,
            ImutExpr::Record(e) => e.meta(),
            ImutExpr::Unary(e) => e.meta(),
            ImutExpr::Bytes(e) => e.meta(),
            ImutExpr::String(e) => e.meta(),
            ImutExpr::BinaryBoolean(e) => e.meta(),
        }
    }
}

// This is a simple accessor
impl<'script> BaseExpr for Expr<'script> {
    fn meta(&self) -> &NodeMeta {
        match self {
            Expr::Assign { mid, .. }
            | Expr::AssignMoveLocal { mid, .. }
            | Expr::Drop { mid, .. } => mid,
            Expr::Comprehension(e) => e.meta(),
            Expr::Emit(e) => e.meta(),
            Expr::Imut(e) => e.meta(),
            Expr::Match(e) => e.meta(),
            Expr::IfElse(e) => e.meta(),
        }
    }
}

// This is a simple accessor
impl<'script> BaseExpr for PathRaw<'script> {
    fn meta(&self) -> &NodeMeta {
        match self {
            PathRaw::Local(e) => e.meta(),
            PathRaw::Const(e) => e.meta(),
            PathRaw::Meta(e) => &e.mid,
            PathRaw::Event(e) => &e.mid,
            PathRaw::State(e) => &e.mid,
            PathRaw::Expr(e) => &e.mid,
            PathRaw::Reserved(e) => e.meta(),
        }
    }
}

// This is a simple accessor
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
impl<'script> BaseExpr for Segment<'script> {
    fn meta(&self) -> &NodeMeta {
        match self {
            Self::Id { mid, .. }
            | Self::Idx { mid, .. }
            | Self::Element { mid, .. }
            | Self::Range { mid, .. }
            | Self::RangeExpr { mid, .. } => mid,
        }
    }
}

// This is a simple accessor
impl<'script> BaseExpr for ImutExprRaw<'script> {
    fn meta(&self) -> &NodeMeta {
        match self {
            ImutExprRaw::Binary(e) => &e.mid,
            ImutExprRaw::Comprehension(e) => &e.mid,
            ImutExprRaw::Invoke(e) => e.meta(),
            ImutExprRaw::List(e) => e.meta(),
            ImutExprRaw::Literal(e) => e.meta(),
            ImutExprRaw::Match(e) => &e.mid,
            ImutExprRaw::Merge(e) => &e.mid,
            ImutExprRaw::Patch(e) => &e.mid,
            ImutExprRaw::Path(e) => e.meta(),
            ImutExprRaw::Present { mid, .. } => mid,
            ImutExprRaw::Record(e) => e.meta(),
            ImutExprRaw::Recur(e) => e.meta(),
            ImutExprRaw::String(e) => &e.mid,
            ImutExprRaw::Unary(e) => &e.mid,
            ImutExprRaw::Bytes(e) => &e.mid,
            ImutExprRaw::BinaryBoolean(e) => &e.mid,
        }
    }
}

// This is a simple accessor
impl<'script> BaseExpr for GroupBy<'script> {
    fn meta(&self) -> &NodeMeta {
        match self {
            GroupBy::Expr { mid, .. } | GroupBy::Set { mid, .. } | GroupBy::Each { mid, .. } => mid,
        }
    }
}

impl<'script> BaseExpr for ReservedPathRaw<'script> {
    fn meta(&self) -> &NodeMeta {
        match self {
            ReservedPathRaw::Args { mid, .. }
            | ReservedPathRaw::Window { mid, .. }
            | ReservedPathRaw::Group { mid, .. } => mid,
        }
    }
}

impl<'script> BaseExpr for TopLevelExprRaw<'script> {
    fn meta(&self) -> &NodeMeta {
        match self {
            TopLevelExprRaw::Const(c) => c.meta(),
            TopLevelExprRaw::FnDefn(e) => e.meta(),
            TopLevelExprRaw::Use(e) => e.meta(),
            TopLevelExprRaw::Expr(e) => e.meta(),
        }
    }
}

impl<'script> BaseExpr for ExprRaw<'script> {
    fn meta(&self) -> &NodeMeta {
        match self {
            ExprRaw::Drop { mid, .. } => mid,
            ExprRaw::MatchExpr(e) => e.meta(),
            ExprRaw::Assign(e) => e.meta(),
            ExprRaw::Comprehension(e) => e.meta(),
            ExprRaw::Emit(e) => e.meta(),
            ExprRaw::Imut(e) => e.meta(),
        }
    }
}

impl<'script> BaseExpr for AnyFnRaw<'script> {
    fn meta(&self) -> &NodeMeta {
        match self {
            AnyFnRaw::Match(m) => m.meta(),
            AnyFnRaw::Normal(m) => m.meta(),
        }
    }
}

impl<'script> BaseExpr for StmtRaw<'script> {
    fn meta(&self) -> &NodeMeta {
        match self {
            StmtRaw::OperatorCreate(s) => s.meta(),
            StmtRaw::OperatorDefinition(s) => s.meta(),
            StmtRaw::ScriptCreate(s) => s.meta(),
            StmtRaw::ScriptDefinition(s) => s.meta(),
            StmtRaw::PipelineDefinition(s) => s.meta(),
            StmtRaw::PipelineCreate(s) => s.meta(),
            StmtRaw::SelectStmt(s) => s.meta(),
            StmtRaw::StreamStmt(s) => s.meta(),
            StmtRaw::WindowDefinition(s) => s.meta(),
            StmtRaw::Use(s) => s.meta(),
        }
    }
}
