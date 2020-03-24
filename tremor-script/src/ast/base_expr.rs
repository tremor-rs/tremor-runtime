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

// Don't cover this file it's only getters
#![cfg_attr(tarpaulin, skip)]

use super::raw::*;
use super::*;
use crate::pos::{Location, Range};

#[doc(hidden)]
/// Implements the BaseExpr trait for a given expression
#[macro_export]
macro_rules! impl_expr {
    ($name:ident) => {
        impl<'script> BaseExpr for $name<'script> {
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
macro_rules! impl_expr2 {
    ($name:ident) => {
        impl<'script> BaseExpr for $name<'script> {
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

impl<'script> BaseExpr for ImutExprInt<'script> {
    fn s(&self, meta: &NodeMetas) -> Location {
        match self {
            ImutExprInt::Binary(e) => e.s(meta),
            ImutExprInt::Comprehension(e) => e.s(meta),
            ImutExprInt::Invoke(e)
            | ImutExprInt::Invoke1(e)
            | ImutExprInt::Invoke2(e)
            | ImutExprInt::Invoke3(e) => e.s(meta),
            ImutExprInt::InvokeAggr(e) => e.s(meta),
            ImutExprInt::List(e) => e.s(meta),
            ImutExprInt::Literal(e) => e.s(meta),
            ImutExprInt::Recur(e) => e.s(meta),
            ImutExprInt::Local { mid, .. } | ImutExprInt::Present { mid, .. } => {
                meta.start(*mid).unwrap_or_default()
            }
            ImutExprInt::Match(e) => e.s(meta),
            ImutExprInt::Merge(e) => e.s(meta),
            ImutExprInt::Patch(e) => e.s(meta),
            ImutExprInt::Path(e) => e.s(meta),
            ImutExprInt::Record(e) => e.s(meta),
            ImutExprInt::Unary(e) => e.s(meta),
        }
    }
    fn e(&self, meta: &NodeMetas) -> Location {
        match self {
            ImutExprInt::Binary(e) => e.e(meta),
            ImutExprInt::Comprehension(e) => e.e(meta),
            ImutExprInt::Invoke(e)
            | ImutExprInt::Invoke1(e)
            | ImutExprInt::Invoke2(e)
            | ImutExprInt::Invoke3(e) => e.e(meta),
            ImutExprInt::InvokeAggr(e) => e.e(meta),
            ImutExprInt::List(e) => e.e(meta),
            ImutExprInt::Literal(e) => e.e(meta),
            ImutExprInt::Match(e) => e.e(meta),
            ImutExprInt::Merge(e) => e.e(meta),
            ImutExprInt::Patch(e) => e.e(meta),
            ImutExprInt::Path(e) => e.e(meta),
            ImutExprInt::Recur(e) => e.e(meta),
            ImutExprInt::Local { mid, .. } | ImutExprInt::Present { mid, .. } => {
                meta.end(*mid).unwrap_or_default()
            }
            ImutExprInt::Record(e) => e.e(meta),
            ImutExprInt::Unary(e) => e.e(meta),
        }
    }
    fn mid(&self) -> usize {
        match self {
            ImutExprInt::Binary(e) => e.mid(),
            ImutExprInt::Comprehension(e) => e.mid(),
            ImutExprInt::Invoke(e)
            | ImutExprInt::Invoke1(e)
            | ImutExprInt::Invoke2(e)
            | ImutExprInt::Invoke3(e) => e.mid(),
            ImutExprInt::InvokeAggr(e) => e.mid(),
            ImutExprInt::List(e) => e.mid(),
            ImutExprInt::Literal(e) => e.mid(),
            ImutExprInt::Match(e) => e.mid(),
            ImutExprInt::Merge(e) => e.mid(),
            ImutExprInt::Patch(e) => e.mid(),
            ImutExprInt::Path(e) => e.mid(),
            ImutExprInt::Recur(e) => e.mid(),
            ImutExprInt::Local { mid, .. } | ImutExprInt::Present { mid, .. } => *mid,
            ImutExprInt::Record(e) => e.mid(),
            ImutExprInt::Unary(e) => e.mid(),
        }
    }
}

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
            Expr::MergeInPlace(e) => e.mid(),
            Expr::PatchInPlace(e) => e.mid(),
        }
    }
}

impl<'script> BaseExpr for PathRaw<'script> {
    fn s(&self, meta: &NodeMetas) -> Location {
        match self {
            PathRaw::Local(e) => e.s(meta),
            PathRaw::Const(e) => e.s(meta),
            PathRaw::Meta(e) => e.start,
            PathRaw::Event(e) => e.start,
            PathRaw::State(e) => e.start,
        }
    }
    fn e(&self, meta: &NodeMetas) -> Location {
        match self {
            PathRaw::Local(e) => e.e(meta),
            PathRaw::Const(e) => e.e(meta),
            PathRaw::Meta(e) => e.end,
            PathRaw::Event(e) => e.end,
            PathRaw::State(e) => e.end,
        }
    }
    fn mid(&self) -> usize {
        0
    }
}

impl<'script> BaseExpr for Path<'script> {
    fn mid(&self) -> usize {
        match self {
            Path::Const(e) | Path::Local(e) => e.mid(),
            Path::Meta(e) => e.mid(),
            Path::Event(e) => e.mid(),
            Path::State(e) => e.mid(),
        }
    }
}

impl<'script> BaseExpr for Segment<'script> {
    fn mid(&self) -> usize {
        match self {
            Self::Id { mid, .. }
            | Self::Idx { mid, .. }
            | Self::Element { mid, .. }
            | Self::Range { mid, .. } => *mid,
        }
    }
}

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
        }
    }
}

impl BaseExpr for TestExpr {
    fn mid(&self) -> usize {
        self.mid
    }
}

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

impl BaseExpr for InvokeAggr {
    fn mid(&self) -> usize {
        self.mid
    }
}

impl<'script> BaseExpr for GroupBy<'script> {
    fn mid(&self) -> usize {
        self.0.mid()
    }
}

impl<'script> BaseExpr for GroupByInt<'script> {
    fn mid(&self) -> usize {
        match self {
            GroupByInt::Expr { mid, .. }
            | GroupByInt::Set { mid, .. }
            | GroupByInt::Each { mid, .. } => *mid,
        }
    }
}
