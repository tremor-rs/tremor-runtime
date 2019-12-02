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

// Don't cover this file it's only getters
#![cfg_attr(tarpaulin, skip)]

use super::raw::*;
use super::*;
use crate::pos::{Location, Range};

#[macro_export]
macro_rules! impl_expr {
    ($name:ident) => {
        impl<'script> BaseExpr for $name<'script> {
            fn s(&self, _meta: &[NodeMeta]) -> Location {
                self.start
            }

            fn e(&self, _meta: &[NodeMeta]) -> Location {
                self.end
            }
            fn mid(&self) -> usize {
                0
            }
        }
    };
}

impl BaseExpr for Range {
    fn s(&self, _meta: &[NodeMeta]) -> Location {
        self.0
    }
    fn e(&self, _meta: &[NodeMeta]) -> Location {
        self.1
    }
    fn mid(&self) -> usize {
        0
    }
}

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

pub trait BaseExpr: Clone {
    fn mid(&self) -> usize;
    fn s(&self, meta: &[NodeMeta]) -> Location {
        meta.get(self.mid()).map(|v| v.start).unwrap_or_default()
    }
    fn e(&self, meta: &[NodeMeta]) -> Location {
        meta.get(self.mid()).map(|v| v.end).unwrap_or_default()
    }

    fn extent(&self, meta: &[NodeMeta]) -> Range {
        Range(self.s(meta), self.e(meta))
    }
}

impl BaseExpr for (Location, Location) {
    fn s(&self, _meta: &[NodeMeta]) -> Location {
        self.0
    }
    fn e(&self, _meta: &[NodeMeta]) -> Location {
        self.1
    }
    fn mid(&self) -> usize {
        0
    }
}

impl<'script> BaseExpr for ImutExpr<'script> {
    fn s(&self, meta: &[NodeMeta]) -> Location {
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
            ImutExpr::Local { mid, .. } | ImutExpr::Present { mid, .. } => {
                meta.get(*mid).map(|v| v.start).unwrap_or_default()
            }
            ImutExpr::Match(e) => e.s(meta),
            ImutExpr::Merge(e) => e.s(meta),
            ImutExpr::Patch(e) => e.s(meta),
            ImutExpr::Path(e) => e.s(meta),
            ImutExpr::Record(e) => e.s(meta),
            ImutExpr::Unary(e) => e.s(meta),
        }
    }
    fn e(&self, meta: &[NodeMeta]) -> Location {
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
            ImutExpr::Local { mid, .. } | ImutExpr::Present { mid, .. } => {
                meta.get(*mid).map(|v| v.end).unwrap_or_default()
            }
            ImutExpr::Record(e) => e.e(meta),
            ImutExpr::Unary(e) => e.e(meta),
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
            ImutExpr::Local { mid, .. } | ImutExpr::Present { mid, .. } => *mid,
            ImutExpr::Record(e) => e.mid(),
            ImutExpr::Unary(e) => e.mid(),
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
    fn s(&self, meta: &[NodeMeta]) -> Location {
        match self {
            PathRaw::Local(e) => e.s(meta),
            PathRaw::Meta(e) => e.start,
            PathRaw::Event(e) => e.start,
        }
    }
    fn e(&self, meta: &[NodeMeta]) -> Location {
        match self {
            PathRaw::Local(e) => e.e(meta),
            PathRaw::Meta(e) => e.end,
            PathRaw::Event(e) => e.end,
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
    fn s(&self, meta: &[NodeMeta]) -> Location {
        match self {
            ImutExprRaw::String(e) => e.start,
            ImutExprRaw::Binary(e) => e.start,
            ImutExprRaw::Comprehension(e) => e.start,
            ImutExprRaw::Invoke(e) => e.s(meta),
            ImutExprRaw::InvokeAggr(e) => e.s(meta),
            ImutExprRaw::List(e) => e.s(meta),
            ImutExprRaw::Literal(e) => e.s(meta),
            ImutExprRaw::Match(e) => e.start,
            ImutExprRaw::Merge(e) => e.start,
            ImutExprRaw::Patch(e) => e.start,
            ImutExprRaw::Path(e) => e.s(meta),
            ImutExprRaw::Present { start, .. } => *start,
            ImutExprRaw::Record(e) => e.s(meta),
            ImutExprRaw::Unary(e) => e.start,
        }
    }
    fn e(&self, meta: &[NodeMeta]) -> Location {
        match self {
            ImutExprRaw::String(e) => e.end,
            ImutExprRaw::Binary(e) => e.end,
            ImutExprRaw::Comprehension(e) => e.end,
            ImutExprRaw::Invoke(e) => e.e(meta),
            ImutExprRaw::InvokeAggr(e) => e.e(meta),
            ImutExprRaw::List(e) => e.e(meta),
            ImutExprRaw::Literal(e) => e.e(meta),
            ImutExprRaw::Match(e) => e.end,
            ImutExprRaw::Merge(e) => e.end,
            ImutExprRaw::Patch(e) => e.end,
            ImutExprRaw::Path(e) => e.e(meta),
            ImutExprRaw::Present { end, .. } => *end,
            ImutExprRaw::Record(e) => e.e(meta),
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
    fn s(&self, _meta: &[NodeMeta]) -> Location {
        self.start
    }

    fn e(&self, _meta: &[NodeMeta]) -> Location {
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
        match self {
            GroupBy::Expr { mid, .. } | GroupBy::Set { mid, .. } | GroupBy::Each { mid, .. } => {
                *mid
            }
        }
    }
}
