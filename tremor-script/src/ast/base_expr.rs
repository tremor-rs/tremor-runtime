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

use super::*;
use crate::pos::{Location, Range};

#[macro_export]
macro_rules! impl_expr {
    ($name:ident) => {
        impl<'script> BaseExpr for $name<'script> {
            fn s(&self) -> Location {
                self.start
            }

            fn e(&self) -> Location {
                self.end
            }
        }
    };
}

pub trait BaseExpr: Clone {
    fn s(&self) -> Location;
    fn e(&self) -> Location;
    fn extent(&self) -> Range {
        Range(self.s(), self.e())
    }
}

impl BaseExpr for (Location, Location) {
    fn s(&self) -> Location {
        self.0
    }
    fn e(&self) -> Location {
        self.1
    }
}

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
            ImutExpr::Local { start, .. } | ImutExpr::Present { start, .. } => *start,
            ImutExpr::Match(e) => e.s(),
            ImutExpr::Merge(e) => e.s(),
            ImutExpr::Patch(e) => e.s(),
            ImutExpr::Path(e) => e.s(),
            ImutExpr::Record(e) => e.s(),
            ImutExpr::Unary(e) => e.s(),
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
            ImutExpr::Local { end, .. } | ImutExpr::Present { end, .. } => *end,
            ImutExpr::Record(e) => e.e(),
            ImutExpr::Unary(e) => e.e(),
        }
    }
}

impl<'script> BaseExpr for Expr<'script> {
    fn s(&self) -> Location {
        match self {
            Expr::Assign { start, .. }
            | Expr::AssignMoveLocal { start, .. }
            | Expr::Drop { start, .. } => *start,
            Expr::Comprehension(e) => e.s(),
            Expr::Emit(e) => e.s(),
            Expr::Imut(e) => e.s(),
            Expr::Match(e) => e.s(),
            Expr::MergeInPlace(e) => e.s(),
            Expr::PatchInPlace(e) => e.s(),
        }
    }
    fn e(&self) -> Location {
        match self {
            Expr::Assign { end, .. }
            | Expr::AssignMoveLocal { end, .. }
            | Expr::Drop { end, .. } => *end,
            Expr::Comprehension(e) => e.e(),
            Expr::Emit(e) => e.e(),
            Expr::Imut(e) => e.e(),
            Expr::Match(e) => e.e(),
            Expr::MergeInPlace(e) => e.e(),
            Expr::PatchInPlace(e) => e.e(),
        }
    }
}

impl<'script> BaseExpr for Path1<'script> {
    fn s(&self) -> Location {
        match self {
            Path1::Local(e) => e.s(),
            Path1::Meta(e) => e.start,
            Path1::Event(e) => e.start,
        }
    }
    fn e(&self) -> Location {
        match self {
            Path1::Local(e) => e.e(),
            Path1::Meta(e) => e.end,
            Path1::Event(e) => e.end,
        }
    }
}

impl<'script> BaseExpr for Path<'script> {
    fn s(&self) -> Location {
        match self {
            Path::Const(e) | Path::Local(e) => e.s(),
            Path::Meta(e) => e.s(),
            Path::Event(e) => e.s(),
        }
    }
    fn e(&self) -> Location {
        match self {
            Path::Const(e) | Path::Local(e) => e.e(),
            Path::Meta(e) => e.e(),
            Path::Event(e) => e.e(),
        }
    }
}

impl<'script> BaseExpr for Segment<'script> {
    fn s(&self) -> Location {
        match self {
            Self::Id { start, .. } | Self::Idx { start, .. } | Self::Element { start, .. } => {
                *start
            }
            Self::Range { start_lower, .. } => *start_lower,
        }
    }
    fn e(&self) -> Location {
        match self {
            Self::Id { end, .. } | Self::Idx { end, .. } | Self::Element { end, .. } => *end,
            Self::Range { end_upper, .. } => *end_upper,
        }
    }
}

impl<'script> BaseExpr for ImutExpr1<'script> {
    fn s(&self) -> Location {
        match self {
            ImutExpr1::String(e) => e.start,
            ImutExpr1::Binary(e) => e.start,
            ImutExpr1::Comprehension(e) => e.start,
            ImutExpr1::Invoke(e) => e.s(),
            ImutExpr1::InvokeAggr(e) => e.s(),
            ImutExpr1::List(e) => e.s(),
            ImutExpr1::Literal(e) => e.s(),
            ImutExpr1::Match(e) => e.start,
            ImutExpr1::Merge(e) => e.start,
            ImutExpr1::Patch(e) => e.start,
            ImutExpr1::Path(e) => e.s(),
            ImutExpr1::Present { start, .. } => *start,
            ImutExpr1::Record(e) => e.s(),
            ImutExpr1::Unary(e) => e.start,
        }
    }
    fn e(&self) -> Location {
        match self {
            ImutExpr1::String(e) => e.end,
            ImutExpr1::Binary(e) => e.end,
            ImutExpr1::Comprehension(e) => e.end,
            ImutExpr1::Invoke(e) => e.e(),
            ImutExpr1::InvokeAggr(e) => e.e(),
            ImutExpr1::List(e) => e.e(),
            ImutExpr1::Literal(e) => e.e(),
            ImutExpr1::Match(e) => e.end,
            ImutExpr1::Merge(e) => e.end,
            ImutExpr1::Patch(e) => e.end,
            ImutExpr1::Path(e) => e.e(),
            ImutExpr1::Present { end, .. } => *end,
            ImutExpr1::Record(e) => e.e(),
            ImutExpr1::Unary(e) => e.end,
        }
    }
}

impl BaseExpr for TestExpr {
    fn s(&self) -> Location {
        self.start
    }

    fn e(&self) -> Location {
        self.end
    }
}

impl BaseExpr for TestExpr1 {
    fn s(&self) -> Location {
        self.start
    }

    fn e(&self) -> Location {
        self.end
    }
}

impl BaseExpr for InvokeAggr {
    fn s(&self) -> Location {
        self.start
    }
    fn e(&self) -> Location {
        self.end
    }
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
