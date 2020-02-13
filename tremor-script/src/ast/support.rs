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

// Don't cover this file it's only simple helpers
#![cfg_attr(tarpaulin, skip)]

use super::*;
use std::fmt;

impl<'script> fmt::Debug for InvokeAggrFn<'script> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "fn(aggr) {}::{}", self.module, self.fun)
    }
}

impl<'script> PartialEq for InvokeAggrFn<'script> {
    fn eq(&self, other: &Self) -> bool {
        self.module == other.module && self.fun == other.fun && self.args == other.args
    }
}

impl<'script> PartialEq for Segment<'script> {
    fn eq(&self, other: &Self) -> bool {
        use Segment::*;
        match (self, other) {
            (Id { mid: id1, .. }, Id { mid: id2, .. }) => id1 == id2,
            (Idx { idx: idx1, .. }, Idx { idx: idx2, .. }) => idx1 == idx2,
            (Element { expr: expr1, .. }, Element { expr: expr2, .. }) => expr1 == expr2,
            (
                Range {
                    range_start: start1,
                    range_end: end1,
                    ..
                },
                Range {
                    range_start: start2,
                    range_end: end2,
                    ..
                },
            ) => start1 == start2 && end1 == end2,
            _ => false,
        }
    }
}
impl<'script> PartialEq for LocalPath<'script> {
    fn eq(&self, other: &Self) -> bool {
        self.idx == other.idx && self.is_const == other.is_const && self.segments == other.segments
    }
}

impl<'script> PartialEq for MetadataPath<'script> {
    fn eq(&self, other: &Self) -> bool {
        self.segments == other.segments
    }
}

impl<'script> PartialEq for EventPath<'script> {
    fn eq(&self, other: &Self) -> bool {
        self.segments == other.segments
    }
}

impl<'script> PartialEq for StatePath<'script> {
    fn eq(&self, other: &Self) -> bool {
        self.segments == other.segments
    }
}

impl fmt::Display for BinOpKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Or => write!(f, "or"),
            Self::Xor => write!(f, "xor"),
            Self::And => write!(f, "and"),
            Self::BitOr => write!(f, "|"),
            Self::BitXor => write!(f, "^"),
            Self::BitAnd => write!(f, "&"),
            Self::Eq => write!(f, "=="),
            Self::NotEq => write!(f, "!="),
            Self::Gte => write!(f, ">="),
            Self::Gt => write!(f, ">"),
            Self::Lte => write!(f, "<="),
            Self::Lt => write!(f, "<"),
            Self::RBitShiftSigned => write!(f, ">>"),
            Self::RBitShiftUnsigned => write!(f, ">>>"),
            Self::LBitShift => write!(f, "<<"),
            Self::Add => write!(f, "+"),
            Self::Sub => write!(f, "-"),
            Self::Mul => write!(f, "*"),
            Self::Div => write!(f, "/"),
            Self::Mod => write!(f, "%"),
        }
    }
}

impl fmt::Display for UnaryOpKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Plus => write!(f, "+"),
            Self::Minus => write!(f, "-"),
            Self::Not => write!(f, "not"),
            Self::BitNot => write!(f, "!"),
        }
    }
}

impl<'script> PartialEq for Invoke<'script> {
    fn eq(&self, other: &Self) -> bool {
        self.mid == other.mid && self.module == other.module && self.fun == other.fun
        //&& self.args == other.args FIXME why??!?
    }
}

impl<'script> fmt::Debug for Invoke<'script> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "fn {}::{}", self.module, self.fun)
    }
}

impl PartialEq for InvokeAggr {
    fn eq(&self, other: &Self) -> bool {
        self.mid == other.mid
            && self.module == other.module
            && self.fun == other.fun
            && self.aggr_id == other.aggr_id
        //&& self.args == other.args FIXME why??!?
    }
}

impl fmt::Debug for InvokeAggr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "fn(aggr) {}::{}", self.module, self.fun)
    }
}
