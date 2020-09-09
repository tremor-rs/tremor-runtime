// Copyright 2020, The Tremor Team
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

use super::{
    BinOpKind, EventPath, Invoke, InvokeAggr, InvokeAggrFn, LocalPath, MetadataPath, Segment,
    StatePath, UnaryOpKind,
};
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
        use Segment::{Element, Id, Idx, Range};
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

impl BinOpKind {
    fn operator_name(self) -> &'static str {
        match self {
            Self::Or => "or",
            Self::Xor => "xor",
            Self::And => "and",
            Self::BitOr => "|",
            Self::BitXor => "^",
            Self::BitAnd => "&",
            Self::Eq => "==",
            Self::NotEq => "!=",
            Self::Gte => ">=",
            Self::Gt => ">",
            Self::Lte => "<=",
            Self::Lt => "<",
            Self::RBitShiftSigned => ">>",
            Self::RBitShiftUnsigned => ">>>",
            Self::LBitShift => "<<",
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
        }
    }
}

impl fmt::Display for BinOpKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.operator_name())
    }
}

impl UnaryOpKind {
    fn operator_name(self) -> &'static str {
        match self {
            Self::Plus => "+",
            Self::Minus => "-",
            Self::Not => "not",
            Self::BitNot => "!",
        }
    }
}

impl fmt::Display for UnaryOpKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.operator_name())
    }
}

impl<'script> PartialEq for Invoke<'script> {
    fn eq(&self, other: &Self) -> bool {
        self.mid == other.mid && self.module == other.module && self.fun == other.fun
    }
}

impl<'script> fmt::Debug for Invoke<'script> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "fn {}::{}", self.module.join("::"), self.fun)
    }
}

impl PartialEq for InvokeAggr {
    fn eq(&self, other: &Self) -> bool {
        self.mid == other.mid
            && self.module == other.module
            && self.fun == other.fun
            && self.aggr_id == other.aggr_id
    }
}

impl fmt::Debug for InvokeAggr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "fn(aggr) {}::{}", self.module, self.fun)
    }
}
