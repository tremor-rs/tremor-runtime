use crate::{CustomFn, TremorFnWrapper};

use super::{
    ArrayPattern, ArrayPredicatePattern, AssignPattern, BinExpr, Bytes, BytesPart, EventPath,
    ImutComprehension, ImutComprehensionCase, ImutExprInt, ImutMatch, ImutPredicateClause,
    Invocable, Invoke, InvokeAggr, List, Literal, LocalPath, Merge, MetadataPath, Patch,
    PatchOperation, Path, Pattern, PredicatePattern, Record, RecordPattern, Recur, ReservedPath,
    Segment, StatePath, StrLitElement, StringLit, TestExpr, TuplePattern, UnaryExpr,
};

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

/// some special kind of equivalence between expressions
/// ignoring metadata ids
#[allow(clippy::module_name_repetitions)]
pub trait AstEq<T = Self> {
    /// returns true if both self and other are the same, ignoring the `mid`
    fn ast_eq(&self, other: &T) -> bool;
}

impl<'script> AstEq for ImutExprInt<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        use ImutExprInt::{
            Binary, Bytes, Comprehension, Invoke, Invoke1, Invoke2, Invoke3, InvokeAggr, List,
            Literal, Local, Match, Merge, Patch, Path, Present, Record, Recur, String, Unary,
        };
        match (self, other) {
            (Record(r1), Record(r2)) => r1.ast_eq(r2),
            (List(l1), List(l2)) => l1.ast_eq(l2),
            (Binary(b1), Binary(b2)) => b1.ast_eq(b2),
            (Unary(u1), Unary(u2)) => u1.ast_eq(u2),
            (Patch(p1), Patch(p2)) => p1.ast_eq(p2),
            (Match(m1), Match(m2)) => m1.ast_eq(m2),
            (Comprehension(c1), Comprehension(c2)) => c1.ast_eq(c2),
            (Merge(m1), Merge(m2)) => m1.ast_eq(m2),
            (Path(p1), Path(p2)) => p1.ast_eq(p2),
            // special case for `Path`(i.e. `LocalPath`) and `Local`
            // which can be considered the same if they reference the same local (without segments)
            (Path(path), local @ Local { .. }) | (local @ Local { .. }, Path(path)) => {
                path.ast_eq(local)
            }
            (String(s1), String(s2)) => s1.ast_eq(s2),
            (
                Local {
                    idx: idx1,
                    is_const: const1,
                    ..
                },
                Local {
                    idx: idx2,
                    is_const: const2,
                    ..
                },
            ) => idx1 == idx2 && const1 == const2,
            (Literal(l1), Literal(l2)) => l1.ast_eq(l2),
            (Present { path: path1, .. }, Present { path: path2, .. }) => path1.ast_eq(path2),
            (Invoke1(i1), Invoke1(i2))
            | (Invoke2(i1), Invoke2(i2))
            | (Invoke3(i1), Invoke3(i2))
            | (Invoke(i1), Invoke(i2)) => i1.ast_eq(i2),
            (InvokeAggr(i1), InvokeAggr(i2)) => i1.ast_eq(i2),
            (Recur(r1), Recur(r2)) => r1.ast_eq(r2),
            (Bytes(b1), Bytes(b2)) => b1.ast_eq(b2),
            _ => false,
        }
    }
}

impl<'script> AstEq for BytesPart<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.data_type == other.data_type
            && self.endianess == other.endianess
            && self.bits == other.bits
            && self.data.0.ast_eq(&other.data.0)
    }
}

impl<'script> AstEq for Bytes<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.value.len() == other.value.len()
            && self
                .value
                .iter()
                .zip(other.value.iter())
                .all(|(b1, b2)| b1.ast_eq(b2))
    }
}
impl<'script> AstEq for Record<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.fields.len() == other.fields.len()
            && self
                .fields
                .iter()
                .zip(other.fields.iter())
                .all(|(f1, f2)| f1.name.ast_eq(&f2.name) && f1.value.ast_eq(&f2.value))
    }
}

impl<'script> AstEq for List<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.exprs.len() == other.exprs.len()
            && self
                .exprs
                .iter()
                .zip(other.exprs.iter())
                .all(|(e1, e2)| e1.0.ast_eq(&e2.0))
    }
}

impl<'script> AstEq for Literal<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<'script> AstEq for StringLit<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.elements.len() == other.elements.len()
            && self
                .elements
                .iter()
                .zip(other.elements.iter())
                .all(|(e1, e2)| e1.ast_eq(e2))
    }
}

impl<'script> AstEq for StrLitElement<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Expr(e1), Self::Expr(e2)) => e1.ast_eq(e2),
            _ => self == other,
        }
    }
}

impl<'script> AstEq for Invoke<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.module.eq(&other.module)
            && self.fun == other.fun
            && self.invocable.ast_eq(&other.invocable)
            && self.args.len() == other.args.len()
            && self
                .args
                .iter()
                .zip(other.args.iter())
                .all(|(a1, a2)| a1.0.ast_eq(&a2.0))
    }
}

impl<'script> AstEq for Invocable<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Intrinsic(wrapper1), Self::Intrinsic(wrapper2)) => wrapper1.ast_eq(wrapper2),
            (Self::Tremor(custom1), Self::Tremor(custom2)) => custom1.ast_eq(custom2),
            _ => false,
        }
    }
}

impl<'script> AstEq for Recur<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.argc == other.argc
            && self.open == other.open
            && self.exprs.len() == other.exprs.len()
            && self
                .exprs
                .iter()
                .zip(other.exprs.iter())
                .all(|(e1, e2)| e1.0.ast_eq(&e2.0))
    }
}

impl AstEq for InvokeAggr {
    fn ast_eq(&self, other: &Self) -> bool {
        self.aggr_id == other.aggr_id && self.module == other.module && self.fun == other.fun
    }
}

impl AstEq for TestExpr {
    fn ast_eq(&self, other: &Self) -> bool {
        self.id == other.id && self.test == other.test && self.extractor == other.extractor
    }
}

impl<'script> AstEq for ImutMatch<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.target.ast_eq(&other.target)
            && self.patterns.len() == other.patterns.len()
            && self
                .patterns
                .iter()
                .zip(other.patterns.iter())
                .all(|(p1, p2)| p1.ast_eq(&p2))
    }
}

impl<'script> AstEq for ImutPredicateClause<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.expr.0.ast_eq(&other.expr.0)
            && match (self.guard.as_ref(), other.guard.as_ref()) {
                (Some(expr1), Some(expr2)) => expr1.ast_eq(expr2),
                (None, None) => true,
                _ => false,
            }
            && self.pattern.ast_eq(&other.pattern)
    }
}

impl<'script> AstEq for Patch<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.target.ast_eq(&other.target)
            && self.operations.len() == other.operations.len()
            && self
                .operations
                .iter()
                .zip(other.operations.iter())
                .all(|(o1, o2)| o1.ast_eq(o2))
    }
}

impl<'script> AstEq for PatchOperation<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Insert {
                    ident: i1,
                    expr: e1,
                },
                Self::Insert {
                    ident: i2,
                    expr: e2,
                },
            )
            | (
                Self::Upsert {
                    ident: i1,
                    expr: e1,
                },
                Self::Upsert {
                    ident: i2,
                    expr: e2,
                },
            )
            | (
                Self::Update {
                    ident: i1,
                    expr: e1,
                },
                Self::Update {
                    ident: i2,
                    expr: e2,
                },
            )
            | (
                Self::Merge {
                    ident: i1,
                    expr: e1,
                },
                Self::Merge {
                    ident: i2,
                    expr: e2,
                },
            ) => i1.ast_eq(i2) && e1.ast_eq(e2),
            (Self::Erase { ident: i1 }, Self::Erase { ident: i2 }) => i1.ast_eq(i2),
            (Self::Copy { from: f1, to: t1 }, Self::Copy { from: f2, to: t2 })
            | (Self::Move { from: f1, to: t1 }, Self::Move { from: f2, to: t2 }) => {
                f1.ast_eq(f2) && t1.ast_eq(t2)
            }
            (Self::TupleMerge { expr: e1 }, Self::TupleMerge { expr: e2 }) => e1.ast_eq(e2),
            _ => false,
        }
    }
}

impl<'script> AstEq for Merge<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.target.ast_eq(&other.target) && self.expr.ast_eq(&other.expr)
    }
}

impl<'script> AstEq for ImutComprehension<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.key_id == other.key_id
            && self.val_id == other.val_id
            && self.target.ast_eq(&other.target)
            && self.cases.len() == other.cases.len()
            && self
                .cases
                .iter()
                .zip(other.cases.iter())
                .all(|(c1, c2)| c1.ast_eq(c2))
    }
}

impl<'script> AstEq for ImutComprehensionCase<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.key_name == other.key_name
            && self.value_name == other.value_name
            && match (self.guard.as_ref(), other.guard.as_ref()) {
                (Some(g1), Some(g2)) => g1.ast_eq(g2),
                (None, None) => true,
                _ => false,
            }
            && self.expr.0.ast_eq(&other.expr.0)
    }
}

impl<'script> AstEq for Pattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Record(r1), Self::Record(r2)) => r1.ast_eq(r2),
            (Self::Array(a1), Self::Array(a2)) => a1.ast_eq(a2),
            (Self::Expr(e1), Self::Expr(e2)) => e1.ast_eq(e2),
            (Self::Assign(a1), Self::Assign(a2)) => a1.ast_eq(a2),
            (Self::Tuple(t1), Self::Tuple(t2)) => t1.ast_eq(t2),
            _ => self == other,
        }
    }
}

impl<'script> AstEq for PredicatePattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::TildeEq {
                    assign: a1,
                    lhs: l1,
                    key: k1,
                    test: t1,
                },
                Self::TildeEq {
                    assign: a2,
                    lhs: l2,
                    key: k2,
                    test: t2,
                },
            ) => a1 == a2 && l1 == l2 && k1 == k2 && t1.ast_eq(t2.as_ref()),
            (
                Self::Bin {
                    lhs: l1,
                    key: k1,
                    rhs: r1,
                    kind: kind1,
                },
                Self::Bin {
                    lhs: l2,
                    key: k2,
                    rhs: r2,
                    kind: kind2,
                },
            ) => l1 == l2 && k1 == k2 && kind1 == kind2 && r1.ast_eq(r2),
            (
                Self::RecordPatternEq {
                    lhs: l1,
                    key: k1,
                    pattern: p1,
                },
                Self::RecordPatternEq {
                    lhs: l2,
                    key: k2,
                    pattern: p2,
                },
            ) => l1 == l2 && k1 == k2 && p1.ast_eq(p2),
            (
                Self::ArrayPatternEq {
                    lhs: l1,
                    key: k1,
                    pattern: p1,
                },
                Self::ArrayPatternEq {
                    lhs: l2,
                    key: k2,
                    pattern: p2,
                },
            ) => l1 == l2 && k1 == k2 && p1.ast_eq(p2),
            _ => self == other,
        }
    }
}

impl<'script> AstEq for RecordPattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.fields.len() == other.fields.len()
            && self
                .fields
                .iter()
                .zip(other.fields.iter())
                .all(|(f1, f2)| f1.ast_eq(&f2))
    }
}

impl<'script> AstEq for ArrayPredicatePattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Expr(e1), Self::Expr(e2)) => e1.ast_eq(e2),
            (Self::Tilde(t1), Self::Tilde(t2)) => t1.ast_eq(t2.as_ref()),
            (Self::Record(r1), Self::Record(r2)) => r1.ast_eq(r2),
            _ => self == other,
        }
    }
}

impl<'script> AstEq for ArrayPattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.exprs.len() == other.exprs.len()
            && self
                .exprs
                .iter()
                .zip(other.exprs.iter())
                .all(|(e1, e2)| e1.ast_eq(e2))
    }
}

impl<'script> AstEq for AssignPattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.id == other.id && self.idx == other.idx && self.pattern.ast_eq(other.pattern.as_ref())
    }
}

impl<'script> AstEq for TuplePattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.open == other.open
            && self.exprs.len() == other.exprs.len()
            && self
                .exprs
                .iter()
                .zip(other.exprs.iter())
                .all(|(e1, e2)| e1.ast_eq(e2))
    }
}

impl<'script> AstEq for Path<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Const(c1), Self::Const(c2)) => c1.ast_eq(c2),
            (Self::Local(l1), Self::Local(l2)) => l1.ast_eq(l2),
            (Self::Event(e1), Self::Event(e2)) => e1.ast_eq(e2),
            (Self::State(s1), Self::State(s2)) => s1.ast_eq(s2),
            (Self::Meta(m1), Self::Meta(m2)) => m1.ast_eq(m2),
            (Self::Reserved(r1), Self::Reserved(r2)) => r1.ast_eq(r2),
            _ => false,
        }
    }
}

impl<'script> AstEq<ImutExprInt<'script>> for Path<'script> {
    fn ast_eq(&self, other: &ImutExprInt<'script>) -> bool {
        match (self, other) {
            // special case if a `Local` references the same local variable as this path
            (Self::Local(local_path), ImutExprInt::Local { idx, is_const, .. })
                if local_path.segments.is_empty() =>
            {
                local_path.idx == *idx && local_path.is_const == *is_const
            }
            (_, ImutExprInt::Path(other)) => self.ast_eq(other),
            _ => false,
        }
    }
}

impl<'script> AstEq for Segment<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Id { key: k1, .. }, Self::Id { key: k2, .. }) => k1 == k2,
            (Self::Idx { idx: i1, .. }, Self::Idx { idx: i2, .. }) => i1 == i2,
            (Self::Element { expr: e1, .. }, Self::Element { expr: e2, .. }) => e1.ast_eq(e2),
            (
                Self::Range {
                    range_start: s1,
                    range_end: e1,
                    ..
                },
                Self::Range {
                    range_start: s2,
                    range_end: e2,
                    ..
                },
            ) => s1.ast_eq(s2.as_ref()) && e1.ast_eq(e2.as_ref()),
            _ => false,
        }
    }
}

impl<'script> AstEq for LocalPath<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.idx == other.idx
            && self.is_const == other.is_const
            && self.segments.len() == other.segments.len()
            && self
                .segments
                .iter()
                .zip(other.segments.iter())
                .all(|(s1, s2)| s1.ast_eq(s2))
    }
}

impl<'script> AstEq for MetadataPath<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.segments.len() == other.segments.len()
            && self
                .segments
                .iter()
                .zip(other.segments.iter())
                .all(|(s1, s2)| s1.ast_eq(s2))
    }
}

impl<'script> AstEq for ReservedPath<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Args { segments: s1, .. }, Self::Args { segments: s2, .. })
            | (Self::Window { segments: s1, .. }, Self::Window { segments: s2, .. })
            | (Self::Group { segments: s1, .. }, Self::Group { segments: s2, .. }) => {
                s1.len() == s2.len() && s1.iter().zip(s2.iter()).all(|(s1, s2)| s1.ast_eq(s2))
            }
            _ => false,
        }
    }
}

impl<'script> AstEq for EventPath<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.segments.len() == other.segments.len()
            && self
                .segments
                .iter()
                .zip(other.segments.iter())
                .all(|(s1, s2)| s1.ast_eq(s2))
    }
}

impl<'script> AstEq for StatePath<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.segments.len() == other.segments.len()
            && self
                .segments
                .iter()
                .zip(other.segments.iter())
                .all(|(s1, s2)| s1.ast_eq(s2))
    }
}

impl<'script> AstEq for BinExpr<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.lhs.ast_eq(&other.lhs) && self.rhs.ast_eq(&other.rhs)
    }
}

impl<'script> AstEq for UnaryExpr<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.expr.ast_eq(&other.expr)
    }
}

impl<'script> AstEq for CustomFn<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        // TODO: add body handling
        self.name == other.name
            && self.args == other.args
            && self.open == other.open
            && self.locals == other.locals
            && self.is_const == other.is_const
            && self.inline == other.inline
    }
}

impl AstEq for TremorFnWrapper {
    fn ast_eq(&self, other: &Self) -> bool {
        self == other
    }
}
