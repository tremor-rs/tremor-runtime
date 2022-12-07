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

use crate::{CustomFn, TremorFnWrapper};

use super::{
    ArrayPattern, ArrayPredicatePattern, AssignPattern, BinExpr, Bytes, BytesPart, ClauseGroup,
    ClausePreCondition, Comprehension, ComprehensionCase, DefaultCase, EventPath, ExprPath,
    Expression, Field, ImutExpr, Invocable, Invoke, InvokeAggr, List, Literal, LocalPath, Match,
    Merge, MetadataPath, NodeId, Patch, PatchOperation, Path, Pattern, PredicateClause,
    PredicatePattern, Record, RecordPattern, Recur, ReservedPath, Segment, StatePath,
    StrLitElement, StringLit, TestExpr, TuplePattern, UnaryExpr,
};

/// some special kind of equivalence between expressions
/// ignoring metadata ids
#[allow(clippy::module_name_repetitions)]
pub trait AstEq<T = Self> {
    /// returns true if both self and other are the same, ignoring the `mid`
    fn ast_eq(&self, other: &T) -> bool;
}

impl<T> AstEq for Vec<T>
where
    T: AstEq,
{
    fn ast_eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().zip(other.iter()).all(|(p1, p2)| p1.ast_eq(p2))
    }
}

impl<T> AstEq for Option<T>
where
    T: AstEq,
{
    fn ast_eq(&self, other: &Self) -> bool {
        match (self.as_ref(), other.as_ref()) {
            (Some(a1), Some(a2)) => a1.ast_eq(a2),
            (None, None) => true,
            _ => false,
        }
    }
}

impl<T> AstEq for &T
where
    T: AstEq,
{
    fn ast_eq(&self, other: &Self) -> bool {
        (*self).ast_eq(other)
    }
}

impl<'script> AstEq for ImutExpr<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        use ImutExpr::{
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
            (Local { idx: idx1, .. }, Local { idx: idx2, .. }) => idx1 == idx2,
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
            && self.data.ast_eq(&other.data)
    }
}

impl<'script> AstEq for Bytes<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.value.ast_eq(&other.value)
    }
}

impl<'script> AstEq for Field<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.name.ast_eq(&other.name) && self.value.ast_eq(&other.value)
    }
}

impl<'script> AstEq for Record<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.fields.ast_eq(&other.fields)
    }
}

impl<'script> AstEq for List<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.exprs.ast_eq(&other.exprs)
    }
}

impl<'script> AstEq for Literal<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<'script> AstEq for StringLit<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.elements.ast_eq(&other.elements)
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
        self.node_id.ast_eq(&other.node_id)
            && self.invocable.ast_eq(&other.invocable)
            && self.args.ast_eq(&other.args)
    }
}

impl AstEq for NodeId {
    fn ast_eq(&self, other: &Self) -> bool {
        self.id == other.id && self.module == other.module
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
        self.argc == other.argc && self.open == other.open && self.exprs.ast_eq(&other.exprs)
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

impl<'script> AstEq for ClausePreCondition<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.path.ast_eq(&other.path)
    }
}

// #[cfg_attr(coverage, no_coverage)] // Coverage creates too much false negatives here, this is covered in tests::clause_group
impl<'script, Ex> AstEq for ClauseGroup<'script, Ex>
where
    Ex: Expression + AstEq + 'script,
{
    #[allow(clippy::suspicious_operation_groupings)]
    fn ast_eq(&self, other: &Self) -> bool {
        use ClauseGroup::{Combined, SearchTree, Simple, Single};
        match (self, other) {
            (
                Single {
                    precondition,
                    pattern,
                },
                Single {
                    precondition: pc,
                    pattern: p,
                },
            ) => precondition.ast_eq(pc) && pattern.ast_eq(p),
            (
                Simple {
                    precondition,
                    patterns,
                },
                Simple {
                    precondition: pc,
                    patterns: p,
                },
            ) => precondition.ast_eq(pc) && patterns.ast_eq(p),
            (
                SearchTree {
                    precondition: precon1,
                    tree: t1,
                    rest: rest1,
                },
                SearchTree {
                    precondition: precon2,
                    tree: t2,
                    rest: rest2,
                },
            ) => {
                precon1.ast_eq(precon2)
                    && rest1.ast_eq(rest2)
                    && t1.len() == t2.len()
                    && t1
                        .iter()
                        .zip(t2.iter())
                        .all(|((k1, (v1, vs1)), (k2, (v2, vs2)))| {
                            k1 == k2 && v1.ast_eq(v2) && vs1.ast_eq(vs2)
                        })
            }
            (
                Combined {
                    precondition,
                    groups,
                },
                Combined {
                    precondition: p,
                    groups: g,
                },
            ) => precondition.ast_eq(p) && groups.ast_eq(g),
            _ => false,
        }
    }
}
// #[cfg_attr(coverage, no_coverage)] // Coverage creates too much false negatives here, this is covered in tests::default_case
impl<'script, Ex> AstEq for DefaultCase<Ex>
where
    Ex: Expression + AstEq + 'script,
{
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DefaultCase::None, DefaultCase::None) | (DefaultCase::Null, DefaultCase::Null) => true,
            (
                DefaultCase::Many {
                    exprs: es1,
                    last_expr: e1,
                },
                DefaultCase::Many {
                    exprs: es2,
                    last_expr: e2,
                },
            ) => e1.ast_eq(e2) && es1.ast_eq(es2),
            (DefaultCase::One(e1), DefaultCase::One(e2)) => e1.ast_eq(e2),
            _ => false,
        }
    }
}
impl<'script, Ex> AstEq for Match<'script, Ex>
where
    Ex: Expression + AstEq + 'script,
{
    fn ast_eq(&self, other: &Self) -> bool {
        self.target.ast_eq(&other.target)
            && self.patterns.ast_eq(&self.patterns)
            && self.default.ast_eq(&other.default)
    }
}

impl<'script, Ex> AstEq for PredicateClause<'script, Ex>
where
    Ex: Expression + AstEq + 'script,
{
    fn ast_eq(&self, other: &Self) -> bool {
        self.exprs.ast_eq(&other.exprs)
            && self.last_expr.ast_eq(&other.last_expr)
            && self.guard.ast_eq(&other.guard)
            && self.pattern.ast_eq(&other.pattern)
    }
}

impl<'script> AstEq for Patch<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.target.ast_eq(&other.target) && self.operations.ast_eq(&other.operations)
    }
}

// #[cfg_attr(coverage, no_coverage)] // Coverage creates too much false negatives here, this is covered in tests::patch_operation
impl<'script> AstEq for PatchOperation<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Insert { ident, expr, .. },
                Self::Insert {
                    ident: i2,
                    expr: e2,
                    ..
                },
            )
            | (
                Self::Upsert { ident, expr, .. },
                Self::Upsert {
                    ident: i2,
                    expr: e2,
                    ..
                },
            )
            | (
                Self::Update { ident, expr, .. },
                Self::Update {
                    ident: i2,
                    expr: e2,
                    ..
                },
            )
            | (
                Self::Default { ident, expr, .. },
                Self::Default {
                    ident: i2,
                    expr: e2,
                    ..
                },
            )
            | (
                Self::Merge { ident, expr, .. },
                Self::Merge {
                    ident: i2,
                    expr: e2,
                    ..
                },
            ) => ident.ast_eq(i2) && expr.ast_eq(e2),
            (Self::Erase { ident: i1, .. }, Self::Erase { ident: i2, .. }) => i1.ast_eq(i2),
            (
                Self::Copy {
                    from: f1, to: t1, ..
                },
                Self::Copy {
                    from: f2, to: t2, ..
                },
            )
            | (
                Self::Move {
                    from: f1, to: t1, ..
                },
                Self::Move {
                    from: f2, to: t2, ..
                },
            ) => f1.ast_eq(f2) && t1.ast_eq(t2),
            (Self::DefaultRecord { expr: e1, .. }, Self::DefaultRecord { expr: e2, .. })
            | (Self::MergeRecord { expr: e1, .. }, Self::MergeRecord { expr: e2, .. }) => {
                e1.ast_eq(e2)
            }
            _ => false,
        }
    }
}

impl<'script> AstEq for Merge<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.target.ast_eq(&other.target) && self.expr.ast_eq(&other.expr)
    }
}

impl<'script, Ex> AstEq for Comprehension<'script, Ex>
where
    Ex: Expression + AstEq + 'script,
{
    fn ast_eq(&self, other: &Self) -> bool {
        self.key_id == other.key_id
            && self.val_id == other.val_id
            && self.target.ast_eq(&other.target)
            && self.cases.ast_eq(&other.cases)
    }
}

impl<'script, Ex> AstEq for ComprehensionCase<'script, Ex>
where
    Ex: Expression + AstEq + 'script,
{
    fn ast_eq(&self, other: &Self) -> bool {
        self.key_name == other.key_name
            && self.value_name == other.value_name
            && self.guard.ast_eq(&other.guard)
            && self.exprs.ast_eq(&other.exprs)
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

// #[cfg_attr(coverage, no_coverage)] // Coverage creates too much false negatives here, this is covered in tests::predicate_pattern
impl<'script> AstEq for PredicatePattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        use PredicatePattern::{ArrayPatternEq, Bin, RecordPatternEq, TildeEq};
        match (self, other) {
            (
                TildeEq {
                    assign,
                    lhs,
                    key,
                    test,
                },
                TildeEq {
                    assign: a2,
                    lhs: l2,
                    key: k2,
                    test: t2,
                },
            ) => assign == a2 && lhs == l2 && key == k2 && test.ast_eq(t2.as_ref()),
            (
                Bin {
                    lhs,
                    key,
                    rhs,
                    kind,
                },
                Bin {
                    lhs: l2,
                    key: k2,
                    rhs: r2,
                    kind: kind2,
                },
            ) => lhs == l2 && key == k2 && kind == kind2 && rhs.ast_eq(r2),
            (
                RecordPatternEq { lhs, key, pattern },
                RecordPatternEq {
                    lhs: l2,
                    key: k2,
                    pattern: p2,
                },
            ) => lhs == l2 && key == k2 && pattern.ast_eq(p2),
            (
                ArrayPatternEq { lhs, key, pattern },
                ArrayPatternEq {
                    lhs: l2,
                    key: k2,
                    pattern: p2,
                },
            ) => lhs == l2 && key == k2 && pattern.ast_eq(p2),
            _ => self == other,
        }
    }
}

impl<'script> AstEq for RecordPattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.fields.ast_eq(&other.fields)
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
        self.exprs.ast_eq(&other.exprs)
    }
}

impl<'script> AstEq for AssignPattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.id == other.id && self.idx == other.idx && self.pattern.ast_eq(other.pattern.as_ref())
    }
}

impl<'script> AstEq for TuplePattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.open == other.open && self.exprs.ast_eq(&other.exprs)
    }
}

impl<'script> AstEq for Path<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Local(l1), Self::Local(l2)) => l1.ast_eq(l2),
            (Self::Event(e1), Self::Event(e2)) => e1.ast_eq(e2),
            (Self::State(s1), Self::State(s2)) => s1.ast_eq(s2),
            (Self::Meta(m1), Self::Meta(m2)) => m1.ast_eq(m2),
            (Self::Reserved(r1), Self::Reserved(r2)) => r1.ast_eq(r2),
            (Self::Expr(e1), Self::Expr(e2)) => e1.ast_eq(e2),
            (_, _) => false,
        }
    }
}

impl<'script> AstEq for ExprPath<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.expr.ast_eq(&other.expr) && self.segments.ast_eq(&other.segments)
    }
}

impl<'script> AstEq<ImutExpr<'script>> for Path<'script> {
    fn ast_eq(&self, other: &ImutExpr<'script>) -> bool {
        match (self, other) {
            // special case if a `Local` references the same local variable as this path
            (Self::Local(local_path), ImutExpr::Local { idx, .. })
                if local_path.segments.is_empty() =>
            {
                local_path.idx == *idx
            }
            (_, ImutExpr::Path(other)) => self.ast_eq(other),
            _ => false,
        }
    }
}
// #[cfg_attr(coverage, no_coverage)] // Coverage creates too much false negatives here, this is covered in tests::segment
impl<'script> AstEq for Segment<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Id { key: k1, .. }, Self::Id { key: k2, .. }) => k1 == k2,
            (Self::Idx { idx: i1, .. }, Self::Idx { idx: i2, .. }) => i1 == i2,
            (Self::Element { expr: e1, .. }, Self::Element { expr: e2, .. }) => e1.ast_eq(e2),
            (
                Self::RangeExpr {
                    start: s1, end: e1, ..
                },
                Self::RangeExpr {
                    start: s2, end: e2, ..
                },
            ) => s1.ast_eq(s2.as_ref()) && e1.ast_eq(e2.as_ref()),
            (
                Self::Range {
                    start: s1, end: e1, ..
                },
                Self::Range {
                    start: s2, end: e2, ..
                },
            ) => s1 == s2 && e1 == e2,
            (_, _) => false,
        }
    }
}

impl<'script> AstEq for LocalPath<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.idx == other.idx && self.segments.ast_eq(&other.segments)
    }
}

impl<'script> AstEq for MetadataPath<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.segments.ast_eq(&other.segments)
    }
}
// #[cfg_attr(coverage, no_coverage)] // Coverage creates too much false negatives here, this is covered in tests::reserved_path
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
        self.segments.ast_eq(&other.segments)
    }
}

impl<'script> AstEq for StatePath<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.segments.ast_eq(&other.segments)
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

#[cfg(test)]
mod tests;
