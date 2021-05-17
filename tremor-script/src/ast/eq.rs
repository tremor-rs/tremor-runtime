use crate::{CustomFn, TremorFnWrapper};

use super::{
    ArrayPattern, ArrayPredicatePattern, AssignPattern, BinExpr, Bytes, BytesPart, ClauseGroup,
    ClausePreCondition, Comprehension, ComprehensionCase, DefaultCase, EventPath, Expression,
    Field, ImutExpr, ImutExprInt, Invocable, Invoke, InvokeAggr, List, Literal, LocalPath, Match,
    Merge, MetadataPath, Patch, PatchOperation, Path, Pattern, PredicateClause, PredicatePattern,
    Record, RecordPattern, Recur, ReservedPath, Segment, StatePath, StrLitElement, StringLit,
    TestExpr, TuplePattern, UnaryExpr,
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

impl<T> AstEq for Vec<T>
where
    T: AstEq,
{
    fn ast_eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().zip(other.iter()).all(|(p1, p2)| p1.ast_eq(&p2))
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
        self.0.ast_eq(&other.0)
    }
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
        self.module.eq(&other.module)
            && self.fun == other.fun
            && self.invocable.ast_eq(&other.invocable)
            && self.args.ast_eq(&other.args)
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
        self.segments.ast_eq(&other.segments)
    }
}

impl<'script, Ex> AstEq for ClauseGroup<'script, Ex>
where
    Ex: Expression + AstEq + 'script,
{
    #[allow(clippy::suspicious_operation_groupings)]
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                ClauseGroup::Single {
                    precondition: precon1,
                    pattern: p1,
                },
                ClauseGroup::Single {
                    precondition: precon2,
                    pattern: p2,
                },
            ) => precon1.ast_eq(precon2) && p1.ast_eq(p2),
            (
                ClauseGroup::Simple {
                    precondition: precon1,
                    patterns: ps1,
                },
                ClauseGroup::Simple {
                    precondition: precon2,
                    patterns: ps2,
                },
            ) => precon1.ast_eq(precon2) && ps1.ast_eq(ps2),
            (
                ClauseGroup::SearchTree {
                    precondition: precon1,
                    tree: t1,
                    rest: rest1,
                },
                ClauseGroup::SearchTree {
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
                ClauseGroup::Combined {
                    precondition: pc1,
                    groups: g1,
                },
                ClauseGroup::Combined {
                    precondition: pc2,
                    groups: g2,
                },
            ) => pc1.ast_eq(pc2) && g1.ast_eq(g2),
            _ => false,
        }
    }
}

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
            && self.segments.ast_eq(&other.segments)
    }
}

impl<'script> AstEq for MetadataPath<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.segments.ast_eq(&other.segments)
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
    #[cfg(not(tarpaulin_include))]
    fn ast_eq(&self, other: &Self) -> bool {
        self == other
    }
}

#[cfg(test)]
mod tests {

    use std::collections::BTreeMap;

    use crate::ast::{ClauseGroup, Expr, ImutExpr};
    use crate::errors::Result;
    use crate::path::ModulePath;
    use crate::registry::registry;

    use super::*;

    /// tests the ast equality of the two last `ImutExprInt` in the given script
    fn test_ast_eq(script: &str, check_equality: bool) -> Result<()> {
        let module_path = ModulePath::load();
        let mut registry = registry();
        crate::std_lib::load(&mut registry);
        let script_script: crate::script::Script =
            crate::script::Script::parse(&module_path, "test", script.to_owned(), &registry)?;
        let script: &crate::ast::Script = script_script.script.suffix();
        let imut_exprs: Vec<&ImutExprInt> = script
            .exprs
            .iter()
            .filter_map(|e| {
                if let Expr::Imut(expr) = e {
                    Some(expr)
                } else {
                    None
                }
            })
            .collect();
        let len = imut_exprs.len();
        assert!(len >= 2);
        let res = imut_exprs[len - 2].ast_eq(imut_exprs[len - 1]);
        assert!(
            res == check_equality,
            "Expressions not AST_eq:\n\n\t{:?}\n\t{:?}",
            imut_exprs[len - 2],
            imut_exprs[len - 1]
        );
        Ok(())
    }

    macro_rules! eq_test {
        ($name:ident, $script:expr) => {
            #[test]
            fn $name() -> Result<()> {
                test_ast_eq($script, true)
            }
        };
    }

    macro_rules! not_eq_test {
        ($name:ident, $script:expr) => {
            #[test]
            fn $name() -> Result<()> {
                test_ast_eq($script, false)
            }
        };
    }

    eq_test!(test_event_path_eq, r#"event.foo ; event["foo"]"#);
    eq_test!(
        test_record_eq,
        r#"
    # setting the stage
    let local = 1 + 2;

    # eq check expressions
    {
        "foo": local
    };
    {
        "foo": local
    }
    "#
    );
    eq_test!(
        test_string_eq,
        r#"
        const answer = 42;
        "hello { answer + 1 } interpolated { answer }";
        "hello { answer + 1 } interpolated { answer }"
        "#
    );
    not_eq_test!(
        test_string_not_eq,
        r#"
        const answer = 42;
        "hello { answer + 1 } interpolated { answer + 2 }";
        "hello { answer + 1 } interpolated { answer }"
        "#
    );
    eq_test!(
        test_invocation_eq,
        r#"
        fn add(x, y) with
          x + y
        end;
        add(add(4, state[1]), 4.2);
        add(add(4, state[1]), 4.2);
        "#
    );
    eq_test!(
        test_list_eq,
        r#"
        let x = event.len;
        [1, -x, <<x:7/unsigned-integer>>, "foo"];
        [1, -x, <<x:7/unsigned-integer>>, "foo"]
        "#
    );
    not_eq_test!(
        test_list_not_eq,
        r#"
        let x = event.len;
        [1, -x, <<x:7/unsigned-integer>>, "foo"];
        [1, -x, <<x:7/signed-integer>>, "foo"]
        "#
    );
    eq_test!(
        test_patch_eq,
        r#"
        let x = {"snot": $meta};
        patch x of
          insert "i" => event.foo,
          upsert "snotty" => state.badger[1],
          update "snot" => null,
          erase "snot",
          copy "snot" => ["badger"],
          merge "beep" => {"fun": not false},
          merge => {"tuple": 4 * 12}
        end;
        patch x of
          insert "i" => event.foo,
          upsert "snotty" => state.badger[1],
          update "snot" => null,
          erase "snot",
          copy "snot" => ["badger"],
          merge "beep" => {"fun": not false},
          merge => {"tuple": 4 * 12}
        end
        "#
    );
    not_eq_test!(
        test_path_not_eq,
        r#"
        let x = {"snot": $meta};
        patch x of
          insert "i" => event.foo,
          upsert "snotty" => state.badger[1],
          update "snot" => null,
          erase "snot",
          copy "snot" => ["badger"],
          merge "beep" => {"fun": not false},
          merge => {"tuple": 4 * 12}
        end;
        patch x of
          insert "i" => event.foo,
          upsert "snotty" => state.badger[1],
          erase "snot",
          update "snot" => null, # order swapped
          copy "snot" => ["badger"],
          merge "beep" => {"fun": not false},
          merge => {"tuple": 4 * 12}
        end
        "#
    );

    eq_test!(
        test_match_eq,
        r#"
        let x = {"foo": "foo"};
        (match x of
            case "literal string" => "string"
            case event.foo => "event.foo"
            case %( _, 12, ... ) => "has 12 at index 1"
            case %[] => "empty_array"
            case %[ %{ present x } ] => "complex"
            case object = %{ absent y, snot == "badger", superhero ~= %{absent name}} when object.superhero.is_snotty => "snotty_badger"
            default => null
        end);
        (match x of
            case "literal string" => "string"
            case event.foo => "event.foo"
            case %( _, 12, ... ) => "has 12 at index 1"
            case %[] => "empty_array"
            case %[ %{ present x } ] => "complex"
            case object = %{ absent y, snot == "badger", superhero ~= %{absent name}} when object.superhero.is_snotty => "snotty_badger"
            default => null
        end)
        "#
    );
    eq_test!(
        test_match_eq_2,
        r#"
        let x = {"foo": "foo"};
        (match x of
            case "literal string" => "string"
            case "other literal string" => "string"
            case "other other literal string" => "string"
            case %{g == 1} => "obj"
            case %{g == 2} => "obj"
            case %{g == 3} => "obj"
            case %{g ~= re|.*|} => "obj"
            case %{g ~= re|.*|} => "obj"
            case %{g == 4} => "obj"
            case event.foo => "event.foo"
            case %( _, 12, ... ) => "has 12 at index 1"
            case %[] => "empty_array"
            case %[ %{ present x } ] => "complex"
            case object = %{ absent y, snot == "badger", superhero ~= %{absent name}} when object.superhero.is_snotty => "snotty_badger"
            default => null
        end);
        (match x of
            case "literal string" => "string"
            case "other literal string" => "string"
            case "other other literal string" => "string"
            case %{g == 1} => "obj"
            case %{g == 2} => "obj"
            case %{g == 3} => "obj"
            case %{g ~= re|.*|} => "obj"
            case %{g ~= re|.*|} => "obj"
            case %{g == 4} => "obj"
            case event.foo => "event.foo"
            case %( _, 12, ... ) => "has 12 at index 1"
            case %[] => "empty_array"
            case %[ %{ present x } ] => "complex"
            case object = %{ absent y, snot == "badger", superhero ~= %{absent name}} when object.superhero.is_snotty => "snotty_badger"
            default => null
        end)
        "#
    );
    not_eq_test!(
        test_match_not_eq,
        r#"
        let x = {"foo": "foo"};
        (match x of
            case "literal string" => "string"
            case event.foo => "event.foo"
            case %( _, 12, ... ) => "has 12 at index 1"
            case %[] => "empty_array"
            case %[ %{ present x } ] => "complex"
            case %{ absent y, snot == "badger", superhero ~= %{absent name}} when event.superhero.is_snotty => "snotty_badger"
            case %{ absent y } => "blarg"
            case %{ absent x } => "blarg"
            case %{ snot == "badger" } => "argh"
            case %{ snot == "meh" } => "argh"
            case %{ snot == "muh" } => "argh"
            case %{ snot == 1 } => "argh"
            case %{ snot == 2 } => "argh"
            case %{ snot == 3 } => "argh"
            default => null
        end);
        (match x of
            case "literal string" => "string"
            case event.foo => "event.foo"
            case %( _, 12, ... ) => "has 12 at index 1"
            case %[] => "empty_array"
            case %[ %{ present x } ] => "complex"
            case %{ absent y, snot == "badger", superhero ~= %{absent name}} when event.superhero.is_snotty => "snotty_badger"
            case %{ snot == "badger" } => "argh"
            case %{ snot == "meh" } => "argh"
            case %{ snot == "muh" } => "argh"
            case %{ snot == 1 } => "argh"
            case %{ snot == 2 } => "argh"
            case %{ snot == 3 } => "argh"            
            default => "not_null"
        end)
        "#
    );

    eq_test!(
        test_comprehension_eq,
        r#"
        (for event[1] of
            case (i, e) =>
                {" #{i}": e}
        end);
        (for event[1] of
            case (i, e) =>
                {" #{i}": e}
        end)
        "#
    );
    not_eq_test!(
        test_comprehension_not_eq,
        r#"
        (for event[1] of
            case (i, e) =>
                {" #{i}": e}
        end);
        (for event[1] of
            case (i, x) =>
                {" #{i}": x}
        end)
        "#
    );

    eq_test!(
        merge_eq_test,
        r#"
        (merge event["foo"] of event["bar"] end);
        (merge event.foo of event.bar end);
        "#
    );

    eq_test!(
        present_eq_test,
        r#"
        (present event.foo);
        (present event["foo"])
        "#
    );
    eq_test!(
        group_path_eq_test,
        r#"
        group[1];
        group[1]
        "#
    );
    not_eq_test!(
        group_path_not_eq_test,
        r#"
        group[1];
        group[0]
        "#
    );
    eq_test!(
        window_path_eq_test,
        r#"
        window;
        window
        "#
    );
    eq_test!(
        args_path_eq_test,
        r#"
        args[0];
        args[0]
        "#
    );
    eq_test!(
        meta_path_eq_test,
        r#"
        $meta[1];
        $meta[1]
        "#
    );
    eq_test!(
        complex_path_eq_test,
        r#"
        let local = {};
        local[event.path][event.start:event["end"]];
        local[event.path][event.start:event["end"]]
        "#
    );
    eq_test!(
        string_eq_test,
        r#"
        " #{ event.foo } bar #{ $ }";
        " #{event.foo} bar #{$}";
        "#
    );

    eq_test!(
        mul_eq_test,
        r#"
        event.m1 * event.m2;
        event.m1 * event.m2;
        "#
    );

    fn imut_expr() -> ImutExpr<'static> {
        ImutExpr(ImutExprInt::Path(Path::Event(EventPath {
            mid: 1,
            segments: vec![],
        })))
    }
    #[test]
    fn clause_group() {
        let pc = PredicateClause {
            mid: 0,
            pattern: Pattern::Default,
            guard: None,
            exprs: vec![],
            last_expr: imut_expr(),
        };
        assert!(ClauseGroup::Single {
            precondition: None,
            pattern: pc.clone(),
        }
        .ast_eq(&ClauseGroup::Single {
            precondition: None,
            pattern: pc.clone(),
        }));
        assert!(ClauseGroup::Simple {
            precondition: None,
            patterns: vec![pc.clone()],
        }
        .ast_eq(&ClauseGroup::Simple {
            precondition: None,
            patterns: vec![pc.clone()],
        }));
        assert!(ClauseGroup::SearchTree {
            precondition: None,
            tree: BTreeMap::new(),
            rest: vec![pc.clone()],
        }
        .ast_eq(&ClauseGroup::SearchTree {
            precondition: None,
            tree: BTreeMap::new(),
            rest: vec![pc.clone()],
        }));
        assert!(ClauseGroup::Combined {
            precondition: None,
            groups: vec![ClauseGroup::Single {
                precondition: None,
                pattern: pc.clone(),
            }],
        }
        .ast_eq(&ClauseGroup::Combined {
            precondition: None,
            groups: vec![ClauseGroup::Single {
                precondition: None,
                pattern: pc.clone(),
            }],
        }));
    }

    #[test]
    fn default_case() {
        assert!(DefaultCase::<ImutExpr>::None.ast_eq(&DefaultCase::None));
        assert!(DefaultCase::One(imut_expr()).ast_eq(&DefaultCase::One(imut_expr())));
        assert!(DefaultCase::Many {
            exprs: vec![],
            last_expr: Box::new(imut_expr())
        }
        .ast_eq(&DefaultCase::Many {
            exprs: vec![],
            last_expr: Box::new(imut_expr())
        }));
    }
    #[test]
    fn recur_eq_test() {
        let e: crate::ast::ImutExprs = vec![imut_expr()];
        let recur1 = Recur {
            mid: 1,
            argc: 2,
            open: true,
            exprs: e.clone(),
        };
        let mut recur2 = Recur {
            mid: 2,
            argc: 2,
            open: true,
            exprs: e,
        };
        assert!(recur1.ast_eq(&recur2));
        recur2.open = false;
        assert!(!recur1.ast_eq(&recur2));
    }

    #[test]
    fn test_path_local_special_case() -> Result<()> {
        let path = ImutExprInt::Path(Path::Local(LocalPath {
            idx: 1,
            mid: 1,
            is_const: false,
            segments: vec![Segment::Idx { idx: 1, mid: 15 }],
        }));
        let local = ImutExprInt::Local {
            idx: 1,
            mid: 42,
            is_const: false,
        };
        // has segments
        assert!(!path.ast_eq(&local));
        let path2 = ImutExprInt::Path(Path::Local(LocalPath {
            idx: 1,
            mid: 1212432,
            is_const: false,
            segments: vec![],
        }));
        assert!(path2.ast_eq(&local));
        // different index
        assert!(!ImutExprInt::Path(Path::Local(LocalPath {
            idx: 2,
            mid: 42,
            is_const: false,
            segments: vec![]
        }))
        .ast_eq(&local));
        // is_const different
        assert!(!ImutExprInt::Path(Path::Local(LocalPath {
            idx: 1,
            mid: 42,
            is_const: true,
            segments: vec![]
        }))
        .ast_eq(&local));

        assert!(Path::Local(LocalPath {
            idx: 1,
            mid: 34786752389,
            is_const: false,
            segments: vec![]
        })
        .ast_eq(&local));
        Ok(())
    }
}
