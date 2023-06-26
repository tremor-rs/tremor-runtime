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
#![allow(clippy::unwrap_used)]
use super::*;
use crate::{registry, CustomFn, NodeMeta};
use matches::assert_matches;
use simd_json::ObjectHasher;
use tremor_value::Object;

fn v(s: &'static str) -> ImutExpr<'static> {
    ImutExpr::literal(NodeMeta::dummy(), Value::from(s))
}

fn p() -> ImutExpr<'static> {
    ImutExpr::Path(Path::State(StatePath {
        mid: NodeMeta::dummy(),
        segments: vec![],
    }))
}

#[test]
fn record() {
    let f1 = super::Field {
        mid: NodeMeta::dummy(),
        name: "snot".into(),
        value: v("badger"),
    };
    let f2 = super::Field {
        mid: NodeMeta::dummy(),
        name: "badger".into(),
        value: v("snot"),
    };

    let r = super::Record {
        base: crate::Object::with_hasher(ObjectHasher::default()),
        mid: NodeMeta::dummy(),
        fields: vec![f1, f2],
    };

    assert_eq!(r.cloned_field_expr("snot"), Some(v("badger")));
    assert_eq!(r.cloned_field_expr("nots"), None);

    let lit = r.cloned_field_literal("badger");
    assert_eq!(lit.as_str(), Some("snot"));
    assert_eq!(r.cloned_field_expr("adgerb"), None);
}

#[test]
fn as_record() {
    let i = v("snot");
    assert!(i.as_record().is_none());
    let i = ImutExpr::Record(Record {
        mid: Box::new(i.meta().clone()),
        base: Object::with_hasher(ObjectHasher::default()),
        fields: Fields::new(),
    });
    assert!(i.as_record().is_some());
}

#[test]
fn as_list() {
    let i = v("snot");
    assert!(i.as_list().is_none());
    let i = ImutExpr::List(List {
        mid: Box::new(i.meta().clone()),
        exprs: vec![],
    });
    assert!(i.as_list().is_some());
}
#[test]
fn as_invoke() {
    let invocable = Invocable::Tremor(CustomFn {
        name: "f".into(),
        body: Vec::new(),
        args: Vec::new(),
        open: false,
        locals: 0,
        is_const: false,
        inline: false,
    });
    assert_eq!("f", invocable.name());
    let invocable2 = Invocable::Intrinsic(
        registry()
            .find("array", "reverse")
            .expect("Expected array::reverse to exist")
            .clone(),
    );
    assert_eq!("reverse", invocable2.name());
    let i = Invoke {
        mid: NodeMeta::dummy(),
        node_id: NodeId {
            module: Vec::new(),
            id: "fun".to_string(),
            mid: NodeMeta::dummy(),
        },
        invocable,
        args: Vec::new(),
    };
    assert!(Expr::Imut(v("snut")).as_invoke().is_none());
    let e = ImutExpr::Invoke(i.clone());
    assert!(Expr::Imut(e).as_invoke().is_some());
    let e = ImutExpr::Invoke1(i.clone());
    assert!(Expr::Imut(e).as_invoke().is_some());
    let e = ImutExpr::Invoke2(i.clone());
    assert!(Expr::Imut(e).as_invoke().is_some());
    let e = ImutExpr::Invoke3(i.clone());
    assert!(Expr::Imut(e).as_invoke().is_some());

    let i2 = Invoke {
        mid: NodeMeta::dummy(),
        node_id: NodeId {
            module: Vec::new(),
            id: "fun".to_string(),
            mid: NodeMeta::dummy(),
        },
        invocable: invocable2,
        args: Vec::new(),
    };
    assert!(Expr::Imut(v("snut")).as_invoke().is_none());
    let e = ImutExpr::Invoke(i2.clone());
    assert!(Expr::Imut(e).as_invoke().is_some());
    let e = ImutExpr::Invoke1(i2.clone());
    assert!(Expr::Imut(e).as_invoke().is_some());
    let e = ImutExpr::Invoke2(i2.clone());
    assert!(Expr::Imut(e).as_invoke().is_some());
    let e = ImutExpr::Invoke3(i2.clone());
    assert!(Expr::Imut(e).as_invoke().is_some());
}

#[test]
fn as_imut() {
    let i = ImutExpr::List(List {
        mid: NodeMeta::dummy(),
        exprs: vec![],
    });
    assert!(Expr::Imut(i).as_imut().is_ok());
    assert!(Expr::Drop {
        mid: NodeMeta::dummy()
    }
    .as_imut()
    .is_err());
}

#[test]
fn try_into_value() -> crate::errors::Result<()> {
    let aggr_reg = crate::registry::aggr();
    let reg = &*crate::FN_REGISTRY.read()?;
    let helper = Helper::new(reg, &aggr_reg);

    let val = v("value");
    assert!(val.try_into_value(&helper).is_ok());

    let list = ImutExpr::List(List {
        mid: NodeMeta::dummy(),
        exprs: vec![],
    });
    assert!(list.try_into_value(&helper).is_ok());

    let path = p();
    assert!(path.try_into_value(&helper).is_err());

    Ok(())
}
#[test]
fn str_lit_as_str() {
    assert_eq!(StrLitElement::Lit("string".into()).as_str(), Some("string"));
    assert_eq!(StrLitElement::Expr(v("string")).as_str(), Some("string"));
    assert_eq!(StrLitElement::Expr(p()).as_str(), None);
}

#[test]
fn replace_last_shadow_use() {
    let path = Path::Event(EventPath {
        mid: NodeMeta::dummy(),
        segments: vec![],
    });
    let expr = Expr::Assign {
        mid: NodeMeta::dummy(),
        path,
        expr: Box::new(Expr::Imut(ImutExpr::Local {
            idx: 42,
            mid: NodeMeta::dummy(),
        })),
    };
    let pc = PredicateClause {
        mid: NodeMeta::dummy(),
        pattern: Pattern::DoNotCare,
        guard: None,
        exprs: vec![],
        last_expr: expr.clone(),
    };

    // Simple
    let mut g = ClauseGroup::Simple {
        precondition: None,
        patterns: vec![pc.clone()],
    };
    g.replace_last_shadow_use(42);
    if let ClauseGroup::Simple { patterns, .. } = g {
        assert_matches!(patterns[0].last_expr, Expr::AssignMoveLocal { idx: 42, .. });
    } else {
        unreachable!()
    };

    // Search Tree
    let mut tree = BTreeMap::new();
    tree.insert(Value::from(42u32), (vec![], expr.clone()));
    let mut g = ClauseGroup::SearchTree {
        precondition: None,
        tree,
        rest: vec![pc.clone()],
    };
    g.replace_last_shadow_use(42);
    if let ClauseGroup::SearchTree { rest, tree, .. } = g {
        let last = &tree.values().next().unwrap().1;
        assert_matches!(last, Expr::AssignMoveLocal { idx: 42, .. });
        assert_matches!(rest[0].last_expr, Expr::AssignMoveLocal { idx: 42, .. });
    } else {
        unreachable!()
    };

    // Combined
    let g1 = ClauseGroup::Simple {
        precondition: None,
        patterns: vec![pc.clone()],
    };
    let g2 = ClauseGroup::Simple {
        precondition: None,
        patterns: vec![pc.clone()],
    };
    let mut g = ClauseGroup::Combined {
        precondition: None,
        groups: vec![g1, g2],
    };
    g.replace_last_shadow_use(42);
    if let ClauseGroup::Combined { groups, .. } = g {
        if let ClauseGroup::Simple { patterns, .. } = &groups[0] {
            assert_matches!(patterns[0].last_expr, Expr::AssignMoveLocal { idx: 42, .. });
        } else {
            unreachable!()
        };
        if let ClauseGroup::Simple { patterns, .. } = &groups[1] {
            assert_matches!(patterns[0].last_expr, Expr::AssignMoveLocal { idx: 42, .. });
        } else {
            unreachable!()
        };
    } else {
        unreachable!()
    }

    // Single
    let mut g = ClauseGroup::Single {
        pattern: pc.clone(),
        precondition: None,
    };
    g.replace_last_shadow_use(42);
    if let ClauseGroup::Single { pattern, .. } = g {
        assert_matches!(pattern.last_expr, Expr::AssignMoveLocal { idx: 42, .. });
    } else {
        unreachable!()
    };
}

#[test]
#[allow(clippy::too_many_lines)]
fn pp_is_exclusive() {
    let eq = PredicatePattern::Bin {
        lhs: "k1".into(),
        key: KnownKey::from("k1"),
        rhs: ImutExpr::literal(NodeMeta::dummy(), Value::from("cake")),
        kind: BinOpKind::Eq,
    };
    let eq2 = PredicatePattern::Bin {
        lhs: "k2".into(),
        key: KnownKey::from("k2"),
        rhs: ImutExpr::literal(NodeMeta::dummy(), Value::from("cake")),
        kind: BinOpKind::Eq,
    };
    assert!(!eq.is_exclusive_to(&eq), "is not exlusive to itself");
    assert!(
        !eq.is_exclusive_to(&eq2),
        "is not exclusive to a different key"
    );
    assert!(
        eq.is_exclusive_to(&PredicatePattern::Bin {
            lhs: "k1".into(),
            key: KnownKey::from("k1"),
            rhs: ImutExpr::literal(NodeMeta::dummy(), Value::from("cookie")),
            kind: BinOpKind::Eq,
        }),
        "is exclusive to the same key and a different value"
    );

    let abs = PredicatePattern::FieldAbsent {
        lhs: "k1".into(),
        key: KnownKey::from("k1"),
    };
    let abs2 = PredicatePattern::FieldAbsent {
        lhs: "k2".into(),
        key: KnownKey::from("k2"),
    };
    assert!(
        !abs.is_exclusive_to(&abs2),
        "absent is not exclusive on different fields"
    );
    assert!(eq.is_exclusive_to(&abs), "eq is exclusive to absent");
    assert!(abs.is_exclusive_to(&eq), "absent is exclusive to eq");
    assert!(
        !eq.is_exclusive_to(&abs2),
        "eq is not exclusive to absend on a different key"
    );
    assert!(
        !abs2.is_exclusive_to(&eq),
        "absent is not exclusive to eq on a different key"
    );

    let pres = PredicatePattern::FieldPresent {
        lhs: "k1".into(),
        key: KnownKey::from("k1"),
    };
    let pres2 = PredicatePattern::FieldPresent {
        lhs: "k2".into(),
        key: KnownKey::from("k2"),
    };
    assert!(
        !pres.is_exclusive_to(&pres2),
        "present is not exclusive on different fields"
    );
    assert!(
        pres.is_exclusive_to(&abs),
        "present and absent are exclusive"
    );
    assert!(
        abs.is_exclusive_to(&pres),
        "present and absent are exclusive"
    );
    assert!(
        !pres.is_exclusive_to(&eq),
        "present and eq are not exclusive"
    );
    assert!(
        !pres.is_exclusive_to(&eq2),
        "present and eq are not exclusive on different keys"
    );
    let teq = PredicatePattern::TildeEq {
        lhs: "k1".into(),
        assign: "k".into(),
        key: "k1".into(),
        test: Box::new(TestExpr {
            mid: NodeMeta::dummy(),
            id: "suffix".into(),
            test: "ake".into(),
            extractor: Extractor::Suffix("ake".into()),
        }),
    };
    let test_eq2 = PredicatePattern::TildeEq {
        lhs: "k2".into(),
        assign: "k".into(),
        key: "k2".into(),
        test: Box::new(TestExpr {
            mid: NodeMeta::dummy(),
            id: "suffix".into(),
            test: "ookie".into(),
            extractor: Extractor::Suffix("ake".into()),
        }),
    };
    let teq3 = PredicatePattern::TildeEq {
        lhs: "k1".into(),
        assign: "k".into(),
        key: "k1".into(),
        test: Box::new(TestExpr {
            mid: NodeMeta::dummy(),
            id: "suffix".into(),
            test: "ookie".into(),
            extractor: Extractor::Suffix("ookie".into()),
        }),
    };
    assert!(
        !teq.is_exclusive_to(&test_eq2),
        "~= is not exclusive on different keys"
    );
    assert!(
        !test_eq2.is_exclusive_to(&teq),
        "~= is not exclusive on different keys"
    );
    assert!(
        teq.is_exclusive_to(&teq3),
        "is not exclusive on if the tests are exclusive"
    );
    assert!(
        teq3.is_exclusive_to(&teq),
        "is not exclusive on if the tests are exclusive"
    );
    assert!(
        !teq.is_exclusive_to(&eq),
        "~= is not exclusive to eq if the patterns are not exclusive"
    );
    assert!(
        !eq.is_exclusive_to(&teq),
        "~= is not exclusive to eq if the patterns are not exclusive"
    );
}
