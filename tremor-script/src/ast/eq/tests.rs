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

use super::*;
use crate::{
    ast::{eq::AstEq, BinOpKind, ClauseGroup, Expr},
    errors::Result,
    registry::registry,
    NodeMeta,
};
use std::collections::BTreeMap;

/// tests the ast equality of the two last `ImutExprInt` in the given script
fn test_ast_eq(script: &str, check_equality: bool) -> Result<()> {
    let mut registry = registry();
    crate::std_lib::load(&mut registry);
    let script_script: crate::script::Script = crate::script::Script::parse(script, &registry)?;
    let script: &crate::ast::Script = &script_script.script;
    let imut_exprs: Vec<&ImutExpr> = script
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
    let first = imut_exprs[len - 2];
    let second = imut_exprs[len - 1];
    let res = first.ast_eq(second);
    assert!(
        res == check_equality,
        "Expressions not AST_eq:\n\n\t{first:?}\n\t{second:?}",
    );
    Ok(())
}

#[test]
fn test_event_path_eq() -> Result<()> {
    test_ast_eq(r#"event.foo ; event["foo"]"#, true)
}

#[test]
fn test_record_eq() -> Result<()> {
    test_ast_eq(
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
    "#,
        true,
    )
}
#[test]
fn test_string_eq() -> Result<()> {
    test_ast_eq(
        r#"
        const answer = 42;
        "hello { answer + 1 } interpolated { answer }";
        "hello { answer + 1 } interpolated { answer }"
        "#,
        true,
    )
}
#[test]
fn test_string_not_eq() -> Result<()> {
    test_ast_eq(
        r#"
        const answer = 42;
        "hello { answer + 1 } interpolated { answer + 2 }";
        "hello { answer + 1 } interpolated { answer }"
        "#,
        false,
    )
}
#[test]
fn test_invocation_eq() -> Result<()> {
    test_ast_eq(
        "
        fn add(x, y) with
          x + y
        end;
        add(add(4, state[1]), 4.2);
        add(add(4, state[1]), 4.2);
        ",
        true,
    )
}
#[test]
fn test_list_eq() -> Result<()> {
    test_ast_eq(
        r#"
        let x = event.len;
        [1, -x, <<x:7/unsigned_integer>>, "foo"];
        [1, -x, <<x:7/unsigned_integer>>, "foo"]
        "#,
        true,
    )
}
#[test]
fn test_list_not_eq() -> Result<()> {
    test_ast_eq(
        r#"
        let x = event.len;
        [1, -x, <<x:7/unsigned_integer>>, "foo"];
        [1, -x, <<x:7/signed_integer>>, "foo"]
        "#,
        false,
    )
}
#[test]
fn test_patch_eq() -> Result<()> {
    test_ast_eq(
        r#"
        let x = {"snot": $meta};
        patch x of
          default => {"bla": "blubb"};
          default "gna" => "gnubb";
          insert "i" => event.foo;
          upsert "snotty" => state.badger[1];
          update "snot" => null;
          erase "snot";
          copy "snot" => "badger";
          merge "beep" => {"fun": not false};
          merge => {"tuple": 4 * 12}
        end;
        patch x of
          default => {"bla": "blubb"};
          default "gna" => "gnubb";
          insert "i" => event.foo;
          upsert "snotty" => state.badger[1];
          update "snot" => null;
          erase "snot";
          copy "snot" => "badger";
          merge "beep" => {"fun": not false};
          merge => {"tuple": 4 * 12}
        end
        "#,
        true,
    )
}
#[test]
fn test_path_not_eq() -> Result<()> {
    test_ast_eq(
        r#"
        let x = {"snot": $meta};
        patch x of
          default => {"bla": "blubb"};
          default "gna" => "gnubb";
          insert "i" => event.foo;
          upsert "snotty" => state.badger[1];
          update "snot" => null;
          erase "snot";
          copy "snot" => "badger";
          merge "beep" => {"fun": not false};
          merge => {"tuple": 4 * 12}
        end;
        patch x of
          default => {"bla": "blubb"};
          default "gna" => "gnubb";
          insert "i" => event.foo;
          upsert "snotty" => state.badger[1];
          erase "snot";
          update "snot" => null; # order swapped
          copy "snot" => "badger";
          merge "beep" => {"fun": not false};
          merge => {"tuple": 4 * 12}
        end
        "#,
        false,
    )
}

#[test]
fn test_match_eq() -> Result<()> {
    test_ast_eq(
        r#"
        let x = {"foo": "foo"};
        (match x of
            case "literal string" => "string"
            case event.foo => "event.foo"
            case %( _, 12, ... ) => "has 12 at index 1"
            case %[] => "empty_array"
            case %[ %{ present x }, ~ json|| ] => "complex"
            case %{ absent y, snot == "badger", superhero ~= %{absent name}} => "snot"
            case object = %{ absent y, snot == "badger", superhero ~= %{absent name}} when object.superhero.is_snotty => "snotty_badger"
            case ~ json|| => "json"
            case _ => null
        end);
        (match x of
            case "literal string" => "string"
            case event.foo => "event.foo"
            case %( _, 12, ... ) => "has 12 at index 1"
            case %[] => "empty_array"
            case %[ %{ present x }, ~ json|| ] => "complex"
            case %{ absent y, snot == "badger", superhero ~= %{absent name}} => "snot"
            case object = %{ absent y, snot == "badger", superhero ~= %{absent name}} when object.superhero.is_snotty => "snotty_badger"
            case ~ json|| => "json"
            case _ => null
        end)
        "#,
        true,
    )
}
#[test]
fn test_match_eq_2() -> Result<()> {
    test_ast_eq(
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
            case _ => null
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
            case _ => null
        end)
        "#,
        true,
    )
}
#[test]
fn test_match_not_eq() -> Result<()> {
    test_ast_eq(
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
            case _ => null
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
            case _ => "not_null"
        end)
        "#,
        false,
    )
}

#[test]
fn test_comprehension_eq() -> Result<()> {
    test_ast_eq(
        r#"
        (for event[1][1:3] of
            case (i, e) =>
                {" #{i}": e}
        end);
        (for event[1][1:3] of
            case (i, e) =>
                {" #{i}": e}
        end)
        "#,
        true,
    )
}
#[test]
fn test_comprehension_not_eq() -> Result<()> {
    test_ast_eq(
        r#"
        (for event[1] of
            case (i, e) =>
                {" #{i}": e}
        end);
        (for event[1] of
            case (i, x) =>
                {" #{i}": x}
        end)
        "#,
        false,
    )
}

#[test]
fn merge_eq_test() -> Result<()> {
    test_ast_eq(
        r#"
        (merge event["foo"] of event["bar"] end);
        (merge event.foo of event.bar end);
        "#,
        true,
    )
}

#[test]
fn present_eq_test() -> Result<()> {
    test_ast_eq(
        r#"
        (present event.foo);
        (present event["foo"])
        "#,
        true,
    )
}
#[test]
fn group_path_eq_test() -> Result<()> {
    test_ast_eq(
        "
        group[1];
        group[1]
        ",
        true,
    )
}
#[test]
fn group_path_not_eq_test() -> Result<()> {
    test_ast_eq(
        "
        group[1];
        group[0]
        ",
        false,
    )
}
#[test]
fn window_path_eq_test() -> Result<()> {
    test_ast_eq(
        "
        window;
        window
        ",
        true,
    )
}
#[test]
fn args_path_eq_test() -> Result<()> {
    test_ast_eq(
        "
        args[0];
        args[0]
        ",
        true,
    )
}
#[test]
fn meta_path_eq_test() -> Result<()> {
    test_ast_eq(
        "
        $meta[1];
        $meta[1]
        ",
        true,
    )
}
#[test]
fn complex_path_eq_test() -> Result<()> {
    test_ast_eq(
        r#"
        let local = {};
        local[event.path][event.start:event["end"]];
        local[event.path][event.start:event["end"]]
        "#,
        true,
    )
}

#[test]
fn expr_path() -> Result<()> {
    test_ast_eq(
        r#"
        ({"key": 42})[event];
        ({"key": 42})[event]
        "#,
        true,
    )
}

#[test]
fn string_eq_test() -> Result<()> {
    test_ast_eq(
        r#"
        " #{ event.foo } bar #{ $ }";
        " #{event.foo} bar #{$}";
        "#,
        true,
    )
}

#[test]
fn mul_eq_test() -> Result<()> {
    test_ast_eq(
        "
        event.m1 * event.m2;
        event.m1 * event.m2;
        ",
        true,
    )
}

fn imut_expr() -> ImutExpr<'static> {
    ImutExpr::Path(Path::Event(EventPath {
        mid: NodeMeta::dummy(),
        segments: vec![],
    }))
}

#[test]
fn predicate_pattern() {
    assert!(PredicatePattern::TildeEq {
        assign: "assign".into(),
        lhs: "lhs".into(),
        key: "key".into(),
        test: Box::new(TestExpr {
            mid: NodeMeta::dummy(),
            id: "id".into(),
            test: "test".into(),
            extractor: crate::extractor::Extractor::Json,
        }),
    }
    .ast_eq(&PredicatePattern::TildeEq {
        assign: "assign".into(),
        lhs: "lhs".into(),
        key: "key".into(),
        test: Box::new(TestExpr {
            mid: NodeMeta::dummy(),
            id: "id".into(),
            test: "test".into(),
            extractor: crate::extractor::Extractor::Json,
        }),
    }));
    assert!(PredicatePattern::Bin {
        lhs: "lhs".into(),
        key: "key".into(),
        rhs: imut_expr(),
        kind: BinOpKind::Eq
    }
    .ast_eq(&PredicatePattern::Bin {
        lhs: "lhs".into(),
        key: "key".into(),
        rhs: imut_expr(),
        kind: BinOpKind::Eq
    }));
    assert!(PredicatePattern::RecordPatternEq {
        lhs: "lhs".into(),
        key: "key".into(),
        pattern: RecordPattern {
            mid: NodeMeta::dummy(),
            fields: vec![]
        }
    }
    .ast_eq(&PredicatePattern::RecordPatternEq {
        lhs: "lhs".into(),
        key: "key".into(),
        pattern: RecordPattern {
            mid: NodeMeta::dummy(),
            fields: vec![]
        }
    }));
    assert!(PredicatePattern::ArrayPatternEq {
        lhs: "lhs".into(),
        key: "key".into(),
        pattern: ArrayPattern {
            mid: NodeMeta::dummy(),
            exprs: vec![]
        }
    }
    .ast_eq(&PredicatePattern::ArrayPatternEq {
        lhs: "lhs".into(),
        key: "key".into(),
        pattern: ArrayPattern {
            mid: NodeMeta::dummy(),
            exprs: vec![]
        }
    }));
}

#[test]
fn reserved_path() {
    assert!(ReservedPath::Args {
        mid: NodeMeta::dummy(),
        segments: vec![Segment::Range {
            mid: NodeMeta::dummy(),
            start: 0,
            end: 7
        }]
    }
    .ast_eq(&ReservedPath::Args {
        mid: NodeMeta::dummy(),
        segments: vec![Segment::Range {
            mid: NodeMeta::dummy(),
            start: 0,
            end: 7
        }]
    }));
    assert!(ReservedPath::Window {
        mid: NodeMeta::dummy(),
        segments: vec![Segment::Range {
            mid: NodeMeta::dummy(),
            start: 0,
            end: 7
        }]
    }
    .ast_eq(&ReservedPath::Window {
        mid: NodeMeta::dummy(),
        segments: vec![Segment::Range {
            mid: NodeMeta::dummy(),
            start: 0,
            end: 7
        }]
    }));
    assert!(ReservedPath::Group {
        mid: NodeMeta::dummy(),
        segments: vec![Segment::Range {
            mid: NodeMeta::dummy(),
            start: 0,
            end: 7
        }]
    }
    .ast_eq(&ReservedPath::Group {
        mid: NodeMeta::dummy(),
        segments: vec![Segment::Range {
            mid: NodeMeta::dummy(),
            start: 0,
            end: 7
        }]
    }));
}

#[test]
fn segment() {
    assert!(Segment::Range {
        mid: NodeMeta::dummy(),
        start: 0,
        end: 7
    }
    .ast_eq(&Segment::Range {
        mid: NodeMeta::dummy(),
        start: 0,
        end: 7
    }));
    assert!(Segment::RangeExpr {
        mid: NodeMeta::dummy(),
        start: Box::new(imut_expr()),
        end: Box::new(imut_expr())
    }
    .ast_eq(&Segment::RangeExpr {
        mid: NodeMeta::dummy(),
        start: Box::new(imut_expr()),
        end: Box::new(imut_expr()),
    }));
}
#[test]
fn recur() {
    assert!(ImutExpr::Recur(Recur {
        mid: NodeMeta::dummy(),
        argc: 1,
        open: true,
        exprs: vec![imut_expr()]
    })
    .ast_eq(&ImutExpr::Recur(Recur {
        mid: NodeMeta::dummy(),
        argc: 1,
        open: true,
        exprs: vec![imut_expr()]
    })));
    assert!(!ImutExpr::Recur(Recur {
        mid: NodeMeta::dummy(),
        argc: 1,
        open: false,
        exprs: vec![imut_expr()]
    })
    .ast_eq(&ImutExpr::Recur(Recur {
        mid: NodeMeta::dummy(),
        argc: 0,
        open: false,
        exprs: vec![]
    })));
}
#[test]
fn clause_group() {
    let pc = PredicateClause {
        mid: NodeMeta::dummy(),
        pattern: Pattern::DoNotCare,
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
        mid: NodeMeta::dummy(),
        argc: 2,
        open: true,
        exprs: e.clone(),
    };
    let mut recur2 = Recur {
        mid: NodeMeta::dummy(),
        argc: 2,
        open: true,
        exprs: e,
    };
    assert!(recur1.ast_eq(&recur2));
    recur2.open = false;
    assert!(!recur1.ast_eq(&recur2));
}

#[test]
fn test_path_local_special_case() {
    let path = ImutExpr::Path(Path::Local(LocalPath {
        idx: 1,
        mid: NodeMeta::dummy(),
        segments: vec![Segment::Idx {
            idx: 1,
            mid: NodeMeta::dummy(),
        }],
    }));
    let local = ImutExpr::Local {
        idx: 1,
        mid: NodeMeta::dummy(),
    };
    // has segments
    assert!(!path.ast_eq(&local));
    let path2 = ImutExpr::Path(Path::Local(LocalPath {
        idx: 1,
        mid: NodeMeta::dummy(),
        segments: vec![],
    }));
    assert!(path2.ast_eq(&local));
    // different index
    assert!(!ImutExpr::Path(Path::Local(LocalPath {
        idx: 2,
        mid: NodeMeta::dummy(),
        segments: vec![]
    }))
    .ast_eq(&local));

    assert!(Path::Local(LocalPath {
        idx: 1,
        mid: NodeMeta::dummy(),
        segments: vec![]
    })
    .ast_eq(&local));
}

#[test]
fn test_patch_operation() {
    let mid = NodeMeta::dummy();
    assert!(PatchOperation::Insert {
        ident: "snot".into(),
        expr: ImutExpr::null_lit(mid.clone()),
        mid: NodeMeta::dummy(),
    }
    .ast_eq(&PatchOperation::Insert {
        ident: "snot".into(),
        expr: ImutExpr::null_lit(mid.clone()),
        mid: NodeMeta::dummy(),
    }));

    assert!(PatchOperation::Upsert {
        ident: "snot".into(),
        expr: ImutExpr::null_lit(mid.clone()),
        mid: NodeMeta::dummy(),
    }
    .ast_eq(&PatchOperation::Upsert {
        ident: "snot".into(),
        expr: ImutExpr::null_lit(mid.clone()),
        mid: NodeMeta::dummy(),
    }));

    assert!(PatchOperation::Update {
        ident: "snot".into(),
        expr: ImutExpr::null_lit(mid.clone()),
        mid: NodeMeta::dummy(),
    }
    .ast_eq(&PatchOperation::Update {
        ident: "snot".into(),
        expr: ImutExpr::null_lit(mid.clone()),
        mid: NodeMeta::dummy(),
    }));

    assert!(PatchOperation::Erase {
        ident: "snot".into(),
        mid: NodeMeta::dummy(),
    }
    .ast_eq(&PatchOperation::Erase {
        ident: "snot".into(),
        mid: NodeMeta::dummy(),
    }));

    assert!(PatchOperation::Copy {
        from: "snot".into(),
        to: "badger".into(),
        mid: NodeMeta::dummy(),
    }
    .ast_eq(&PatchOperation::Copy {
        from: "snot".into(),
        to: "badger".into(),
        mid: NodeMeta::dummy(),
    }));
    assert!(PatchOperation::Move {
        from: "snot".into(),
        to: "badger".into(),
        mid: NodeMeta::dummy(),
    }
    .ast_eq(&PatchOperation::Move {
        from: "snot".into(),
        to: "badger".into(),
        mid: NodeMeta::dummy(),
    }));

    assert!(PatchOperation::Merge {
        ident: "snot".into(),
        expr: ImutExpr::null_lit(mid.clone()),
        mid: NodeMeta::dummy(),
    }
    .ast_eq(&PatchOperation::Merge {
        ident: "snot".into(),
        expr: ImutExpr::null_lit(mid.clone()),
        mid: NodeMeta::dummy(),
    }));

    assert!(PatchOperation::MergeRecord {
        expr: ImutExpr::null_lit(mid.clone()),
        mid: NodeMeta::dummy(),
    }
    .ast_eq(&PatchOperation::MergeRecord {
        expr: ImutExpr::null_lit(mid.clone()),
        mid: NodeMeta::dummy(),
    }));

    assert!(PatchOperation::Default {
        ident: "snot".into(),
        expr: ImutExpr::null_lit(mid.clone()),
        mid: NodeMeta::dummy(),
    }
    .ast_eq(&PatchOperation::Default {
        ident: "snot".into(),
        expr: ImutExpr::null_lit(mid.clone()),
        mid: NodeMeta::dummy(),
    }));

    assert!(PatchOperation::DefaultRecord {
        expr: ImutExpr::null_lit(mid.clone()),
        mid: NodeMeta::dummy(),
    }
    .ast_eq(&PatchOperation::DefaultRecord {
        expr: ImutExpr::null_lit(mid),
        mid: NodeMeta::dummy(),
    }));
}
