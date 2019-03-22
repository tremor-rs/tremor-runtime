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

#![forbid(warnings)]
#![cfg_attr(
    feature = "cargo-clippy",
    deny(clippy::all, clippy::result_unwrap_used, clippy::unnecessary_unwrap)
)]

pub mod errors;
mod interpreter;
pub mod registry;

pub use crate::interpreter::Script;
pub use crate::registry::{registry, Context, Registry, TremorFn, TremorFnWrapper};
use hashbrown::HashMap;
pub use serde_json::Value;

pub type ValueMap = HashMap<String, Value>;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_glob() {
        let json = r#"{"key1": "data"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse(r#"key1=g"da?a""#, &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_glob_with_slash() {
        let json = r#"{"key1": "d/a/ta"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse(r#"key1=g"d*ta""#, &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_equals() {
        let json = r#"{"key1": "data1"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse(r#"key1="data1""#, &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_quote() {
        let json = r#"{"key1": "da\\u1234ta1"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse(r#"key1="da\u1234ta1""#, &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_subkey_equals() {
        let json = r#"{"key1": {"sub1": "data1"}}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse(r#"key1.sub1="data1""#, &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_compound_strings() {
        let json = r#"{
               "key1": {
                       "subkey1": "data1"
                   },
                   "key2": {
                       "subkey2": "data2"
                    },
                   "key3": "data3"
                }"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse(r#"key1.subkey1="data1" or key3="data3" or (key1.subkey1=g"dat*" and key2.subkey2="data2")"#, &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_int_eq() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key=5", &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_int_gt() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key>1 OR key>4", &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_negint_gt() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key>-6", &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_int_lt() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key<10 key<9", &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(1)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_double_lt() {
        let json = r#"{"key":5.0}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key<10.0 key<9.0", &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(1)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_int_ltoe() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key<=5 key<=11", &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(1)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_int_gtoe() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key >= 3 key >= 4", &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(1)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_int_gtoe_double() {
        let json = r#"{"key":5.0}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key >= 3.5 key >= 4.5", &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(1)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_regex() {
        let json = r#"{"key":"data"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key=/d.*/", &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_regex_false() {
        let json = r#"{"key":"data"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key=/e.*/", &r).expect("Failed to parse script");
        assert_eq!(Ok(None), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_negregex() {
        let json = r#"{"key":"data"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("NOT key=/d.*/", &r).expect("Failed to parse script");
        assert_eq!(Ok(None), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_regex_bug() {
        let json = r#"{"key":"\\/"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse(r#"key=/\\//"#, &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_neg_compound_strings() {
        let json = r#"{
               "key1": {
                       "subkey1": "data1"
                   },
                   "key2": {
                       "subkey2": "data2"
                    },
                   "key3": "data3"
                }"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse(r#"!(key1.subkey1:"data1" OR NOT (key3:"data3") OR NOT (key1.subkey1:"dat" and key2.subkey2="data2"))"#, &r).expect("Failed to parse script");
        assert_eq!(Ok(None), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_list_contains_str() {
        let json = r#"{"key":"data"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s =
            Script::parse(r#"key:["foo", "data", "bar"]"#, &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_list_contains_int() {
        let json = r#"{"key":4}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key:[3, 4, 5]", &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_list_contains_float() {
        let json = r#"{"key":4.1}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key:[3.1, 4.1, 5.1]", &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_jsonlist_contains_str() {
        let json = r#"{"key":["v1", "v2", "v3"]}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse(r#"key:"v2""#, &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_jsonlist_contains_int() {
        let json = r#"{"key":[3, 4, 5]}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key:4", &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_jsonlist_contains_float() {
        let json = r#"{"key":[3.1, 4.1, 5.1]}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key:4.1", &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_bad_rule_syntax() {
        let r: Registry<()> = registry();
        assert_eq!(true, Script::parse(r#""key"#, &r).is_err());
    }

    #[test]
    fn test_ip_match() {
        let json = r#"{"key":"10.66.77.88"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key=10.66.77.88", &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_cidr_match() {
        let json = r#"{"key":"10.66.77.88"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key=10.66.0.0/16", &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_ip_match_false() {
        let json = r#"{"key":"10.66.78.88"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key=10.66.77.88", &r).expect("Failed to parse script");
        assert_eq!(Ok(None), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_cidr_match_false() {
        let json = r#"{"key":"10.67.77.88"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key=10.66.0.0/16", &r).expect("Failed to parse script");
        assert_eq!(Ok(None), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_exists() {
        let json = r#"{"key":"10.67.77.88"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key", &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_missing() {
        let json = r#"{"key":"10.67.77.88"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("!key", &r).expect("Failed to parse script");
        assert_eq!(Ok(None), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_add() {
        let json = r#"{"key":"val"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s =
            Script::parse(r#"key="val" {key2 := "newval";}"#, &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
        assert_eq!(vals["key2"], json!("newval"));
    }

    #[test]
    fn test_add_nested() {
        let json = r#"{"key":"val"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse(r#"key="val" {newkey.newsubkey.other := "newval";}"#, &r)
            .expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
        assert_eq!(vals["newkey"]["newsubkey"]["other"], json!("newval"));
    }

    #[test]
    fn test_update() {
        let json = r#"{"key":"val"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse(r#"_ {key := "newval";}"#, &r).expect("Failed to parse script");
        let _ = s.run(&(), &mut vals, &mut HashMap::new());
        //        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
        assert_eq!(vals["key"], json!("newval"));
    }

    #[test]
    fn test_update_nested() {
        let json = r#"{"key":{"key1": "val"}}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s =
            Script::parse(r#"_ {key.key1 := "newval";}"#, &r).expect("Failed to parse script");
        let _ = s.run(&(), &mut vals, &mut HashMap::new());
        //        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
        assert_eq!(vals["key"]["key1"], json!("newval"));
    }

    #[test]
    fn test_update_type_conflict() {
        let json = r#"{"key":"key1"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s =
            Script::parse(r#"_ {key.key1 := "newval";}"#, &r).expect("Failed to parse script");
        let _ = s.run(&(), &mut vals, &mut HashMap::new());

        // assert_eq!(
        //     Err(ErrorCode::MutationError(MutationError::TypeConflict)),
        //     s.run(&(), &mut vals, &mut HashMap::new())
        // );
        assert_eq!(vals["key"], json!("key1"));
    }

    #[test]
    fn test_mut_multi() {
        let json = r#"{"key":"val"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse(
            r#"key="val" {
                           newkey.newsubkey.other := "newval";
                           newkey.newsubkey.other2 := "newval2";
                         }"#,
            &r,
        )
        .expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
        assert_eq!(
            Value::String("newval".to_string()),
            vals["newkey"]["newsubkey"]["other"]
        );
        assert_eq!(
            Value::String("newval2".to_string()),
            vals["newkey"]["newsubkey"]["other2"]
        );
    }

    #[test]
    fn test_underscore() {
        let json = r#"{"key":"val"}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse(r#"_"#, &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn doctest_time_test() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).expect("failed to parse json");
        let r: Registry<()> = registry();
        let mut s = Script::parse("key<10 key<9", &r).expect("Failed to parse script");
        assert_eq!(Ok(Some(1)), s.run(&(), &mut vals, &mut HashMap::new()));
    }

    #[test]
    fn test_mutate_var() {
        let mut m: ValueMap = HashMap::new();

        m.insert("a".to_string(), json!(1));
        m.insert("b".to_string(), json!(1));
        m.insert("c".to_string(), json!(1));

        let mut v = Value::Null;

        let script = r#"
import a, b;
export b, c, actual, actual1, actual2;


_ { $actual1:= false; $actual2 := false; $actual := false; }

$a=1 && $b=1 && !$c { $actual1 := true; }

_ { $a := 2; $b := 3; $c := 4; }

$a=2 && $b=3 && $c=4 && $actual1=true { $actual2 := true; }

$actual1=true && $actual2=true { $actual := true; }

"#;
        let r: Registry<()> = registry();
        let mut s = Script::parse(script, &r).expect("Failed to parse script");
        let _ = s.run(&(), &mut v, &mut m);
        assert_eq!(m["a"], json!(1));
        assert_eq!(m["b"], json!(3));
        assert_eq!(m["c"], json!(4));
        assert_eq!(m["actual1"], json!(true));
        assert_eq!(m["actual2"], json!(true));
        assert_eq!(m["actual"], json!(true));
    }

    #[test]
    fn test_mutate() {
        let mut m: ValueMap = HashMap::new();

        let mut v = json!(
            {
                "a": 1,
                "b": 1,
            }
        );
        let script = r#"
export actual, actual1, actual2;

_ { $actual1:= false; $actual2 := false; $actual := false; }

a=1 && b=1 && !c { $actual1 := true; }

_ { b := 3; c := 4; }

a=1 && b=3 && c=4 && $actual1=true { $actual2 := true; }

$actual1=true && $actual2=true { $actual := true; }

"#;
        let r: Registry<()> = registry();
        let mut s = Script::parse(script, &r).expect("Failed to parse script");
        let _ = s.run(&(), &mut v, &mut m);
        assert_eq!(v["a"], json!(1));
        assert_eq!(v["b"], json!(3));
        assert_eq!(v["c"], json!(4));
        assert_eq!(m["actual1"], json!(true));
        assert_eq!(m["actual2"], json!(true));
        assert_eq!(m["actual"], json!(true));
    }

    #[test]
    fn unknown_key_test() {
        let mut m: ValueMap = HashMap::new();
        let mut v = json!({"a": 1});
        let script = r#"
export b;
v:1 { $b := 2; return; }
_ { $b := 3; }
"#;
        let r: Registry<()> = registry();
        let mut s = Script::parse(script, &r).expect("Failed to parse script");
        let _ = s.run(&(), &mut v, &mut m);
        assert_eq!(v["a"], json!(1));
        assert_eq!(m["b"], json!(3));
    }
    #[test]
    #[ignore]
    fn comment_before_interface() {
        let mut m: ValueMap = HashMap::new();
        let mut v = json!({"a": 1});

        let script = r#"
# comment
export b;
_ { $b := 2; }
"#;
        let r: Registry<()> = registry();
        let mut s = Script::parse(script, &r).expect("Failed to parse script");
        let _ = s.run(&(), &mut v, &mut m);
        assert_eq!(v["a"], json!(1));
        assert_eq!(m["b"], json!(2));
    }

    #[test]
    fn comment_after_interface() {
        let mut m: ValueMap = HashMap::new();
        let mut v = json!({"a": 1});

        let script = r#"
export b;
# comment
_ { $b := 2; }
"#;
        let r: Registry<()> = registry();
        let mut s = Script::parse(script, &r).expect("Failed to parse script");
        let _ = s.run(&(), &mut v, &mut m);
        assert_eq!(v["a"], json!(1));
        assert_eq!(m["b"], json!(2));
    }

}
