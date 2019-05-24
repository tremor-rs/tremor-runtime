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
#![recursion_limit = "265"]
#![forbid(warnings)]

mod ast;
mod compat;
pub mod errors;
#[allow(unused, dead_code)]
mod highlighter;
mod interpreter;
mod lexer;
#[allow(unused, dead_code)]
mod parser;
#[allow(unused, dead_code)]
mod pos;
mod registry;
mod runtime;
mod std_lib;
#[allow(unused, dead_code, clippy::transmute_ptr_to_ptr)]
mod str_suffix;
mod tilde;

#[cfg(test)]
#[macro_use]
extern crate matches;

extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate rental;

use serde::de::{Deserialize, Deserializer};
use serde::ser::{self, Serialize};
pub use simd_json::value::borrowed::Map;
pub use simd_json::value::borrowed::Value;
use simd_json::OwnedValue;

pub use crate::interpreter::{LocalMap, Return, Script, ValueStack};
pub use crate::registry::{registry, Context, Registry, TremorFn, TremorFnWrapper};

rental! {
    pub mod rentals {
        use simd_json::value::borrowed;
        use super::*;
        use std::borrow::Cow;


        #[rental_mut(covariant,debug)]
        pub struct Value {
            raw: Box<Vec<u8>>,
            parsed: borrowed::Value<'raw>
        }
    }
}

impl PartialEq<simd_json::OwnedValue> for rentals::Value {
    fn eq(&self, other: &simd_json::OwnedValue) -> bool {
        //TODO: This  is ugly but good enough for now as it's only used in tests
        self.rent(|this| &simd_json::OwnedValue::from(this.clone()) == other)
    }
}

impl From<simd_json::OwnedValue> for rentals::Value {
    fn from(v: simd_json::OwnedValue) -> Self {
        rentals::Value::new(Box::new(vec![]), |_| v.into())
    }
}

impl Clone for LineValue {
    fn clone(&self) -> Self {
        LineValue::new(Box::new(vec![]), |_| {
            self.rent(|parsed| -> Value<'static> {
                Into::<OwnedValue>::into(parsed.clone()).into()
            })
        })
    }
}

impl Serialize for LineValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        self.rent(|d| d.serialize(serializer))
    }
}

impl<'de> Deserialize<'de> for LineValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<LineValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        let r = OwnedValue::deserialize(deserializer)?;
        Ok(LineValue::new(Box::new(vec![]), |_| r.into()))
    }
}

impl PartialEq for LineValue {
    fn eq(&self, other: &LineValue) -> bool {
        self.rent(|s| other.rent(|o| s == o))
    }
}

pub use rentals::Value as LineValue;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interpreter::ValueStack;
    use ast::{Expr, Literal, LiteralValue};
    use halfbrown::hashmap;
    use lexer::{LexerError, TokenFuns, TokenSpan};
    use simd_json::borrowed::{Map, Value};
    use simd_json::json;
    use std::iter::FromIterator;

    #[derive(Clone, Debug)]
    struct FakeContext {}
    impl Context for FakeContext {}

    macro_rules! parse_lit {
        ($src:expr, $expected:pat) => {{
            let _vals: Value = json!({}).into();
            let _r: Registry<()> = registry();
            let lexed_tokens = Vec::from_iter(lexer::tokenizer($src));
            let mut filtered_tokens = Vec::<Result<TokenSpan, LexerError>>::new();

            for t in lexed_tokens.clone().into_iter() {
                let keep = !t.clone().expect("").value.is_ignorable();
                if keep {
                    filtered_tokens.push(t.clone());
                }
            }
            let actual = parser::grammar::ScriptParser::new()
                .parse(filtered_tokens.clone())
                .expect("exeuction failed");
            assert_matches!(
                actual.exprs[0],
                Expr::Literal(Literal {
                    value: $expected, ..
                })
            );
        }};
    }

    macro_rules! eval {
        ($src:expr, $expected:expr) => {{
            let _vals: Value = json!({}).into();
            let _r: Registry<()> = registry();
            let src = format!("{} ", $src);
            let lexed_tokens = Vec::from_iter(lexer::tokenizer(src.as_str()));
            let mut filtered_tokens = Vec::<Result<TokenSpan, LexerError>>::new();

            for t in lexed_tokens.clone().into_iter() {
                let keep = !t.clone().expect("").value.is_ignorable();
                if keep {
                    filtered_tokens.push(t.clone());
                }
            }
            let reg: Registry<FakeContext> = registry::registry();
            let runnable: interpreter::Script<FakeContext> =
                interpreter::Script::parse($src, reg).expect("parse failed");
            let mut event = simd_json::borrowed::Value::Object(Map::new());
            let ctx = FakeContext {};
            let mut global_map = Value::Object(interpreter::LocalMap::new());
            let mut stack = ValueStack::default();
            let value = runnable.run(&ctx, &mut event, &mut global_map, &stack);
            stack.clear();
            assert_eq!(Ok(Return::Emit($expected)), value);
        }};
    }

    macro_rules! eval_global {
        ($src:expr, $expected:expr) => {{
            let _vals: Value = json!({}).into();
            let _r: Registry<()> = registry();
            let src = format!("{}", $src);
            let lexed_tokens = Vec::from_iter(lexer::tokenizer(src.as_str()));
            let mut filtered_tokens = Vec::<Result<TokenSpan, LexerError>>::new();

            for t in lexed_tokens.clone().into_iter() {
                let keep = !t.clone().expect("").value.is_ignorable();
                if keep {
                    filtered_tokens.push(t.clone());
                }
            }
            let reg: Registry<FakeContext> = registry::registry();
            let runnable: interpreter::Script<FakeContext> =
                interpreter::Script::parse($src, reg).expect("parse failed");
            let mut event = simd_json::borrowed::Value::Object(Map::new());
            let ctx = FakeContext {};
            let mut global_map = Value::Object(interpreter::LocalMap::new());
            let mut stack = ValueStack::default();
            let _value = runnable.run(&ctx, &mut event, &mut global_map, &stack);
            stack.clear();
            // dbg!(("eval global", value));
            assert_eq!(global_map, $expected);
        }};
    }

    macro_rules! eval_event {
        ($src:expr, $expected:expr) => {{
            let _vals: Value = json!({}).into();
            let _r: Registry<()> = registry();
            let src = format!("{}", $src);
            let lexed_tokens = Vec::from_iter(lexer::tokenizer(src.as_str()));
            let mut filtered_tokens = Vec::<Result<TokenSpan, LexerError>>::new();

            for t in lexed_tokens.clone().into_iter() {
                let keep = !t.clone().expect("").value.is_ignorable();
                if keep {
                    filtered_tokens.push(t.clone());
                }
            }
            let reg: Registry<FakeContext> = registry::registry();
            let runnable: interpreter::Script<FakeContext> =
                interpreter::Script::parse($src, reg).expect("parse failed");
            let mut event = simd_json::borrowed::Value::Object(Map::new());
            let ctx = FakeContext {};
            let mut global_map = Value::Object(interpreter::LocalMap::new());
            let mut stack = ValueStack::default();
            let _value = runnable.run(&ctx, &mut event, &mut global_map, &stack);
            stack.clear();
            assert_eq!(event, $expected);
        }};
    }

    #[test]
    fn test_literal_expr() {
        use simd_json::OwnedValue;
        use Value::I64;
        parse_lit!("null;", LiteralValue::Native(OwnedValue::Null));
        parse_lit!("true;", LiteralValue::Native(OwnedValue::Bool(true)));
        parse_lit!("false;", LiteralValue::Native(OwnedValue::Bool(false)));
        parse_lit!("0;", LiteralValue::Native(OwnedValue::I64(0)));
        parse_lit!("123;", LiteralValue::Native(OwnedValue::I64(123)));
        parse_lit!("123.456;", LiteralValue::Native(OwnedValue::F64(_))); // 123.456 we can't match aginst float ...
        parse_lit!(
            "123.456e10;",
            LiteralValue::Native(OwnedValue::F64(_)) // 123.456e10 we can't match against float
        );
        parse_lit!("\"hello\";", LiteralValue::Native(OwnedValue::String(_))); // we can't match against a COW
        eval!("null;", Value::Null);
        eval!("true;", Value::Bool(true));
        eval!("false;", Value::Bool(false));
        eval!("0;", Value::I64(0));
        eval!("123;", Value::I64(123));
        eval!("123.456;", Value::F64(123.456));
        eval!("123.456e10;", Value::F64(123.456e10));
        eval!("\"hello\";", Value::String("hello".into()));
        eval!("\"hello\";\"world\";", Value::String("world".into()));
        eval!(
            "true;\"hello\";[1,2,3,4,5];",
            Value::Array(vec![I64(1), I64(2), I64(3), I64(4), I64(5)])
        );
    }

    #[test]
    fn test_let_expr() {
        //use ast::Assign;
        //use ast::LocalPath;
        //use ast::Path;
        //use ast::Segment;
        use Value::I64;
        /*
                parse!(
                    "let test = null;",
                    Expr::Assign(Assign {
                        path: Path::Local(LocalPath {
                            segments: vec![Segment::Ident("test".to_string())],
                            start: pos::Location::new(1,5,5),
                            end: pos::Location::new(1,9,9),
                        }),
                        expr: Box::new(Expr::Literal(Literal::Nil)),
                        start: pos::Location::new(1,5,5),
                        end: pos::Location::new(1,17,4),
                    })
                );
        */
        eval!("let test = null;", Value::Null);
        eval!("let test = 10;", Value::I64(10));
        eval!("let test = 10.2345;", Value::F64(10.2345));
        eval!(
            "\"hello\"; let test = \"world\";",
            Value::String(std::borrow::Cow::Borrowed("world"))
        );
        eval!(
            "\"hello\"; let test = [2,4,6,8];",
            Value::Array(vec![I64(2), I64(4), I64(6), I64(8)])
        );
        eval!(
            "\"hello\"; let $test = \"world\";",
            Value::String(std::borrow::Cow::Borrowed("world"))
        );
        eval!(
            "\"hello\"; let $test = [2,4,6,8];",
            Value::Array(vec![I64(2), I64(4), I64(6), I64(8)])
        );
    }

    #[test]
    fn test_assign_local() {
        use Value::I64;
        eval_global!(
            "\"hello\"; let test = [2,4,6,8]; let $out = test;",
            Value::Object(hashmap! {
                std::borrow::Cow::Borrowed("out") => Value::Array(vec![I64(2), I64(4), I64(6), I64(8)]),
            })
        );
        eval_global!(
            "\"hello\"; let test = [2,4,6,8]; let test = [test]; let $out = test;",
            Value::Object(hashmap! {
                std::borrow::Cow::Borrowed("out") => Value::Array(vec![Value::Array(vec![I64(2), I64(4), I64(6), I64(8)])]),
            })
        );
    }

    #[test]
    fn test_assign_meta() {
        use simd_json::borrowed::Value::Array;
        use simd_json::borrowed::Value::I64;
        eval_global!(
            "\"hello\"; let $test = [2,4,6,8];",
            Value::Object(hashmap! {
                "test".into() => Array(vec![I64(2), I64(4), I64(6), I64(8)]),
            })
        );
        eval_global!(
            "\"hello\"; let test = [2,4,6,8]; let $test = [test];",
            Value::Object(hashmap! {
                "test".into() => Array(vec![Array(vec![I64(2), I64(4), I64(6), I64(8)])]),
            })
        );
    }

    #[test]
    fn test_assign_event() {
        use simd_json::borrowed::Value::Array;
        use simd_json::borrowed::Value::Object;
        use simd_json::borrowed::Value::I64;
        eval_event!(
            "\"hello\"; let event.test = [2,4,6,8];",
            Object(hashmap! {
                std::borrow::Cow::Borrowed("test") => Array(vec![I64(2), I64(4), I64(6), I64(8)]),
            })
        );
        eval_event!(
            "\"hello\"; let $test = [2,4,6,8]; let event.test = [$test];",
            Object(hashmap! {
                std::borrow::Cow::Borrowed("test") => Array(vec![Array(vec![I64(2), I64(4), I64(6), I64(8)])]),
            })
        );
    }

    #[test]
    fn test_single_json_expr_is_valid() {
        eval!("true", Value::Bool(true));
        eval!("true;", Value::Bool(true));
        eval!(
            "{ \"snot\": \"badger\" }",
            Value::Object(hashmap! { "snot".into() => "badger".into() })
        );
    }
}
