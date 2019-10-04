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

pub mod ast;
mod compat;
mod ctx;
mod datetime;
pub mod errors;
pub mod grok;
#[allow(unused, dead_code)]
pub mod highlighter;
pub mod influx;
pub mod interpreter;
pub mod lexer;
#[allow(unused, dead_code)]
pub mod parser;
#[allow(unused, dead_code)]
pub mod pos;
pub mod registry;
pub mod script;
mod std_lib;
#[allow(unused, dead_code, clippy::transmute_ptr_to_ptr)]
mod str_suffix;
mod tilde;
pub use ctx::EventContext;
pub mod prelude;

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
pub use simd_json::value::borrowed::Object;
pub use simd_json::value::borrowed::Value;
use simd_json::value::ValueTrait;

pub use crate::registry::{
    aggr_registry, registry, AggrRegistry, Registry, TremorAggrFn, TremorAggrFnWrapper, TremorFn,
    TremorFnWrapper,
};
pub use crate::script::{QueryRentalWrapper, Return, Script, StmtRentalWrapper};

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ValueAndMeta<'event> {
    pub value: Value<'event>,
    pub meta: Value<'event>,
}

impl<'event> Default for ValueAndMeta<'event> {
    fn default() -> Self {
        ValueAndMeta {
            value: Value::Object(Object::default()),
            meta: Value::Object(Object::default()),
        }
    }
}

impl<'v> From<Value<'v>> for ValueAndMeta<'v> {
    fn from(value: Value<'v>) -> ValueAndMeta<'v> {
        ValueAndMeta {
            value,
            meta: Value::Object(Object::default()),
        }
    }
}

rental! {
    pub mod rentals {
        use simd_json::value::borrowed;
        use super::*;
        use std::borrow::Cow;


        #[rental_mut(covariant,debug)]
        pub struct Value {
            raw: Vec<Vec<u8>>,
            parsed: ValueAndMeta<'raw>
        }

    }
}

impl rentals::Value {
    pub fn parts(&self) -> (&mut Value, &mut Value) {
        #[allow(clippy::transmute_ptr_to_ptr)]
        #[allow(mutable_transmutes)]
        unsafe {
            let data = self.suffix();
            let unwind_event: &mut Value<'_> = std::mem::transmute(&data.value);
            let event_meta: &mut Value<'_> = std::mem::transmute(&data.meta);
            (unwind_event, event_meta)
        }
    }
    pub fn consume<E, F>(&mut self, other: LineValue, join_f: F) -> Result<(), E>
    where
        E: std::error::Error,
        F: Fn(&mut ValueAndMeta<'static>, ValueAndMeta<'static>) -> Result<(), E>,
    {
        // This function works around a rental limitation that is meant
        // to protect it's users: Rental does not allow you to get both
        // the owned and borrowed part at the same time.
        //
        // The reason for that is that once those are taken out of the
        // rental the link of lifetimes between them would be broken
        // and you'd risk invalid pointers or memory leaks.
        // We however do not really want to take them out, all we want
        // is combine two rentals. We can do this since:
        // 1) the owned values are inside a vector, while the vector
        //    itself may be relocated by adding to it, the values
        //    in it will stay in the same location.
        // 2) we are only ever adding / extending never deleting
        //    or modifying.
        //
        // So what this function does it is crowbars the content
        // from a rental into an accessible struct then uses this
        // to modify it's content by adding the owned parts of
        // `other` into the owned part `self` and the running
        // a merge function on the borrowed parts
        pub struct ScrewRental {
            pub parsed: ValueAndMeta<'static>,
            pub raw: Vec<Vec<u8>>,
        }
        #[allow(clippy::transmute_ptr_to_ptr)]
        unsafe {
            use std::mem::transmute;
            let self_unrent: &mut ScrewRental = transmute(self);
            let mut other_unrent: ScrewRental = transmute(other);
            self_unrent.raw.append(&mut other_unrent.raw);
            join_f(&mut self_unrent.parsed, other_unrent.parsed)?;
        }
        Ok(())
    }
}

impl From<simd_json::BorrowedValue<'static>> for rentals::Value {
    fn from(v: simd_json::BorrowedValue<'static>) -> Self {
        rentals::Value::new(vec![], |_| ValueAndMeta {
            value: v,
            meta: Value::Object(Object::new()),
        })
    }
}

impl
    From<(
        simd_json::BorrowedValue<'static>,
        simd_json::BorrowedValue<'static>,
    )> for rentals::Value
{
    fn from(
        v: (
            simd_json::BorrowedValue<'static>,
            simd_json::BorrowedValue<'static>,
        ),
    ) -> Self {
        rentals::Value::new(vec![], |_| ValueAndMeta {
            value: v.0,
            meta: v.1,
        })
    }
}

impl
    From<(
        simd_json::BorrowedValue<'static>,
        simd_json::value::borrowed::Object<'static>,
    )> for rentals::Value
{
    fn from(
        v: (
            simd_json::BorrowedValue<'static>,
            simd_json::value::borrowed::Object<'static>,
        ),
    ) -> Self {
        rentals::Value::new(vec![], |_| ValueAndMeta {
            value: v.0,
            meta: Value::Object(v.1),
        })
    }
}

impl Clone for LineValue {
    fn clone(&self) -> Self {
        // The only safe way to clone a line value is to clone
        // the data and then turn it into a owned value
        // then turn this owned value back into a borrowed value
        // we need to do this dance to 'free' value from the
        // linked lifetime.
        // An alternative would be keeping the raw data in an ARC
        // instea of a Box.
        use simd_json::OwnedValue;
        LineValue::new(vec![], |_| {
            let v = self.suffix();
            ValueAndMeta {
                value: Into::<OwnedValue>::into(v.value.clone()).into(),
                meta: Into::<OwnedValue>::into(v.meta.clone()).into(),
            }
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

pub enum LineValueDeserError {
    ValueMissing,
    MetaMissing,
}

impl<'de> Deserialize<'de> for LineValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<LineValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        use simd_json::OwnedValue;
        // We need to convert the data first into a owned value since
        // serde doesn't like lifetimes. Then we convert it into a line
        // value.
        // FIXME we should find a way to not do this!
        let r = OwnedValue::deserialize(deserializer)?;
        // FIXME after POC
        let value = if let Some(value) = r.get("value") {
            value
        } else {
            return Err(D::Error::custom("value field missing"));
        };
        let meta = if let Some(meta) = r.get("meta") {
            meta
        } else {
            return Err(D::Error::custom("meta field missing"));
        };
        Ok(LineValue::new(vec![], |_| ValueAndMeta {
            value: value.clone().into(),
            meta: meta.clone().into(),
        }))
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
    use crate::ast::{Expr, ImutExpr, Literal};
    use crate::errors::*;
    use crate::interpreter::AggrType;
    use crate::lexer::{TokenFuns, TokenSpan};
    use halfbrown::hashmap;
    use simd_json::borrowed::{Object, Value};

    macro_rules! parse_lit {
        ($src:expr, $expected:pat) => {{
            let r: Registry = registry();
            let ar: AggrRegistry = aggr_registry();
            let lexed_tokens: Result<Vec<TokenSpan>> = lexer::tokenizer($src).collect();
            let lexed_tokens = lexed_tokens.expect("");
            let mut filtered_tokens: Vec<Result<TokenSpan>> = Vec::new();

            for t in lexed_tokens {
                let keep = !t.value.is_ignorable();
                if keep {
                    filtered_tokens.push(Ok(t));
                }
            }

            let actual = parser::grammar::ScriptParser::new()
                .parse(filtered_tokens)
                .expect("execution failed")
                .up_script(&r, &ar)
                .expect("execution failed");
            assert_matches!(
                actual.0.exprs[0],
                Expr::Imut(ImutExpr::Literal(Literal {
                    value: $expected, ..
                }))
            );
        }};
    }

    macro_rules! eval {
        ($src:expr, $expected:expr) => {{
            let _r: Registry = registry();
            let src = format!("{} ", $src);
            let lexed_tokens: Result<Vec<TokenSpan>> = lexer::tokenizer(&src).collect();
            let lexed_tokens = lexed_tokens.expect("");
            let mut filtered_tokens: Vec<Result<TokenSpan>> = Vec::new();

            for t in lexed_tokens {
                let keep = !t.value.is_ignorable();
                if keep {
                    filtered_tokens.push(Ok(t));
                }
            }
            let reg: Registry = registry::registry();
            let runnable: Script = Script::parse($src, &reg).expect("parse failed");
            let mut event = simd_json::borrowed::Value::Object(Object::new());
            let mut global_map = Value::Object(hashmap! {});
            let value = runnable.run(
                &EventContext { at: 0 },
                AggrType::Emit,
                &mut event,
                &mut global_map,
            );
            assert_eq!(
                Ok(Return::Emit {
                    value: $expected,
                    port: None
                }),
                value
            );
        }};
    }

    macro_rules! eval_global {
        ($src:expr, $expected:expr) => {{
            let _r: Registry = registry();
            //let src = format!("{}", $src);
            let lexed_tokens: Result<Vec<TokenSpan>> = lexer::tokenizer($src).collect();
            let lexed_tokens = lexed_tokens.expect("");
            let mut filtered_tokens: Vec<Result<TokenSpan>> = Vec::new();

            for t in lexed_tokens {
                let keep = !t.value.is_ignorable();
                if keep {
                    filtered_tokens.push(Ok(t));
                }
            }
            let reg: Registry = registry::registry();
            let runnable: Script = Script::parse($src, &reg).expect("parse failed");
            let mut event = simd_json::borrowed::Value::Object(Object::new());
            let mut global_map = Value::Object(hashmap! {});
            let _value = runnable.run(
                &EventContext { at: 0 },
                AggrType::Emit,
                &mut event,
                &mut global_map,
            );
            assert_eq!(global_map, $expected);
        }};
    }

    macro_rules! eval_event {
        ($src:expr, $expected:expr) => {{
            let _r: Registry = registry();
            //let src = format!("{}", $src);
            let lexed_tokens: Result<Vec<TokenSpan>> = lexer::tokenizer($src).collect();
            let lexed_tokens = lexed_tokens.expect("");
            let mut filtered_tokens: Vec<Result<TokenSpan>> = Vec::new();

            for t in lexed_tokens {
                let keep = !t.value.is_ignorable();
                if keep {
                    filtered_tokens.push(Ok(t));
                }
            }
            let reg: Registry = registry::registry();
            let runnable: Script = Script::parse($src, &reg).expect("parse failed");
            let mut event = simd_json::borrowed::Value::Object(Object::new());
            let mut global_map = Value::Object(hashmap! {});
            let _value = runnable.run(
                &EventContext { at: 0 },
                AggrType::Emit,
                &mut event,
                &mut global_map,
            );
            assert_eq!(event, $expected);
        }};
    }

    #[test]
    fn test_literal_expr() {
        use simd_json::BorrowedValue;
        use Value::I64;
        parse_lit!("null;", BorrowedValue::Null);
        parse_lit!("true;", BorrowedValue::Bool(true));
        parse_lit!("false;", BorrowedValue::Bool(false));
        parse_lit!("0;", BorrowedValue::I64(0));
        parse_lit!("123;", BorrowedValue::I64(123));
        parse_lit!("123.456;", BorrowedValue::F64(_)); // 123.456 we can't match aginst float ...
        parse_lit!(
            "123.456e10;",
            BorrowedValue::F64(_) // 123.456e10 we can't match against float
        );
        parse_lit!("\"hello\";", BorrowedValue::String(_)); // we can't match against a COW
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
        use Value::I64;
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
    fn test_present() {
        eval!(r#"let t = {}; present t"#, Value::Bool(true));
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.a"#,
            Value::Bool(true)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.a.b"#,
            Value::Bool(true)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.a.b.c"#,
            Value::Bool(true)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.a.b.c[0]"#,
            Value::Bool(true)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.a.b.c[1]"#,
            Value::Bool(true)
        );

        eval!(r#"let t = {}; present r"#, Value::Bool(false));
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.x"#,
            Value::Bool(false)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.a.x"#,
            Value::Bool(false)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.x.b"#,
            Value::Bool(false)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.a.b.x"#,
            Value::Bool(false)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.a.x.c"#,
            Value::Bool(false)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.x.b.c"#,
            Value::Bool(false)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.a.b.c[2]"#,
            Value::Bool(false)
        );
    }

    #[test]
    fn test_assign_local() {
        use Value::I64;
        dbg!();
        eval_global!(
            "\"hello\"; let test = [2,4,6,8]; let $out = test;",
            Value::Object(hashmap! {
                std::borrow::Cow::Borrowed("out") => Value::Array(vec![I64(2), I64(4), I64(6), I64(8)]),
            })
        );
        dbg!();
        eval_global!(
            "\"hello\"; let test = [4,6,8,10]; let test = [test]; let $out = test;",
            Value::Object(hashmap! {
                std::borrow::Cow::Borrowed("out") => Value::Array(vec![Value::Array(vec![I64(4), I64(6), I64(8), I64(10)])]),
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
        use simd_json::borrowed::Value::I64;
        eval_event!(
            "\"hello\"; let event.test = [2,4,6,8];",
            Value::Object(hashmap! {
                std::borrow::Cow::Borrowed("test") => Array(vec![I64(2), I64(4), I64(6), I64(8)]),
            })
        );
        eval_event!(
            "\"hello\"; let $test = [2,4,6,8]; let event.test = [$test];",
            Value::Object(hashmap! {
                std::borrow::Cow::Borrowed("test") => Array(vec![Array(vec![I64(2), I64(4), I64(6), I64(8)])]),
            })
        );
    }

    #[test]
    fn test_single_json_expr_is_valid() {
        eval!("true ", Value::Bool(true));
        eval!("true;", Value::Bool(true));
        eval!(
            "{ \"snot\": \"badger\" }",
            Value::Object(hashmap! { "snot".into() => "badger".into() })
        );
    }
}
