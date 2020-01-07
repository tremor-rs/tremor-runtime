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
#![forbid(warnings)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::result_unwrap_used,
    clippy::option_unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]
#![allow(clippy::must_use_candidate)]

pub mod ast;
mod compat;
mod ctx;
mod datetime;
pub mod docs;
pub mod errors;
pub mod grok;
pub mod highlighter;
pub mod influx;
pub mod interpreter;
pub mod lexer;
// We need this because of lalrpop
#[allow(unused)]
pub mod parser;

pub mod pos;
pub mod query;
pub mod registry;
pub mod script;
mod std_lib;
mod tilde;
pub mod utils;
pub use ctx::{EventContext, EventOriginUri};
pub use interpreter::{AggrType, FALSE, NULL, TRUE};
pub mod prelude;

extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate rental;

use serde::de::{Deserialize, Deserializer};
use serde::ser::{self, Serialize};
pub use simd_json::value::borrowed::Object;
pub use simd_json::value::borrowed::Value;
use simd_json::value::Value as ValueTrait;

pub use crate::registry::{
    aggr as aggr_registry, registry, Aggr as AggrRegistry, Registry, TremorAggrFn,
    TremorAggrFnWrapper, TremorFn, TremorFnWrapper,
};
pub use crate::script::{Return, Script};

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ValueAndMeta<'event> {
    pub value: Value<'event>,
    pub meta: Value<'event>,
}

impl<'event> Default for ValueAndMeta<'event> {
    fn default() -> Self {
        ValueAndMeta {
            value: Value::from(Object::default()),
            meta: Value::from(Object::default()),
        }
    }
}

impl<'v> From<Value<'v>> for ValueAndMeta<'v> {
    fn from(value: Value<'v>) -> ValueAndMeta<'v> {
        ValueAndMeta {
            value,
            meta: Value::from(Object::default()),
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
    #[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr)]
    pub fn parts(&self) -> (&mut Value, &mut Value) {
        unsafe {
            let data = self.suffix();
            let unwind_event: &mut Value<'_> = std::mem::transmute(&data.value);
            let event_meta: &mut Value<'_> = std::mem::transmute(&data.meta);
            (unwind_event, event_meta)
        }
    }
    pub fn consume<E, F>(&mut self, other: Self, join_f: F) -> Result<(), E>
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
        Self::new(vec![], |_| ValueAndMeta {
            value: v,
            meta: Value::from(Object::new()),
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
        Self::new(vec![], |_| ValueAndMeta {
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
        Self::new(vec![], |_| ValueAndMeta {
            value: v.0,
            meta: Value::from(v.1),
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
        Self::new(vec![], |_| {
            let v = self.suffix();
            ValueAndMeta {
                value: v.value.clone_static(),
                meta: v.meta.clone_static(),
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
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
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
        Ok(Self::new(vec![], |_| ValueAndMeta {
            value: value.clone().into(),
            meta: meta.clone().into(),
        }))
    }
}

impl PartialEq for LineValue {
    fn eq(&self, other: &Self) -> bool {
        self.rent(|s| other.rent(|o| s == o))
    }
}

pub use rentals::Value as LineValue;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::*;
    use crate::interpreter::AggrType;
    use crate::lexer::{TokenFuns, TokenSpan};
    use halfbrown::hashmap;
    use simd_json::borrowed::{Object, Value};
    use simd_json::ValueBuilder;

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
            let mut event = Value::from(Object::new());
            let mut global_map = Value::from(Object::new());
            let value = runnable.run(
                &EventContext {
                    at: 0,
                    origin_uri: None,
                },
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
            let mut event = Value::object();
            let mut global_map = Value::from(hashmap! {});
            let _value = runnable.run(
                &EventContext {
                    at: 0,
                    origin_uri: None,
                },
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
            let mut event = Value::object();
            let mut global_map = Value::from(hashmap! {});
            let _value = runnable.run(
                &EventContext {
                    at: 0,
                    origin_uri: None,
                },
                AggrType::Emit,
                &mut event,
                &mut global_map,
            );
            assert_eq!(event, $expected);
        }};
    }

    #[test]
    fn test_literal_expr() {
        eval!("null;", Value::null());
        eval!("true;", Value::from(true));
        eval!("false;", Value::from(false));
        eval!("0;", Value::from(0));
        eval!("123;", Value::from(123));
        eval!("123.456;", Value::from(123.456)); // 123.456 we can't match aginst float ...
        eval!(
            "123.456e10;",
            Value::from(123.456e10) // 123.456e10 we can't match against float
        );
        eval!("\"hello\";", Value::from("hello")); // we can't match against a COW
        eval!("null;", Value::null());
        eval!("true;", Value::from(true));
        eval!("false;", Value::from(false));
        eval!("0;", Value::from(0));
        eval!("123;", Value::from(123));
        eval!("123.456;", Value::from(123.456));
        eval!("123.456e10;", Value::from(123.456e10));
        eval!("\"hello\";", Value::from("hello"));
        eval!("\"hello\";\"world\";", Value::from("world"));
        eval!(
            "true;\"hello\";[1,2,3,4,5];",
            Value::from(vec![1u64, 2, 3, 4, 5])
        );
        eval!(
            "true;\"hello\";[1,2,3,4,5,];",
            Value::from(vec![1u64, 2, 3, 4, 5])
        );
    }

    #[test]
    fn test_let_expr() {
        eval!("let test = null;", Value::null());
        eval!("let test = 10;", Value::from(10));
        eval!("let test = 10.2345;", Value::from(10.2345));
        eval!("\"hello\"; let test = \"world\";", Value::from("world"));
        eval!(
            "\"hello\"; let test = [2,4,6,8];",
            Value::from(vec![2u64, 4, 6, 8])
        );
        eval!("\"hello\"; let $test = \"world\";", Value::from("world"));
        eval!(
            "\"hello\"; let $test = [2,4,6,8];",
            Value::from(vec![2u64, 4, 6, 8])
        );
    }

    #[test]
    fn test_present() {
        eval!(r#"let t = {}; present t"#, Value::from(true));
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"],}}}; present t.a"#,
            Value::from(true)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.a.b"#,
            Value::from(true)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]},}}; present t.a.b.c"#,
            Value::from(true)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.a.b.c[0]"#,
            Value::from(true)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.a.b.c[1]"#,
            Value::from(true)
        );

        eval!(r#"let t = {}; present r"#, Value::from(false));
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.x"#,
            Value::from(false)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.a.x"#,
            Value::from(false)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.x.b"#,
            Value::from(false)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.a.b.x"#,
            Value::from(false)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.a.x.c"#,
            Value::from(false)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.x.b.c"#,
            Value::from(false)
        );
        eval!(
            r#"let t = {"a":{"b": {"c": ["d", "e"]}}}; present t.a.b.c[2]"#,
            Value::from(false)
        );
    }

    #[test]
    fn test_assign_local() {
        dbg!();
        eval_global!(
            "\"hello\"; let test = [2,4,6,8]; let $out = test;",
            Value::from(hashmap! {
                "out".into() => Value::from(vec![2u64, 4, 6, 8]),
            })
        );
        dbg!();
        eval_global!(
            "\"hello\"; let test = [4,6,8,10]; let test = [test]; let $out = test;",
            Value::from(hashmap! {
                "out".into() => Value::from(vec![vec![4u64, 6, 8, 10]]),
            })
        );
    }

    #[test]
    fn test_assign_meta() {
        eval_global!(
            "\"hello\"; let $test = [2,4,6,8];",
            Value::from(hashmap! {
                "test".into() => Value::from(vec![2u64, 4, 6, 8]),
            })
        );
        eval_global!(
            "\"hello\"; let test = [2,4,6,8]; let $test = [test];",
            Value::from(hashmap! {
                "test".into() => Value::from(vec![vec![2u64, 4, 6, 8]]),
            })
        );
    }

    #[test]
    fn test_assign_event() {
        eval_event!(
            "\"hello\"; let event.test = [2,4,6,8];",
            Value::from(hashmap! {
                std::borrow::Cow::Borrowed("test") => Value::from(vec![2u64, 4, 6, 8]),
            })
        );
        eval_event!(
            "\"hello\"; let $test = [2,4,6,8]; let event.test = [$test];",
            Value::from(hashmap! {
                std::borrow::Cow::Borrowed("test") => Value::from(vec![vec![2u64, 4, 6, 8]]),
            })
        );
    }

    #[test]
    fn test_single_json_expr_is_valid() {
        eval!("true ", Value::from(true));
        eval!("true;", Value::from(true));
        eval!(
            "{ \"snot\": \"badger\" }",
            Value::from(hashmap! { "snot".into() => "badger".into() })
        );
    }
}
