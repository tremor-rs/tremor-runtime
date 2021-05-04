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

//! Tremor script scripting language

#![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]
// TODO We need this for simd-json-derive
#![allow(clippy::missing_errors_doc)]

/// The Tremor Script AST
pub mod ast;
mod compat;
/// Context struct for tremor-script
pub mod ctx;
mod datetime;
/// Tremor script function doc helper
pub mod docs;
/// Errors
pub mod errors;
/// Grok implementation
pub mod grok;
/// Tremor Script highlighter
pub mod highlighter;
/// Tremor Script Interpreter
pub mod interpreter;
/// The Tremor Script Lexer
pub mod lexer;
// We need this because of lalrpop
#[allow(unused)]
pub(crate) mod parser;
/// Support for module paths
pub mod path;
/// Tremor Script Position
pub mod pos;
/// Prelude module with important exports
pub mod prelude;
/// Tremor Query
pub mod query;
/// Function registry
pub mod registry;
/// Tremor Script
pub mod script;
mod std_lib;
mod tilde;
/// Utility functions
pub mod utils;

extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate rental;

pub use crate::ast::query::SelectType;
pub use crate::ctx::{EventContext, EventOriginUri};
use crate::prelude::*;
pub use crate::query::Query;
pub use crate::registry::{
    aggr as aggr_registry, registry, Aggr as AggrRegistry, CustomFn, Registry, TremorAggrFn,
    TremorAggrFnWrapper, TremorFn, TremorFnWrapper,
};
pub use crate::script::{Return, Script};
use lazy_static::lazy_static;
use std::sync::atomic::{AtomicU32, Ordering};

use ast::{Consts, InvokeAggrFn};
pub use interpreter::{AggrType, FALSE, NULL, TRUE};
pub use tremor_common::stry;
pub use tremor_value::{KnownKey, Object, Value};

/// Default recursion limit
pub static RECURSION_LIMIT: AtomicU32 = AtomicU32::new(1024);

/// recursion limit
#[inline]
pub fn recursion_limit() -> u32 {
    RECURSION_LIMIT.load(Ordering::Relaxed)
}

/// Combined struct for an event value and metadata
#[derive(
    Clone, Debug, PartialEq, Serialize, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub struct ValueAndMeta<'event> {
    v: Value<'event>,
    m: Value<'event>,
}

impl simd_json_derive::Serialize for LineValue {
    fn json_write<W>(&self, writer: &mut W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        self.rent(|d| d.json_write(writer))
    }
}

impl<'input> simd_json_derive::Deserialize<'input> for LineValue {
    fn from_tape(tape: &mut simd_json_derive::Tape<'input>) -> simd_json::Result<Self>
    where
        Self: Sized + 'input,
    {
        let ValueAndMeta { v, m } = simd_json_derive::Deserialize::from_tape(tape)?;

        Ok(Self::new(vec![], |_| {
            ValueAndMeta::from_parts(v.into_static(), m.into_static())
        }))
    }
}

impl<'event> ValueAndMeta<'event> {
    /// A value from it's parts
    #[must_use]
    pub fn from_parts(v: Value<'event>, m: Value<'event>) -> Self {
        Self { v, m }
    }
    /// Event value
    #[must_use]
    pub fn value(&self) -> &Value<'event> {
        &self.v
    }
    /// Event value forced to borrowd mutable (uses `mem::transmute`)
    ///
    /// # Safety
    /// This isn't save, use with care and reason about mutability!
    #[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr, clippy::mut_from_ref)]
    #[must_use]
    pub unsafe fn force_value_mut(&self) -> &mut Value<'event> {
        std::mem::transmute(&self.v)
    }
    /// Event value
    #[must_use]
    pub fn value_mut(&mut self) -> &mut Value<'event> {
        &mut self.v
    }
    /// Event metadata
    #[must_use]
    pub fn meta(&self) -> &Value<'event> {
        &self.m
    }
    /// Deconstruicts the value into it's parts
    #[must_use]
    pub fn into_parts(self) -> (Value<'event>, Value<'event>) {
        (self.v, self.m)
    }
}

impl<'event> Default for ValueAndMeta<'event> {
    fn default() -> Self {
        ValueAndMeta {
            v: Value::object(),
            m: Value::object(),
        }
    }
}

impl<'v> From<Value<'v>> for ValueAndMeta<'v> {
    fn from(v: Value<'v>) -> ValueAndMeta<'v> {
        ValueAndMeta {
            v,
            m: Value::object(),
        }
    }
}

rental! {
    /// Tremor script rentals to work around lifetime
    /// issues
    pub(crate) mod rentals {
        use super::*;

        /// Rental wrapped value with the data it was parsed
        /// from from
        #[rental_mut(covariant, debug)]
        pub struct Value {
            raw: Vec<Vec<u8>>,
            parsed: ValueAndMeta<'raw>
        }

    }
}

impl rentals::Value {
    /// Borrow the parts (event and metadata) from a rental.
    /// This borrows the data as immutable and then transmutes it
    /// to be mutable.
    #[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr)]
    #[must_use]
    pub fn parts<'value, 'borrow>(
        &'borrow self,
    ) -> (&'borrow mut Value<'value>, &'borrow mut Value<'value>)
    where
        'borrow: 'value,
    {
        unsafe {
            let data = self.suffix();
            let unwind_event: &'borrow mut Value<'value> = std::mem::transmute(data.value());
            let event_meta: &'borrow mut Value<'value> = std::mem::transmute(data.meta());
            (unwind_event, event_meta)
        }
    }
    /// Consumes an event into another
    /// This function works around a rental limitation that is meant
    /// to protect its users: Rental does not allow you to get both
    /// the owned and borrowed part at the same time.
    ///
    /// The reason for that is that once those are taken out of the
    /// rental the link of lifetimes between them would be broken
    /// and you'd risk invalid pointers or memory leaks.
    /// We however do not really want to take them out, all we want
    /// is combine two rentals. We can do this since:
    /// 1) the owned values are inside a vector, while the vector
    ///    itself may be relocated by adding to it, the values
    ///    in it will stay in the same location.
    /// 2) we are only ever adding / extending never deleting
    ///    or modifying.
    ///
    /// So what this function does it is crowbars the content
    /// from a rental into an accessible struct then uses this
    /// to modify its content by adding the owned parts of
    /// `other` into the owned part `self` and the running
    /// a merge function on the borrowed parts
    pub fn consume<'run, E, F>(&'run mut self, other: Self, join_f: F) -> Result<(), E>
    where
        E: std::error::Error,
        F: Fn(&mut ValueAndMeta<'static>, ValueAndMeta<'static>) -> Result<(), E>,
    {
        struct ScrewRental {
            pub parsed: ValueAndMeta<'static>,
            pub raw: Vec<Vec<u8>>,
        }
        #[allow(clippy::transmute_ptr_to_ptr)]
        unsafe {
            use std::mem::transmute;
            let self_unrent: &'run mut ScrewRental = transmute(self);
            let mut other_unrent: ScrewRental = transmute(other);
            self_unrent.raw.append(&mut other_unrent.raw);
            join_f(&mut self_unrent.parsed, other_unrent.parsed)?;
        }
        Ok(())
    }
}

impl From<Value<'static>> for rentals::Value {
    fn from(v: Value<'static>) -> Self {
        Self::new(vec![], |_| ValueAndMeta::from(v))
    }
}

impl<T1, T2> From<(T1, T2)> for rentals::Value
where
    Value<'static>: From<T1> + From<T2>,
{
    fn from((v, m): (T1, T2)) -> Self {
        Self::new(vec![], |_| ValueAndMeta {
            v: Value::from(v),
            m: Value::from(m),
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
        // instead of a Box.
        Self::new(vec![], |_| {
            let v = self.suffix();
            ValueAndMeta::from_parts(v.value().clone_static(), v.meta().clone_static())
        })
    }
}
impl Default for LineValue {
    fn default() -> Self {
        Self::new(vec![], |_| ValueAndMeta::default())
    }
}

/// An error occurred while deserializing
/// a value into an Event.
pub enum LineValueDeserError {
    /// The value was missing the `value` key
    ValueMissing,
    /// The value was missing the `metadata` key
    MetaMissing,
}

impl PartialEq for LineValue {
    fn eq(&self, other: &Self) -> bool {
        self.rent(|s| other.rent(|o| s == o))
    }
}

pub use rentals::Value as LineValue;

pub(crate) const NO_AGGRS: [InvokeAggrFn<'static>; 0] = [];
lazy_static! {
    static ref NO_CONSTS: Consts<'static> = Consts::new();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::*;
    use crate::interpreter::AggrType;
    use crate::lexer::TokenSpan;
    use crate::path::ModulePath;
    use halfbrown::hashmap;

    macro_rules! eval {
        ($src:expr, $expected:expr) => {{
            let _r: Registry = registry();
            let mut src: String = format!("{} ", $src.to_string());
            let src1 = src.clone();
            let lexed_tokens: Result<Vec<TokenSpan>> = lexer::Tokenizer::new(&mut src).collect();
            let lexed_tokens = lexed_tokens.expect("");
            let mut filtered_tokens: Vec<Result<TokenSpan>> = Vec::new();

            for t in lexed_tokens {
                let keep = !t.value.is_ignorable();
                if keep {
                    filtered_tokens.push(Ok(t));
                }
            }
            let reg: Registry = registry::registry();
            let runnable: Script =
                Script::parse(&ModulePath { mounts: vec![] }, "<test>", src1, &reg)
                    .expect("parse failed");
            let mut event = Value::object();
            let mut state = Value::null();
            let mut global_map = Value::object();
            let value = runnable.run(
                &EventContext::new(0, None),
                AggrType::Emit,
                &mut event,
                &mut state,
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
            let mut src = format!("{} ", $src).to_string();
            let src1 = src.clone();
            let lexed_tokens: Result<Vec<TokenSpan>> = lexer::Tokenizer::new(&mut src).collect();
            let lexed_tokens = lexed_tokens.expect("");
            let mut filtered_tokens: Vec<Result<TokenSpan>> = Vec::new();

            for t in lexed_tokens {
                let keep = !t.value.is_ignorable();
                if keep {
                    filtered_tokens.push(Ok(t));
                }
            }
            let reg: Registry = registry::registry();
            let runnable: Script =
                Script::parse(&ModulePath { mounts: vec![] }, "<test>", src1, &reg)
                    .expect("parse failed");
            let mut event = Value::object();
            let mut state = Value::null();
            let mut global_map = Value::from(hashmap! {});
            let _value = runnable.run(
                &EventContext::new(0, None),
                AggrType::Emit,
                &mut event,
                &mut state,
                &mut global_map,
            );
            assert_eq!(global_map, $expected);
        }};
    }

    macro_rules! eval_event {
        ($src:expr, $expected:expr) => {{
            let _r: Registry = registry();
            let mut src = format!("{} ", $src).to_string();
            let src1 = src.clone();
            let lexed_tokens: Result<Vec<TokenSpan>> = lexer::Tokenizer::new(&mut src).collect();
            let lexed_tokens = lexed_tokens.expect("");
            let mut filtered_tokens: Vec<Result<TokenSpan>> = Vec::new();

            for t in lexed_tokens {
                let keep = !t.value.is_ignorable();
                if keep {
                    filtered_tokens.push(Ok(t));
                }
            }
            let reg: Registry = registry::registry();
            let runnable: Script =
                Script::parse(&ModulePath { mounts: vec![] }, "<test>", src1, &reg)
                    .expect("parse failed");
            let mut event = Value::object();
            let mut state = Value::null();
            let mut global_map = Value::from(hashmap! {});
            let _value = runnable.run(
                &EventContext::new(0, None),
                AggrType::Emit,
                &mut event,
                &mut state,
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
    fn test_arith_expr() {
        eval!("1 + 1;", Value::from(2));
        eval!("2 - 1;", Value::from(1));
        eval!("1 - 2;", Value::from(-1));
    }

    #[test]
    fn test_assign_local() {
        eval_global!(
            "\"hello\"; let test = [2,4,6,8]; let $out = test;",
            Value::from(hashmap! {
                "out".into() => Value::from(vec![2u64, 4, 6, 8]),
            })
        );
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
                "test".into() => Value::from(vec![2u64, 4, 6, 8]),
            })
        );
        eval_event!(
            "\"hello\"; let $test = [2,4,6,8]; let event.test = [$test];",
            Value::from(hashmap! {
                "test".into() => Value::from(vec![vec![2u64, 4, 6, 8]]),
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
