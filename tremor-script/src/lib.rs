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
    clippy::pedantic,
    clippy::mod_module_files
)]

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

#[macro_use]
extern crate serde;
/// Memory arena for souces
pub mod arena;
/// The Tremor Script AST
pub mod ast;
/// Context struct for tremor-script
pub mod ctx;
mod datetime;
/// Tremor Deploy ( troy )
pub mod deploy;

/// Tremor script function doc helper
pub mod docs;
/// Errors
pub mod errors;
mod extractor;
/// Grok implementation
pub mod grok;
/// Tremor Script highlighter
pub mod highlighter;
/// Tremor Script Interpreter
pub mod interpreter;
/// The Tremor Script Lexer
pub mod lexer;
pub(crate) mod parser;
/// Support for module paths
pub mod path;
/// Tremor Script Position
pub mod pos;
/// Prelude module with important exports
pub mod prelude;
/// Tremor Query ( trickle )
pub mod query;
/// Function registry
pub mod registry;
/// Tremor Script
pub mod script;
/// Self referential structs
pub mod srs;
mod std_lib;
/// Utility functions
pub mod utils;
pub use srs::{EventPayload, ValueAndMeta};

pub use crate::ast::deploy::raw::run_script;
pub use crate::ast::module;
pub use crate::ast::query::SelectType;
pub use crate::ast::NodeMeta;
pub use crate::ctx::{EventContext, EventOriginUri};
pub use crate::errors::{Kind as ErrorKind, Result};
pub use crate::query::Query;
pub use crate::registry::{
    aggr as aggr_registry, registry, Aggr as AggrRegistry, CustomFn, Registry, TremorAggrFn,
    TremorAggrFnWrapper, TremorFn, TremorFnWrapper,
};
pub use crate::script::{Return, Script};
use ast::{Consts, InvokeAggrFn};
pub use interpreter::{AggrType, FALSE, NULL, TRUE};
use lazy_static::lazy_static;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    RwLock,
};
use tremor_common::stry;
use tremor_value::{KnownKey, Object, Value};

/// Default recursion limit
pub static RECURSION_LIMIT: AtomicU32 = AtomicU32::new(1024);
/// No aggregates
pub const NO_AGGRS: [InvokeAggrFn<'static>; 0] = [];

/// recursion limit
#[inline]
pub fn recursion_limit() -> u32 {
    RECURSION_LIMIT.load(Ordering::Relaxed)
}

lazy_static! {
    /// No Constants
    pub static ref NO_CONSTS: Consts<'static> = Consts::new();
}

lazy_static! {
    /// Function registory for the pipeline to look up functions
    // We wrap the registry in a mutex so that we can add functions from the outside
    // if required.
    pub static ref FN_REGISTRY: RwLock<Registry> = {
        let registry: Registry = crate::registry();
        RwLock::new(registry)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interpreter::AggrType;
    use crate::prelude::*;

    macro_rules! eval {
        ($src:expr, $expected:expr) => {{
            let reg: Registry = registry::registry();
            let runnable: Script = Script::parse($src, &reg).expect("parse failed");
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
            let reg: Registry = registry::registry();
            let runnable: Script = Script::parse($src, &reg).expect("parse failed");
            let mut event = Value::object();
            let mut state = Value::null();
            let mut global_map = Value::object();
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
            let reg: Registry = registry::registry();
            let runnable: Script = Script::parse($src, &reg).expect("parse failed");
            let mut event = Value::object();
            let mut state = Value::null();
            let mut global_map = Value::object();
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
            Value::from(vec![1_u64, 2, 3, 4, 5])
        );
        eval!(
            "true;\"hello\";[1,2,3,4,5,];",
            Value::from(vec![1_u64, 2, 3, 4, 5])
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
            Value::from(vec![2_u64, 4, 6, 8])
        );
        eval!("\"hello\"; let $test = \"world\";", Value::from("world"));
        eval!(
            "\"hello\"; let $test = [2,4,6,8];",
            Value::from(vec![2_u64, 4, 6, 8])
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
            literal!({
                "out": vec![2_u64, 4, 6, 8],
            })
        );
        eval_global!(
            "\"hello\"; let test = [4,6,8,10]; let test = [test]; let $out = test;",
            literal!({
                "out": vec![vec![4_u64, 6, 8, 10]],
            })
        );
    }

    #[test]
    fn test_assign_meta() {
        eval_global!(
            "\"hello\"; let $test = [2,4,6,8];",
            literal!({
                "test": vec![2_u64, 4, 6, 8],
            })
        );
        eval_global!(
            "\"hello\"; let test = [2,4,6,8]; let $test = [test];",
            literal!({
                "test": vec![vec![2_u64, 4, 6, 8]],
            })
        );
    }

    #[test]
    fn test_assign_event() {
        eval_event!(
            "\"hello\"; let event.test = [2,4,6,8];",
            literal!({
                "test": vec![2_u64, 4, 6, 8],
            })
        );
        eval_event!(
            "\"hello\"; let $test = [2,4,6,8]; let event.test = [$test];",
            literal!({
                "test": vec![vec![2_u64, 4, 6, 8]],
            })
        );
    }

    #[test]
    fn test_single_json_expr_is_valid() {
        eval!("true ", Value::from(true));
        eval!("true;", Value::from(true));
        eval!("{ \"snot\": \"badger\" }", literal!({ "snot": "badger"}));
    }
}
