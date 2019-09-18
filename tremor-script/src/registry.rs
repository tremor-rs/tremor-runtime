// Copyright 2018-2019, Wayfair GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a cstd::result::Result::Err(*right_val)::Result::Err(*right_val)License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::ast::BaseExpr;
use crate::tremor_fn;
use halfbrown::HashMap;
use hostname::get_hostname;
use simd_json::BorrowedValue as Value;
use std::default::Default;
use std::fmt;
use std::ops::Range;

pub trait TremorAggrFn: Send {
    fn accumulate<'event>(&mut self, args: &[Value<'event>]) -> FResult<()>;
    fn compensate<'event>(&mut self, args: &[Value<'event>]) -> FResult<()>;
    fn emit<'event>(&self) -> FResult<Value<'event>>;
    fn init(&mut self);
    fn snot_clone(&self) -> Box<dyn TremorAggrFn>;
    fn arity(&self) -> Range<usize>;
    fn valid_arity(&self, n: usize) -> bool {
        self.arity().contains(&n)
    }
}

pub type TremorFn<Ctx> = for<'event> fn(&Ctx, &[&Value<'event>]) -> FResult<Value<'event>>;
pub type FResult<T> = std::result::Result<T, FunctionError>;

#[allow(unused_variables)]
pub fn registry<Ctx: 'static + Context>() -> Registry<Ctx> {
    let mut registry = Registry::default();

    registry.insert(tremor_fn!(system::hostname(_context) {
        if let Some(hostname) = get_hostname(){
            Ok(Value::from(hostname))
        } else {
            Err(FunctionError::RuntimeError{
                mfa: mfa("system", "hostname", 0),
                error: "could not get hostname".into()
            })
        }
    }));
    crate::std_lib::load(&mut registry);
    registry
}

#[allow(unused_variables)]
pub fn aggr_registry() -> AggrRegistry {
    let mut registry = AggrRegistry::default();
    crate::std_lib::load_aggr(&mut registry);
    registry
}

#[derive(Clone, Debug, PartialEq)]
pub struct MFA {
    m: String,
    f: String,
    a: usize,
}

pub fn mfa(m: &str, f: &str, a: usize) -> MFA {
    MFA {
        m: m.to_string(),
        f: f.to_string(),
        a,
    }
}

pub fn to_runtime_error<E: core::fmt::Display>(mfa: MFA, e: E) -> FunctionError {
    FunctionError::RuntimeError {
        mfa,
        error: format!("{}", e),
    }
}

#[derive(Debug, PartialEq)]
pub enum FunctionError {
    BadArity { mfa: MFA, calling_a: usize },
    RuntimeError { mfa: MFA, error: String },
    MissingModule { m: String },
    MissingFunction { m: String, f: String },
    BadType { mfa: MFA },
}

impl FunctionError {
    pub fn into_err<Ctx: Context, O: BaseExpr, I: BaseExpr>(
        self,
        outer: &O,
        inner: &I,
        registry: Option<&Registry<Ctx>>,
    ) -> crate::errors::Error {
        use crate::errors::{best_hint, ErrorKind};
        use FunctionError::*;
        let outer = outer.extent();
        let inner = inner.extent();
        match self {
            BadArity { mfa, calling_a } => {
                ErrorKind::BadArity(outer, inner, mfa.m, mfa.f, mfa.a..mfa.a, calling_a).into()
            }
            RuntimeError { mfa, error } => {
                ErrorKind::RuntimeError(outer, inner, mfa.m, mfa.f, mfa.a, error).into()
            }
            MissingModule { m } => {
                let suggestion = if let Some(registry) = registry {
                    let modules: Vec<String> = registry.functions.keys().cloned().collect();
                    best_hint(&m, &modules, 2)
                } else {
                    None
                };
                ErrorKind::MissingModule(outer, inner, m, suggestion).into()
            }
            MissingFunction { m, f } => {
                let suggestion = if let Some(registry) = registry {
                    if let Some(module) = registry.functions.get(&m) {
                        let functions: Vec<String> = module.keys().cloned().collect();
                        best_hint(&m, &functions, 2)
                    } else {
                        // This should never happen but lets be safe
                        None
                    }
                } else {
                    None
                };
                ErrorKind::MissingFunction(outer, inner, m, f, suggestion).into()
            }
            BadType { mfa } => ErrorKind::BadType(outer, inner, mfa.m, mfa.f, mfa.a).into(),
        }
    }
}

pub trait Context: Default + Clone + PartialEq + std::fmt::Debug {
    fn ingest_ns(&self) -> u64;
}

impl Context for () {
    fn ingest_ns(&self) -> u64 {
        0
    }
}

#[derive(Clone)]
pub struct TremorFnWrapper<Ctx: Context> {
    pub module: String,
    pub name: String,
    pub fun: TremorFn<Ctx>,
    pub argc: usize,
}

impl<Ctx: Context + 'static> TremorFnWrapper<Ctx> {
    pub fn invoke<'event>(&self, context: &Ctx, args: &[&Value<'event>]) -> FResult<Value<'event>> {
        (self.fun)(context, args)
    }
}

impl<Ctx: Context + 'static> fmt::Debug for TremorFnWrapper<Ctx> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}::{}", self.module, self.name)
    }
}
impl<Ctx: Context + 'static> PartialEq for TremorFnWrapper<Ctx> {
    fn eq(&self, other: &TremorFnWrapper<Ctx>) -> bool {
        self.module == other.module && self.name == other.name
    }
}

#[derive(Debug, Clone)]
pub struct Registry<Ctx: Context + 'static> {
    functions: HashMap<String, HashMap<String, TremorFnWrapper<Ctx>>>,
}

#[macro_export]
macro_rules! tremor_fn {

    ($module:ident :: $name:ident($context:ident, $($arg:ident),*) $code:block) => {
        {
            macro_rules! replace_expr {
                ($_t:tt $sub:expr) => {
                    $sub
                };
            }
            use simd_json::BorrowedValue as Value;
            use $crate::TremorFnWrapper;
            #[allow(unused_imports)] // We might not use all of this imports
            use $crate::registry::{FResult, FunctionError, mfa, MFA, to_runtime_error as to_runtime_error_ext};
            const ARGC: usize = {0usize $(+ replace_expr!($arg 1usize))*};
            fn $name<'event, 'c, Ctx: $crate::Context + 'static>($context: &'c Ctx, args: &[&Value<'event>]) -> FResult<Value<'event>>{

                fn this_mfa() -> MFA {
                    mfa(stringify!($module), stringify!($name), ARGC)
                }
                #[allow(dead_code)] // We want to expose this
                fn to_runtime_error<E: core::fmt::Display>(error: E) -> FunctionError {
                    to_runtime_error_ext(this_mfa(), error)
                }
                // rust claims that the pattern is unreachable even
                // though it isn't, so linting it
                match args {
                    [$(
                        $arg,
                    )*] => {$code}
                    _ => Err(FunctionError::BadArity{
                        mfa: this_mfa(),
                        calling_a:args.len()
                    })
                }
            }
            TremorFnWrapper {
                module: stringify!($module).to_string(),
                name: stringify!($name).to_string(),
                argc: ARGC,
                fun: $name
            }
        }
    };

    ($module:ident :: $name:ident($context:ident, $($arg:ident : $type:ident),*) $code:block) => {
        {
            macro_rules! replace_expr {
                ($_t:tt $sub:expr) => {
                    $sub
                };
            }
            use simd_json::BorrowedValue as Value;
            use $crate::TremorFnWrapper;
            #[allow(unused_imports)] // We might not use all of this imports
            use $crate::registry::{FResult, FunctionError, mfa, MFA, to_runtime_error as to_runtime_error_ext};
            const ARGC: usize = {0usize $(+ replace_expr!($arg 1usize))*};
            fn $name<'event, 'c, Ctx: $crate::Context + 'static>($context: &'c Ctx, args: &[&Value<'event>]) -> FResult<Value<'event>>{
                fn this_mfa() -> MFA {
                    mfa(stringify!($module), stringify!($name), ARGC)
                }
                #[allow(dead_code)] // We want to expose this
                fn to_runtime_error<E: core::fmt::Display>(error: E) -> FunctionError {
                    to_runtime_error_ext(this_mfa(), error)
                }
                // rust claims that the pattern is unreachable even
                // though it isn't, so linting it
                match args {
                    [$(
                        Value::$type($arg),
                    )*] => {$code}
                    [$(
                        $arg,
                    )*] => Err(FunctionError::BadType{
                        mfa: this_mfa(),
                    }),
                    _ => Err(FunctionError::BadArity{
                        mfa: this_mfa(),
                        calling_a: args.len()
                    })
                }
            }
            TremorFnWrapper {
                module: stringify!($module).to_string(),
                name: stringify!($name).to_string(),
                fun: $name,
                argc: ARGC
            }
        }
    };

    ($module:ident :: $name:ident($context:ident) $code:block) => {
        {
            use simd_json::BorrowedValue as Value;
            use $crate::TremorFnWrapper;
            #[allow(unused_imports)] // We might not use all of this imports
            use $crate::registry::{FResult, FunctionError, mfa, MFA, to_runtime_error as to_runtime_error_ext};
            const ARGC: usize = 0;
            fn $name<'event, 'c, Ctx: $crate::Context + 'static>($context: &'c Ctx, args: &[&Value<'event>]) -> FResult<Value<'event>>{
                fn this_mfa() -> MFA {
                    mfa(stringify!($module), stringify!($name), ARGC)
                }
                #[allow(dead_code)] // We want to expose this
                fn to_runtime_error<E: core::fmt::Display>(error: E) -> FunctionError {
                    to_runtime_error_ext(this_mfa(), error)
                }
                // rust claims that the pattern is unreachable even
                // though it isn't, so linting it
                match args {
                    [] => {$code}
                    _ => Err(FunctionError::BadArity{
                        mfa: this_mfa(),
                        calling_a: args.len()
                    })
                }
            }
            TremorFnWrapper {
                module: stringify!($module).to_string(),
                name: stringify!($name).to_string(),
                fun: $name,
                argc: 0
            }
        }
    };
}

impl<Ctx: Context + 'static> Default for Registry<Ctx> {
    fn default() -> Self {
        Registry {
            functions: HashMap::new(),
        }
    }
}

impl<Ctx: Context + 'static> Registry<Ctx> {
    pub fn find(&self, module: &str, function: &str) -> FResult<TremorFn<Ctx>> {
        if let Some(functions) = self.functions.get(module) {
            if let Some(rf) = functions.get(function) {
                // TODO: We couldn't return the function wrapper but we can return
                // the function It's not a big issue but it would have been nice to
                // know why.
                Ok(rf.fun)
            } else {
                Err(FunctionError::MissingFunction {
                    m: module.to_string(),
                    f: function.to_string(),
                })
            }
        } else {
            Err(FunctionError::MissingModule {
                m: module.to_string(),
            })
        }
    }

    pub fn insert(&mut self, function: TremorFnWrapper<Ctx>) -> &mut Registry<Ctx> {
        match self.functions.get_mut(&function.module) {
            Some(module) => {
                module.insert(function.name.clone(), function);
            }

            None => {
                let mut module = HashMap::new();
                let module_name = function.module.clone();
                module.insert(function.name.clone(), function);
                self.functions.insert(module_name, module);
            }
        }
        self
    }
}

pub struct TremorAggrFnWrapper {
    pub module: String,
    pub name: String,
    pub fun: Box<dyn TremorAggrFn>,
}

impl Clone for TremorAggrFnWrapper {
    fn clone(&self) -> Self {
        TremorAggrFnWrapper {
            module: self.module.clone(),
            name: self.name.clone(),
            fun: self.fun.snot_clone(),
        }
    }
}

impl TremorAggrFnWrapper {
    pub fn accumulate<'event>(&mut self, args: &[Value<'event>]) -> FResult<()> {
        self.fun.accumulate(args)
    }
    pub fn compensate<'event>(&mut self, args: &[Value<'event>]) -> FResult<()> {
        self.fun.compensate(args)
    }
    pub fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        self.fun.emit()
    }
    pub fn init<'event>(&mut self) {
        self.fun.init()
    }
    pub fn valid_arity<'event>(&self, n: usize) -> bool {
        self.fun.valid_arity(n)
    }
    pub fn arity<'event>(&self) -> Range<usize> {
        self.fun.arity()
    }
}

impl fmt::Debug for TremorAggrFnWrapper {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "(aggr){}::{}", self.module, self.name)
    }
}
impl PartialEq for TremorAggrFnWrapper {
    fn eq(&self, other: &TremorAggrFnWrapper) -> bool {
        self.module == other.module && self.name == other.name
    }
}

#[derive(Debug, Clone)]
pub struct AggrRegistry {
    functions: HashMap<String, HashMap<String, TremorAggrFnWrapper>>,
}

impl Default for AggrRegistry {
    fn default() -> Self {
        AggrRegistry {
            functions: HashMap::new(),
        }
    }
}

impl AggrRegistry {
    pub fn find(&self, module: &str, function: &str) -> FResult<&TremorAggrFnWrapper> {
        if let Some(functions) = self.functions.get(module) {
            if let Some(rf) = functions.get(function) {
                // TODO: We couldn't return the function wrapper but we can return
                // the function It's not a big issue but it would have been nice to
                // know why.
                Ok(rf)
            } else {
                Err(FunctionError::MissingFunction {
                    m: module.to_string(),
                    f: function.to_string(),
                })
            }
        } else {
            Err(FunctionError::MissingModule {
                m: module.to_string(),
            })
        }
    }

    pub fn insert(&mut self, function: TremorAggrFnWrapper) -> &mut AggrRegistry {
        match self.functions.get_mut(&function.module) {
            Some(module) => {
                module.insert(function.name.clone(), function);
            }

            None => {
                let mut module = HashMap::new();
                let module_name = function.module.clone();
                module.insert(function.name.clone(), function);
                self.functions.insert(module_name, module);
            }
        }
        self
    }
}

// Test utility to grab a function from the registry
#[cfg(test)]
pub fn fun<'event>(m: &str, f: &str) -> impl Fn(&[&Value<'event>]) -> FResult<Value<'event>> {
    let f = registry().find(m, f).expect("could not find function");
    move |args: &[&Value]| -> FResult<Value> { f(&(), &args) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use simd_json::BorrowedValue as Value;

    #[test]
    pub fn call_a_function_from_a_registry_works() {
        let max = fun("math", "max");
        let one = Value::from(1);
        let two = Value::from(2);
        assert_eq!(Ok(Value::from(2)), max(&[&one, &two]));
    }

    #[test]
    pub fn bad_arity() {
        let f = tremor_fn! (module::name(_context, _a: I64){
            Ok(format!("{}", _a).into())
        });
        let one = Value::from(1);
        let two = Value::from(2);

        assert!(f.invoke(&(), &[&one, &two]).is_err());
    }

    #[test]
    pub fn bad_type() {
        let f = tremor_fn!(module::name(_context, _a: I64){
            Ok(format!("{}", _a).into())
        });

        let one = Value::from("1");
        assert!(f.invoke(&(), &[&one]).is_err());
    }

    #[test]
    pub fn add() {
        let f = tremor_fn!(math::add(_context, _a, _b){
            match (_a, _b) {
                (Value::F64(a), Value::F64(b)) => Ok(Value::from(a + b)),
                (Value::I64(a), Value::I64(b)) => Ok(Value::from(a + b)),
                _ =>Err(to_runtime_error("could not add numbers"))
            }
        });

        let two = Value::from(2);
        let three = Value::from(3);
        assert_eq!(Ok(Value::from(5)), f.invoke(&(), &[&two, &three]));
    }

    #[test]
    pub fn t3() {
        let f = tremor_fn!(math::add(_context, _a: I64, _b: I64, _c: I64){
            Ok(Value::from(_a + _b + _c))
        });
        let one = Value::from(1);
        let two = Value::from(2);
        let three = Value::from(3);

        assert_eq!(Ok(Value::from(6)), f.invoke(&(), &[&one, &two, &three]));
    }

    #[test]
    pub fn registry_format_with_3_args() {
        let f = fun("math", "max");

        let one = Value::from(1);
        let two = Value::from(2);
        let three = Value::from(3);
        assert!(f(&[&one, &two, &three]).is_err());
    }

    #[test]
    pub fn format() {
        let format = fun("string", "format");
        let empty = Value::from("empty");
        assert_eq!(Ok(Value::from("empty")), format(&[&empty]));
        let format = fun("string", "format");
        let one = Value::from(1);
        let two = Value::from(2);
        let fmt = Value::from("{}{}");
        assert_eq!(Ok(Value::from("12")), format(&[&fmt, &one, &two]));
        let format = fun("string", "format");
        let fmt = Value::from("{} + {}");
        assert_eq!(Ok(Value::from("1 + 2")), format(&[&fmt, &one, &two]));
    }

    #[test]
    pub fn format_literal_curlies() {
        let format = fun("string", "format");
        let s = Value::from("{{}}");
        assert_eq!(Ok(Value::from("{}")), (format(&[&s])));
    }

    #[test]
    pub fn format_literal_curlies_with_other_curlies_in_same_line() {
        let format = fun("string", "format");
        let v1 = Value::from("a string with {} in it has {} {{}}");
        let v2 = Value::from("{}");
        let v3 = Value::from(1);
        assert_eq!(
            Ok(Value::from("a string with {} in it has 1 {}")),
            format(&[&v1, &v2, &v3])
        );
    }

    #[test]
    pub fn format_evil_test() {
        let format = fun("string", "format");
        let v = Value::from("}}");
        assert_eq!(Ok(Value::from("}")), format(&[&v]));
    }

    #[test]
    pub fn format_escaped_foo() {
        let format = fun("string", "format");
        let v = Value::from("{{foo}}");
        assert_eq!(Ok(Value::from("{foo}")), format(&[&v]));
    }

    #[test]
    pub fn format_three_parenthesis() {
        let format = fun("string", "format");
        let v1 = Value::from("{{{}}}");
        let v2 = Value::from("foo");
        assert_eq!(Ok(Value::from("{foo}")), format(&[&v1, &v2]));
    }

    #[test]
    pub fn umatched_parenthesis() {
        let format = fun("string", "format");
        let v = Value::from("{");
        let res = format(&[&v]);

        assert!(res.is_err());
    }

    #[test]
    pub fn unmatched_3_parenthesis() {
        let format = fun("string", "format");
        let v = Value::from("{{{");
        let res = format(&[&v]);

        assert!(res.is_err());
    }

    #[test]
    pub fn unmatched_closed_parenthesis() {
        let format = fun("string", "format");
        let v = Value::from("}");
        let res = format(&[&v]);

        assert!(res.is_err());
    }

    #[test]
    pub fn unmatched_closed_3_parenthesis() {
        let format = fun("string", "format");
        let v = Value::from("}}}");
        let res = format(&[&v]);

        assert!(res.is_err());
    }

    #[test]
    pub fn unmatched_parenthesis_with_string() {
        let format = fun("string", "format");
        let v = Value::from("{foo}");
        let res = format(&[&v]);

        assert!(res.is_err());
    }

    #[test]
    pub fn unmatched_parenthesis_with_argument() {
        let format = fun("string", "format");
        let v1 = Value::from("{foo}");
        let v2 = Value::from("1");
        let res = format(&[&v1, &v2]);
        assert!(res.is_err());
    }

    #[test]
    pub fn unmatched_parenthesis_too_few() {
        let format = fun("string", "format");
        let v = Value::from("{}");
        let res = format(&[&v]);

        assert!(res.is_err());
    }

    #[test]
    pub fn unmatched_parenthesis_too_many() {
        let format = fun("string", "format");
        let v1 = Value::from("{}");
        let v2 = Value::from("1");
        let v3 = Value::from("2");
        let res = format(&[&v1, &v2, &v3]);
        assert!(res.is_err());
    }

    #[test]
    pub fn bad_format() {
        let format = fun("string", "format");
        let v = Value::from(7);
        let res = format(&[&v]);

        assert!(res.is_err());
    }

}
