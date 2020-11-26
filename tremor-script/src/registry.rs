// Copyright 2020, The Tremor Team
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

mod custom_fn;
pub use self::custom_fn::CustomFn;
pub(crate) use self::custom_fn::{RECUR, RECUR_PTR};
use crate::ast::{BaseExpr, NodeMetas};
use crate::errors::{best_hint, Error, ErrorKind, Result};
use crate::utils::hostname as get_hostname;
use crate::{tremor_fn, EventContext};
use downcast_rs::{impl_downcast, DowncastSync};
use halfbrown::HashMap;
use simd_json::BorrowedValue as Value;
use std::default::Default;
use std::fmt;
use std::ops::RangeInclusive;

/// An aggregate function
pub trait TremorAggrFn: DowncastSync + Sync + Send {
    /// Accumulate a value
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()>;
    /// Compensate for a value being removed
    fn compensate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()>;
    /// Emits the function
    fn emit<'event>(&mut self) -> FResult<Value<'event>>;
    /// Initialises an aggregate function
    fn init(&mut self);
    /// Emits the value and reinitialises the function
    /// this can be used to optimise this process
    fn emit_and_init<'event>(&mut self) -> FResult<Value<'event>> {
        let r = self.emit()?;
        self.init();
        Ok(r)
    }
    /// Merges the state of a differently windowed function into this
    /// this requires `&self` and `&src` to be of the same type.
    fn merge(&mut self, src: &dyn TremorAggrFn) -> FResult<()>;
    /// allows cloning the functions without implementing
    /// `Clone` to avoid rust complaining
    fn boxed_clone(&self) -> Box<dyn TremorAggrFn>;
    /// The arity of the function
    fn arity(&self) -> RangeInclusive<usize>;
    /// Tests if a given arity is valid
    fn valid_arity(&self, n: usize) -> bool {
        self.arity().contains(&n)
    }
    /// A possible warnings the use of this function should cause
    fn warning(&self) -> Option<String> {
        None
    }
}
impl_downcast!(sync TremorAggrFn);

/// A tremor function
pub trait TremorFn: Sync + Send {
    /// Invoce the function and calculate the result
    fn invoke<'event>(&self, ctx: &EventContext, args: &[&Value<'event>])
        -> FResult<Value<'event>>;
    /// allows cloning the functions without implementing
    /// `Clone` to avoid rust complaining
    fn boxed_clone(&self) -> Box<dyn TremorFn>;
    /// The arity of the function
    fn arity(&self) -> RangeInclusive<usize>;
    /// Tests if a given arity is valid
    fn valid_arity(&self, n: usize) -> bool {
        self.arity().contains(&n)
    }
    /// returns if this function is `const`. const functions
    /// that get passed all constant arguments can be evaluated
    /// at compile time.
    fn is_const(&self) -> bool {
        false
    }
}
/// The result of a function
pub type FResult<T> = std::result::Result<T, FunctionError>;

/// Creates a new function registry and inserts some placeholder
/// functions.
#[must_use]
pub fn registry() -> Registry {
    let mut registry = Registry::default();

    registry
        .insert(tremor_fn!(system::hostname(_context) {
            Ok(Value::from( get_hostname()))
        }))
        .insert(tremor_fn! (system::ingest_ns(ctx) {
            Ok(Value::from(ctx.ingest_ns()))
        }))
        .insert(tremor_fn!(system::instance(_context) {
            Ok(Value::from("tremor-script"))
        }));

    crate::std_lib::load(&mut registry);
    registry
}

/// Creates a new aggregate function registry
#[must_use]
pub fn aggr() -> Aggr {
    let mut registry = Aggr::default();
    crate::std_lib::load_aggr(&mut registry);
    registry
}

/// Module Function Arity triple
#[derive(Clone, Debug, PartialEq)]
pub struct MFA {
    m: String,
    f: String,
    a: usize,
}

impl MFA {
    /// creates a MFA
    #[must_use]
    pub fn new(m: &str, f: &str, a: usize) -> Self {
        MFA {
            m: m.to_string(),
            f: f.to_string(),
            a,
        }
    }
}

/// creates a MFA
#[must_use]
pub fn mfa(m: &str, f: &str, a: usize) -> MFA {
    MFA::new(m, f, a)
}

/// Turns an error and a MFA into a function error
pub fn to_runtime_error<E: core::fmt::Display>(mfa: MFA, e: E) -> FunctionError {
    FunctionError::RuntimeError {
        mfa,
        error: format!("{}", e),
    }
}

/// Function error
#[derive(Debug)]
pub enum FunctionError {
    /// The function was called with a bad arity
    BadArity {
        /// The function was called with a bad arity
        mfa: MFA,
        /// The function was called with a bad arity
        calling_a: usize,
    },
    /// The function encountered a runtime error
    RuntimeError {
        /// The function was called with a bad arity
        mfa: MFA,
        /// The function was called with a bad arity
        error: String,
    },
    /// The module doesn't exit
    MissingModule {
        /// The function was called with a bad arity
        m: String,
    },
    /// The function doesn't exist
    MissingFunction {
        /// The function was called with a bad arity
        m: String,
        /// The function was called with a bad arity
        f: String,
    },
    /// A bad type was passed
    BadType {
        /// The function was called with a bad arity
        mfa: MFA,
    },
    /// A generic error
    Error(Box<Error>),
}

#[cfg(not(tarpaulin_include))]
impl PartialEq for FunctionError {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

#[cfg(not(tarpaulin_include))]
impl From<Error> for FunctionError {
    fn from(error: Error) -> Self {
        Self::Error(Box::new(error))
    }
}

impl FunctionError {
    /// Turns a function error into a tremor script error
    pub fn into_err<O: BaseExpr, I: BaseExpr>(
        self,
        outer: &O,
        inner: &I,
        registry: Option<&Registry>,
        meta: &NodeMetas,
    ) -> crate::errors::Error {
        use FunctionError::{
            BadArity, BadType, Error, MissingFunction, MissingModule, RuntimeError,
        };
        let outer = outer.extent(meta);
        let inner = inner.extent(meta);
        match self {
            BadArity { mfa, calling_a } => {
                ErrorKind::BadArity(outer, inner, mfa.m, mfa.f, mfa.a..=mfa.a, calling_a).into()
            }
            RuntimeError { mfa, error } => {
                ErrorKind::RuntimeError(outer, inner, mfa.m, mfa.f, mfa.a, error).into()
            }
            MissingModule { m } => {
                let suggestion = registry.and_then(|registry| {
                    let modules: Vec<String> = registry.functions.keys().cloned().collect();
                    best_hint(&m, &modules, 2)
                });
                ErrorKind::MissingModule(outer, inner, m, suggestion).into()
            }
            MissingFunction { m, f } => {
                let suggestion = registry.and_then(|registry| {
                    registry.functions.get(&m).and_then(|module| {
                        let functions: Vec<String> = module.keys().cloned().collect();
                        best_hint(&m, &functions, 2)
                    })
                });
                ErrorKind::MissingFunction(outer, inner, vec![m], f, suggestion).into()
            }
            BadType { mfa } => ErrorKind::BadType(outer, inner, mfa.m, mfa.f, mfa.a).into(),
            Error(e) => *e,
        }
    }
}

/// Wrapper around a function
pub struct TremorFnWrapper {
    /// Name of the module the function is in
    module: String,
    /// Name of the function
    name: String,
    /// Boxed dyn of the implementaiton
    fun: Box<dyn TremorFn>,
}

impl TremorFnWrapper {
    /// Creates a new wrapper
    #[must_use]
    pub fn new(module: String, name: String, fun: Box<dyn TremorFn>) -> Self {
        Self { module, name, fun }
    }
    /// Invokes the function
    pub fn invoke<'event>(
        &self,
        context: &EventContext,
        args: &[&Value<'event>],
    ) -> FResult<Value<'event>> {
        self.fun.invoke(context, args)
    }

    /// Check if a given arity is valit for the function
    #[must_use]
    pub fn valid_arity(&self, n: usize) -> bool {
        self.fun.valid_arity(n)
    }
    /// Returns the functions arity
    #[must_use]
    pub fn arity(&self) -> RangeInclusive<usize> {
        self.fun.arity()
    }

    /// Returns if the function is considered const
    #[must_use]
    pub fn is_const(&self) -> bool {
        self.fun.is_const()
    }
}

impl Clone for TremorFnWrapper {
    fn clone(&self) -> Self {
        Self {
            module: self.module.clone(),
            name: self.name.clone(),
            fun: self.fun.boxed_clone(),
        }
    }
}
#[cfg(not(tarpaulin_include))]
impl fmt::Debug for TremorFnWrapper {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}::{}", self.module, self.name)
    }
}
#[cfg(not(tarpaulin_include))]
impl PartialEq for TremorFnWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.module == other.module && self.name == other.name
    }
}

/// Tremor Function registry
#[derive(Debug, Clone)]
pub struct Registry {
    functions: HashMap<String, HashMap<String, TremorFnWrapper>>,
    modules: Vec<String>,
}

#[doc(hidden)]
/// Creates a Tremor function from a given implementation
#[macro_export]
macro_rules! tremor_fn {
    ($module:ident :: $name:ident($context:ident, $($arg:ident),*) $code:block) => {
        {
            use $crate::tremor_fn_;
            tremor_fn_!($module :: $name(false, $context, $($arg),*) $code)
        }
    };
    ($module:ident :: $name:ident($context:ident, $($arg:ident : $type:ident),*) $code:block) => {
        {
            use $crate::tremor_fn_;
            tremor_fn_!($module :: $name(false, $context, $($arg : $type),*) $code)
        }
    };
    ($module:ident :: $name:ident($context:ident) $code:block) => {
        {
            use $crate::tremor_fn_;
            tremor_fn_!($module::$name(false, $context) $code)
        }
    };
}

#[doc(hidden)]
/// Creates a constant Tremor function from a given implementation
#[macro_export]
macro_rules! tremor_const_fn {
    ($module:ident :: $name:ident($context:ident, $($arg:ident),*) $code:block) => {
        {
            use $crate::tremor_fn_;
            tremor_fn_!($module :: $name(true, $context, $($arg),*) $code)
        }
    };
    ($module:ident :: $name:ident($context:ident, $($arg:ident : $type:ident),*) $code:block) => {
        {
            use $crate::tremor_fn_;
            tremor_fn_!($module :: $name(true, $context, $($arg : $type),*) $code)
        }
    };
    ($module:ident :: $name:ident($context:ident) $code:block) => {
        {
            use $crate::tremor_fn_;
            tremor_fn_!($module::$name(true, $context) $code)
        }
    };
}

#[doc(hidden)]
/// Internal tremor function creation macro - DO NOT USE
#[macro_export]
macro_rules! tremor_fn_ {
    ($module:ident :: $name:ident($const:expr, $context:ident, $($arg:ident),*) $code:block) => {
        {
            macro_rules! replace_expr {
                ($_t:tt $sub:expr) => {
                    $sub
                };
            }
            use simd_json::BorrowedValue as Value;
            use $crate::EventContext;
            use $crate::registry::{TremorFnWrapper, TremorFn};
            #[allow(unused_imports)] // We might not use all of this imports
            use $crate::registry::{FResult, FunctionError, mfa, MFA, to_runtime_error as to_runtime_error_ext};
            const ARGC: usize = {0_usize $(+ replace_expr!($arg 1_usize))*};
            mod $name {
                #[derive(Clone, Debug, Default)]
                pub(crate) struct Func {}
            };
            impl TremorFn for $name::Func {
                #[allow(unused_variables)]
                fn invoke<'event, 'c>(
                    & self,
                    $context: &'c EventContext,
                    args: &[&Value<'event>],
                ) -> FResult<Value<'event>> {
                    fn this_mfa() -> MFA {
                        mfa(stringify!($module), stringify!($name), ARGC)
                    }
                    #[allow(dead_code)] // We need this as it is a macro and might not be used
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

                fn boxed_clone(&self) -> Box<dyn TremorFn> {
                    Box::new(self.clone())
                }
                fn arity(&self) -> std::ops::RangeInclusive<usize> {
                    ARGC..=ARGC
                }
                fn is_const(&self) -> bool {
                    $const
                }
            }

            TremorFnWrapper::new(
                stringify!($module).to_string(),
                stringify!($name).to_string(),
                 Box::new($name::Func{})
            )
        }
    };

    ($module:ident :: $name:ident($const:expr, $context:ident, $($arg:ident : $type:ident),*) $code:block) => {
        {
            macro_rules! replace_expr {
                ($_t:tt $sub:expr) => {
                    $sub
                };
            }
            use simd_json::BorrowedValue as Value;
            use $crate::EventContext;
            use $crate::registry::{TremorFnWrapper, TremorFn};
            #[allow(unused_imports)] // We might not use all of this imports
            use $crate::registry::{FResult, FunctionError, mfa, MFA, to_runtime_error as to_runtime_error_ext};
            const ARGC: usize = {0_usize $(+ replace_expr!($arg 1_usize))*};
            mod $name {
                #[derive(Clone, Debug, Default)]
                pub(crate) struct Func {}
            };
            impl TremorFn for $name::Func {
                #[allow(unused_variables)]
                fn invoke<'event, 'c>(
                    & self,
                    $context: &'c EventContext,
                    args: &[&Value<'event>],
                ) -> FResult<Value<'event>> {
                    fn this_mfa() -> MFA {
                        mfa(stringify!($module), stringify!($name), ARGC)
                    }
                    #[allow(dead_code)] // We need this as it is a macro and might not be used
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
                fn boxed_clone(&self) -> Box<dyn TremorFn> {
                    Box::new(self.clone())
                }
                fn arity(&self) -> std::ops::RangeInclusive<usize> {
                    ARGC..=ARGC
                }
                fn is_const(&self) -> bool {
                    $const
                }
            }

            TremorFnWrapper::new(
                stringify!($module).to_string(),
                stringify!($name).to_string(),
                Box::new($name::Func{})
            )
        }
    };

    ($module:ident :: $name:ident($const:expr, $context:ident) $code:block) => {
        {
            use simd_json::BorrowedValue as Value;
            use $crate::EventContext;
            use $crate::registry::{TremorFnWrapper, TremorFn};
            #[allow(unused_imports)] // We might not use all of this imports
            use $crate::registry::{FResult, FunctionError, mfa, MFA, to_runtime_error as to_runtime_error_ext};
            const ARGC: usize = 0;
            mod $name {
                #[derive(Clone, Debug, Default)]
                pub(crate) struct Func {}
            };
            impl TremorFn for $name::Func {
                #[allow(unused_variables)]
                fn invoke<'event, 'c>(
                    &self,
                    $context: &'c EventContext,
                    args: &[&Value<'event>],
                ) -> FResult<Value<'event>> {

                    fn this_mfa() -> MFA {
                        mfa(stringify!($module), stringify!($name), ARGC)
                    }
                    #[allow(dead_code)] // We need this as it is a macro and might not be used
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
                fn boxed_clone(&self) -> Box<dyn TremorFn> {
                    Box::new(self.clone())
                }
                fn arity(&self) -> std::ops::RangeInclusive<usize> {
                    ARGC..=ARGC
                }
                fn is_const(&self) -> bool {
                    $const
                }
            }

            TremorFnWrapper::new(
                stringify!($module).to_string(),
                stringify!($name).to_string(),
                Box::new($name::Func{})
            )
        }
    };
}

#[cfg(not(tarpaulin_include))]
impl Default for Registry {
    fn default() -> Self {
        Self {
            functions: HashMap::new(),
            modules: Vec::new(),
        }
    }
}

impl Registry {
    /// finds a function in the registry
    pub fn find(&self, module: &str, function: &str) -> FResult<&TremorFnWrapper> {
        self.functions
            .get(module)
            .ok_or_else(|| FunctionError::MissingModule {
                m: module.to_string(),
            })
            .and_then(|functions| {
                // TODO: We couldn't return the function wrapper but we can return
                // the function It's not a big issue but it would have been nice to
                // know why.
                functions
                    .get(function)
                    .ok_or_else(|| FunctionError::MissingFunction {
                        m: module.to_string(),
                        f: function.to_string(),
                    })
            })
    }

    /// Inserts a function into the registry, overwriting it if it already exists
    pub fn insert(&mut self, function: TremorFnWrapper) -> &mut Self {
        if let Some(module) = self.functions.get_mut(&function.module) {
            module.insert(function.name.clone(), function);
        } else {
            let mut module = HashMap::new();
            let module_name = function.module.clone();
            module.insert(function.name.clone(), function);
            self.functions.insert(module_name, module);
        }
        self
    }

    /// Finds a module in the registry
    #[must_use]
    pub fn find_module(&self, module: &str) -> Option<&HashMap<String, TremorFnWrapper>> {
        self.functions.get(module)
    }
}

/// Wrapper around an aggregate function
pub struct TremorAggrFnWrapper {
    module: String,
    name: String,
    fun: Box<dyn TremorAggrFn>,
}

impl Clone for TremorAggrFnWrapper {
    fn clone(&self) -> Self {
        Self {
            module: self.module.clone(),
            name: self.name.clone(),
            fun: self.fun.boxed_clone(),
        }
    }
}

impl TremorAggrFnWrapper {
    /// Creates a new wrapper
    #[must_use]
    pub fn new(module: String, name: String, fun: Box<dyn TremorAggrFn>) -> Self {
        Self { module, name, fun }
    }

    /// Accumulate a value
    pub fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        self.fun.accumulate(args)
    }

    /// Compensate for a value being removed
    pub fn compensate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        self.fun.compensate(args)
    }

    /// Emits the function
    pub fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        self.fun.emit()
    }

    /// Initialises an aggregate function
    pub fn init(&mut self) {
        self.fun.init()
    }

    /// Tests if a given arity is valid
    #[must_use]
    pub fn valid_arity(&self, n: usize) -> bool {
        self.fun.valid_arity(n)
    }

    /// The arity of the function
    #[must_use]
    pub fn arity(&self) -> RangeInclusive<usize> {
        self.fun.arity()
    }

    /// A possible warnings the use of this function should cause
    #[must_use]
    pub fn warning(&self) -> Option<String> {
        self.fun.warning()
    }

    /// Merges the state of a differently windowed function into this
    /// this requires `&self` and `&src` to be of the same type.
    pub fn merge(&mut self, src: &Self) -> FResult<()> {
        use std::borrow::Borrow;
        self.fun.merge(src.fun.borrow())
    }
}

#[cfg(not(tarpaulin_include))]
impl fmt::Debug for TremorAggrFnWrapper {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "(aggr){}::{}", self.module, self.name)
    }
}

#[cfg(not(tarpaulin_include))]
impl PartialEq for TremorAggrFnWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.module == other.module && self.name == other.name
    }
}

/// Aggregate registry
#[derive(Debug, Clone)]
pub struct Aggr {
    functions: HashMap<String, HashMap<String, TremorAggrFnWrapper>>,
}

#[cfg(not(tarpaulin_include))]
impl Default for Aggr {
    fn default() -> Self {
        Self {
            functions: HashMap::new(),
        }
    }
}

impl Aggr {
    /// finds a function in the registry
    pub fn find(&self, module: &str, function: &str) -> FResult<&TremorAggrFnWrapper> {
        self.functions
            .get(module)
            .ok_or_else(|| FunctionError::MissingModule {
                m: module.to_string(),
            })
            .and_then(|functions| {
                // TODO: We couldn't return the function wrapper but we can return
                // the function It's not a big issue but it would have been nice to
                // know why.
                functions
                    .get(function)
                    .ok_or_else(|| FunctionError::MissingFunction {
                        m: module.to_string(),
                        f: function.to_string(),
                    })
            })
    }

    /// Inserts a function into the registry, overwriting it if it already exists
    pub fn insert(&mut self, function: TremorAggrFnWrapper) -> &mut Self {
        if let Some(module) = self.functions.get_mut(&function.module) {
            module.insert(function.name.clone(), function);
        } else {
            let mut module = HashMap::new();
            let module_name = function.module.clone();
            module.insert(function.name.clone(), function);
            self.functions.insert(module_name, module);
        }
        self
    }

    /// Finds a module in the registry
    #[must_use]
    pub fn find_module(&self, module: &str) -> Option<&HashMap<String, TremorAggrFnWrapper>> {
        self.functions.get(module)
    }
}

#[cfg(test)]
pub use tests::fun;
#[cfg(test)]
mod tests {
    use super::*;
    use simd_json::{BorrowedValue as Value, Value as ValueTrait};

    // Test utility to grab a function from the registry
    pub fn fun<'event>(m: &str, f: &str) -> impl Fn(&[&Value<'event>]) -> FResult<Value<'event>> {
        let f = registry()
            .find(m, f)
            .expect("could not find function")
            .clone();
        move |args: &[&Value]| -> FResult<Value> { f.invoke(&EventContext::new(0, None), &args) }
    }
    #[test]
    pub fn call_a_function_from_a_registry_works() {
        let max = fun("math", "max");
        let one = Value::from(1);
        let two = Value::from(2);
        assert_eq!(Ok(Value::from(2)), max(&[&one, &two]));
    }

    #[test]
    pub fn bad_arity() {
        let f = tremor_fn! (module::name(_context, _a){
            Ok(format!("{}", _a).into())
        });
        let one = Value::from(1);
        let two = Value::from(2);

        assert!(f
            .invoke(&EventContext::new(0, None), &[&one, &two])
            .is_err());
    }

    #[test]
    pub fn bad_type() {
        let f = tremor_fn!(module::name(_context, _a: String){
            Ok(format!("{}", _a).into())
        });

        let one = Value::from(1);
        assert!(f.invoke(&EventContext::new(0, None), &[&one]).is_err());
    }

    #[test]
    pub fn add() {
        let f = tremor_fn!(math::add(_context, _a, _b){
            if let (Some(a), Some(b)) = (_a.as_i64(), _b.as_i64()) {
                Ok(Value::from(a + b))
            } else if let (Some(a), Some(b)) = (_a.as_f64(), _b.as_f64()) {
                    Ok(Value::from(a + b))
                } else {
                Err(FunctionError::BadType{
                    mfa: this_mfa(),
                })
            }
        });

        let two = Value::from(2);
        let three = Value::from(3);
        assert_eq!(
            Ok(Value::from(5)),
            f.invoke(&EventContext::new(0, None), &[&two, &three])
        );
    }

    #[test]
    pub fn t3() {
        let f = tremor_fn!(math::add(_context, _a, _b, _c){
            if let (Some(a), Some(b), Some(c)) = (_a.as_i64(), _b.as_i64(), _c.as_i64()) {
                Ok(Value::from(a + b + c))
            } else {
                Err(FunctionError::BadType{
                    mfa: this_mfa(),
                })
            }
        });
        let one = Value::from(1);
        let two = Value::from(2);
        let three = Value::from(3);

        assert_eq!(
            Ok(Value::from(6)),
            f.invoke(&EventContext::new(0, None), &[&one, &two, &three])
        );
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
    pub fn unmatched_parenthesis() {
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
