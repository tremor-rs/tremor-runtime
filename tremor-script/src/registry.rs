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

use crate::errors::*;
use crate::tremor_fn;
use halfbrown::HashMap;
use hostname::get_hostname;
use simd_json::BorrowedValue;
use simd_json::OwnedValue;
use std::default::Default;

pub type TremorFn<Ctx> = fn(&Ctx, &[&BorrowedValue]) -> Result<OwnedValue>;

use std::fmt;

#[allow(unused_variables)]
pub fn registry<Ctx: 'static + Context>() -> Registry<Ctx> {
    let mut registry = Registry::default();

    registry
        .insert(tremor_fn! (math::max(_context, a, b) {
            match (a, b) {
                (BorrowedValue::F64(a), BorrowedValue::F64(b)) if a > b  => Ok(OwnedValue::from(a.to_owned())),
                (BorrowedValue::F64(a), BorrowedValue::F64(b)) => Ok(OwnedValue::from(b.to_owned())),
                (BorrowedValue::I64(a), BorrowedValue::I64(b)) if a > b  => Ok(OwnedValue::from(a.to_owned())),
                (BorrowedValue::I64(a), BorrowedValue::I64(b)) => Ok(OwnedValue::from(b.to_owned())),
                (BorrowedValue::F64(a), BorrowedValue::I64(b)) if *a > *b as f64 => Ok(OwnedValue::from(a.to_owned())),
                (BorrowedValue::F64(a), BorrowedValue::I64(b)) => Ok(OwnedValue::from(b.to_owned())),
                (BorrowedValue::I64(a), BorrowedValue::F64(b)) if (*a as f64) > *b => Ok(OwnedValue::from(a.to_owned())),
                (BorrowedValue::I64(a), BorrowedValue::F64(b)) => Ok(OwnedValue::from(b.to_owned())),
                _ => Err(ErrorKind::BadType("math".to_string(), "max".to_string(), 2).into()),
            }
        }))
        .insert(tremor_fn!(math::min(_context, a, b) {
            match (a, b) {
                (BorrowedValue::F64(a), BorrowedValue::F64(b)) if a < b  => Ok(OwnedValue::from(a.to_owned())),
                (BorrowedValue::F64(a), BorrowedValue::F64(b)) => Ok(OwnedValue::from(b.to_owned())),
                (BorrowedValue::I64(a), BorrowedValue::I64(b)) if a < b  => Ok(OwnedValue::from(a.to_owned())),
                (BorrowedValue::I64(a), BorrowedValue::I64(b)) => Ok(OwnedValue::from(b.to_owned())),
                (BorrowedValue::F64(a), BorrowedValue::I64(b)) if *a < *b as f64 => Ok(OwnedValue::from(a.to_owned())),
                (BorrowedValue::F64(a), BorrowedValue::I64(b)) => Ok(OwnedValue::from(b.to_owned())),
                (BorrowedValue::I64(a), BorrowedValue::F64(b)) if (*a as f64) < *b => Ok(OwnedValue::from(a.to_owned())),
                (BorrowedValue::I64(a), BorrowedValue::F64(b)) => Ok(OwnedValue::from(b.to_owned())),
                _ => Err(ErrorKind::BadType("math".to_string(), "min".to_string(), 2).into()),
            }
        }))
        .insert(tremor_fn!(system::hostname(_context) {
            if let Some(hostname) = get_hostname(){
                Ok(Value::from(hostname))
            } else {
                Err(ErrorKind::RuntimeError("system".to_owned(), "hostname".to_owned(), 0, "could not get hostname".into()).into())
            }

        }));
    crate::std_lib::load(&mut registry);
    registry
}

pub trait Context {}

impl Context for () {}

#[derive(Clone)]
pub struct TremorFnWrapper<Ctx: Context> {
    pub module: String,
    pub name: String,
    pub fun: TremorFn<Ctx>,
}

#[derive(Debug, Clone)]
pub struct Registry<Ctx: Context + 'static> {
    functions: HashMap<String, HashMap<String, TremorFnWrapper<Ctx>>>,
}

impl<Ctx: Context + 'static> TremorFnWrapper<Ctx> {
    pub fn invoke(&self, context: &Ctx, args: &[&BorrowedValue]) -> Result<OwnedValue> {
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

#[macro_export]
macro_rules! tremor_fn {

    ($module:ident :: $name:ident($context:ident, $($arg:ident),*) $code:block) => {
        {
            use simd_json::OwnedValue as Value;
            use simd_json::BorrowedValue;
            use $crate::TremorFnWrapper;
            fn $name<'c, Ctx: $crate::Context + 'static>($context: &'c Ctx, args: &[&BorrowedValue]) -> Result<Value>{
                // rust claims that the pattern is unreachable even
                // though it isn't, so linting it
                match args {
                    [$(
                        $arg,
                    )*] => {$code}
                    _ => Err($crate::errors::Error::from($crate::errors::ErrorKind::BadArrity(stringify!($module).to_string(), stringify!($name).to_string(), args.len())))
                }
            }
            TremorFnWrapper {
                module: stringify!($module).to_string(),
                name: stringify!($name).to_string(),
                fun: $name
            }
        }
    };

    ($module:ident :: $name:ident($context:ident, $($arg:ident : $type:ident),*) $code:block) => {
        {
            use simd_json::OwnedValue as Value;
            use simd_json::BorrowedValue;
            use $crate::TremorFnWrapper;
            fn $name<'c, Ctx: $crate::Context + 'static>($context: &'c Ctx, args: &[&BorrowedValue]) -> Result<Value>{
                // rust claims that the pattern is unreachable even
                // though it isn't, so linting it
                match args {
                    [$(
                        BorrowedValue::$type($arg),
                    )*] => {$code}
                    [$(
                        $arg,
                    )*] => {
                        Err($crate::errors::Error::from($crate::errors::ErrorKind::BadType(stringify!($module).to_string(), stringify!($name).to_string(), args.len())))
                    },
                    _ => Err($crate::errors::Error::from($crate::errors::ErrorKind::BadArrity(stringify!($module).to_string(), stringify!($name).to_string(), args.len())))
                }
            }
            TremorFnWrapper {
                module: stringify!($module).to_string(),
                name: stringify!($name).to_string(),
                fun: $name
            }
        }
    };

    ($module:ident :: $name:ident($context:ident) $code:block) => {
        {
            use simd_json::OwnedValue as Value;
            use simd_json::BorrowedValue;
            use $crate::TremorFnWrapper;
            fn $name<'c, Ctx: $crate::Context + 'static>($context: &'c Ctx, args: &[&BorrowedValue]) -> Result<Value>{                // rust claims that the pattern is unreachable even
                // though it isn't, so linting it
                match args {
                    [] => {$code}
                    _ => Err($crate::errors::Error::from($crate::errors::ErrorKind::BadArrity(stringify!($module).to_string(), stringify!($name).to_string(), args.len())))
                }
            }
            TremorFnWrapper {
                module: stringify!($module).to_string(),
                name: stringify!($name).to_string(),
                fun: $name
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
    pub fn find(&self, module: &str, function: &str) -> Result<TremorFn<Ctx>> {
        if let Some(functions) = self.functions.get(module) {
            if let Some(rf) = functions.get(function) {
                // TODO: We couldn't return the function wrapper but we can return
                // the function It's not a big issue but it would have been nice to
                // know why.
                Ok(rf.fun)
            } else {
                Err(ErrorKind::MissingFunction(module.to_string(), function.to_string()).into())
            }
        } else {
            Err(ErrorKind::MissingModule(module.to_string()).into())
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

// Test utility to grab a function from the registry
#[cfg(test)]
pub fn fun(m: &str, f: &str) -> impl Fn(&[&BorrowedValue]) -> Result<OwnedValue> {
    let f = registry().find(m, f).expect("could not find function");
    move |args: &[&BorrowedValue]| -> Result<OwnedValue> { f(&(), &args) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use simd_json::BorrowedValue as Value;

    #[test]
    pub fn call_a_function_from_a_registry_works() {
        let max = fun("math", "max");
        let one = BorrowedValue::from(1);
        let two = BorrowedValue::from(2);
        assert_eq!(Ok(OwnedValue::from(2)), max(&[&one, &two]));
    }

    #[test]
    pub fn bad_arrity() {
        let f = tremor_fn! (module::name(_context, _a: I64){
            Ok(format!("{}", _a).into())
        });
        let one = BorrowedValue::from(1);
        let two = BorrowedValue::from(2);

        assert!(f.invoke(&(), &[&one, &two]).is_err());
    }

    #[test]
    pub fn bad_type() {
        let f = tremor_fn!(module::name(_context, _a: I64){
            Ok(format!("{}", _a).into())
        });

        let one = BorrowedValue::from("1");
        assert!(f.invoke(&(), &[&one]).is_err());
    }

    #[test]
    pub fn add() {
        let f = tremor_fn!(math::add(_context, _a, _b){
            match (_a, _b) {
                (BorrowedValue::F64(a), BorrowedValue::F64(b)) => Ok(OwnedValue::from(a + b)),
                (BorrowedValue::I64(a), BorrowedValue::I64(b)) => Ok(OwnedValue::from(a + b)),
                _ => Err(ErrorKind::RuntimeError(
                    "math".to_string(),
                    "add".to_string(),
                    2,
                    "could not add numbers".into(),
                ).into())

            }

        });

        let two = BorrowedValue::from(2);
        let three = BorrowedValue::from(3);
        assert_eq!(Ok(OwnedValue::from(5)), f.invoke(&(), &[&two, &three]));
    }

    #[test]
    pub fn t3() {
        let f = tremor_fn!(math::add(_context, _a: I64, _b: I64, _c: I64){
            Ok(OwnedValue::from(_a + _b + _c))
        });
        let one = BorrowedValue::from(1);
        let two = BorrowedValue::from(2);
        let three = BorrowedValue::from(3);

        assert_eq!(
            Ok(OwnedValue::from(6)),
            f.invoke(&(), &[&one, &two, &three])
        );
    }

    #[test]
    pub fn registry_format_with_3_args() {
        let f = fun("math", "max");

        let one = BorrowedValue::from(1);
        let two = BorrowedValue::from(2);
        let three = BorrowedValue::from(3);
        assert!(f(&[&one, &two, &three]).is_err());
    }

    #[test]
    pub fn format() {
        let format = fun("string", "format");
        let empty = Value::from("empty");
        assert_eq!(Ok(OwnedValue::from("empty")), format(&[&empty]));
        let format = fun("string", "format");
        let one = BorrowedValue::from(1);
        let two = BorrowedValue::from(2);
        let fmt = BorrowedValue::from("{}{}");
        assert_eq!(Ok(OwnedValue::from("12")), format(&[&fmt, &one, &two]));
        let format = fun("string", "format");
        let fmt = BorrowedValue::from("{} + {}");
        assert_eq!(Ok(OwnedValue::from("1 + 2")), format(&[&fmt, &one, &two]));
    }

    #[test]
    pub fn format_literal_curlies() {
        let format = fun("string", "format");
        let s = Value::from("{{}}");
        assert_eq!(Ok(OwnedValue::from("{}")), (format(&[&s])));
    }

    #[test]
    pub fn format_literal_curlies_with_other_curlies_in_same_line() {
        let format = fun("string", "format");
        let v1 = Value::from("a string with {} in it has {} {{}}");
        let v2 = Value::from("{}");
        let v3 = Value::from(1);
        assert_eq!(
            Ok(OwnedValue::from("a string with {} in it has 1 {}")),
            format(&[&v1, &v2, &v3])
        );
    }

    #[test]
    pub fn format_evil_test() {
        let format = fun("string", "format");
        let v = Value::from("}}");
        assert_eq!(Ok(OwnedValue::from("}")), format(&[&v]));
    }

    #[test]
    pub fn format_escaped_foo() {
        let format = fun("string", "format");
        let v = Value::from("{{foo}}");
        assert_eq!(Ok(OwnedValue::from("{foo}")), format(&[&v]));
    }

    #[test]
    pub fn format_three_parenthesis() {
        let format = fun("string", "format");
        let v1 = Value::from("{{{}}}");
        let v2 = Value::from("foo");
        assert_eq!(Ok(OwnedValue::from("{foo}")), format(&[&v1, &v2]));
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
