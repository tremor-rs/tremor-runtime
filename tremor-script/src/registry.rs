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

use crate::errors::*;
use crate::tremor_fn;
use hashbrown::HashMap;
use hostname::get_hostname;
use std::default::Default;

pub type TremorFn<Ctx> = fn(&Ctx, &[Value]) -> Result<Value>;

use serde_json::Value;
use std::fmt;

#[allow(unused_variables)]
pub fn registry<Ctx: 'static + Context>() -> Registry<Ctx> {
    let mut registry = Registry::default();

    #[allow(unused_variables)]
    fn format<Ctx: Context + 'static>(context: &Ctx, args: &[Value]) -> Result<Value> {
        match args.len() {
            0 => Err(ErrorKind::RuntimeError(
                "string".to_string(),
                "format".to_string(),
                args.len(),
                "format requires at least 1 parameter, 0 are supplied".into(),
            )
            .into()),
            _ => {
                match &args[0] {
                    Value::String(format) => {
                        let mut arg_stack = if args.is_empty() {
                            vec![]
                        } else {
                            args[1..].to_vec()
                        };
                        arg_stack.reverse();

                        let mut out = String::new();
                        let mut iter = format.chars().enumerate();
                        while let Some(char) = iter.next() {
                            match char {
                        (pos, '{')  => match iter.next() {
                            Some((_, '}')) => {
                                let arg = match arg_stack.pop() {
                                    Some(a) => a,
                                    None => return Err(ErrorKind::RuntimeError("string".to_owned(), "format".to_owned(), args.len(), format!("the arguments passed to the format function are less than the `{{}}` specifiers in the format string. The placeholder at {} can not be filled", pos)).into()),
                                };

                                if let Value::String(s) = arg {
                                    out.push_str(s.as_str())
                                } else {
                                    out.push_str(format!("{}", arg).as_str());
                                }
                            }
                            Some((_, '{')) => {
                                out.push('{');
                            }
                            _ => {
                                return Err(ErrorKind::RuntimeError("string".to_owned(), "format". to_owned(), args.len(), format!("the format specifier at {} is invalid. If you want to use `{{` as a literal in the string, you need to escape it with `{{{{`", pos)).into())
                            }
                        },
                        (pos, '}') => match iter.next() {
                            Some((pos, '}')) => out.push('}'),
                            _ => {
                                return Err(ErrorKind::RuntimeError("string".to_owned(), "format".to_owned(), args.len(), format!("the format specifier at {} is invalid. You have to terminate `}}` with another `}}` to escape it", pos)).into());
                            }
                        },
                        (_, c) => out.push(c),
                    }
                        }

                        if arg_stack.is_empty() {
                            Ok(Value::String(out))
                        } else {
                            Err(ErrorKind::RuntimeError("string".to_owned(), "format".to_owned(), args.len(), "too many parameters passed. Ensure that you have the same number of {{}} in your format string".into()).into())
                        }
                    }
                    _ => {
                        Err(ErrorKind::RuntimeError("string".to_owned(), "fprmat".to_owned(), args.len(), "expected 1st parameter to format to be a format specifier e.g. to  print a number use `string::format(\"{{}}\", 1)`".into()).into())

                    }
                }
            }
        }
    }
    registry
        .insert(tremor_fn! (math::max(_context, a: Number, b: Number){
            if a.as_f64() > b.as_f64() {
                Ok(Value::Number(a.to_owned()))
            } else {
                Ok(Value::Number(b.to_owned()))
            }
        }))
        .insert(tremor_fn!(math::min(_context, a: Number, b: Number) {
            if a.as_f64() < b.as_f64() {
                Ok(Value::Number(a.to_owned()))

            } else {
                Ok(Value::Number(b.to_owned()))
            }
        }))
        .insert(tremor_fn!(system::hostname(_context) {
            if let Some(hostname) = get_hostname(){
                Ok(Value::String(hostname))
            } else {
                Err(ErrorKind::RuntimeError("system".to_owned(), "hostname".to_owned(), 0, "could not get hostname".into()).into())
            }

        }))
        .insert(TremorFnWrapper {
            module: "string".to_owned(),
            name: "format".to_string(),
            fun: format,
        });
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

pub struct Registry<Ctx: Context> {
    functions: HashMap<String, HashMap<String, TremorFnWrapper<Ctx>>>,
}

impl<Ctx: Context + 'static> TremorFnWrapper<Ctx> {
    pub fn invoke(&self, context: &Ctx, args: &[Value]) -> Result<Value> {
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
    ($module:ident :: $name:ident($context:ident, $($arg:ident : $type:ident),*) $code:block) => {
        {
            use serde_json::Value;
            use $crate::{TremorFnWrapper, Context};
            fn $name<'c, 'a, Ctx: Context + 'static>($context: &'c Ctx, args: &'a [Value]) -> Result<Value>{
                // rust claims that the pattern is unreachable even
                // though it isn't, so linting it
                match args {
                    [$(
                        Value::$type($arg),
                    )*] => {$code}
                    [$(
                        $arg,
                    )*] => Err($crate::errors::Error::from($crate::errors::ErrorKind::BadType(stringify!($module).to_string(), stringify!($name).to_string(), args.len()))),
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
            use serde_json::Value;
            use $crate::{TremorFnWrapper, Context};
            fn $name<'c, 'a, Ctx: Context + 'static>($context: &'c Ctx, args: &'a [Value]) -> Result<Value>{
                // rust claims that the pattern is unreachable even
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry;
    use serde_json::json;

    fn fun<Ctx: Context + 'static>(m: &str, f: &str) -> TremorFn<Ctx> {
        registry().find(m, f).expect("could not find function")
    }
    #[test]
    pub fn call_a_function_from_a_registry_works() {
        let max = fun("math", "max");
        assert_eq!(Ok(json!(2)), max(&(), &[json!(1), json!(2)]));
    }

    #[test]
    pub fn bad_arrity() {
        let f = tremor_fn! (module::name(_context, _a: Number){
            Ok(format!("{}", _a).into())
        });
        let args = &[json!(1), json!(2)];

        assert!(f.invoke(&(), args).is_err());
    }

    #[test]
    pub fn bad_type() {
        let f = tremor_fn!(module::name(_context, _a: Number){
            Ok(format!("{}", _a).into())
        });

        assert!(f.invoke(&(), &[json!("1")]).is_err());
    }

    #[test]
    pub fn add() {
        let f = tremor_fn!(math::add(_context, _a: Number, _b: Number){
            match (_a.as_f64(), _b.as_f64()) {
                (Some(a), Some(b)) =>             Ok(json!(a + b)),
                _ => Err(ErrorKind::RuntimeError(
                    "math".to_string(),
                    "add".to_string(),
                    2,
                    "could not add numbers".into(),
                ).into())

            }

        });

        assert_eq!(Ok(json!(5.0)), f.invoke(&(), &[json!(2), json!(3)]));
    }

    #[test]
    pub fn t3() {
        let f = tremor_fn!(math::add(_context, _a: Number, _b: Number, _c: Number){
            match (_a.as_f64(), _b.as_f64(), _c.as_f64()) {
                (Some(a), Some(b), Some(c)) => Ok(json!(a + b + c)),
                _ => Err(ErrorKind::RuntimeError(
                    "math".to_string(),
                    "add".to_string(),
                    3,
                    "could not add numbers".into(),
                ).into())

            }

        });
        let args = &[json!(1), json!(2), json!(3)];

        assert_eq!(Ok(json!(6.0)), f.invoke(&(), args));
    }

    #[test]
    pub fn registry_format_with_3_args() {
        let f = fun("math", "max");

        assert!(f(&(), &[json!(1), json!(2), json!(3)]).is_err());
    }

    #[test]
    pub fn format() {
        let format = fun("string", "format");
        assert_eq!(Ok(json!("empty")), format(&(), &[json!("empty")]));
        let format = fun("string", "format");
        assert_eq!(
            Ok(json!("12")),
            format(&(), &[json!("{}{}"), json!(1), json!(2)])
        );
        let format = fun("string", "format");
        assert_eq!(
            Ok(json!("1 + 2")),
            format(&(), &[json!("{} + {}"), json!(1), json!(2)])
        );
    }

    #[test]
    pub fn format_literal_curlies() {
        let format = fun("string", "format");
        assert_eq!(Ok(json!("{}")), (format(&(), &[json!("{{}}")])));
    }

    #[test]
    pub fn format_literal_curlies_with_other_curlies_in_same_line() {
        let format = fun("string", "format");
        assert_eq!(
            Ok(json!("a string with {} in it has 1 {}")),
            format(
                &(),
                &[
                    json!("a string with {} in it has {} {{}}"),
                    json!("{}"),
                    json!(1)
                ]
            )
        );
    }

    #[test]
    pub fn format_evil_test() {
        let format = fun("string", "format");
        assert_eq!(Ok(json!("}")), format(&(), &[json!("}}")]));
    }

    #[test]
    pub fn format_escaped_foo() {
        let format = fun("string", "format");
        assert_eq!(Ok(json!("{foo}")), format(&(), &[json!("{{foo}}")]));
    }

    #[test]
    pub fn format_three_parenthesis() {
        let format = fun("string", "format");
        assert_eq!(
            Ok(json!("{foo}")),
            format(&(), &[json!("{{{}}}"), json!("foo")])
        );
    }

    #[test]
    pub fn umatched_parenthesis() {
        let format = fun("string", "format");
        let res = format(&(), &[json!("{")]);

        assert!(res.is_err());
    }

    #[test]
    pub fn unmatched_3_parenthesis() {
        let format = fun("string", "format");
        let res = format(&(), &[json!("{{{")]);

        assert!(res.is_err());
    }

    #[test]
    pub fn unmatched_closed_parenthesis() {
        let format = fun("string", "format");
        let res = format(&(), &[json!("}")]);

        assert!(res.is_err());
    }

    #[test]
    pub fn unmatched_closed_3_parenthesis() {
        let format = fun("string", "format");
        let res = format(&(), &[json!("}}}")]);

        assert!(res.is_err());
    }

    #[test]
    pub fn unmatched_parenthesis_with_string() {
        let format = fun("string", "format");
        let res = format(&(), &[json!("{foo}")]);

        assert!(res.is_err());
    }

    #[test]
    pub fn unmatched_parenthesis_with_argument() {
        let format = fun("string", "format");
        let res = format(&(), &[json!("{foo}"), json!("1")]);

        assert!(res.is_err());
    }

    #[test]
    pub fn unmatched_parenthesis_too_few() {
        let format = fun("string", "format");
        let res = format(&(), &[json!("{}")]);

        assert!(res.is_err());
    }

    #[test]
    pub fn unmatched_parenthesis_too_many() {
        let format = fun("string", "format");
        let res = format(&(), &[json!("{}"), json!("1"), json!("2")]);

        assert!(res.is_err());
    }

    #[test]
    pub fn bad_format() {
        let format = fun("string", "format");
        let res = format(&(), &[json!(7)]);

        assert!(res.is_err());
    }

}
