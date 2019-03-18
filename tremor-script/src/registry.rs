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
        if args.is_empty() {
            return Err(ErrorKind::RuntimeError(
                "string".to_string(),
                "format".to_string(),
                args.len(),
            )
            .into());
        }
        let format = args[0].as_str().to_owned();

        match format {
            Some(fmt) => {
                let mut out = String::from(fmt);
                for arg in args[1..].iter() {
                    if let Some(a) = arg.as_str() {
                        out = out.replacen("{}", a, 1);
                    } else {
                        return Err(ErrorKind::RuntimeError(
                            "string".to_string(),
                            "format".to_string(),
                            args.len(),
                        )
                        .into());
                    }
                }
                Ok(Value::String(out.to_owned()))
            }
            None => {
                Err(
                    ErrorKind::RuntimeError("string".to_string(), "format".to_string(), args.len())
                        .into(),
                )
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
            Ok(Value::String(get_hostname().unwrap()))
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

    #[test]
    pub fn call_a_function_from_a_registry_works() {
        let returned_value = registry().find("math", "max");
        assert_eq!(
            json!(2),
            (returned_value.unwrap()(&(), &[json!(1), json!(2)]).unwrap())
        );
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
        let f = tremor_fn!(module::name(_context, _a: Number, _b: Number){
            Ok(json!(_a.as_f64().unwrap() + _b.as_f64().unwrap()))
        });

        assert_eq!(f.invoke(&(), &[json!(2), json!(3)]).unwrap(), json!(5.0));
    }

    #[test]
    pub fn t3() {
        let f = tremor_fn!(module::name(_context, _a: Number, _b: Number, _c: Number)  {
            Ok(json!(_a.as_i64().unwrap()  + _b.as_i64().unwrap()  + _c.as_i64().unwrap()))
        });
        let args = &[json!(1), json!(2), json!(3)];

        assert_eq!(f.invoke(&(), args).unwrap(), json!(6));
    }

    #[test]
    pub fn registry_format_with_3_args() {
        let f = registry().find("math", "max").unwrap();

        assert!(f(&(), &[json!(1), json!(2), json!(3)]).is_err());
    }

    #[ignore]
    #[test]
    pub fn format() {
        let format = registry().find("string", "format");
        assert_eq!(
            json!("empty"),
            (format.unwrap()(&(), &[json!("empty")]).unwrap())
        );
        let format = registry().find("string", "format");
        assert_eq!(
            json!("12"),
            (format.unwrap()(&(), &[json!("{}{}"), json!(1), json!(2)]).unwrap())
        );
        let format = registry().find("string", "format");
        assert_eq!(
            json!("1 + 2"),
            (format.unwrap()(&(), &[json!("{} + {}"), json!(1), json!(2)]).unwrap())
        );
    }
    #[ignore]
    #[test]
    pub fn format_literal_curlies() {
        let format = registry().find("string", "format");
        assert_eq!(
            json!("{}"),
            (format.unwrap()(&(), &[json!("{{}}")]).unwrap())
        );
        let format = registry().find("string", "format");
        assert_eq!(
            json!("a string with {} in it has {} {}"),
            (format.unwrap()(
                &(),
                &[
                    json!("a string with {} in it has {} {{}}"),
                    json!("{}"),
                    json!(1)
                ]
            )
            .unwrap())
        );
    }

}
