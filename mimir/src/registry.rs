// Copyright 2018, Wayfair GmbH
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

use std::collections::HashMap;
use std::default::Default;
use std::error::Error;
use std::fmt::Display;

pub type TremorFn = fn(&dyn Context, &[Value]) -> Result<Value, FnError>;

use serde_json::Value;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum FnError {
    BadArity,
    BadType,
    ExecutionError,
}

impl Error for FnError {}

impl Display for FnError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FnError::BadArity => write!(f, "bad arity"),
            FnError::BadType => write!(f, "invalid type"),
            FnError::ExecutionError => write!(f, "function failed to execute"),
        }
    }
}

pub trait Context {}

impl Context for () {}

#[derive(Clone)]
pub struct TremorFnWrapper {
    pub module: String,
    pub name: String,
    pub fun: TremorFn,
}

pub struct Registry {
    functions: HashMap<String, HashMap<String, TremorFnWrapper>>,
}

impl TremorFnWrapper {
    pub fn invoke<T: Context + 'static>(
        &self,
        context: &T,
        args: &[Value],
    ) -> Result<Value, FnError> {
        (self.fun)(context, args)
    }
}

impl fmt::Debug for TremorFnWrapper {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}{}", self.module, self.name)
    }
}
impl PartialEq for TremorFnWrapper {
    fn eq(&self, other: &TremorFnWrapper) -> bool {
        self.module == other.module && self.name == other.name
    }
}

#[macro_export]
macro_rules! tremor_fn {
    ($module:ident :: $name:ident($context:ident, $($arg:ident : $type:ident),*) $code:block) => {
        {
            use serde_json::Value;
            TremorFnWrapper {
                module: stringify!($module).to_string(),
                name: stringify!($name).to_string(),
                fun: |context, args| {
                    // rust claims that the pattern is unreachable even
                    // though it isn't, so linting it
                    #[allow(unreachable_patterns)]
                    match args {
                        [$(
                            Value::$type($arg),
                        )*] => {$code}
                        [$(
                            $arg,
                        )*] => Err(FnError::BadType),
                        _ => Err(FnError::BadArity)
                    }
                }
            }
        }
    };


    ($module:ident :: $name:ident($context: ident) $code:block) => {
        {
            TremorFnWrapper {
                module: stringify!($module).to_string(),
                name: stringify!($name).to_string(),
                fun: |_context, _| {
                   { $code }
                }

            }
        }
    }
}

impl Default for Registry {
    fn default() -> Self {
        Registry {
            functions: HashMap::new(),
        }
    }
}

impl Registry {
    pub fn find(&self, module: &str, function: &str) -> Result<TremorFnWrapper, FnError> {
        if let Some(functions) = self.functions.get(module) {
            match functions.get(function) {
                Some(rf) => Ok(rf.to_owned()),
                None => Err(FnError::ExecutionError),
            }
        } else {
            Err(FnError::ExecutionError)
        }
    }

    pub fn insert(&mut self, function: TremorFnWrapper) -> &mut Registry {
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
    use serde_json::json;

    use super::*;
    use crate::REGISTRY;

    #[test]
    pub fn call_a_function_from_a_registry_works() {
        let returned_value = REGISTRY.lock().unwrap().find("math", "max");
        assert_eq!(
            json!(2),
            (returned_value
                .unwrap()
                .invoke(&(), &[json!(1), json!(2)])
                .unwrap())
        );
    }

    #[test]
    pub fn bad_arrity() {
        let f = tremor_fn! (module::name(_context, _a: Number){
            Ok(format!("{}", _a).into())
        });
        let args = &[json!(1), json!(2)];

        assert_eq!(f.invoke(&(), args), Err(FnError::BadArity));
    }

    #[test]
    pub fn bad_type() {
        let f = tremor_fn!(module::name(_context, _a: Number){
            Ok(format!("{}", _a).into())
        });

        assert_eq!(f.invoke(&(), &[json!("1")]), Err(FnError::BadType));
    }

    #[test]
    pub fn add() {
        let f = tremor_fn!(module::name(_context, _a: Number, _b: Number){
            dbg!((_a, _b));
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
        let f = REGISTRY.lock().unwrap().find("math", "max").unwrap();

        assert_eq!(
            f.invoke(&(), &[json!(1), json!(2), json!(3)]),
            Err(FnError::BadArity)
        );
    }

}
