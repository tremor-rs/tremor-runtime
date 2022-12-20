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

use crate::errors::Result;
use simd_json::ValueAccess;
use tremor_script::{
    registry::{mfa, FResult, FunctionError, Mfa, Registry},
    EventContext, TremorFn, TremorFnWrapper
};
use tremor_value::Value;
/// Install's common functions into a registry
///
/// # Errors
///  * if we can't install extensions

#[derive(Clone, Debug, Default)]
struct PluggableLoggingFm {
    name: String,
}

impl PluggableLoggingFm {
    fn info() -> Self {
        Self {
            name: "info".into(),
        }
    }

    fn debug() -> Self {
        Self {
            name: "debug".into(),
        }
    }

    fn error() -> Self {
        Self {
            name: "error".into(),
        }
    }

    fn warn() -> Self {
        Self {
            name: "warn".into(),
        }
    }

    fn trace() -> Self {
        Self {
            name: "trace".into(),
        }
    }
}

impl TremorFn for PluggableLoggingFm {
    /// It can handle both named formatting (with identifier) and positional formatting (in order)
    fn invoke<'event, 'c>(
        &self,
        _ctx: &'c EventContext,
        args: &[&Value<'event>],
    ) -> FResult<Value<'event>> {
        let this_mfa = || mfa("logging", &self.name, args.len());

        if args.is_empty() {
            return Err(FunctionError::BadArity {
                mfa: this_mfa(),
                calling_a: args.len(),
            });
        }

        if let Some((format, arg_stack)) = args
            .split_first()
            .and_then(|(f, arg_stack)| Some((f.as_str()?, arg_stack)))
        {
            let out = match arg_stack.len() {
                0 => {
                    // arg_stack.len() < 1 (== 0)
                    positional_formatting(this_mfa(), format, &Value::Array(vec![]))
                    // `arg_stack` is empty
                }
                1 => {
                    let mut arg_stack: Vec<&&Value> = arg_stack.iter().collect();
                    let &arg_stack = arg_stack.pop().ok_or(FunctionError::RuntimeError {
                        mfa: this_mfa(),
                        error: "missing argument".into(),
                    })?;

                    if arg_stack.as_object().is_some() {
                        // retrieve format args as a hashmap
                        named_formatting(this_mfa(), format, arg_stack)
                    } else if arg_stack.as_array().is_some() {
                        positional_formatting(this_mfa(), format, arg_stack)
                    } else {
                        positional_formatting(
                            this_mfa(),
                            format,
                            &Value::Array(vec![arg_stack.clone()]),
                        )
                    }
                }
                _ => {
                    let arg_stack: Vec<Value> =
                        arg_stack.iter().map(|v| v.clone_static()).collect();
                    positional_formatting(this_mfa(), format, &Value::Array(arg_stack))
                }
            };

            match out {
                Ok(out_ok) => {
                    let mut result = tremor_value::Object::default();
                    result.insert("args".into(), out_ok.clone());
                    match self.name.as_str() {
                        "debug" => {
                            debug!("{}", out_ok);

                            result.insert("level".into(), Value::from("DEBUG"));
                            Ok(Value::Object(Box::new(result)))
                        }
                        "error" => {
                            error!("{}", out_ok);

                            result.insert("level".into(), Value::from("ERROR"));
                            Ok(Value::Object(Box::new(result)))
                        }
                        "info" => {
                            info!("{}", out_ok.clone());

                            result.insert("level".into(), Value::from("INFO"));
                            Ok(Value::Object(Box::new(result)))
                        }
                        "trace" => {
                            trace!("{}", out_ok.clone());

                            result.insert("level".into(), Value::from("TRACE"));
                            Ok(Value::Object(Box::new(result)))
                        }
                        "warn" => {
                            warn!("{}", out_ok.clone());

                            result.insert("level".into(), Value::from("WARN"));
                            Ok(Value::Object(Box::new(result)))
                        }
                        func => Err(FunctionError::RuntimeError {
                            mfa: this_mfa(),
                            error: format!("Unknown logging function \"{func}\""),
                        }),
                    }
                }
                err => err,
            }
        } else {
            Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("expected 1st parameter to format to be a format specifier e.g. to print a number use `logging::{}(\"{{}}\", 1)`", self.name)})
        }
    }

    fn boxed_clone(&self) -> Box<dyn TremorFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> std::ops::RangeInclusive<usize> {
        1_usize..=usize::max_value()
    }
    fn is_const(&self) -> bool {
        true
    }

    fn valid_arity(&self, n: usize) -> bool {
        self.arity().contains(&n)
    }
}

/// Almost exact copy from String
fn positional_formatting<'event>(
    this_mfa: Mfa,
    format: &str,
    arg_stack: &Value,
) -> FResult<Value<'event>> {
    let mut remaining_args: Vec<&Value>;

    if let Some(arg_stack) = arg_stack.as_array() {
        remaining_args = arg_stack.iter().rev().collect();
    } else {
        return Err(FunctionError::RuntimeError{mfa: this_mfa, error: ("`arg_stack` of type `&Value` should hold a vector as `Vec<&Value>`, but couldn't be cast as one").to_owned()});
    }

    let mut out = String::with_capacity(format.len());
    let mut iter = format.chars().enumerate();
    while let Some((pos, char)) = iter.next() {
        match char {
            '\\' => if let Some((_, c)) = iter.next() {
                if c != '{' && c != '}' {
                    out.push('\\');
                };
                out.push(c);
            } else {
                return Err(FunctionError::RuntimeError{mfa: this_mfa, error: format!("bad escape sequence at {pos}")});
            }
            '{' => match iter.next() {
                Some((_, '}')) => if let Some(arg) = remaining_args.pop() {
                    out.push_str(&match format_tremor_value(arg) {
                        Ok(s) => s,
                        Err(e) => return Err(FunctionError::RuntimeError{mfa: this_mfa, error: format!("{e}")})
                    });
                } else {
                        return Err(FunctionError::RuntimeError{mfa: this_mfa, error: format!("the arguments passed to the format function are less than the `{{}}` specifiers in the format string. The placeholder at {pos} can not be filled")});
                },
                Some((_, '{')) => {
                    out.push('{');
                }
                _ => {
                    return Err(FunctionError::RuntimeError{mfa: this_mfa, error: format!("the format specifier at {pos} is invalid. If you want to use `{{` as a literal in the string, you need to escape it with `{{{{`")})
                }
            },
            '}' => if let Some((_, '}')) =  iter.next() {
                out.push('}');
            } else {
                return Err(FunctionError::RuntimeError{mfa: this_mfa, error: format!("the format specifier at {pos} is invalid. You have to terminate `}}` with another `}}` to escape it")});
            },
            c => out.push(c),
        }
    }
    Ok(Value::from(out))
}

fn named_formatting<'event>(
    this_mfa: Mfa,
    format: &str,
    arg_stack: &Value,
) -> FResult<Value<'event>> {
    let format_fields: &tremor_value::Object;

    if let Some(arg_stack) = arg_stack.as_object() {
        format_fields = arg_stack;
    } else {
        return Err(FunctionError::RuntimeError{
			mfa: this_mfa,
			error: ("`arg_stack` of type `&Value` should hold a vector as `&tremor_value::Object` aka `&HashMap<Cow<str, Wide>, Value>`, but couldn't be cast as one").to_owned()});
    }

    let mut out = String::with_capacity(format.len());
    let mut iter = format.chars().enumerate();
    while let Some((pos, char)) = iter.next() {
        match char {
            '\\' => {
                if let Some((_, c)) = iter.next() {
                    if c != '{' && c != '}' {
                        out.push('\\');
                    };
                    out.push(c);
                } else {
                    return Err(FunctionError::RuntimeError {
                        mfa: this_mfa,
                        error: format!(
                            "bad escape sequence at {pos}: cannot end string with escape character"
                        ),
                    });
                }
            }
            '{' => {
                let mut identifier = String::new(); // named placeholder "format specifier"
                let mut init = true;
                loop {
                    // reading the identifier name
                    match iter.next() {
                        Some((_, '{')) => {
                            if init {
                                out.push('{');
                                break;
                            }
                            return Err(FunctionError::RuntimeError{
								mfa: this_mfa,
								error: format!("the format specifier at {pos} is invalid. You have to terminate `{{` with another `{{` to escape it")});
                            	},
                        Some((_, '}')) => {
                            match format_fields.get(identifier.as_str()) {
                                Some(value) =>  {
                                    match format_tremor_value(value) {
                                        Ok(result) => {
                                            out.push_str(result.as_str());
                                        },
                                        Err(e) => return Err(FunctionError::RuntimeError{mfa: this_mfa, error: format!("{e}")})
                                    };
                                },
                                None => return Err(FunctionError::RuntimeError{mfa: this_mfa, error: format!("the format name at {pos} cannot be found among* the `Value:Object` given. Cannot insert field mapped with name {identifier} because it was not found in the given hashmap of args. *This is sus")})
                            }
                            break;
                        },
                        Some((_, c)) => {
                            init = false;
                            identifier.push(c);
                        },
                        None => return Err(FunctionError::RuntimeError{mfa: this_mfa, error: format!("the format specifier at {pos} is invalid. Identifier truncated. No ending `}}` found.")})
                    }
                }
            }
            '}' => {
                if let Some((_, '}')) = iter.next() {
                    out.push('}');
                } else {
                    return Err(FunctionError::RuntimeError{mfa: this_mfa, error: format!("the format specifier at {pos} is invalid. You have to terminate `}}` with another `}}` to escape it")});
                }
            }
            c => out.push(c),
        }
    }
    Ok(Value::from(out))
}

fn format_tremor_value(value: &Value) -> Result<String> {
    let mut result = String::new();
    let parsing_error = Err("Not Implemented. Could not deserialize from `Tremor::Value` to any supported rust types.\nSupported types are either &str, i32, bool, Vec, bytes, HashMap".into());

    if let Some(string) = value.as_str() {
        result.push_str(string);
        return Ok(result);
    }

    if let Some(integer32) = value.as_i32() {
        result.push_str(&integer32.to_string());
        return Ok(result);
    }

    if let Some(float32) = value.as_f32() {
        result.push_str(&float32.to_string());
        return Ok(result);
    }

    if let Some(boolean) = value.as_bool() {
        result.push_str(&boolean.to_string());
        return Ok(result);
    }

    if let Some(array) = value.as_array() {
        let mut array = array.clone();
        result.push('[');
        let mut last = String::new();
        if !array.is_empty() {
            // Keeping the last value of the array for a nice formatting
            // We will be able to get [hello, nice, last] instead of [hello, nice, last, ]
            if let Some(v) = &array.pop() {
                last = match format_tremor_value(v) {
                    Ok(v) => v,
                    Err(_) => {
                        return parsing_error;
                    }
                };
            } else {
                return Err("tremor_value::Array is empty but should not be".into());
            }
        }
        for v in &array {
            result.push_str(&match format_tremor_value(&v.clone_static()) {
                Ok(v) => v,
                Err(_) => {
                    return parsing_error;
                }
            });
            result.push_str(", ");
        }
        // last element
        result.push_str(&last);
        result.push(']');

        return Ok(result);
    }

    if let Some(bytes) = value.as_bytes() {
        result.push_str(
            bytes
                .to_vec()
                .iter()
                .map(|int: &u8| *int as char)
                .collect::<String>()
                .as_str(),
        );
        return Ok(result);
    }

    if let Some(obj) = value.as_object() {
        let mut obj = obj.clone();
        result.push('{');
        let mut last = String::new();
        if !obj.is_empty() {
            // Keeping the last pair of key:value of the object for a nice formatting
            // We will be able to get {hello: nice, ok: last} instead of {hello: nice, ok: last, }

            if let Some(pair) = obj.iter().last() {
                let p: (&str, &String) = (
                    &pair.0.clone(),
                    &match format_tremor_value(&pair.1.clone_static()) {
                        Ok(v) => v,
                        Err(_) => {
                            return parsing_error;
                        }
                    },
                );

                obj.remove(p.0);
                let mut p2 = String::new();
                p2.push_str(p.0);
                p2.push_str(": ");
                p2.push_str(p.1);
                last = p2;
            } else {
                return Err("tremor_value::Object is empty but should not be".into());
            }
        }

        for pair in obj.iter() {
            let pair = (&pair.0.clone().to_string(), pair.1);
            result.push_str(pair.0); //  key
            result.push_str(": "); //  sep
            result.push_str(
                // value
                &match format_tremor_value(&pair.1.clone_static()) {
                    Ok(v) => v,
                    Err(_) => {
                        return parsing_error;
                    }
                },
            );
            result.push_str(", "); //pair sep
        }
        // last element
        result.push_str(&last);
        result.push('}');

        return Ok(result);
    }
    parsing_error
}

/// Extend function registry with standard logging functions
pub fn load(reg: &mut Registry) {
    (reg)
        .insert(TremorFnWrapper::new(
            "logging".to_string(),
            "info".to_string(),
            Box::new(PluggableLoggingFm::info()),
        ))
        .insert(TremorFnWrapper::new(
            "logging".to_string(),
            "warn".to_string(),
            Box::new(PluggableLoggingFm::warn()),
        ))
        .insert(TremorFnWrapper::new(
            "logging".to_string(),
            "debug".to_string(),
            Box::new(PluggableLoggingFm::debug()),
        ))
        .insert(TremorFnWrapper::new(
            "logging".to_string(),
            "error".to_string(),
            Box::new(PluggableLoggingFm::error()),
        ))
        .insert(TremorFnWrapper::new(
            "logging".to_string(),
            "trace".to_string(),
            Box::new(PluggableLoggingFm::trace()),
        ));
}

#[cfg(test)]
mod test {
    use super::*;
    use tremor_script::ctx::EventContext;
    use tremor_script::{Registry};
    use tremor_value::{Value, Object, literal};

    #[test]
    fn test_info() {
        let mut reg = Registry::default();
        load(&mut reg);
        let fun = reg.find("logging", "info").unwrap();
        let ctx = EventContext::new(0, None);

        let mut obj = Object::new();
        obj.insert("key".into(), Value::from("value"));
        obj.insert(
            "key2".into(),
            Value::Array(vec![
                Value::from("value2"),
                Value::from("p"),
                Value::from("ojn"),
            ]),
        );

        let res = fun.invoke(
            &ctx,
            &[&Value::from("info {key2} {key}"), &Value::from(obj)],
        );
        assert_eq!(
            Ok(literal!({"args": "info [value2, p, ojn] value", "level": "INFO"})),
            res
        );
    }

    #[test]
    fn test_warn() {
        let mut reg = Registry::default();
        load(&mut reg);
        let fun = reg.find("logging", "warn").unwrap();
        let ctx = EventContext::new(0, None);

        let mut obj = Object::new();
        obj.insert("key".into(), Value::from("value"));
        obj.insert(
            "key2".into(),
            Value::Array(vec![
                Value::from("value2"),
                Value::from("p"),
                Value::from("ojn"),
            ]),
        );

        let res = fun.invoke(
            &ctx,
            &[&Value::from("info {key2} {key}"), &Value::from(obj)],
        );
        assert_eq!(
            Ok(literal!({"args": "info [value2, p, ojn] value", "level": "WARN"})),
            res
        );
    }

    #[test]
    fn test_debug() {
        let mut reg = Registry::default();
        load(&mut reg);
        let fun = reg.find("logging", "debug").unwrap();
        let ctx = EventContext::new(0, None);

        let mut obj = Object::new();
        obj.insert("key".into(), Value::from("value"));
        obj.insert(
            "key2".into(),
            Value::Array(vec![
                Value::from("value2"),
                Value::from("p"),
                Value::from("ojn"),
            ]),
        );

        let res = fun.invoke(
            &ctx,
            &[&Value::from("info {key2} {key}"), &Value::from(obj)],
        );
        assert_eq!(
            Ok(literal!({"args": "info [value2, p, ojn] value", "level": "DEBUG"})),
            res
        );
    }

    #[test]
    fn test_error() {
        let mut reg = Registry::default();
        load(&mut reg);
        let fun = reg.find("logging", "error").unwrap();
        let ctx = EventContext::new(0, None);

        let mut obj = Object::new();
        obj.insert("key".into(), Value::from("value"));
        obj.insert(
            "key2".into(),
            Value::Array(vec![
                Value::from("value2"),
                Value::from("p"),
                Value::from("ojn"),
            ]),
        );

        let res = fun.invoke(
            &ctx,
            &[&Value::from("info {key2} {key}"), &Value::from(obj)],
        );
        assert_eq!(
            Ok(literal!({"args": "info [value2, p, ojn] value", "level": "ERROR"})),
            res
        );
    }

    #[test]
    fn test_trace() {
        let mut reg = Registry::default();
        load(&mut reg);
        let fun = reg.find("logging", "trace").unwrap();
        let ctx = EventContext::new(0, None);

        let mut obj = Object::new();
        obj.insert("key".into(), Value::from("value"));
        obj.insert(
            "key2".into(),
            Value::Array(vec![
                Value::from("value2"),
                Value::from("p"),
                Value::from("ojn"),
            ]),
        );

        let res = fun.invoke(
            &ctx,
            &[&Value::from("info {key2} {key}"), &Value::from(obj)],
        );
        assert_eq!(
            Ok(literal!({"args": "info [value2, p, ojn] value", "level": "TRACE"})),
            res
        );
    }
}
