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

use crate::registry::{mfa, Context, FResult, FunctionError, Registry, TremorFnWrapper};
use crate::tremor_fn;
use simd_json::BorrowedValue as Value;

macro_rules! map_function {
    ($name:ident, $fun:ident) => {
        tremor_fn! (string::$name(_context, _input: String) {
            Ok(Value::from(_input.$fun()))
        })
    };
    ($fun:ident) => {
        tremor_fn! (string::$fun(_context, _input: String) {
            Ok(Value::from(_input.$fun()))
        })
    }
}
pub fn load<Ctx: 'static + Context>(registry: &mut Registry<Ctx>) {
    fn format<'event, Ctx: Context + 'static>(
        _context: &Ctx,
        args: &[&Value<'event>],
    ) -> FResult<Value<'event>> {
        let this_mfa = || mfa("string", "format", args.len());

        match args.len() {
            0 => Err(FunctionError::BadArity {
                mfa: this_mfa(), calling_a: args.len()
            }),
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
                                    None => return Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("the arguments passed to the format function are less than the `{{}}` specifiers in the format string. The placeholder at {} can not be filled", pos)}),
                                };

                                if let Value::String(s) = arg {
                                    out.push_str(&s)
                                } else {
                                    out.push_str(arg.to_string().as_str());
                                }
                            }
                            Some((_, '{')) => {
                                out.push('{');
                            }
                            _ => {
                                return  Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("the format specifier at {} is invalid. If you want to use `{{` as a literal in the string, you need to escape it with `{{{{`", pos)})
                            }
                        },
                        (pos, '}') => match iter.next() {
                            Some((_pos, '}')) => out.push('}'),
                            _ => {
                                return  Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("the format specifier at {} is invalid. You have to terminate `}}` with another `}}` to escape it", pos)});
                            }
                        },
                        (_, c) => out.push(c),
                    }
                        }

                        if arg_stack.is_empty() {
                            Ok(Value::from(out))
                        } else {
                            Err(FunctionError::RuntimeError{mfa: this_mfa(), error: "too many parameters passed. Ensure that you have the same number of {{}} in your format string".into()})
                        }
                    }
                    _ => {
                        Err(FunctionError::RuntimeError{mfa: this_mfa(), error: "expected 1st parameter to format to be a format specifier e.g. to  print a number use `string::format(\"{{}}\", 1)`".into()})

                    }
                }
            }
        }
    }
    registry
        .insert(
            tremor_fn! (string::replace(_context, _input: String, _from: String, _to: String) {
                let from: &str = _from;
                let to: &str = _to;
                Ok(Value::from(_input.replace(from, to)))
            }),
        )
        .insert(map_function!(is_empty))
        .insert(tremor_fn! (string::len(_context, _input: String) {
            Ok(Value::from(_input.chars().count() as i64))
        }))
        .insert(tremor_fn! (string::bytes(_context, _input: String) {
            Ok(Value::from(_input.len() as i64))
        }))
        .insert(tremor_fn! (string::trim(_context, _input: String) {
            Ok(Value::from(_input.trim().to_string()))
        }))
        .insert(tremor_fn! (string::trim_start(_context, _input: String) {
            Ok(Value::from(_input.trim_start().to_string()))
        }))
        .insert(tremor_fn! (string::trim_end(_context, _input: String) {
            Ok(Value::from(_input.trim_end().to_string()))
        }))
        .insert(map_function!(lowercase, to_lowercase))
        .insert(map_function!(uppercase, to_uppercase))
        .insert(tremor_fn!(string::capitalize(_context, _input: String) {
            let mut c = _input.chars();
            Ok(match c.next() {
                None => Value::from(""),
                Some(f) => Value::from(f.to_uppercase().collect::<String>() + c.as_str()),
            })
        }))
        .insert(
            tremor_fn!(string::substr(_context, _input: String, _start: I64, _end: I64) {
                // Since rust doens't handle UTF8 indexes we have to translate this
                // _input.char_indices() - get an iterator over codepoint indexes
                //   .nth(*_start as usize) - try to get the nth character as a byte index - returns an option of a two tuple 
                //   .map(|v| v.0) - map to the first argument (byte index)
                //   .unwrap_or_else(|| 0) - since this is an option we need to safely extract the value so we default it to 0 for start or len for end
                let start = _input.char_indices().nth(*_start as usize).map(|v| v.0).unwrap_or_else(|| 0);
                let end = _input.char_indices().nth(*_end as usize).map(|v| v.0).unwrap_or_else(|| _input.len());

                Ok(Value::from((&_input[start..end]).to_string()))
            }),
        )
        .insert(
            tremor_fn! (string::split(_context, _input: String, _sep: String) {
                let sep: &str = _sep;
                Ok(Value::Array(_input.split(sep).map(|v| Value::from(v.to_string())).collect()))
            }),
        )
        .insert(TremorFnWrapper {
            module: "string".to_string(),
            name: "format".to_string(),
            fun: format,
            argc: 2,
        });
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use simd_json::BorrowedValue as Value;
    macro_rules! assert_val {
        ($e:expr, $r:expr) => {
            assert_eq!($e, Ok(Value::from($r)))
        };
    }

    #[test]
    fn replace() {
        let f = fun("string", "replace");
        let v1 = Value::from("this is a test");
        let v2 = Value::from("test");
        let v3 = Value::from("cake");

        assert_val!(f(&[&v1, &v2, &v3]), "this is a cake")
    }

    #[test]
    fn is_empty() {
        let f = fun("string", "is_empty");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), false);
        let v = Value::from("");
        assert_val!(f(&[&v]), true)
    }

    #[test]
    fn len() {
        let f = fun("string", "len");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), 14)
    }

    #[test]
    fn trim() {
        let f = fun("string", "trim");
        let v = Value::from(" this is a test ");
        assert_val!(f(&[&v]), "this is a test")
    }

    #[test]
    fn trim_start() {
        let f = fun("string", "trim_start");
        let v = Value::from(" this is a test ");
        assert_val!(f(&[&v]), "this is a test ")
    }

    #[test]
    fn trim_end() {
        let f = fun("string", "trim_end");
        let v = Value::from(" this is a test ");
        assert_val!(f(&[&v]), " this is a test")
    }

    #[test]
    fn lowercase() {
        let f = fun("string", "lowercase");
        let v = Value::from("THIS IS A TEST");
        assert_val!(f(&[&v]), "this is a test")
    }

    #[test]
    fn uppercase() {
        let f = fun("string", "uppercase");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), "THIS IS A TEST")
    }

    #[test]
    fn capitalize() {
        let f = fun("string", "capitalize");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), "This is a test")
    }

    #[test]
    fn substr() {
        let f = fun("string", "substr");
        let v = Value::from("this is a test");
        let s = Value::from(5);
        let e = Value::from(7);
        assert_val!(f(&[&v, &s, &e]), "is");
        let f = fun("string", "substr");
        let v = Value::from("♥ utf8");
        let s = Value::from(0);
        let e = Value::from(1);
        assert_val!(f(&[&v, &s, &e]), "♥");
        let s = Value::from(2);
        let e = Value::from(6);
        assert_val!(f(&[&v, &s, &e]), "utf8");
    }

    #[test]
    fn split() {
        let f = fun("string", "split");
        let v1 = Value::from("this is a test");
        let v2 = Value::from(" ");
        assert_val!(
            f(&[&v1, &v2]),
            Value::Array(vec![
                Value::from("this"),
                Value::from("is"),
                Value::from("a"),
                Value::from("test")
            ])
        )
    }
}
