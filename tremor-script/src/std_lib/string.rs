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

use crate::prelude::*;
use crate::registry::{mfa, FResult, FunctionError, Registry, TremorFn, TremorFnWrapper};
use crate::EventContext;
use crate::Value;
use crate::{tremor_const_fn, tremor_fn_};

macro_rules! map_function {
    ($name:ident, $fun:ident) => {
        tremor_const_fn! (string|$name(_context, _input: String) {
            Ok(Value::from(_input.$fun()))
        })
    };
    ($fun:ident) => {
        tremor_const_fn! (string|$fun(_context, _input: String) {
            Ok(Value::from(_input.$fun()))
        })
    }
}

#[derive(Clone, Debug, Default)]
struct StringFormat {}
impl TremorFn for StringFormat {
    fn invoke<'event>(
        &self,
        _ctx: &EventContext,
        args: &[&Value<'event>],
    ) -> FResult<Value<'event>> {
        let this_mfa = || mfa("string", "format", args.len());
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
            let mut arg_stack: Vec<_> = arg_stack.iter().rev().collect();

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
                        return Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("bad escape sequence at {pos}")});

                    }
                    '{' => match iter.next() {
                        Some((_, '}')) => if let Some(arg) = arg_stack.pop() {
                            if let Some(s) = arg.as_str() {
                                out.push_str(s);
                            } else {
                                out.push_str(arg.encode().as_str());
                            };
                        } else {
                             return Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("the arguments passed to the format function are less than the `{{}}` specifiers in the format string. The placeholder at {pos} can not be filled")});
                        },
                        Some((_, '{')) => {
                            out.push('{');
                        }
                        _ => {
                            return Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("the format specifier at {pos} is invalid. If you want to use `{{` as a literal in the string, you need to escape it with `{{{{`")})
                        }
                    },
                    '}' => if let Some((_, '}')) =  iter.next() {
                        out.push('}');
                    } else {
                        return Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("the format specifier at {pos} is invalid. You have to terminate `}}` with another `}}` to escape it")});
                    },
                    c => out.push(c),
                }
            }
            if arg_stack.is_empty() {
                Ok(Value::from(out))
            } else {
                Err(FunctionError::RuntimeError{mfa: this_mfa(), error: "too many parameters passed. Ensure that you have the same number of {} in your format string".into()})
            }
        } else {
            Err(FunctionError::RuntimeError{mfa: this_mfa(), error: "expected 1st parameter to format to be a format specifier e.g. to print a number use `string::format(\"{}\", 1)`".into()})
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
}

pub fn load(registry: &mut Registry) {
    registry
        .insert(
            tremor_const_fn! (string|replace(_context, _input: String, _from: String, _to: String) {
                let from: &str = _from;
                let to: &str = _to;
                Ok(Value::from(_input.replace(from, to)))
            }),
        )
        .insert(map_function!(is_empty))
        .insert(tremor_const_fn! (string|len(_context, _input: String) {
            Ok(Value::from(_input.chars().count() as i64))
        }))
        .insert(tremor_const_fn! (string|bytes(_context, _input: String) {
            Ok(Value::from(_input.len() as i64))
        }))
        .insert(tremor_const_fn! (string|trim(_context, _input: String) {
            Ok(Value::from(_input.trim().to_string()))
        }))
        .insert(tremor_const_fn! (string|trim_start(_context, _input: String) {
            Ok(Value::from(_input.trim_start().to_string()))
        }))
        .insert(tremor_const_fn! (string|trim_end(_context, _input: String) {
            Ok(Value::from(_input.trim_end().to_string()))
        }))
        .insert(map_function!(lowercase, to_lowercase))
        .insert(map_function!(uppercase, to_uppercase))
        .insert(tremor_const_fn!(string|capitalize(_context, _input: String) {
            let mut c = _input.chars();
            Ok(match c.next() {
                None => Value::from(""),
                Some(f) => Value::from(f.to_uppercase().collect::<String>() + c.as_str()),
            })
        }))
        .insert(tremor_const_fn!(string|substr(_context, _input, _start, _end) {
                let ((input, start), end) = _input.as_str().zip(_start.as_usize()).zip(_end.as_usize()).ok_or_else(||FunctionError::BadType{mfa: this_mfa()})?;
                // Since rust doesn't handle UTF8 indexes we have to translate this
                // _input.char_indices() - get an iterator over codepoint indexes
                //   .nth(*_start as usize) - try to get the nth character as a byte index - returns an option of a two tuple
                //   .map(|v| v.0) - map to the first argument (byte index)
                //   .unwrap_or_else(|| 0) - since this is an option we need to safely extract the value so we default it to 0 for start or len for end
                let start = input.char_indices().nth(start).map_or_else(|| 0, |v| v.0);
                let end = input.char_indices().nth(end).map_or_else(|| input.len(), |v| v.0);
                Ok(Value::from(input.get(start..end).unwrap_or(input).to_string()))
            }),
        )
        .insert(tremor_const_fn! (string|split(_context, _input: String, _sep: String) {
                let sep: &str = _sep;
                Ok(Value::from(_input.split(sep).map(|v| Value::from(v.to_string())).collect::<Vec<_>>()))
            }),
        )
        .insert(tremor_const_fn! (string|from_utf8_lossy(_context, _bytes: Bytes) {
                Ok(Value::from(String::from_utf8_lossy(_bytes).to_string()))
            }),
        ).insert(tremor_const_fn! (string|contains(_context, _input: String, _contains: String) {
                use std::borrow::Borrow;
                let s: &str = _contains.borrow();
                Ok(Value::from(_input.contains(s)))
            }),
        ).insert(tremor_const_fn! (string|into_binary(_context, _input: String) {
                Ok(Value::Bytes(_input.as_bytes().to_vec().into()))
            }),
        ).insert(tremor_const_fn! (string|reverse(_context, _input: String) {
                let input = _input.clone();
                let output = input.chars().rev().collect::<String>();
                Ok(Value::from(output))
            }),
        ).insert(TremorFnWrapper::new(
            "string".to_string(),
            "format".to_string(),
            Box::<StringFormat>::default(),
        ));
}

#[cfg(test)]
mod test {
    use crate::registry::{fun, FunctionError};
    use crate::Value;
    #[test]
    fn from_utf8_lossy() {
        let f = fun("string", "from_utf8_lossy");
        let v = Value::Bytes("badger".as_bytes().into());
        assert_val!(f(&[&v]), "badger");
        let v = Value::Bytes(b"badger\xF0\x90\x80".to_vec().into());
        assert_val!(f(&[&v]), "badger\u{fffd}");
    }
    #[test]
    fn reverse() {
        let f = fun("string", "reverse");
        let v = Value::from("badger");
        assert_val!(f(&[&v]), Value::from("regdab"));
    }
    #[test]
    fn into_binary() {
        let f = fun("string", "into_binary");
        let v = Value::from("badger");
        assert_val!(f(&[&v]), Value::Bytes("badger".as_bytes().into()));
    }
    #[test]
    fn replace() {
        let f = fun("string", "replace");
        let v1 = Value::from("this is a test");
        let v2 = Value::from("test");
        let v3 = Value::from("cake");
        assert_val!(f(&[&v1, &v2, &v3]), "this is a cake");

        let v1 = Value::from("this is a test with test present twice");
        assert_val!(
            f(&[&v1, &v2, &v3]),
            "this is a cake with cake present twice"
        );
    }

    #[test]
    fn is_empty() {
        let f = fun("string", "is_empty");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), false);

        let v = Value::from("");
        assert_val!(f(&[&v]), true);
    }

    #[test]
    fn len() {
        let f = fun("string", "len");

        let v = Value::from("");
        assert_val!(f(&[&v]), 0);

        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), 14);

        let v = Value::from("this is another test \u{2665}");
        assert_val!(f(&[&v]), 22);
    }

    #[test]
    fn bytes() {
        let f = fun("string", "bytes");

        let v = Value::from("");
        assert_val!(f(&[&v]), 0);

        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), 14);

        let v = Value::from("this is another test \u{2665}");
        assert_val!(f(&[&v]), 24);
    }

    #[test]
    fn trim() {
        let f = fun("string", "trim");

        let v = Value::from("   ");
        assert_val!(f(&[&v]), "");

        let v = Value::from(" this is a test ");
        assert_val!(f(&[&v]), "this is a test");

        // Non-breaking spaces, i.e., non-ASCII whitespace
        let v = Value::from("\u{00A0}\u{00A0}this is a test\u{00A0}\u{00A0}");
        assert_val!(f(&[&v]), "this is a test");
    }

    #[test]
    fn trim_start() {
        let f = fun("string", "trim_start");
        let v = Value::from(" this is a test ");
        assert_val!(f(&[&v]), "this is a test ");
    }

    #[test]
    fn trim_end() {
        let f = fun("string", "trim_end");
        let v = Value::from(" this is a test ");
        assert_val!(f(&[&v]), " this is a test");
    }

    #[test]
    fn lowercase() {
        let f = fun("string", "lowercase");
        let v = Value::from("THIS IS A TEST");
        assert_val!(f(&[&v]), "this is a test");
    }

    #[test]
    fn uppercase() {
        let f = fun("string", "uppercase");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), "THIS IS A TEST");
    }

    #[test]
    fn capitalize() {
        let f = fun("string", "capitalize");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), "This is a test");
    }

    #[test]
    fn substr() {
        let f = fun("string", "substr");
        let v = Value::from("this is a test");
        let s = Value::from(5);
        let e = Value::from(7);
        assert_val!(f(&[&v, &s, &e]), "is");
        let f = fun("string", "substr");
        let v = Value::from("\u{2665} utf8");
        let s = Value::from(0);
        let e = Value::from(1);
        assert_val!(f(&[&v, &s, &e]), "\u{2665}");
        let s = Value::from(2);
        let e = Value::from(6);
        assert_val!(f(&[&v, &s, &e]), "utf8");
    }

    #[test]
    fn split() {
        let f = fun("string", "split");
        let v1 = Value::from("this is a test");
        let v2 = Value::from(" ");
        assert_val!(f(&[&v1, &v2]), Value::from(vec!["this", "is", "a", "test"]));
    }

    #[test]
    fn contains() {
        let f = fun("string", "contains");
        let v1 = Value::from("hello snot badger");
        let v2 = Value::from("snot");
        let v3 = Value::from("gnot");
        assert_val!(f(&[&v1, &v2]), Value::from(true));
        assert_val!(f(&[&v1, &v3]), Value::from(false));
        let v1 = Value::from("snot badger");
        let v2 = Value::from("snot badger");
        assert_val!(f(&[&v1, &v2]), Value::from(true));
    }

    #[test]
    fn format() {
        let f = fun("string", "format");
        let v1 = Value::from("a string with {}");
        let v2 = Value::from("more text");
        let v3 = Value::from(123);
        assert_val!(f(&[&v1, &v2]), Value::from("a string with more text"));
        assert_val!(f(&[&v1, &v3]), Value::from("a string with 123"));

        // Format string missing
        match f(&[&v3]) {
            Err(FunctionError::RuntimeError { error, .. }) => {
                assert_eq!(error, "expected 1st parameter to format to be a format specifier e.g. to print a number use `string::format(\"{}\", 1)`");
            }
            // ALLOW: panicking is the idiomatic way to signal failure in tests
            _any_other_result => panic!("Call should have failed: format string missing"),
        }

        // Too few arguments for format string
        // TODO: this error message isn't as helpful as it could be
        match f(&[&v1]) {
            Err(FunctionError::RuntimeError { error, .. }) => {
                assert_eq!(error, "the arguments passed to the format function are less than the `{}` specifiers in the format string. The placeholder at 14 can not be filled");
            }
            // ALLOW: panicking is the idiomatic way to signal failure in tests
            _any_other_result => panic!("Call should have failed: too few arguments provided"),
        }

        // Too many arguments for format string
        match f(&[&v1, &v2, &v3]) {
            Err(FunctionError::RuntimeError { error, .. }) => {
                assert_eq!(error, "too many parameters passed. Ensure that you have the same number of {} in your format string");
            }
            // ALLOW: panicking is the idiomatic way to signal failure in tests
            _any_other_result => panic!("Call should have failed: too many arguments provided"),
        }
    }
}
