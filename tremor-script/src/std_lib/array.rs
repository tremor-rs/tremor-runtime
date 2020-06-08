// Copyright 2018-2020, Wayfair GmbH
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

use crate::registry::Registry;
use crate::tremor_const_fn;
use simd_json::{prelude::*, BorrowedValue as Value};

pub fn load(registry: &mut Registry) {
    registry
        .insert(tremor_const_fn! (array::len(_context, _input: Array) {
            Ok(Value::from(_input.len() as i64))
        }))
        .insert(tremor_const_fn! (array::is_empty(_context, _input: Array) {
            Ok(Value::from(_input.is_empty()))
        }))
        .insert(tremor_const_fn! (array::contains(_context, _input, _contains) {
            if let Some(input) = _input.as_array() {
                Ok(Value::from(input.contains(&_contains)))
            } else {
                Err(FunctionError::BadType{mfa: mfa("array", "contains", 2)})
            }
        }))
        .insert(tremor_const_fn! (array::push(_context, _input, _value) {
            if let Some(input) = _input.as_array() {
                // We clone because we do NOT want to mutate the value
                // that was passed in
                let mut output = input.clone();
                let v: Value = (*_value).clone();
                output.push(v);
                Ok(Value::from(output))
            } else {
                 Err(FunctionError::BadType{mfa: mfa("array", "push", 2)})
            }
        }))
        .insert(tremor_const_fn! (array::unzip(_context, _input: Array) {
                let r: FResult<Vec<(Value, Value)>> = _input.iter().map(|a| if let Some(a) = a.as_array() {
                    if a.len() == 2 {
                        let second: Value = a[0].clone();
                        let first: Value = a[1].clone();
                        Ok((first, second))
                    } else {
                        Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("Onlay arrays that consist of tuples (arrays of two elements) can be unzipped but this array contained {} elements", a.len())})
                    }
                } else {
                    Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("Onlay arrays that consist of tuples (arrays of two elements) can be unzipped but this array contained: {:?}", a)})
                }).collect();
                let (r, l): (Vec<_>, Vec<_>) = r?.into_iter().unzip();
                Ok(Value::from(vec![l,
                    r,
                ]))
        }))
        .insert(tremor_const_fn!(array::zip(_context, _left: Array, _right: Array) {
            if _left.len() != _right.len() {
                return Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("Zipping two arrays requires them to have the same length, but the first array provided has {} elements while the second one has {} elements", _left.len(), _right.len())});
            };
            // TODO: Dear rust this is stupi! I don't want to call to_owned I just want consume values
            Ok(Value::from(_left.iter()
                .zip(_right.iter())
                .map(|(l, r)| Value::from(vec![l.clone(), r.clone()]))
                .collect::<Vec<_>>()))
        }))
        .insert(
            tremor_const_fn!(array::flatten(_context, _input) {
                Ok(Value::from(flatten_value(_input)))
            }))
        .insert(
            tremor_const_fn!(array::join(_context, _input: Array, _sep: String) {
                let input: Vec<String> = _input.iter().map(ToString::to_string).collect();
                Ok(Value::from(input.join(_sep)))
            }),
        )
        .insert(tremor_const_fn!(array::coalesce(_context, _input: Array) {
            Ok(Value::from(_input.iter().filter_map(|v| if v.is_null()  {
                None
            }else {
                Some(v.clone())
            }).collect::<Vec<_>>()))
        }));
}

//TODO this is not very nice
fn flatten_value<'event>(v: &Value<'event>) -> Vec<Value<'event>> {
    if let Some(a) = v.as_array() {
        a.iter().flat_map(flatten_value).collect()
    } else {
        vec![v.clone()]
    }
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use simd_json::prelude::*;
    use simd_json::BorrowedValue as Value;

    #[test]
    fn len() {
        let f = fun("array", "len");
        let v = Value::from(vec!["this", "is", "a", "test"]);
        assert_val!(f(&[&v]), 4);
        let v = Value::Array(vec![]);
        assert_val!(f(&[&v]), 0);
    }

    #[test]
    fn is_empty() {
        let f = fun("array", "is_empty");
        let v = Value::from(vec!["this", "is", "a", "test"]);
        assert_val!(f(&[&v]), false);
        let v = Value::array();
        assert_val!(f(&[&v]), true);
    }

    #[test]
    fn contains() {
        let f = fun("array", "contains");
        let v1 = Value::from(vec!["this", "is", "a", "test"]);
        let v2 = Value::from("is");
        assert_val!(f(&[&v1, &v2]), true);
    }

    #[test]
    fn join() {
        let f = fun("array", "join");
        let v1 = Value::from(vec!["this", "is", "a", "cake"]);
        let v2 = Value::from(" ");
        assert_val!(f(&[&v1, &v2]), Value::from("this is a cake"));
    }

    #[test]
    fn push() {
        let f = fun("array", "push");
        let v1 = Value::from(vec!["this", "is", "a", "test"]);
        let v2 = Value::from("cake");
        assert_val!(
            f(&[&v1, &v2]),
            Value::from(vec!["this", "is", "a", "test", "cake"])
        );
    }

    #[test]
    fn zip() {
        let f = fun("array", "zip");
        let v1 = Value::from(vec!["this", "is", "a", "test"]);
        let v2 = Value::from(vec!["cake", "really", "good", "cake"]);
        assert_val!(
            f(&[&v1, &v2]),
            Value::from(vec![
                vec!["this", "cake"],
                vec!["is", "really"],
                vec!["a", "good"],
                vec!["test", "cake"],
            ])
        );
    }

    #[test]
    fn unzip() {
        let f = fun("array", "unzip");
        let v = Value::from(vec![
            vec!["this", "cake"],
            vec!["is", "really"],
            vec!["a", "good"],
            vec!["test", "cake"],
        ]);
        assert_val!(
            f(&[&v]),
            Value::from(vec![
                vec!["this", "is", "a", "test"],
                vec!["cake", "really", "good", "cake"],
            ])
        );
    }

    #[test]
    fn flatten() {
        let f = fun("array", "flatten");
        let v = Value::from(vec![
            Value::from(vec!["this", "cake"]),
            Value::from(vec!["is", "really"]),
            Value::from(vec!["a", "good"]),
            Value::from(vec!["test", "cake"]),
            Value::from("!"),
        ]);
        assert_val!(
            f(&[&v]),
            Value::from(vec![
                "this", "cake", "is", "really", "a", "good", "test", "cake", "!",
            ])
        );
    }

    #[test]
    fn coalesce() {
        let f = fun("array", "coalesce");
        let v = Value::from(vec![
            Value::from("this"),
            Value::null(),
            Value::from("cake"),
            Value::from("is"),
            Value::from("really"),
            Value::null(),
            Value::from("a"),
            Value::from("good"),
            Value::from("test"),
            Value::null(),
            Value::from("cake"),
            Value::from("!"),
        ]);
        assert_val!(
            f(&[&v]),
            Value::from(vec![
                "this", "cake", "is", "really", "a", "good", "test", "cake", "!",
            ])
        );
    }
}
