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

use crate::registry::{Context, Registry};
use crate::tremor_fn;
use simd_json::{BorrowedValue, OwnedValue};

pub fn load<Ctx: 'static + Context>(registry: &mut Registry<Ctx>) {
    registry
        .insert(tremor_fn! (array::len(_context, _input: Array) {
            Ok(OwnedValue::from(_input.len() as i64))
        }))
        .insert(tremor_fn! (array::is_empty(_context, _input: Array) {
            Ok(OwnedValue::from(_input.is_empty()))
        }))
        .insert(tremor_fn! (array::contains(_context, _input, _contains) {
            match _input {
                BorrowedValue::Array(input) => Ok(OwnedValue::from(input.contains(&_contains))),
                _ => Err(FunctionError::BadType{mfa: mfa("array", "contains", 2)}),
            }
        }))
        .insert(tremor_fn! (array::push(_context, _input, _value) {
            match _input {
                BorrowedValue::Array( input) => {
                    // TODO: This is ugly
                    let mut input: Vec<OwnedValue> = input.iter().map(|e| e.clone().into()).collect();
                    let v: OwnedValue = (*_value).clone().into();
                    input.push(v);
                    Ok(OwnedValue::Array(input))
                }
                _ => Err(FunctionError::BadType{mfa: mfa("array", "push", 2)}),
            }
        }))
        .insert(tremor_fn! (array::unzip(_context, _input: Array) {
                let r: FResult<Vec<(OwnedValue, OwnedValue)>> = _input.iter().map(|a| match a {
                    BorrowedValue::Array(a) => if a.len() == 2 {
                        let second: OwnedValue = a[0].clone().into();
                        let first: OwnedValue = a[1].clone().into();
                        Ok((first, second))

                    } else {
                        Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("Onlay arrays that consist of tuples (arrays of two elements) can be unzipped but this array contained {} elements", a.len())})
                    }
                    other => Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("Onlay arrays that consist of tuples (arrays of two elements) can be unzipped but this array contained: {:?}", other)})
                }).collect();
                let (r, l): (Vec<_>, Vec<_>) = r?.into_iter().unzip();
                Ok(OwnedValue::Array(vec![
                    OwnedValue::Array(l),
                    OwnedValue::Array(r),
                ]))
        }))
        .insert(tremor_fn!(array::zip(_context, _left: Array, _right: Array) {
            if _left.len() != _right.len() {
                return Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("Zipping two arrays requires them to have the same length, but the first array provided has {} elements while the second one has {} elements", _left.len(), _right.len())});
            };
            // TODO: Dear rust this is stupi! I don't want to call to_owned I just want consume values
            Ok(OwnedValue::Array(
                _left.iter()
                    .zip(_right.iter())
                    .map(|(l, r)| OwnedValue::Array(vec![l.clone().into(), r.clone().into()]))
                    .collect(),
            ))
        }))
        .insert(
            tremor_fn!(array::flatten(_context, _input) {
                Ok(OwnedValue::Array(flatten_value(_input)))
            }))
        .insert(
            tremor_fn!(array::join(_context, _input: Array, _sep: String) {
                let input: Vec<String> = _input.iter().map(ToString::to_string).collect();
                Ok(OwnedValue::from(input.join(_sep)))
            }),
        )
        .insert(tremor_fn!(array::coalesce(_context, _input: Array) {
            Ok(OwnedValue::Array(_input.iter().filter_map(|v| match v {
                BorrowedValue::Null => None,
                other => Some(other.clone().into())
            }).collect()))
        }));
}

//TODO this is not very nice
fn flatten_value(v: &BorrowedValue) -> Vec<OwnedValue> {
    match v {
        BorrowedValue::Array(a) => {
            let mut r = Vec::with_capacity(a.len());
            for e in a {
                r.append(&mut flatten_value(e))
            }
            r
        }
        other => vec![other.clone().into()],
    }
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use simd_json::{BorrowedValue as Value, OwnedValue};
    macro_rules! assert_val {
        ($e:expr, $r:expr) => {
            assert_eq!($e, Ok(OwnedValue::from($r)))
        };
    }
    #[test]
    fn len() {
        let f = fun("array", "len");
        let v = Value::Array(vec![
            Value::from("this"),
            Value::from("is"),
            Value::from("a"),
            Value::from("test"),
        ]);
        assert_val!(f(&[&v]), 4);
        let v = Value::Array(vec![]);
        assert_val!(f(&[&v]), 0);
    }

    #[test]
    fn is_empty() {
        let f = fun("array", "is_empty");
        let v = Value::Array(vec![
            Value::from("this"),
            Value::from("is"),
            Value::from("a"),
            Value::from("test"),
        ]);
        assert_val!(f(&[&v]), false);
        let v = Value::Array(vec![]);
        assert_val!(f(&[&v]), true);
    }

    #[test]
    fn contains() {
        let f = fun("array", "contains");
        let v1 = Value::Array(vec![
            Value::from("this"),
            Value::from("is"),
            Value::from("a"),
            Value::from("test"),
        ]);
        let v2 = Value::from("is");
        assert_val!(f(&[&v1, &v2]), true);
    }

    #[test]
    fn join() {
        let f = fun("array", "join");
        let v1 = Value::Array(vec![
            Value::from("this"),
            Value::from("is"),
            Value::from("a"),
            Value::from("cake"),
        ]);
        let v2 = Value::from(" ");
        assert_val!(f(&[&v1, &v2]), Value::from("this is a cake"));
    }

    #[test]
    fn push() {
        let f = fun("array", "push");
        let v1 = Value::Array(vec![
            Value::from("this"),
            Value::from("is"),
            Value::from("a"),
            Value::from("test"),
        ]);
        let v2 = Value::from("cake");
        assert_val!(
            f(&[&v1, &v2]),
            Value::Array(vec![
                Value::from("this"),
                Value::from("is"),
                Value::from("a"),
                Value::from("test"),
                Value::from("cake")
            ])
        );
    }

    #[test]
    fn zip() {
        let f = fun("array", "zip");
        let v1 = Value::Array(vec![
            Value::from("this"),
            Value::from("is"),
            Value::from("a"),
            Value::from("test"),
        ]);
        let v2 = Value::Array(vec![
            Value::from("cake"),
            Value::from("really"),
            Value::from("good"),
            Value::from("cake"),
        ]);
        assert_val!(
            f(&[&v1, &v2]),
            OwnedValue::Array(vec![
                OwnedValue::Array(vec![OwnedValue::from("this"), OwnedValue::from("cake")]),
                OwnedValue::Array(vec![OwnedValue::from("is"), OwnedValue::from("really")]),
                OwnedValue::Array(vec![OwnedValue::from("a"), OwnedValue::from("good")]),
                OwnedValue::Array(vec![OwnedValue::from("test"), OwnedValue::from("cake")]),
            ])
        );
    }

    #[test]
    fn unzip() {
        let f = fun("array", "unzip");
        let v = Value::Array(vec![
            Value::Array(vec![Value::from("this"), Value::from("cake")]),
            Value::Array(vec![Value::from("is"), Value::from("really")]),
            Value::Array(vec![Value::from("a"), Value::from("good")]),
            Value::Array(vec![Value::from("test"), Value::from("cake")]),
        ]);
        assert_val!(
            f(&[&v]),
            OwnedValue::Array(vec![
                OwnedValue::Array(vec![
                    OwnedValue::from("this"),
                    OwnedValue::from("is"),
                    OwnedValue::from("a"),
                    OwnedValue::from("test")
                ]),
                OwnedValue::Array(vec![
                    OwnedValue::from("cake"),
                    OwnedValue::from("really"),
                    OwnedValue::from("good"),
                    OwnedValue::from("cake")
                ]),
            ])
        );
    }

    #[test]
    fn flatten() {
        let f = fun("array", "flatten");
        let v = Value::Array(vec![
            Value::Array(vec![Value::from("this"), Value::from("cake")]),
            Value::Array(vec![Value::from("is"), Value::from("really")]),
            Value::Array(vec![Value::from("a"), Value::from("good")]),
            Value::Array(vec![Value::from("test"), Value::from("cake")]),
            Value::from("!"),
        ]);
        assert_val!(
            f(&[&v]),
            OwnedValue::Array(vec![
                OwnedValue::from("this"),
                OwnedValue::from("cake"),
                OwnedValue::from("is"),
                OwnedValue::from("really"),
                OwnedValue::from("a"),
                OwnedValue::from("good"),
                OwnedValue::from("test"),
                OwnedValue::from("cake"),
                OwnedValue::from("!"),
            ])
        );
    }

    #[test]
    fn coalesce() {
        let f = fun("array", "coalesce");
        let v = Value::Array(vec![
            Value::from("this"),
            Value::Null,
            Value::from("cake"),
            Value::from("is"),
            Value::from("really"),
            Value::Null,
            Value::from("a"),
            Value::from("good"),
            Value::from("test"),
            Value::Null,
            Value::from("cake"),
            Value::from("!"),
        ]);
        assert_val!(
            f(&[&v]),
            OwnedValue::Array(vec![
                OwnedValue::from("this"),
                OwnedValue::from("cake"),
                OwnedValue::from("is"),
                OwnedValue::from("really"),
                OwnedValue::from("a"),
                OwnedValue::from("good"),
                OwnedValue::from("test"),
                OwnedValue::from("cake"),
                OwnedValue::from("!"),
            ])
        );
    }
}
