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
use crate::registry::{Context, Registry};
use crate::tremor_fn;
use simd_json::value::owned::Map;
use simd_json::OwnedValue;

pub fn load<Ctx: 'static + Context>(registry: &mut Registry<Ctx>) {
    registry
        .insert(tremor_fn! (object::len(_context, _input: Object) {
            Ok(OwnedValue::from(_input.len() as i64))
        }))
        .insert(tremor_fn! (object::is_empty(_context, _input: Object) {
            Ok(OwnedValue::from(_input.is_empty()))
        }))
        .insert(
            tremor_fn! (object::contains(_context, _input: Object, _contains: String) {
                Ok(OwnedValue::from(_input.get(_contains).is_some()))
            }),
        )
        .insert(tremor_fn! (object::keys(_context, _input: Object) {
            Ok(OwnedValue::Array(_input.keys().map(|k| OwnedValue::from(k.to_string())).collect()))
        }))
        .insert(tremor_fn! (object::values(_context, _input: Object) {
            Ok(OwnedValue::Array(_input.values().cloned().map(OwnedValue::from).collect()))

        }))
        .insert(tremor_fn! (object::to_array(_context, _input: Object) {
            Ok(OwnedValue::Array(
                _input.into_iter()
                    .map(|(k, v)| OwnedValue::Array(vec![OwnedValue::from(k.to_string()), v.clone().into()]))
                    .collect(),
            ))
        }))
    .insert(tremor_fn! (object::from_array(_context, _input: Array) {
        let r: Result<Map> = _input.iter().map(|a| match a {
            BorrowedValue::Array(a) => if a.len() == 2 {
                let mut a = a.clone(); // TODO: this is silly.
                let second = a.pop().expect("We know this has an element");
                if let BorrowedValue::String(first) = a.pop().expect("We know this has an ellement") {
                    Ok((first.to_string(), OwnedValue::from(second)))
                } else {
                    Err(ErrorKind::RuntimeError("object".to_owned(), "from_array". to_owned(), 1, format!("The first element of the tuple needs to be a string: {:?}", a)).into())
                }
            } else {
                Err(ErrorKind::RuntimeError("object".to_owned(), "from_array". to_owned(), 1, format!("Onlay arrays that consist of tuples (arrays of two elements) can be trurned into an object this array contained {} elements", a.len())).into())
            }
            other => Err(ErrorKind::RuntimeError("object".to_owned(), "from_array". to_owned(), 1, format!("Onlay arrays that consist of tuples (arrays of two elements) can be turned into objects but this array contained: {:?}", other)).into())
        }).collect();
        Ok(OwnedValue::Object(r?))
    })).insert(tremor_fn!(object::select(_context, _input: Object, _keys: Array) {
        let keys: Vec<_> = _keys.iter().filter_map(|k| match k {
            BorrowedValue::String(s) => Some(s.clone()),
            _ => None
                
        }).collect();
        let r: Map =_input.iter().filter_map(|(k, v)| {
            let k = k.to_owned();
            if keys.contains(&k) {
                Some((k.to_string(), OwnedValue::from(v.clone())))
            } else {
                None
            }
        }).collect();
        Ok(OwnedValue::Object(r))
    })).insert(tremor_fn!(object::merge(_context, _left: Object, _right: Object) {
        Ok(OwnedValue::Object(_left.iter().chain(_right.iter()).map(|(k, v)| (k.to_string(), OwnedValue::from(v.clone()))).collect()))
    }));
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use halfbrown::hashmap;
    use simd_json::{BorrowedValue as Value, OwnedValue};
    macro_rules! assert_val {
        ($e:expr, $r:expr) => {
            assert_eq!($e, Ok(OwnedValue::from($r)))
        };
    }

    #[test]
    fn len() {
        let f = fun("object", "len");
        let v = Value::Object(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        assert_val!(f(&[&v]), 2);
        let v = Value::Object(hashmap! {});
        assert_val!(f(&[&v]), 0);
    }

    #[test]
    fn is_empty() {
        let f = fun("object", "is_empty");
        let v = Value::Object(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        assert_val!(f(&[&v]), false);
        let v = Value::Object(hashmap! {});
        assert_val!(f(&[&v]), true);
    }

    #[test]
    fn contains() {
        let f = fun("object", "contains");
        let v1 = Value::Object(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        let v2 = Value::from("this");
        assert_val!(f(&[&v1, &v2]), true);
        let v1 = Value::Object(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        let v2 = Value::from("that");
        assert_val!(f(&[&v1, &v2]), false);
    }

    #[test]
    fn keys() {
        let f = fun("object", "keys");
        let v = Value::Object(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        assert_val!(
            f(&[&v]),
            OwnedValue::Array(vec![OwnedValue::from("this"), OwnedValue::from("a"),])
        );
    }
    #[test]
    fn values() {
        let f = fun("object", "values");
        let v = Value::Object(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        assert_val!(
            f(&[&v]),
            OwnedValue::Array(vec![OwnedValue::from("is"), OwnedValue::from("test")])
        );
    }

    #[test]
    fn to_array() {
        let f = fun("object", "to_array");
        let v = Value::Object(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        assert_val!(
            f(&[&v]),
            OwnedValue::Array(vec![
                OwnedValue::Array(vec![OwnedValue::from("this"), OwnedValue::from("is")]),
                OwnedValue::Array(vec![OwnedValue::from("a"), OwnedValue::from("test")])
            ])
        );
    }

    #[test]
    fn from_array() {
        let f = fun("object", "from_array");
        let v = Value::Array(vec![
            Value::Array(vec![Value::from("this"), Value::from("is")]),
            Value::Array(vec![Value::from("a"), Value::from("test")]),
        ]);
        assert_val!(
            f(&[&v]),
            OwnedValue::Object(hashmap! {
                "this".to_string() => OwnedValue::from("is"),
                "a".to_string() => OwnedValue::from("test")
            })
        );
    }

    #[test]
    fn select() {
        let f = fun("object", "select");
        let v1 = Value::Object(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        let v2 = Value::Array(vec![Value::from("this"), Value::from("is")]);
        assert_val!(
            f(&[&v1, &v2]),
            OwnedValue::Object(hashmap! {
                "this".to_string() => OwnedValue::from("is"),
            })
        );
    }
    #[test]
    fn merge() {
        let f = fun("object", "merge");
        let v1 = Value::Object(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        let v2 = Value::Object(hashmap! {
            "with".into() => Value::from("cake"),
            "a".into() => Value::from("cake")
        });
        assert_val!(
            f(&[&v1, &v2]),
            OwnedValue::Object(hashmap! {
                "this".to_string() => OwnedValue::from("is"),
                "a".to_string() => OwnedValue::from("cake"),
                "with".to_string() => OwnedValue::from("cake"),
            })
        );
    }
}
