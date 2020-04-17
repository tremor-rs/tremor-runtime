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
use simd_json::value::borrowed::Object;

pub fn load(registry: &mut Registry) {
    registry
        .insert(tremor_const_fn! (record::len(_context, _input: Object) {
            Ok(Value::from(_input.len() as i64))
        }))
        .insert(tremor_const_fn! (record::is_empty(_context, _input: Object) {
            Ok(Value::from(_input.is_empty()))
        }))
        .insert(
            tremor_const_fn! (record::contains(_context, _input: Object, _contains: String) {
                Ok(Value::from(_input.get(_contains).is_some()))
            }),
        )
        .insert(tremor_const_fn! (record::keys(_context, _input: Object) {
            Ok(Value::Array(_input.keys().map(|k| Value::from(k.to_string())).collect()))
        }))
        .insert(tremor_const_fn! (record::values(_context, _input: Object) {
            Ok(Value::Array(_input.values().cloned().map(Value::from).collect()))

        }))
        .insert(tremor_const_fn! (record::to_array(_context, _input: Object) {
            Ok(Value::Array(
                _input.iter()
                    .map(|(k, v)| Value::Array(vec![Value::from(k.clone()), v.clone()]))
                    .collect(),
            ))
        }))
        .insert(tremor_const_fn! (record::from_array(_context, _input: Array) {
        #[allow(clippy::option_unwrap_used)]
        let r: FResult<Object> = _input.iter().map(|a| match a {
            Value::Array(a) => if a.len() == 2 {
                let mut a = a.clone(); // TODO: this is silly.
                //ALLOW: We know this has an element
                let second = a.pop().unwrap();
                //ALLOW: We know this has an element
                if let Value::String(first) = a.pop().unwrap() {
                    Ok((first, second))
                } else {
                    Err(to_runtime_error(format!("The first element of the tuple needs to be a string: {:?}", a)))
                }
            } else {
                Err(to_runtime_error( format!("Onlay arrays that consist of tuples (arrays of two elements) can be trurned into an record this array contained {} elements", a.len())))
            }
            other => Err(to_runtime_error(format!("Onlay arrays that consist of tuples (arrays of two elements) can be turned into records but this array contained: {:?}", other)))
        }).collect();
        Ok(Value::from(r?))
        })).insert(tremor_const_fn!(record::select(_context, _input: Object, _keys: Array) {
        let keys: Vec<_> = _keys.iter().filter_map(|k| match k {
            Value::String(s) => Some(s.clone()),
            _ => None
        }).collect();
        let r: Object =_input.iter().filter_map(|(k, v)| {
            if keys.contains(&k) {
                Some((k.clone(), v.clone()))
            } else {
                None
            }
        }).collect();
        Ok(Value::from(r))
    }))
        .insert(tremor_const_fn!(record::merge(_context, _left: Object, _right: Object) {
        Ok(Value::from(_left.iter().chain(_right.iter()).map(|(k, v)| (k.clone(), v.clone())).collect::<Object>()))
        })).insert(tremor_const_fn!(record::rename(_context, _target: Object, _renameings: Object) {
            Ok(Value::from(_target.iter().map(|(k, v)| if let Some(Value::String(k1)) = _renameings.get(k) {
                (k1.clone(), v.clone())
            } else {
                (k.clone(), v.clone())
            }).collect::<Object>()))
        }));
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use halfbrown::hashmap;
    use simd_json::BorrowedValue as Value;
    macro_rules! assert_val {
        ($e:expr, $r:expr) => {
            assert_eq!($e, Ok(Value::from($r)))
        };
    }

    #[test]
    fn len() {
        let f = fun("record", "len");
        let v = Value::from(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        assert_val!(f(&[&v]), 2);
        let v = Value::from(hashmap! {});
        assert_val!(f(&[&v]), 0);
    }

    #[test]
    fn is_empty() {
        let f = fun("record", "is_empty");
        let v = Value::from(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        assert_val!(f(&[&v]), false);
        let v = Value::from(hashmap! {});
        assert_val!(f(&[&v]), true);
    }

    #[test]
    fn contains() {
        let f = fun("record", "contains");
        let v1 = Value::from(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        let v2 = Value::from("this");
        assert_val!(f(&[&v1, &v2]), true);
        let v1 = Value::from(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        let v2 = Value::from("that");
        assert_val!(f(&[&v1, &v2]), false);
    }

    #[test]
    fn keys() {
        let f = fun("record", "keys");
        let v = Value::from(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        assert_val!(
            f(&[&v]),
            Value::Array(vec![Value::from("this"), Value::from("a"),])
        );
    }
    #[test]
    fn values() {
        let f = fun("record", "values");
        let v = Value::from(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        assert_val!(
            f(&[&v]),
            Value::Array(vec![Value::from("is"), Value::from("test")])
        );
    }

    #[test]
    fn to_array() {
        let f = fun("record", "to_array");
        let v = Value::from(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        assert_val!(
            f(&[&v]),
            Value::Array(vec![
                Value::Array(vec![Value::from("this"), Value::from("is")]),
                Value::Array(vec![Value::from("a"), Value::from("test")])
            ])
        );
    }

    #[test]
    fn from_array() {
        let f = fun("record", "from_array");
        let v = Value::Array(vec![
            Value::Array(vec![Value::from("this"), Value::from("is")]),
            Value::Array(vec![Value::from("a"), Value::from("test")]),
        ]);
        assert_val!(
            f(&[&v]),
            Value::from(hashmap! {
                "this".into() => Value::from("is"),
                "a".into() => Value::from("test")
            })
        );
    }

    #[test]
    fn select() {
        let f = fun("record", "select");
        let v1 = Value::from(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        let v2 = Value::Array(vec![Value::from("this"), Value::from("is")]);
        assert_val!(
            f(&[&v1, &v2]),
            Value::from(hashmap! {
                "this".into() => Value::from("is"),
            })
        );
    }
    #[test]
    fn merge() {
        let f = fun("record", "merge");
        let v1 = Value::from(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        let v2 = Value::from(hashmap! {
            "with".into() => Value::from("cake"),
            "a".into() => Value::from("cake")
        });
        assert_val!(
            f(&[&v1, &v2]),
            Value::from(hashmap! {
                "this".into() => Value::from("is"),
                "a".into() => Value::from("cake"),
                "with".into() => Value::from("cake"),
            })
        );
    }
}
