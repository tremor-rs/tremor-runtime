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
use crate::registry::Registry;
use crate::tremor_const_fn;
use crate::Object;
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
            Ok(Value::from(_input.keys().map(|k| Value::from(k.to_string())).collect::<Vec<_>>()))
        }))
        .insert(tremor_const_fn! (record::values(_context, _input: Object) {
            Ok(Value::from(_input.values().cloned().map(Value::from).collect::<Vec<_>>()))

        }))
        .insert(tremor_const_fn! (record::to_array(_context, _input: Object) {
            Ok(Value::from(
                _input.iter()
                    .map(|(k, v)| Value::from(vec![Value::from(k.clone()), v.clone()]))
                    .collect::<Vec<_>>(),
            ))
        }))
        .insert(tremor_const_fn! (record::from_array(_context, _input: Array) {
        let r: FResult<Object> = _input.iter().map(|a| match a {
            Value::Array(a) => if a.len() == 2 {
                let mut a = a.clone(); // TODO: this is silly.
                //ALLOW: We know this has an element
                let second = a.pop().expect("this can never happen");
                if let Some(Value::String(first)) = a.pop() {
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
        let keys: Vec<_> = _keys.iter().filter_map(ValueAccess::as_str).collect();
        let r: Object =_input.iter().filter_map(|(k, v)| {
            let k: &str = &k;
            if keys.contains(&k) {
                Some((k.to_string().into(), v.clone()))
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
    use crate::Value;
    use halfbrown::hashmap;

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
        assert_val!(f(&[&v]), Value::from(vec!["this", "a"]));
    }
    #[test]
    fn values() {
        let f = fun("record", "values");
        let v = Value::from(hashmap! {
            "this".into() => Value::from("is"),
            "a".into() => Value::from("test")
        });
        assert_val!(f(&[&v]), Value::from(vec!["is", "test"]));
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
            Value::from(vec![vec!["this", "is"], vec!["a", "test"]])
        );
    }

    #[test]
    fn from_array() {
        let f = fun("record", "from_array");
        let v = Value::from(vec![vec!["this", "is"], vec!["a", "test"]]);
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
        let v2 = Value::from(vec!["this", "is"]);
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
