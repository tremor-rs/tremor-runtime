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

use crate::registry::Registry;
use crate::{errors, prelude::*};
use crate::{tremor_const_fn, tremor_fn_};

macro_rules! map_function {
    ($name:ident, $fun:ident) => {
        tremor_const_fn! (type|$name(_context, _input) {
            Ok(Value::from(_input.$fun()))
        })
    };
        ($fun:ident) => {
            tremor_const_fn!(type|$fun(_context, _input) {
                Ok(Value::from(_input.$fun()))
            })
        }
    }

pub fn load(registry: &mut Registry) {
    registry
        .insert(map_function!(is_null))
        .insert(map_function!(is_bool))
        .insert(map_function!(is_integer, is_i64))
        .insert(map_function!(is_float, is_f64))
        .insert(map_function!(is_string, is_str))
        .insert(map_function!(is_array))
        .insert(map_function!(is_record, is_object))
        .insert(tremor_const_fn! (type|as_string(_context, _input) {
            Ok(errors::t2s(_input.value_type()).into())
        }))
        .insert(tremor_const_fn! (type|is_number(_context, _input) {
            Ok(match _input.value_type() {
                ValueType::I64 | ValueType::F64 | ValueType::U64 => Value::from(true),
                _ => Value::from(false),
            })
        }))
        .insert(tremor_const_fn! (type|is_binary(_context, _input) {
            Ok(match _input.value_type() {
                ValueType::Custom("bytes") => Value::from(true),
                _ => Value::from(false),
            })
        }));
}

#[cfg(test)]
mod test {
    use crate::prelude::*;
    use crate::registry::fun;
    use crate::Value;

    #[test]
    fn is_null() {
        let f = fun("type", "is_null");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), false);
        assert_val!(f(&[&Value::null()]), true);
    }

    #[test]
    fn is_bool() {
        let f = fun("type", "is_bool");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), false);
        let v = Value::from(true);
        assert_val!(f(&[&v]), true);
    }

    #[test]
    fn is_integer() {
        let f = fun("type", "is_integer");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), false);
        let v = Value::from(42);
        assert_val!(f(&[&v]), true);
    }

    #[test]
    fn is_float() {
        let f = fun("type", "is_float");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), false);
        let v = Value::from(42.0);
        assert_val!(f(&[&v]), true);
    }

    #[test]
    fn is_number() {
        let f = fun("type", "is_number");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), false);
        let v = Value::from(42);
        assert_val!(f(&[&v]), true);
        let v = Value::from(42.0);
        assert_val!(f(&[&v]), true);
    }

    #[test]
    fn is_string() {
        let f = fun("type", "is_string");
        let v = Value::from(42);
        assert_val!(f(&[&v]), false);
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), true);
    }

    #[test]
    fn is_array() {
        let f = fun("type", "is_array");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), false);
        let v = Value::Array(vec![]);
        assert_val!(f(&[&v]), true);
    }

    #[test]
    fn is_record() {
        let f = fun("type", "is_record");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), false);
        let v = Value::object();
        assert_val!(f(&[&v]), true);
    }

    #[test]
    fn as_string() {
        let f = fun("type", "as_string");
        let v = Value::null();
        assert_val!(f(&[&v]), "null");
        let v = Value::from(true);
        assert_val!(f(&[&v]), "bool");
        let v = Value::from(42);
        assert_val!(f(&[&v]), "integer");
        let v = Value::from(42.0);
        assert_val!(f(&[&v]), "float");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), "string");
        let v = Value::Array(vec![]);
        assert_val!(f(&[&v]), "array");
        let v = Value::object();
        assert_val!(f(&[&v]), "record");
    }
}
