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

//cuse crate::errors::*;
use crate::registry::{Context, Registry};
use crate::tremor_fn;
use simd_json::{OwnedValue, ValueTrait, ValueType};

macro_rules! map_function {
    ($name:ident, $fun:ident) => {
        tremor_fn! (type::$name(_context, _input) {
            Ok(OwnedValue::from(_input.$fun()))
        })
    };
        ($fun:ident) => {
            tremor_fn!(type::$fun(_context, _input) {
                Ok(OwnedValue::from(_input.$fun()))
            })
        }
    }

pub fn load<Ctx: 'static + Context>(registry: &mut Registry<Ctx>) {
    registry
        .insert(map_function!(is_null))
        .insert(map_function!(is_bool))
        .insert(map_function!(is_integer, is_i64))
        .insert(map_function!(is_float, is_f64))
        .insert(map_function!(is_string))
        .insert(map_function!(is_array))
        .insert(map_function!(is_record, is_object))
        .insert(tremor_fn! (type::as_string(_context, _input) {
            Ok(match _input.kind() {
                ValueType::Null => OwnedValue::from("null"),
                ValueType::Bool => OwnedValue::from("bool"),
                ValueType::I64 => OwnedValue::from("number"),
                ValueType::F64 => OwnedValue::from("number"),
                ValueType::String => OwnedValue::from("string"),
                ValueType::Array => OwnedValue::from("array"),
                ValueType::Object => OwnedValue::from("record"),
            })
        }))
        .insert(tremor_fn! (type::is_number(_context, _input) {
            Ok(match _input.kind() {
                ValueType::I64 => OwnedValue::from(true),
                ValueType::F64 => OwnedValue::from(true),
                _ => OwnedValue::from(false),
            })
        }));
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use simd_json::value::borrowed::Map;
    use simd_json::{BorrowedValue as Value, OwnedValue};

    macro_rules! assert_val {
        ($e:expr, $r:expr) => {
            assert_eq!($e, Ok(OwnedValue::from($r)))
        };
    }

    #[test]
    fn is_null() {
        let f = fun("type", "is_null");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), false);
        assert_val!(f(&[&Value::Null]), true)
    }

    #[test]
    fn is_bool() {
        let f = fun("type", "is_bool");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), false);
        let v = Value::Bool(true);
        assert_val!(f(&[&v]), true)
    }

    #[test]
    fn is_integer() {
        let f = fun("type", "is_integer");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), false);
        let v = Value::from(42);
        assert_val!(f(&[&v]), true)
    }

    #[test]
    fn is_float() {
        let f = fun("type", "is_float");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), false);
        let v = Value::from(42.0);
        assert_val!(f(&[&v]), true)
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
        let v = Value::Object(Map::new());
        assert_val!(f(&[&v]), true);
    }

    #[test]
    fn as_string() {
        let f = fun("type", "as_string");
        let v = Value::Null;
        assert_val!(f(&[&v]), "null");
        let v = Value::Bool(true);
        assert_val!(f(&[&v]), "bool");
        let v = Value::from(42);
        assert_val!(f(&[&v]), "number");
        let v = Value::from(42.0);
        assert_val!(f(&[&v]), "number");
        let v = Value::from("this is a test");
        assert_val!(f(&[&v]), "string");
        let v = Value::Array(vec![]);
        assert_val!(f(&[&v]), "array");
        let v = Value::Object(Map::new());
        assert_val!(f(&[&v]), "record");
    }

}
