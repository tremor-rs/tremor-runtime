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
use crate::tremor_const_fn;
use value_trait::prelude::ValueTryAsScalar;

pub fn load(registry: &mut Registry) {
    registry
        .insert(tremor_const_fn! (float|parse(_context, _input: String) {
            _input.parse::<f64>().map_err(to_runtime_error).map(Value::from)
        }))
        .insert(tremor_const_fn!(float|is_finite(_context, _input) {
            _input.try_cast_f64().map(f64::is_finite).map(Value::from).map_err(to_runtime_error)
        }))
        .insert(tremor_const_fn!(float|is_nan(_context, _input) {
            _input.try_cast_f64().map(f64::is_nan).map(Value::from).map_err(to_runtime_error)
        }))
        .insert(tremor_const_fn!(float|is_infinite(_context, _input) {
            _input.try_cast_f64().map(f64::is_infinite).map(Value::from).map_err(to_runtime_error)
        }));
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use crate::Value;

    #[test]
    fn parse() {
        let f = fun("float", "parse");
        let v = Value::from("42.314");
        assert_val!(f(&[&v]), 42.314);
    }

    #[test]
    fn is_finite() {
        let f = fun("float", "is_finite");
        let v = Value::from(f64::NAN);
        assert_val!(f(&[&v]), false);
        let v = Value::from(f64::INFINITY);
        assert_val!(f(&[&v]), false);
        let v = Value::from(7);
        assert_val!(f(&[&v]), true);
        let v = Value::from(4.2);
        assert_val!(f(&[&v]), true);
    }
    #[test]
    fn is_nan() {
        let f = fun("float", "is_nan");
        let v = Value::from(f64::NAN);
        assert_val!(f(&[&v]), true);
        let v = Value::from(f64::INFINITY);
        assert_val!(f(&[&v]), false);
        let v = Value::from(7);
        assert_val!(f(&[&v]), false);
        let v = Value::from(4.2);
        assert_val!(f(&[&v]), false);
    }
    #[test]
    fn is_infinite() {
        let f = fun("float", "is_infinite");
        let v = Value::from(f64::NAN);
        assert_val!(f(&[&v]), false);
        let v = Value::from(f64::INFINITY);
        assert_val!(f(&[&v]), true);
        let v = Value::from(7);
        assert_val!(f(&[&v]), false);
        let v = Value::from(4.2);
        assert_val!(f(&[&v]), false);
    }
}
