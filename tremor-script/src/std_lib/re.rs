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

use crate::registry::Registry;
use crate::tremor_const_fn;
use regex::Regex;

pub fn load(registry: &mut Registry) {
    registry
        .insert(
            tremor_const_fn! (re::replace(_context, _re: String, _input: String, _to: String) {
                let re = Regex::new(_re).map_err(to_runtime_error)?;
                let input: &str = _input;
                let to: &str = _to;
                Ok(Value::from(re.replace(input, to).to_string()))
            }),
        )
        .insert(
            tremor_const_fn! (re::replace_all(_context, _re: String, _input: String, _to: String) {
                let re = Regex::new(_re).map_err(to_runtime_error)?;
                let input: &str = _input;
                let to: &str = _to;
                Ok(Value::from(re.replace_all(input, to).to_string()))
            }),
        )
        .insert(
            tremor_const_fn! (re::is_match(_context, _re: String, _input: String) {
                let re = Regex::new(_re).map_err(to_runtime_error)?;
                let input: &str = _input;
                Ok(Value::from(re.is_match(input)))
            }),
        )
        .insert(
            tremor_const_fn! (re::split(_context, _re: String, _input: String) {
                let re = Regex::new(_re).map_err(to_runtime_error)?;
                let input: &str = _input;
                let res: Vec<Value> = re.split(input).map(|v| Value::from(v.to_string())).collect();
                Ok(Value::from(res))
            }),
        );
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
        let f = fun("re", "replace");
        let v1 = Value::from("t...");
        let v2 = Value::from("this is a test");
        let v3 = Value::from("cake");

        assert_val!(f(&[&v1, &v2, &v3]), "cake is a test")
    }

    #[test]
    fn replace_all() {
        let f = fun("re", "replace_all");
        let v1 = Value::from("t...");
        let v2 = Value::from("this is a test");
        let v3 = Value::from("cake");
        assert_val!(f(&[&v1, &v2, &v3]), "cake is a cake")
    }

    #[test]
    fn is_match() {
        let f = fun("re", "is_match");
        let v1 = Value::from("t...");
        let v2 = Value::from("this is a test");
        assert_val!(f(&[&v1, &v2]), true);
        let v1 = Value::from("x...");
        let v2 = Value::from("this is a test");
        assert_val!(f(&[&v1, &v2]), false);
    }
    #[test]
    fn split() {
        let f = fun("re", "split");
        let v1 = Value::from(" ");
        let v2 = Value::from("this is a test");
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
