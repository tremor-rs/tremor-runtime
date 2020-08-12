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
use simd_json::to_owned_value;

pub fn load(registry: &mut Registry) {
    registry
        .insert(tremor_const_fn! (json::decode(_context, _input: String) {
            // We need to clone here since we do not want to destroy the
            // original value
            let s: String = _input.to_string();
            // Screw you rust
            let mut bytes = s.into_bytes();
            // We need to do this since otherwise we depend on the clone of s
            to_owned_value(bytes.as_mut_slice()).map_err(to_runtime_error).map(Value::from)
        }))
        .insert(tremor_const_fn! (json::encode(_context, _input) {
            simd_json::to_string(_input).map(Value::from).map_err(to_runtime_error)
        }))
        .insert(tremor_const_fn! (json::encode_pretty(_context, _input) {
            simd_json::to_string_pretty(_input).map(Value::from).map_err(to_runtime_error)
        }));
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use simd_json::BorrowedValue as Value;

    #[test]
    fn decode() {
        let f = fun("json", "decode");
        let v = Value::from(r#"["this","is","a","cake"]"#);
        assert_val!(f(&[&v]), Value::from(vec!["this", "is", "a", "cake"]));
    }
    #[test]
    fn encode() {
        let f = fun("json", "encode");
        let v = Value::from(vec!["this", "is", "a", "cake"]);
        assert_val!(f(&[&v]), Value::from(r#"["this","is","a","cake"]"#));
    }
    #[test]
    fn encode_pretty() {
        let f = fun("json", "encode_pretty");
        let v = Value::from(vec!["this", "is", "a", "cake"]);
        assert_val!(
            f(&[&v]),
            Value::from(
                r#"[
  "this",
  "is",
  "a",
  "cake"
]"#
            )
        );
    }
}
