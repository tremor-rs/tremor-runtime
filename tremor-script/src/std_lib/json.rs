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
use serde_json;
use simd_json::{to_owned_value, OwnedValue};

pub fn load<Ctx: 'static + Context>(registry: &mut Registry<Ctx>) {
    registry
        .insert(tremor_fn! (json::decode(_context, _input: String) {
            // We need to clone here since we do not want to destroy the
            // original value
            let mut s: String = _input.to_string();
            println!("{}", &s);
            // Screw you rust
            let mut bytes = unsafe{s.as_bytes_mut()};
            Ok(to_owned_value(&mut bytes)?)
        }))
        .insert(tremor_fn! (json::encode(_context, _input) {
            Ok(OwnedValue::from(serde_json::to_string(_input)?))
        }))
        .insert(tremor_fn! (json::encode_pretty(_context, _input) {
            Ok(OwnedValue::from(serde_json::to_string_pretty(_input)?))
        }));
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
    fn decode() {
        let f = fun("json", "decode");
        let v = Value::from(r#"["this","is","a","cake"]"#);
        assert_val!(
            f(&[&v]),
            OwnedValue::Array(vec![
                OwnedValue::from("this"),
                OwnedValue::from("is"),
                OwnedValue::from("a"),
                OwnedValue::from("cake")
            ])
        );
    }
    #[test]
    fn encode() {
        let f = fun("json", "encode");
        let v = Value::Array(vec![
            Value::from("this"),
            Value::from("is"),
            Value::from("a"),
            Value::from("cake"),
        ]);
        assert_val!(f(&[&v]), OwnedValue::from(r#"["this","is","a","cake"]"#));
    }
    #[test]
    fn encode_pretty() {
        let f = fun("json", "encode_pretty");
        let v = Value::Array(vec![
            Value::from("this"),
            Value::from("is"),
            Value::from("a"),
            Value::from("cake"),
        ]);
        assert_val!(
            f(&[&v]),
            OwnedValue::from(
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
