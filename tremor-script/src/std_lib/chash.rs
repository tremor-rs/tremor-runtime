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
use jumphash;
use simd_json::BorrowedValue as Value;
use std::io;
use std::io::Write;

pub fn load<Ctx: 'static + Context>(registry: &mut Registry<Ctx>) {
    registry.insert(
        tremor_fn! (chash::jump(_context, _key: String, _slot_count: I64) {
            // This is 'tremor\0\0'  and '\0\0tremor' as integers
            let jh = jumphash::JumpHasher::new_with_keys(8_390_880_576_440_238_080, 128_034_676_764_530);
            Ok(jh.slot(&_key, *_slot_count as u32).into())
        }),
    ).insert(
        tremor_fn!(chash::jump_with_keys(_context, _k1: I64, _k2: I64, _key: String, _slot_count: I64) {
            let jh = jumphash::JumpHasher::new_with_keys(*_k1 as u64, *_k2 as u64);
            Ok(jh.slot(&_key, *_slot_count as u32).into())
        }),
    ).insert(
        tremor_fn!(chash::sorted_serialize(_context, _data) {
            let mut d: Vec<u8> = Vec::new();
            sorted_serialize_(_data, &mut d).map_err(|_| FunctionError::RuntimeError{mfa: this_mfa(), error: "Failed to serialize".to_string()})?;
            Ok(Value::String(String::from_utf8(d).map_err(|_| FunctionError::RuntimeError{mfa: this_mfa(), error: "Encountered invalid UTF8 in serialisation".to_string()})?))
        }),
    );
}

fn sorted_serialize_<'v, W: Write>(j: &Value<'v>, w: &mut W) -> io::Result<()> {
    match j {
        Value::Null | Value::Bool(_) | Value::I64(_) | Value::F64(_) | Value::String(_) => {
            write!(w, "{}", j.to_string())
        }
        Value::Array(a) => {
            let mut iter = a.iter();
            write!(w, "[")?;

            if let Some(e) = iter.next() {
                sorted_serialize_(e, w)?
            }

            for e in iter {
                write!(w, ",")?;
                sorted_serialize_(e, w)?
            }
            write!(w, "]")
        }
        Value::Object(o) => {
            let mut v: Vec<(String, Value<'v>)> =
                o.iter().map(|(k, v)| (k.to_string(), v.clone())).collect();

            v.sort_by_key(|(k, _)| k.to_string());
            let mut iter = v.into_iter();

            write!(w, "{{")?;

            if let Some((k, v)) = iter.next() {
                sorted_serialize_(&Value::from(k), w)?;
                write!(w, ":")?;
                sorted_serialize_(&v, w)?;
            }

            for (k, v) in iter {
                write!(w, ",")?;
                sorted_serialize_(&Value::from(k), w)?;
                write!(w, ":")?;
                sorted_serialize_(&v, w)?;
            }
            write!(w, "}}")
        }
    }
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use simd_json::{json, BorrowedValue as Value, OwnedValue};
    macro_rules! assert_val {
        ($e:expr, $r:expr) => {
            assert_eq!($e, Ok(OwnedValue::from($r)))
        };
    }

    #[test]
    fn sorted_serialize() {
        let f = fun("chash", "sorted_serialize");
        let v1: Value = json!({
            "1": [2, 1],
            "0": {
                "5": 0,
                "3": 4
            }
        })
        .into();

        assert_val!(f(&[&v1]), r#"{"0":{"3":4,"5":0},"1":[2,1]}"#);
        let v1: Value = json!({
            "0": {
                "3": 4,
                "5": 0,
            },
            "1": [2, 1],
        })
        .into();
        assert_val!(f(&[&v1]), r#"{"0":{"3":4,"5":0},"1":[2,1]}"#)
    }

}
