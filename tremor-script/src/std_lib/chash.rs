// Copyright 2020, The Tremor Team
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
#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]

use crate::registry::Registry;
use crate::{tremor_const_fn, utils::sorted_serialize};
use simd_json::prelude::*;

pub fn load(registry: &mut Registry) {
    registry.insert(
        tremor_const_fn! (chash::jump(_context, _key, _slot_count) {
            if let (Some(key), Some(slot_count)) =  (_key.as_str(), _slot_count.as_u32()) {
                // This is 'tremor\0\0'  and '\0\0tremor' as integers
                let jh = jumphash::JumpHasher::new_with_keys(8_390_880_576_440_238_080, 128_034_676_764_530);
                Ok(jh.slot(&key, slot_count).into())
            } else {
                 Err(FunctionError::BadType{mfa: this_mfa()})
            }
        }),
    ).insert(
        tremor_const_fn!(chash::jump_with_keys(_context, _k1, _k2, _key, _slot_count) {
            if let (Some(k1), Some(k2), Some(key), Some(slot_count)) =  (_k1.as_u64(), _k2.as_u64(), _key.as_str(), _slot_count.as_u32()) {
                let jh = jumphash::JumpHasher::new_with_keys(k1, k2);
                Ok(jh.slot(&key, slot_count).into())
            } else {
                 Err(FunctionError::BadType{mfa: this_mfa()})
            }
        }),
    ).insert(
        tremor_const_fn!(chash::sorted_serialize(_context, _data) {
            let ser = sorted_serialize(_data).map_err(|_| FunctionError::RuntimeError{mfa: this_mfa(), error: "Failed to serialize".to_string()})?;
            Ok(Value::from(ser))
        }),
    );
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use simd_json::{json, BorrowedValue as Value};

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
