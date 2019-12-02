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
#![allow(clippy::cast_precision_loss)]

use crate::registry::Registry;
use crate::tremor_const_fn;
use simd_json::Value as ValueTrait;

// ALLOW: Until we have u64 support in clippy
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
pub fn load(registry: &mut Registry) {
    registry.insert(tremor_const_fn! (range::range(_context, a, b) {
        if let (Some(a), Some(b)) = (a.as_u64(), b.as_u64()) {
            let range: Vec<Value> = (a..b).map(|x| Value::from(x)).collect();
            Ok(Value::from(range))
        } else if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
            let range: Vec<Value> = (a..b).map(|x| Value::from(x)).collect();
            Ok(Value::from(range))
        } else {
            Err(FunctionError::BadType{mfa: this_mfa()})
        }
    }));
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
    fn range() {
        let f = fun("range", "range");
        let s = Value::from(0);
        let e = Value::from(42);
        let a: Vec<i32> = (0i32..42i32).collect();
        assert_val!(f(&[&s, &e]), a);
    }
}
