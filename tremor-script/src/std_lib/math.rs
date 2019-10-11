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

// ALLOW: Until we have u64 support in clippy
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
pub fn load(registry: &mut Registry) {
    registry
        .insert(tremor_const_fn! (math::floor(_context, _input) {
            match _input {
                Value::I64(v) => Ok(Value::I64(*v)),
                Value::F64(v) => Ok(Value::I64(v.floor() as i64)),
                _ => Err(FunctionError::BadType{mfa: this_mfa()}),
            }
        }))
        .insert(tremor_const_fn! (math::ceil(_context, _input) {
            match _input {
                Value::I64(v) => Ok(Value::I64(*v)),
                Value::F64(v) => Ok(Value::I64(v.ceil() as i64)),
                _ => Err(FunctionError::BadType{mfa: this_mfa()}),
            }
        }))
        .insert(tremor_const_fn! (math::round(_context, _input) {
            match _input {
                Value::I64(v) => Ok(Value::I64(*v)),
                Value::F64(v) => Ok(Value::I64(v.round() as i64)),
                _ => Err(FunctionError::BadType{mfa: this_mfa()}),
            }
        }))
        .insert(tremor_const_fn! (math::trunc(_context, _input) {
            match _input {
                Value::I64(v) => Ok(Value::I64(*v)),
                Value::F64(v) => Ok(Value::I64(v.trunc() as i64)),
                _ => Err(FunctionError::BadType{mfa: this_mfa()}),
            }
        }))
        .insert(tremor_const_fn! (math::max(_context, a, b) {
            match (a, b) {
                (Value::F64(a), Value::F64(b)) if a > b  => Ok(Value::from(a.to_owned())),
                (Value::F64(_a), Value::F64(b)) => Ok(Value::from(b.to_owned())),
                (Value::I64(a), Value::I64(b)) if a > b  => Ok(Value::from(a.to_owned())),
                (Value::I64(_a), Value::I64(b)) => Ok(Value::from(b.to_owned())),
                (Value::F64(a), Value::I64(b)) if *a > *b as f64 => Ok(Value::from(a.to_owned())),
                (Value::F64(_a), Value::I64(b)) => Ok(Value::from(b.to_owned())),
                (Value::I64(a), Value::F64(b)) if (*a as f64) > *b => Ok(Value::from(a.to_owned())),
                (Value::I64(_a), Value::F64(b)) => Ok(Value::from(b.to_owned())),
                _ => Err(FunctionError::BadType{mfa: this_mfa()}),
            }
        }))
        .insert(tremor_const_fn!(math::min(_context, a, b) {
            match (a, b) {
                (Value::F64(a), Value::F64(b)) if a < b  => Ok(Value::from(a.to_owned())),
                (Value::F64(_a), Value::F64(b)) => Ok(Value::from(b.to_owned())),
                (Value::I64(a), Value::I64(b)) if a < b  => Ok(Value::from(a.to_owned())),
                (Value::I64(_a), Value::I64(b)) => Ok(Value::from(b.to_owned())),
                (Value::F64(a), Value::I64(b)) if *a < *b as f64 => Ok(Value::from(a.to_owned())),
                (Value::F64(_a), Value::I64(b)) => Ok(Value::from(b.to_owned())),
                (Value::I64(a), Value::F64(b)) if (*a as f64) < *b => Ok(Value::from(a.to_owned())),
                (Value::I64(_a), Value::F64(b)) => Ok(Value::from(b.to_owned())),
                _ => Err(FunctionError::BadType{mfa: this_mfa()}),
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
    fn floor() {
        let f = fun("math", "floor");
        let v = Value::from(42);
        assert_val!(f(&[&v]), 42);
        let v = Value::from(42.9);
        assert_val!(f(&[&v]), 42);
    }
    #[test]
    fn ceil() {
        let f = fun("math", "ceil");
        let v = Value::from(42);
        assert_val!(f(&[&v]), 42);
        let v = Value::from(41.1);
        assert_val!(f(&[&v]), 42);
    }

    #[test]
    fn round() {
        let f = fun("math", "round");
        let v = Value::from(42);
        assert_val!(f(&[&v]), 42);
        let v = Value::from(41.4);
        assert_val!(f(&[&v]), 41);
        let v = Value::from(41.5);
        assert_val!(f(&[&v]), 42);
    }
    #[test]
    fn trunc() {
        let f = fun("math", "trunc");
        let v = Value::from(42);
        assert_val!(f(&[&v]), 42);
        let v = Value::from(42.9);
        assert_val!(f(&[&v]), 42);
    }

    #[test]
    fn max() {
        let f = fun("math", "max");
        let v1 = Value::from(41);
        let v2 = Value::from(42);
        assert_val!(f(&[&v1, &v2]), 42);
        let v1 = Value::from(41.5);
        let v2 = Value::from(42);
        assert_val!(f(&[&v1, &v2]), 42);
    }
    #[test]
    fn min() {
        let f = fun("math", "min");
        let v1 = Value::from(42);
        let v2 = Value::from(43);
        assert_val!(f(&[&v1, &v2]), 42);
        let v1 = Value::from(42);
        let v2 = Value::from(42.5);
        assert_val!(f(&[&v1, &v2]), 42);
    }
}
