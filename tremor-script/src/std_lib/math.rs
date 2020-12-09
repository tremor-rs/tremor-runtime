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
#![allow(clippy::cast_precision_loss)]

use crate::prelude::*;
use crate::registry::Registry;
use crate::tremor_const_fn;
use std::cmp::{max, min};

macro_rules! math_fn {
    ($name:ident) => {
        tremor_const_fn! (math::$name(_context, _input) {
            if let Some(v) = _input.as_u64() {
                Ok(Value::from(v))
            } else if let Some(v) = _input.as_i64() {
                Ok(Value::from(v))
            } else if let Some(v) = _input.cast_f64() {
                let f = v.$name();
                if f < 0.0 {
                    Ok(Value::from(f as i64))
                } else {
                    Ok(Value::from(f as u64))

                }
            } else{
                Err(FunctionError::BadType{mfa: this_mfa()})
            }
        })
    };
}
// ALLOW: Until we have u64 support in clippy
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
pub fn load(registry: &mut Registry) {
    registry
        .insert(math_fn!(floor))
        .insert(math_fn!(ceil))
        .insert(math_fn!(round))
        .insert(math_fn!(trunc))
        .insert(tremor_const_fn! (math::max(_context, a, b) {
            if let (Some(a), Some(b)) = (a.as_u64(), b.as_u64()) {
                Ok(Value::from(max(a, b)))
            } else if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
                Ok(Value::from(max(a, b)))
            } else if let (Some(a), Some(b)) = (a.cast_f64(), b.cast_f64()) {
                if a >= b {
                    Ok(Value::from(a))
                } else {
                    Ok(Value::from(b))
                }
            } else {
                Err(FunctionError::BadType{mfa: this_mfa()})
            }
        }))
        .insert(tremor_const_fn! (math::min(_context, a, b) {
            if let (Some(a), Some(b)) = (a.as_u64(), b.as_u64()) {
                Ok(Value::from(min(a, b)))
            } else if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
                Ok(Value::from(min(a, b)))
            } else if let (Some(a), Some(b)) = (a.cast_f64(), b.cast_f64()) {
                if a <= b {
                    Ok(Value::from(a))
                } else {
                    Ok(Value::from(b))
                }
            } else {
                Err(FunctionError::BadType{mfa: this_mfa()})
            }
        }));
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use crate::Value;

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
        assert_val!(f(&[&v1, &v2]), 42.0);
    }
    #[test]
    fn min() {
        let f = fun("math", "min");
        let v1 = Value::from(42);
        let v2 = Value::from(43);
        assert_val!(f(&[&v1, &v2]), 42);
        let v1 = Value::from(42);
        let v2 = Value::from(42.5);
        assert_val!(f(&[&v1, &v2]), 42.0);
    }
}
