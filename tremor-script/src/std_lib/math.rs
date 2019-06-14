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
use simd_json::OwnedValue;

pub fn load<Ctx: 'static + Context>(registry: &mut Registry<Ctx>) {
    registry
        .insert(tremor_fn! (math::floor(_context, _input) {
            match _input {
                BorrowedValue::I64(v) => Ok(OwnedValue::I64(*v)),
                BorrowedValue::F64(v) => Ok(OwnedValue::I64(v.floor() as i64)),
                _ => Err(FunctionError::BadType{mfa: this_mfa()}),
            }
        }))
        .insert(tremor_fn! (math::ceil(_context, _input) {
            match _input {
                BorrowedValue::I64(v) => Ok(OwnedValue::I64(*v)),
                BorrowedValue::F64(v) => Ok(OwnedValue::I64(v.ceil() as i64)),
                _ => Err(FunctionError::BadType{mfa: this_mfa()}),
            }
        }))
        .insert(tremor_fn! (math::round(_context, _input) {
            match _input {
                BorrowedValue::I64(v) => Ok(OwnedValue::I64(*v)),
                BorrowedValue::F64(v) => Ok(OwnedValue::I64(v.round() as i64)),
                _ => Err(FunctionError::BadType{mfa: this_mfa()}),
            }
        }))
        .insert(tremor_fn! (math::trunc(_context, _input) {
            match _input {
                BorrowedValue::I64(v) => Ok(OwnedValue::I64(*v)),
                BorrowedValue::F64(v) => Ok(OwnedValue::I64(v.trunc() as i64)),
                _ => Err(FunctionError::BadType{mfa: this_mfa()}),
            }
        }))
        .insert(tremor_fn! (math::max(_context, a, b) {
            match (a, b) {
                (BorrowedValue::F64(a), BorrowedValue::F64(b)) if a > b  => Ok(OwnedValue::from(a.to_owned())),
                (BorrowedValue::F64(_a), BorrowedValue::F64(b)) => Ok(OwnedValue::from(b.to_owned())),
                (BorrowedValue::I64(a), BorrowedValue::I64(b)) if a > b  => Ok(OwnedValue::from(a.to_owned())),
                (BorrowedValue::I64(_a), BorrowedValue::I64(b)) => Ok(OwnedValue::from(b.to_owned())),
                (BorrowedValue::F64(a), BorrowedValue::I64(b)) if *a > *b as f64 => Ok(OwnedValue::from(a.to_owned())),
                (BorrowedValue::F64(_a), BorrowedValue::I64(b)) => Ok(OwnedValue::from(b.to_owned())),
                (BorrowedValue::I64(a), BorrowedValue::F64(b)) if (*a as f64) > *b => Ok(OwnedValue::from(a.to_owned())),
                (BorrowedValue::I64(_a), BorrowedValue::F64(b)) => Ok(OwnedValue::from(b.to_owned())),
                _ => Err(FunctionError::BadType{mfa: this_mfa()}),
            }
        }))
        .insert(tremor_fn!(math::min(_context, a, b) {
            match (a, b) {
                (BorrowedValue::F64(a), BorrowedValue::F64(b)) if a < b  => Ok(OwnedValue::from(a.to_owned())),
                (BorrowedValue::F64(_a), BorrowedValue::F64(b)) => Ok(OwnedValue::from(b.to_owned())),
                (BorrowedValue::I64(a), BorrowedValue::I64(b)) if a < b  => Ok(OwnedValue::from(a.to_owned())),
                (BorrowedValue::I64(_a), BorrowedValue::I64(b)) => Ok(OwnedValue::from(b.to_owned())),
                (BorrowedValue::F64(a), BorrowedValue::I64(b)) if *a < *b as f64 => Ok(OwnedValue::from(a.to_owned())),
                (BorrowedValue::F64(_a), BorrowedValue::I64(b)) => Ok(OwnedValue::from(b.to_owned())),
                (BorrowedValue::I64(a), BorrowedValue::F64(b)) if (*a as f64) < *b => Ok(OwnedValue::from(a.to_owned())),
                (BorrowedValue::I64(_a), BorrowedValue::F64(b)) => Ok(OwnedValue::from(b.to_owned())),
                _ => Err(FunctionError::BadType{mfa: this_mfa()}),
            }
        }));
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use simd_json::{BorrowedValue, OwnedValue};

    macro_rules! assert_val {
        ($e:expr, $r:expr) => {
            assert_eq!($e, Ok(OwnedValue::from($r)))
        };
    }

    #[test]
    fn floor() {
        let f = fun("math", "floor");
        let v = BorrowedValue::from(42);
        assert_val!(f(&[&v]), OwnedValue::from(42));
        let v = BorrowedValue::from(42.9);
        assert_val!(f(&[&v]), OwnedValue::from(42));
    }
    #[test]
    fn ceil() {
        let f = fun("math", "ceil");
        let v = BorrowedValue::from(42);
        assert_val!(f(&[&v]), OwnedValue::from(42));
        let v = BorrowedValue::from(41.1);
        assert_val!(f(&[&v]), OwnedValue::from(42));
        let v = BorrowedValue::from(42);
        assert_val!(f(&[&v]), OwnedValue::from(42));
    }

    #[test]
    fn round() {
        let f = fun("math", "round");
        let v = BorrowedValue::from(42);
        assert_val!(f(&[&v]), OwnedValue::from(42));
        let v = BorrowedValue::from(41.4);
        assert_val!(f(&[&v]), OwnedValue::from(41));
        let v = BorrowedValue::from(41.5);
        assert_val!(f(&[&v]), OwnedValue::from(42));
    }
    #[test]
    fn trunc() {
        let f = fun("math", "trunc");
        let v = BorrowedValue::from(42);
        assert_val!(f(&[&v]), OwnedValue::from(42));
        let v = BorrowedValue::from(42.9);
        assert_val!(f(&[&v]), OwnedValue::from(42));
    }
}
