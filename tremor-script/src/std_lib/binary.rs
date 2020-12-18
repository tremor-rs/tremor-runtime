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

use crate::prelude::*;
use crate::registry::Registry;
use crate::{tremor_const_fn, tremor_fn_};

pub fn load(registry: &mut Registry) {
    registry
        .insert(tremor_const_fn! (binary::len(_context, _input: Bytes) {
            Ok(Value::from(_input.len()))
        }))
        .insert(
            tremor_const_fn! (binary::from_bytes(_context, _input: Array) {
                _input.iter().map(|v| v.as_u8().ok_or_else(||to_runtime_error("array contains non bytes"))).collect::<FResult<Vec<u8>>>().map(beef::Cow::from).map(Value::Bytes)
            }),
        ).insert(
            tremor_const_fn! (binary::into_bytes(_context, _input: Bytes) {
                Ok(_input.iter().copied().map(Value::from).collect::<Value>())
            }),
        );
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use crate::Value;

    #[test]
    fn len() {
        let f = fun("binary", "len");
        let v = Value::Bytes("snot".as_bytes().into());
        assert_val!(f(&[&v]), Value::from(4));
    }

    #[test]
    fn from_bytes() {
        let f = fun("binary", "from_bytes");
        let v = Value::from(b"snot".to_vec());
        assert_val!(f(&[&v]), Value::Bytes("snot".as_bytes().into()));
    }

    #[test]
    fn intp_array() {
        let f = fun("binary", "into_bytes");
        let v = Value::Bytes("snot".as_bytes().into());
        assert_val!(f(&[&v]), Value::from(b"snot".to_vec()));
    }
}
