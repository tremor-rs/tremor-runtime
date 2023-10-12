// Copyright 2020-2021, The Tremor Team
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
use crate::{tremor_const_fn, tremor_fn_};
use tremor_common::base64::{Engine, BASE64};

pub fn load(registry: &mut Registry) {
    registry
        .insert(tremor_const_fn! (base64|encode(_context, input: Bytes) {
            Ok(Value::from(BASE64.encode(input)))
        }))
        .insert(tremor_const_fn! (base64|decode(_context, _input: String) {
            BASE64.decode(_input.as_bytes()).map(|v| Value::Bytes(v.into())).map_err(to_runtime_error)
        }));
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use crate::Value;

    #[test]
    fn decode() {
        let f = fun("base64", "decode");
        let v = Value::from("c25vdA==");
        assert_val!(f(&[&v]), Value::Bytes("snot".as_bytes().into()));
    }
    #[test]
    fn encode() {
        let f = fun("base64", "encode");
        let v = Value::Bytes("snot".as_bytes().into());
        assert_val!(f(&[&v]), Value::from("c25vdA=="));
    }
}
