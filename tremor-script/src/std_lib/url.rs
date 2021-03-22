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
use crate::tremor_fn;
use percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC};

pub fn load(registry: &mut Registry) {
    registry
        .insert(tremor_fn! (url|decode(ctx, s: String) {
            let ds = percent_decode_str(&s).decode_utf8();
            if let Ok(decoded) = ds {
                Ok(Value::from(decoded.to_string()))
            } else {
                Err(to_runtime_error(format!("Could not urldecode value: {}", s)))
            }
        }))
        .insert(tremor_fn! (url|encode(ctx, s: String) {
            Ok(Value::from(utf8_percent_encode(&s, NON_ALPHANUMERIC).to_string()))
        }));
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use crate::Value;

    macro_rules! assert_val {
        ($e:expr, $r:expr) => {
            assert_eq!($e, Ok(Value::from($r)))
        };
    }

    #[test]
    fn shook_endecode_smoke_test() {
        let d = fun("url", "decode");
        let e = fun("url", "encode");

        let v = Value::from("snot badger");
        assert_val!(e(&[&v]), "snot%20badger");

        let v = Value::from("%22snot%20badger%22");
        assert_val!(d(&[&v]), r#""snot badger""#);
    }
}
