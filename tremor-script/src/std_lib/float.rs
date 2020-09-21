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

use crate::registry::Registry;
use crate::tremor_const_fn;

pub fn load(registry: &mut Registry) {
    registry.insert(tremor_const_fn! (float::parse(_context, _input: String) {
        _input.parse::<f64>().map_err(to_runtime_error).map(Value::from)
    }));
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use simd_json::BorrowedValue as Value;

    #[test]
    fn parse() {
        let f = fun("float", "parse");
        let v = Value::from("42.314");
        assert_val!(f(&[&v]), 42.314);
    }
}
