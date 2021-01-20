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

/// simd-json integrates with serde, this module holds this integration.
/// note that when parsing to a dom you should use the functions in
/// `to_owned_value` or `to_borrowed_value` as they provide much
/// better performance.
///
/// However if have to use serde for other reasons or are parsing
/// directly to structs this is th4 place to go.
///
// mod de;
// mod se;
mod value;
pub use self::value::*;

#[cfg(test)]
mod test {
    use crate::Value;
    use simd_json::json;

    #[test]
    fn de_se() {
        let s = r#"{"value":{"array":[1,1.0,true,null],"string":"badger"}}"#.to_string();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(
            v,
            json!({"value": {"array": [1, 1.0, true, ()], "string": "badger"}})
        );
        assert_eq!(s, serde_json::to_string(&v).unwrap());
    }
}
