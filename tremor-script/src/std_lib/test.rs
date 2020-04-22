// Copyright 2018-2020, Wayfair GmbH
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
use simd_json::prelude::*;
use simd_json::BorrowedValue;

// ALLOW: Until we have u64 support in clippy
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
pub fn load(registry: &mut Registry) {
    registry.insert(tremor_fn! (test::assert(ctx, desc, expected, got) {
        if expected != got {
            Err(to_runtime_error(format!(r#"
Assertion for {} failed:
  expected: {}
       got: {}
"#, desc, expected.encode(), got.encode())))
        } else {
            Ok(BorrowedValue::from(true))
        }
    }));
}
