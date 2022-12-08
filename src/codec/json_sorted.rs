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

//! The `json-sorted` codec supports the Javascript Object Notation format with a consistent sort order.
//!
//! Specification: [JSON](https://json.org).
//!
//! Deserialization supports minified and fat JSON. Duplicate keys are not preserved, the last key overwrites previous ones.
//!
//! Serialization supports minified JSON only.

use super::json::Sorting;

/// Sorted
#[derive(Clone, Copy, Debug)]
pub struct Sorted {}
impl Sorting for Sorted {
    const SORTED: bool = true;
}
