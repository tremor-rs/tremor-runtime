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

//! The JSON extractor converts the input string into its respective JSON representation conforming to The Javascript Object Notation Data Interchange Format (RFC 8259).
//!
//! ## Predicate
//!
//! When used with `~`, the predicate will pass if the input is a valid JSON
//!
//! ## Extraction
//!
//! If the predicate passes, the extractor will return the JSON representation of the target. Similar to the [`json` codec](../codecs/json.md), consecutive duplicate keys overwrite previous ones.
//!
//! ```tremor
//! match { "test" : "{ \"foo\":\"bar\", "\snot"\:"\badger"\ }" } of
//!    case foo  = %{ test ~= json|| } => foo
//!   default => "ko"
//! end;
//! ```

use super::Result;

pub(crate) fn execute(s: &str, result_needed: bool) -> Result<'static> {
    let mut s = s.as_bytes().to_vec();
    // We will never use s afterwards so it's OK to destroy it's content
    let encoded = s.as_mut_slice();
    tremor_value::parse_to_value(encoded).map_or(Result::NoMatch, |decoded| {
        if result_needed {
            Result::Match(decoded.into_static())
        } else {
            Result::MatchNull
        }
    })
}
