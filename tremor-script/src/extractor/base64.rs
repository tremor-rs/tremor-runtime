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

//! The `base64` extractor decodes content encoded with the Base64 binary-to-text encoding scheme into the corresponding string value.
//!
//! ## Predicate
//!
//! When used as a predicate test with `~`, and the referent target is a valid string and is base64 encoded, then the test succeeds.
//!
//! ## Extraction
//!
//! If predicate test succeeds, then the decoded base64 content it extracted and returned as a string literal.
//!
//! ## Example
//!
//! ```tremor
//! match { "test": "8J+MiiBzbm90IGJhZGdlcg==", "footle":·"bar" } of
//!   case foo = %{test ~= base64|| } => foo
//! end;
//! ## Output: 🌊 snot badger
//! ```
use super::{Error, Result, StdResult};
use base64::Engine;
use tremor_value::Value;

pub(crate) fn execute(s: &str, result_needed: bool) -> Result<'static> {
    let decoded = match ::base64::prelude::BASE64_STANDARD_NO_PAD.decode(s) {
        Ok(d) => d,
        StdResult::Err(_) => return Result::NoMatch,
    };
    if result_needed {
        match String::from_utf8(decoded) {
            Ok(s) => Result::Match(Value::from(s)),
            StdResult::Err(e) => Result::Err(Error {
                msg: format!("failed to decode: {e}"),
            }),
        }
    } else {
        Result::MatchNull
    }
}
