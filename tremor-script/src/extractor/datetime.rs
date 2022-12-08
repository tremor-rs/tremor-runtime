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

//! The datetime extractor parses the input into a timestamp. The format of the target needs to be specified as a parameter to the extractor.
//!
//! ## Predicate
//!
//! When used with `~`, the predicate parses if the target can be parsed to a nanosecond-precision timestamp. The predicate will fail if it encounters any of the errors described in the Error section below.
//!
//! ## Extraction
//!
//! If the predicate parses, the extractor returns the 64-bit nanosecond-precise UTC UNIX timestamp.
//!
//! ## Example
//!
//! ```tremor
//! match { "test" : "2019-01-01 09:42" } of
//!   case foo = %{ test ~= datetime|%Y-%m-%d %H:%M| } => foo.test
//!   default => "ko"
//! end;
//! ## output: 1546335720000000000
//! ```
//!
//! ## Errors
//!
//! The extractor will fail due to one of the following:
//!
//! * Incorrect input is passed
//! * Input doesn't match the format passed
//! * Input doesn't contain the Year, Month, Day, Hour & Minute section irrespective of the format passed.
//! * Input contains more components than the format passed

use super::Result;
use tremor_value::Value;

pub(crate) fn execute(
    s: &str,
    result_needed: bool,
    format: &str,
    has_timezone: bool,
) -> Result<'static> {
    crate::datetime::_parse(s, format, has_timezone).map_or(Result::NoMatch, |d| {
        if result_needed {
            Result::Match(Value::from(d))
        } else {
            Result::MatchNull
        }
    })
}
