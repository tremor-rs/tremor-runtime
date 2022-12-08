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

//! The grok extractor is useful for parsing unstructured data into a structured form. It is based on logstash's grok plugin. Grok uses regular expressions, so any regular expression can be used as a grok pattern.
//!
//! Grok pattern is of the form `%{SYNTAX : SEMANTIC}` where `SYNTAX` is the name of the pattern that matches the text and `SEMANTIC` is the identifier
//!
//! ## Predicate
//!
//! When used with `~`, the predicate passes if the target matches the pattern passed by the input (fetched from the grok pattern's file).
//!
//! ## Extraction
//!
//! If the predicate passes, the extractor returns the matches found when the target was matched to the pattern.
//!
//! ## Example
//!
//! ```tremor
//! match { "meta": "55.3.244.1 GET /index.html 15824 0.043" } of
//!   case rp = %{ meta ~= grok |%{IP:client} %{WORD:method} %{URIPATHPARAM:request} %{NUMBER:bytes} %{NUMBER:duration} | } => rp
//!   default => "no match"
//! end;
//! ```

use super::Result;
pub(crate) use crate::grok::Pattern;

pub(crate) fn execute(s: &str, result_needed: bool, pattern: &Pattern) -> Result<'static> {
    pattern.matches(s.as_bytes()).map_or(Result::NoMatch, |o| {
        if result_needed {
            Result::Match(o)
        } else {
            Result::MatchNull
        }
    })
}
