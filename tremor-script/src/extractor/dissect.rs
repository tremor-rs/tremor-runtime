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

//! The Dissect extractor is loosely based on logstash's dissect plugin. It extracts data from strings in form of key-value pairs based on the pattern specified. It supports patterns which makes it lightweight compared to other extractors such as grok or regex.
//!
//! Tokens are enclosed within `%{ }` and any characters placed between tokens is considered as a delimiter. There should be at least one delimiter in between 2 tokens except if one of them is a padding token.
//!
//! ## Predicate
//!
//! When used as a predicate test with `~`, it passes if and only if the referent target matches the pattern exhaustively.
//!
//! ## Extraction
//!
//! If the predicate passes, then a record of matched entries are returned where the keys are the fields specified in the pattern, and the values are the values extracted from the input string.
//!
//! ## Patterns
//!
//! The pattern can contain any of the following types:
//!
//! | Name           | Syntax                  | Description                                                                           | Sample Pattern                 | Input      | Output                                    | Notes                                                                                                                              |
//! |----------------|-------------------------|---------------------------------------------------------------------------------------|--------------------------------|------------|-------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
//! | Simple         | `%{ field }`            | Given field is used as the key for the pattern extracted                              | `%{name}`                      | John       | `"name" : "John"`                         |                                                                                                                                    |
//! | Append         | `% { + field }`         | Appends the value to another field specified with the same field name.                | `% {+name} %{+name}`           | John Doe   | `"name" : "John Doe"`                     | `+` symbol on the first token is optional<br />Does not support types                                                              |
//! | Named keys     | `%{& field}`            | Returns key value pair of the field. Takes the key from the previously matched field. | `%{ code } % {&country}`       | DE Germany | `"DE" : "Germany"`                        | Needs a field present earlier with the same name                                                                                   |
//! | Empty field    | `% { field }`           | Will return an empty value if no data is present                                      | `%{ code } %{country}`         | Germany    | `"code" : "",`<br />`"country" : "Germany"` |                                                                                                                                    |
//! | Skipped fields | `%{? field}`            | Skips the extracted value                                                             | `%{ ? first_name} {last_name}` | John Doe   | `"last_name" : "Doe"`                     |                                                                                                                                    |
//! | Typed Values   | `% { field : type}`     | Extracts the value from the data and converts it to another type specified            | `%{age : int}`                 | 22         | `"age" : 22`                              | Supported Types: int, float                                                                                                        |
//! | Padding        | `%{_}` or `%{_(chars)}` | `Removes padding from the field in the output`                                        | `%{name} %{_} %{age}`          | John 22    | `"name" : "John", "age":"22"`             | The field being extracted may not contain the padding.<br />A custom padding can be specified by using the `%{_(custom)}` notation |
//!
//! The operation can error in the following cases:
//!
//! - There is no delimiter between 2 tokens.
//! - Skipped, Append or Named field with no name (e.g. `%{?}`).
//! - A delimiter specified in the pattern is missing.
//! - A named field is not found in the pattern.
//! - the type specified is not valid.
//! - Types used with the append token.
//! - Pattern isn't completely parsed (at end of input).
//! - Input isn't completely parsed.
//!
//! Examples:
//!
//! ```tremor
//! match { "test": "http://example.com/"} of
//!   case foo = %{test ~= dissect|%{protocol}://%{host}.%{.tld} | } => foo
//!   default => "ko"
//! end;
//! ```
//!
//! Will output:
//!
//! ```bash
//! "test": {
//!     "protocol": "http",
//!     "host": "example",
//!     ".tld": "com/"
//! }
//! ```
//!
//! ```tremor
//! match { "test": "2019-04-20------------------- high 3 foo bar" } of
//!   case foo = %{test ~= dissect|%{date}%{_(-)} %{?priority} %{&priority} %{+snot} %{+snot}| } => foo
//!   default => "ko"
//! end;
//! ```
//!
//! Will output:
//!
//! ```bash
//! {
//!    "test": {
//!      "date": "2019-04-20",
//!      "high": "3",
//!      "snot": "foo bar"
//!    }
//! ```

use super::Result;
pub(crate) use dissect::Pattern;
use tremor_value::Value;

pub(crate) fn execute(s: &str, result_needed: bool, pattern: &Pattern) -> Result<'static> {
    pattern.run(s).map_or(Result::NoMatch, |o| {
        if result_needed {
            Result::Match(
                o.into_iter()
                    .map(|(k, v)| {
                        let v: simd_json::BorrowedValue<'static> = v.into_static();
                        let v: Value<'static> = Value::from(v);
                        (beef::Cow::from(k.to_string()), v)
                    })
                    .collect(),
            )
        } else {
            Result::MatchNull
        }
    })
}
