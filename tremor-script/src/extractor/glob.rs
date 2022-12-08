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

//! Glob is an extractor that checks if the input string matches the specified [Unix shell-style pattern](https://en.wikipedia.org/wiki/Glob_(programming)#Unix-like). The extractor fails if an pattern is specified that is not valid or the string doesn't match the pattern.
//!
//! ## Predicate
//!
//! When used as a predicate with `~`, the predicate will pass if the input matches the glob pattern passed as the parameter to the extractor.
//!
//! ## Extraction
//!
//! The extractor returns true if the predicate passes else returns an error
//!
//! ## Patterns
//!
//! Patterns can be of the following types:
//!
//! | Pattern | Matches                                                                               |
//! |---------|---------------------------------------------------------------------------------------|
//! | `?`     | Single character                                                                      |
//! | `*`     | any (0 or more) sequence or characters                                                |
//! | `[…]`   | any character inside the bracket. Supports ranges (e,g. `[0-9]` will match any digit) |
//! | `[!…]`  | negation of `[…]`                                                                     |
//!
//! Meta characters (e..g `*`, `?` ) can be matched by using `[ ]`. (e.g. `[ * ]` will match a string that contains `*`).
//!
//! ```tremor
//! match { "test" : "INFO" } of
//!   case foo = %{ test ~= glob|INFO*| } => foo
//!   default => "ko"
//! end;
//! ## will output true
//! ```

pub use ::glob::Pattern;
