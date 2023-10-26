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

//! The regex extractor extracts fields from data by parsing a regular expression provided by the user. It accepts a "perl-style regular expression"/
//!
//! ## Predicate
//!
//! When used with `~`, the predicate passes if a valid regular expression is passed.
//!
//! ## Extraction
//!
//! If the predicate passes, the extractor returns the matched values from the target. Returns an error if the regex fails to match.
//!
//! ## Example
//!
//! ```tremor
//! drop match { "test": "http://example.com/", "footle": "bar" } of
//!   case foo = %{ test ~= re|^http://.*/$|, footle == "bar" } => foo
//!   default => "ko"
//! end
//! ```
//!
//! The extractor is called by using the `~=` operator and specifying `re` as the extractor followed by regular expression after the pipe operator.
//!
//! The following syntax is supported:
//!
//! ### Matching one character
//!
//! ```text
//! .             any character except new line (includes new line with s flag)
//! \d            digit (\p{Nd})
//! \D            not digit
//! \pN           One-letter name Unicode character class
//! \p{Greek}     Unicode character class (general category or script)
//! \PN           Negated one-letter name Unicode character class
//! \P{Greek}     negated Unicode character class (general category or script)
//! ```
//!
//! ### Character classes
//!
//! ```text
//! [xyz]         A character class matching either x, y or z (union).
//! [^xyz]        A character class matching any character except x, y and z.
//! [a-z]         A character class matching any character in range a-z.
//! [[:alpha:]]   ASCII character class ([A-Za-z])
//! [[:^alpha:]]  Negated ASCII character class ([^A-Za-z])
//! [x[^xyz]]     Nested/grouping character class (matching any character except y and z)
//! [a-y&&xyz]    Intersection (matching x or y)
//! [0-9&&[^4]]   Subtraction using intersection and negation (matching 0-9 except 4)
//! [0-9--4]      Direct subtraction (matching 0-9 except 4)
//! [a-g~~b-h]    Symmetric difference (matching `a` and `h` only)
//! [\[\]]        Escaping in character classes (matching [ or ])
//! ```
//!
//! Any named character class may appear inside a bracketed `[...]` character class. For example, `[\p{Greek}[:digit:]]`matches any Greek or ASCII digit. `[\p{Greek}&&\pL]` matches Greek letters.
//!
//! Precedence in character classes, from most binding to least:
//!
//! 1. Ranges: `a-cd` == `[a-c]d`
//! 2. Union: `ab&&bc` == `[ab]&&[bc]`
//! 3. Intersection: `^a-z&&b` == `^[a-z&&b]`
//! 4. Negation
//!
//! #### Composites
//!
//! ```text
//! xy    concatenation (x followed by y)
//! x|y   alternation (x or y, prefer x)
//! ```
//!
//! #### Repetitions
//!
//! ```text
//! x*        zero or more of x (greedy)
//! x+        one or more of x (greedy)
//! x?        zero or one of x (greedy)
//! x*?       zero or more of x (ungreedy/lazy)
//! x+?       one or more of x (ungreedy/lazy)
//! x??       zero or one of x (ungreedy/lazy)
//! x{n,m}    at least n x and at most m x (greedy)
//! x{n,}     at least n x (greedy)
//! x{n}      exactly n x
//! x{n,m}?   at least n x and at most m x (ungreedy/lazy)
//! x{n,}?    at least n x (ungreedy/lazy)
//! x{n}?     exactly n x
//! ```
//!
//! #### Empty matches
//!
//! ```text
//! ^     the beginning of text (or start-of-line with multi-line mode)
//! $     the end of text (or end-of-line with multi-line mode)
//! \A    only the beginning of text (even with multi-line mode enabled)
//! \z    only the end of text (even with multi-line mode enabled)
//! \b    a Unicode word boundary (\w on one side and \W, \A, or \z on other)
//! \B    not a Unicode word boundary
//! ```
//!
//! #### Grouping and flags
//!
//! ```text
//! (exp)          numbered capture group (indexed by opening parenthesis)
//! (?P<name>exp)  named (also numbered) capture group (allowed chars: [_0-9a-zA-Z])
//! (?:exp)        non-capturing group
//! (?flags)       set flags within current group
//! (?flags:exp)   set flags for exp (non-capturing)
//! ```
//!
//! Flags are each a single character. For example, `(?x)` sets the flag `x` and `(?-x)` clears the flag `x`. Multiple flags can be set or cleared at the same time: `(?xy)` sets both the `x` and `y` flags and `(?x-y)` sets the `x` flag and clears the `y` flag.
//!
//! All flags are by default set to off unless stated otherwise. They are:
//!
//! ```text
//! i     case-insensitive: letters match both upper and lower case
//! m     multi-line mode: ^ and $ match begin/end of line
//! s     allow . to match \n
//! U     swap the meaning of x* and x*?
//! u     Unicode support (enabled by default)
//! x     ignore whitespace and allow line comments (starting with `#`)
//! ```

use super::Result;
pub(crate) use ::regex::Regex;
use beef::Cow;
use halfbrown::HashMap;
use tremor_value::Value;
use value_trait::prelude::*;

pub(crate) fn execute(s: &str, result_needed: bool, compiled: &Regex) -> Result<'static> {
    compiled.captures(s).map_or(Result::NoMatch, |caps| {
        if result_needed {
            let matches: HashMap<Cow<str>, Value, _> = compiled
                .capture_names()
                .flatten()
                .filter_map(|n| {
                    Some((
                        n.to_string().into(),
                        Value::from(caps.name(n)?.as_str().to_string()),
                    ))
                })
                .collect();
            Result::Match(Value::from(matches))
        } else {
            Result::MatchNull
        }
    })
}

pub(crate) fn execute_rerg(s: &str, result_needed: bool, re: &Regex) -> Result<'static> {
    if !result_needed {
        return if re.captures(s).is_some() {
            Result::MatchNull
        } else {
            Result::NoMatch
        };
    }

    let names: Vec<&str> = re.capture_names().flatten().collect();
    let mut results = Value::object_with_capacity(names.len());
    let captures = re.captures_iter(s);

    for c in captures {
        for (name, cap) in names.iter().filter_map(|name| Some((*name, c.name(name)?))) {
            if let Some(a) = results.get_mut(name).and_then(Value::as_array_mut) {
                a.push(cap.as_str().into());
            } else {
                results.try_insert(name, vec![cap.as_str()]);
            }
        }
    }
    Result::Match(results.into_static())
}
