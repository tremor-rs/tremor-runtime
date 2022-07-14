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

// cases:
//  1) Invalid 'glob/regex' => Does not compile
//  2) Fatal/bug during extraction => return Error

//  3) Is OK, but does not match. i.e. re|(?P<snot>bla*)| / <- "cake" => valid no match next case statement Ok({})
//  4) Matches. i.e. re|(?P<snot>bla*)| / <- "blaaaaa" => valid and match Ok({"snot": "blaaaaa"})
//  4) Matches. i.e. re|(?P<snot>bla*)?| / <- "cake" => valid and match Ok({})
//
//  '{}' -> json|| => Ok({})
//  Predicate for json||: "is valid json"
//  '{blarg' -> json|| =>
mod base64;
mod cidr;
mod datetime;
mod dissect;
mod glob;
mod grok;
mod influx;
mod json;
mod kv;
mod re;

use crate::{grok::PATTERNS_FILE_DEFAULT_PATH, prelude::*};
use crate::{EventContext, Value};
use re::Regex;
use std::{fmt, iter::Iterator, result::Result as StdResult};
use tremor_common::base64::decode;

use self::cidr::SnotCombiner;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Result<'result> {
    // a match without a value
    MatchNull,
    // We matched and captured a result
    Match(Value<'result>),
    // We didn't match
    NoMatch,
    // We encountered an error
    Err(Error),
}

impl<'result> From<bool> for Result<'result> {
    fn from(b: bool) -> Self {
        if b {
            Result::MatchNull
        } else {
            Result::NoMatch
        }
    }
}

impl<'result> Result<'result> {
    pub fn is_match(&self) -> bool {
        match self {
            Result::Match(_) | Result::MatchNull => true,
            Result::NoMatch | Result::Err(_) => false,
        }
    }
    pub fn into_match(self) -> Option<Value<'result>> {
        match self {
            Result::MatchNull => Some(TRUE),
            Result::Match(v) => Some(v),
            Result::NoMatch | Result::Err(_) => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
/// Encapsulates supported extractors
pub enum Extractor {
    /// Tests if a string starts with a prefix
    Prefix(String),
    /// Tests if a string ends with a suffix
    Suffix(String),
    /// Glob recognizer
    Glob {
        rule: String,
        #[serde(skip)]
        compiled: glob::Pattern,
    },
    /// PCRE recognizer
    Re {
        rule: String,
        #[serde(skip)]
        compiled: Regex,
    },
    /// PCRE with repeats recognizer
    Rerg {
        rule: String,
        #[serde(skip)]
        compiled: Regex,
    },
    /// Base64 recognizer
    Base64,
    /// KV ( Key/Value ) recognizer
    Kv(kv::Pattern),
    /// JSON recognizer
    Json,
    /// Dissect recognizer
    Dissect {
        rule: String,
        #[serde(skip)]
        compiled: ::dissect::Pattern,
    },
    /// Grok recognizer
    Grok {
        rule: String,
        #[serde(skip)]
        compiled: grok::Pattern,
    },
    /// CIDR notation recognizer
    Cidr {
        rules: Vec<String>,
        #[serde(skip)]
        range: Option<SnotCombiner>,
    },
    /// Influx line protocol recognizer
    Influx,
    /// Datetime recognizer
    Datetime {
        format: String,
        #[serde(skip)]
        has_timezone: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub msg: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl Extractor {
    pub fn cost(&self) -> u64 {
        match self {
            Extractor::Prefix(..) | Extractor::Suffix(..) => 25,
            Extractor::Base64 | Extractor::Grok { .. } => 50,
            Extractor::Glob { .. } => 100,
            Extractor::Cidr { .. } | Extractor::Datetime { .. } => 200,
            Extractor::Kv(_) | Extractor::Json | Extractor::Dissect { .. } => 500,
            Extractor::Influx => 750,
            Extractor::Re { .. } | Extractor::Rerg { .. } => 1000,
        }
    }

    pub fn extract<'event>(
        &self,
        result_needed: bool,
        v: &Value<'event>,
        ctx: &EventContext,
    ) -> Result<'event> {
        if let Some(s) = v.as_str() {
            match self {
                Self::Prefix(pfx) => s.starts_with(pfx).into(),
                Self::Suffix(sfx) => s.ends_with(sfx).into(),
                Self::Glob { compiled: glob, .. } => glob.matches(s).into(),
                Self::Kv(pattern) => kv::execute(s, result_needed, pattern),
                Self::Base64 => base64::execute(s, result_needed),
                Self::Json => json::execute(s, result_needed),
                Self::Cidr { range, .. } => cidr::execute(s, result_needed, range.as_ref()),
                Self::Dissect { compiled, .. } => dissect::execute(s, result_needed, compiled),
                Self::Grok { compiled, .. } => grok::execute(s, result_needed, compiled),
                Self::Influx => influx::execute(s, result_needed, ctx.ingest_ns()),
                Self::Datetime {
                    format,
                    has_timezone,
                } => datetime::execute(s, result_needed, format, *has_timezone),
                Self::Rerg { compiled, .. } => re::execute_rerg(s, result_needed, compiled),
                Self::Re { compiled, .. } => re::execute(s, result_needed, compiled),
            }
        } else {
            Result::NoMatch
        }
    }
    /// This is affected only if we use == comparisons
    pub fn is_exclusive_to(&self, value: &Value) -> bool {
        value.as_str().map_or(true, |s| {
            match self {
                // If the glob pattern does not match the string we compare to,
                // we know that the two are exclusive
                // match event of
                //   case %{a ~= glob|my*glob|} => ...
                //   case %{a == "snot"} => ...
                // end
                // the same holds true for regular expressions
                Extractor::Prefix(pfx) => !s.starts_with(pfx),
                Extractor::Suffix(sfx) => !s.ends_with(sfx),
                Extractor::Glob { compiled, .. } => !compiled.matches(s),
                Extractor::Rerg { compiled, .. } | Extractor::Re { compiled, .. } => {
                    !compiled.is_match(s)
                }
                Extractor::Base64 => decode(s).is_err(),
                Extractor::Kv(p) => p.run::<Value>(s).is_none(),
                Extractor::Json => {
                    let mut s = String::from(s);
                    let r = {
                        let s1 = s.as_mut_str();
                        // ALLOW: This is a temporary value
                        let s2 = unsafe { s1.as_bytes_mut() };
                        tremor_value::parse_to_value(s2).is_err()
                    };
                    r
                }
                Extractor::Dissect { compiled, .. } => {
                    let mut s = String::from(s);
                    compiled.run(s.as_mut_str()).is_none()
                }
                Extractor::Grok { compiled, .. } => compiled.matches(s.as_bytes()).is_err(),
                Extractor::Cidr { .. } => false, // IpAddr::from_str(s).is_err(), Never assume this is exclusive since it may have a lot of edge cases
                Extractor::Influx => tremor_influx::decode::<Value>(s, 0).is_err(),
                Extractor::Datetime {
                    format,
                    has_timezone,
                } => crate::datetime::_parse(s, format, *has_timezone).is_err(),
            }
        })
    }

    pub fn new(id: &str, rule_text: &str) -> StdResult<Self, Error> {
        let id = id.to_lowercase();
        let e = match id.as_str() {
            "glob" => {
                if is_prefix(rule_text) {
                    // ALLOW: we know the rule has a `*` at the end
                    Extractor::Prefix(rule_text[..rule_text.len() - 1].to_string())
                } else if is_suffix(rule_text) {
                    // ALLOW: we know the rule has a `*` at the b eginning
                    Extractor::Suffix(rule_text[1..].to_string())
                } else {
                    Extractor::Glob {
                        compiled: glob::Pattern::new(rule_text)?,
                        rule: rule_text.to_string(),
                    }
                }
            }
            "re" => Extractor::Re {
                compiled: Regex::new(rule_text)?,
                rule: rule_text.to_string(),
            },
            "rerg" => Extractor::Rerg {
                compiled: Regex::new(rule_text)?,
                rule: rule_text.to_string(),
            },
            "base64" => Extractor::Base64,
            "kv" => Extractor::Kv(kv::Pattern::compile(rule_text)?),
            "json" => Extractor::Json,
            "dissect" => Extractor::Dissect {
                rule: rule_text.to_string(),
                compiled: dissect::Pattern::compile(rule_text)
                    .map_err(|e| Error { msg: e.to_string() })?,
            },
            "grok" => {
                let rule = rule_text.to_string();
                let compiled = grok::Pattern::from_file(PATTERNS_FILE_DEFAULT_PATH, rule_text)
                    .or_else(|_| grok::Pattern::new(rule_text))?;
                {
                    Extractor::Grok { rule, compiled }
                }
            }
            "cidr" => {
                if rule_text.is_empty() {
                    Extractor::Cidr {
                        range: None,
                        rules: vec![],
                    }
                } else {
                    let rules = rule_text
                        .split(',')
                        .map(|x| x.trim().to_owned())
                        .collect::<Vec<String>>();
                    Extractor::Cidr {
                        range: Some(SnotCombiner::from_rules(rules.clone())?),
                        rules,
                    }
                }
            }

            "influx" => Extractor::Influx,
            "datetime" => Extractor::Datetime {
                format: rule_text.to_string(),
                has_timezone: crate::datetime::has_tz(rule_text),
            },
            other => {
                return Err(Error {
                    msg: format!("Unsupported extractor {other}"),
                })
            }
        };
        Ok(e)
    }
}

fn is_prefix(rule_text: &str) -> bool {
    Regex::new(r"^[^*?]+\*$")
        .map(|re| re.is_match(rule_text))
        .unwrap_or_default()
}

fn is_suffix(rule_text: &str) -> bool {
    Regex::new(r"^\*[^*?]+$")
        .map(|re| re.is_match(rule_text))
        .unwrap_or_default()
}

impl<T: std::error::Error> From<T> for Error {
    fn from(x: T) -> Self {
        Self { msg: x.to_string() }
    }
}

impl PartialEq<Extractor> for Extractor {
    fn eq(&self, other: &Self) -> bool {
        match (&self, other) {
            (Self::Base64, Self::Base64)
            | (Self::Json, Self::Json)
            | (Self::Influx, Self::Influx) => true,
            (Self::Re { rule: rule_l, .. }, Self::Re { rule: rule_r, .. })
            | (Self::Glob { rule: rule_l, .. }, Self::Glob { rule: rule_r, .. })
            | (Self::Dissect { rule: rule_l, .. }, Self::Dissect { rule: rule_r, .. })
            | (Self::Grok { rule: rule_l, .. }, Self::Grok { rule: rule_r, .. })
            | (Self::Datetime { format: rule_l, .. }, Self::Datetime { format: rule_r, .. })
            | (Self::Prefix(rule_l), Self::Prefix(rule_r))
            | (Self::Suffix(rule_l), Self::Suffix(rule_r)) => rule_l == rule_r,
            (Self::Kv(rule_l), Self::Kv(rule_r)) => rule_l == rule_r,
            (Self::Cidr { range: rule_l, .. }, Self::Cidr { range: rule_r, .. }) => {
                rule_l == rule_r
            }

            _ => false,
        }
    }
}

#[cfg(test)]
mod test;
