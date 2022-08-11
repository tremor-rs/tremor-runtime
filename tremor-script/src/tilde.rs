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
use halfbrown::{hashmap, HashMap};

use crate::{datetime, grok::Pattern as GrokPattern, EventContext, Object, Value};
use crate::{grok::PATTERNS_FILE_DEFAULT_PATH, prelude::*};
use beef::Cow;
use cidr_utils::{
    cidr::{IpCidr, Ipv4Cidr},
    utils::IpCidrCombiner,
};
use dissect::Pattern;
use regex::Regex;
use std::fmt;
use std::hash::BuildHasherDefault;
use std::iter::{Iterator, Peekable};
use std::net::{IpAddr, Ipv4Addr};
use std::slice::Iter;
use std::str::FromStr;
use tremor_influx as influx;
use tremor_kv as kv;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExtractorResult<'result> {
    // a match without a value
    MatchNull,
    // We matched and captured a result
    Match(Value<'result>),
    // We didn't match
    NoMatch,
    // We encountered an error
    Err(ExtractorError),
}

impl<'result> From<bool> for ExtractorResult<'result> {
    fn from(b: bool) -> Self {
        if b {
            ExtractorResult::MatchNull
        } else {
            ExtractorResult::NoMatch
        }
    }
}

impl<'result> ExtractorResult<'result> {
    pub fn is_match(&self) -> bool {
        match self {
            ExtractorResult::Match(_) | ExtractorResult::MatchNull => true,
            ExtractorResult::NoMatch | ExtractorResult::Err(_) => false,
        }
    }
    pub fn into_match(self) -> Option<Value<'result>> {
        match self {
            ExtractorResult::MatchNull => Some(TRUE),
            ExtractorResult::Match(v) => Some(v),
            ExtractorResult::NoMatch | ExtractorResult::Err(_) => None,
        }
    }
}

fn parse_network(address: Ipv4Addr, mut itr: Peekable<Iter<u8>>) -> Option<IpCidr> {
    let mut network_length = match itr.next()? {
        c if *c >= b'0' && *c <= b'9' => *c - b'0',
        _ => return None,
    };
    network_length = match itr.next() {
        Some(c) if *c >= b'0' && *c <= b'9' => network_length * 10 + *c - b'0',
        None => network_length,
        Some(_) => return None,
    };
    if network_length > 32 {
        None
    } else {
        Some(IpCidr::V4(
            Ipv4Cidr::from_prefix_and_bits(address, network_length).ok()?,
        ))
    }
}

fn parse_ipv4_fast(ipstr: &str) -> Option<IpCidr> {
    let mut itr = ipstr.as_bytes().iter().peekable();
    //// A
    let mut a: u8 = 0;
    while let Some(c) = itr.next() {
        match *c {
            b'0'..=b'9' => {
                a = if let Some(a) = a.checked_mul(10).and_then(|a| a.checked_add(c - b'0')) {
                    a
                } else {
                    return parse_ipv6_fast(ipstr);
                };
            }
            b'a'..=b'f' | b'A'..=b'F' => return parse_ipv6_fast(ipstr),
            b'/' => return parse_network(Ipv4Addr::new(0, 0, 0, a), itr),
            b'.' => {
                itr.peek()?;
                break;
            }
            _ => return None,
        }
    }
    if itr.peek().is_none() {
        return Some(IpCidr::V4(
            Ipv4Cidr::from_prefix_and_bits(Ipv4Addr::new(0, 0, 0, a), 32).ok()?,
        ));
    };

    //// B
    let mut b: u8 = 0;
    while let Some(e) = itr.next() {
        match *e {
            b'0'..=b'9' => b = b.checked_mul(10).and_then(|b| b.checked_add(e - b'0'))?,
            b'/' => return parse_network(Ipv4Addr::new(a, 0, 0, b), itr),
            b'.' => {
                itr.peek()?;
                break;
            }
            _ => return None,
        }
    }
    if itr.peek().is_none() {
        return Some(IpCidr::V4(
            Ipv4Cidr::from_prefix_and_bits(Ipv4Addr::new(a, 0, 0, b), 32).ok()?,
        ));
    };

    //// C
    let mut c: u8 = 0;
    while let Some(e) = itr.next() {
        match *e {
            b'0'..=b'9' => c = c.checked_mul(10).and_then(|c| c.checked_add(e - b'0'))?,
            b'/' => return parse_network(Ipv4Addr::new(a, b, 0, c), itr),
            b'.' => {
                itr.peek()?;
                break;
            }
            _ => return None,
        }
    }
    if itr.peek().is_none() {
        return Some(IpCidr::V4(
            Ipv4Cidr::from_prefix_and_bits(Ipv4Addr::new(a, b, 0, c), 32).ok()?,
        ));
    };

    //// D
    let mut d: u8 = 0;
    while let Some(e) = itr.next() {
        match *e {
            b'0'..=b'9' => d = d.checked_mul(10).and_then(|d| d.checked_add(e - b'0'))?,
            b'/' => return parse_network(Ipv4Addr::new(a, b, c, d), itr),
            _ => return None,
        }
    }
    let address = Ipv4Addr::new(a, b, c, d);
    Some(IpCidr::V4(
        Ipv4Cidr::from_prefix_and_bits(address, 32).ok()?,
    ))
}

fn parse_ipv6_fast(s: &str) -> Option<IpCidr> {
    IpCidr::from_str(s).ok()
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
        compiled: dissect::Pattern,
    },
    /// Grok recognizer
    Grok {
        rule: String,
        #[serde(skip)]
        compiled: GrokPattern,
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

#[derive(Debug, Serialize)]
pub struct SnotCombiner {
    rules: Vec<String>,
    #[serde(skip)]
    combiner: IpCidrCombiner,
}

impl SnotCombiner {
    fn from_rules(rules: Vec<String>) -> Result<Self, ExtractorError> {
        let mut combiner = IpCidrCombiner::new();
        for x in &rules {
            //Cidr::from_str(x).map_err(|e| ExtractorError { msg: e.to_string() })?;
            if let Some(y) = parse_ipv4_fast(x) {
                combiner.push(y);
            } else {
                return Err(ExtractorError {
                    msg: format!("could not parse CIDR: '{}'", x),
                });
            }
        }
        Ok(Self { rules, combiner })
    }
}

impl PartialEq for SnotCombiner {
    fn eq(&self, other: &Self) -> bool {
        self.rules == other.rules
    }
}

impl Clone for SnotCombiner {
    fn clone(&self) -> Self {
        if let Ok(clone) = Self::from_rules(self.rules.clone()) {
            clone
        } else {
            Self {
                combiner: IpCidrCombiner::new(),
                rules: vec![],
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtractorError {
    pub msg: String,
}

impl fmt::Display for ExtractorError {
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
    /// This is affected only if we use == compairisons
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
                Extractor::Base64 => base64::decode(s).is_err(),
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
                Extractor::Influx => influx::decode::<Value>(s, 0).is_err(),
                Extractor::Datetime {
                    format,
                    has_timezone,
                } => datetime::_parse(s, format, *has_timezone).is_err(),
            }
        })
    }
    pub fn new(id: &str, rule_text: &str) -> Result<Self, ExtractorError> {
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
                compiled: Pattern::compile(rule_text)
                    .map_err(|e| ExtractorError { msg: e.to_string() })?,
            },
            "grok" => {
                let rule = rule_text.to_string();
                let compiled = GrokPattern::from_file(PATTERNS_FILE_DEFAULT_PATH, rule_text)
                    .or_else(|_| GrokPattern::new(rule_text))?;
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
                has_timezone: datetime::has_tz(rule_text),
            },
            other => {
                return Err(ExtractorError {
                    msg: format!("Unsupported extractor {}", other),
                })
            }
        };
        Ok(e)
    }

    #[allow(clippy::too_many_lines)]
    pub fn extract<'event>(
        &self,
        result_needed: bool,
        v: &Value<'event>,
        ctx: &EventContext,
    ) -> ExtractorResult<'event> {
        use ExtractorResult::{Err, Match, MatchNull, NoMatch};
        if let Some(s) = v.as_str() {
            match self {
                Self::Prefix(pfx) => s.starts_with(pfx).into(),
                Self::Suffix(sfx) => s.ends_with(sfx).into(),
                Self::Glob { compiled: glob, .. } => glob.matches(s).into(),
                Self::Kv(kv) => kv.run::<Value>(s).map_or(NoMatch, |r| {
                    if result_needed {
                        Match(r.into_static())
                    } else {
                        MatchNull
                    }
                }),
                Self::Base64 => {
                    let decoded = match base64::decode(s) {
                        Ok(d) => d,
                        Result::Err(_) => return NoMatch,
                    };
                    if result_needed {
                        match String::from_utf8(decoded) {
                            Ok(s) => Match(Value::from(s)),
                            Result::Err(e) => Err(ExtractorError {
                                msg: format!("failed to decode: {}", e),
                            }),
                        }
                    } else {
                        MatchNull
                    }
                }
                Self::Json => {
                    let mut s = s.as_bytes().to_vec();
                    // We will never use s afterwards so it's OK to destroy it's content
                    let encoded = s.as_mut_slice();
                    tremor_value::parse_to_value(encoded).map_or(NoMatch, |decoded| {
                        if result_needed {
                            Match(decoded.into_static())
                        } else {
                            MatchNull
                        }
                    })
                }
                Self::Cidr {
                    range: Some(combiner),
                    ..
                } => IpAddr::from_str(s).map_or(NoMatch, |input| {
                    if combiner.combiner.contains(input) {
                        if result_needed {
                            Cidr::from_str(s)
                                .map_or(NoMatch, |cidr| Match(Value::from(Object::from(cidr))))
                        } else {
                            MatchNull
                        }
                    } else {
                        NoMatch
                    }
                }),
                Self::Cidr { range: None, .. } => Cidr::from_str(s).map_or(NoMatch, |c| {
                    if result_needed {
                        Match(Value::from(Object::from(c)))
                    } else {
                        MatchNull
                    }
                }),
                Self::Dissect {
                    compiled: pattern, ..
                } => pattern.run(s).map_or(NoMatch, |o| {
                    if result_needed {
                        Match(
                            o.into_iter()
                                .map(|(k, v)| {
                                    let v: simd_json::BorrowedValue<'static> = v.into_static();
                                    let v: Value<'static> = Value::from(v);
                                    (beef::Cow::from(k.to_string()), v)
                                })
                                .collect(),
                        )
                    } else {
                        MatchNull
                    }
                }),
                Self::Grok {
                    compiled: ref pattern,
                    ..
                } => pattern.matches(s.as_bytes()).map_or(NoMatch, |o| {
                    if result_needed {
                        Match(o)
                    } else {
                        MatchNull
                    }
                }),
                Self::Influx => influx::decode::<Value>(s, ctx.ingest_ns())
                    .ok()
                    .flatten()
                    .map_or(NoMatch, |r| {
                        if result_needed {
                            Match(r.into_static())
                        } else {
                            MatchNull
                        }
                    }),
                Self::Datetime {
                    ref format,
                    has_timezone,
                } => datetime::_parse(s, format, *has_timezone).map_or(NoMatch, |d| {
                    if result_needed {
                        Match(Value::from(d))
                    } else {
                        MatchNull
                    }
                }),
                Self::Rerg { compiled: re, .. } => {
                    if !result_needed {
                        return if re.captures(s).is_some() {
                            MatchNull
                        } else {
                            NoMatch
                        };
                    }

                    let names: Vec<&str> = re.capture_names().flatten().collect();
                    let mut results = Value::object_with_capacity(names.len());
                    let captures = re.captures_iter(s);

                    for c in captures {
                        for name in &names {
                            if let Some(cap) = c.name(name) {
                                match results.get_mut(*name) {
                                    Some(Value::Array(a)) => {
                                        a.push(cap.as_str().into());
                                    }
                                    Some(_other) => {
                                        // error by construction - we always expect Value::array here
                                        // make compiler happy - silently ignore and continue
                                    }
                                    None => {
                                        if results.insert(*name, vec![cap.as_str()]).is_err() {
                                            // ALLOW: we know results is an object
                                            unreachable!();
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Match(results.into_static())
                }

                Self::Re { compiled, .. } => compiled.captures(s).map_or(NoMatch, |caps| {
                    if result_needed {
                        let matches: HashMap<beef::Cow<str>, Value> = compiled
                            .capture_names()
                            .flatten()
                            .filter_map(|n| {
                                Some((
                                    n.to_string().into(),
                                    Value::from(caps.name(n)?.as_str().to_string()),
                                ))
                            })
                            .collect();
                        Match(Value::from(matches))
                    } else {
                        MatchNull
                    }
                }),
            }
        } else {
            NoMatch
        }
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

impl<T: std::error::Error> From<T> for ExtractorError {
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

#[derive(Debug)]
pub struct Cidr(pub IpCidr);

impl Cidr {
    pub fn from_str(s: &str) -> Result<Self, ExtractorError> {
        if let Some(cidr) = parse_ipv4_fast(s) {
            Ok(Self(cidr))
        } else {
            Err(ExtractorError {
                msg: format!("Invalid CIDR: '{}'", s),
            })
        }
    }
}

impl std::ops::Deref for Cidr {
    type Target = IpCidr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'cidr> From<Cidr>
    for HashMap<Cow<'cidr, str>, Value<'cidr>, BuildHasherDefault<fxhash::FxHasher>>
{
    fn from(x: Cidr) -> Self {
        match x.0 {
            IpCidr::V4(y) => hashmap!(
                       "prefix".into() => Value::from(y.get_prefix_as_u8_array().to_vec()),
                       "mask".into() => Value::from(y.get_mask_as_u8_array().to_vec()),
            ),
            IpCidr::V6(y) => hashmap!(
                       "prefix".into() => Value::from(y.get_prefix_as_u16_array().to_vec()),
                       "mask".into() => Value::from(y.get_mask_as_u16_array().to_vec()),
            ),
        }
    }
}

#[cfg(test)]
mod test;
