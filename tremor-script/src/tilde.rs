// Copyright 2018-2019, Wayfair GmbH
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
//  1) Invalid 'glob/regex' => Does not compiule
//  2) Fatal/bug during extraction => return Error

//  3) Is OK, but does not match. i.e. re|(?P<snot>bla*)| / <- "cake" => valid no match next case statement Ok({})
//  4) Matches. i.e. re|(?P<snot>bla*)| / <- "blaaaaa" => valid and match Ok({"snot": "blaaaaa"})
//  4) Matches. i.e. re|(?P<snot>bla*)?| / <- "cake" => valid and match Ok({})
//
//  '{}' -> json|| => Ok({})
//  Predicate for json||: "is valid json"
//  '{blarg' -> json|| =>
use base64;
use halfbrown::{hashmap, HashMap};

use crate::datetime;
use crate::grok::*;
use crate::influx;
use crate::EventContext;
use cidr_utils::{
    cidr::{IpCidr, Ipv4Cidr},
    utils::IpCidrCombiner,
};
use dissect::Pattern;
use glob;
use kv;
use regex::Regex;
use serde::de::{self, Deserialize, Deserializer, MapAccess, Visitor};
use simd_json::borrowed::Value;
use simd_json::OwnedValue;
use std::borrow::Cow;
use std::fmt;
use std::iter::{Iterator, Peekable};
use std::net::{IpAddr, Ipv4Addr};
use std::slice::Iter;
use std::str::FromStr;

// use std::marker::PhantomData;

fn parse_network(address: Ipv4Addr, mut itr: Peekable<Iter<u8>>) -> Option<IpCidr> {
    let mut network_length = match itr.next()? {
        c if *c >= b'0' && *c <= b'9' => *c - b'0',
        _ => return None,
    };
    network_length = match itr.next() {
        Some(c) if *c >= b'0' && *c <= b'9' => network_length * 10 + *c - b'0',
        None => network_length,
        _ => return None,
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
            b'/' => return parse_network(Ipv4Addr::new(a, 0, 0, 0), itr),
            b'.' => {
                if itr.peek().is_none() {
                    return None;
                } else {
                    break;
                }
            }
            _ => return None,
        }
    }
    if itr.peek().is_none() {
        return Some(IpCidr::V4(
            Ipv4Cidr::from_prefix_and_bits(Ipv4Addr::new(a, 0, 0, 0), 32).ok()?,
        ));
    };

    //// B
    let mut b: u8 = 0;
    while let Some(e) = itr.next() {
        match *e {
            b'0'..=b'9' => {
                b = if let Some(b) = b.checked_mul(10).and_then(|b| b.checked_add(e - b'0')) {
                    b
                } else {
                    return None;
                };
            }
            b'/' => return parse_network(Ipv4Addr::new(a, 0, 0, b), itr),
            b'.' => {
                if itr.peek().is_none() {
                    return None;
                } else {
                    break;
                }
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
            b'0'..=b'9' => {
                c = if let Some(c) = c.checked_mul(10).and_then(|c| c.checked_add(e - b'0')) {
                    c
                } else {
                    return None;
                };
            }
            b'/' => return parse_network(Ipv4Addr::new(a, b, 0, c), itr),
            b'.' => {
                if itr.peek().is_none() {
                    return None;
                } else {
                    break;
                }
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
            b'0'..=b'9' => {
                d = if let Some(d) = d.checked_mul(10).and_then(|d| d.checked_add(e - b'0')) {
                    d
                } else {
                    return None;
                };
            }
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

// {"Re":{"rule":"(snot)?foo(?P<snot>.*)"}}
#[derive(Debug, Clone, Serialize)]
pub enum Extractor {
    Glob {
        rule: String,
        #[serde(skip)]
        compiled: glob::Pattern,
    },
    Re {
        rule: String,
        #[serde(skip)]
        compiled: Regex,
    },
    Base64,
    Kv(kv::Pattern),
    Json,
    Dissect {
        rule: String,
        #[serde(skip)]
        compiled: dissect::Pattern,
    },

    Grok {
        rule: String,
        #[serde(skip)]
        compiled: GrokPattern,
    },
    Cidr {
        rules: Vec<String>,
        #[serde(skip)]
        range: Option<SnotCombiner>,
    },
    Influx,
    Datetime {
        format: String,
        #[serde(skip)]
        has_timezone: bool,
    },
}

impl<'de> Deserialize<'de> for Extractor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            Line,
            Column,
            Absolute,
        };

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;
                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`line`, `column` or `absolute`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "line" => Ok(Field::Line),
                            "column" => Ok(Field::Column),
                            "absolute" => Ok(Field::Absolute),
                            _ => Err(de::Error::missing_field("missing")),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct ExtractorVisitor;
        impl<'de> Visitor<'de> for ExtractorVisitor {
            type Value = Extractor;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("enum Extractor")
            }

            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match s {
                    "Base64" => Ok(Extractor::Base64),
                    "Influx" => Ok(Extractor::Influx),
                    "Json" => Ok(Extractor::Json),
                    _ => Err(de::Error::invalid_value(de::Unexpected::Str(&s), &self)),
                }
            }

            fn visit_map<V>(self, mut map: V) -> Result<Extractor, V::Error>
            where
                V: MapAccess<'de>,
            {
                let s: Option<(String, HashMap<String, serde_json::Value>)> = map.next_entry()?;

                match s {
                    Some((ref x, ref args)) if x.as_str() == "Glob" => {
                        let rule = args.get("rule").expect("expected a rule").to_string();
                        let rule = rule.trim_start_matches("\"").trim_end_matches("\"");
                        Ok(Extractor::new("glob", &rule).expect("should have worked"))
                    }
                    Some((ref x, ref args)) if x.as_str() == "Re" => {
                        let rule = args.get("rule").expect("expected a rule").to_string();
                        let rule = rule.trim_start_matches("\"").trim_end_matches("\"");
                        Ok(Extractor::new("re", &rule).expect("should have worked"))
                    }
                    Some((ref x, ref args)) if x.as_str() == "Dissect" => {
                        let rule = args.get("rule").expect("expected a rule").to_string();
                        let rule = rule.trim_start_matches("\"").trim_end_matches("\"");
                        Ok(Extractor::new("dissect", &rule).expect("should have worked"))
                    }
                    Some((ref x, ref args)) if x.as_str() == "Grok" => {
                        let rule = args.get("rule").expect("expected a rule").to_string();
                        let rule = rule.trim_start_matches("\"").trim_end_matches("\"");
                        Ok(Extractor::new("grok", &rule).expect("should have worked"))
                    }
                    Some((ref x, ref _args)) if x.as_str() == "Kv" => {
                        // let field_seps = args.get("field_separators").expect("expected a rule").foreach().map(|x| x.to_string()).collect();
                        // let field_seps = field_seps.trim_start_matches("\"").trim_end_matches("\"");
                        // let kv_seps = args.get("key_separators").expect("expected a rule").to_string();
                        // let kv_seps = kv_seps.trim_start_matches("\"").trim_end_matches("\"");
                        //dbg!(("kv", &field_seps, &kv_seps));
                        Ok(Extractor::new("kv", "").expect("should have worked"))
                    }
                    Some((ref x, ref args)) if x.as_str() == "Datetime" => {
                        let format = args.get("format").expect("expected a format").to_string();
                        let format = format.trim_start_matches("\"").trim_end_matches("\"");
                        Ok(Extractor::new("datetime", &format).expect("should have worked"))
                    }
                    Some((ref x, ref args)) if x.as_str() == "Cidr" => {
                        let rules = args.get("rules").expect("expected a set of rules");
                        let mut s = "".to_string();
                        let a = rules.as_array().expect("it should be an array");
                        for rule in a {
                            let rule = rule.to_string();
                            let rule = rule.trim_start_matches("\"").trim_end_matches("\"");
                            dbg!(&rule);
                            let _ = rule;
                            s = format!("{}{}, ", s, rule);
                        }
                        s = s.to_string().trim_end_matches(", ").to_string();
                        dbg!(("cidr", &s));
                        Ok(Extractor::new("cidr", &s).expect("should have worked"))
                    }
                    Some((ref x, _)) => {
                        Err(de::Error::invalid_value(de::Unexpected::Str(&x), &self))
                    }
                    _ => unreachable!(),
                }
            }
        }

        deserializer.deserialize_any(ExtractorVisitor)
    }
}

#[derive(Debug, Serialize)]
pub struct SnotCombiner {
    rules: Vec<String>,
    #[serde(skip)]
    combiner: IpCidrCombiner,
}

// FIXME add deser for SnotCombiner

impl SnotCombiner {
    fn from_rules(rules: Vec<String>) -> Result<Self, ExtractorError> {
        let mut combiner = IpCidrCombiner::new();
        for x in &rules {
            //Cidr::from_str(x).map_err(|e| ExtractorError { msg: e.to_string() })?;
            if let Some(y) = parse_ipv4_fast(x) {
                combiner.push(y)
            } else {
                return Err(ExtractorError {
                    msg: format!("could not parse CIDR: '{}'", x),
                });
            }
        }
        Ok(Self { combiner, rules })
    }
}

impl PartialEq for SnotCombiner {
    fn eq(&self, other: &Self) -> bool {
        self.rules == other.rules
    }
}

impl Clone for SnotCombiner {
    fn clone(&self) -> Self {
        if let Ok(clone) = SnotCombiner::from_rules(self.rules.clone()) {
            clone
        } else {
            SnotCombiner {
                combiner: IpCidrCombiner::new(),
                rules: vec![],
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)] // , Deserialize)]
pub struct ExtractorError {
    pub msg: String,
}

impl fmt::Display for ExtractorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl Extractor {
    pub fn new(id: &str, rule_text: &str) -> Result<Extractor, ExtractorError> {
        let id = id.to_lowercase();
        let e = match id.as_str() {
            "glob" => Extractor::Glob {
                compiled: glob::Pattern::new(&rule_text)?,
                rule: rule_text.to_string(),
            },
            "re" => Extractor::Re {
                compiled: Regex::new(&rule_text)?,
                rule: rule_text.to_string(),
            },
            "base64" => Extractor::Base64,
            "kv" => Extractor::Kv(kv::Pattern::compile(rule_text)?), //FIXME: How to handle different seperators?
            "json" => Extractor::Json,
            "dissect" => Extractor::Dissect {
                rule: rule_text.to_string(),
                compiled: Pattern::try_from(rule_text)
                    .map_err(|e| ExtractorError { msg: e.to_string() })?,
            },
            "grok" => match GrokPattern::from_file(
                crate::grok::PATTERNS_FILE_DEFAULT_PATH.to_owned(),
                rule_text.to_string(),
            ) {
                Ok(pat) => Extractor::Grok {
                    rule: rule_text.to_string(),
                    compiled: pat,
                },
                Err(_) => {
                    let mut grok = grok::Grok::default();
                    let pat = grok.compile(&rule_text, true)?;
                    Extractor::Grok {
                        rule: rule_text.to_string(),
                        compiled: GrokPattern {
                            definition: rule_text.to_string(),
                            pattern: pat,
                        },
                    }
                }
            },
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
                    msg: format!("Unsuupotred extractor {}", other),
                })
            }
        };
        Ok(e)
    }

    pub fn extract<'event, 'run, 'script>(
        &'script self,
        result_needed: bool,
        v: &'run Value<'event>,
        ctx: &'run EventContext,
    ) -> Result<Value<'event>, ExtractorError>
    where
        'script: 'event,
        'event: 'run,
    {
        match v {
            Value::String(ref s) => match self {
                Extractor::Re { compiled: re, .. } => {
                    if let Some(caps) = re.captures(s) {
                        if !result_needed {
                            return Ok(Value::Null);
                        }
                        let matches: HashMap<std::borrow::Cow<str>, Value> = re
                            .capture_names()
                            .flatten()
                            .filter_map(|n| {
                                Some((
                                    n.into(),
                                    Value::String(caps.name(n)?.as_str().to_string().into()),
                                ))
                            })
                            .collect();
                        Ok(Value::Object(matches.clone()))
                    } else {
                        Err(ExtractorError {
                            msg: "regular expression didn't match'".into(),
                        })
                    }
                }
                Extractor::Glob { compiled: glob, .. } => {
                    if glob.matches(s) {
                        Ok(Value::Bool(true))
                    } else {
                        Err(ExtractorError {
                            msg: "glob expression didn't match".into(),
                        })
                    }
                }
                Extractor::Kv(kv) => {
                    if let Some(r) = kv.run(s) {
                        if !result_needed {
                            return Ok(Value::Null);
                        }
                        //FIXME: This is needed for removing the lifetimne from the result
                        // The reason for this madness is that r might refference some
                        // data in v and we can't guarantee that v outlifes r.
                        let r: OwnedValue = Value::Object(r.clone()).into();
                        Ok(r.into())
                    } else {
                        Err(ExtractorError {
                            msg: "Failed to split kv list".into(),
                        })
                    }
                }
                Extractor::Base64 => {
                    let encoded = s.to_string().clone();
                    let decoded = base64::decode(&encoded)?;
                    if !result_needed {
                        return Ok(Value::Null);
                    }

                    Ok(Value::String(
                        String::from_utf8(decoded)
                            .map_err(|_| ExtractorError {
                                msg: "failed to decode".into(),
                            })?
                            .into(),
                    ))
                }
                Extractor::Json => {
                    let mut s = s.to_string();
                    // We will never use s afterwards so it's OK to destroy it's content
                    let encoded: &mut [u8] = unsafe { s.as_bytes_mut() };
                    let decoded =
                        simd_json::to_owned_value(encoded).map_err(|_| ExtractorError {
                            msg: "Error in decoding to a json object".to_string(),
                        })?;
                    if !result_needed {
                        return Ok(Value::Null);
                    }
                    Ok(decoded.into())
                }
                Extractor::Cidr {
                    range: Some(combiner),
                    ..
                } => {
                    let input = IpAddr::from_str(s).map_err(|_| ExtractorError {
                        msg: "input is invalid".into(),
                    })?;
                    if combiner.combiner.contains(input) {
                        if !result_needed {
                            return Ok(Value::Null);
                        }

                        Ok(Value::Object(Cidr::from_str(s)?.into()))
                    } else {
                        Err(ExtractorError {
                            msg: "IP does not belong to any CIDR specified".into(),
                        })
                    }
                }
                Extractor::Cidr { range: None, .. } => {
                    let c = Cidr::from_str(s)?;
                    if !result_needed {
                        return Ok(Value::Null);
                    };
                    Ok(Value::Object(c.into()))
                }
                Extractor::Dissect {
                    compiled: pattern, ..
                } => Ok(Value::Object(pattern.extract(s)?.0)),
                Extractor::Grok {
                    compiled: ref pattern,
                    ..
                } => {
                    let o = pattern.matches(s.as_bytes().to_vec())?;
                    if !result_needed {
                        return Ok(Value::Null);
                    };
                    Ok(o.into())
                }
                Extractor::Influx => match influx::parse(s, ctx.ingest_ns()) {
                    Ok(ref _x) if !result_needed => Ok(Value::Null),
                    Ok(None) => Err(ExtractorError {
                        msg: "The input is invalid".into(),
                    }),
                    Ok(Some(x)) => {
                        let r: OwnedValue = x.into();
                        Ok(r.into())
                    }
                    Err(_) => Err(ExtractorError {
                        msg: "The input is invalid".into(),
                    }),
                },
                Extractor::Datetime {
                    ref format,
                    has_timezone,
                } => {
                    let d =
                        datetime::_parse(s, format, *has_timezone).map_err(|e| ExtractorError {
                            msg: format!("Invalid datetime specified: {}", e.to_string()),
                        })?;
                    if !result_needed {
                        return Ok(Value::Null);
                    };
                    Ok(Value::from(d))
                }
            },
            _ => Err(ExtractorError {
                msg: "Extractors are currently only supported against Strings".into(),
            }),
        }
    }
}

impl<T: std::error::Error> From<T> for ExtractorError {
    fn from(x: T) -> ExtractorError {
        ExtractorError { msg: x.to_string() }
    }
}

impl PartialEq<Extractor> for Extractor {
    fn eq(&self, other: &Extractor) -> bool {
        match (&self, other) {
            (Extractor::Base64, Extractor::Base64) => true,
            (Extractor::Kv(l), Extractor::Kv(r)) => l == r,
            (Extractor::Json, Extractor::Json) => true,
            (Extractor::Re { rule: rule_l, .. }, Extractor::Re { rule: rule_r, .. }) => {
                rule_l == rule_r
            }
            (Extractor::Glob { rule: rule_l, .. }, Extractor::Glob { rule: rule_r, .. }) => {
                rule_l == rule_r
            }
            (Extractor::Dissect { rule: rule_l, .. }, Extractor::Dissect { rule: rule_r, .. }) => {
                rule_l == rule_r
            }
            (Extractor::Grok { rule: rule_l, .. }, Extractor::Grok { rule: rule_r, .. }) => {
                rule_l == rule_r
            }
            (Extractor::Cidr { range: range_l, .. }, Extractor::Cidr { range: range_r, .. }) => {
                range_l == range_r
            }
            (Extractor::Influx, Extractor::Influx) => true,
            (
                Extractor::Datetime {
                    format: format_l, ..
                },
                Extractor::Datetime {
                    format: format_r, ..
                },
            ) => format_l == format_r,
            _ => false,
        }
    }
}

#[derive(Debug)]
pub struct Cidr(pub IpCidr);

impl Cidr {
    pub fn from_str(s: &str) -> Result<Cidr, ExtractorError> {
        if let Some(cidr) = parse_ipv4_fast(s) {
            Ok(Cidr(cidr))
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

#[allow(clippy::implicit_hasher)]
// ^ we will not be using this with custom hashers, so we do not need to generalise the function over all hashers
impl<'cidr> From<Cidr> for HashMap<Cow<'cidr, str>, Value<'cidr>> {
    fn from(x: Cidr) -> HashMap<Cow<'cidr, str>, Value<'cidr>> {
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
mod tests {
    use super::*;
    use halfbrown::hashmap;
    use simd_json::borrowed::Value;
    #[test]
    fn test_re_extractor() {
        let ex = Extractor::new("re", "(snot)?foo(?P<snot>.*)").expect("bad extractor");
        match ex {
            Extractor::Re { .. } => {
                assert_eq!(
                    ex.extract(
                        true,
                        &Value::String("foobar".to_string().into()),
                        &EventContext { at: 0 }
                    ),
                    Ok(Value::Object(
                        hashmap! { "snot".into() => Value::String("bar".into()) }
                    ))
                );
            }
            _ => unreachable!(),
        };
    }
    #[test]
    fn test_kv_extractor() {
        let ex = Extractor::new("kv", "").expect("bad extractor");
        match ex {
            Extractor::Kv { .. } => {
                assert_eq!(
                    ex.extract(
                        true,
                        &Value::String("a:b c:d".to_string().into()),
                        &EventContext { at: 0 }
                    ),
                    Ok(Value::Object(hashmap! {
                        "a".into() => "b".into(),
                       "c".into() => "d".into()
                    }))
                );
            }
            _ => unreachable!(),
        };
    }

    #[test]
    fn test_json_extractor() {
        let ex = Extractor::new("json", "").expect("bad extractor");
        match ex {
            Extractor::Json => {
                assert_eq!(
                    ex.extract(
                        true,
                        &Value::String(r#"{"a":"b", "c":"d"}"#.to_string().into()),
                        &EventContext { at: 0 }
                    ),
                    Ok(Value::Object(hashmap! {
                        "a".into() => "b".into(),
                        "c".into() => "d".into()
                    }))
                );
            }
            _ => unreachable!(),
        };
    }

    #[test]
    fn test_glob_extractor() {
        let ex = Extractor::new("glob", "*INFO*").expect("bad extractor");
        match ex {
            Extractor::Glob { .. } => {
                assert_eq!(
                    ex.extract(
                        true,
                        &Value::String("INFO".to_string().into()),
                        &EventContext { at: 0 }
                    ),
                    Ok(Value::Bool(true))
                );
            }
            _ => unreachable!(),
        };
    }

    #[test]
    fn test_base64_extractor() {
        let ex = Extractor::new("base64", "").expect("bad extractor");
        match ex {
            Extractor::Base64 => {
                assert_eq!(
                    ex.extract(
                        true,
                        &Value::String("8J+agHNuZWFreSByb2NrZXQh".into()),
                        &EventContext { at: 0 }
                    ),
                    Ok("🚀sneaky rocket!".into())
                );
            }
            _ => unreachable!(),
        };
    }

    #[test]
    fn test_dissect_extractor() {
        let ex = Extractor::new("dissect", "%{name}").expect("bad extractor");
        match ex {
            Extractor::Dissect { .. } => {
                assert_eq!(
                    ex.extract(
                        true,
                        &Value::String("John".to_string().into()),
                        &EventContext { at: 0 }
                    ),
                    Ok(Value::Object(hashmap! {
                        "name".into() => Value::from("John")
                    }))
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_grok_extractor() {
        let pattern = r#"^<%%{POSINT:syslog_pri}>(?:(?<syslog_version>\d{1,3}) )?(?:%{SYSLOGTIMESTAMP:syslog_timestamp0}|%{TIMESTAMP_ISO8601:syslog_timestamp1}) %{SYSLOGHOST:syslog_hostname}  ?(?:%{TIMESTAMP_ISO8601:syslog_ingest_timestamp} )?(%{WORD:wf_pod} %{WORD:wf_datacenter} )?%{GREEDYDATA:syslog_message}"#;

        let ex = Extractor::new("grok", pattern).expect("bad extractor");
        match ex {
            Extractor::Grok { .. } => {
                let output = ex.extract(true, &Value::from(
                    "<%1>123 Jul   7 10:51:24 hostname 2019-04-01T09:59:19+0010 pod dc foo bar baz",
                ), &EventContext{at: 0});

                assert_eq!(
                    output,
                    Ok(Value::Object(hashmap!(
                    "syslog_timestamp1".into() =>  "".into(),
                              "syslog_ingest_timestamp".into() => "2019-04-01T09:59:19+0010".into(),
                              "wf_datacenter".into() => "dc".into(),
                              "syslog_hostname".into() => "hostname".into(),
                              "syslog_pri".into() => "1".into(),
                              "wf_pod".into() => "pod".into(),
                              "syslog_message".into() => "foo bar baz".into(),
                              "syslog_version".into() => "123".into(),
                              "syslog_timestamp0".into() =>  "Jul   7 10:51:24".into()

                                       )))
                );
            }

            _ => unreachable!(),
        }
    }
    #[test]
    fn test_cidr_extractor() {
        let ex = Extractor::new("cidr", "").expect("");
        match ex {
            Extractor::Cidr { .. } => {
                assert_eq!(
                    ex.extract(true, &Value::from("192.168.1.0"), &EventContext { at: 0 }),
                    Ok(Value::Object(hashmap! (
                        "prefix".into() => Value::from(vec![Value::I64(192), 168.into(), 1.into(), 0.into()]),
                        "mask".into() => Value::from(vec![Value::I64(255), 255.into(), 255.into(), 255.into()])


                    )))
                );
                assert_eq!(
                    ex.extract(
                        true,
                        &Value::from("192.168.1.0/24"),
                        &EventContext { at: 0 }
                    ),
                    Ok(Value::Object(hashmap! (
                                        "prefix".into() => Value::from(vec![Value::I64(192), 168.into(), 1.into(), 0.into()]),
                                        "mask".into() => Value::from(vec![Value::I64(255), 255.into(), 255.into(), 0.into()])


                    )))
                );

                assert_eq!(
                    ex.extract(true, &Value::from("192.168.1.0"), &EventContext { at: 0 }),
                    Ok(Value::Object(hashmap!(
                                "prefix".into() => Value::from(vec![Value::I64(192), 168.into(), 1.into(), 0.into()]),
                                "mask".into() => Value::from(vec![Value::I64(255), 255.into(), 255.into(), 255.into()])
                    )))
                );

                assert_eq!(
                    ex.extract(
                        true,
                        &Value::from("2001:4860:4860:0000:0000:0000:0000:8888"),
                        &EventContext { at: 0 }
                    ),
                    Ok(Value::Object(hashmap!(
                                "prefix".into() => Value::from(vec![Value::I64(8193),  18528.into(), 18528.into(), 0.into(), 0.into(), 0.into(), 0.into(), 34952.into()]),
                                "mask".into() => Value::from(vec![Value::I64(65535), 65535.into(), 65535.into(), 65535.into(), 65535.into(), 65535.into(), 65535.into(), 65535.into()])
                    )))
                );
            }
            _ => unreachable!(),
        }

        let rex = Extractor::new("cidr", "10.22.0.0/24, 10.22.1.0/24").expect("bad rex");
        match rex {
            Extractor::Cidr { .. } => {
                assert_eq!(
                    rex.extract(true, &Value::from("10.22.0.254"), &EventContext { at: 0 }),
                    Ok(Value::Object(hashmap! (
                            "prefix".into() => Value::from(vec![Value::I64(10), 22.into(), 0.into(), 254.into()]),
                            "mask".into() => Value::from(vec![Value::I64(255), 255.into(), 255.into(), 255.into()]),
                    )))
                );

                assert_eq!(
                    rex.extract(true, &Value::from("99.98.97.96"), &EventContext { at: 0 }),
                    Err(ExtractorError {
                        msg: "IP does not belong to any CIDR specified".into()
                    })
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_influx_extractor() {
        let ex = Extractor::new("influx", "").expect("bad extractor");
        match ex {
            Extractor::Influx => assert_eq!(
                ex.extract(
                    true,
                    &Value::from(
                        "wea\\ ther,location=us-midwest temperature=82 1465839830100400200"
                    ),
                    &EventContext { at: 0 }
                ),
                Ok(Value::Object(hashmap! (
                       "measurement".into() => "wea ther".into(),
                       "tags".into() => Value::Object(hashmap!( "location".into() => "us-midwest".into())),
                    "fields".into() => Value::Object(hashmap!("temperature".into() => 82.0f64.into
                                                              ())),
                       "timestamp".into() => Value::I64(1_465_839_830_100_400_200)
                )))
            ),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_datetime_extractor() {
        let ex = Extractor::new("datetime", "%Y-%m-%d %H:%M:%S").expect("bad extractor");
        match ex {
            Extractor::Datetime { .. } => assert_eq!(
                ex.extract(
                    true,
                    &Value::from("2019-06-20 00:00:00"),
                    &EventContext { at: 0 }
                ),
                Ok(Value::I64(1_560_988_800_000_000_000))
            ),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_ser_extractor() {
        let se = serde_json::to_string(&Extractor::Base64).expect("ser not ok for base64");
        let de: Extractor = serde_json::from_str(&se).expect("deser not ok for base64");
        assert_eq!(Extractor::Base64, de);
        assert_eq!("\"Base64\"", se);

        let se = serde_json::to_string(&Extractor::Influx).expect("ser not ok for influx");
        let de: Extractor = serde_json::from_str(&se).expect("deser not ok for influx");
        assert_eq!(Extractor::Influx, de);
        assert_eq!("\"Influx\"", se);

        let se = serde_json::to_string(&Extractor::Json).expect("ser not ok for json");
        let de: Extractor = serde_json::from_str(&se).expect("deser not ok for json");
        assert_eq!(Extractor::Json, de);
        assert_eq!("\"Json\"", se);

        let ex = Extractor::new("glob", "*").expect("bad extractor");
        let se = serde_json::to_string(&ex).expect("ser not ok for glob");
        let de: Extractor = serde_json::from_str(&se).expect("deser not ok for glob");
        assert_eq!(ex, de);
        assert_eq!("{\"Glob\":{\"rule\":\"*\"}}", se);

        let ex = Extractor::new("re", "(snot)?foo(?P<snot>.*)").expect("bad extractor");
        let se = serde_json::to_string(&ex).expect("ser not ok for re");
        let de: Extractor = serde_json::from_str(&se).expect("deser not ok for re");
        assert_eq!(ex, de);
        assert_eq!("{\"Re\":{\"rule\":\"(snot)?foo(?P<snot>.*)\"}}", se);

        let ex = Extractor::new("kv", "").expect("bad extractor");
        let se = serde_json::to_string(&ex).expect("ser not ok for kv");
        let de: Extractor = serde_json::from_str(&se).expect("deser not ok for kv");
        assert_eq!(ex, de);
        assert_eq!(
            "{\"Kv\":{\"field_seperators\":[\" \"],\"key_seperators\":[\":\"]}}",
            se
        );

        let ex = Extractor::new("datetime", "%Y-%m-%d %H:%M:%S").expect("bad extractor");
        let se = serde_json::to_string(&ex).expect("ser not ok for datetime");
        let de: Extractor = serde_json::from_str(&se).expect("deser not ok for datetime");
        assert_eq!(ex, de);
        assert_eq!("{\"Datetime\":{\"format\":\"%Y-%m-%d %H:%M:%S\"}}", se);

        let pattern = r#"^<%%{POSINT:syslog_pri}>(?:(?<syslog_version>\d{1,3}) )?(?:%{SYSLOGTIMESTAMP:syslog_timestamp0}|%{TIMESTAMP_ISO8601:syslog_timestamp1}) %{SYSLOGHOST:syslog_hostname}  ?(?:%{TIMESTAMP_ISO8601:syslog_ingest_timestamp} )?(%{WORD:wf_pod} %{WORD:wf_datacenter} )?%{GREEDYDATA:syslog_message}"#;
        let ex = Extractor::new("grok", pattern).expect("bad extractor");
        let se = serde_json::to_string(&ex).expect("ser not ok for re");
        //        let de: Extractor = serde_json::from_str(&se).expect("deser not ok for re");
        //        assert_eq!(ex, de);
        assert_eq!("{\"Grok\":{\"rule\":\"^<%%{POSINT:syslog_pri}>(?:(?<syslog_version>\\\\d{1,3}) )?(?:%{SYSLOGTIMESTAMP:syslog_timestamp0}|%{TIMESTAMP_ISO8601:syslog_timestamp1}) %{SYSLOGHOST:syslog_hostname}  ?(?:%{TIMESTAMP_ISO8601:syslog_ingest_timestamp} )?(%{WORD:wf_pod} %{WORD:wf_datacenter} )?%{GREEDYDATA:syslog_message}\"}}", se);

        let ex = Extractor::new("dissect", "*INFO*").expect("bad extractor");
        let se = serde_json::to_string(&ex).expect("ser not ok for grok");
        dbg!(&se);
        let de: Extractor = serde_json::from_str(&se).expect("deser not ok for grok");
        dbg!(&se);
        assert_eq!(ex, de);
        //        assert_eq!("{\"Dissect\":{\"rule\":\"*INFO*\"}}", se);

        let ex = Extractor::new("cidr", "10.22.0.0/24, 10.22.1.0/24").expect("bad extractor");
        let se = serde_json::to_string(&ex).expect("ser not ok for cidr");
        dbg!(&se);
        let de: Extractor = serde_json::from_str(&se).expect("deser not ok for cidr");
        dbg!(&se);
        assert_eq!(ex, de);
    }
}
