// Copyright 2018-2020, Wayfair GmbH
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
use crate::grok::Pattern as GrokPattern;
use crate::EventContext;
use cidr_utils::{
    cidr::{IpCidr, Ipv4Cidr},
    utils::IpCidrCombiner,
};
use dissect::Pattern;
use glob;
use regex::Regex;
use simd_json::borrowed::{Object, Value};
use simd_json::prelude::*;
use std::borrow::Cow;
use std::fmt;
use std::iter::{Iterator, Peekable};
use std::net::{IpAddr, Ipv4Addr};
use std::slice::Iter;
use std::str::FromStr;
use tremor_influx as influx;
use tremor_kv as kv;

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

#[derive(Debug, Clone, Serialize)]
pub(crate) enum Extractor {
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

#[derive(Clone, Debug, PartialEq)] // , Deserialize)]
pub struct ExtractorError {
    pub msg: String,
}

#[cfg_attr(tarpaulin, skip)]
impl fmt::Display for ExtractorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl Extractor {
    pub fn new(id: &str, rule_text: &str) -> Result<Self, ExtractorError> {
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
                compiled: Pattern::compile(rule_text)
                    .map_err(|e| ExtractorError { msg: e.to_string() })?,
            },
            "grok" => {
                if let Ok(pat) =
                    GrokPattern::from_file(crate::grok::PATTERNS_FILE_DEFAULT_PATH, rule_text)
                {
                    Extractor::Grok {
                        rule: rule_text.to_string(),
                        compiled: pat,
                    }
                } else {
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
    pub fn extract<'script, 'event, 'run, 'influx>(
        &'script self,
        result_needed: bool,
        v: &'run Value<'event>,
        ctx: &'run EventContext,
    ) -> Result<Value<'event>, ExtractorError>
    where
        'script: 'event,
        'event: 'run,
        'run: 'influx,
    {
        if let Some(s) = v.as_str() {
            match self {
                Self::Re { compiled: re, .. } => {
                    if let Some(caps) = re.captures(s) {
                        if !result_needed {
                            return Ok(Value::null());
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
                        Ok(Value::from(matches.clone()))
                    } else {
                        Err(ExtractorError {
                            msg: "regular expression didn't match'".into(),
                        })
                    }
                }
                Self::Glob { compiled: glob, .. } => {
                    if glob.matches(s) {
                        Ok(Value::from(true))
                    } else {
                        Err(ExtractorError {
                            msg: "glob expression didn't match".into(),
                        })
                    }
                }
                Self::Kv(kv) => {
                    if let Some(r) = kv.run::<Value>(s) {
                        if !result_needed {
                            return Ok(Value::null());
                        }
                        Ok(r.into_static())
                    } else {
                        Err(ExtractorError {
                            msg: "Failed to split kv list".into(),
                        })
                    }
                }
                Self::Base64 => {
                    let encoded = s.to_string();
                    let decoded = base64::decode(&encoded)?;
                    if !result_needed {
                        return Ok(Value::null());
                    }

                    Ok(Value::String(
                        String::from_utf8(decoded)
                            .map_err(|_| ExtractorError {
                                msg: "failed to decode".into(),
                            })?
                            .into(),
                    ))
                }
                Self::Json => {
                    let mut s = s.to_string();
                    // We will never use s afterwards so it's OK to destroy it's content
                    let encoded: &mut [u8] = unsafe { s.as_bytes_mut() };
                    let decoded =
                        simd_json::to_owned_value(encoded).map_err(|_| ExtractorError {
                            msg: "Error in decoding to a json object".to_string(),
                        })?;
                    if !result_needed {
                        return Ok(Value::null());
                    }
                    Ok(decoded.into())
                }
                Self::Cidr {
                    range: Some(combiner),
                    ..
                } => {
                    let input = IpAddr::from_str(s).map_err(|_| ExtractorError {
                        msg: "input is invalid".into(),
                    })?;
                    if combiner.combiner.contains(input) {
                        if !result_needed {
                            return Ok(Value::null());
                        }

                        Ok(Value::from(Object::from(Cidr::from_str(s)?)))
                    } else {
                        Err(ExtractorError {
                            msg: "IP does not belong to any CIDR specified".into(),
                        })
                    }
                }
                Self::Cidr { range: None, .. } => {
                    let c = Cidr::from_str(s)?;
                    if !result_needed {
                        return Ok(Value::null());
                    };
                    Ok(Value::from(Object::from(c)))
                }
                Self::Dissect {
                    compiled: pattern, ..
                } => {
                    if let Some(o) = pattern.run(s) {
                        Ok(Value::from(o))
                    } else {
                        Err(ExtractorError {
                            msg: "No match".into(),
                        })
                    }
                }
                Self::Grok {
                    compiled: ref pattern,
                    ..
                } => {
                    let o = pattern.matches(s.as_bytes())?;
                    if !result_needed {
                        return Ok(Value::null());
                    };
                    Ok(o.into())
                }
                Self::Influx => match influx::decode::<'influx, Value<'influx>>(s, ctx.ingest_ns())
                {
                    Ok(ref _x) if !result_needed => Ok(Value::null()),
                    Ok(Some(r)) => Ok(r.into_static()),
                    Ok(None) | Err(_) => Err(ExtractorError {
                        msg: "The input is invalid".into(),
                    }),
                },
                Self::Datetime {
                    ref format,
                    has_timezone,
                } => {
                    let d =
                        datetime::_parse(s, format, *has_timezone).map_err(|e| ExtractorError {
                            msg: format!("Invalid datetime specified: {}", e.to_string()),
                        })?;
                    if !result_needed {
                        return Ok(Value::null());
                    };
                    Ok(Value::from(d))
                }
            }
        } else {
            Err(ExtractorError {
                msg: "Extractors are currently only supported against Strings".into(),
            })
        }
    }
}

impl<T: std::error::Error> From<T> for ExtractorError {
    fn from(x: T) -> Self {
        Self { msg: x.to_string() }
    }
}

impl PartialEq<Extractor> for Extractor {
    #[allow(clippy::match_same_arms)]
    fn eq(&self, other: &Self) -> bool {
        match (&self, other) {
            (Self::Base64, Self::Base64)
            | (Self::Json, Self::Json)
            | (Self::Influx, Self::Influx) => true,
            (Self::Kv(rule_l), Self::Kv(rule_r)) => rule_l == rule_r,
            (Self::Re { rule: rule_l, .. }, Self::Re { rule: rule_r, .. }) => rule_l == rule_r,
            (Self::Glob { rule: rule_l, .. }, Self::Glob { rule: rule_r, .. }) => rule_l == rule_r,
            (Self::Dissect { rule: rule_l, .. }, Self::Dissect { rule: rule_r, .. }) => {
                rule_l == rule_r
            }
            (Self::Grok { rule: rule_l, .. }, Self::Grok { rule: rule_r, .. }) => rule_l == rule_r,
            (Self::Cidr { range: rule_l, .. }, Self::Cidr { range: rule_r, .. }) => {
                rule_l == rule_r
            }
            (Self::Datetime { format: rule_l, .. }, Self::Datetime { format: rule_r, .. }) => {
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

#[allow(clippy::implicit_hasher, clippy::use_self)]
// ^ we will not be using this with custom hashers, so we do not need to generalise the function over all hashers
impl<'cidr> From<Cidr> for HashMap<Cow<'cidr, str>, Value<'cidr>> {
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
mod test {
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
                        &EventContext::new(0, None)
                    ),
                    Ok(Value::from(
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
                        &EventContext::new(0, None)
                    ),
                    Ok(Value::from(hashmap! {
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
                        &EventContext::new(0, None)
                    ),
                    Ok(Value::from(hashmap! {
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
                        &EventContext::new(0, None)
                    ),
                    Ok(Value::from(true))
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
                        &EventContext::new(0, None)
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
                        &EventContext::new(0, None)
                    ),
                    Ok(Value::from(hashmap! {
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
                ), &EventContext::new(0, None));

                assert_eq!(
                    output,
                    Ok(Value::from(hashmap!(
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
                    ex.extract(
                        true,
                        &Value::from("192.168.1.0"),
                        &EventContext::new(0, None)
                    ),
                    Ok(Value::from(hashmap! (
                        "prefix".into() => Value::from(vec![Value::from(192), 168.into(), 1.into(), 0.into()]),
                        "mask".into() => Value::from(vec![Value::from(255), 255.into(), 255.into(), 255.into()])


                    )))
                );
                assert_eq!(
                    ex.extract(
                        true,
                        &Value::from("192.168.1.0/24"),
                        &EventContext::new(0, None)
                    ),
                    Ok(Value::from(hashmap! (
                                        "prefix".into() => Value::from(vec![Value::from(192), 168.into(), 1.into(), 0.into()]),
                                        "mask".into() => Value::from(vec![Value::from(255), 255.into(), 255.into(), 0.into()])


                    )))
                );

                assert_eq!(
                    ex.extract(
                        true,
                        &Value::from("192.168.1.0"),
                        &EventContext::new(0, None)
                    ),
                    Ok(Value::from(hashmap!(
                                "prefix".into() => Value::from(vec![Value::from(192), 168.into(), 1.into(), 0.into()]),
                                "mask".into() => Value::from(vec![Value::from(255), 255.into(), 255.into(), 255.into()])
                    )))
                );

                assert_eq!(
                    ex.extract(
                        true,
                        &Value::from("2001:4860:4860:0000:0000:0000:0000:8888"),
                        &EventContext::new(0, None)
                    ),
                    Ok(Value::from(hashmap!(
                                "prefix".into() => Value::from(vec![Value::from(8193),  18528.into(), 18528.into(), 0.into(), 0.into(), 0.into(), 0.into(), 34952.into()]),
                                "mask".into() => Value::from(vec![Value::from(65535), 65535.into(), 65535.into(), 65535.into(), 65535.into(), 65535.into(), 65535.into(), 65535.into()])
                    )))
                );
            }
            _ => unreachable!(),
        }

        let rex = Extractor::new("cidr", "10.22.0.0/24, 10.22.1.0/24").expect("bad rex");
        match rex {
            Extractor::Cidr { .. } => {
                assert_eq!(
                    rex.extract(
                        true,
                        &Value::from("10.22.0.254"),
                        &EventContext::new(0, None)
                    ),
                    Ok(Value::from(hashmap! (
                            "prefix".into() => Value::from(vec![Value::from(10), 22.into(), 0.into(), 254.into()]),
                            "mask".into() => Value::from(vec![Value::from(255), 255.into(), 255.into(), 255.into()]),
                    )))
                );

                assert_eq!(
                    rex.extract(
                        true,
                        &Value::from("99.98.97.96"),
                        &EventContext::new(0, None)
                    ),
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
                    &EventContext::new(0, None)
                ),
                Ok(Value::from(hashmap! (
                       "measurement".into() => "wea ther".into(),
                       "tags".into() => Value::from(hashmap!("location".into() => "us-midwest".into())),
                       "fields".into() => Value::from(hashmap!("temperature".into() => 82.0f64.into())),
                       "timestamp".into() => Value::from(1_465_839_830_100_400_200_u64)
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
                    &EventContext::new(0, None)
                ),
                Ok(Value::from(1_560_988_800_000_000_000_u64))
            ),
            _ => unreachable!(),
        }
    }
}
