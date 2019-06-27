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

use crate::grok::*;
use crate::influx;
use crate::std_lib::datetime::_parse;
use cidr_utils::{cidr::IpCidr, utils::IpCidrCombiner};
use dissect::Pattern;
use glob;
use kv;
use regex::Regex;
use simd_json::borrowed::Value;
use simd_json::OwnedValue;
use std::borrow::Cow;
use std::fmt;
use std::iter::Iterator;
use std::net::IpAddr;
use std::str::FromStr;

// {"Re":{"rule":"(snot)?foo(?P<snot>.*)"}}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Extractor {
    Glob {
        rule: String,
        #[serde(skip)]
        compiled: Option<glob::Pattern>,
    },
    Re {
        rule: String,
        #[serde(skip)]
        compiled: Option<Regex>,
    },
    Base64,
    Kv,
    Json,
    Dissect {
        rule: String,
        #[serde(default = "default_dissect_pattern")]
        #[serde(skip)]
        compiled: Result<dissect::Pattern, ExtractorError>,
    },

    Grok {
        rule: String,
        #[serde(skip)]
        compiled: Option<GrokPattern>,
    },
    Cidr {
        range: Option<Vec<String>>,
    },
    Influx,
    Datetime {
        format: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExtractorError {
    pub msg: String,
}

impl fmt::Display for ExtractorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

fn default_dissect_pattern() -> Result<dissect::Pattern, ExtractorError> {
    Ok(dissect::Pattern::default())
}

/// This is a stupid hack because we can't depend on display for E
/// since rust is being an idiot
pub trait Snot: fmt::Display {}
impl Snot for glob::PatternError {}
impl Snot for regex::Error {}
impl Snot for String {}
impl Snot for &str {}
impl Snot for base64::DecodeError {}
impl Snot for simd_json::Error {}
impl Snot for dissect::DissectError {}
impl<E: Snot> From<E> for ExtractorError {
    fn from(e: E) -> Self {
        ExtractorError {
            msg: format!("{}", e),
        }
    }
}

impl Extractor {
    pub fn new(id: &str, rule_text: &str) -> Result<Extractor, ExtractorError> {
        let id = id.to_lowercase();
        let e = match id.as_str() {
            "glob" => Extractor::Glob {
                compiled: Some(glob::Pattern::new(&rule_text)?),
                rule: rule_text.to_string(),
            },
            "re" => Extractor::Re {
                compiled: Some(Regex::new(&rule_text)?),
                rule: rule_text.to_string(),
            },
            "base64" => Extractor::Base64,
            "kv" => Extractor::Kv, //FIXME: How to handle different seperators?
            "json" => Extractor::Json,
            "dissect" => Extractor::Dissect {
                rule: rule_text.to_string(),
                compiled: Pattern::try_from(rule_text)
                    .map_err(|e| ExtractorError { msg: e.to_string() }),
            },
            "grok" => match GrokPattern::from_file(
                crate::grok::PATTERNS_FILE_DEFAULT_PATH.to_owned(),
                rule_text.to_string(),
            ) {
                Ok(pat) => Extractor::Grok {
                    rule: rule_text.to_string(),
                    compiled: Some(pat),
                },
                Err(_) => {
                    let mut grok = grok::Grok::default();
                    match grok.compile(&rule_text, true) {
                        Ok(pat) => Extractor::Grok {
                            rule: rule_text.to_string(),
                            compiled: Some(GrokPattern {
                                definition: rule_text.to_string(),
                                pattern: pat,
                            }),
                        },
                        Err(_) => Extractor::Grok {
                            rule: rule_text.to_string(),
                            compiled: None,
                        },
                    }
                }
            },
            "cidr" => {
                if rule_text.is_empty() {
                    Extractor::Cidr { range: None }
                } else {
                    let values = rule_text
                        .split(',')
                        .map(|x| x.trim().to_owned())
                        .collect::<Vec<String>>();
                    Extractor::Cidr {
                        range: Some(values),
                    }
                }
            }

            "influx" => Extractor::Influx,
            "datetime" => Extractor::Datetime {
                format: rule_text.to_string(),
            },
            other => return Err(format!("Unsupported extractor '{}'.", other).into()),
        };
        Ok(e)
    }

    pub fn extract<'event, 'run, 'script>(
        &'script self,
        v: &'run Value<'event>,
    ) -> Result<Value<'event>, ExtractorError>
    where
        'script: 'event,
        'event: 'run,
    {
        match v {
            Value::String(ref s) => match self {
                Extractor::Re {
                    compiled: Some(re), ..
                } => {
                    if let Some(caps) = re.captures(s) {
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
                        Err("regular expression dind't match'".into())
                    }
                }
                Extractor::Re { .. } => Err("invalid regular expression".into()),
                Extractor::Glob {
                    compiled: Some(glob),
                    ..
                } => {
                    if glob.matches(s) {
                        Ok(true.into())
                    } else {
                        Err("glob expression didn't match".into())
                    }
                }
                Extractor::Glob { .. } => Err("invalid glob pattern".into()),

                Extractor::Kv => {
                    if let Some(r) = kv::split(s, &[' '], &[':']) {
                        //FIXME: This is needed for removing the lifetimne from the result
                        let r: OwnedValue = Value::Object(r.clone()).into();
                        Ok(r.into())
                    } else {
                        Err("Failed to split kv list".into())
                    }
                }
                Extractor::Base64 => {
                    let encoded = s.to_string().clone();
                    let decoded = base64::decode(&encoded)?;
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
                    let decoded = simd_json::to_owned_value(encoded)?;
                    Ok(decoded.into())
                }
                Extractor::Cidr { range: Some(range) } => {
                    let mut combiner = IpCidrCombiner::new();
                    range.iter().for_each(|x| {
                        if let Ok(y) = Cidr::from_str(x) {
                            combiner.push(y.0);
                        }
                    });

                    let input = IpAddr::from_str(s).map_err(|_| ExtractorError {
                        msg: "input is invalid".into(),
                    })?;
                    if combiner.contains(input) {
                        Ok(Value::Object(
                            Cidr::from_str(s)
                                .map_err(|_| ExtractorError {
                                    msg: "The CIDR is invalid".into(),
                                })?
                                .into(),
                        ))
                    } else {
                        Err("IP does not belong to any CIDR specified".into())
                    }
                }
                Extractor::Cidr { range: None } => Ok(Value::Object(Cidr::from_str(s)?.into())),
                Extractor::Dissect {
                    compiled: Ok(ref pattern),
                    ..
                } => Ok(Value::Object(pattern.extract(s)?.0)),
                Extractor::Dissect {
                    compiled: Err(error),
                    ..
                } => Err(error.to_owned()),
                Extractor::Grok {
                    compiled: Some(ref pattern),
                    ..
                } => match pattern.matches(s.as_bytes().to_vec()) {
                    Ok(x) => Ok(x.into()),
                    Err(_) => Err("cannot find match for grok pattern".into()),
                },
                Extractor::Grok { .. } => Err("invalid grok operation".into()),
                Extractor::Influx => match influx::parse(s) {
                    Ok(x) => Ok(Value::Object(x.into())),
                    Err(_) => Err("The input is invalid".into()),
                },
                Extractor::Datetime { format } => {
                    Ok(Value::from(_parse(s, format).map_err(|e| {
                        ExtractorError {
                            msg: format!("Invalid datetime specified: {}", e.to_string()),
                        }
                    })?))
                }
            },
            _ => Err("Extractors are currently only supported against Strings".into()),
        }
    }
}
impl PartialEq<Extractor> for Extractor {
    fn eq(&self, other: &Extractor) -> bool {
        match (&self, other) {
            (Extractor::Base64, Extractor::Base64) => true,
            (Extractor::Kv, Extractor::Kv) => true,
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
            (Extractor::Cidr { range: range_l }, Extractor::Cidr { range: range_r }) => {
                range_l == range_r
            }
            (Extractor::Influx, Extractor::Influx) => true,
            (
                Extractor::Datetime { format: format_l },
                Extractor::Datetime { format: format_r },
            ) => format_l == format_r,
            _ => false,
        }
    }
}

#[derive(Debug)]
pub struct Cidr(pub IpCidr);

impl Cidr {
    pub fn from_str(s: &str) -> Result<Cidr, String> {
        Ok(Cidr(
            IpCidr::from_str(s).map_err(|_| "invalid cidr operation")?,
        ))
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
                    ex.extract(&Value::String("foobar".to_string().into())),
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
                    ex.extract(&Value::String("a:b c:d".to_string().into())),
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
                    ex.extract(&Value::String(r#"{"a":"b", "c":"d"}"#.to_string().into())),
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
                    ex.extract(&Value::String("INFO".to_string().into())),
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
                    ex.extract(&Value::String("8J+agHNuZWFreSByb2NrZXQh".into())),
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
                    ex.extract(&Value::String("John".to_string().into())),
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
                let output = ex.extract(&Value::from(
                    "<%1>123 Jul   7 10:51:24 hostname 2019-04-01T09:59:19+0010 pod dc foo bar baz",
                ));

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
                    ex.extract(&Value::from("192.168.1.0/24")),
                    Ok(Value::Object(hashmap! (
                                        "prefix".into() => Value::from(vec![Value::I64(192), 168.into(), 1.into(), 0.into()]),
                                        "mask".into() => Value::from(vec![Value::I64(255), 255.into(), 255.into(), 0.into()])


                    )))
                );

                assert_eq!(
                    ex.extract(&Value::from("192.168.1.0")),
                    Ok(Value::Object(hashmap!(
                                "prefix".into() => Value::from(vec![Value::I64(192), 168.into(), 1.into(), 0.into()]),
                                "mask".into() => Value::from(vec![Value::I64(255), 255.into(), 255.into(), 255.into()])
                    )))
                );

                assert_eq!(
                    ex.extract(&Value::from("2001:4860:4860:0000:0000:0000:0000:8888")),
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
                    rex.extract(&Value::from("10.22.0.254")),
                    Ok(Value::Object(hashmap! (
                            "prefix".into() => Value::from(vec![Value::I64(10), 22.into(), 0.into(), 254.into()]),
                            "mask".into() => Value::from(vec![Value::I64(255), 255.into(), 255.into(), 255.into()]),
                    )))
                );

                assert_eq!(
                    rex.extract(&Value::from("99.98.97.96")),
                    Err("IP does not belong to any CIDR specified".into())
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
                ex.extract(&Value::from(
                    "wea\\ ther,location=us-midwest temperature=82 1465839830100400200"
                )),
                Ok(Value::Object(hashmap! (
                       "measurement".into() => "wea ther".into(),
                       "tags".into() => Value::Object(hashmap!( "location".into() => "us-midwest".into())),
                       "fields".into() => Value::Object(hashmap!("temperature".into() => 82.0f64.into())),
                       "timestamp".into() => Value::I64(1465839830100400200)
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
                ex.extract(&Value::from("2019-06-20 00:00:00")),
                Ok(Value::I64(1560988800000_000_000))
            ),
            _ => unreachable!(),
        }
    }
}
