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
use halfbrown::HashMap;

use dissect::Pattern;
use glob;
use kv;
use regex::Regex;
use simd_json::borrowed::Value;
use simd_json::OwnedValue;
use std::fmt;
use std::iter::Iterator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Cidr {
    V4 {
        a: u8,
        b: u8,
        c: u8,
        d: u8,
        mask: u32,
    },
}
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
    Cidr {
        rule: String,
        #[serde(skip)]
        compiled: Option<Cidr>,
    },
    Dissect {
        rule: String,
        #[serde(skip)]
        compiled: Option<dissect::Pattern>,
    },
    //FIXME: Cidr,
    //FIXME: Grok,
    //FIXME: Influx
}

#[derive(Debug, PartialEq)]
pub struct ExtractorError {
    pub msg: String,
}

impl fmt::Display for ExtractorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
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
            //FIXME: "cidr" => Some(Extractor::Cidr),
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
                compiled: Pattern::try_from(rule_text).ok(),
            },
            // FIXME: "grok" => Extractor::Grok,
            // FIXME: "json" => Extractor::Json,
            // FIXME: "influx" => Extractor::Influx,
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
        match &v {
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
                        Err("glob expression dind't match'".into())
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
                        String::from_utf8(decoded).expect("not valid utf-8").into(),
                    ))
                }
                Extractor::Json => {
                    let mut s = s.to_string();
                    // We will never use s afterwards so it's OK to destroy it's content
                    let encoded: &mut [u8] = unsafe { s.as_bytes_mut() };
                    let decoded = simd_json::to_owned_value(encoded)?;
                    Ok(decoded.into())
                }
                Extractor::Cidr { .. } => unimplemented!(),
                Extractor::Dissect {
                    compiled: Some(ref pattern),
                    ..
                } => Ok(Value::Object(pattern.extract(s)?.0)),
                Extractor::Dissect { .. } => Err("invalid dissect operation".into()),
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
            //FIXME: (Extractor::Cidr, Extractor::Cidr) => true,
            //FIXME: (Extractor::Grok, Extractor::Grok) => true,
            //FIXME: (Extractor::Json, Extractor::Json) => true,
            //FIXME: (Extractor::Kv, Extractor::Kv) => true,
            //FIXME: (Extractor::Influx, Extractor::Influx) => true,
            _ => false,
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
                        "name".into() => "John".into()
                    }))
                );
            }
            _ => unreachable!(),
        }
    }
}
