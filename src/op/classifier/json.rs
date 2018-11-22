// Copyright 2018, Wayfair GmbH
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

//! # JSON based classifier
//!
//! Simple classification based on matching JSON values
//!
//! ## Configuration
//!
//! This operator has no configuration
use error::TSError;
use errors::*;
use pipeline::prelude::*;
use regex::Regex;
use serde_json;
use serde_yaml;
use std::io::Write;

#[derive(Debug)]
enum Cmp {
    Eq(String),
    Contains(String),
}

#[derive(Debug)]
struct Rule {
    key: String,
    cmp: Cmp,
    class: String,
}

#[derive(Debug)]
pub struct Classifier {
    classes: Vec<Rule>,
}

#[derive(Clone, Debug, Deserialize)]
struct ConfRule {
    rule: String,
    class: String,
}

#[derive(Debug, Deserialize)]
struct Config {
    rules: Vec<ConfRule>,
}

lazy_static! {
    static ref RULE_RE: Regex = Regex::new("(.*)([:=])(.*)").unwrap();
}

fn unqote_str(s: &str) -> Option<String> {
    match serde_json::from_str(s) {
        Ok(serde_json::Value::String(s)) => Some(s),
        _ => None,
    }
}
impl Classifier {
    pub fn new(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        let classes: Vec<Rule> = config
            .rules
            .iter()
            .filter_map(|r| match RULE_RE.captures(&r.rule) {
                Some(c) => match (&c[2], unqote_str(&c[3])) {
                    ("=", Some(v)) => Some(Rule {
                        key: c[1].to_string(),
                        class: r.class.clone(),
                        cmp: Cmp::Eq(v),
                    }),
                    (":", Some(v)) => Some(Rule {
                        key: c[1].to_string(),
                        class: r.class.clone(),
                        cmp: Cmp::Contains(v),
                    }),
                    _ => {
                        println_stderr!("invalid rule: {}", r.rule);
                        None
                    }
                },
                _ => {
                    println_stderr!("invalid rule: {}", r.rule);
                    None
                }
            }).collect();

        if classes.len() != config.rules.len() {
            Err("Invalid rules!")?
        } else {
            Ok(Classifier { classes })
        }
    }
}

impl Opable for Classifier {
    opable_types!(ValueType::JSON, ValueType::JSON);
    fn exec(&mut self, mut event: EventData) -> EventResult {
        if !event.is_type(ValueType::JSON) {
            let t = event.value.t();
            return EventResult::Error(
                event,
                Some(TSError::from(TypeError::with_location(
                    &"classifier::jsonr",
                    t,
                    ValueType::JSON,
                ))),
            );
        };
        let mut c = None;

        if let EventValue::JSON(ref val) = event.value {
            for r in &self.classes {
                if let Some(serde_json::Value::String(v)) = val.get(&r.key) {
                    match r.cmp {
                        Cmp::Eq(ref e) => {
                            if v.eq(e) {
                                c = Some(r.class.clone());
                                break;
                            }
                        }
                        Cmp::Contains(ref e) => {
                            if v.contains(e) {
                                c = Some(r.class.clone());
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            unreachable!()
        };

        match c {
            None => {
                event.set_var(&"classification", "default");
                EventResult::Next(event)
            }
            Some(class) => {
                event.set_var(&"classification", class.clone());
                EventResult::Next(event)
            }
        }
    }
}
