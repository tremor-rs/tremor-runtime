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

// KV parsing
//
// Parses a string into a map. It is possible to split based on different characters that represent
// either field or key value boundaries.
//
// A good part of the logstash functionality will be handled outside of this function and in a
// generic way in tremor script.
//
// Features (in relation to LS):
//
// | Setting                | Translation                                             | Supported |
// |------------------------|---------------------------------------------------------|-----------|
// | allow_duplicate_values | not supported, since we deal with JSON maps             | No        |
// | default_keys           | should be handled in TS (via assignment)                | TS        |
// | exclude_keys           | should behandled in TS (via delete_keys?)               | TS        |
// | field_split            | supported, array of strings                             | Yes       |
// | field_split_pattern    | not supported                                           | No        |
// | include_brackets       | should be handled in TS (via map + dissect?)            | TS        |
// | include_keys           | should be handled in TS (via select)                    | TS        |
// | prefix                 | should be handled in TS (via map + string::format)      | TS        |
// | recursive              | not supported                                           | No        |
// | remove_char_key        | should be handled in TS (via map + re::replace)         | TS        |
// | remove_char_value      | should be handled in TS (via map + re::replace)         | TS        |
// | source                 | handled in TS at call time                              | TS        |
// | target                 | handled in TS at return time                            | TS        |
// | tag_on_failure         | handled in TS at return time                            | TS        |
// | tag_on_timeout         | currently not supported                                 | No        |
// | timeout_millis         | currently not supported                                 | No        |
// | transform_key          | should be handled in TS (via map + ?)                   | TS        |
// | transform_value        | should be handled in TS (via map + ?)                   | TS        |
// | trim_key               | should be handled in TS (via map + ?)                   | TS        |
// | trim_value             | should be handled in TS (via map + ?)                   | TS        |
// | value_split            | supported, array of strings                             | Yes       |
// | value_split_pattern    | not supported                                           | No        |
// | whitespace             | we always run in 'lenient mode' as is the default of LS | No        |
#![forbid(warnings)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::result_unwrap_used,
    clippy::option_unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]
#![allow(clippy::must_use_candidate)]

use serde::{Deserialize, Serialize};
use simd_json::value::borrowed::{Object, Value};
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum Error {
    InvalidPattern(usize),
    DoubleSeperator(String),
    InvalidEscape(char),
    UnterminatedEscape,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPattern(p) => write!(f, "invalid pattern at character {}", p),
            Self::DoubleSeperator(s) => write!(f, "The seperator '{}' is used for both key value seperation as well as pair seperation.", s),
            Self::InvalidEscape(s) => write!(f, "Invalid escape sequence \\'{}' is not valid.", s),
            Self::UnterminatedEscape => write!(f, "Unterminated escape at the end of line or of a delimiter %{{ can't be escaped"),
        }
    }
}

impl std::error::Error for Error {}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct Pattern {
    field_seperators: Vec<String>,
    key_seperators: Vec<String>,
}

impl std::default::Default for Pattern {
    fn default() -> Self {
        Self {
            field_seperators: vec![" ".to_string()],
            key_seperators: vec![":".to_string()],
        }
    }
}

fn handle_escapes(s: &str) -> Result<String, Error> {
    let mut res = String::with_capacity(s.len());
    let mut cs = s.chars();
    while let Some(c) = cs.next() {
        match c {
            '\\' => {
                if let Some(c1) = cs.next() {
                    match c1 {
                        '\\' => res.push(c1),
                        'n' => res.push('\n'),
                        't' => res.push('\t'),
                        'r' => res.push('\r'),
                        other => return Err(Error::InvalidEscape(other)),
                    }
                } else {
                    return Err(Error::UnterminatedEscape);
                }
            }
            c => res.push(c),
        }
    }
    Ok(res)
}

impl Pattern {
    pub fn compile(pattern: &str) -> Result<Self, Error> {
        let mut field_seperators = Vec::new();
        let mut key_seperators = Vec::new();
        let mut i = 0;
        loop {
            if pattern[i..].starts_with("%{key}") {
                i += 6;
                if let Some(i1) = pattern[i..].find("%{val}") {
                    if i1 != 0 {
                        key_seperators.push(handle_escapes(&pattern[i..i + i1])?);
                    }
                    i += i1 + 6;
                } else {
                    return Err(Error::InvalidPattern(i));
                }
            } else if let Some(i1) = pattern[i..].find("%{key}") {
                if i1 != 0 {
                    field_seperators.push(handle_escapes(&pattern[i..i + i1])?);
                }
                i += i1;
            } else if pattern[i..].is_empty() {
                break;
            } else {
                field_seperators.push(handle_escapes(&pattern[i..])?);
                break;
            }
        }
        if field_seperators.is_empty() {
            field_seperators.push(" ".to_string())
        }
        if key_seperators.is_empty() {
            key_seperators.push(":".to_string())
        }
        field_seperators.sort();
        key_seperators.sort();
        field_seperators.dedup();
        key_seperators.dedup();

        for fs in &field_seperators {
            if key_seperators.iter().any(|ks| ks.find(fs).is_some()) {
                return Err(Error::DoubleSeperator(fs.to_string()));
            }

            if field_seperators
                .iter()
                .any(|fs2| fs2 != fs && fs2.contains(fs))
            {
                return Err(Error::DoubleSeperator(fs.to_string()));
            }
        }

        for ks in &key_seperators {
            if field_seperators.iter().any(|fs| fs.find(ks).is_some()) {
                return Err(Error::DoubleSeperator(ks.to_string()));
            }

            if key_seperators
                .iter()
                .any(|ks2| ks2 != ks && ks2.contains(ks))
            {
                return Err(Error::DoubleSeperator(ks.to_string()));
            }
        }

        Ok(Self {
            field_seperators,
            key_seperators,
        })
    }
    /// Splits a string that represents KV pairs.
    ///
    /// * `input` - The input string
    ///
    /// Note: Fields that have on value are dropped.
    pub fn run<'input>(&self, input: &'input str) -> Option<Object<'input>> {
        let r: Object = multi_split(input, &self.field_seperators)
            .iter()
            .filter_map(|field| {
                let kv: Vec<&str> = multi_split(field, &self.key_seperators);
                if kv.len() == 2 {
                    Some((kv[0].into(), Value::from(kv[1])))
                } else {
                    None
                }
            })
            .collect();
        if r.is_empty() {
            None
        } else {
            Some(r)
        }
    }
}

fn multi_split<'input>(input: &'input str, seperators: &[String]) -> Vec<&'input str> {
    use std::mem;
    let mut i: Vec<&str> = vec![input];
    let mut i1 = vec![];
    let mut r: Vec<&str>;
    for s in seperators {
        i1.clear();
        for e in &i {
            r = e.split(s.as_str()).collect();
            i1.append(&mut r);
        }
        mem::swap(&mut i, &mut i1);
    }
    i
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_multisplit() {
        let seps = vec![String::from(" "), String::from(";")];
        let input = "this=is;a=test for:seperators";

        let i = multi_split(input, &seps);
        assert_eq!(i, vec!["this=is", "a=test", "for:seperators"]);
    }
    #[test]

    fn simple_split() {
        let kv = Pattern::compile("%{key}=%{val}").expect("Failed to build pattern");
        let r = kv.run("this=is a=test").expect("Failed to split input");
        assert_eq!(r.len(), 2);
        assert_eq!(r["this"], "is");
        assert_eq!(r["a"], "test");
    }

    #[test]
    fn simple_split2() {
        let kv = Pattern::compile("&%{key}=%{val}").expect("Failed to build pattern");
        let r = kv.run("this=is&a=test").expect("Failed to split input");
        assert_eq!(r.len(), 2);
        assert_eq!(r["this"], "is");
        assert_eq!(r["a"], "test");
    }
    #[test]
    fn newline_simple_() {
        let kv = Pattern::compile(r#"\n%{key}=%{val}"#).expect("Failed to build pattern");
        let r = kv.run("this=is\na=test").expect("Failed to split input");
        assert_eq!(r.len(), 2);
        assert_eq!(r["this"], "is");
        assert_eq!(r["a"], "test");
    }

    #[test]
    fn simple_split3() {
        let kv = Pattern::compile("&").expect("Failed to build pattern");
        let r = kv.run("this:is&a:test").expect("Failed to split input");
        assert_eq!(r.len(), 2);
        assert_eq!(r["this"], "is");
        assert_eq!(r["a"], "test");
    }

    #[test]
    fn simple_split4() {
        let kv = Pattern::compile("%{key}%{%{val}").expect("Failed to build pattern");
        let r = kv.run("this%{is a%{test").expect("Failed to split input");
        assert_eq!(r.len(), 2);
        assert_eq!(r["this"], "is");
        assert_eq!(r["a"], "test");
    }

    #[test]
    fn simple_split5() {
        let kv = Pattern::compile("%{key}%{key}%{val}").expect("Failed to build pattern");
        dbg!(&kv);
        let r = kv
            .run("this%{key}is a%{key}test")
            .expect("Failed to split input");
        assert_eq!(r.len(), 2);
        assert_eq!(r["this"], "is");
        assert_eq!(r["a"], "test");
    }

    #[test]
    fn invalid_pattern() {
        let kv = Pattern::compile("%{key} %{val} ");
        assert_eq!(kv.err(), Some(Error::DoubleSeperator(String::from(" "))));
        let kv = Pattern::compile("%{key}=%{val}; %{key}:%{val} %{key}:%{val}");
        assert_eq!(kv.err(), Some(Error::DoubleSeperator(String::from(" "))));
        let kv = Pattern::compile("%{key}=%{val};%{key}:%{val} :%{key}:%{val}");
        assert_eq!(kv.err(), Some(Error::DoubleSeperator(String::from(":"))));
    }
    #[test]
    fn one_field() {
        let kv = Pattern::compile("%{key}=%{val}").expect("Failed to build pattern");
        let r = kv.run("this=is").expect("Failed to split input");
        assert_eq!(r.len(), 1);
        assert_eq!(r["this"], "is");
    }

    #[test]
    fn no_split() {
        let kv = Pattern::compile("%{key}=%{val}").expect("Failed to build pattern");
        let r = kv.run("this is a test");
        assert!(r.is_none());
    }

    #[test]
    fn different_seperatpors() {
        let kv = Pattern::compile("%{key}=%{val};%{key}:%{val} %{key}:%{val}")
            .expect("Failed to build pattern");
        let r = kv
            .run("this=is;a=test for:seperators")
            .expect("Failed to split input");
        dbg!(&r);
        dbg!(&kv);
        assert_eq!(r.len(), 3);
        assert_eq!(r["this"], "is");
        assert_eq!(r["a"], "test");
        assert_eq!(r["for"], "seperators");
    }

    #[test]
    fn different_seperatpors2() {
        let kv = Pattern::compile("%{key}=%{val}%{key}:%{val} %{key}:%{val};")
            .expect("Failed to build pattern");
        let r = kv
            .run("this=is;a=test for:seperators")
            .expect("Failed to split input");
        dbg!(&r);
        dbg!(&kv);
        assert_eq!(r.len(), 3);
        assert_eq!(r["this"], "is");
        assert_eq!(r["a"], "test");
        assert_eq!(r["for"], "seperators");
    }

    #[test]
    fn invalid_pattern2() {
        let kv = Pattern::compile("%{key}=%{val};%{key}:%{val} %{key}:%{val}")
            .expect("Failed to build pattern");
        let r = kv
            .run("this=is;a=test for:seperators")
            .expect("Failed to split input");
        dbg!(&r);
        dbg!(&kv);
        assert_eq!(r.len(), 3);
        assert_eq!(r["this"], "is");
        assert_eq!(r["a"], "test");
        assert_eq!(r["for"], "seperators");
    }
}
/*

Functions:

map::select(<map>, [<key>, ...])
  keeps only the given keys in an object this would make include keys be part of TS



*/

/*

https://codesearch.csnzoo.com/source/xref/wayfair%3Apuppet/modules/logstash/templates/conf/logstash.solr.transform.conf.erb

15 kv {
16   source       => "full_message"
17   include_keys => ["hits", "status", "QTime"]
18 }
19
20 kv {
21   source       => "full_message"
22   field_split  => "&"
23   include_keys => ["qt"]
24 }

https://codesearch.csnzoo.com/source/xref/wayfair%3Apuppet/modules/logstash/templates/conf/logstash.solr.transform.conf.erb

92   kv {
93     source       => "naxsi_params"
94     field_split  => "&"
95     include_keys => ["ip", "server", "uri", "learning", "vers", "total_processed", "total_blocked", "block"]
96   }

142       kv {
143         source       => "dispatcher_params"
144         field_split  => "&"
145         include_keys => ["_controller", "_action"]
146         remove_field => "dispatcher_params"
147       }

https://codesearch.csnzoo.com/source/xref/wayfair%3Apuppet/modules/logstash/templates/conf/logstash.git.transform.conf.erb

22     kv {
23       source => "syslog_message"
24       remove_field => [ "syslog_message" ]
25     }

 */
