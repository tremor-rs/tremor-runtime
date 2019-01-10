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

#![cfg_attr(feature = "cargo-clippy", deny(clippy::all))]
//! Test regex matching the string data and evaluating to true
//! ```
//! use mimir::*;
//! let json = r#"{"key":"data"}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:/d.*/").unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Test regex not matching the string data and evaluating to false
//! ```
//! use mimir::*;
//! let json = r#"{"key":"data"}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:/e.*/").unwrap();
//! assert_eq!(false, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Test negations with compound rule
//! ```
//! use mimir::*;
//! let json = r#"{
//!                 "key1": {
//!                             "subkey1": "data1"
//!                         },
//!                 "key2": {
//!                             "subkey2": "data2"
//!                         },
//!                 "key3": "data3"
//!             }"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "!(key1.subkey1:\"data1\" OR NOT key3:\"data3\" or NOT (key1.subkey1:\"dat\" and key2.subkey2=\"data2\"))").unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Test find string value in mimir array.  This search uses the ':' contains in array operator meaning
//! in this case one of the values "foo", "data", or "bar".  Since there is a 'key' element containing
//! the value "data" in the document the result of this rule evaluation will be true.
//! ```
//! use mimir::*;
//! let json = r#"{"key":"data"}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, r#"key:["foo", "data", "bar"]"#).unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Test find int value in mimir array
//! ```
//! use mimir::*;
//! let json = r#"{"key":4}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:[3, 4, 5]").unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Test find float value in mimir array
//! ```
//! use mimir::*;
//! let json = r#"{"key":4.1}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:[3.1, 4.1, 5.1]").unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Find string value in array from document using the contains (":") operator.  In this case
//! there is a string array element in the document keyed by 'key' containing the values
//! "v1", "v2" and "v3.  The rule key:"v2" will be a match since "v2" is contained in the document
//! array.
//! ```
//! use mimir::*;
//! let json = r#"{"key":["v1", "v2", "v3"]}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:\"v2\"").unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Test find int value in json array
//! ```
//! use mimir::*;
//! let json = r#"{"key":[3, 4, 5]}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:4").unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Test find float value in json array
//! ```
//! use mimir::*;
//! let json = r#"{"key":[3.1, 4.1, 5.1]}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:4.1").unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Test wildcard search with mimir
//! ```
//! use mimir::*;
//! let json = r#"{"key":"data"}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:g\"da?a\"").unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Test wildcard search with mimir (not a match)
//! ```
//! use mimir::*;
//! let json = r#"{"key":"data"}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:g\"daa?a\"").unwrap();
//! assert_eq!(false, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Testing glob match using star match
//! ```
//! use mimir::*;
//! let json = r#"{"key1": "this is a glob blahblah"}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key1:g\"this is a glob*\"").unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Test equal comparison of two integers.
//! ```
//! use mimir::*;
//! let json = r#"{"key":5}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key=5").unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Test greater than comparison of two integers.
//! The value 5 from the json data is compared if it is
//! greatere than 1 and greater than 4.  Returns true.
//! ```
//! use mimir::*;
//! let json = r#"{"key":5}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key>1").unwrap();
//! r.add_rule(1, "key>4").unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Test greater than comparison of two integers.
//! The value 5 from the json data is compared if it is
//! greatere than -6.  Returns true.
//! ```
//! use mimir::*;
//! let json = r#"{"key":5}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key>-6").unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Test less than comparison of two integers.
//! The value 5 from the json data is compared if it is
//! greatere less than 10 and 9.  Returns true.
//!```
//! use mimir::*;
//! let json = r#"{"key":5}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key<10").unwrap();
//! r.add_rule(1, "key<9").unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Test less than comparison of two floating point numbers.
//! The value 5.0 from the json data is compared if it is
//! less than 10 and 9.  Returns true.
//! ```
//! use mimir::*;
//! let json = r#"{"key":5.0}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key<10.0").unwrap();
//! r.add_rule(1, "key<9.0").unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Test less than or equals comparison of two integers.
//! The value 5 from the json data is compared if it is
//! less than or equals to 11 and 5.  Returns true.
//! ```
//! use mimir::*;
//! let json = r#"{"key":5}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key<=5").unwrap();
//! r.add_rule(1, "key<=11").unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Test greater than or equals comparison of two integers.
//! The value 5 from the json data is compared if it is
//! greater than or equals to 3 and 4.  Returns true.
//! ```
//! use mimir::*;
//! let json = r#"{"key":5}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key >= 3").unwrap();
//! r.add_rule(1, "key >= 4").unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```
//!
//! Test greater than or equals comparison of two floating point numbers.
//! The value 5.0 from the json data is compared if it is
//! greater than or equals to 3.5 and 4.5.  Returns true.
//! ```
//! use mimir::*;
//! let json = r#"{"key":5.0}"#;
//! let mut vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key >= 3.5").unwrap();
//! r.add_rule(1, "key >= 4.5").unwrap();
//! assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
//! ```

#[macro_use]
extern crate lalrpop_util;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate lazy_static;

use glob::Pattern;
use pcre2::bytes::Regex;
use serde_json::Value;
use std::error::Error;
use std::str;
use std::str::FromStr;

pub type MimirValue = Value;

lazy_static! {
    static ref IP_REGEX: regex::Regex =
        regex::Regex::new(r"(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,2})/?(\d{1,2})?").unwrap();
}

lalrpop_mod!(
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::all))]
    parser
);

#[derive(Debug)]
pub struct CIDR {
    o1: u8,
    o2: u8,
    o3: u8,
    o4: u8,
    bits: u8,
    ip: u32,
    mask: u32,
}

impl CIDR {
    pub fn new(o1: u8, o2: u8, o3: u8, o4: u8, bits: u8) -> Self {
        let ip: u32 =
            (u32::from(o1) << 24) | (u32::from(o2) << 16) | (u32::from(o3) << 8) | u32::from(o4);
        let mask: u32 = 0xffff_ffff << (32 - bits);
        CIDR {
            o1,
            o2,
            o3,
            o4,
            bits,
            ip,
            mask,
        }
    }
}

#[derive(Debug)]
pub struct Stmt {
    item: Item,
    actions: Vec<Action>,
}

impl Stmt {
    pub fn test(&self, value: &Value) -> Result<bool, ErrorCode> {
        self.item.test(value)
    }
}

#[derive(Debug)]
pub enum Action {
    Set { path: Vec<String>, value: Value },
}

#[derive(Debug, Clone)]
pub enum MutationError {
    /// A type conflict while trying to set a nested map
    TypeConflict,
    /// The path is invalid or empty
    BadPath,
}

impl Error for MutationError {}

impl std::fmt::Display for MutationError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Action {
    pub fn execute(&self, json: &mut Value) -> Result<(), MutationError> {
        match self {
            Action::Set { path, value } => Action::mut_value(json, path, 0, value.clone()),
        }
    }

    fn mut_value(
        json: &mut Value,
        ks: &[String],
        i: usize,
        val: Value,
    ) -> Result<(), MutationError> {
        use serde_json::Map;
        fn make_nested_object(ks: &[String], i: usize, val: Value) -> Value {
            if let Some(key) = ks.get(i) {
                let mut m = Map::new();
                m.insert(key.to_string(), make_nested_object(ks, i + 1, val));
                Value::Object(m)
            } else {
                val
            }
        }
        if !json.is_object() {
            return Err(MutationError::TypeConflict);
        }
        if let Some(key) = ks.get(i) {
            // This is the last key1
            if i + 1 >= ks.len() {
                if let Some(ref mut v) = json.get_mut(&key) {
                    match v {
                        Value::Object(ref mut m) => {
                            m.insert(key.to_string(), val);
                            Ok(())
                        }
                        _ => Err(MutationError::TypeConflict),
                    }
                } else {
                    match json {
                        Value::Object(ref mut m) => {
                            m.insert(key.to_string(), val);
                            Ok(())
                        }
                        _ => Err(MutationError::TypeConflict),
                    }
                }
            } else if let Some(ref mut v) = json.get_mut(&key) {
                match v {
                    Value::Object(_m) => Action::mut_value(v, ks, i + 1, val),
                    _ => Err(MutationError::TypeConflict),
                }
            } else {
                match json {
                    Value::Object(ref mut m) => {
                        m.insert(key.to_string(), make_nested_object(ks, i + 1, val));
                        Ok(())
                    }
                    _ => Err(MutationError::TypeConflict),
                }
            }
        } else {
            Err(MutationError::BadPath)
        }
    }
}

#[derive(Debug)]
pub enum Item {
    Filter(Filter),
    Not(Box<Item>),
    And(Box<Item>, Box<Item>),
    Or(Box<Item>, Box<Item>),
}

impl Item {
    pub fn test(&self, value: &Value) -> Result<bool, ErrorCode> {
        match self {
            Item::Not(item) => {
                let res = item.test(value)?;
                Ok(!res)
            }
            Item::And(left, right) => {
                if left.test(value)? {
                    right.test(value)
                } else {
                    Ok(false)
                }
            }
            Item::Or(left, right) => {
                if left.test(value)? {
                    Ok(false)
                } else {
                    right.test(value)
                }
            }
            Item::Filter(filter) => filter.test(value),
        }
    }
}

#[derive(Debug)]
pub struct Filter {
    path: Vec<String>,
    cmp: Cmp,
}

impl Filter {
    pub fn test(&self, value: &Value) -> Result<bool, ErrorCode> {
        self.find_and_test(&self.path, 0, value)
    }

    fn find_and_test(&self, ks: &[String], i: usize, value: &Value) -> Result<bool, ErrorCode> {
        if let Some(key) = ks.get(i) {
            if let Some(v1) = value.get(key) {
                self.find_and_test(ks, i + 1, v1)
            } else {
                Ok(false)
            }
        } else {
            self.cmp.test(value)
        }
    }
}

#[derive(Debug)]
enum Cmp {
    Exists,
    Eq(Value),
    Gt(Value),
    Gte(Value),
    Lt(Value),
    Lte(Value),
    Contains(Value),
    Regex(Regex),
    Glob(Pattern),
    CIDRMatch(CIDR),
    IsInList(Vec<Value>),
}
impl Cmp {
    pub fn test(&self, event_value: &Value) -> Result<bool, ErrorCode> {
        match self {
            Cmp::Exists => Ok(true),
            Cmp::IsInList(list) => Ok(list.contains(event_value)),
            Cmp::Glob(g) => match event_value {
                Value::String(gmatch) => Ok(g.matches(gmatch)),
                _ => Ok(false),
            },
            Cmp::Regex(rx) => match event_value {
                Value::String(rxmatch) => match rx.is_match(&rxmatch.as_bytes()) {
                    Ok(n) => Ok(n),
                    Err(_) => Err(ErrorCode::RegexError),
                },
                _ => Ok(false),
            },
            Cmp::CIDRMatch(cidr) => match event_value {
                Value::String(ip_str) => {
                    if let Some(caps) = IP_REGEX.captures(ip_str) {
                        let a = u32::from_str(caps.get(1).map_or("", |m| m.as_str())).unwrap();
                        let b = u32::from_str(caps.get(2).map_or("", |m| m.as_str())).unwrap();
                        let c = u32::from_str(caps.get(3).map_or("", |m| m.as_str())).unwrap();
                        let d = u32::from_str(caps.get(4).map_or("", |m| m.as_str())).unwrap();
                        let ip = (a << 24) | (b << 16) | (c << 8) | d;

                        Ok(ip & cidr.mask == cidr.ip & cidr.mask)
                    } else {
                        Ok(false)
                    }
                }
                _ => Ok(false),
            },
            Cmp::Eq(expected_value) => Ok(event_value == expected_value),
            Cmp::Gt(expected_value) => match (event_value, expected_value) {
                (Value::Number(event_value), Value::Number(expected_value)) => {
                    if event_value.is_i64() && expected_value.is_i64() {
                        Ok(event_value.as_i64().unwrap() > expected_value.as_i64().unwrap())
                    } else if event_value.is_f64() && expected_value.is_f64() {
                        Ok(event_value.as_f64().unwrap() > expected_value.as_f64().unwrap())
                    } else {
                        Ok(false)
                    }
                }
                (Value::String(s), Value::String(is)) => Ok(s > is),
                _ => Ok(false),
            },
            Cmp::Lt(expected_value) => match (event_value, expected_value) {
                (Value::Number(event_value), Value::Number(expected_value)) => {
                    if event_value.is_i64() && expected_value.is_i64() {
                        Ok(event_value.as_i64().unwrap() < expected_value.as_i64().unwrap())
                    } else if event_value.is_f64() && expected_value.is_f64() {
                        Ok(event_value.as_f64().unwrap() < expected_value.as_f64().unwrap())
                    } else {
                        Ok(false)
                    }
                }
                (Value::String(s), Value::String(is)) => Ok(s < is),
                _ => Ok(false),
            },
            Cmp::Gte(expected_value) => match (event_value, expected_value) {
                (Value::Number(event_value), Value::Number(expected_value)) => {
                    if event_value.is_i64() && expected_value.is_i64() {
                        Ok(event_value.as_i64().unwrap() >= expected_value.as_i64().unwrap())
                    } else if event_value.is_f64() && expected_value.is_f64() {
                        Ok(event_value.as_f64().unwrap() >= expected_value.as_f64().unwrap())
                    } else {
                        Ok(false)
                    }
                }
                (Value::String(s), Value::String(is)) => Ok(s >= is),
                _ => Ok(false),
            },
            Cmp::Lte(expected_value) => match (event_value, expected_value) {
                (Value::Number(event_value), Value::Number(expected_value)) => {
                    if event_value.is_i64() && expected_value.is_i64() {
                        Ok(event_value.as_i64().unwrap() <= expected_value.as_i64().unwrap())
                    } else if event_value.is_f64() && expected_value.is_f64() {
                        Ok(event_value.as_f64().unwrap() <= expected_value.as_f64().unwrap())
                    } else {
                        Ok(false)
                    }
                }
                (Value::String(s), Value::String(is)) => Ok(s <= is),
                _ => Ok(false),
            },
            Cmp::Contains(expected_value) => match (event_value, expected_value) {
                (Value::String(event_value), Value::String(expected_value)) => {
                    Ok(event_value.contains(expected_value))
                }
                (Value::Number(event_value), Value::Number(expected_value)) => {
                    if event_value.is_i64() && expected_value.is_i64() {
                        Ok(event_value.as_i64().unwrap() == expected_value.as_i64().unwrap())
                    } else if event_value.is_f64() && expected_value.is_f64() {
                        #[allow(clippy::float_cmp)] // TODO: define a error
                        Ok(event_value.as_f64().unwrap() == expected_value.as_f64().unwrap())
                    } else {
                        Ok(false)
                    }
                }
                (Value::Array(event_value), _) => Ok(event_value.contains(expected_value)),

                _ => Ok(false),
            },
        }
    }
}

#[derive(Debug)]
pub struct Rule<T> {
    id: T,
    statement: Stmt,
}

#[derive(Default, Debug)]
pub struct Rules<T> {
    rules: Vec<Rule<T>>,
}

#[derive(Debug, Clone)]
pub enum ErrorCode {
    NoError,
    KeyNotFound,
    RegexError,
    ParseError,
    MutationError(MutationError),
}

impl Error for ErrorCode {}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::convert::From<MutationError> for ErrorCode {
    fn from(m: MutationError) -> Self {
        ErrorCode::MutationError(m)
    }
}

impl<T> Rules<T> {
    pub fn add_rule(&mut self, id: T, rule: &str) -> Result<usize, ErrorCode> {
        let res = parser::StmtParser::new().parse(rule);

        match res {
            Ok(n) => {
                self.rules.push(Rule { id, statement: n });
                Ok(self.rules.len())
            }
            Err(e) => {
                println!("Parser error: {}", e);
                Err(ErrorCode::ParseError)
            }
        }
    }

    pub fn eval_first_wins(&self, value: &mut Value) -> Result<Option<&T>, ErrorCode> {
        for Rule { id, statement } in &self.rules {
            if statement.test(value)? {
                for action in &statement.actions {
                    action.execute(value)?;
                }
                return Ok(Some(id));
            };

            //resp.get_mutevent();
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob() {
        let json = r#"{"key1": "data"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key1:g\"da?a\"").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_contains() {
        let json = r#"{"key1": "data3"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key1:\"data\"").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_equals() {
        let json = r#"{"key1": "data1"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key1=\"data1\"").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_subkey_equals() {
        let json = r#"{"key1": {"sub1": "data1"}}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key1.sub1=\"data1\"").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_subkey_contains() {
        let json = r#"{"key1": {"sub1": "data1"}}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key1.sub1:\"dat\"").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_compound_strings() {
        let json = r#"{
               "key1": {
                       "subkey1": "data1"
                   },
                   "key2": {
                       "subkey2": "data2"
                    },
                   "key3": "data3"
                }"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key1.subkey1:\"data1\" or key3:\"data3\" or (key1.subkey1:\"dat\" and key2.subkey2=\"data2\")").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_int_eq() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key=5").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_int_gt() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key>1").unwrap();
        r.add_rule(1, "key>4").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_negint_gt() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key>-6").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_int_lt() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key<10").unwrap();
        r.add_rule(1, "key<9").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_double_lt() {
        let json = r#"{"key":5.0}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key<10.0").unwrap();
        r.add_rule(1, "key<9.0").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_int_ltoe() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key<=5").unwrap();
        r.add_rule(1, "key<=11").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_int_gtoe() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key >= 3").unwrap();
        r.add_rule(1, "key >= 4").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_int_gtoe_double() {
        let json = r#"{"key":5.0}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key >= 3.5").unwrap();
        r.add_rule(1, "key >= 4.5").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_regex() {
        let json = r#"{"key":"data"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:/d.*/").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_regex_false() {
        let json = r#"{"key":"data"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:/e.*/").unwrap();
        assert_eq!(false, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_negregex() {
        let json = r#"{"key":"data"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "NOT key:/d.*/").unwrap();
        assert_eq!(false, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_regex_bug() {
        let json = r#"{"key":"\\/"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, r#"key:/\\//"#).unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_neg_compound_strings() {
        let json = r#"{
               "key1": {
                       "subkey1": "data1"
                   },
                   "key2": {
                       "subkey2": "data2"
                    },
                   "key3": "data3"
                }"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "!(key1.subkey1:\"data1\" OR NOT (key3:\"data3\") OR NOT (key1.subkey1:\"dat\" and key2.subkey2=\"data2\"))").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_list_contains_str() {
        let json = r#"{"key":"data"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:[\"foo\", \"data\", \"bar\"]").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_list_contains_int() {
        let json = r#"{"key":4}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:[3, 4, 5]").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_list_contains_float() {
        let json = r#"{"key":4.1}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:[3.1, 4.1, 5.1]").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_jsonlist_contains_str() {
        let json = r#"{"key":["v1", "v2", "v3"]}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:\"v2\"").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_jsonlist_contains_int() {
        let json = r#"{"key":[3, 4, 5]}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:4").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_jsonlist_contains_float() {
        let json = r#"{"key":[3.1, 4.1, 5.1]}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:4.1").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_bad_rule_syntax() {
        let mut r = Rules::default();
        assert_eq!(true, r.add_rule(0, "\"key").is_err());
    }

    #[test]
    fn test_ip_match() {
        let json = r#"{"key":"10.66.77.88"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:10.66.77.88").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_cidr_match() {
        let json = r#"{"key":"10.66.77.88"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:10.66.0.0/16").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_ip_match_false() {
        let json = r#"{"key":"10.66.78.88"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:10.66.77.88").unwrap();
        assert_eq!(false, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_cidr_match_false() {
        let json = r#"{"key":"10.67.77.88"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:10.66.0.0/16").unwrap();
        assert_eq!(false, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_exists() {
        let json = r#"{"key":"10.67.77.88"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key").unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_missing() {
        let json = r#"{"key":"10.67.77.88"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "!key").unwrap();
        assert_eq!(false, r.eval_first_wins(&mut vals).unwrap().is_some());
    }

    #[test]
    fn test_mut() {
        let json = r#"{"key":"val"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, r#"key="val" {newkey.newsubkey.other := "newval";}"#)
            .unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
        assert_eq!(
            Value::String("newval".to_string()),
            vals["newkey"]["newsubkey"]["other"]
        );
    }
    #[test]
    fn test_mut_multi() {
        let json = r#"{"key":"val"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(
            0,
            r#"key="val" {
                           newkey.newsubkey.other := "newval";
                           newkey.newsubkey.other2 := "newval2";
                         }"#,
        )
        .unwrap();
        assert_eq!(true, r.eval_first_wins(&mut vals).unwrap().is_some());
        assert_eq!(
            Value::String("newval".to_string()),
            vals["newkey"]["newsubkey"]["other"]
        );
        assert_eq!(
            Value::String("newval2".to_string()),
            vals["newkey"]["newsubkey"]["other2"]
        );
    }

}
