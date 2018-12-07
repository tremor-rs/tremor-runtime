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
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:/d.*/").unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Test regex not matching the string data and evaluating to false
//! ```
//! use mimir::*;
//! let json = r#"{"key":"data"}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:/e.*/").unwrap();
//! assert_eq!(false, r.eval_first_wins(&vals).unwrap().is_some());
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
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "!(key1.subkey1:\"data1\" NOT key3:\"data3\" or NOT (key1.subkey1:\"dat\" and key2.subkey2=\"data2\"))").unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Test find string value in mimir array.  This search uses the ':' contains in array operator meaning
//! in this case one of the values "foo", "data", or "bar".  Since there is a 'key' element containing
//! the value "data" in the document the result of this rule evaluation will be true.
//! ```
//! use mimir::*;
//! let json = r#"{"key":"data"}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, r#"key:["foo", "data", "bar"]"#).unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Test find int value in mimir array
//! ```
//! use mimir::*;
//! let json = r#"{"key":4}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:[3, 4, 5]").unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Test find float value in mimir array
//! ```
//! use mimir::*;
//! let json = r#"{"key":4.1}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:[3.1, 4.1, 5.1]").unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Find string value in array from document using the contains (":") operator.  In this case
//! there is a string array element in the document keyed by 'key' containing the values
//! "v1", "v2" and "v3.  The rule key:"v2" will be a match since "v2" is contained in the document
//! array.
//! ```
//! use mimir::*;
//! let json = r#"{"key":["v1", "v2", "v3"]}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:\"v2\"").unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Test find int value in json array
//! ```
//! use mimir::*;
//! let json = r#"{"key":[3, 4, 5]}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:4").unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Test find float value in json array
//! ```
//! use mimir::*;
//! let json = r#"{"key":[3.1, 4.1, 5.1]}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:4.1").unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Test wildcard search with mimir
//! ```
//! use mimir::*;
//! let json = r#"{"key":"data"}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:g\"da?a\"").unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Test wildcard search with mimir (not a match)
//! ```
//! use mimir::*;
//! let json = r#"{"key":"data"}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key:g\"daa?a\"").unwrap();
//! assert_eq!(false, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Testing glob match using star match
//! ```
//! use mimir::*;
//! let json = r#"{"key1": "this is a glob blahblah"}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key1:g\"this is a glob*\"").unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Test equal comparison of two integers.
//! ```
//! use mimir::*;
//! let json = r#"{"key":5}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key=5").unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Test greater than comparison of two integers.
//! The value 5 from the json data is compared if it is
//! greatere than 1 and greater than 4.  Returns true.
//! ```
//! use mimir::*;
//! let json = r#"{"key":5}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key>1").unwrap();
//! r.add_rule(1, "key>4").unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Test greater than comparison of two integers.
//! The value 5 from the json data is compared if it is
//! greatere than -6.  Returns true.
//! ```
//! use mimir::*;
//! let json = r#"{"key":5}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key>-6").unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Test less than comparison of two integers.
//! The value 5 from the json data is compared if it is
//! greatere less than 10 and 9.  Returns true.
//!```
//! use mimir::*;
//! let json = r#"{"key":5}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key<10").unwrap();
//! r.add_rule(1, "key<9").unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Test less than comparison of two floating point numbers.
//! The value 5.0 from the json data is compared if it is
//! less than 10 and 9.  Returns true.
//! ```
//! use mimir::*;
//! let json = r#"{"key":5.0}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key<10.0").unwrap();
//! r.add_rule(1, "key<9.0").unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Test less than or equals comparison of two integers.
//! The value 5 from the json data is compared if it is
//! less than or equals to 11 and 5.  Returns true.
//! ```
//! use mimir::*;
//! let json = r#"{"key":5}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key<=5").unwrap();
//! r.add_rule(1, "key<=11").unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Test greater than or equals comparison of two integers.
//! The value 5 from the json data is compared if it is
//! greater than or equals to 3 and 4.  Returns true.
//! ```
//! use mimir::*;
//! let json = r#"{"key":5}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key >= 3").unwrap();
//! r.add_rule(1, "key >= 4").unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```
//!
//! Test greater than or equals comparison of two floating point numbers.
//! The value 5.0 from the json data is compared if it is
//! greater than or equals to 3.5 and 4.5.  Returns true.
//! ```
//! use mimir::*;
//! let json = r#"{"key":5.0}"#;
//! let vals: MimirValue = serde_json::from_str(json).unwrap();
//! let mut r = Rules::default();
//! r.add_rule(0, "key >= 3.5").unwrap();
//! r.add_rule(1, "key >= 4.5").unwrap();
//! assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
//! ```

#[macro_use]
extern crate lalrpop_util;
#[macro_use]
extern crate serde_json;

use glob::Pattern;
use pcre2::bytes::Regex;
use serde_json::Value;
use std::error::Error;

lalrpop_mod!(
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::all))]
    parser
);

pub type MimirValue = serde_json::Value;

#[derive(Debug)]
pub enum Node {
    Lastkey(String),
    Ruleval(Value),
    List(Vec<Value>),
    Rex(Regex),
    Glob(Pattern),
    Op(Box<Node>, Operator, Box<Node>),
    Neg(Operator, Box<Node>),
    Error(ErrorCode),
    None,
}

#[derive(Debug, Copy, Clone)]
pub enum Operator {
    Con,
    Eq,
    Gt,
    Lt,
    Gtoe,
    Ltoe,
    And,
    Or,
    Dot,
    Not,
}

#[derive(Debug)]
pub struct EvalNode<'a> {
    lastkey: Option<&'a str>,
    ruledat: &'a Node,
    lastval: Option<&'a Value>,
    pub result: Option<bool>,
    pub error: ErrorCode,
}

#[derive(Debug)]
pub struct Rule<T> {
    id: T,
    rule: Node,
}

#[derive(Default, Debug)]
pub struct Rules<T> {
    rules: Vec<Rule<T>>,
}

impl<T> Rules<T> {
    pub fn add_rule(&mut self, id: T, rule: &str) -> Result<usize, ErrorCode> {
        let res = parser::ItemsParser::new().parse(rule);

        match res {
            Ok(n) => {
                self.rules.push(Rule { id, rule: *n });
                Ok(self.rules.len())
            }
            Err(_e) => Err(ErrorCode::ParseError),
        }
    }

    pub fn eval_first_wins(&self, vals: &Value) -> Result<Option<&T>, ErrorCode> {
        for Rule { id, rule } in &self.rules {
            let resp = Node::eval(rule, vals);

            match (resp.result, resp.error) {
                (Some(rb), ErrorCode::NoError) => {
                    if rb {
                        return Ok(Some(id));
                    }
                }
                _ => continue,
            }
        }

        Ok(None)
    }
}

impl<'a> EvalNode<'a> {
    fn dot_node_(&mut self, node: &EvalNode<'a>) {
        match (self.lastkey, self.lastval, node.lastkey) {
            (Some(_), Some(lv), Some(lk2)) => {
                self.lastkey = Some(lk2);
                self.lastval = Some(&lv[lk2]);
            }
            _ => self.error = ErrorCode::KeyNotFound,
        }
    }

    fn set_val_if_missing_(&mut self, val: &'a Value) {
        match (self.lastkey, self.lastval, self.error) {
            (Some(lk), None, ErrorCode::NoError) => {
                self.lastval = Some(&val[lk]);
            }
            _ => return,
        }
    }

    pub fn set_val_if_missing(mut self, val: &'a Value) -> EvalNode {
        self.set_val_if_missing_(val);
        self
    }

    pub fn dot_node(mut self, node: &EvalNode<'a>) -> EvalNode<'a> {
        self.dot_node_(node);
        self
    }

    fn gt_(&mut self, node: &EvalNode<'a>) {
        let bval = match (self.lastval, node.ruledat) {
            (Some(lv), Node::Ruleval(n)) => match (lv, n) {
                (Value::Number(num), Value::Number(inum)) => {
                    if num.is_i64() && inum.is_i64() {
                        num.as_i64().unwrap() > inum.as_i64().unwrap()
                    } else if num.is_f64() && inum.is_f64() {
                        num.as_f64().unwrap() > inum.as_f64().unwrap()
                    } else {
                        false
                    }
                }
                _ => false,
            },
            _ => false,
        };

        self.ruledat = node.ruledat;
        self.result = Some(bval);
    }

    fn gtoe_(&mut self, node: &EvalNode<'a>) {
        let bval = match (self.lastval, node.ruledat) {
            (Some(lv), Node::Ruleval(n)) => match (lv, n) {
                (Value::Number(num), Value::Number(inum)) => {
                    if num.is_i64() && inum.is_i64() {
                        num.as_i64().unwrap() >= inum.as_i64().unwrap()
                    } else if num.is_f64() && inum.is_f64() {
                        num.as_f64().unwrap() >= inum.as_f64().unwrap()
                    } else {
                        false
                    }
                }
                _ => false,
            },
            _ => false,
        };

        self.ruledat = node.ruledat;
        self.result = Some(bval);
    }

    fn lt_(&mut self, node: &EvalNode<'a>) {
        let bval = match (self.lastval, node.ruledat) {
            (Some(lv), Node::Ruleval(n)) => match (lv, n) {
                (Value::Number(num), Value::Number(inum)) => {
                    if num.is_i64() && inum.is_i64() {
                        num.as_i64().unwrap() < inum.as_i64().unwrap()
                    } else if num.is_f64() && inum.is_f64() {
                        num.as_f64().unwrap() < inum.as_f64().unwrap()
                    } else {
                        false
                    }
                }
                _ => false,
            },
            _ => false,
        };

        self.ruledat = node.ruledat;
        self.result = Some(bval);
    }

    fn ltoe_(&mut self, node: &EvalNode<'a>) {
        let bval = match (self.lastval, node.ruledat) {
            (Some(lv), Node::Ruleval(n)) => match (lv, n) {
                (Value::Number(num), Value::Number(inum)) => {
                    if num.is_i64() && inum.is_i64() {
                        num.as_i64().unwrap() <= inum.as_i64().unwrap()
                    } else if num.is_f64() && inum.is_f64() {
                        num.as_f64().unwrap() <= inum.as_f64().unwrap()
                    } else {
                        false
                    }
                }
                _ => false,
            },
            _ => false,
        };

        self.ruledat = node.ruledat;
        self.result = Some(bval);
    }

    fn contains_(&mut self, node: &EvalNode<'a>) {
        let bval = match (self.lastval, node.ruledat) {
            (Some(lv), Node::Ruleval(rv)) => match (lv, rv) {
                (Value::String(lv_s), Value::String(rv_s)) => lv_s.contains(rv_s),
                (Value::Number(lv_n), Value::Number(rv_n)) => lv_n == rv_n,
                (Value::Array(a), _) => a.contains(&rv),
                _ => false,
            },
            (Some(lv), Node::List(rv)) => rv.contains(lv),
            (Some(lv), Node::Rex(rx)) => match lv {
                Value::String(rxmatch) => match rx.is_match(&rxmatch.as_bytes()) {
                    Ok(n) => n,
                    Err(_) => {
                        self.error = ErrorCode::RegexError;
                        false
                    }
                },
                _ => false,
            },
            (Some(lv), Node::Glob(wc)) => match lv {
                Value::String(wcmatch) => wc.matches(&wcmatch),
                _ => false,
            },
            _ => false,
        };

        self.ruledat = node.ruledat;
        self.result = Some(bval);
    }

    fn equals_(&mut self, node: &EvalNode<'a>) {
        let bval = match (self.lastval, node.ruledat) {
            (Some(lv), Node::Ruleval(rv)) => match (lv, rv) {
                (Value::String(lv_s), Value::String(rv_s)) => lv_s == rv_s,
                (Value::Number(num), Value::Number(inum)) => num == inum,
                _ => false,
            },
            _ => false,
        };

        self.ruledat = node.ruledat;
        self.result = Some(bval);
    }

    pub fn contains(mut self, node: &EvalNode<'a>) -> EvalNode<'a> {
        self.contains_(node);
        self
    }

    pub fn equals(mut self, node: &EvalNode<'a>) -> EvalNode<'a> {
        self.equals_(node);
        self
    }

    pub fn gt(mut self, node: &EvalNode<'a>) -> EvalNode<'a> {
        self.gt_(node);
        self
    }

    pub fn lt(mut self, node: &EvalNode<'a>) -> EvalNode<'a> {
        self.lt_(node);
        self
    }

    pub fn gtoe(mut self, node: &EvalNode<'a>) -> EvalNode<'a> {
        self.gtoe_(node);
        self
    }

    pub fn ltoe(mut self, node: &EvalNode<'a>) -> EvalNode<'a> {
        self.ltoe_(node);
        self
    }

    fn doand_(&mut self, node: &EvalNode<'a>) {
        let b = match (self.result, node.result) {
            (Some(ar), Some(br)) => ar && br,
            _ => false,
        };

        self.result = Some(b);
    }

    fn door_(&mut self, node: &EvalNode<'a>) {
        let b = match (self.result, node.result) {
            (Some(ar), Some(br)) => ar || br,
            _ => false,
        };

        self.result = Some(b);
    }

    fn doand(mut self, node: &EvalNode<'a>) -> EvalNode<'a> {
        self.doand_(node);
        self
    }

    fn door(mut self, node: &EvalNode<'a>) -> EvalNode<'a> {
        self.door_(node);
        self
    }
}

#[derive(Debug, Copy, Clone)]
pub enum ErrorCode {
    NoError,
    KeyNotFound,
    RegexError,
    ParseError,
}

impl Error for ErrorCode {}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Node {
    pub fn eval<'a>(node: &'a Node, vals: &'a Value) -> EvalNode<'a> {
        match node {
            Node::Op(a, b, c) => match b {
                Operator::Con => Node::eval(&a, vals)
                    .set_val_if_missing(vals)
                    .contains(&Node::eval(&c, vals)),
                Operator::Eq => Node::eval(&a, vals)
                    .set_val_if_missing(vals)
                    .equals(&Node::eval(&c, vals)),
                Operator::Gt => Node::eval(&a, vals)
                    .set_val_if_missing(vals)
                    .gt(&Node::eval(&c, vals)),
                Operator::Lt => Node::eval(&a, vals)
                    .set_val_if_missing(vals)
                    .lt(&Node::eval(&c, vals)),
                Operator::Gtoe => Node::eval(&a, vals)
                    .set_val_if_missing(vals)
                    .gtoe(&Node::eval(&c, vals)),
                Operator::Ltoe => Node::eval(&a, vals)
                    .set_val_if_missing(vals)
                    .ltoe(&Node::eval(&c, vals)),
                Operator::Dot => Node::eval(&a, vals)
                    .set_val_if_missing(vals)
                    .dot_node(&Node::eval(&c, vals)),
                Operator::And => Node::eval(&a, vals).doand(&Node::eval(&c, vals)),
                Operator::Or => Node::eval(&a, vals).door(&Node::eval(&c, vals)),
                _ => EvalNode {
                    lastkey: None,
                    ruledat: &Node::None,
                    lastval: None,
                    result: None,
                    error: ErrorCode::NoError,
                },
            },
            Node::Neg(_, n) => {
                let mut enode: EvalNode = Node::eval(&n, vals);
                match enode.result {
                    Some(r) => {
                        if r {
                            enode.result = Some(false)
                        } else {
                            enode.result = Some(true)
                        }
                    }
                    _ => enode.result = None,
                };
                enode
            }
            Node::Lastkey(s) => EvalNode {
                lastkey: Some(s),
                ruledat: &Node::None,
                lastval: None,
                result: None,
                error: ErrorCode::NoError,
            },
            Node::Ruleval(_) | Node::Rex(_) | Node::List(_) | Node::Glob(_) => EvalNode {
                lastkey: None,
                ruledat: node,
                lastval: None,
                result: None,
                error: ErrorCode::NoError,
            },
            _ => EvalNode {
                lastkey: None,
                ruledat: &Node::None,
                lastval: None,
                result: None,
                error: ErrorCode::NoError,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob() {
        let json = r#"{"key1": "data"}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key1:g\"da?a\"").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_contains() {
        let json = r#"{"key1": "data3"}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key1:\"data\"").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_equals() {
        let json = r#"{"key1": "data1"}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key1=\"data1\"").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_subkey_equals() {
        let json = r#"{"key1": {"sub1": "data1"}}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key1.sub1=\"data1\"").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_subkey_contains() {
        let json = r#"{"key1": {"sub1": "data1"}}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key1.sub1:\"dat\"").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
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
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key1.subkey1:\"data1\" key3:\"data3\" or (key1.subkey1:\"dat\" and key2.subkey2=\"data2\")").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_int_eq() {
        let json = r#"{"key":5}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key=5").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_int_gt() {
        let json = r#"{"key":5}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key>1").unwrap();
        r.add_rule(1, "key>4").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_negint_gt() {
        let json = r#"{"key":5}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key>-6").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_int_lt() {
        let json = r#"{"key":5}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key<10").unwrap();
        r.add_rule(1, "key<9").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_double_lt() {
        let json = r#"{"key":5.0}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key<10.0").unwrap();
        r.add_rule(1, "key<9.0").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_int_ltoe() {
        let json = r#"{"key":5}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key<=5").unwrap();
        r.add_rule(1, "key<=11").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_int_gtoe() {
        let json = r#"{"key":5}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key >= 3").unwrap();
        r.add_rule(1, "key >= 4").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_int_gtoe_double() {
        let json = r#"{"key":5.0}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key >= 3.5").unwrap();
        r.add_rule(1, "key >= 4.5").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_regex() {
        let json = r#"{"key":"data"}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:/d.*/").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_regex_false() {
        let json = r#"{"key":"data"}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:/e.*/").unwrap();
        assert_eq!(false, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_negregex() {
        let json = r#"{"key":"data"}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "NOT key:/d.*/").unwrap();
        assert_eq!(false, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_regex_bug() {
        let json = r#"{"key":"\\/"}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, r#"key:/\\//"#).unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
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
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "!(key1.subkey1:\"data1\" NOT key3:\"data3\" or NOT (key1.subkey1:\"dat\" and key2.subkey2=\"data2\"))").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_list_contains_str() {
        let json = r#"{"key":"data"}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:[\"foo\", \"data\", \"bar\"]").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_list_contains_int() {
        let json = r#"{"key":4}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:[3, 4, 5]").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_list_contains_float() {
        let json = r#"{"key":4.1}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:[3.1, 4.1, 5.1]").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_jsonlist_contains_str() {
        let json = r#"{"key":["v1", "v2", "v3"]}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:\"v2\"").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_jsonlist_contains_int() {
        let json = r#"{"key":[3, 4, 5]}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:4").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_jsonlist_contains_float() {
        let json = r#"{"key":[3.1, 4.1, 5.1]}"#;
        let vals: Value = serde_json::from_str(json).unwrap();
        let mut r = Rules::default();
        r.add_rule(0, "key:4.1").unwrap();
        assert_eq!(true, r.eval_first_wins(&vals).unwrap().is_some());
    }

    #[test]
    fn test_bad_rule_syntax() {
        let mut r = Rules::default();
        assert_eq!(true, r.add_rule(0, "\"key").is_err());
    }
}
