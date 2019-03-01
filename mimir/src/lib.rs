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
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":"data"}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key:/d.*/").unwrap();
//! assert_eq!(true, s.run(&(), &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Test regex not matching the string data and evaluating to false
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":"data"}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key:/e.*/").unwrap();
//! assert_eq!(false, s.run(&(), &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Test negations with compound rule
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{
//!                 "key1": {
//!                             "subkey1": "data1"
//!                         },
//!                 "key2": {
//!                             "subkey2": "data2"
//!                         },
//!                 "key3": "data3"
//!             }"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "!(key1.subkey1:\"data1\" OR NOT key3:\"data3\" or NOT (key1.subkey1:\"dat\" and key2.subkey2=\"data2\"))").unwrap();
//! assert_eq!(false, s.run(&(), &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Test find string value in mimir array.  This search uses the ':' contains in array operator meaning
//! in this case one of the values "foo", "data", or "bar".  Since there is a 'key' element containing
//! the value "data" in the document the result of this rule evaluation will be true.
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":"data"}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( r#"key:["foo", "data", "bar"]"#).unwrap();
//! assert_eq!(true, s.run(&(), &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Test find int value in mimir array
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":4}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key:[3, 4, 5]").unwrap();
//! assert_eq!(true, s.run(&(), &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Test find float value in mimir array
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":4.1}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key:[3.1, 4.1, 5.1]").unwrap();
//! assert_eq!(true, s.run(&(), &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Find string value in array from document using the contains (":") operator.  In this case
//! there is a string array element in the document keyed by 'key' containing the values
//! "v1", "v2" and "v3.  The rule key:"v2" will be a match since "v2" is contained in the document
//! array.
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":["v1", "v2", "v3"]}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key:\"v2\"").unwrap();
//! assert_eq!(true, s.run(&(), &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Test find int value in json array
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":[3, 4, 5]}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key:4").unwrap();
//! assert_eq!(true, s.run(&(), &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Test find float value in json array
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":[3.1, 4.1, 5.1]}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key:4.1").unwrap();
//! assert_eq!(true, s.run(&(), &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Test wildcard search with mimir
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":"data"}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key:g\"da?a\"").unwrap();
//! assert_eq!(true, s.run(&(), &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Test wildcard search with mimir (not a match)
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":"data"}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key:g\"daa?a\"").unwrap();
//! assert_eq!(false, s.run(&(), &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Testing glob match using star match
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key1": "this is a glob blahblah"}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key1:g\"this is a glob*\"").unwrap();
//! assert_eq!(true, s.run(&(),  &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Test equal comparison of two integers.
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":5}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key=5").unwrap();
//! assert_eq!(true, s.run(&(), &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Test greater than comparison of two integers.
//! The value 5 from the json data is compared if it is
//! greatere than 1 and greater than 4.  Returns true.
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":5}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key>1 key>4").unwrap();
//! assert_eq!(true, s.run(&(), &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Test greater than comparison of two integers.
//! The value 5 from the json data is compared if it is
//! greatere than -6.  Returns true.
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":5}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key>-6").unwrap();
//! assert_eq!(true, s.run(&(), &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Test less than comparison of two integers.
//! The value 5 from the json data is compared if it is
//! greater less than 10 and 9.  Returns true.
//!
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":5}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key<10 key<9").unwrap();
//! assert_eq!(true, s.run(&(), &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Test less than comparison of two floating point numbers.
//! The value 5.0 from the json data is compared if it is
//! less than 10 and 9.  Returns true.
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":5.0}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key<10.0 key<9.0").unwrap();
//! assert_eq!(true, s.run(&(), &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Test less than or equals comparison of two integers.
//! The value 5 from the json data is compared if it is
//! less than or equals to 11 and 5.  Returns true.
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":5}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key<=5 key<=11").unwrap();
//! assert_eq!(true, s.run(&(),  &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Test greater than or equals comparison of two integers.
//! The value 5 from the json data is compared if it is
//! greater than or equals to 3 and 4.  Returns true.
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":5}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key >= 3 key >= 4").unwrap();
//! assert_eq!(true, s.run(&(), &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```
//!
//! Test greater than or equals comparison of two floating point numbers.
//! The value 5.0 from the json data is compared if it is
//! greater than or equals to 3.5 and 4.5.  Returns true.
//! ```
//! use mimir::*; use std::collections::HashMap;
//! let json = r#"{"key":5.0}"#;
//! let mut vals: Value = serde_json::from_str(json).unwrap();
//! let mut s = Script::parse( "key >= 3.5 key >= 4.5").unwrap();
//! assert_eq!(true, s.run(&(),  &mut vals, &mut HashMap::new()).unwrap().is_some());
//! ```

pub mod errors;
pub mod registry;

use crate::errors::*;
use glob::Pattern;
use pcre2::bytes::Regex;
use registry::{Context, FnError, Registry, TremorFnWrapper};
pub use serde_json::Value;
use std::collections::HashMap;
use std::str;
use std::str::FromStr;
use std::string::ToString;
use std::sync::Mutex;

use hostname::get_hostname;
use lalrpop_util::lalrpop_mod;
use lazy_static::lazy_static;

pub type ValueMap = HashMap<String, Value>;

lazy_static! {
    static ref IP_REGEX: regex::Regex =
        regex::Regex::new(r"(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,2})/?(\d{1,2})?").unwrap();
    static ref REGISTRY: Mutex<Registry> = {
        let mut registry = Registry::default();
        #[allow(unused_variables)]
        registry
            .insert(tremor_fn! (math::max(_context, a: Number, b: Number){
                if a.as_f64() > b.as_f64() {
                    Ok(Value::Number(a.to_owned()))
                } else {
                    Ok(Value::Number(b.to_owned()))
                }
            }))
            .insert(tremor_fn!(math::min(_context, a: Number, b: Number) {
                if a.as_f64() < b.as_f64() {
                    Ok(Value::Number(a.to_owned()))

                } else {
                    Ok(Value::Number(b.to_owned()))
                }
            }))
            .insert(tremor_fn!(system::hostname(_context) {
                Ok(Value::String(get_hostname().unwrap()))
            }))
            .insert(TremorFnWrapper {
                module: "string".to_owned(),
                name: "format".to_string(),
                fun: |context, args| {
                    let format = args[0].as_str().to_owned();

                    match format {
                        Some(fmt) => {
                            let mut out = String::from(fmt);
                            for arg in args[1..].iter() {
                                if let Some(a) = arg.as_str() {
                                    out = out.replacen("{}", a, 1);
                                } else {
                                    return Err(FnError::ExecutionError);
                                }
                            }

                            Ok(Value::String(out.to_owned()))
                        }

                        None => Err(FnError::ExecutionError),
                    }
                },
            });

        Mutex::new(registry)
    };
}

lalrpop_mod!(
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::all))]
    parser
);

#[derive(Default, Debug)]
pub struct Interface {
    imports: Vec<Id>,
    exports: Vec<Id>,
}

#[derive(Debug)]
pub struct Script {
    interface: Interface,
    statements: Vec<Stmt>,
}

impl Script {
    pub fn parse(script: &str) -> Result<Self> {
        parser::ScriptParser::new()
            .parse(script)
            .map_err(|e| ErrorKind::ParserError(e.to_string()).into())
    }
    pub fn run<T: Context + 'static>(
        &self,
        context: &T,
        value: &mut Value,
        state: &mut ValueMap,
    ) -> Result<Option<usize>> {
        let mut m: ValueMap = HashMap::new();
        let mut result: Option<usize> = None;
        // Copy state variables that are imported
        m.extend(
            self.interface
                .imports
                .iter()
                .filter_map(|import| Some((import.id.clone(), state.get(&import.id)?.clone()))),
        );
        'outer: for (i, statement) in self.statements.iter().enumerate() {
            if statement.test(context, value, &m)? {
                result = Some(i);
                for action in &statement.actions {
                    if action.execute(context, value, &mut m)? {
                        break 'outer;
                    }
                }
            };
        }

        // copy updated variables that are exported
        state.extend(
            self.interface
                .exports
                .iter()
                .filter_map(|export| Some((export.id.clone(), m.get(&export.id)?.clone()))),
        );

        Ok(result)
    }
}
#[derive(Debug)]
pub struct Id {
    id: String,
    quoted: bool,
}

impl PartialEq<String> for Id {
    fn eq(&self, s: &String) -> bool {
        &self.id == s
    }
}

impl PartialEq<str> for Id {
    fn eq(&self, s: &str) -> bool {
        self.id == s
    }
}

impl ToString for Id {
    fn to_string(&self) -> String {
        self.id.to_string()
    }
}

impl Id {
    fn new(s: &str) -> Self {
        if &s[0..=0] == "'" {
            Self {
                id: s.trim_matches('\'').into(),
                quoted: true,
            }
        } else {
            Self {
                id: s.into(),
                quoted: false,
            }
        }
    }
}

#[derive(Debug)]
pub enum Path {
    DataPath(Vec<Id>),
    Var(Id),
}

#[derive(Debug)]
pub enum RHSValue {
    Literal(Value),
    Lookup(Path),
    Function(TremorFnWrapper, Vec<RHSValue>),
    List(Vec<RHSValue>), // TODO: Split out const list for optimisation
}

impl RHSValue {
    pub fn reduce<T: Context + 'static>(
        &self,
        context: &T,
        data: &Value,
        vars: &ValueMap,
    ) -> Option<Value> {
        match self {
            RHSValue::Literal(l) => Some(l.to_owned()),
            RHSValue::Lookup(Path::DataPath(path)) => self.find(&path, 0, data),
            RHSValue::Lookup(Path::Var(var)) => Some(vars.get(&var.id)?.clone()),
            RHSValue::List(list) => {
                let out: Vec<Value> = list
                    .iter()
                    .filter_map(|i| i.reduce(context, data, vars))
                    .collect();
                if out.len() == list.len() {
                    Some(Value::Array(out))
                } else {
                    None
                }
            }
            RHSValue::Function(f, a) => Some(
                f.invoke::<T>(
                    context.to_owned(),
                    a.iter()
                        .map(|val| val.reduce(context, data, vars).unwrap())
                        .collect::<Vec<Value>>()
                        .as_slice(),
                )
                .unwrap(),
            ),
        }
    }
    fn find(&self, ks: &[Id], i: usize, value: &Value) -> Option<Value> {
        if let Some(key) = ks.get(i) {
            let v1 = value.get(&key.id)?;
            self.find(ks, i + 1, v1)
        } else {
            Some(value.to_owned())
        }
    }
}

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
    pub fn test<T: Context + 'static>(
        &self,
        context: &T,
        value: &Value,
        vars: &ValueMap,
    ) -> Result<bool> {
        self.item.test(context, value, vars)
    }
}

#[derive(Debug)]
pub enum Action {
    Set { lhs: Path, rhs: RHSValue },
    Return,
}

impl Action {
    pub fn execute<T: Context + 'static>(
        &self,
        context: &T,
        json: &mut Value,
        vars: &mut ValueMap,
    ) -> Result<bool> {
        match self {
            Action::Set {
                lhs: Path::DataPath(path),
                rhs,
            } => {
                // TODO what do we do if nothing was found?
                if let Some(v) = rhs.reduce(context, json, vars) {
                    Action::mut_value(json, path, 0, v)?;
                };
                Ok(false)
            }
            Action::Set {
                lhs: Path::Var(var),
                rhs,
            } => {
                // TODO what do we do if nothing was found?
                if let Some(v) = rhs.reduce(context, json, vars) {
                    vars.insert(var.id.clone(), v);
                }
                Ok(false)
            }
            _ => Ok(true),
        }
    }

    fn mut_value(json: &mut Value, ks: &[Id], i: usize, val: Value) -> Result<()> {
        use serde_json::Map;
        use serde_json::Value::*;
        fn make_nested_object(ks: &[Id], i: usize, val: Value) -> Value {
            if let Some(key) = ks.get(i) {
                let mut m = Map::new();
                m.insert(key.to_string(), make_nested_object(ks, i + 1, val));
                Object(m)
            } else {
                val
            }
        }
        if !json.is_object() {
            Err(ErrorKind::MutationTypeConflict("not an object".into()).into())
        } else if let Some(key) = ks.get(i) {
            // This is the last key
            let last = i + 1 >= ks.len();
            if last {
                match json {
                    Object(ref mut m) => {
                        m.insert(key.to_string(), val);
                        Ok(())
                    }
                    _ => Err(ErrorKind::MutationTypeConflict("not an object".to_owned()).into()),
                }
            } else if let Some(ref mut v) = json.get_mut(&key.id) {
                match v {
                    Object(_) => Action::mut_value(v, ks, i + 1, val),
                    _ => Err(ErrorKind::MutationTypeConflict("not an object".to_owned()).into()),
                }
            } else {
                match json {
                    Object(ref mut m) => {
                        m.insert(key.to_string(), make_nested_object(ks, i + 1, val));
                        Ok(())
                    }
                    _ => Err(ErrorKind::MutationTypeConflict("not an object".to_owned()).into()),
                }
            }
        } else {
            Err(ErrorKind::BadPath("empty".into()).into())
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
    pub fn test<T: Context + 'static>(
        &self,
        context: &T,
        value: &Value,
        vars: &ValueMap,
    ) -> Result<bool> {
        match self {
            Item::Not(item) => {
                let res = item.test(context, value, vars)?;
                Ok(!res)
            }
            Item::And(left, right) => {
                if left.test(context, value, vars)? {
                    right.test(context, value, vars)
                } else {
                    Ok(false)
                }
            }
            Item::Or(left, right) => {
                if left.test(context, value, vars)? {
                    Ok(true)
                } else {
                    right.test(context, value, vars)
                }
            }
            Item::Filter(filter) => filter.test(context, value, vars),
        }
    }
}

#[derive(Debug)]
pub struct Filter {
    lhs: Path,
    rhs: Cmp,
}

impl Filter {
    pub fn test<T: Context + 'static>(
        &self,
        context: &T,
        value: &Value,
        vars: &ValueMap,
    ) -> Result<bool> {
        match &self.lhs {
            Path::DataPath(ref path) => self.find_and_test(context, &path, 0, value, value, vars),
            Path::Var(ref var) => match vars.get(&var.id) {
                Some(val) => self.rhs.test(context, val, value, vars),
                _ => Ok(false),
            },
        }
    }

    fn find_and_test<T: Context + 'static>(
        &self,
        context: &T,
        ks: &[Id],
        i: usize,
        value: &Value,
        data: &Value,
        vars: &ValueMap,
    ) -> Result<bool> {
        if let Some(key) = ks.get(i) {
            if let Some(v1) = value.get(&key.id) {
                self.find_and_test(context, ks, i + 1, v1, data, vars)
            } else {
                Ok(false)
            }
        } else {
            self.rhs.test(context, value, data, vars)
        }
    }
}

#[derive(Debug)]
enum Cmp {
    Exists,
    Eq(RHSValue),
    Gt(RHSValue),
    Gte(RHSValue),
    Lt(RHSValue),
    Lte(RHSValue),
    Contains(RHSValue),
    Regex(Regex),
    Glob(Pattern),
    CIDRMatch(CIDR),
}
impl Cmp {
    pub fn test<T: Context + 'static>(
        &self,
        context: &T,
        event_value: &Value,
        data: &Value,
        vars: &ValueMap,
    ) -> Result<bool> {
        use serde_json::Value::*;
        match self {
            Cmp::Exists => Ok(true),
            Cmp::Glob(g) => match event_value {
                String(gmatch) => Ok(g.matches(gmatch)),
                _ => Ok(false),
            },
            Cmp::Regex(rx) => match event_value {
                String(rxmatch) => rx
                    .is_match(&rxmatch.as_bytes())
                    .map_err(|_| ErrorKind::RegexpError(format!("{:?}", rx)).into()),

                _ => Ok(false),
            },
            Cmp::CIDRMatch(cidr) => match event_value {
                String(ip_str) => {
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
            Cmp::Eq(expected_value) => {
                if let Some(v) = expected_value.reduce(context, data, vars) {
                    Ok(event_value == &v)
                } else {
                    Ok(false)
                }
            }
            Cmp::Gt(expected_value) => {
                match (event_value, &expected_value.reduce(context, data, vars)) {
                    (Number(event_value), Some(Number(expected_value))) => {
                        compare_numbers!(event_value, expected_value, >)
                    }
                    (String(s), Some(String(is))) => Ok(s > is),
                    _ => Ok(false),
                }
            }

            Cmp::Lt(expected_value) => {
                match (event_value, &expected_value.reduce(context, data, vars)) {
                    (Number(event_value), Some(Number(expected_value))) => {
                        compare_numbers!(event_value, expected_value, <)
                    }
                    (String(s), Some(String(is))) => Ok(s < is),
                    _ => Ok(false),
                }
            }

            Cmp::Gte(expected_value) => {
                match (event_value, &expected_value.reduce(context, data, vars)) {
                    (Number(event_value), Some(Number(expected_value))) => {
                        compare_numbers!(event_value, expected_value, >=)
                    }
                    (String(s), Some(String(is))) => Ok(s >= is),
                    _ => Ok(false),
                }
            }
            Cmp::Lte(expected_value) => {
                match (event_value, &expected_value.reduce(context, data, vars)) {
                    (Number(event_value), Some(Number(expected_value))) => {
                        compare_numbers!(event_value, expected_value, <=)
                    }
                    (String(s), Some(String(is))) => Ok(s <= is),
                    _ => Ok(false),
                }
            }
            Cmp::Contains(expected_value) => {
                match (event_value, &expected_value.reduce(context, data, vars)) {
                    (String(event_value), Some(String(expected_value))) => {
                        Ok(event_value.contains(expected_value))
                    }
                    (Number(event_value), Some(Number(expected_value))) => {
                        compare_numbers!(event_value, expected_value, ==)
                    }
                    (Array(event_value), Some(v)) => Ok(event_value.contains(v)),
                    (event_value, Some(Array(expected_value))) => {
                        Ok(expected_value.contains(event_value))
                    }

                    _ => Ok(false),
                }
            }
        }
    }
}

#[macro_export]
macro_rules! compare_numbers {
    {$x:expr, $y:expr, $operator:tt} => {
        {
            let x = $x;
            let y = $y;

            if x.is_i64() & & y.is_i64() {
                Ok(x.as_i64().unwrap() $operator y.as_i64().unwrap())
            } else if x.is_f64() & & y.is_f64() {
              #[allow(clippy::float_cmp)] // TODO: define a error
                Ok(x.as_f64().unwrap() $operator y.as_f64().unwrap())
            } else {
                Ok(false)
              }
            }
        }
    }

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_glob() {
        let json = r#"{"key1": "data"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key1:g\"da?a\"").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_contains() {
        let json = r#"{"key1": "data3"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse(r#"key1:"data""#).unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_equals() {
        let json = r#"{"key1": "data1"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key1=\"data1\"").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_quote() {
        let json = r#"{"key1": "da\\u1234ta1"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key1=\"da\\u1234ta1\"").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_subkey_equals() {
        let json = r#"{"key1": {"sub1": "data1"}}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key1.sub1=\"data1\"").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_subkey_contains() {
        let json = r#"{"key1": {"sub1": "data1"}}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key1.sub1:\"dat\"").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
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
        let s =Script::parse("key1.subkey1:\"data1\" or key3:\"data3\" or (key1.subkey1:\"dat\" and key2.subkey2=\"data2\")").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_int_eq() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key=5").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_int_gt() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key>1 OR key>4").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_negint_gt() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key>-6").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_int_lt() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key<10 key<9").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_double_lt() {
        let json = r#"{"key":5.0}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key<10.0 key<9.0").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_int_ltoe() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key<=5 key<=11").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_int_gtoe() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key >= 3 key >= 4").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_int_gtoe_double() {
        let json = r#"{"key":5.0}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key >= 3.5 key >= 4.5").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_regex() {
        let json = r#"{"key":"data"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key:/d.*/").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_regex_false() {
        let json = r#"{"key":"data"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key:/e.*/").unwrap();
        assert_eq!(
            false,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_negregex() {
        let json = r#"{"key":"data"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("NOT key:/d.*/").unwrap();
        assert_eq!(
            false,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_regex_bug() {
        let json = r#"{"key":"\\/"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse(r#"key:/\\//"#).unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
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
        let s =Script::parse("!(key1.subkey1:\"data1\" OR NOT (key3:\"data3\") OR NOT (key1.subkey1:\"dat\" and key2.subkey2=\"data2\"))").unwrap();
        assert_eq!(
            false,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_list_contains_str() {
        let json = r#"{"key":"data"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key:[\"foo\", \"data\", \"bar\"]").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_list_contains_int() {
        let json = r#"{"key":4}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key:[3, 4, 5]").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_list_contains_float() {
        let json = r#"{"key":4.1}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key:[3.1, 4.1, 5.1]").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_jsonlist_contains_str() {
        let json = r#"{"key":["v1", "v2", "v3"]}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key:\"v2\"").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_jsonlist_contains_int() {
        let json = r#"{"key":[3, 4, 5]}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key:4").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_jsonlist_contains_float() {
        let json = r#"{"key":[3.1, 4.1, 5.1]}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key:4.1").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_bad_rule_syntax() {
        assert_eq!(true, Script::parse("\"key").is_err());
    }

    #[test]
    fn test_ip_match() {
        let json = r#"{"key":"10.66.77.88"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key:10.66.77.88").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_cidr_match() {
        let json = r#"{"key":"10.66.77.88"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key:10.66.0.0/16").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_ip_match_false() {
        let json = r#"{"key":"10.66.78.88"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key:10.66.77.88").unwrap();
        assert_eq!(
            false,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_cidr_match_false() {
        let json = r#"{"key":"10.67.77.88"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key:10.66.0.0/16").unwrap();
        assert_eq!(
            false,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_exists() {
        let json = r#"{"key":"10.67.77.88"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_missing() {
        let json = r#"{"key":"10.67.77.88"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("!key").unwrap();
        assert_eq!(
            false,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_add() {
        let json = r#"{"key":"val"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse(r#"key="val" {key2 := "newval";}"#).unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
        assert_eq!(vals["key2"], json!("newval"));
    }

    #[test]
    fn test_add_nested() {
        let json = r#"{"key":"val"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse(r#"key="val" {newkey.newsubkey.other := "newval";}"#).unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
        assert_eq!(vals["newkey"]["newsubkey"]["other"], json!("newval"));
    }

    #[test]
    fn test_update() {
        let json = r#"{"key":"val"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse(r#"_ {key := "newval";}"#).unwrap();
        let _ = s.run(&(), &mut vals, &mut HashMap::new());
        //        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
        assert_eq!(vals["key"], json!("newval"));
    }

    #[test]
    fn test_update_nested() {
        let json = r#"{"key":{"key1": "val"}}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse(r#"_ {key.key1 := "newval";}"#).unwrap();
        let _ = s.run(&(), &mut vals, &mut HashMap::new());
        //        assert_eq!(Ok(Some(0)), s.run(&(), &mut vals, &mut HashMap::new()));
        assert_eq!(vals["key"]["key1"], json!("newval"));
    }

    #[test]
    fn test_update_type_conflict() {
        let json = r#"{"key":"key1"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse(r#"_ {key.key1 := "newval";}"#).unwrap();
        let _ = s.run(&(), &mut vals, &mut HashMap::new());

        // assert_eq!(
        //     Err(ErrorCode::MutationError(MutationError::TypeConflict)),
        //     s.run(&(), &mut vals, &mut HashMap::new())
        // );
        assert_eq!(vals["key"], json!("key1"));
    }

    #[test]
    fn test_mut_multi() {
        let json = r#"{"key":"val"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse(
            r#"key="val" {
                           newkey.newsubkey.other := "newval";
                           newkey.newsubkey.other2 := "newval2";
                         }"#,
        )
        .unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
        assert_eq!(
            Value::String("newval".to_string()),
            vals["newkey"]["newsubkey"]["other"]
        );
        assert_eq!(
            Value::String("newval2".to_string()),
            vals["newkey"]["newsubkey"]["other2"]
        );
    }

    #[test]
    fn test_underscore() {
        let json = r#"{"key":"val"}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse(r#"_"#).unwrap();
        assert_eq!(
            0,
            s.run(&(), &mut vals, &mut HashMap::new()).unwrap().unwrap()
        );
    }

    #[test]
    fn doctest_time_test() {
        let json = r#"{"key":5}"#;
        let mut vals: Value = serde_json::from_str(json).unwrap();
        let s = Script::parse("key<10 key<9").unwrap();
        assert_eq!(
            true,
            s.run(&(), &mut vals, &mut HashMap::new())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn script_test() {
        let script = r#"
            import imported_var;
            export classification, dimension, rate, index_type, below, timeframe;

            _ { $index_type := index; $below := 5; $timeframe := 10000; }

            application="app1 hello" { $classification := "applog_app1"; $rate := 1250; }
            application="app2" { $classification := "applog_app2"; $rate := 2500; $below := 10; }
            application="app3" { $classification := "applog_app3"; $rate := 18750; }
            application="app4" { $classification := "applog_app4"; $rate := 750; $below := 10;   }
            application="app5" { $classification := "applog_app5"; $rate := 18750; }
            $classification { $dimension := application; return; }

            index_type="applog_app6" { $dimension := logger_name; $classification := "applog_app6"; $rate := 4500; return; }

            index_type="syslog_app1" { $classification := "syslog_app1"; $rate := 2500; }
            tags:"tag1" { $classification := "syslog_app2"; $rate := 125; }
            index_type="syslog_app2" { $classification := "syslog_app2"; $rate := 125; }
            index_type="syslog_app3" { $classification := "syslog_app3"; $rate := 1750; }
            index_type="syslog_app4" { $classification := "syslog_app4"; $rate := 1750; }
            index_type="syslog_app5" { $classification := "syslog_app5"; $rate := 7500; }
            index_type="syslog_app6" { $classification := "syslog_app6"; $rate := 125; }
            $classification { $dimension := syslog_hostname; return; }

            index_type="edilog" { $classification := "edilog"; $dimension := syslog_hostname; $rate := 3750; return; }

             index_type="sqlserverlog" { $classification := "sqlserverlog"; $dimension := [src_ip, dst_ip]; $rate := 125; return; }

            # type="applog" { $classification := "applog"; $dimension := [src_ip, dst_ip]; $rate := 75; return; }

            _ { $classification := "default"; $rate := 250; }
"#;
        let s = Script::parse(script).unwrap();
        assert_eq!(&s.interface.imports[0], "imported_var");
    }

    #[test]
    fn test_mutate_var() {
        let mut m: ValueMap = HashMap::new();

        m.insert("a".to_string(), json!(1));
        m.insert("b".to_string(), json!(1));
        m.insert("c".to_string(), json!(1));

        let mut v = Value::Null;

        let script = r#"
import a, b;
export b, c, actual, actual1, actual2;


_ { $actual1:= false; $actual2 := false; $actual := false; }

$a=1 && $b=1 && !$c { $actual1 := true; }

_ { $a := 2; $b := 3; $c := 4; }

$a=2 && $b=3 && $c=4 && $actual1=true { $actual2 := true; }

$actual1=true && $actual2=true { $actual := true; }

"#;
        let s = Script::parse(script).unwrap();
        let _ = s.run(&(), &mut v, &mut m);
        assert_eq!(m["a"], json!(1));
        assert_eq!(m["b"], json!(3));
        assert_eq!(m["c"], json!(4));
        assert_eq!(m["actual1"], json!(true));
        assert_eq!(m["actual2"], json!(true));
        assert_eq!(m["actual"], json!(true));
    }

    #[test]
    fn test_mutate() {
        let mut m: ValueMap = HashMap::new();

        let mut v = json!(
            {
                "a": 1,
                "b": 1,
            }
        );
        let script = r#"
export actual, actual1, actual2;

_ { $actual1:= false; $actual2 := false; $actual := false; }

a=1 && b=1 && !c { $actual1 := true; }

_ { b := 3; c := 4; }

a=1 && b=3 && c=4 && $actual1=true { $actual2 := true; }

$actual1=true && $actual2=true { $actual := true; }

"#;
        let s = Script::parse(script).unwrap();
        let _ = s.run(&(), &mut v, &mut m);
        assert_eq!(v["a"], json!(1));
        assert_eq!(v["b"], json!(3));
        assert_eq!(v["c"], json!(4));
        assert_eq!(m["actual1"], json!(true));
        assert_eq!(m["actual2"], json!(true));
        assert_eq!(m["actual"], json!(true));
    }

    #[test]
    fn unknown_key_test() {
        let mut m: ValueMap = HashMap::new();
        let mut v = json!({"a": 1});
        let script = r#"
export b;
v:1 { $b := 2; return; }
_ { $b := 3; }
"#;
        let s = Script::parse(script).unwrap();
        let _ = s.run(&(), &mut v, &mut m);
        assert_eq!(v["a"], json!(1));
        assert_eq!(m["b"], json!(3));
    }
    #[test]
    #[ignore]
    fn comment_before_interface() {
        let mut m: ValueMap = HashMap::new();
        let mut v = json!({"a": 1});

        let script = r#"
# comment
export b;
_ { $b := 2; }
"#;
        let s = Script::parse(script).unwrap();
        let _ = s.run(&(), &mut v, &mut m);
        assert_eq!(v["a"], json!(1));
        assert_eq!(m["b"], json!(2));
    }

    #[test]
    fn comment_after_interface() {
        let mut m: ValueMap = HashMap::new();
        let mut v = json!({"a": 1});

        let script = r#"
export b;
# comment
_ { $b := 2; }
"#;
        let s = Script::parse(script).unwrap();
        let _ = s.run(&(), &mut v, &mut m);
        assert_eq!(v["a"], json!(1));
        assert_eq!(m["b"], json!(2));
    }

    #[test]
    pub fn registry_formats_string_with_misleading_brackets() {
        let script = r#"
          export format;

        _ { $format := string::format("{foo{}}", "bar");}
      "#;

        let s = Script::parse(script).unwrap();

        let mut m: ValueMap = HashMap::new();
        let mut v = json!({"v":1});
        let _ = s.run(&(), &mut v, &mut m);

        assert_eq!(m["format"], json!("{foobar}"));
    }

    #[test]
    #[ignore]
    fn test_constructed_lit_list() {
        assert!(false);
    }

    #[test]
    #[ignore]
    fn assign_var() {
        assert!(false);
    }

    #[test]
    #[ignore]
    fn compare_var() {
        assert!(false);
    }

    #[test]
    #[ignore]
    fn import_var() {
        assert!(false);
    }

    #[test]
    #[ignore]
    fn export_var() {
        assert!(false);
    }

    #[test]
    #[ignore]
    fn return_test() {
        assert!(false);
    }

    #[test]
    #[ignore]
    fn comment_test() {
        assert!(false);
    }

}
