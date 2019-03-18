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

mod stage1;

use crate::errors::*;
use crate::{Context, Registry, TremorFn, Value, ValueMap};
use glob::Pattern;
use lazy_static::lazy_static;
use pcre2::bytes::Regex;
use stage1::{parser, Id, Interface, CIDR};
use std::fmt;
use std::str::FromStr;

lazy_static! {
    static ref IP_REGEX: regex::Regex =
        regex::Regex::new(r"(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,2})/?(\d{1,2})?").unwrap();
}

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

#[derive(Debug)]
pub struct Script<Ctx: Context + 'static> {
    // TODO: This could become a fixed sized vector
    locals: ValueMap,
    interface: Interface,
    statements: Vec<Stmt<Ctx>>,
}

impl<Ctx: Context + 'static> Script<Ctx> {
    pub fn parse(script: &str, registry: &Registry<Ctx>) -> Result<Self> {
        let stage1 = parser::ScriptParser::new()
            .parse(script)
            .map_err(|e| Error::from(ErrorKind::ParserError(format!("{}", e))))?;
        stage1.bind(&registry)
    }
    pub fn run(
        &mut self,
        context: &Ctx,
        value: &mut Value,
        state: &mut ValueMap,
    ) -> Result<Option<usize>> {
        let mut result: Option<usize> = None;
        let mut i = 0;
        'outer: for statement in self.statements.iter_mut() {
            if statement.test(context, value, &mut self.locals, state)? {
                result = Some(i);
                i += 1;
                for action in &mut statement.actions {
                    if action.execute(context, value, &mut self.locals, state)? {
                        break 'outer;
                    }
                }
            };
        }
        self.locals.clear();
        Ok(result)
    }
    pub fn new(interface: Interface, statements: Vec<Stmt<Ctx>>) -> Self {
        Script {
            locals: ValueMap::new(),
            interface,
            statements,
        }
    }
}

#[derive(Debug)]
pub struct Stmt<Ctx: Context + 'static> {
    item: Item<Ctx>,
    actions: Vec<Action<Ctx>>,
}

impl<Ctx: Context + 'static> Stmt<Ctx> {
    pub fn test(
        &mut self,
        context: &Ctx,
        value: &Value,
        locals: &mut ValueMap,
        globals: &mut ValueMap,
    ) -> Result<bool> {
        self.item.test(context, value, locals, globals)
    }
    pub fn new(item: Item<Ctx>, actions: Vec<Action<Ctx>>) -> Self {
        Self { item, actions }
    }
}
pub enum RHSValue<Ctx: Context + 'static> {
    Literal(Value),
    Lookup(Vec<Id>),
    LookupLocal(Id),
    LookupGlobal(Id),
    List(Vec<RHSValue<Ctx>>, Value), // TODO: Split out const list for optimisation
    Function(String, String, TremorFn<Ctx>, Vec<RHSValue<Ctx>>, Value),
}

impl<Ctx: Context + 'static> fmt::Debug for RHSValue<Ctx> {
    fn fmt(&self, fmter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RHSValue::Literal(v) => write!(fmter, "Literal({:?})", v),
            RHSValue::Lookup(v) => write!(fmter, "Lookup({:?})", v),
            RHSValue::LookupLocal(v) => write!(fmter, "LookupLocal({:?})", v),
            RHSValue::LookupGlobal(v) => write!(fmter, "LookupGlobal({:?})", v),
            RHSValue::List(v, _) => write!(fmter, "List({:?})", v),
            RHSValue::Function(m, f, _, args, _) => write!(fmter, "{}::{}({:?})", m, f, args),
        }
    }
}

impl<Ctx: Context + 'static> RHSValue<Ctx> {
    pub fn reduce<'v, 's: 'v, 'l: 'v, 'g: 'v>(
        &'s mut self,
        context: &Ctx,
        data: &'v Value,
        locals: &'l ValueMap,
        globals: &'g ValueMap,
    ) -> Option<&'v Value> {
        match self {
            RHSValue::Literal(l) => Some(l),
            RHSValue::Lookup(_path) => self.find(0, data),
            RHSValue::LookupLocal(id) => Some(locals.get(id.id())?),
            RHSValue::LookupGlobal(id) => {
                // If we hace set an imported variable it's goiung to be in local
                // and shadow the global one so we need to check here first.
                if let Some(v) = locals.get(id.id()) {
                    Some(v)
                } else if let Some(v) = globals.get(id.id()) {
                    Some(v)
                } else {
                    None
                }
            }
            RHSValue::List(ref mut list, ref mut computed) => {
                // Some(&Value::Array(list.map(|e| e.reduce(..).to_owned())))
                let out = if let Value::Array(a) = computed {
                    a.clear();
                    a
                } else {
                    unreachable!()
                };
                for e in list {
                    if let Some(v) = e.reduce(context, data, locals, globals) {
                        out.push(v.to_owned())
                    } else {
                        return None;
                    }
                }
                Some(computed)
            }
            RHSValue::Function(_, _, f, a, ref mut result) => {
                let mut args = Vec::new();
                for e in a {
                    if let Some(v) = e.reduce(context, data, locals, globals) {
                        args.push(v.to_owned())
                    } else {
                        return None;
                    }
                }
                *result = f(context, &args).unwrap();
                Some(result)
            }
        }
    }
    fn find<'v>(&self, i: usize, value: &'v Value) -> Option<&'v Value> {
        if let RHSValue::Lookup(ks) = self {
            if let Some(key) = ks.get(i) {
                let v1 = value.get(key.id())?;
                self.find(i + 1, v1)
            } else {
                Some(value)
            }
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub enum Item<Ctx: Context + 'static> {
    Filter(Filter<Ctx>),
    Not(Box<Item<Ctx>>),
    And(Box<Item<Ctx>>, Box<Item<Ctx>>),
    Or(Box<Item<Ctx>>, Box<Item<Ctx>>),
}

impl<Ctx: Context + 'static> Item<Ctx> {
    pub fn test(
        &mut self,
        context: &Ctx,
        value: &Value,
        locals: &mut ValueMap,
        globals: &mut ValueMap,
    ) -> Result<bool> {
        match self {
            Item::Not(item) => {
                let res = item.test(context, value, locals, globals)?;
                Ok(!res)
            }
            Item::And(left, right) => {
                if left.test(context, value, locals, globals)? {
                    right.test(context, value, locals, globals)
                } else {
                    Ok(false)
                }
            }
            Item::Or(left, right) => {
                if left.test(context, value, locals, globals)? {
                    Ok(true)
                } else {
                    right.test(context, value, locals, globals)
                }
            }
            Item::Filter(filter) => filter.test(context, value, locals, globals),
        }
    }
}

#[derive(Debug)]
pub enum Action<Ctx: Context + 'static> {
    Set { path: Vec<Id>, rhs: RHSValue<Ctx> },
    SetLocal { id: Id, rhs: RHSValue<Ctx> },
    SetGlobal { id: Id, rhs: RHSValue<Ctx> },
    Return,
}

impl<Ctx: Context + 'static> Action<Ctx> {
    pub fn execute(
        &mut self,
        context: &Ctx,
        json: &mut Value,
        locals: &mut ValueMap,
        globals: &mut ValueMap,
    ) -> Result<bool> {
        match self {
            Action::Set { path, rhs } => {
                // TODO what do we do if nothing was found?
                if let Some(v) = rhs.reduce(context, json, locals, globals) {
                    Action::<Ctx>::mut_value(json, path, 0, v.to_owned())?;
                };
                Ok(false)
            }
            Action::SetLocal { id, rhs } => {
                // TODO what do we do if nothing was found?
                if let Some(v) = rhs.reduce(context, json, locals, globals) {
                    locals.insert(id.id().clone(), v.to_owned());
                }
                Ok(false)
            }
            Action::SetGlobal { id, rhs } => {
                // TODO what do we do if nothing was found?
                if let Some(v) = rhs.reduce(context, json, locals, globals) {
                    //TODO: We kind of need to set both global na and local where
                    // we export but don't import
                    let v1 = v.clone();
                    let v2 = v.clone();
                    locals.insert(id.id().clone(), v1);
                    globals.insert(id.id().clone(), v2);
                }
                Ok(false)
            }
            Action::Return => Ok(true),
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
            } else if let Some(ref mut v) = json.get_mut(key.id()) {
                match v {
                    Object(_) => Action::<Ctx>::mut_value(v, ks, i + 1, val),
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
pub enum Filter<Ctx: Context + 'static> {
    Value(Vec<Id>, Cmp<Ctx>),
    Local(Id, Cmp<Ctx>),
    Global(Id, Cmp<Ctx>),
}

impl<Ctx: Context + 'static> Filter<Ctx> {
    pub fn test(
        &mut self,
        context: &Ctx,
        value: &Value,
        locals: &ValueMap,
        globals: &ValueMap,
    ) -> Result<bool> {
        match self {
            Filter::Value(_path, _rhs) => {
                self.find_and_test(context, 0, value, value, locals, globals)
            }
            Filter::Local(id, rhs) => {
                if let Some(val) = locals.get(id.id()) {
                    rhs.test(context, val, value, locals, globals)
                } else {
                    Ok(false)
                }
            }
            Filter::Global(id, rhs) => {
                // We might have to set a local variable that shadows a global one
                // so we need to cehck this fiorst.
                if let Some(val) = locals.get(id.id()) {
                    rhs.test(context, val, value, locals, globals)
                } else if let Some(val) = globals.get(id.id()) {
                    rhs.test(context, val, value, locals, globals)
                } else {
                    Ok(false)
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)] // We allow this here since it's a iterator function
    fn find_and_test(
        &mut self,
        context: &Ctx,
        i: usize,
        value: &Value,
        data: &Value,
        locals: &ValueMap,
        globals: &ValueMap,
    ) -> Result<bool> {
        if let Filter::Value(ks, rhs) = self {
            if let Some(key) = ks.get(i) {
                if let Some(v1) = value.get(key.id()) {
                    self.find_and_test(context, i + 1, v1, data, locals, globals)
                } else {
                    Ok(false)
                }
            } else {
                rhs.test(context, value, data, locals, globals)
            }
        } else {
            // we only call find_and test on Filter::Value
            unreachable!()
        }
    }
}

#[derive(Debug)]
pub enum Cmp<Ctx: Context + 'static> {
    Exists,
    Eq(RHSValue<Ctx>),
    Gt(RHSValue<Ctx>),
    Gte(RHSValue<Ctx>),
    Lt(RHSValue<Ctx>),
    Lte(RHSValue<Ctx>),
    Contains(RHSValue<Ctx>),
    Regex(Regex),
    Glob(Pattern),
    CIDRMatch(CIDR),
}

impl<Ctx: Context + 'static> Cmp<Ctx> {
    pub fn test(
        &mut self,
        context: &Ctx,
        event_value: &Value,
        data: &Value,
        locals: &ValueMap,
        globals: &ValueMap,
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

                        Ok(cidr.contains(ip))
                    } else {
                        Ok(false)
                    }
                }
                _ => Ok(false),
            },
            Cmp::Eq(expected_value) => {
                if let Some(v) = expected_value.reduce(context, data, locals, globals) {
                    Ok(event_value == v)
                } else {
                    Ok(false)
                }
            }
            Cmp::Gt(expected_value) => {
                match (
                    event_value,
                    &expected_value.reduce(context, data, locals, globals),
                ) {
                    (Number(event_value), Some(Number(expected_value))) => {
                        compare_numbers!(event_value, expected_value, >)
                    }
                    (String(s), Some(String(is))) => Ok(s > is),
                    _ => Ok(false),
                }
            }

            Cmp::Lt(expected_value) => {
                match (
                    event_value,
                    &expected_value.reduce(context, data, locals, globals),
                ) {
                    (Number(event_value), Some(Number(expected_value))) => {
                        compare_numbers!(event_value, expected_value, <)
                    }
                    (String(s), Some(String(is))) => Ok(s < is),
                    _ => Ok(false),
                }
            }

            Cmp::Gte(expected_value) => {
                match (
                    event_value,
                    &expected_value.reduce(context, data, locals, globals),
                ) {
                    (Number(event_value), Some(Number(expected_value))) => {
                        compare_numbers!(event_value, expected_value, >=)
                    }
                    (String(s), Some(String(is))) => Ok(s >= is),
                    _ => Ok(false),
                }
            }
            Cmp::Lte(expected_value) => {
                match (
                    event_value,
                    &expected_value.reduce(context, data, locals, globals),
                ) {
                    (Number(event_value), Some(Number(expected_value))) => {
                        compare_numbers!(event_value, expected_value, <=)
                    }
                    (String(s), Some(String(is))) => Ok(s <= is),
                    _ => Ok(false),
                }
            }
            Cmp::Contains(expected_value) => {
                match (
                    event_value,
                    &expected_value.reduce(context, data, locals, globals),
                ) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::registry;
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
        let r: Registry<()> = registry();
        let s = Script::parse(script, &r).unwrap();
        assert_eq!(&s.interface.imports()[0], "imported_var");
    }
}
