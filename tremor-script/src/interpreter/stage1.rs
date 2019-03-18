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

use crate::errors::*;
use crate::interpreter::*;
use crate::{Context, Registry, Value};
use glob::Pattern;
use lalrpop_util::lalrpop_mod;
use pcre2::bytes::Regex;
use std::str;
use std::string::ToString;

lalrpop_mod!(
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::all))]
    pub parser
);

#[derive(Debug)]
pub struct Id {
    id: String,
    quoted: bool,
}

impl Id {
    pub fn id(&self) -> &String {
        &self.id
    }
}

impl PartialEq<Id> for Id {
    fn eq(&self, i: &Id) -> bool {
        self.id == i.id
    }
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
    pub fn new(s: &str) -> Self {
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
    pub fn contains(&self, ip: u32) -> bool {
        ip & self.mask == self.ip & self.mask
    }
}

#[derive(Default, Debug)]
pub struct Interface {
    imports: Vec<Id>,
    exports: Vec<Id>,
}

impl Interface {
    pub fn imports(&self) -> &Vec<Id> {
        &self.imports
    }
}

#[derive(Debug)]
pub struct ScriptStage1 {
    interface: Interface,
    statements: Vec<StmtStage1>,
}

impl ScriptStage1 {
    pub fn bind<Ctx: Context + 'static>(self, registry: &Registry<Ctx>) -> Result<Script<Ctx>> {
        let mut statements = Vec::new();
        for stmt in self.statements {
            statements.push(stmt.bind(&self.interface, registry)?);
        }
        Ok(Script::new(self.interface, statements))
    }
}

#[derive(Debug)]
pub enum RHSValueStage1 {
    Literal(Value),
    Lookup(Path),
    List(Vec<RHSValueStage1>), // TODO: Split out const list for optimisation
    Function(String, String, Vec<RHSValueStage1>),
}

impl RHSValueStage1 {
    pub fn bind<Ctx: Context + 'static>(
        self,
        interface: &Interface,
        registry: &Registry<Ctx>,
    ) -> Result<RHSValue<Ctx>> {
        Ok(match self {
            RHSValueStage1::Literal(v) => RHSValue::Literal(v),
            RHSValueStage1::Lookup(Path::DataPath(path)) => RHSValue::Lookup(path),
            RHSValueStage1::Lookup(Path::Var(v)) => {
                if interface.imports.contains(&v) {
                    RHSValue::LookupGlobal(v)
                } else {
                    RHSValue::LookupLocal(v)
                }
            }
            RHSValueStage1::List(l) => {
                let mut list = Vec::new();
                for i in l {
                    list.push(i.bind(interface, registry)?);
                }
                let len = list.len();
                RHSValue::List(list, Value::Array(Vec::with_capacity(len)))
            }
            RHSValueStage1::Function(m, f, a) => {
                let mut args = Vec::new();
                for arg in a {
                    args.push(arg.bind(interface, registry)?);
                }
                RHSValue::Function(
                    m.clone(),
                    f.clone(),
                    registry.find(&m, &f)?,
                    args,
                    Value::Null,
                )
            }
        })
    }
    /*
    fn is_static(&self) -> bool {
        match self {
            RHSValueStage1::Literal(_) => true,
            RHSValueStage1::List(l) => {
                for e in l {
                    if !e.is_static() {
                        return false;
                    }
                }
                true
            }
            _ => false,
        }
    }
    */
}

#[derive(Debug)]
pub enum ItemStage1 {
    Filter(FilterStage1),
    Not(Box<ItemStage1>),
    And(Box<ItemStage1>, Box<ItemStage1>),
    Or(Box<ItemStage1>, Box<ItemStage1>),
}
impl ItemStage1 {
    pub fn bind<Ctx: Context + 'static>(
        self,
        interface: &Interface,
        registry: &Registry<Ctx>,
    ) -> Result<Item<Ctx>> {
        Ok(match self {
            ItemStage1::Filter(f) => Item::Filter(f.bind(interface, registry)?),
            ItemStage1::Not(i) => Item::Not(Box::new(i.bind(interface, registry)?)),
            ItemStage1::And(l, r) => Item::And(
                Box::new(l.bind(interface, registry)?),
                Box::new(r.bind(interface, registry)?),
            ),
            ItemStage1::Or(l, r) => Item::Or(
                Box::new(l.bind(interface, registry)?),
                Box::new(r.bind(interface, registry)?),
            ),
        })
    }
}

#[derive(Debug)]
pub struct FilterStage1 {
    lhs: Path,
    rhs: CmpStage1,
}

impl FilterStage1 {
    pub fn bind<Ctx: Context + 'static>(
        self,
        interface: &Interface,
        registry: &Registry<Ctx>,
    ) -> Result<Filter<Ctx>> {
        let rhs = self.rhs.bind(interface, registry)?;
        Ok(match self.lhs {
            Path::DataPath(path) => Filter::Value(path, rhs),
            Path::Var(id) => {
                if interface.imports.contains(&id) {
                    Filter::Global(id, rhs)
                } else {
                    Filter::Local(id, rhs)
                }
            }
        })
    }
}

#[derive(Debug)]
pub enum CmpStage1 {
    Exists,
    Eq(RHSValueStage1),
    Gt(RHSValueStage1),
    Gte(RHSValueStage1),
    Lt(RHSValueStage1),
    Lte(RHSValueStage1),
    Contains(RHSValueStage1),
    Regex(Regex),
    Glob(Pattern),
    CIDRMatch(CIDR),
}

impl CmpStage1 {
    pub fn bind<Ctx: Context + 'static>(
        self,
        interface: &Interface,
        registry: &Registry<Ctx>,
    ) -> Result<Cmp<Ctx>> {
        Ok(match self {
            CmpStage1::Exists => Cmp::Exists,
            CmpStage1::Eq(v) => Cmp::Eq(v.bind(interface, registry)?),
            CmpStage1::Gt(v) => Cmp::Gt(v.bind(interface, registry)?),
            CmpStage1::Gte(v) => Cmp::Gte(v.bind(interface, registry)?),
            CmpStage1::Lt(v) => Cmp::Lt(v.bind(interface, registry)?),
            CmpStage1::Lte(v) => Cmp::Lte(v.bind(interface, registry)?),
            CmpStage1::Contains(v) => Cmp::Contains(v.bind(interface, registry)?),
            CmpStage1::Regex(v) => Cmp::Regex(v),
            CmpStage1::Glob(v) => Cmp::Glob(v),
            CmpStage1::CIDRMatch(v) => Cmp::CIDRMatch(v),
        })
    }
}

#[derive(Debug)]
pub struct StmtStage1 {
    item: ItemStage1,
    actions: Vec<ActionStage1>,
}
impl StmtStage1 {
    pub fn bind<Ctx: Context + 'static>(
        self,
        interface: &Interface,
        registry: &Registry<Ctx>,
    ) -> Result<Stmt<Ctx>> {
        let mut actions = Vec::new();
        for a in self.actions {
            actions.push(a.bind(interface, registry)?);
        }

        Ok(Stmt::new(self.item.bind(interface, registry)?, actions))
    }
}

#[derive(Debug)]
pub enum ActionStage1 {
    Set { lhs: Path, rhs: RHSValueStage1 },
    Return,
}

impl ActionStage1 {
    pub fn bind<Ctx: Context + 'static>(
        self,
        interface: &Interface,
        registry: &Registry<Ctx>,
    ) -> Result<Action<Ctx>> {
        Ok(match self {
            ActionStage1::Return => Action::Return,
            ActionStage1::Set { lhs, rhs } => match lhs {
                Path::DataPath(path) => Action::Set {
                    path,
                    rhs: rhs.bind(interface, registry)?,
                },
                Path::Var(id) => {
                    if interface.exports.contains(&id) {
                        Action::SetGlobal {
                            id,
                            rhs: rhs.bind(interface, registry)?,
                        }
                    } else {
                        Action::SetLocal {
                            id,
                            rhs: rhs.bind(interface, registry)?,
                        }
                    }
                }
            },
        })
    }
}
