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

pub mod base_expr;
pub mod query;
mod support;
pub mod upable;
use crate::errors::*;
use crate::impl_expr;
use crate::interpreter::{exec_binary2, exec_unary};
// TODO remove after testing
//use crate::interpreter::exec_binary2
use crate::pos::{Location, Range};
use crate::registry::{Aggr as AggrRegistry, Registry, TremorAggrFnWrapper, TremorFnWrapper};
use crate::tilde::Extractor;
use crate::EventContext;
pub use base_expr::BaseExpr;
use halfbrown::HashMap;
pub use query::*;
use serde::Serialize;
use simd_json::value::{borrowed, ValueTrait};
use simd_json::{BorrowedValue as Value, KnownKey};
use std::borrow::{Borrow, Cow};
use std::mem;
use upable::Upable;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Script1<'script> {
    pub exprs: Exprs1<'script>,
}

#[derive(Serialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Warning {
    pub outer: Range,
    pub inner: Range,
    pub msg: String,
}

pub struct Helper<'script, 'registry>
where
    'script: 'registry,
{
    reg: &'registry Registry,
    aggr_reg: &'registry AggrRegistry,
    can_emit: bool,
    is_in_aggr: bool,
    operators: Vec<OperatorDecl<'script>>,
    scripts: Vec<ScriptDecl<'script>>,
    aggregates: Vec<InvokeAggrFn<'script>>,
    warnings: Vec<Warning>,
    shadowed_vars: Vec<String>,
    pub locals: HashMap<String, usize>,
    pub consts: HashMap<String, usize>,
}

impl<'script, 'registry> Helper<'script, 'registry>
where
    'script: 'registry,
{
    pub fn has_locals(&self) -> bool {
        self.locals
            .iter()
            .any(|(n, _)| !n.starts_with(" __SHADOW "))
    }
    pub fn swap(
        &mut self,
        aggregates: &mut Vec<InvokeAggrFn<'script>>,
        consts: &mut HashMap<String, usize>,
        locals: &mut HashMap<String, usize>,
    ) {
        mem::swap(&mut self.aggregates, aggregates);
        mem::swap(&mut self.consts, consts);
        mem::swap(&mut self.locals, locals);
    }

    pub fn new(reg: &'registry Registry, aggr_reg: &'registry AggrRegistry) -> Self {
        Helper {
            reg,
            aggr_reg,
            is_in_aggr: false,
            can_emit: true,
            operators: Vec::new(),
            scripts: Vec::new(),
            aggregates: Vec::new(),
            warnings: Vec::new(),
            locals: HashMap::new(),
            consts: HashMap::new(),
            shadowed_vars: Vec::new(),
        }
    }

    fn register_shadow_var(&mut self, id: &str) -> usize {
        let r = self.reserve_shadow();
        self.shadowed_vars.push(id.to_string());
        r
    }

    fn end_shadow_var(&mut self) {
        self.shadowed_vars.pop();
    }

    fn shadow_name(&self, id: usize) -> String {
        format!(" __SHADOW {}__ ", id)
    }

    fn find_shadow_var(&self, id: &str) -> Option<String> {
        let mut r = None;
        for (i, s) in self.shadowed_vars.iter().enumerate() {
            if s == id {
                //FIXME: make sure we never overwrite this,
                r = Some(self.shadow_name(i))
            }
        }
        r
    }

    fn reserve_shadow(&mut self) -> usize {
        self.var_id(&self.shadow_name(self.shadowed_vars.len()))
    }

    fn reserve_2_shadow(&mut self) -> (usize, usize) {
        let l = self.shadowed_vars.len();
        let n1 = self.shadow_name(l);
        let n2 = self.shadow_name(l + 1);
        (self.var_id(&n1), self.var_id(&n2))
    }

    fn var_id(&mut self, id: &str) -> usize {
        let id = if let Some(shadow) = self.find_shadow_var(id) {
            shadow
        } else {
            id.to_string()
        };

        if let Some(idx) = self.locals.get(id.as_str()) {
            *idx
        } else {
            self.locals.insert(id.to_string(), self.locals.len());
            self.locals.len() - 1
        }
    }
    fn is_const(&self, id: &str) -> Option<&usize> {
        self.consts.get(id)
    }
}

impl<'script> Script1<'script> {
    pub fn up_script<'registry>(
        self,
        reg: &'registry Registry,
        aggr_reg: &'registry AggrRegistry,
    ) -> Result<(Script<'script>, Vec<Warning>)> {
        let mut helper = Helper::new(reg, aggr_reg);
        let mut consts: Vec<Value> = vec![Value::Null, Value::Null, Value::Null];
        helper.consts.insert("window".to_owned(), WINDOW_CONST_ID);
        helper.consts.insert("group".to_owned(), GROUP_CONST_ID);
        helper.consts.insert("args".to_owned(), ARGS_CONST_ID);

        let mut exprs = vec![];
        let len = self.exprs.len();
        for (i, e) in self.exprs.into_iter().enumerate() {
            match e {
                Expr1::Const {
                    name,
                    expr,
                    start,
                    end,
                } => {
                    if helper.consts.contains_key(&name.to_string()) {
                        return Err(ErrorKind::DoubleConst(
                            Range::from((start, end)).expand_lines(2),
                            Range::from((start, end)),
                            name.to_string(),
                        )
                        .into());
                    }
                    helper.consts.insert(name.to_string(), consts.len());
                    let expr = expr.up(&mut helper)?;
                    if i == len - 1 {
                        exprs.push(Expr::Imut(ImutExpr::Local {
                            id: name.clone(),
                            is_const: true,
                            idx: consts.len(),
                            start,
                            end,
                        }))
                    }

                    consts.push(reduce2(expr)?);
                }
                #[allow(unreachable_code, unused_variables)]
                #[cfg_attr(tarpaulin, skip)]
                Expr1::FnDecl(_f) => {
                    return Err("Functions are not supported outside of modules.".into());
                }
                other => exprs.push(other.up(&mut helper)?),
            }
        }

        // We make sure the if we return `event` we turn it into `emit event`
        // While this is not required logically it allows us to
        // take advantage of the `emit event` optiisation
        if let Some(e) = exprs.pop() {
            match e.borrow() {
                Expr::Imut(ImutExpr::Path(Path::Event(p))) => {
                    if p.segments.is_empty() {
                        let expr = EmitExpr {
                            start: p.s(),
                            end: p.e(),
                            expr: ImutExpr::Path(Path::Event(p.clone())),
                            port: None,
                        };
                        exprs.push(Expr::Emit(Box::new(expr)))
                    } else {
                        exprs.push(e)
                    }
                }
                _ => exprs.push(e),
            }
        } else {
            return Err(ErrorKind::EmptyScript.into());
        }

        // let aggregates  = Vec::new();
        // mem::swap(&mut aggregates, &mut helper.aggregates);
        Ok((
            Script {
                exprs,
                consts,
                aggregates: helper.aggregates,
                locals: helper.locals.len(),
            },
            helper.warnings,
        ))
    }

    #[cfg(feature = "fns")]
    #[cfg_attr(tarpaulin, skip)]
    pub fn load_module<'registry>(
        self,
        name: &str,
        module: &mut HashMap<String, TremorFnWrapper>,
        reg: &'registry Registry,
        aggr_reg: &'registry AggrRegistry,
    ) -> Result<Vec<Warning>> {
        use crate::registry::CustomFn;
        let mut helper = Helper::new(reg, aggr_reg);

        for e in self.exprs {
            match e {
                /*
                Expr1::Const {
                    name,
                    expr,
                    start,
                    end,
                } => {
                    if helper.consts.contains_key(&name.to_string()) {
                        return Err(ErrorKind::DoubleConst(
                            Range::from((start, end)).expand_lines(2),
                            Range::from((start, end)),
                            name.to_string(),
                        )
                        .into());
                    }
                    helper.consts.insert(name.to_string(), consts.len());
                    let expr = expr.up(&mut helper)?;
                    if i == len - 1 {
                        exprs.push(Expr::Imut(ImutExpr::Local {
                            id: name.clone(),
                            is_const: true,
                            idx: consts.len(),
                            start,
                            end,
                        }))
                    }

                    consts.push(reduce2(expr)?);
                }
                */
                Expr1::FnDecl(f) => {
                    unsafe {
                        let f = f.up(&mut helper)?;
                        module.insert(
                            f.name.id.to_string(),
                            TremorFnWrapper {
                                module: name.to_string(),
                                name: f.name.id.to_string(),
                                fun: Box::new(CustomFn {
                                    args: f.args.iter().map(|i| i.id.to_string()).collect(),
                                    locals: f.locals,
                                    // This should only ever be called from the registry which will make sure
                                    // the source will stay loaded
                                    body: mem::transmute(f.body),
                                }),
                            },
                        );
                    }
                }
                _other => return Err("Only functions are allowed in modules".into()),
            }
        }

        // let aggregates  = Vec::new();
        // mem::swap(&mut aggregates, &mut helper.aggregates);
        Ok(helper.warnings)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Script<'script> {
    pub exprs: Exprs<'script>,
    pub consts: Vec<Value<'script>>,
    pub aggregates: Vec<InvokeAggrFn<'script>>,
    pub locals: usize,
}

use crate::interpreter::*;
use crate::script::Return;
use crate::stry;

impl<'run, 'script, 'event> Script<'script>
where
    'script: 'event,
    'event: 'run,
{
    pub fn run(
        &'script self,
        context: &'run crate::EventContext,
        aggr: AggrType,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
    ) -> Result<Return<'event>> {
        let mut local = LocalStack::with_size(self.locals);

        let mut exprs = self.exprs.iter().peekable();
        let opts = ExecOpts {
            result_needed: true,
            aggr,
        };

        let env = Env {
            context,
            consts: &self.consts,
            aggrs: &self.aggregates,
        };

        while let Some(expr) = exprs.next() {
            if exprs.peek().is_none() {
                match stry!(expr.run(opts.with_result(), &env, event, meta, &mut local)) {
                    Cont::Drop => return Ok(Return::Drop),
                    Cont::Emit(value, port) => return Ok(Return::Emit { value, port }),
                    Cont::EmitEvent(port) => {
                        return Ok(Return::EmitEvent { port });
                    }
                    Cont::Cont(v) => {
                        return Ok(Return::Emit {
                            value: v.into_owned(),
                            port: None,
                        })
                    }
                }
            } else {
                match stry!(expr.run(opts.without_result(), &env, event, meta, &mut local)) {
                    Cont::Drop => return Ok(Return::Drop),
                    Cont::Emit(value, port) => return Ok(Return::Emit { value, port }),
                    Cont::EmitEvent(port) => {
                        return Ok(Return::EmitEvent { port });
                    }
                    Cont::Cont(_v) => (),
                }
            }
        }
        // We know that we never get here, sadly rust doesn't
        #[cfg_attr(tarpaulin, skip)]
        Ok(Return::Emit {
            value: Value::Null,
            port: None,
        })
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct Ident<'script> {
    pub start: Location,
    pub end: Location,
    pub id: Cow<'script, str>,
}
impl_expr!(Ident);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Field1<'script> {
    pub start: Location,
    pub end: Location,
    pub name: StringLit1<'script>,
    pub value: ImutExpr1<'script>,
}

impl<'script> Upable<'script> for Field1<'script> {
    type Target = Field<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Field {
            start: self.start,
            end: self.end,
            name: ImutExpr1::String(self.name).up(helper)?,
            value: self.value.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Field<'script> {
    pub start: Location,
    pub end: Location,
    pub name: ImutExpr<'script>,
    pub value: ImutExpr<'script>,
}
impl_expr!(Field);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Record1<'script> {
    pub start: Location,
    pub end: Location,
    pub fields: Fields1<'script>,
}
impl_expr!(Record1);

impl<'script> Upable<'script> for Record1<'script> {
    type Target = Record<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Record {
            start: self.start,
            end: self.end,
            fields: self.fields.up(helper)?,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Record<'script> {
    pub start: Location,
    pub end: Location,
    pub fields: Fields<'script>,
}
impl_expr!(Record);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct List1<'script> {
    pub start: Location,
    pub end: Location,
    pub exprs: ImutExprs1<'script>,
}
impl_expr!(List1);

impl<'script> Upable<'script> for List1<'script> {
    type Target = List<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(List {
            start: self.start,
            end: self.end,
            exprs: self.exprs.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct List<'script> {
    pub start: Location,
    pub end: Location,
    pub exprs: ImutExprs<'script>,
}
impl_expr!(List);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Literal<'script> {
    pub start: Location,
    pub end: Location,
    pub value: Value<'script>,
}
impl_expr!(Literal);

pub struct StrLitElements<'script>(pub Vec<Cow<'script, str>>, pub ImutExprs1<'script>);

impl<'script> From<StrLitElements<'script>> for StringLit1<'script> {
    fn from(mut es: StrLitElements<'script>) -> StringLit1<'script> {
        let string = if es.0.len() == 1 {
            es.0.pop().unwrap_or_default()
        } else {
            let mut s = String::new();
            for e in es.0 {
                s.push_str(&e);
            }
            s.into()
        };
        StringLit1 {
            start: Location::default(),
            end: Location::default(),
            string,
            exprs: es.1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StringLit1<'script> {
    pub start: Location,
    pub end: Location,
    pub string: Cow<'script, str>,
    pub exprs: ImutExprs1<'script>,
}

pub fn reduce2<'script>(expr: ImutExpr<'script>) -> Result<Value<'script>> {
    match expr {
        ImutExpr::Literal(Literal { value: v, .. }) => Ok(v),
        other => Err(ErrorKind::NotConstant(other.extent(), other.extent().expand_lines(2)).into()),
    }
}

/*
pub fn reduce3<'script>(expr: &ImutExpr<'script>) -> Result<Value<'script>> {
    match expr {
        ImutExpr::Literal(Literal { value: v, .. }) => Ok(*v),
        other => Err(ErrorKind::NotConstant(other.extent(), other.extent().expand_lines(2)).into()),
    }
}
*/

// This is compile time only we don't care
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Expr1<'script> {
    Const {
        name: Cow<'script, str>,
        expr: ImutExpr1<'script>,
        start: Location,
        end: Location,
    },
    MatchExpr(Box<Match1<'script>>),
    Assign(Box<Assign1<'script>>),
    Comprehension(Box<Comprehension1<'script>>),
    Drop {
        start: Location,
        end: Location,
    },
    Emit(Box<EmitExpr1<'script>>),
    FnDecl(FnDecl1<'script>),
    Imut(ImutExpr1<'script>), //Test(TestExpr1)
}

impl<'script> Upable<'script> for Expr1<'script> {
    type Target = Expr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(match self {
            Expr1::Const { start, end, .. } => {
                // There is no code path that leads here,
                // we still rather have an error in case we made
                // an error then unreachable
                #[cfg_attr(tarpaulin, skip)]
                return Err(ErrorKind::InvalidConst(
                    Range::from((start, end)).expand_lines(2),
                    Range::from((start, end)),
                )
                .into());
            }
            Expr1::MatchExpr(m) => Expr::Match(Box::new(m.up(helper)?)),
            Expr1::Assign(a) => {
                let path = a.path.up(helper)?;

                match a.expr.up(helper)? {
                    Expr::Imut(ImutExpr::Merge(m)) => {
                        if path_eq(&path, &m.target) {
                            Expr::MergeInPlace(Box::new(*m))
                        } else {
                            Expr::Assign {
                                start: a.start,
                                end: a.end,
                                path,
                                expr: Box::new(ImutExpr::Merge(m).into()),
                            }
                        }
                    }
                    Expr::Imut(ImutExpr::Patch(m)) => {
                        if path_eq(&path, &m.target) {
                            Expr::PatchInPlace(Box::new(*m))
                        } else {
                            Expr::Assign {
                                start: a.start,
                                end: a.end,
                                path,
                                expr: Box::new(ImutExpr::Patch(m).into()),
                            }
                        }
                    }
                    expr => Expr::Assign {
                        start: a.start,
                        end: a.end,
                        path,
                        expr: Box::new(expr),
                    },
                }
            }
            Expr1::Comprehension(c) => Expr::Comprehension(Box::new(c.up(helper)?)),
            Expr1::Drop { start, end } => {
                if !helper.can_emit {
                    return Err(ErrorKind::InvalidDrop(
                        Range(start, end).expand_lines(2),
                        Range(start, end),
                    )
                    .into());
                }
                Expr::Drop { start, end }
            }
            Expr1::Emit(e) => Expr::Emit(Box::new(e.up(helper)?)),
            Expr1::Imut(i) => i.up(helper)?.into(),
            Expr1::FnDecl(f) => {
                // There is no code path that leads here,
                // we still rather have an error in case we made
                // an error then unreachable
                #[cfg_attr(tarpaulin, skip)]
                return Err(ErrorKind::InvalidFn(f.extent().expand_lines(2), f.extent()).into());
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FnDecl1<'script> {
    pub start: Location,
    pub end: Location,
    pub name: Ident<'script>,
    pub args: Vec<Ident<'script>>,
    pub body: Exprs1<'script>,
}
impl_expr!(FnDecl1);

impl<'script> Upable<'script> for FnDecl1<'script> {
    type Target = FnDecl<'script>;
    #[cfg_attr(tarpaulin, skip)]
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let can_emit = helper.can_emit;
        let mut aggrs = Vec::new();
        let mut locals = HashMap::new();
        let mut consts = HashMap::new();

        for (i, a) in self.args.iter().enumerate() {
            locals.insert(a.id.to_string(), i);
        }

        helper.can_emit = false;
        helper.swap(&mut aggrs, &mut consts, &mut locals);
        let body = self.body.up(helper)?;
        helper.swap(&mut aggrs, &mut consts, &mut locals);
        helper.can_emit = can_emit;

        Ok(FnDecl {
            start: self.start,
            end: self.end,
            name: self.name,
            args: self.args,
            body,
            locals: locals.len(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FnDecl<'script> {
    pub start: Location,
    pub end: Location,
    pub name: Ident<'script>,
    pub args: Vec<Ident<'script>>,
    pub body: Exprs<'script>,
    pub locals: usize,
}

impl_expr!(FnDecl);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ImutExpr1<'script> {
    Record(Box<Record1<'script>>),
    List(Box<List1<'script>>),
    Patch(Box<Patch1<'script>>),
    Merge(Box<Merge1<'script>>),
    Match(Box<ImutMatch1<'script>>),
    Comprehension(Box<ImutComprehension1<'script>>),
    Path(Path1<'script>),
    Binary(Box<BinExpr1<'script>>),
    Unary(Box<UnaryExpr1<'script>>),
    Literal(Literal<'script>),
    Invoke(Invoke1<'script>),
    InvokeAggr(InvokeAggr1<'script>),
    Present {
        path: Path1<'script>,
        start: Location,
        end: Location,
    },
    String(StringLit1<'script>),
}

impl<'script> Upable<'script> for ImutExpr1<'script> {
    type Target = ImutExpr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(match self {
            ImutExpr1::Binary(b) => match b.up(helper)? {
                b1 @ BinExpr {
                    lhs: ImutExpr::Literal(_),
                    rhs: ImutExpr::Literal(_),
                    ..
                } => {
                    let start = b1.start;
                    let end = b1.end;
                    let lhs = reduce2(b1.lhs.clone())?;
                    let rhs = reduce2(b1.rhs.clone())?;
                    // TODO make this work
                    let value = if let Ok(v) = exec_binary2(&b1, &b1.lhs, b1.kind, &lhs, &rhs) {
                        //let value = if let Some(v) = exec_binary(b1.kind, &lhs, &rhs) {
                        v.into_owned()
                    } else {
                        // TODO handle bitshift vs general binary operator errors
                        /*
                        return Err(ErrorKind::InvalidBitshift(
                            Range::from((start, end)).expand_lines(2),
                            Range::from((start, end)),
                        )
                        */
                        return Err(ErrorKind::InvalidBinary(
                            Range::from((start, end)).expand_lines(2),
                            Range::from((start, end)),
                            b1.kind,
                            lhs.value_type(),
                            rhs.value_type(),
                        )
                        .into());
                    };
                    let lit = Literal { start, end, value };
                    ImutExpr::Literal(lit)
                }
                b1 => ImutExpr::Binary(Box::new(b1)),
            },
            ImutExpr1::Unary(u) => match u.up(helper)? {
                u1 @ UnaryExpr {
                    expr: ImutExpr::Literal(_),
                    ..
                } => {
                    let start = u1.start;
                    let end = u1.end;
                    let expr = reduce2(u1.expr)?;
                    let value = if let Some(v) = exec_unary(u1.kind, &expr) {
                        v.into_owned()
                    } else {
                        return Err(ErrorKind::InvalidUnary(
                            Range::from((start, end)).expand_lines(2),
                            Range::from((start, end)),
                            u1.kind,
                            expr.value_type(),
                        )
                        .into());
                    };

                    let lit = Literal { start, end, value };
                    ImutExpr::Literal(lit)
                }
                u1 => ImutExpr::Unary(Box::new(u1)),
            },
            ImutExpr1::String(mut s) => {
                let lit = ImutExpr1::Literal(Literal {
                    start: s.start,
                    end: s.end,
                    value: Value::String(s.string),
                });
                if s.exprs.is_empty() {
                    lit.up(helper)?
                } else {
                    let mut args = vec![lit];
                    args.append(&mut s.exprs);
                    ImutExpr1::Invoke(Invoke1 {
                        start: s.start,
                        end: s.end,
                        module: "string".into(),
                        fun: "format".into(),
                        args,
                    })
                    .up(helper)?
                }
            }
            ImutExpr1::Record(r) => {
                let r = r.up(helper)?;
                if r.fields.iter().all(|f| is_lit(&f.name) && is_lit(&f.value)) {
                    let obj: Result<borrowed::Object> = r
                        .fields
                        .into_iter()
                        .map(|f| {
                            reduce2(f.name.clone()).and_then(|n| {
                                // ALLOW: The grammer guarantees the key of a record is always a string
                                let n = n.as_str().unwrap_or_else(|| unreachable!());
                                reduce2(f.value).map(|v| (n.to_owned().into(), v))
                            })
                        })
                        .collect();
                    ImutExpr::Literal(Literal {
                        start: r.start,
                        end: r.end,
                        value: Value::from(obj?),
                    })
                } else {
                    ImutExpr::Record(r)
                }
            }
            ImutExpr1::List(l) => {
                let l = l.up(helper)?;
                if l.exprs.iter().all(is_lit) {
                    let elements: Result<Vec<Value>> = l.exprs.into_iter().map(reduce2).collect();
                    ImutExpr::Literal(Literal {
                        start: l.start,
                        end: l.end,
                        value: Value::Array(elements?),
                    })
                } else {
                    ImutExpr::List(l)
                }
            }
            ImutExpr1::Patch(p) => ImutExpr::Patch(Box::new(p.up(helper)?)),
            ImutExpr1::Merge(m) => ImutExpr::Merge(Box::new(m.up(helper)?)),
            ImutExpr1::Present { path, start, end } => ImutExpr::Present {
                path: path.up(helper)?,
                start,
                end,
            },
            ImutExpr1::Path(p) => match p.up(helper)? {
                Path::Local(LocalPath {
                    ref id,
                    is_const,
                    start,
                    end,
                    idx,
                    ref segments,
                }) if segments.is_empty() => ImutExpr::Local {
                    id: id.clone(),
                    start,
                    end,
                    idx,
                    is_const,
                },
                p => ImutExpr::Path(p),
            },
            ImutExpr1::Literal(l) => ImutExpr::Literal(l),
            ImutExpr1::Invoke(i) => {
                if i.is_aggregate(helper) {
                    ImutExpr::InvokeAggr(i.into_aggregate().up(helper)?)
                } else {
                    let i = i.up(helper)?;
                    if i.invocable.is_const() && i.args.iter().all(|f| is_lit(&f)) {
                        let args: Result<Vec<Value<'script>>> =
                            i.args.into_iter().map(reduce2).collect();
                        let args = args?;
                        let mut args2: Vec<&Value<'script>> = Vec::new();
                        let start = i.start;
                        let end = i.end;
                        unsafe {
                            for i in 0..args.len() {
                                args2.push(args.get_unchecked(i));
                            }
                        }
                        let v = i
                            .invocable
                            .invoke(&EventContext::default(), &args2)
                            .map_err(|e| {
                                e.into_err(&(start, end), &(start, end), Some(&helper.reg))
                            })?;
                        ImutExpr::Literal(Literal {
                            value: v,
                            start,
                            end,
                        })
                    } else {
                        match i.args.len() {
                            1 => ImutExpr::Invoke1(i),
                            2 => ImutExpr::Invoke2(i),
                            3 => ImutExpr::Invoke3(i),
                            _ => ImutExpr::Invoke(i),
                        }
                    }
                }
            }
            ImutExpr1::InvokeAggr(i) => {
                let i = i.up(helper)?;
                ImutExpr::InvokeAggr(i)
            }
            ImutExpr1::Match(m) => ImutExpr::Match(Box::new(m.up(helper)?)),
            ImutExpr1::Comprehension(c) => ImutExpr::Comprehension(Box::new(c.up(helper)?)),
        })
    }
}

fn path_eq<'script>(path: &Path<'script>, expr: &ImutExpr<'script>) -> bool {
    let path_expr: ImutExpr = ImutExpr::Path(path.clone());

    let target_expr = match expr.clone() {
        ImutExpr::Local {
            id,
            idx,
            start,
            end,
            is_const,
        } => ImutExpr::Path(Path::Local(LocalPath {
            id,
            segments: vec![],
            idx,
            start,
            end,
            is_const,
        })),
        other => other,
    };
    path_expr == target_expr
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Expr<'script> {
    Match(Box<Match<'script>>),
    PatchInPlace(Box<Patch<'script>>),
    MergeInPlace(Box<Merge<'script>>),
    Assign {
        start: Location,
        end: Location,
        path: Path<'script>,
        expr: Box<Expr<'script>>,
    },
    // Moves
    AssignMoveLocal {
        start: Location,
        end: Location,
        path: Path<'script>,
        idx: usize,
    },
    Comprehension(Box<Comprehension<'script>>),
    Drop {
        start: Location,
        end: Location,
    },
    Emit(Box<EmitExpr<'script>>),
    Imut(ImutExpr<'script>),
}

impl<'script> From<ImutExpr<'script>> for Expr<'script> {
    fn from(imut: ImutExpr<'script>) -> Expr<'script> {
        Expr::Imut(imut)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ImutExpr<'script> {
    Record(Record<'script>),
    List(List<'script>),
    Binary(Box<BinExpr<'script>>),
    Unary(Box<UnaryExpr<'script>>),
    Patch(Box<Patch<'script>>),
    Match(Box<ImutMatch<'script>>),
    Comprehension(Box<ImutComprehension<'script>>),
    Merge(Box<Merge<'script>>),
    Path(Path<'script>),
    Local {
        id: Cow<'script, str>,
        idx: usize,
        start: Location,
        end: Location,
        is_const: bool,
    },
    Literal(Literal<'script>),
    Present {
        path: Path<'script>,
        start: Location,
        end: Location,
    },
    Invoke1(Invoke<'script>),
    Invoke2(Invoke<'script>),
    Invoke3(Invoke<'script>),
    Invoke(Invoke<'script>),
    InvokeAggr(InvokeAggr),
}

fn is_lit<'script>(e: &ImutExpr<'script>) -> bool {
    match e {
        ImutExpr::Literal(_) => true,
        _ => false,
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EmitExpr1<'script> {
    pub start: Location,
    pub end: Location,
    pub expr: ImutExpr1<'script>,
    pub port: Option<ImutExpr1<'script>>,
}
impl_expr!(EmitExpr1);

impl<'script> Upable<'script> for EmitExpr1<'script> {
    type Target = EmitExpr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        if !helper.can_emit {
            return Err(
                ErrorKind::InvalidEmit(self.extent().expand_lines(2), self.extent()).into(),
            );
        }
        Ok(EmitExpr {
            start: self.start,
            end: self.end,
            expr: self.expr.up(helper)?,
            port: self.port.up(helper)?,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EmitExpr<'script> {
    pub start: Location,
    pub end: Location,
    pub expr: ImutExpr<'script>,
    pub port: Option<ImutExpr<'script>>,
}
impl_expr!(EmitExpr);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Assign1<'script> {
    pub start: Location,
    pub end: Location,
    pub path: Path1<'script>,
    pub expr: Expr1<'script>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Invoke1<'script> {
    pub start: Location,
    pub end: Location,
    pub module: String,
    pub fun: String,
    pub args: ImutExprs1<'script>,
}
impl_expr!(Invoke1);

impl<'script> Upable<'script> for Invoke1<'script> {
    type Target = Invoke<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let invocable = helper
            .reg
            .find(&self.module, &self.fun)
            .map_err(|e| e.into_err(&self, &self, Some(&helper.reg)))?;

        let args = self.args.up(helper)?;

        Ok(Invoke {
            start: self.start,
            end: self.end,
            module: self.module,
            fun: self.fun,
            invocable: invocable.clone(),
            args,
        })
    }
}

impl<'script> Invoke1<'script> {
    fn is_aggregate<'registry>(&self, helper: &mut Helper<'script, 'registry>) -> bool {
        helper.aggr_reg.find(&self.module, &self.fun).is_ok()
    }

    fn into_aggregate(self) -> InvokeAggr1<'script> {
        InvokeAggr1 {
            start: self.start,
            end: self.end,
            module: self.module,
            fun: self.fun,
            args: self.args,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct Invoke<'script> {
    pub start: Location,
    pub end: Location,
    pub module: String,
    pub fun: String,
    #[serde(skip)]
    pub invocable: TremorFnWrapper,
    pub args: ImutExprs<'script>,
}
impl_expr!(Invoke);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct InvokeAggr1<'script> {
    pub start: Location,
    pub end: Location,
    pub module: String,
    pub fun: String,
    pub args: ImutExprs1<'script>,
}
impl_expr!(InvokeAggr1);

impl<'script> Upable<'script> for InvokeAggr1<'script> {
    type Target = InvokeAggr;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        if helper.is_in_aggr {
            return Err(ErrorKind::AggrInAggr(self.extent(), self.extent().expand_lines(2)).into());
        };
        helper.is_in_aggr = true;
        let invocable = helper
            .aggr_reg
            .find(&self.module, &self.fun)
            .map_err(|e| e.into_err(&self, &self, Some(&helper.reg)))?
            .clone();
        if !invocable.valid_arity(self.args.len()) {
            return Err(ErrorKind::BadArity(
                self.extent(),
                self.extent().expand_lines(2),
                self.module.clone(),
                self.fun.clone(),
                invocable.arity(),
                self.args.len(),
            )
            .into());
        }
        if let Some(warning) = invocable.warning() {
            helper.warnings.push(Warning {
                inner: self.extent(),
                outer: self.extent(),
                msg: warning,
            });
        }
        let aggr_id = helper.aggregates.len();
        let args = self.args.up(helper)?;

        helper.aggregates.push(InvokeAggrFn {
            start: self.start,
            end: self.end,
            invocable,
            args,
            module: self.module.clone(),
            fun: self.fun.clone(),
        });
        helper.is_in_aggr = false;

        Ok(InvokeAggr {
            start: self.start,
            end: self.end,
            module: self.module,
            fun: self.fun,
            aggr_id,
        })
    }
}

#[derive(Clone, Serialize)]
pub struct InvokeAggr {
    pub start: Location,
    pub end: Location,
    pub module: String,
    pub fun: String,
    pub aggr_id: usize,
}

#[derive(Clone, Serialize)]
pub struct InvokeAggrFn<'script> {
    pub start: Location,
    pub end: Location,
    #[serde(skip)]
    pub invocable: TremorAggrFnWrapper,
    pub module: String,
    pub fun: String,
    pub args: ImutExprs<'script>,
}
impl_expr!(InvokeAggrFn);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TestExpr {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub test: String,
    pub extractor: Extractor,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TestExpr1 {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub test: String,
}

impl<'script> Upable<'script> for TestExpr1 {
    type Target = TestExpr;
    fn up<'registry>(self, _helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match Extractor::new(&self.id, &self.test) {
            Ok(ex) => Ok(TestExpr {
                id: self.id,
                test: self.test,
                extractor: ex,
                start: self.start,
                end: self.end,
            }),
            Err(e) => Err(ErrorKind::InvalidExtractor(
                self.extent().expand_lines(2),
                self.extent(),
                self.id,
                self.test,
                e.msg,
            )
            .into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Match1<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExpr1<'script>,
    pub patterns: Predicates1<'script>,
}

impl<'script> Upable<'script> for Match1<'script> {
    type Target = Match<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let patterns = self.patterns.up(helper)?;

        let defaults = patterns.iter().filter(|p| p.pattern.is_default()).count();
        match defaults {
            0 => helper.warnings.push(Warning{
                outer: Range(self.start, self.end),
                inner: Range(self.start, self.end),
                msg: "This match expression has no default clause, if the other clauses do not cover all posiblities this will lead to events being discarded with runtime errors.".into()
            }),
            x if x > 1 => helper.warnings.push(Warning{
                outer: Range(self.start, self.end),
                inner: Range(self.start, self.end),
                msg: "A match statement with more then one default clause will enver reach any but the first default clause.".into()
            }),

            _ => ()
        }

        Ok(Match {
            start: self.start,
            end: self.end,
            target: self.target.up(helper)?,
            patterns,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutMatch1<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExpr1<'script>,
    pub patterns: ImutPredicates1<'script>,
}

impl<'script> Upable<'script> for ImutMatch1<'script> {
    type Target = ImutMatch<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let patterns = self.patterns.up(helper)?;
        let defaults = patterns.iter().filter(|p| p.pattern.is_default()).count();
        match defaults {
            0 => helper.warnings.push(Warning{
                outer: Range(self.start, self.end),
                inner: Range(self.start, self.end),
                msg: "This match expression has no default clause, if the other clauses do not cover all posiblities this will lead to events being discarded with runtime errors.".into()
            }),
            x if x > 1 => helper.warnings.push(Warning{
                outer: Range(self.start, self.end),
                inner: Range(self.start, self.end),
                msg: "A match statement with more then one default clause will enver reach any but the first default clause.".into()
            }),

            _ => ()
        }

        Ok(ImutMatch {
            start: self.start,
            end: self.end,
            target: self.target.up(helper)?,
            patterns,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Match<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExpr<'script>,
    pub patterns: Predicates<'script>,
}
impl_expr!(Match);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutMatch<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExpr<'script>,
    pub patterns: ImutPredicates<'script>,
}
impl_expr!(ImutMatch);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PredicateClause1<'script> {
    pub start: Location,
    pub end: Location,
    pub pattern: Pattern1<'script>,
    pub guard: Option<ImutExpr1<'script>>,
    pub exprs: Exprs1<'script>,
}

impl<'script> Upable<'script> for PredicateClause1<'script> {
    type Target = PredicateClause<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // We run the pattern first as this might reserve a local shadow
        let pattern = self.pattern.up(helper)?;
        let exprs = self.exprs.up(helper)?;
        let guard = self.guard.up(helper)?;
        // If we are in an assign pattern we'd have created
        // a shadow variable, this needs to be undoine at the end
        if pattern.is_assign() {
            helper.end_shadow_var();
        }
        Ok(PredicateClause {
            start: self.start,
            end: self.end,
            pattern,
            guard,
            exprs,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutPredicateClause1<'script> {
    pub start: Location,
    pub end: Location,
    pub pattern: Pattern1<'script>,
    pub guard: Option<ImutExpr1<'script>>,
    pub exprs: ImutExprs1<'script>,
}

impl<'script> Upable<'script> for ImutPredicateClause1<'script> {
    type Target = ImutPredicateClause<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // We run the pattern first as this might reserve a local shadow
        let pattern = self.pattern.up(helper)?;
        let exprs = self.exprs.up(helper)?;
        let guard = self.guard.up(helper)?;
        // If we are in an assign pattern we'd have created
        // a shadow variable, this needs to be undoine at the end
        if pattern.is_assign() {
            helper.end_shadow_var();
        }
        Ok(ImutPredicateClause {
            start: self.start,
            end: self.end,
            pattern,
            guard,
            exprs,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PredicateClause<'script> {
    pub start: Location,
    pub end: Location,
    pub pattern: Pattern<'script>,
    pub guard: Option<ImutExpr<'script>>,
    pub exprs: Exprs<'script>,
}
impl_expr!(PredicateClause);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutPredicateClause<'script> {
    pub start: Location,
    pub end: Location,
    pub pattern: Pattern<'script>,
    pub guard: Option<ImutExpr<'script>>,
    pub exprs: ImutExprs<'script>,
}
impl_expr!(ImutPredicateClause);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Patch1<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExpr1<'script>,
    pub operations: PatchOperations1<'script>,
}

impl<'script> Upable<'script> for Patch1<'script> {
    type Target = Patch<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let operations = self.operations.up(helper)?;

        Ok(Patch {
            start: self.start,
            end: self.end,
            target: self.target.up(helper)?,
            operations,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Patch<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExpr<'script>,
    pub operations: PatchOperations<'script>,
}
impl_expr!(Patch);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PatchOperation1<'script> {
    Insert {
        ident: ImutExpr1<'script>,
        expr: ImutExpr1<'script>,
    },
    Upsert {
        ident: ImutExpr1<'script>,
        expr: ImutExpr1<'script>,
    },
    Update {
        ident: ImutExpr1<'script>,
        expr: ImutExpr1<'script>,
    },
    Erase {
        ident: ImutExpr1<'script>,
    },
    Copy {
        from: ImutExpr1<'script>,
        to: ImutExpr1<'script>,
    },
    Move {
        from: ImutExpr1<'script>,
        to: ImutExpr1<'script>,
    },
    Merge {
        ident: ImutExpr1<'script>,
        expr: ImutExpr1<'script>,
    },
    TupleMerge {
        expr: ImutExpr1<'script>,
    },
}

impl<'script> Upable<'script> for PatchOperation1<'script> {
    type Target = PatchOperation<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use PatchOperation1::*;
        Ok(match self {
            Insert { ident, expr } => PatchOperation::Insert {
                ident: ident.up(helper)?,
                expr: expr.up(helper)?,
            },
            Upsert { ident, expr } => PatchOperation::Upsert {
                ident: ident.up(helper)?,
                expr: expr.up(helper)?,
            },
            Update { ident, expr } => PatchOperation::Update {
                ident: ident.up(helper)?,
                expr: expr.up(helper)?,
            },
            Erase { ident } => PatchOperation::Erase {
                ident: ident.up(helper)?,
            },
            Copy { from, to } => PatchOperation::Copy {
                from: from.up(helper)?,
                to: to.up(helper)?,
            },
            Move { from, to } => PatchOperation::Move {
                from: from.up(helper)?,
                to: to.up(helper)?,
            },
            Merge { ident, expr } => PatchOperation::Merge {
                ident: ident.up(helper)?,
                expr: expr.up(helper)?,
            },
            TupleMerge { expr } => PatchOperation::TupleMerge {
                expr: expr.up(helper)?,
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PatchOperation<'script> {
    Insert {
        ident: ImutExpr<'script>,
        expr: ImutExpr<'script>,
    },
    Upsert {
        ident: ImutExpr<'script>,
        expr: ImutExpr<'script>,
    },
    Update {
        ident: ImutExpr<'script>,
        expr: ImutExpr<'script>,
    },
    Erase {
        ident: ImutExpr<'script>,
    },
    Copy {
        from: ImutExpr<'script>,
        to: ImutExpr<'script>,
    },
    Move {
        from: ImutExpr<'script>,
        to: ImutExpr<'script>,
    },
    Merge {
        ident: ImutExpr<'script>,
        expr: ImutExpr<'script>,
    },
    TupleMerge {
        expr: ImutExpr<'script>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Merge1<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExpr1<'script>,
    pub expr: ImutExpr1<'script>,
}

impl<'script> Upable<'script> for Merge1<'script> {
    type Target = Merge<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Merge {
            start: self.start,
            end: self.end,
            target: self.target.up(helper)?,
            expr: self.expr.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Merge<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExpr<'script>,
    pub expr: ImutExpr<'script>,
}
impl_expr!(Merge);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Comprehension1<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExpr1<'script>,
    pub cases: ComprehensionCases1<'script>,
}

impl<'script> Upable<'script> for Comprehension1<'script> {
    type Target = Comprehension<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // We compute the target before shadowing the key and value

        let target = self.target.up(helper)?;

        // We know that each case wiull have a key and a value as a shadowed
        // variable so we reserve two ahead of time so we know what id's those
        // will be.
        let (key_id, val_id) = helper.reserve_2_shadow();

        let cases = self.cases.up(helper)?;

        Ok(Comprehension {
            start: self.start,
            end: self.end,
            target,
            cases,
            key_id,
            val_id,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutComprehension1<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExpr1<'script>,
    pub cases: ImutComprehensionCases1<'script>,
}

impl<'script> Upable<'script> for ImutComprehension1<'script> {
    type Target = ImutComprehension<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // We compute the target before shadowing the key and value

        let target = self.target.up(helper)?;

        // We know that each case wiull have a key and a value as a shadowed
        // variable so we reserve two ahead of time so we know what id's those
        // will be.
        let (key_id, val_id) = helper.reserve_2_shadow();

        let cases = self.cases.up(helper)?;

        Ok(ImutComprehension {
            start: self.start,
            end: self.end,
            target,
            cases,
            key_id,
            val_id,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Comprehension<'script> {
    pub start: Location,
    pub end: Location,
    pub key_id: usize,
    pub val_id: usize,
    pub target: ImutExpr<'script>,
    pub cases: ComprehensionCases<'script>,
}
impl_expr!(Comprehension);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutComprehension<'script> {
    pub start: Location,
    pub end: Location,
    pub key_id: usize,
    pub val_id: usize,
    pub target: ImutExpr<'script>,
    pub cases: ImutComprehensionCases<'script>,
}
impl_expr!(ImutComprehension);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ComprehensionCase1<'script> {
    pub start: Location,
    pub end: Location,
    pub key_name: Cow<'script, str>,
    pub value_name: Cow<'script, str>,
    pub guard: Option<ImutExpr1<'script>>,
    pub exprs: Exprs1<'script>,
}

impl<'script> Upable<'script> for ComprehensionCase1<'script> {
    type Target = ComprehensionCase<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // regiter key and value as shadowed variables
        let key_idx = helper.register_shadow_var(&self.key_name);
        let val_idx = helper.register_shadow_var(&self.value_name);

        let guard = self.guard.up(helper)?;
        let mut exprs = self.exprs.up(helper)?;

        if let Some(expr) = exprs.pop() {
            exprs.push(replace_last_shadow_use(
                val_idx,
                replace_last_shadow_use(key_idx, expr),
            ));
        };

        // unregister them again
        helper.end_shadow_var();
        helper.end_shadow_var();
        Ok(ComprehensionCase {
            start: self.start,
            end: self.end,
            key_name: self.key_name,
            value_name: self.value_name,
            guard,
            exprs,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutComprehensionCase1<'script> {
    pub start: Location,
    pub end: Location,
    pub key_name: Cow<'script, str>,
    pub value_name: Cow<'script, str>,
    pub guard: Option<ImutExpr1<'script>>,
    pub exprs: ImutExprs1<'script>,
}

impl<'script> Upable<'script> for ImutComprehensionCase1<'script> {
    type Target = ImutComprehensionCase<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // regiter key and value as shadowed variables
        helper.register_shadow_var(&self.key_name);
        helper.register_shadow_var(&self.value_name);

        let guard = self.guard.up(helper)?;
        let exprs = self.exprs.up(helper)?;

        // unregister them again
        helper.end_shadow_var();
        helper.end_shadow_var();
        Ok(ImutComprehensionCase {
            start: self.start,
            end: self.end,
            key_name: self.key_name,
            value_name: self.value_name,
            guard,
            exprs,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ComprehensionCase<'script> {
    pub start: Location,
    pub end: Location,
    pub key_name: Cow<'script, str>,
    pub value_name: Cow<'script, str>,
    pub guard: Option<ImutExpr<'script>>,
    pub exprs: Exprs<'script>,
}
impl_expr!(ComprehensionCase);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutComprehensionCase<'script> {
    pub start: Location,
    pub end: Location,
    pub key_name: Cow<'script, str>,
    pub value_name: Cow<'script, str>,
    pub guard: Option<ImutExpr<'script>>,
    pub exprs: ImutExprs<'script>,
}
impl_expr!(ImutComprehensionCase);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Pattern1<'script> {
    //Predicate(PredicatePattern1<'script>),
    Record(RecordPattern1<'script>),
    Array(ArrayPattern1<'script>),
    Expr(ImutExpr1<'script>),
    Assign(AssignPattern1<'script>),
    Default,
}

impl<'script> Upable<'script> for Pattern1<'script> {
    type Target = Pattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use Pattern1::*;
        Ok(match self {
            //Predicate(pp) => Pattern::Predicate(pp.up(helper)?),
            Record(rp) => Pattern::Record(rp.up(helper)?),
            Array(ap) => Pattern::Array(ap.up(helper)?),
            Expr(expr) => Pattern::Expr(expr.up(helper)?),
            Assign(ap) => Pattern::Assign(ap.up(helper)?),
            Default => Pattern::Default,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Pattern<'script> {
    //Predicate(PredicatePattern<'script>),
    Record(RecordPattern<'script>),
    Array(ArrayPattern<'script>),
    Expr(ImutExpr<'script>),
    Assign(AssignPattern<'script>),
    Default,
}
impl<'script> Pattern<'script> {
    fn is_default(&self) -> bool {
        if let Pattern::Default = self {
            true
        } else {
            false
        }
    }
    fn is_assign(&self) -> bool {
        if let Pattern::Assign(_) = self {
            true
        } else {
            false
        }
    }
}

pub trait BasePattern {}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PredicatePattern1<'script> {
    TildeEq {
        assign: Cow<'script, str>,
        lhs: Cow<'script, str>,
        test: TestExpr1,
    },
    Eq {
        lhs: Cow<'script, str>,
        rhs: ImutExpr1<'script>,
        not: bool,
    },
    RecordPatternEq {
        lhs: Cow<'script, str>,
        pattern: RecordPattern1<'script>,
    },
    ArrayPatternEq {
        lhs: Cow<'script, str>,
        pattern: ArrayPattern1<'script>,
    },
    FieldPresent {
        lhs: Cow<'script, str>,
    },
    FieldAbsent {
        lhs: Cow<'script, str>,
    },
}

impl<'script> Upable<'script> for PredicatePattern1<'script> {
    type Target = PredicatePattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use PredicatePattern1::*;
        Ok(match self {
            TildeEq { assign, lhs, test } => PredicatePattern::TildeEq {
                assign,
                key: KnownKey::from(lhs.clone()),
                lhs,
                test: Box::new(test.up(helper)?),
            },
            Eq { lhs, rhs, not } => PredicatePattern::Eq {
                key: KnownKey::from(lhs.clone()),
                lhs,
                rhs: rhs.up(helper)?,
                not,
            },
            RecordPatternEq { lhs, pattern } => PredicatePattern::RecordPatternEq {
                key: KnownKey::from(lhs.clone()),
                lhs,
                pattern: pattern.up(helper)?,
            },
            ArrayPatternEq { lhs, pattern } => PredicatePattern::ArrayPatternEq {
                key: KnownKey::from(lhs.clone()),
                lhs,
                pattern: pattern.up(helper)?,
            },
            FieldPresent { lhs } => PredicatePattern::FieldPresent {
                key: KnownKey::from(lhs.clone()),
                lhs,
            },
            FieldAbsent { lhs } => PredicatePattern::FieldAbsent {
                key: KnownKey::from(lhs.clone()),
                lhs,
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PredicatePattern<'script> {
    TildeEq {
        assign: Cow<'script, str>,
        lhs: Cow<'script, str>,
        #[serde(skip)]
        key: KnownKey<'script>,
        test: Box<TestExpr>,
    },
    Eq {
        lhs: Cow<'script, str>,
        #[serde(skip)]
        key: KnownKey<'script>,
        rhs: ImutExpr<'script>,
        not: bool,
    },
    RecordPatternEq {
        lhs: Cow<'script, str>,
        #[serde(skip)]
        key: KnownKey<'script>,
        pattern: RecordPattern<'script>,
    },
    ArrayPatternEq {
        lhs: Cow<'script, str>,
        #[serde(skip)]
        key: KnownKey<'script>,
        pattern: ArrayPattern<'script>,
    },
    FieldPresent {
        lhs: Cow<'script, str>,
        #[serde(skip)]
        key: KnownKey<'script>,
    },
    FieldAbsent {
        lhs: Cow<'script, str>,
        #[serde(skip)]
        key: KnownKey<'script>,
    },
}

impl<'script> PredicatePattern<'script> {
    pub fn key(&self) -> &KnownKey<'script> {
        use PredicatePattern::*;
        match self {
            TildeEq { key, .. }
            | Eq { key, .. }
            | RecordPatternEq { key, .. }
            | ArrayPatternEq { key, .. }
            | FieldPresent { key, .. }
            | FieldAbsent { key, .. } => &key,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RecordPattern1<'script> {
    pub start: Location,
    pub end: Location,
    pub fields: PatternFields1<'script>,
}

impl<'script> Upable<'script> for RecordPattern1<'script> {
    type Target = RecordPattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let fields = self.fields.up(helper)?;
        Ok(RecordPattern {
            start: self.start,
            end: self.end,
            fields,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RecordPattern<'script> {
    pub start: Location,
    pub end: Location,
    pub fields: PatternFields<'script>,
}
impl_expr!(RecordPattern);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ArrayPredicatePattern1<'script> {
    Expr(ImutExpr1<'script>),
    Tilde(TestExpr1),
    Record(RecordPattern1<'script>),
    //Array(ArrayPattern),
}
impl<'script> Upable<'script> for ArrayPredicatePattern1<'script> {
    type Target = ArrayPredicatePattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use ArrayPredicatePattern1::*;
        Ok(match self {
            Expr(expr) => ArrayPredicatePattern::Expr(expr.up(helper)?),
            Tilde(te) => ArrayPredicatePattern::Tilde(te.up(helper)?),
            Record(rp) => ArrayPredicatePattern::Record(rp.up(helper)?),
            //Array(ap) => ArrayPredicatePattern::Array(ap),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ArrayPredicatePattern<'script> {
    Expr(ImutExpr<'script>),
    Tilde(TestExpr),
    Record(RecordPattern<'script>),
    Array(ArrayPattern<'script>),
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ArrayPattern1<'script> {
    pub start: Location,
    pub end: Location,
    pub exprs: ArrayPredicatePatterns1<'script>,
}

impl<'script> Upable<'script> for ArrayPattern1<'script> {
    type Target = ArrayPattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let exprs = self.exprs.up(helper)?;
        Ok(ArrayPattern {
            start: self.start,
            end: self.end,
            exprs,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ArrayPattern<'script> {
    pub start: Location,
    pub end: Location,
    pub exprs: ArrayPredicatePatterns<'script>,
}

impl_expr!(ArrayPattern);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AssignPattern1<'script> {
    pub id: Cow<'script, str>,
    pub pattern: Box<Pattern1<'script>>,
}

impl<'script> Upable<'script> for AssignPattern1<'script> {
    type Target = AssignPattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(AssignPattern {
            idx: helper.register_shadow_var(&self.id),
            id: self.id,
            pattern: Box::new(self.pattern.up(helper)?),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AssignPattern<'script> {
    pub id: Cow<'script, str>,
    pub idx: usize,
    pub pattern: Box<Pattern<'script>>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Path1<'script> {
    Local(LocalPath1<'script>),
    Event(EventPath1<'script>),
    Meta(MetadataPath1<'script>),
}

impl<'script> Upable<'script> for Path1<'script> {
    type Target = Path<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use Path1::*;
        Ok(match self {
            Local(p) => {
                let p = p.up(helper)?;
                if p.is_const {
                    Path::Const(p)
                } else {
                    Path::Local(p)
                }
            }
            Event(p) => Path::Event(p.up(helper)?),
            Meta(p) => Path::Meta(p.up(helper)?),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Path<'script> {
    Const(LocalPath<'script>),
    Local(LocalPath<'script>),
    Event(EventPath<'script>),
    Meta(MetadataPath<'script>),
}

impl<'script> Path<'script> {
    pub fn segments(&self) -> &[Segment] {
        match self {
            Path::Const(path) | Path::Local(path) => &path.segments,
            Path::Meta(path) => &path.segments,
            Path::Event(path) => &path.segments,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Segment1<'script> {
    Element {
        expr: ImutExpr1<'script>,
        start: Location,
        end: Location,
    },
    Range {
        start_lower: Location,
        range_start: ImutExpr1<'script>,
        end_lower: Location,
        start_upper: Location,
        range_end: ImutExpr1<'script>,
        end_upper: Location,
    },
}

impl<'script> Upable<'script> for Segment1<'script> {
    type Target = Segment<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use Segment1::*;
        Ok(match self {
            Element { expr, start, end } => {
                let expr = expr.up(helper)?;
                let r = expr.extent();
                match expr {
                    ImutExpr::Literal(l) => match reduce2(ImutExpr::Literal(l))? {
                        Value::String(id) => Segment::Id {
                            key: KnownKey::from(id.clone()),
                            id: id.clone(),
                            start,
                            end,
                        },
                        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                        Value::I64(idx) if idx >= 0 => Segment::Idx {
                            idx: idx as usize,
                            start,
                            end,
                        },
                        other => {
                            return Err(ErrorKind::TypeConflict(
                                r.expand_lines(2),
                                r,
                                other.value_type(),
                                vec![ValueType::I64, ValueType::String],
                            )
                            .into());
                        }
                    },
                    expr => Segment::Element { start, end, expr },
                }
            }
            Range {
                start_lower,
                range_start,
                end_lower,
                start_upper,
                range_end,
                end_upper,
            } => Segment::Range {
                start_lower,
                range_start: Box::new(range_start.up(helper)?),
                end_lower,
                start_upper,
                range_end: Box::new(range_end.up(helper)?),
                end_upper,
            },
        })
    }
}

impl<'script> Segment1<'script> {
    pub fn from_id(id: Ident<'script>) -> Self {
        Segment1::Element {
            start: id.start,
            end: id.end,
            expr: ImutExpr1::Literal(Literal {
                start: id.start,
                end: id.end,
                value: Value::String(id.id),
            }),
        }
    }
    pub fn from_str(id: &'script str, start: Location, end: Location) -> Self {
        Segment1::Element {
            start,
            end,
            expr: ImutExpr1::Literal(Literal {
                start,
                end,
                value: Value::String(id.into()),
            }),
        }
    }
}

impl<'script> From<ImutExpr1<'script>> for Expr1<'script> {
    fn from(imut: ImutExpr1<'script>) -> Expr1<'script> {
        Expr1::Imut(imut)
    }
}

#[derive(Clone, Debug, Serialize)]
pub enum Segment<'script> {
    Id {
        id: Cow<'script, str>,
        #[serde(skip)]
        key: KnownKey<'script>,
        start: Location,
        end: Location,
    },
    Idx {
        idx: usize,
        start: Location,
        end: Location,
    },
    Element {
        expr: ImutExpr<'script>,
        start: Location,
        end: Location,
    },
    Range {
        start_lower: Location,
        range_start: Box<ImutExpr<'script>>,
        end_lower: Location,
        start_upper: Location,
        range_end: Box<ImutExpr<'script>>,
        end_upper: Location,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct LocalPath1<'script> {
    pub start: Location,
    pub end: Location,
    pub segments: Segments1<'script>,
}
impl_expr!(LocalPath1);

impl<'script> Upable<'script> for LocalPath1<'script> {
    type Target = LocalPath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let segments = self.segments.up(helper)?;
        let mut segments = segments.into_iter();
        if let Some(Segment::Id { id, .. }) = segments.next() {
            let segments = segments.collect();
            if let Some(idx) = helper.is_const(&id) {
                Ok(LocalPath {
                    id,
                    is_const: true,
                    idx: *idx,
                    start: self.start,
                    end: self.end,
                    segments,
                })
            } else {
                let idx = helper.var_id(&id);
                Ok(LocalPath {
                    id,
                    is_const: false,
                    idx,
                    start: self.start,
                    end: self.end,
                    segments,
                })
            }
        } else {
            // We should never encounter this
            error_oops(&(self.start, self.end), "Empty local path")
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct LocalPath<'script> {
    pub id: Cow<'script, str>,
    pub idx: usize,
    pub is_const: bool,
    pub start: Location,
    pub end: Location,
    pub segments: Segments<'script>,
}
impl_expr!(LocalPath);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MetadataPath1<'script> {
    pub start: Location,
    pub end: Location,
    pub segments: Segments1<'script>,
}
impl<'script> Upable<'script> for MetadataPath1<'script> {
    type Target = MetadataPath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let segments = self.segments.up(helper)?;
        Ok(MetadataPath {
            start: self.start,
            end: self.end,
            segments,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct MetadataPath<'script> {
    pub start: Location,
    pub end: Location,
    pub segments: Segments<'script>,
}
impl_expr!(MetadataPath);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EventPath1<'script> {
    pub start: Location,
    pub end: Location,
    pub segments: Segments1<'script>,
}
impl<'script> Upable<'script> for EventPath1<'script> {
    type Target = EventPath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let segments = self.segments.up(helper)?;
        Ok(EventPath {
            start: self.start,
            end: self.end,
            segments,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct EventPath<'script> {
    pub start: Location,
    pub end: Location,
    pub segments: Segments<'script>,
}
impl_expr!(EventPath);

#[derive(Copy, Clone, Debug, PartialEq, Serialize)]
pub enum BinOpKind {
    Or,
    Xor,
    And,

    BitOr,
    BitXor,
    BitAnd,

    Eq,
    NotEq,

    Gte,
    Gt,
    Lte,
    Lt,

    RBitShiftSigned,
    RBitShiftUnsigned,
    LBitShift,

    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BinExpr1<'script> {
    pub start: Location,
    pub end: Location,
    pub kind: BinOpKind,
    pub lhs: ImutExpr1<'script>,
    pub rhs: ImutExpr1<'script>,
    // query_stream_not_defined(stmt: &Box<S>, inner: &I, name: String)
}

impl<'script> Upable<'script> for BinExpr1<'script> {
    type Target = BinExpr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(BinExpr {
            start: self.start,
            end: self.end,
            kind: self.kind,
            lhs: self.lhs.up(helper)?,
            rhs: self.rhs.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BinExpr<'script> {
    pub start: Location,
    pub end: Location,
    pub kind: BinOpKind,
    pub lhs: ImutExpr<'script>,
    pub rhs: ImutExpr<'script>,
}
impl_expr!(BinExpr);

#[derive(Copy, Clone, Debug, PartialEq, Serialize)]
pub enum UnaryOpKind {
    Plus,
    Minus,
    Not,
    BitNot,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct UnaryExpr1<'script> {
    pub start: Location,
    pub end: Location,
    pub kind: UnaryOpKind,
    pub expr: ImutExpr1<'script>,
}

impl<'script> Upable<'script> for UnaryExpr1<'script> {
    type Target = UnaryExpr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(UnaryExpr {
            start: self.start,
            end: self.end,
            kind: self.kind,
            expr: self.expr.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct UnaryExpr<'script> {
    pub start: Location,
    pub end: Location,
    pub kind: UnaryOpKind,
    pub expr: ImutExpr<'script>,
}
impl_expr!(UnaryExpr);

pub type Exprs<'script> = Vec<Expr<'script>>;
pub type Exprs1<'script> = Vec<Expr1<'script>>;
pub type ImutExprs<'script> = Vec<ImutExpr<'script>>;
pub type ImutExprs1<'script> = Vec<ImutExpr1<'script>>;
pub type Fields<'script> = Vec<Field<'script>>;
pub type Fields1<'script> = Vec<Field1<'script>>;
pub type Segments<'script> = Vec<Segment<'script>>;
pub type Segments1<'script> = Vec<Segment1<'script>>;
pub type PatternFields<'script> = Vec<PredicatePattern<'script>>;
pub type PatternFields1<'script> = Vec<PredicatePattern1<'script>>;
pub type Predicates<'script> = Vec<PredicateClause<'script>>;
pub type Predicates1<'script> = Vec<PredicateClause1<'script>>;
pub type ImutPredicates<'script> = Vec<ImutPredicateClause<'script>>;
pub type ImutPredicates1<'script> = Vec<ImutPredicateClause1<'script>>;
pub type PatchOperations<'script> = Vec<PatchOperation<'script>>;
pub type PatchOperations1<'script> = Vec<PatchOperation1<'script>>;
pub type ComprehensionCases<'script> = Vec<ComprehensionCase<'script>>;
pub type ComprehensionCases1<'script> = Vec<ComprehensionCase1<'script>>;
pub type ImutComprehensionCases<'script> = Vec<ImutComprehensionCase<'script>>;
pub type ImutComprehensionCases1<'script> = Vec<ImutComprehensionCase1<'script>>;
pub type ArrayPredicatePatterns<'script> = Vec<ArrayPredicatePattern<'script>>;
pub type ArrayPredicatePatterns1<'script> = Vec<ArrayPredicatePattern1<'script>>;
pub type WithExprs1<'script> = Vec<(Ident<'script>, ImutExpr1<'script>)>;

fn replace_last_shadow_use<'script>(replace_idx: usize, expr: Expr<'script>) -> Expr<'script> {
    match expr {
        Expr::Assign {
            path,
            expr,
            start,
            end,
        } => match expr.borrow() {
            Expr::Imut(ImutExpr::Local { idx, .. }) if idx == &replace_idx => {
                Expr::AssignMoveLocal {
                    start,
                    end,
                    idx: *idx,
                    path,
                }
            }

            _ => Expr::Assign {
                path,
                expr,
                start,
                end,
            },
        },
        Expr::Match(m) => {
            let mut m: Match<'script> = *m;
            let mut patterns = vec![];
            // In each pattern we can replace the use in the last assign

            for mut p in m.patterns {
                //let mut p = p.clone();
                if let Some(expr) = p.exprs.pop() {
                    p.exprs.push(replace_last_shadow_use(replace_idx, expr))
                }
                patterns.push(p)
            }
            m.patterns = patterns;
            //p.patterns
            Expr::Match(Box::new(m))
        }
        other => other,
    }
}

pub type Stmts<'script> = Vec<Stmt<'script>>;
pub type Stmts1<'script> = Vec<Stmt1<'script>>;
