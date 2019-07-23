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

mod base_expr;
use crate::errors::*;
//use crate::interpreter::Interpreter;
use crate::interpreter::exec_binary;
use crate::pos::{Location, Range};
use crate::registry::{Context, Registry, TremorFn};
use crate::tilde::Extractor;
pub use base_expr::BaseExpr;
use halfbrown::HashMap;
use simd_json::value::borrowed;
use simd_json::value::ValueTrait;
use simd_json::BorrowedValue as Value;
use std::borrow::Borrow;
use std::borrow::Cow;
use std::fmt;

#[derive(Debug, PartialEq, Serialize)]
pub struct Script1<'script> {
    pub exprs: Exprs1<'script>,
}

#[derive(Serialize, Debug)]
pub struct Warning {
    pub outer: Range,
    pub inner: Range,
    pub msg: String,
}

pub struct Helper<'h, Ctx>
where
    Ctx: Context + Clone + 'static,
{
    reg: &'h Registry<Ctx>,
    warnings: Vec<Warning>,
    local_idx: usize,
    shadowed_vars: Vec<String>,
    pub locals: HashMap<String, usize>,
    pub consts: HashMap<String, usize>,
}

impl<'h, Ctx> Helper<'h, Ctx>
where
    Ctx: Context + Clone + 'static,
{
    pub fn new(reg: &'h Registry<Ctx>) -> Self {
        Helper {
            reg,
            warnings: Vec::new(),
            locals: HashMap::new(),
            consts: HashMap::new(),
            local_idx: 0,
            shadowed_vars: Vec::new(),
        }
    }

    pub fn into_warnings(self) -> Vec<Warning> {
        self.warnings
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
            self.locals.insert(id.to_string(), self.local_idx);
            self.local_idx += 1;
            self.local_idx - 1
        }
    }
    fn is_const(&self, id: &str) -> Option<&usize> {
        self.consts.get(id)
    }
}

impl<'script> Script1<'script> {
    pub fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<Script<'script, Ctx>> {
        let mut consts: Vec<Value> = vec![];
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
                    let expr = expr.up(helper)?;
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
                other => exprs.push(other.up(helper)?),
            }
        }

        // We make sure the if we return `event` we turn it into `emit event`
        // While this is not required logically it allows us to
        // take advantage of the `emit event` optiisation
        if let Some(e) = exprs.pop() {
            match e.borrow() {
                Expr::Emit(_) => exprs.push(e),
                Expr::Imut(ImutExpr::Path(Path::Event(EventPath { segments, .. })))
                    if segments.is_empty() =>
                {
                    if let Expr::Imut(i) = e {
                        let expr = EmitExpr {
                            start: i.s(),
                            end: i.e(),
                            expr: i,
                            port: None,
                        };
                        exprs.push(Expr::Emit(expr))
                    } else {
                        unreachable!()
                    }
                }
                _ => exprs.push(e),
            }
        } else {
            return Err(ErrorKind::EmptyScript.into());
        }

        Ok(Script { exprs, consts })
    }
}

#[derive(Debug, PartialEq, Serialize)]
pub struct Script<'script, Ctx: Context + Clone + 'static> {
    pub exprs: Exprs<'script, Ctx>,
    pub consts: Vec<Value<'script>>,
}

#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct Ident<'script> {
    pub id: Cow<'script, str>,
}

macro_rules! impl_expr {
    ($name:ident) => {
        impl<'script, Ctx: Context + Clone + 'static> BaseExpr for $name<'script, Ctx> {
            fn s(&self) -> Location {
                self.start
            }

            fn e(&self) -> Location {
                self.end
            }
        }
    };
}

macro_rules! impl_expr1 {
    ($name:ident) => {
        impl<'script> BaseExpr for $name<'script> {
            fn s(&self) -> Location {
                self.start
            }

            fn e(&self) -> Location {
                self.end
            }
        }
    };
}

/*
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TodoExpr {
    pub start: Location,
    pub end: Location,
}
impl_expr1!(TodoExpr);
 */

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Field1<'script> {
    pub start: Location,
    pub end: Location,
    pub name: Cow<'script, str>,
    pub value: ImutExpr1<'script>,
}

impl<'script> Field1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<Field<'script, Ctx>> {
        Ok(Field {
            start: self.start,
            end: self.end,
            name: self.name,
            value: self.value.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Field<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub name: Cow<'script, str>,
    pub value: ImutExpr<'script, Ctx>,
}
impl_expr!(Field);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Record1<'script> {
    pub start: Location,
    pub end: Location,
    pub fields: Fields1<'script>,
}
impl_expr1!(Record1);

impl<'script> Record1<'script> {
    pub fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<Record<'script, Ctx>> {
        let fields: Result<Fields<'script, Ctx>> =
            self.fields.into_iter().map(|p| p.up(helper)).collect();
        Ok(Record {
            start: self.start,
            end: self.end,
            fields: fields?,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Record<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub fields: Fields<'script, Ctx>,
}
impl_expr!(Record);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct List1<'script> {
    pub start: Location,
    pub end: Location,
    pub exprs: ImutExprs1<'script>,
}
impl_expr1!(List1);

impl<'script> List1<'script> {
    pub fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<List<'script, Ctx>> {
        let exprs: Result<ImutExprs<'script, Ctx>> =
            self.exprs.into_iter().map(|p| p.up(helper)).collect();
        Ok(List {
            start: self.start,
            end: self.end,
            exprs: exprs?,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct List<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub exprs: ImutExprs<'script, Ctx>,
}
impl_expr!(List);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Literal<'script> {
    pub start: Location,
    pub end: Location,
    pub value: Value<'script>,
}
impl_expr1!(Literal);

fn reduce2<'script, Ctx: Context + Clone + 'static>(
    expr: ImutExpr<'script, Ctx>,
) -> Result<Value<'script>> {
    match expr {
        ImutExpr::Literal(Literal { value: v, .. }) => Ok(v),
        _ => unreachable!(),
    }
}

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
    Emit(EmitExpr1<'script>),
    Imut(ImutExpr1<'script>), //Test(TestExpr1)
}

impl<'script> Expr1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<Expr<'script, Ctx>> {
        Ok(match self {
            Expr1::Const { start, end, .. } => {
                return Err(ErrorKind::InvalidConst(
                    Range::from((start, end)).expand_lines(2),
                    Range::from((start, end)),
                )
                .into())
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
            Expr1::Drop { start, end } => Expr::Drop { start, end },
            Expr1::Emit(e) => Expr::Emit(e.up(helper)?),
            Expr1::Imut(i) => i.up(helper)?.into(),
        })
    }
}

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
    Present {
        path: Path1<'script>,
        start: Location,
        end: Location,
    },
}

impl<'script> ImutExpr1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<ImutExpr<'script, Ctx>> {
        Ok(match self {
            ImutExpr1::Binary(b) => match b.up(helper)? {
                b1 @ BinExpr {
                    lhs: ImutExpr::Literal(_),
                    rhs: ImutExpr::Literal(_),
                    ..
                } => {
                    let start = b1.start;
                    let end = b1.end;
                    let lhs = reduce2(b1.lhs)?;
                    let rhs = reduce2(b1.rhs)?;
                    let value = if let Some(v) = exec_binary(b1.kind, &lhs, &rhs) {
                        v.into_owned()
                    } else {
                        return Err(ErrorKind::InvalidBinary(
                            Range::from((start, end)).expand_lines(2),
                            Range::from((start, end)),
                            b1.kind,
                            lhs.kind(),
                            rhs.kind(),
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
                    let expr = ImutExpr::Unary(Box::new(u1));
                    let value = reduce2(expr)?;
                    let lit = Literal { start, end, value };
                    ImutExpr::Literal(lit)
                }
                u1 => ImutExpr::Unary(Box::new(u1)),
            },
            ImutExpr1::Record(r) => {
                let r = r.up(helper)?;
                if r.fields.iter().all(|e| is_lit(&e.value)) {
                    let obj: Result<borrowed::Map> = r
                        .fields
                        .into_iter()
                        .map(|f| {
                            let n = f.name.clone();
                            reduce2(f.value).map(|v| (n, v))
                        })
                        .collect();
                    ImutExpr::Literal(Literal {
                        start: r.start,
                        end: r.end,
                        value: Value::Object(obj?),
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
                let i = i.up(helper)?;
                match i.args.len() {
                    1 => ImutExpr::Invoke1(i),
                    2 => ImutExpr::Invoke2(i),
                    3 => ImutExpr::Invoke3(i),
                    _ => ImutExpr::Invoke(i),
                }
            }
            ImutExpr1::Match(m) => ImutExpr::Match(Box::new(m.up(helper)?)),
            ImutExpr1::Comprehension(c) => ImutExpr::Comprehension(Box::new(c.up(helper)?)),
        })
    }
}

fn path_eq<'script, Ctx: Context + Clone + 'static>(
    path: &Path<'script, Ctx>,
    expr: &ImutExpr<'script, Ctx>,
) -> bool {
    let path_expr: ImutExpr<Ctx> = ImutExpr::Path(path.clone());

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
pub enum Expr<'script, Ctx: Context + Clone + 'static> {
    Match(Box<Match<'script, Ctx>>),
    PatchInPlace(Box<Patch<'script, Ctx>>),
    MergeInPlace(Box<Merge<'script, Ctx>>),
    Assign {
        start: Location,
        end: Location,
        path: Path<'script, Ctx>,
        expr: Box<Expr<'script, Ctx>>,
    },
    // Moves
    AssignMoveLocal {
        start: Location,
        end: Location,
        path: Path<'script, Ctx>,
        idx: usize,
    },
    Comprehension(Box<Comprehension<'script, Ctx>>),
    Drop {
        start: Location,
        end: Location,
    },
    Emit(EmitExpr<'script, Ctx>),
    Imut(ImutExpr<'script, Ctx>),
}

impl<'script, Ctx: Context + Clone + 'static> From<ImutExpr<'script, Ctx>> for Expr<'script, Ctx> {
    fn from(imut: ImutExpr<'script, Ctx>) -> Expr<'script, Ctx> {
        Expr::Imut(imut)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ImutExpr<'script, Ctx: Context + Clone + 'static> {
    Record(Record<'script, Ctx>),
    List(List<'script, Ctx>),
    Binary(Box<BinExpr<'script, Ctx>>),
    Unary(Box<UnaryExpr<'script, Ctx>>),
    Patch(Box<Patch<'script, Ctx>>),
    Match(Box<ImutMatch<'script, Ctx>>),
    Comprehension(Box<ImutComprehension<'script, Ctx>>),
    Merge(Box<Merge<'script, Ctx>>),
    Path(Path<'script, Ctx>),
    Local {
        id: Cow<'script, str>,
        idx: usize,
        start: Location,
        end: Location,
        is_const: bool,
    },
    Literal(Literal<'script>),
    Present {
        path: Path<'script, Ctx>,
        start: Location,
        end: Location,
    },
    Invoke1(Invoke<'script, Ctx>),
    Invoke2(Invoke<'script, Ctx>),
    Invoke3(Invoke<'script, Ctx>),
    Invoke(Invoke<'script, Ctx>),
}

fn is_lit<'script, Ctx: Context + Clone + 'static>(e: &ImutExpr<'script, Ctx>) -> bool {
    match e {
        ImutExpr::Literal(_) => true,
        _ => false,
    }
}

impl<'script, Ctx: Context + Clone + 'static> BaseExpr for ImutExpr<'script, Ctx> {
    fn s(&self) -> Location {
        match self {
            ImutExpr::Binary(e) => e.s(),
            ImutExpr::Comprehension(e) => e.s(),
            ImutExpr::Invoke(e) => e.s(),
            ImutExpr::Invoke1(e) => e.s(),
            ImutExpr::Invoke2(e) => e.s(),
            ImutExpr::Invoke3(e) => e.s(),
            ImutExpr::List(e) => e.s(),
            ImutExpr::Literal(e) => e.s(),
            ImutExpr::Local { start, .. } => *start,
            ImutExpr::Match(e) => e.s(),
            ImutExpr::Merge(e) => e.s(),
            ImutExpr::Patch(e) => e.s(),
            ImutExpr::Path(e) => e.s(),
            ImutExpr::Present { start, .. } => *start,
            ImutExpr::Record(e) => e.s(),
            ImutExpr::Unary(e) => e.s(),
        }
    }
    fn e(&self) -> Location {
        match self {
            ImutExpr::Binary(e) => e.e(),
            ImutExpr::Comprehension(e) => e.e(),
            ImutExpr::Invoke(e) => e.e(),
            ImutExpr::Invoke1(e) => e.e(),
            ImutExpr::Invoke2(e) => e.e(),
            ImutExpr::Invoke3(e) => e.e(),
            ImutExpr::List(e) => e.e(),
            ImutExpr::Literal(e) => e.e(),
            ImutExpr::Local { end, .. } => *end,
            ImutExpr::Match(e) => e.e(),
            ImutExpr::Merge(e) => e.e(),
            ImutExpr::Patch(e) => e.e(),
            ImutExpr::Path(e) => e.e(),
            ImutExpr::Present { end, .. } => *end,
            ImutExpr::Record(e) => e.e(),
            ImutExpr::Unary(e) => e.e(),
        }
    }
}
impl<'script, Ctx: Context + Clone + 'static> BaseExpr for Expr<'script, Ctx> {
    fn s(&self) -> Location {
        match self {
            Expr::Assign { start, .. } => *start,
            Expr::AssignMoveLocal { start, .. } => *start,
            Expr::Comprehension(e) => e.s(),
            Expr::Drop { start, .. } => *start,
            Expr::Emit(e) => e.s(),
            Expr::Imut(e) => e.s(),
            Expr::Match(e) => e.s(),
            Expr::MergeInPlace(e) => e.s(),
            Expr::PatchInPlace(e) => e.s(),
        }
    }
    fn e(&self) -> Location {
        match self {
            Expr::Assign { end, .. } => *end,
            Expr::AssignMoveLocal { end, .. } => *end,
            Expr::Comprehension(e) => e.e(),
            Expr::Drop { end, .. } => *end,
            Expr::Emit(e) => e.e(),
            Expr::Imut(e) => e.e(),
            Expr::Match(e) => e.e(),
            Expr::MergeInPlace(e) => e.e(),
            Expr::PatchInPlace(e) => e.e(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EmitExpr1<'script> {
    pub start: Location,
    pub end: Location,
    pub expr: ImutExpr1<'script>,
    pub port: Option<String>,
}

impl<'script> EmitExpr1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<EmitExpr<'script, Ctx>> {
        Ok(EmitExpr {
            start: self.start,
            end: self.end,
            expr: self.expr.up(helper)?,
            port: self.port,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EmitExpr<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub expr: ImutExpr<'script, Ctx>,
    pub port: Option<String>,
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
impl_expr1!(Invoke1);

impl<'script> Invoke1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<Invoke<'script, Ctx>> {
        let args: Result<ImutExprs<'script, Ctx>> = self
            .args
            .clone()
            .into_iter()
            .map(|p| p.up(helper))
            .collect();
        let args = args?;
        let invocable = helper
            .reg
            .find(&self.module, &self.fun)
            .map_err(|e| e.into_err(&self, &self, Some(&helper.reg)))?;

        Ok(Invoke {
            start: self.start,
            end: self.end,
            module: self.module,
            fun: self.fun,
            invocable,
            args,
        })
    }
}

#[derive(Clone, Serialize)]
pub struct Invoke<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub module: String,
    pub fun: String,
    #[serde(skip)]
    pub invocable: TremorFn<Ctx>,
    pub args: ImutExprs<'script, Ctx>,
}
impl_expr!(Invoke);

impl<'script, Ctx: Context + Clone + 'static> PartialEq for Invoke<'script, Ctx> {
    fn eq(&self, other: &Self) -> bool {
        self.start == other.start
            && self.end == other.end
            && self.module == other.module
            && self.fun == other.fun
        //&& self.args == other.args FIXME why??!?
    }
}

impl<'script, Ctx: Context + Clone + 'static> fmt::Debug for Invoke<'script, Ctx> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "fn {}::{}", self.module, self.fun)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TestExpr {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub test: String,
    pub extractor: Extractor,
}
impl BaseExpr for TestExpr {
    fn s(&self) -> Location {
        self.start
    }

    fn e(&self) -> Location {
        self.end
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TestExpr1 {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub test: String,
}

impl BaseExpr for TestExpr1 {
    fn s(&self) -> Location {
        self.start
    }

    fn e(&self) -> Location {
        self.end
    }
}
impl TestExpr1 {
    fn up<Ctx: Context + Clone + 'static>(self, _helper: &mut Helper<Ctx>) -> Result<TestExpr> {
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

impl<'script> Match1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<Match<'script, Ctx>> {
        let patterns: Result<Predicates<'script, Ctx>> =
            self.patterns.into_iter().map(|p| p.up(helper)).collect();
        let patterns = patterns?;

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

impl<'script> ImutMatch1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<ImutMatch<'script, Ctx>> {
        let patterns: Result<ImutPredicates<'script, Ctx>> =
            self.patterns.into_iter().map(|p| p.up(helper)).collect();
        let patterns = patterns?;

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
pub struct Match<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExpr<'script, Ctx>,
    pub patterns: Predicates<'script, Ctx>,
}
impl_expr!(Match);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutMatch<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExpr<'script, Ctx>,
    pub patterns: ImutPredicates<'script, Ctx>,
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

impl<'script> PredicateClause1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<PredicateClause<'script, Ctx>> {
        // We run the pattern first as this might reserve a local shadow
        let pattern = self.pattern.up(helper)?;
        let exprs: Result<Exprs<'script, Ctx>> =
            self.exprs.into_iter().map(|p| p.up(helper)).collect();
        let guard = if let Some(guard) = self.guard {
            Some(guard.up(helper)?)
        } else {
            None
        };
        let exprs = exprs?;
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

impl<'script> ImutPredicateClause1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<ImutPredicateClause<'script, Ctx>> {
        // We run the pattern first as this might reserve a local shadow
        let pattern = self.pattern.up(helper)?;
        let exprs: Result<ImutExprs<'script, Ctx>> =
            self.exprs.into_iter().map(|p| p.up(helper)).collect();
        let guard = if let Some(guard) = self.guard {
            Some(guard.up(helper)?)
        } else {
            None
        };
        let exprs = exprs?;
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
pub struct PredicateClause<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub pattern: Pattern<'script, Ctx>,
    pub guard: Option<ImutExpr<'script, Ctx>>,
    pub exprs: Exprs<'script, Ctx>,
}
impl_expr!(PredicateClause);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutPredicateClause<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub pattern: Pattern<'script, Ctx>,
    pub guard: Option<ImutExpr<'script, Ctx>>,
    pub exprs: ImutExprs<'script, Ctx>,
}
impl_expr!(ImutPredicateClause);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Patch1<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExpr1<'script>,
    pub operations: PatchOperations1<'script>,
}

impl<'script> Patch1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<Patch<'script, Ctx>> {
        let operations: Result<PatchOperations<'script, Ctx>> =
            self.operations.into_iter().map(|p| p.up(helper)).collect();

        Ok(Patch {
            start: self.start,
            end: self.end,
            target: self.target.up(helper)?,
            operations: operations?,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Patch<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExpr<'script, Ctx>,
    pub operations: PatchOperations<'script, Ctx>,
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
    /*
    Move {
        from: Expr1<'script>,
        to: Expr1<'script>,
    },
    */
    Merge {
        ident: ImutExpr1<'script>,
        expr: ImutExpr1<'script>,
    },
    TupleMerge {
        expr: ImutExpr1<'script>,
    },
}

impl<'script> PatchOperation1<'script> {
    pub fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<PatchOperation<'script, Ctx>> {
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
            /*
            Move { from, to } => PatchOperation::Move {
                from: from.up(helper)?,
                to: to.up(helper)?,
            },
            */
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
pub enum PatchOperation<'script, Ctx: Context + Clone + 'static> {
    Insert {
        ident: ImutExpr<'script, Ctx>,
        expr: ImutExpr<'script, Ctx>,
    },
    Upsert {
        ident: ImutExpr<'script, Ctx>,
        expr: ImutExpr<'script, Ctx>,
    },
    Update {
        ident: ImutExpr<'script, Ctx>,
        expr: ImutExpr<'script, Ctx>,
    },
    Erase {
        ident: ImutExpr<'script, Ctx>,
    },
    /*
    Move {
        from: Expr<'script, Ctx>,
        to: Expr<'script, Ctx>,
    },
    */
    Merge {
        ident: ImutExpr<'script, Ctx>,
        expr: ImutExpr<'script, Ctx>,
    },
    TupleMerge {
        expr: ImutExpr<'script, Ctx>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Merge1<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExpr1<'script>,
    pub expr: ImutExpr1<'script>,
}
impl<'script> Merge1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<Merge<'script, Ctx>> {
        //let patterns: Result<Predicates> = self.patterns.into_iter().map(|p| p.up(helper)).collect();

        Ok(Merge {
            start: self.start,
            end: self.end,
            target: self.target.up(helper)?,
            expr: self.expr.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Merge<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExpr<'script, Ctx>,
    pub expr: ImutExpr<'script, Ctx>,
}
impl_expr!(Merge);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Comprehension1<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExpr1<'script>,
    pub cases: ComprehensionCases1<'script>,
}

impl<'script> Comprehension1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<Comprehension<'script, Ctx>> {
        // We compute the target before shadowing the key and value

        let target = self.target.up(helper)?;

        // We know that each case wiull have a key and a value as a shadowed
        // variable so we reserve two ahead of time so we know what id's those
        // will be.
        let (key_id, val_id) = helper.reserve_2_shadow();

        let cases: Result<ComprehensionCases<'script, Ctx>> =
            self.cases.into_iter().map(|p| p.up(helper)).collect();

        Ok(Comprehension {
            start: self.start,
            end: self.end,
            target,
            cases: cases?,
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

impl<'script> ImutComprehension1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<ImutComprehension<'script, Ctx>> {
        // We compute the target before shadowing the key and value

        let target = self.target.up(helper)?;

        // We know that each case wiull have a key and a value as a shadowed
        // variable so we reserve two ahead of time so we know what id's those
        // will be.
        let (key_id, val_id) = helper.reserve_2_shadow();

        let cases: Result<ImutComprehensionCases<'script, Ctx>> =
            self.cases.into_iter().map(|p| p.up(helper)).collect();

        Ok(ImutComprehension {
            start: self.start,
            end: self.end,
            target,
            cases: cases?,
            key_id,
            val_id,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Comprehension<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub key_id: usize,
    pub val_id: usize,
    pub target: ImutExpr<'script, Ctx>,
    pub cases: ComprehensionCases<'script, Ctx>,
}
impl_expr!(Comprehension);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutComprehension<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub key_id: usize,
    pub val_id: usize,
    pub target: ImutExpr<'script, Ctx>,
    pub cases: ImutComprehensionCases<'script, Ctx>,
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

impl<'script> ComprehensionCase1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<ComprehensionCase<'script, Ctx>> {
        // regiter key and value as shadowed variables
        let key_idx = helper.register_shadow_var(&self.key_name);
        let val_idx = helper.register_shadow_var(&self.value_name);

        let guard = if let Some(guard) = self.guard {
            Some(guard.up(helper)?)
        } else {
            None
        };
        let exprs: Result<Exprs<'script, Ctx>> =
            self.exprs.into_iter().map(|p| p.up(helper)).collect();
        let mut exprs = exprs?;

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

impl<'script> ImutComprehensionCase1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<ImutComprehensionCase<'script, Ctx>> {
        // regiter key and value as shadowed variables
        let _key_idx = helper.register_shadow_var(&self.key_name);
        let _val_idx = helper.register_shadow_var(&self.value_name);

        let guard = if let Some(guard) = self.guard {
            Some(guard.up(helper)?)
        } else {
            None
        };
        let exprs: Result<ImutExprs<'script, Ctx>> =
            self.exprs.into_iter().map(|p| p.up(helper)).collect();
        let exprs = exprs?;

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
pub struct ComprehensionCase<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub key_name: Cow<'script, str>,
    pub value_name: Cow<'script, str>,
    pub guard: Option<ImutExpr<'script, Ctx>>,
    pub exprs: Exprs<'script, Ctx>,
}
impl_expr!(ComprehensionCase);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutComprehensionCase<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub key_name: Cow<'script, str>,
    pub value_name: Cow<'script, str>,
    pub guard: Option<ImutExpr<'script, Ctx>>,
    pub exprs: ImutExprs<'script, Ctx>,
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
impl<'script> Pattern1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<Pattern<'script, Ctx>> {
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
pub enum Pattern<'script, Ctx: Context + Clone + 'static> {
    //Predicate(PredicatePattern<'script, Ctx>),
    Record(RecordPattern<'script, Ctx>),
    Array(ArrayPattern<'script, Ctx>),
    Expr(ImutExpr<'script, Ctx>),
    Assign(AssignPattern<'script, Ctx>),
    Default,
}
impl<'script, Ctx: Context + Clone + 'static> Pattern<'script, Ctx> {
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

impl<'script> PredicatePattern1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<PredicatePattern<'script, Ctx>> {
        use PredicatePattern1::*;
        Ok(match self {
            TildeEq { assign, lhs, test } => PredicatePattern::TildeEq {
                assign,
                lhs,
                test: Box::new(test.up(helper)?),
            },
            Eq { lhs, rhs, not } => PredicatePattern::Eq {
                lhs,
                rhs: rhs.up(helper)?,
                not,
            },
            RecordPatternEq { lhs, pattern } => PredicatePattern::RecordPatternEq {
                lhs,
                pattern: pattern.up(helper)?,
            },
            ArrayPatternEq { lhs, pattern } => PredicatePattern::ArrayPatternEq {
                lhs,
                pattern: pattern.up(helper)?,
            },
            FieldPresent { lhs } => PredicatePattern::FieldPresent { lhs },
            FieldAbsent { lhs } => PredicatePattern::FieldAbsent { lhs },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PredicatePattern<'script, Ctx: Context + Clone + 'static> {
    TildeEq {
        assign: Cow<'script, str>,
        lhs: Cow<'script, str>,
        test: Box<TestExpr>,
    },
    Eq {
        lhs: Cow<'script, str>,
        rhs: ImutExpr<'script, Ctx>,
        not: bool,
    },
    RecordPatternEq {
        lhs: Cow<'script, str>,
        pattern: RecordPattern<'script, Ctx>,
    },
    ArrayPatternEq {
        lhs: Cow<'script, str>,
        pattern: ArrayPattern<'script, Ctx>,
    },
    FieldPresent {
        lhs: Cow<'script, str>,
    },
    FieldAbsent {
        lhs: Cow<'script, str>,
    },
}

impl<'script, Ctx: Context + Clone + 'static> PredicatePattern<'script, Ctx> {
    pub fn lhs(&self) -> &str {
        use PredicatePattern::*;
        match self {
            TildeEq { lhs, .. } => &lhs,
            Eq { lhs, .. } => &lhs,
            RecordPatternEq { lhs, .. } => &lhs,
            ArrayPatternEq { lhs, .. } => &lhs,
            FieldPresent { lhs } => &lhs,
            FieldAbsent { lhs } => &lhs,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RecordPattern1<'script> {
    pub start: Location,
    pub end: Location,
    pub fields: PatternFields1<'script>,
}

impl<'script> RecordPattern1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<RecordPattern<'script, Ctx>> {
        let fields: Result<PatternFields<'script, Ctx>> =
            self.fields.into_iter().map(|p| p.up(helper)).collect();
        Ok(RecordPattern {
            start: self.start,
            end: self.end,
            fields: fields?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RecordPattern<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub fields: PatternFields<'script, Ctx>,
}
impl_expr!(RecordPattern);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ArrayPredicatePattern1<'script> {
    Expr(ImutExpr1<'script>),
    Tilde(TestExpr1),
    Record(RecordPattern1<'script>),
    //Array(ArrayPattern),
}
impl<'script> ArrayPredicatePattern1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<ArrayPredicatePattern<'script, Ctx>> {
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
pub enum ArrayPredicatePattern<'script, Ctx: Context + Clone + 'static> {
    Expr(ImutExpr<'script, Ctx>),
    Tilde(TestExpr),
    Record(RecordPattern<'script, Ctx>),
    Array(ArrayPattern<'script, Ctx>),
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ArrayPattern1<'script> {
    pub start: Location,
    pub end: Location,
    pub exprs: ArrayPredicatePatterns1<'script>,
}

impl<'script> ArrayPattern1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<ArrayPattern<'script, Ctx>> {
        let exprs: Result<Vec<ArrayPredicatePattern<'script, Ctx>>> =
            self.exprs.into_iter().map(|p| p.up(helper)).collect();
        Ok(ArrayPattern {
            start: self.start,
            end: self.end,
            exprs: exprs?,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ArrayPattern<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub exprs: ArrayPredicatePatterns<'script, Ctx>,
}

impl_expr!(ArrayPattern);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AssignPattern1<'script> {
    pub id: Cow<'script, str>,
    pub pattern: Box<Pattern1<'script>>,
}

impl<'script> AssignPattern1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<AssignPattern<'script, Ctx>> {
        Ok(AssignPattern {
            idx: helper.register_shadow_var(&self.id),
            id: self.id,
            pattern: Box::new(self.pattern.up(helper)?),
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AssignPattern<'script, Ctx: Context + Clone + 'static> {
    pub id: Cow<'script, str>,
    pub idx: usize,
    pub pattern: Box<Pattern<'script, Ctx>>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Path1<'script> {
    Local(LocalPath1<'script>),
    Event(EventPath1<'script>),
    Meta(MetadataPath1<'script>),
}

impl<'script> Path1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<Path<'script, Ctx>> {
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
pub enum Path<'script, Ctx: Context + Clone + 'static> {
    Const(LocalPath<'script, Ctx>),
    Local(LocalPath<'script, Ctx>),
    Event(EventPath<'script, Ctx>),
    Meta(MetadataPath<'script, Ctx>),
}

impl<'script, Ctx: Context + Clone + 'static> Path<'script, Ctx> {
    pub fn segments(&self) -> &[Segment<Ctx>] {
        match self {
            Path::Const(path) => &path.segments,
            Path::Local(path) => &path.segments,
            Path::Meta(path) => &path.segments,
            Path::Event(path) => &path.segments,
        }
    }
}
impl<'script, Ctx: Context + Clone + 'static> BaseExpr for Path<'script, Ctx> {
    fn s(&self) -> Location {
        match self {
            Path::Const(e) => e.s(),
            Path::Local(e) => e.s(),
            Path::Meta(e) => e.s(),
            Path::Event(e) => e.s(),
        }
    }
    fn e(&self) -> Location {
        match self {
            Path::Const(e) => e.e(),
            Path::Local(e) => e.e(),
            Path::Meta(e) => e.e(),
            Path::Event(e) => e.e(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Segment1<'script> {
    ElementSelector {
        expr: ImutExpr1<'script>,
        start: Location,
        end: Location,
    },
    RangeSelector {
        start_lower: Location,
        range_start: ImutExpr1<'script>,
        end_lower: Location,
        start_upper: Location,
        range_end: ImutExpr1<'script>,
        end_upper: Location,
    },
}

impl<'script> Segment1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<Segment<'script, Ctx>> {
        use Segment1::*;
        Ok(match self {
            ElementSelector { expr, start, end } => {
                let expr = expr.up(helper)?;
                let r: Range = expr.extent();
                match expr {
                    ImutExpr::Literal(l) => match reduce2::<Ctx>(ImutExpr::Literal(l))? {
                        Value::String(id) => Segment::IdSelector {
                            id: id.clone(),
                            start,
                            end,
                        },
                        Value::I64(idx) if idx >= 0 => Segment::IdxSelector {
                            idx: idx as usize,
                            start,
                            end,
                        },
                        other => {
                            return Err(ErrorKind::TypeConflict(
                                r.expand_lines(2),
                                r,
                                other.kind(),
                                vec![ValueType::I64, ValueType::String],
                            )
                            .into());
                        }
                    },
                    expr => Segment::ElementSelector { start, end, expr },
                }
            }
            RangeSelector {
                start_lower,
                range_start,
                end_lower,
                start_upper,
                range_end,
                end_upper,
            } => Segment::RangeSelector {
                start_lower,
                range_start: Box::new(range_start.up(helper)?),
                end_lower,
                start_upper,
                range_end: Box::new(range_end.up(helper)?),
                end_upper,
            },
        })
    }
    pub fn from_id(id: Ident<'script>, start: Location, end: Location) -> Self {
        Segment1::ElementSelector {
            start,
            end,
            expr: ImutExpr1::Literal(Literal {
                start,
                end,
                value: Value::String(id.id),
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
pub enum Segment<'script, Ctx: Context + Clone + 'static> {
    IdSelector {
        id: Cow<'script, str>,
        start: Location,
        end: Location,
    },
    IdxSelector {
        idx: usize,
        start: Location,
        end: Location,
    },
    ElementSelector {
        expr: ImutExpr<'script, Ctx>,
        start: Location,
        end: Location,
    },
    RangeSelector {
        start_lower: Location,
        range_start: Box<ImutExpr<'script, Ctx>>,
        end_lower: Location,
        start_upper: Location,
        range_end: Box<ImutExpr<'script, Ctx>>,
        end_upper: Location,
    },
}

impl<'script, Ctx: Context + Clone + 'static> PartialEq for Segment<'script, Ctx> {
    fn eq(&self, other: &Self) -> bool {
        use Segment::*;
        match (self, other) {
            (IdSelector { id: id1, .. }, IdSelector { id: id2, .. }) => id1 == id2,
            (IdxSelector { idx: idx1, .. }, IdxSelector { idx: idx2, .. }) => idx1 == idx2,
            (ElementSelector { expr: expr1, .. }, ElementSelector { expr: expr2, .. }) => {
                expr1 == expr2
            }
            (
                RangeSelector {
                    range_start: start1,
                    range_end: end1,
                    ..
                },
                RangeSelector {
                    range_start: start2,
                    range_end: end2,
                    ..
                },
            ) => start1 == start2 && end1 == end2,
            _ => false,
        }
    }
}

impl<'script, Ctx: Context + Clone + 'static> BaseExpr for Segment<'script, Ctx> {
    fn s(&self) -> Location {
        match self {
            Segment::IdSelector { start, .. } => *start,
            Segment::IdxSelector { start, .. } => *start,
            Segment::ElementSelector { start, .. } => *start,
            Segment::RangeSelector { start_lower, .. } => *start_lower,
        }
    }
    fn e(&self) -> Location {
        match self {
            Segment::IdSelector { end, .. } => *end,
            Segment::IdxSelector { end, .. } => *end,
            Segment::ElementSelector { end, .. } => *end,
            Segment::RangeSelector { end_upper, .. } => *end_upper,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct LocalPath1<'script> {
    pub start: Location,
    pub end: Location,
    pub segments: Segments1<'script>,
}
impl<'script> LocalPath1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<LocalPath<'script, Ctx>> {
        let segments: Result<Segments<'script, Ctx>> =
            self.segments.into_iter().map(|p| p.up(helper)).collect();
        let mut segments = segments?.into_iter();
        if let Some(Segment::IdSelector { id, .. }) = segments.next() {
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
            //error!
            unreachable!()
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct LocalPath<'script, Ctx: Context + Clone + 'static> {
    pub id: Cow<'script, str>,
    pub idx: usize,
    pub is_const: bool,
    pub start: Location,
    pub end: Location,
    pub segments: Segments<'script, Ctx>,
}
impl_expr!(LocalPath);

impl<'script, Ctx: Context + Clone + 'static> PartialEq for LocalPath<'script, Ctx> {
    fn eq(&self, other: &Self) -> bool {
        self.idx == other.idx && self.is_const == other.is_const && self.segments == other.segments
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MetadataPath1<'script> {
    pub start: Location,
    pub end: Location,
    pub segments: Segments1<'script>,
}
impl<'script> MetadataPath1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<MetadataPath<'script, Ctx>> {
        let segments: Result<Segments<'script, Ctx>> =
            self.segments.into_iter().map(|p| p.up(helper)).collect();
        Ok(MetadataPath {
            start: self.start,
            end: self.end,
            segments: segments?,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct MetadataPath<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub segments: Segments<'script, Ctx>,
}
impl_expr!(MetadataPath);

impl<'script, Ctx: Context + Clone + 'static> PartialEq for MetadataPath<'script, Ctx> {
    fn eq(&self, other: &Self) -> bool {
        self.segments == other.segments
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EventPath1<'script> {
    pub start: Location,
    pub end: Location,
    pub segments: Segments1<'script>,
}
impl<'script> EventPath1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<EventPath<'script, Ctx>> {
        let segments: Result<Segments<'script, Ctx>> =
            self.segments.into_iter().map(|p| p.up(helper)).collect();
        Ok(EventPath {
            start: self.start,
            end: self.end,
            segments: segments?,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct EventPath<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub segments: Segments<'script, Ctx>,
}
impl_expr!(EventPath);

impl<'script, Ctx: Context + Clone + 'static> PartialEq for EventPath<'script, Ctx> {
    fn eq(&self, other: &Self) -> bool {
        self.segments == other.segments
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Serialize)]
pub enum BinOpKind {
    Or,
    And,
    Eq,
    NotEq,

    Gte,
    Gt,
    Lte,
    Lt,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl fmt::Display for BinOpKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BinOpKind::Or => write!(f, "or"),
            BinOpKind::And => write!(f, "and"),

            BinOpKind::Eq => write!(f, "=="),
            BinOpKind::NotEq => write!(f, "!="),
            BinOpKind::Gte => write!(f, ">="),
            BinOpKind::Gt => write!(f, ">"),
            BinOpKind::Lte => write!(f, "<="),
            BinOpKind::Lt => write!(f, "<"),

            BinOpKind::Add => write!(f, "+"),
            BinOpKind::Sub => write!(f, "-"),
            BinOpKind::Mul => write!(f, "*"),
            BinOpKind::Div => write!(f, "/"),
            BinOpKind::Mod => write!(f, "%"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BinExpr1<'script> {
    pub start: Location,
    pub end: Location,
    pub kind: BinOpKind,
    pub lhs: ImutExpr1<'script>,
    pub rhs: ImutExpr1<'script>,
}
impl<'script> BinExpr1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<BinExpr<'script, Ctx>> {
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
pub struct BinExpr<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub kind: BinOpKind,
    pub lhs: ImutExpr<'script, Ctx>,
    pub rhs: ImutExpr<'script, Ctx>,
}
impl_expr!(BinExpr);

#[derive(Copy, Clone, Debug, PartialEq, Serialize)]
pub enum UnaryOpKind {
    Plus,
    Minus,
    Not,
}
impl fmt::Display for UnaryOpKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UnaryOpKind::Plus => write!(f, "+"),
            UnaryOpKind::Minus => write!(f, "-"),
            UnaryOpKind::Not => write!(f, "not"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct UnaryExpr1<'script> {
    pub start: Location,
    pub end: Location,
    pub kind: UnaryOpKind,
    pub expr: ImutExpr1<'script>,
}

impl<'script> UnaryExpr1<'script> {
    fn up<Ctx: Context + Clone + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<UnaryExpr<'script, Ctx>> {
        Ok(UnaryExpr {
            start: self.start,
            end: self.end,
            kind: self.kind,
            expr: self.expr.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct UnaryExpr<'script, Ctx: Context + Clone + 'static> {
    pub start: Location,
    pub end: Location,
    pub kind: UnaryOpKind,
    pub expr: ImutExpr<'script, Ctx>,
}
impl_expr!(UnaryExpr);

pub type Exprs<'script, Ctx> = Vec<Expr<'script, Ctx>>;
pub type Exprs1<'script> = Vec<Expr1<'script>>;
pub type ImutExprs<'script, Ctx> = Vec<ImutExpr<'script, Ctx>>;
pub type ImutExprs1<'script> = Vec<ImutExpr1<'script>>;
pub type Fields<'script, Ctx> = Vec<Field<'script, Ctx>>;
pub type Fields1<'script> = Vec<Field1<'script>>;
pub type Segments<'script, Ctx> = Vec<Segment<'script, Ctx>>;
pub type Segments1<'script> = Vec<Segment1<'script>>;
pub type PatternFields<'script, Ctx> = Vec<PredicatePattern<'script, Ctx>>;
pub type PatternFields1<'script> = Vec<PredicatePattern1<'script>>;
pub type Predicates<'script, Ctx> = Vec<PredicateClause<'script, Ctx>>;
pub type Predicates1<'script> = Vec<PredicateClause1<'script>>;
pub type ImutPredicates<'script, Ctx> = Vec<ImutPredicateClause<'script, Ctx>>;
pub type ImutPredicates1<'script> = Vec<ImutPredicateClause1<'script>>;
pub type PatchOperations<'script, Ctx> = Vec<PatchOperation<'script, Ctx>>;
pub type PatchOperations1<'script> = Vec<PatchOperation1<'script>>;
pub type ComprehensionCases<'script, Ctx> = Vec<ComprehensionCase<'script, Ctx>>;
pub type ComprehensionCases1<'script> = Vec<ComprehensionCase1<'script>>;
pub type ImutComprehensionCases<'script, Ctx> = Vec<ImutComprehensionCase<'script, Ctx>>;
pub type ImutComprehensionCases1<'script> = Vec<ImutComprehensionCase1<'script>>;
pub type ArrayPredicatePatterns<'script, Ctx> = Vec<ArrayPredicatePattern<'script, Ctx>>;
pub type ArrayPredicatePatterns1<'script> = Vec<ArrayPredicatePattern1<'script>>;

fn replace_last_shadow_use<'script, Ctx: Context + Clone + 'static>(
    replace_idx: usize,
    expr: Expr<'script, Ctx>,
) -> Expr<'script, Ctx> {
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
            let mut m: Match<'script, Ctx> = *m;
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
