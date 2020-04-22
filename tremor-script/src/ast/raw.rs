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

#![doc(hidden)]
// We want to keep the names here
#![allow(clippy::module_name_repetitions)]
use super::upable::Upable;
use super::*;
use crate::errors::*;
use crate::impl_expr;
use crate::interpreter::{exec_binary, exec_unary};
use crate::pos::{Location, Range};
use crate::registry::CustomFn;
use crate::registry::{Aggr as AggrRegistry, Registry};
use crate::tilde::Extractor;
use crate::EventContext;
pub use base_expr::BaseExpr;
use halfbrown::hashmap;
use halfbrown::HashMap;
pub use query::*;
use serde::Serialize;
use simd_json::value::borrowed;
use simd_json::{prelude::*, BorrowedValue as Value, KnownKey};
use std::borrow::Cow;

/// A raw script we got to put this here because of silly lalrpoop focing it to be public
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ScriptRaw<'script> {
    imports: Imports<'script>,
    exprs: ExprsRaw<'script>,
    doc: Option<Vec<Cow<'script, str>>>,
}

impl<'script> ScriptRaw<'script> {
    pub(crate) fn new(
        imports: Imports<'script>,
        exprs: ExprsRaw<'script>,
        doc: Option<Vec<Cow<'script, str>>>,
    ) -> Self {
        Self {
            imports,
            exprs,
            doc,
        }
    }
    pub(crate) fn up_script<'registry>(
        self,
        reg: &'registry Registry,
        aggr_reg: &'registry AggrRegistry,
    ) -> Result<(Script<'script>, Vec<Warning>)> {
        const COMPILATION_UNIT_PART: u64 = 0; // FIXME include preprocessing

        let mut helper = Helper::new(reg, aggr_reg);
        helper
            .consts
            .insert(vec!["window".to_owned()], WINDOW_CONST_ID);
        helper
            .consts
            .insert(vec!["group".to_owned()], GROUP_CONST_ID);
        helper.consts.insert(vec!["args".to_owned()], ARGS_CONST_ID);

        // TODO: Document why three `null` values are put in the constants vector.
        let mut consts: Vec<Value> = vec![Value::null(); 3];
        let mut exprs = vec![];
        let last_idx = self.exprs.len() - 1;
        for (i, e) in self.exprs.into_iter().enumerate() {
            match e {
                ExprRaw::Module(m) => {
                    m.define(reg, aggr_reg, &mut consts, &mut helper)?;
                }
                ExprRaw::Const {
                    name,
                    expr,
                    start,
                    end,
                    comment,
                } => {
                    let name_v = vec![name.to_string()];
                    if helper.consts.insert(name_v.clone(), consts.len()).is_some() {
                        return Err(ErrorKind::DoubleConst(
                            Range::from((start, end)).expand_lines(2),
                            Range::from((start, end)),
                            name.to_string(),
                        )
                        .into());
                    }

                    let expr = expr.up(&mut helper)?;
                    if i == last_idx {
                        exprs.push(Expr::Imut(ImutExprInt::Local {
                            is_const: true,
                            idx: consts.len(),
                            mid: helper.add_meta_w_name(
                                start,
                                end,
                                name.clone(),
                                COMPILATION_UNIT_PART,
                            ),
                        }))
                    }
                    let v = reduce2(expr, &helper)?;
                    let value_type = v.value_type();
                    consts.push(v);
                    helper.docs.consts.push(ConstDoc {
                        name: name,
                        doc: comment
                            .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
                        value_type,
                    });
                }
                #[allow(unreachable_code, unused_variables)]
                #[cfg_attr(tarpaulin, skip)]
                ExprRaw::FnDecl(f) => {
                    helper.docs.fns.push(f.doc());
                    let f = f.up(&mut helper)?;
                    let f = CustomFn {
                        name: f.name.id,
                        args: f.args.iter().map(|i| i.id.to_string()).collect(),
                        locals: f.locals,
                        body: f.body,
                        is_const: false, // FIXME .unwrap()
                        open: f.open,
                        inline: f.inline,
                    };

                    helper.register_fun(f)?;
                }
                other => exprs.push(other.up(&mut helper)?),
            }
        }

        // We make sure the if we return `event` we turn it into `emit event`
        // While this is not required logically it allows us to
        // take advantage of the `emit event` optimisation
        if let Some(e) = exprs.last_mut() {
            if let Expr::Imut(ImutExprInt::Path(Path::Event(p))) = e {
                if p.segments.is_empty() {
                    let expr = EmitExpr {
                        mid: p.mid(),
                        expr: ImutExprInt::Path(Path::Event(p.clone())),
                        port: None,
                    };
                    *e = Expr::Emit(Box::new(expr));
                }
            }
        } else {
            return Err(ErrorKind::EmptyScript.into());
        }

        helper.docs.module = Some(ModDoc {
            name: "self".into(),
            doc: self
                .doc
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        });
        Ok((
            Script {
                imports: vec![], // Compiled out
                exprs,
                consts,
                aggregates: helper.aggregates,
                locals: helper.locals.len(),
                node_meta: helper.meta,
                compilation_units: hashmap![], // FIXME cpp
                functions: helper.func_vec,
                docs: helper.docs,
            },
            helper.warnings,
        ))
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct ModuleRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub name: IdentRaw<'script>,
    pub exprs: ExprsRaw<'script>,
    pub doc: Option<Vec<Cow<'script, str>>>,
}
impl_expr!(ModuleRaw);

impl<'script> ModuleRaw<'script> {
    pub(crate) fn define<'registry>(
        self,
        reg: &'registry Registry,
        aggr_reg: &'registry AggrRegistry,
        consts: &mut Vec<Value<'script>>,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<()> {
        helper.module.push(self.name.id.to_string());
        for e in self.exprs.into_iter() {
            match e {
                ExprRaw::Module(m) => {
                    m.define(reg, aggr_reg, consts, helper)?;
                }
                ExprRaw::Const {
                    name,
                    expr,
                    start,
                    end,
                    comment,
                } => {
                    let mut name_v = helper.module.clone();
                    name_v.push(name.to_string());
                    if helper.consts.contains_key(&name_v) {
                        return Err(ErrorKind::DoubleConst(
                            Range::from((start, end)).expand_lines(2),
                            Range::from((start, end)),
                            name.to_string(),
                        )
                        .into());
                    }

                    helper.consts.insert(name_v.clone(), consts.len());
                    let expr = expr.up(helper)?;
                    let v = reduce2(expr, &helper)?;
                    let value_type = v.value_type();
                    consts.push(v);
                    helper.docs.consts.push(ConstDoc {
                        name: name,
                        doc: comment
                            .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
                        value_type,
                    });
                }
                #[allow(unreachable_code, unused_variables)]
                #[cfg_attr(tarpaulin, skip)]
                ExprRaw::FnDecl(f) => {
                    helper.docs.fns.push(f.doc());
                    let f = f.up(helper)?;
                    let f = CustomFn {
                        name: f.name.id,
                        args: f.args.iter().map(|i| i.id.to_string()).collect(),
                        locals: f.locals,
                        body: f.body,
                        is_const: false, // FIXME .unwrap()
                        open: f.open,
                        inline: f.inline,
                    };

                    helper.register_fun(f)?;
                }
                e => {
                    return error_generic(
                        &e,
                        &e,
                        "Can't have expressions inside of modules",
                        &helper.meta,
                    )
                }
            }
        }
        helper.module.pop();
        Ok(())
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct IdentRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub id: Cow<'script, str>,
}
impl_expr!(IdentRaw);

impl<'script> Upable<'script> for IdentRaw<'script> {
    type Target = Ident<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Self::Target {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            id: self.id,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FieldRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) name: StringLitRaw<'script>,
    pub(crate) value: ImutExprRaw<'script>,
}

impl<'script> Upable<'script> for FieldRaw<'script> {
    type Target = Field<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let name = ImutExprRaw::String(self.name).up(helper)?;
        Ok(Field {
            mid: helper.add_meta(self.start, self.end),
            name,
            value: self.value.up(helper)?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RecordRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) fields: FieldsRaw<'script>,
}
impl_expr!(RecordRaw);

impl<'script> Upable<'script> for RecordRaw<'script> {
    type Target = Record<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Record {
            mid: helper.add_meta(self.start, self.end),
            fields: self.fields.up(helper)?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ListRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) exprs: ImutExprsRaw<'script>,
}
impl_expr!(ListRaw);

impl<'script> Upable<'script> for ListRaw<'script> {
    type Target = List<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(List {
            mid: helper.add_meta(self.start, self.end),
            exprs: self.exprs.up(helper)?.into_iter().map(ImutExpr).collect(),
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct LiteralRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) value: Value<'script>,
}
impl_expr!(LiteralRaw);
impl<'script> Upable<'script> for LiteralRaw<'script> {
    type Target = Literal<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Literal {
            mid: helper.add_meta(self.start, self.end),
            value: self.value,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StringLitRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) string: Cow<'script, str>,
    pub(crate) exprs: ImutExprsRaw<'script>,
}

/// we're forced to make this pub because of lalrpop
pub struct StrLitElements<'script>(
    pub(crate) Vec<Cow<'script, str>>,
    pub(crate) ImutExprsRaw<'script>,
);

impl<'script> From<StrLitElements<'script>> for StringLitRaw<'script> {
    fn from(mut es: StrLitElements<'script>) -> StringLitRaw<'script> {
        es.0.reverse();
        es.1.reverse();

        let string = if es.0.len() == 1 {
            es.0.pop().unwrap_or_default()
        } else {
            let mut s = String::new();
            for e in es.0 {
                s.push_str(&e);
            }
            s.into()
        };
        StringLitRaw {
            start: Location::default(),
            end: Location::default(),
            string,
            exprs: es.1,
        }
    }
}

pub(crate) fn reduce2<'script>(
    expr: ImutExprInt<'script>,
    helper: &Helper,
) -> Result<Value<'script>> {
    match expr {
        ImutExprInt::Literal(Literal { value: v, .. }) => Ok(v),
        other => Err(ErrorKind::NotConstant(
            other.extent(&helper.meta),
            other.extent(&helper.meta).expand_lines(2),
        )
        .into()),
    }
}

impl<'script> ImutExprInt<'script> {
    pub(crate) fn reduce(self, helper: &Helper) -> Result<Self> {
        match self {
            ImutExprInt::Unary(u) => match *u {
                u1
                @
                UnaryExpr {
                    expr: ImutExprInt::Literal(_),
                    ..
                } => {
                    let expr = reduce2(u1.expr.clone(), &helper)?;
                    let value = if let Some(v) = exec_unary(u1.kind, &expr) {
                        v.into_owned()
                    } else {
                        let ex = u1.extent(&helper.meta);
                        return Err(ErrorKind::InvalidUnary(
                            ex.expand_lines(2),
                            ex,
                            u1.kind,
                            expr.value_type(),
                        )
                        .into());
                    };

                    let lit = Literal { mid: u1.mid, value };
                    Ok(ImutExprInt::Literal(lit))
                }
                u1 => Ok(ImutExprInt::Unary(Box::new(u1))),
            },

            ImutExprInt::Binary(b) => {
                match *b {
                    b1
                    @
                    BinExpr {
                        lhs: ImutExprInt::Literal(_),
                        rhs: ImutExprInt::Literal(_),
                        ..
                    } => {
                        let lhs = reduce2(b1.lhs.clone(), &helper)?;
                        let rhs = reduce2(b1.rhs.clone(), &helper)?;
                        // TODO remove duplicate params?
                        let value =
                            exec_binary(&b1, &b1, &helper.meta, b1.kind, &lhs, &rhs)?.into_owned();
                        let lit = Literal { mid: b1.mid, value };
                        Ok(ImutExprInt::Literal(lit))
                    }
                    b1 => Ok(ImutExprInt::Binary(Box::new(b1))),
                }
            }
            ImutExprInt::List(l) => {
                if l.exprs.iter().map(|v| &v.0).all(is_lit) {
                    let elements: Result<Vec<Value>> =
                        l.exprs.into_iter().map(|v| reduce2(v.0, &helper)).collect();
                    Ok(ImutExprInt::Literal(Literal {
                        mid: l.mid,
                        value: Value::Array(elements?),
                    }))
                } else {
                    Ok(ImutExprInt::List(l))
                }
            }
            ImutExprInt::Record(r) => {
                if r.fields.iter().all(|f| is_lit(&f.name) && is_lit(&f.value)) {
                    let obj: Result<borrowed::Object> = r
                        .fields
                        .into_iter()
                        .map(|f| {
                            reduce2(f.name.clone(), &helper).and_then(|n| {
                                // ALLOW: The grammer guarantees the key of a record is always a string
                                let n = n.as_str().unwrap_or_else(|| unreachable!());
                                reduce2(f.value, &helper).map(|v| (n.to_owned().into(), v))
                            })
                        })
                        .collect();
                    Ok(ImutExprInt::Literal(Literal {
                        mid: r.mid,
                        value: Value::from(obj?),
                    }))
                } else {
                    Ok(ImutExprInt::Record(r))
                }
            }
            ImutExprInt::Invoke1(i)
            | ImutExprInt::Invoke2(i)
            | ImutExprInt::Invoke3(i)
            | ImutExprInt::Invoke(i) => {
                if i.invocable.is_const() && i.args.iter().all(|f| is_lit(&f.0)) {
                    let ex = i.extent(&helper.meta);
                    let args: Result<Vec<Value<'script>>> =
                        i.args.into_iter().map(|v| reduce2(v.0, &helper)).collect();
                    let args = args?;
                    let mut args2: Vec<&Value<'script>> = Vec::new();
                    unsafe {
                        for i in 0..args.len() {
                            args2.push(args.get_unchecked(i));
                        }
                    }
                    let v = i
                        .invocable
                        .invoke(&EventContext::default(), &args2)
                        .map_err(|e| e.into_err(&ex, &ex, Some(&helper.reg), &helper.meta))?
                        .into_static();
                    Ok(ImutExprInt::Literal(Literal {
                        value: v,
                        mid: i.mid,
                    }))
                } else {
                    Ok(match i.args.len() {
                        1 => ImutExprInt::Invoke1(i),
                        2 => ImutExprInt::Invoke2(i),
                        3 => ImutExprInt::Invoke3(i),
                        _ => ImutExprInt::Invoke(i),
                    })
                }
            }
            other => Ok(other),
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ExprRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Const {
        /// we're forced to make this pub because of lalrpop
        name: Cow<'script, str>,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        start: Location,
        /// we're forced to make this pub because of lalrpop
        end: Location,
        /// we're forced to make this pub because of lalrpop
        comment: Option<Vec<Cow<'script, str>>>,
    },
    /// we're forced to make this pub because of lalrpop
    Module(ModuleRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    MatchExpr(Box<MatchRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Assign(Box<AssignRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Comprehension(Box<ComprehensionRaw<'script>>),
    Drop {
        /// we're forced to make this pub because of lalrpop
        start: Location,
        /// we're forced to make this pub because of lalrpop
        end: Location,
    },
    /// we're forced to make this pub because of lalrpop
    Emit(Box<EmitExprRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    FnDecl(AnyFnRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Imut(ImutExprRaw<'script>),
}

impl<'script> Upable<'script> for ExprRaw<'script> {
    type Target = Expr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(match self {
            ExprRaw::Module(ModuleRaw { start, end, .. }) => {
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
            ExprRaw::Const { start, end, .. } => {
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
            ExprRaw::MatchExpr(m) => Expr::Match(Box::new(m.up(helper)?)),
            ExprRaw::Assign(a) => {
                let path = a.path.up(helper)?;
                let mid = helper.add_meta(a.start, a.end);
                match a.expr.up(helper)? {
                    Expr::Imut(ImutExprInt::Merge(m)) => {
                        if path_eq(&path, &m.target) {
                            Expr::MergeInPlace(Box::new(*m))
                        } else {
                            Expr::Assign {
                                mid,
                                path,
                                expr: Box::new(ImutExprInt::Merge(m).into()),
                            }
                        }
                    }
                    Expr::Imut(ImutExprInt::Patch(m)) => {
                        if path_eq(&path, &m.target) {
                            Expr::PatchInPlace(Box::new(*m))
                        } else {
                            Expr::Assign {
                                mid,
                                path,
                                expr: Box::new(ImutExprInt::Patch(m).into()),
                            }
                        }
                    }
                    expr => Expr::Assign {
                        mid,
                        path,
                        expr: Box::new(expr),
                    },
                }
            }
            ExprRaw::Comprehension(c) => Expr::Comprehension(Box::new(c.up(helper)?)),
            ExprRaw::Drop { start, end } => {
                let mid = helper.add_meta(start, end);
                if !helper.can_emit {
                    return Err(ErrorKind::InvalidDrop(
                        Range(start, end).expand_lines(2),
                        Range(start, end),
                    )
                    .into());
                }
                Expr::Drop { mid }
            }
            ExprRaw::Emit(e) => Expr::Emit(Box::new(e.up(helper)?)),
            ExprRaw::Imut(i) => i.up(helper)?.into(),
            ExprRaw::FnDecl(f) => {
                // There is no code path that leads here,
                // we still rather have an error in case we made
                // an error then unreachable
                #[cfg_attr(tarpaulin, skip)]
                return Err(ErrorKind::InvalidFn(
                    f.extent(&helper.meta).expand_lines(2),
                    f.extent(&helper.meta),
                )
                .into());
            }
        })
    }
}

impl<'script> BaseExpr for ExprRaw<'script> {
    fn mid(&self) -> usize {
        0
    }
    fn s(&self, meta: &NodeMetas) -> Location {
        match self {
            ExprRaw::Const { start, .. } => *start,
            ExprRaw::Module(e) => e.s(meta),
            ExprRaw::MatchExpr(e) => e.s(meta),
            ExprRaw::Assign(e) => e.s(meta),
            ExprRaw::Comprehension(e) => e.s(meta),
            ExprRaw::Drop { start, .. } => *start,
            ExprRaw::Emit(e) => e.s(meta),
            ExprRaw::FnDecl(e) => e.s(meta),
            ExprRaw::Imut(e) => e.s(meta),
        }
    }
    fn e(&self, meta: &NodeMetas) -> Location {
        match self {
            ExprRaw::Const { end, .. } => *end,
            ExprRaw::Module(e) => e.e(meta),
            ExprRaw::MatchExpr(e) => e.e(meta),
            ExprRaw::Assign(e) => e.e(meta),
            ExprRaw::Comprehension(e) => e.e(meta),
            ExprRaw::Drop { end, .. } => *end,
            ExprRaw::Emit(e) => e.e(meta),
            ExprRaw::FnDecl(e) => e.e(meta),
            ExprRaw::Imut(e) => e.e(meta),
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FnDeclRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) name: IdentRaw<'script>,
    pub(crate) args: Vec<IdentRaw<'script>>,
    pub(crate) body: ExprsRaw<'script>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
    pub(crate) open: bool,
    pub(crate) inline: bool,
}
impl_expr!(FnDeclRaw);

impl<'script> FnDeclRaw<'script> {
    pub(crate) fn doc(&self) -> FnDoc<'script> {
        FnDoc {
            name: self.name.id.clone(),
            args: self.args.iter().map(|a| a.id.clone()).collect(),
            open: self.open,
            doc: self
                .doc
                .clone()
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        }
    }
}

impl<'script> Upable<'script> for FnDeclRaw<'script> {
    type Target = FnDecl<'script>;
    #[cfg_attr(tarpaulin, skip)]
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let can_emit = helper.can_emit;
        let mut aggrs = Vec::new();
        let mut consts = HashMap::new();
        let mut locals: HashMap<_, _> = self
            .args
            .iter()
            .enumerate()
            .map(|(i, a)| (a.id.to_string(), i))
            .collect();

        helper.can_emit = false;
        helper.is_open = self.open;
        helper.fn_argc = self.args.len();

        helper.swap(&mut aggrs, &mut consts, &mut locals);
        helper.possible_leaf = true;
        let body = self.body.up(helper)?;
        helper.possible_leaf = false;
        helper.swap(&mut aggrs, &mut consts, &mut locals);
        helper.can_emit = can_emit;
        let name = self.name.up(helper)?;
        Ok(FnDecl {
            mid: helper.add_meta_w_name(self.start, self.end, &name.id),
            name,
            args: self.args.up(helper)?,
            body,
            locals: locals.len(),
            open: self.open,
            inline: self.inline,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum AnyFnRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Match(MatchFnDeclRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Normal(FnDeclRaw<'script>),
}
impl<'script> AnyFnRaw<'script> {
    pub(crate) fn doc(&self) -> FnDoc<'script> {
        match self {
            AnyFnRaw::Match(f) => f.doc(),
            AnyFnRaw::Normal(f) => f.doc(),
        }
    }
}

impl<'script> Upable<'script> for AnyFnRaw<'script> {
    type Target = FnDecl<'script>;
    #[cfg_attr(tarpaulin, skip)]
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match self {
            AnyFnRaw::Normal(f) => f.up(helper),
            AnyFnRaw::Match(f) => f.up(helper),
        }
    }
}

impl<'script> BaseExpr for AnyFnRaw<'script> {
    fn mid(&self) -> usize {
        0
    }

    fn s(&self, _meta: &NodeMetas) -> Location {
        match self {
            AnyFnRaw::Match(m) => m.start,
            AnyFnRaw::Normal(m) => m.start,
        }
    }

    fn e(&self, _meta: &NodeMetas) -> Location {
        match self {
            AnyFnRaw::Match(m) => m.end,
            AnyFnRaw::Normal(m) => m.end,
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MatchFnDeclRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) name: IdentRaw<'script>,
    pub(crate) args: Vec<IdentRaw<'script>>,
    pub(crate) cases: Vec<PredicateClauseRaw<'script>>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
    pub(crate) open: bool,
    pub(crate) inline: bool,
}
impl_expr!(MatchFnDeclRaw);

impl<'script> MatchFnDeclRaw<'script> {
    pub(crate) fn doc(&self) -> FnDoc<'script> {
        FnDoc {
            name: self.name.id.clone(),
            args: self.args.iter().map(|a| a.id.clone()).collect(),
            open: self.open,
            doc: self
                .doc
                .clone()
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        }
    }
}

impl<'script> Upable<'script> for MatchFnDeclRaw<'script> {
    type Target = FnDecl<'script>;
    #[cfg_attr(tarpaulin, skip)]
    fn up<'registry>(mut self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let can_emit = helper.can_emit;
        let mut aggrs = Vec::new();
        let mut locals = HashMap::new();
        let mut consts = HashMap::new();
        consts.insert(vec!["args".to_owned()], ARGS_CONST_ID);

        for (i, a) in self.args.iter().enumerate() {
            locals.insert(a.id.to_string(), i);
        }

        helper.is_open = self.open;
        helper.fn_argc = self.args.len();

        helper.can_emit = false;
        helper.swap(&mut aggrs, &mut consts, &mut locals);

        let target = self
            .args
            .iter()
            .map(|a| {
                ImutExprRaw::Path(PathRaw::Local(LocalPathRaw {
                    start: a.start,
                    end: a.end,
                    segments: vec![SegmentRaw::from_id(a.clone())],
                }))
            })
            .collect();

        let mut patterns = Vec::new();

        std::mem::swap(&mut self.cases, &mut patterns);

        let patterns = patterns
            .into_iter()
            .map(|mut c: PredicateClauseRaw| {
                if c.pattern == PatternRaw::Default {
                    c
                } else {
                    let mut exprs: ExprsRaw<'script> = self
                        .args
                        .iter()
                        .enumerate()
                        .map(|(i, a)| {
                            ExprRaw::Assign(Box::new(AssignRaw {
                                start: c.start,
                                end: c.end,
                                path: PathRaw::Local(LocalPathRaw {
                                    start: a.start,
                                    end: a.end,
                                    segments: vec![SegmentRaw::from_id(a.clone())],
                                }),
                                expr: ExprRaw::Imut(ImutExprRaw::Path(PathRaw::Local(
                                    LocalPathRaw {
                                        start: a.start,
                                        end: a.end,
                                        segments: vec![
                                            SegmentRaw::from_str(FN_RES_NAME, a.start, a.end),
                                            SegmentRaw::from_usize(i, a.start, a.end),
                                        ],
                                    },
                                ))),
                            }))
                        })
                        .collect();
                    exprs.append(&mut c.exprs);
                    c.exprs = exprs;
                    c
                }
            })
            .collect();

        let body = ExprRaw::MatchExpr(Box::new(MatchRaw {
            start: self.start,
            end: self.end,
            target: ImutExprRaw::List(Box::new(ListRaw {
                start: self.start,
                end: self.end,
                exprs: target,
            })),
            patterns,
        }));
        helper.possible_leaf = true;
        let body = body.up(helper)?;
        helper.possible_leaf = false;
        let body = vec![body];

        helper.swap(&mut aggrs, &mut consts, &mut locals);
        helper.can_emit = can_emit;
        let name = self.name.up(helper)?;
        Ok(FnDecl {
            mid: helper.add_meta_w_name(self.start, self.end, &name),
            name,
            args: self.args.up(helper)?,
            body,
            locals: locals.len(),
            open: self.open,
            inline: self.inline,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ImutExprRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Record(Box<RecordRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    List(Box<ListRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Patch(Box<PatchRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Merge(Box<MergeRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Match(Box<ImutMatchRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Comprehension(Box<ImutComprehensionRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Path(PathRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Binary(Box<BinExprRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Unary(Box<UnaryExprRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Literal(LiteralRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Invoke(InvokeRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Present {
        /// we're forced to make this pub because of lalrpop
        path: PathRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        start: Location,
        /// we're forced to make this pub because of lalrpop
        end: Location,
    },
    /// we're forced to make this pub because of lalrpop
    String(StringLitRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Recur(RecurRaw<'script>),
}

impl<'script> Upable<'script> for ImutExprRaw<'script> {
    type Target = ImutExprInt<'script>;
    #[allow(clippy::too_many_lines)]
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let was_leaf = helper.possible_leaf;
        helper.possible_leaf = false;
        let r = Ok(match self {
            ImutExprRaw::Recur(r) => {
                helper.possible_leaf = was_leaf;
                ImutExprInt::Recur(r.up(helper)?)
            }
            ImutExprRaw::Binary(b) => {
                ImutExprInt::Binary(Box::new(b.up(helper)?)).reduce(helper)?
            }
            ImutExprRaw::Unary(u) => ImutExprInt::Unary(Box::new(u.up(helper)?)).reduce(helper)?,
            ImutExprRaw::String(mut s) => {
                let lit = ImutExprRaw::Literal(LiteralRaw {
                    start: s.start,
                    end: s.end,
                    value: Value::String(s.string),
                });
                if s.exprs.is_empty() {
                    lit.up(helper)?
                } else {
                    let mut args = vec![lit];
                    args.append(&mut s.exprs);
                    ImutExprRaw::Invoke(InvokeRaw {
                        start: s.start,
                        end: s.end,
                        module: vec!["core".into(), "string".into()],
                        fun: "format".into(),
                        args,
                    })
                    .up(helper)?
                    .reduce(helper)?
                }
            }
            ImutExprRaw::Record(r) => ImutExprInt::Record(r.up(helper)?).reduce(helper)?,
            ImutExprRaw::List(l) => ImutExprInt::List(l.up(helper)?).reduce(helper)?,
            ImutExprRaw::Patch(p) => ImutExprInt::Patch(Box::new(p.up(helper)?)).reduce(helper)?,
            ImutExprRaw::Merge(m) => ImutExprInt::Merge(Box::new(m.up(helper)?)).reduce(helper)?,
            ImutExprRaw::Present { path, start, end } => ImutExprInt::Present {
                path: path.up(helper)?,
                mid: helper.add_meta(start, end),
            }
            .reduce(helper)?,
            ImutExprRaw::Path(p) => match p.up(helper)? {
                Path::Local(LocalPath {
                    is_const,
                    mid,
                    idx,
                    ref segments,
                }) if segments.is_empty() => ImutExprInt::Local { mid, idx, is_const },
                p => ImutExprInt::Path(p),
            }
            .reduce(helper)?,
            ImutExprRaw::Literal(l) => ImutExprInt::Literal(l.up(helper)?).reduce(helper)?,
            ImutExprRaw::Invoke(i) => {
                if i.is_aggregate(helper) {
                    ImutExprInt::InvokeAggr(i.into_aggregate().up(helper)?)
                } else {
                    let i = i.up(helper)?;
                    let i = if i.can_inline() {
                        i.inline()?
                    } else {
                        match i.args.len() {
                            1 => ImutExprInt::Invoke1(i),
                            2 => ImutExprInt::Invoke2(i),
                            3 => ImutExprInt::Invoke3(i),
                            _ => ImutExprInt::Invoke(i),
                        }
                    };
                    i.reduce(helper)?
                }
            }
            ImutExprRaw::Match(m) => {
                helper.possible_leaf = was_leaf;

                ImutExprInt::Match(Box::new(m.up(helper)?))
            }
            ImutExprRaw::Comprehension(c) => ImutExprInt::Comprehension(Box::new(c.up(helper)?)),
        });
        helper.possible_leaf = was_leaf;
        r
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RecurRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub exprs: ImutExprsRaw<'script>,
}
impl_expr!(RecurRaw);

impl<'script> Upable<'script> for RecurRaw<'script> {
    type Target = Recur<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let was_leaf = helper.possible_leaf;
        helper.possible_leaf = false;
        if !was_leaf {
            return error_generic(
                &self,
                &self,
                "Can not emit from this location",
                &helper.meta,
            );
        };
        if (helper.is_open && helper.fn_argc < self.exprs.len())
            || (!helper.is_open && helper.fn_argc != self.exprs.len())
        {
            return error_generic(
                &self,
                &self,
                format!(
                    "Wrong number of arguments expected {} but got {}",
                    helper.fn_argc,
                    self.exprs.len()
                ),
                &helper.meta,
            );
        }
        let exprs = self.exprs.up(helper)?.into_iter().map(ImutExpr).collect();
        helper.possible_leaf = was_leaf;

        Ok(Recur {
            mid: helper.add_meta(self.start, self.end),
            argc: helper.fn_argc,
            open: helper.is_open,

            exprs,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EmitExprRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub expr: ImutExprRaw<'script>,
    pub port: Option<ImutExprRaw<'script>>,
}
impl_expr!(EmitExprRaw);

impl<'script> Upable<'script> for EmitExprRaw<'script> {
    type Target = EmitExpr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        if !helper.can_emit {
            return Err(ErrorKind::InvalidEmit(
                self.extent(&helper.meta).expand_lines(2),
                self.extent(&helper.meta),
            )
            .into());
        }
        Ok(EmitExpr {
            mid: helper.add_meta(self.start, self.end),
            expr: self.expr.up(helper)?,
            port: self.port.up(helper)?,
        })
    }
}
/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AssignRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) path: PathRaw<'script>,
    pub(crate) expr: ExprRaw<'script>,
}
impl_expr!(AssignRaw);

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PredicateClauseRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) pattern: PatternRaw<'script>,
    pub(crate) guard: Option<ImutExprRaw<'script>>,
    pub(crate) exprs: ExprsRaw<'script>,
}

impl<'script> Upable<'script> for PredicateClauseRaw<'script> {
    type Target = PredicateClause<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let was_leaf = helper.possible_leaf;
        helper.possible_leaf = false;
        // We run the pattern first as this might reserve a local shadow
        let pattern = self.pattern.up(helper)?;
        let guard = self.guard.up(helper)?;
        helper.possible_leaf = was_leaf;
        let exprs = self.exprs.up(helper)?;

        // If we are in an assign pattern we'd have created
        // a shadow variable, this needs to be undoine at the end
        if pattern.is_assign() {
            helper.end_shadow_var();
        }
        Ok(PredicateClause {
            mid: helper.add_meta(self.start, self.end),
            pattern,
            guard,
            exprs,
        })
    }
}
/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutPredicateClauseRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) pattern: PatternRaw<'script>,
    pub(crate) guard: Option<ImutExprRaw<'script>>,
    pub(crate) exprs: ImutExprsRaw<'script>,
}

impl<'script> Upable<'script> for ImutPredicateClauseRaw<'script> {
    type Target = ImutPredicateClause<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // We run the pattern first as this might reserve a local shadow
        let pattern = self.pattern.up(helper)?;
        let exprs = self.exprs.up(helper)?.into_iter().map(ImutExpr).collect();
        let guard = self.guard.up(helper)?;
        // If we are in an assign pattern we'd have created
        // a shadow variable, this needs to be undoine at the end
        if pattern.is_assign() {
            helper.end_shadow_var();
        }
        Ok(ImutPredicateClause {
            mid: helper.add_meta(self.start, self.end),
            pattern,
            guard,
            exprs,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PatchRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) target: ImutExprRaw<'script>,
    pub(crate) operations: PatchOperationsRaw<'script>,
}

impl<'script> Upable<'script> for PatchRaw<'script> {
    type Target = Patch<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let operations = self.operations.up(helper)?;

        Ok(Patch {
            mid: helper.add_meta(self.start, self.end),
            target: self.target.up(helper)?,
            operations,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PatchOperationRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Insert {
        /// we're forced to make this pub because of lalrpop
        ident: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    Upsert {
        /// we're forced to make this pub because of lalrpop
        ident: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    Update {
        /// we're forced to make this pub because of lalrpop
        ident: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    Erase {
        /// we're forced to make this pub because of lalrpop
        ident: ImutExprRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    Copy {
        /// we're forced to make this pub because of lalrpop
        from: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        to: ImutExprRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    Move {
        /// we're forced to make this pub because of lalrpop
        from: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        to: ImutExprRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    Merge {
        /// we're forced to make this pub because of lalrpop
        ident: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    TupleMerge {
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
    },
}

impl<'script> Upable<'script> for PatchOperationRaw<'script> {
    type Target = PatchOperation<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use PatchOperationRaw::*;
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
pub struct MergeRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExprRaw<'script>,
    pub expr: ImutExprRaw<'script>,
}

impl<'script> Upable<'script> for MergeRaw<'script> {
    type Target = Merge<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Merge {
            mid: helper.add_meta(self.start, self.end),
            target: self.target.up(helper)?,
            expr: self.expr.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ComprehensionRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExprRaw<'script>,
    pub cases: ComprehensionCasesRaw<'script>,
}
impl_expr!(ComprehensionRaw);

impl<'script> Upable<'script> for ComprehensionRaw<'script> {
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
            mid: helper.add_meta(self.start, self.end),
            target,
            cases,
            key_id,
            val_id,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutComprehensionRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExprRaw<'script>,
    pub cases: ImutComprehensionCasesRaw<'script>,
}

impl<'script> Upable<'script> for ImutComprehensionRaw<'script> {
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
            mid: helper.add_meta(self.start, self.end),
            target,
            cases,
            key_id,
            val_id,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ComprehensionCaseRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) key_name: Cow<'script, str>,
    pub(crate) value_name: Cow<'script, str>,
    pub(crate) guard: Option<ImutExprRaw<'script>>,
    pub(crate) exprs: ExprsRaw<'script>,
}

impl<'script> Upable<'script> for ComprehensionCaseRaw<'script> {
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
            mid: helper.add_meta(self.start, self.end),
            key_name: self.key_name,
            value_name: self.value_name,
            guard,
            exprs,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutComprehensionCaseRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) key_name: Cow<'script, str>,
    pub(crate) value_name: Cow<'script, str>,
    pub(crate) guard: Option<ImutExprRaw<'script>>,
    pub(crate) exprs: ImutExprsRaw<'script>,
}

impl<'script> Upable<'script> for ImutComprehensionCaseRaw<'script> {
    type Target = ImutComprehensionCase<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // regiter key and value as shadowed variables
        helper.register_shadow_var(&self.key_name);
        helper.register_shadow_var(&self.value_name);

        let guard = self.guard.up(helper)?;
        let exprs = self.exprs.up(helper)?.into_iter().map(ImutExpr).collect();

        // unregister them again
        helper.end_shadow_var();
        helper.end_shadow_var();
        Ok(ImutComprehensionCase {
            mid: helper.add_meta(self.start, self.end),
            key_name: self.key_name,
            value_name: self.value_name,
            guard,
            exprs,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PatternRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Record(RecordPatternRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Array(ArrayPatternRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Tuple(TuplePatternRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Expr(ImutExprRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Assign(AssignPatternRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    DoNotCare,
    /// we're forced to make this pub because of lalrpop
    Default,
}

impl<'script> Upable<'script> for PatternRaw<'script> {
    type Target = Pattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use PatternRaw::*;
        Ok(match self {
            //Predicate(pp) => Pattern::Predicate(pp.up(helper)?),
            Record(rp) => Pattern::Record(rp.up(helper)?),
            Array(ap) => Pattern::Array(ap.up(helper)?),
            Tuple(tp) => Pattern::Tuple(tp.up(helper)?),
            Expr(expr) => Pattern::Expr(expr.up(helper)?),
            Assign(ap) => Pattern::Assign(ap.up(helper)?),
            DoNotCare => Pattern::DoNotCare,
            Default => Pattern::Default,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PredicatePatternRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    TildeEq {
        /// we're forced to make this pub because of lalrpop
        assign: Cow<'script, str>,
        /// we're forced to make this pub because of lalrpop
        lhs: Cow<'script, str>,
        /// we're forced to make this pub because of lalrpop
        test: TestExprRaw,
    },
    /// we're forced to make this pub because of lalrpop
    Bin {
        /// we're forced to make this pub because of lalrpop
        lhs: Cow<'script, str>,
        /// we're forced to make this pub because of lalrpop
        rhs: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        kind: BinOpKind,
    },
    /// we're forced to make this pub because of lalrpop
    RecordPatternEq {
        /// we're forced to make this pub because of lalrpop
        lhs: Cow<'script, str>,
        /// we're forced to make this pub because of lalrpop
        pattern: RecordPatternRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    ArrayPatternEq {
        /// we're forced to make this pub because of lalrpop
        lhs: Cow<'script, str>,
        /// we're forced to make this pub because of lalrpop
        pattern: ArrayPatternRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    FieldPresent {
        /// we're forced to make this pub because of lalrpop
        lhs: Cow<'script, str>,
    },
    /// we're forced to make this pub because of lalrpop
    FieldAbsent {
        /// we're forced to make this pub because of lalrpop
        lhs: Cow<'script, str>,
    },
}

impl<'script> Upable<'script> for PredicatePatternRaw<'script> {
    type Target = PredicatePattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use PredicatePatternRaw::*;
        Ok(match self {
            TildeEq { assign, lhs, test } => PredicatePattern::TildeEq {
                assign,
                key: KnownKey::from(lhs.clone()),
                lhs,
                test: Box::new(test.up(helper)?),
            },
            Bin { lhs, rhs, kind } => PredicatePattern::Bin {
                key: KnownKey::from(lhs.clone()),
                lhs,
                rhs: rhs.up(helper)?,
                kind,
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
/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RecordPatternRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) fields: PatternFieldsRaw<'script>,
}

impl<'script> Upable<'script> for RecordPatternRaw<'script> {
    type Target = RecordPattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let fields = self.fields.up(helper)?;
        let present_fields: Vec<Cow<str>> = fields
            .iter()
            .filter_map(|f| {
                if let PredicatePattern::FieldPresent { lhs, .. } = f {
                    Some(lhs.clone())
                } else {
                    None
                }
            })
            .collect();

        let absent_fields: Vec<Cow<str>> = fields
            .iter()
            .filter_map(|f| {
                if let PredicatePattern::FieldAbsent { lhs, .. } = f {
                    Some(lhs.clone())
                } else {
                    None
                }
            })
            .collect();

        for present in &present_fields {
            let duplicated = fields.iter().any(|f| {
                if let PredicatePattern::FieldPresent { .. } = f {
                    false
                } else {
                    f.lhs() == present
                }
            });
            if duplicated {
                let extent = (self.start, self.end).into();
                helper.warnings.push(Warning {
                    inner: extent,
                    outer: extent.expand_lines(2),
                    msg: format!("The field {} is checked with both present and another extractor, this is redundant as extractors imply presence. It may also overwrite the result of th extractor.", present),
                })
            }
        }

        for absent in &absent_fields {
            let duplicated = fields.iter().any(|f| {
                if let PredicatePattern::FieldAbsent { .. } = f {
                    false
                } else {
                    f.lhs() == absent
                }
            });
            if duplicated {
                let extent = (self.start, self.end).into();
                helper.warnings.push(Warning {
                    inner: extent,
                    outer: extent.expand_lines(2),
                    msg: format!("The field {} is checked with both absence and another extractor, this test can never be true.", absent),
                })
            }
        }

        Ok(RecordPattern {
            mid: helper.add_meta(self.start, self.end),
            fields,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ArrayPredicatePatternRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Expr(ImutExprRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Tilde(TestExprRaw),
    /// we're forced to make this pub because of lalrpop
    Record(RecordPatternRaw<'script>),
    /// Ignore
    Ignore,
}
impl<'script> Upable<'script> for ArrayPredicatePatternRaw<'script> {
    type Target = ArrayPredicatePattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use ArrayPredicatePatternRaw::*;
        Ok(match self {
            Expr(expr) => ArrayPredicatePattern::Expr(expr.up(helper)?),
            Tilde(te) => ArrayPredicatePattern::Tilde(te.up(helper)?),
            Record(rp) => ArrayPredicatePattern::Record(rp.up(helper)?),
            Ignore => ArrayPredicatePattern::Ignore,
            //Array(ap) => ArrayPredicatePattern::Array(ap),
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ArrayPatternRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) exprs: ArrayPredicatePatternsRaw<'script>,
}

impl<'script> Upable<'script> for ArrayPatternRaw<'script> {
    type Target = ArrayPattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let exprs = self.exprs.up(helper)?;
        Ok(ArrayPattern {
            mid: helper.add_meta(self.start, self.end),
            exprs,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TuplePatternRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) exprs: ArrayPredicatePatternsRaw<'script>,
    pub(crate) open: bool,
}

impl<'script> Upable<'script> for TuplePatternRaw<'script> {
    type Target = TuplePattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let exprs = self.exprs.up(helper)?;
        Ok(TuplePattern {
            mid: helper.add_meta(self.start, self.end),
            exprs,
            open: self.open,
        })
    }
}

pub(crate) const FN_RES_NAME: &str = "__fn_assign_this_is_ugly";

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AssignPatternRaw<'script> {
    pub(crate) id: Cow<'script, str>,
    pub(crate) pattern: Box<PatternRaw<'script>>,
}

impl<'script> Upable<'script> for AssignPatternRaw<'script> {
    type Target = AssignPattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(AssignPattern {
            idx: helper.register_shadow_var(&self.id),
            id: self.id,
            pattern: Box::new(self.pattern.up(helper)?),
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PathRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Local(LocalPathRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Event(EventPathRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    State(StatePathRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Meta(MetadataPathRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Const(ConstPathRaw<'script>),
}

impl<'script> Upable<'script> for PathRaw<'script> {
    type Target = Path<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use PathRaw::*;
        Ok(match self {
            Local(p) => {
                let p = p.up(helper)?;
                if p.is_const {
                    Path::Const(p)
                } else {
                    Path::Local(p)
                }
            }
            Const(p) => Path::Const(p.up(helper)?),
            Event(p) => Path::Event(p.up(helper)?),
            State(p) => Path::State(p.up(helper)?),
            Meta(p) => Path::Meta(p.up(helper)?),
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum SegmentRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Element {
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        start: Location,
        /// we're forced to make this pub because of lalrpop
        end: Location,
    },
    /// we're forced to make this pub because of lalrpop
    Range {
        /// we're forced to make this pub because of lalrpop
        start_lower: Location,
        /// we're forced to make this pub because of lalrpop
        range_start: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        end_lower: Location,
        /// we're forced to make this pub because of lalrpop
        start_upper: Location,
        /// we're forced to make this pub because of lalrpop
        range_end: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        end_upper: Location,
    },
}

impl<'script> Upable<'script> for SegmentRaw<'script> {
    type Target = Segment<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use SegmentRaw::*;
        const COMPILATION_UNIT_PART: u64 = 0; // FIXME cpp
        Ok(match self {
            Element { expr, start, end } => {
                let expr = expr.up(helper)?;
                let r = expr.extent(&helper.meta);
                match expr {
                    ImutExprInt::Literal(l) => match reduce2(ImutExprInt::Literal(l), &helper)? {
                        Value::String(id) => {
                            let mid =
                                helper.add_meta_w_name(start, end, &id, COMPILATION_UNIT_PART);
                            Segment::Id {
                                key: KnownKey::from(id.clone()),
                                mid,
                            }
                        }
                        other => {
                            if let Some(idx) = other.as_usize() {
                                let mid = helper.add_meta(start, end);
                                Segment::Idx { idx, mid }
                            } else {
                                return Err(ErrorKind::TypeConflict(
                                    r.expand_lines(2),
                                    r,
                                    other.value_type(),
                                    vec![ValueType::I64, ValueType::String],
                                )
                                .into());
                            }
                        }
                    },
                    expr => {
                        let mid = helper.add_meta(start, end);
                        Segment::Element { mid, expr }
                    }
                }
            }
            Range {
                start_lower,
                range_start,
                end_lower,
                start_upper,
                range_end,
                end_upper,
            } => {
                let lower_mid = helper.add_meta(start_lower, end_lower);
                let upper_mid = helper.add_meta(start_upper, end_upper);
                let mid = helper.add_meta(start_lower, end_upper);
                Segment::Range {
                    lower_mid,
                    upper_mid,
                    range_start: Box::new(range_start.up(helper)?),
                    range_end: Box::new(range_end.up(helper)?),
                    mid,
                }
            }
        })
    }
}

impl<'script> SegmentRaw<'script> {
    pub fn from_id(id: IdentRaw<'script>) -> Self {
        SegmentRaw::Element {
            start: id.start,
            end: id.end,
            expr: ImutExprRaw::Literal(LiteralRaw {
                start: id.start,
                end: id.end,
                value: Value::String(id.id),
            }),
        }
    }
    pub fn from_str(id: &'script str, start: Location, end: Location) -> Self {
        SegmentRaw::Element {
            start,
            end,
            expr: ImutExprRaw::Literal(LiteralRaw {
                start,
                end,
                value: Value::from(id),
            }),
        }
    }
    pub fn from_usize(id: usize, start: Location, end: Location) -> Self {
        SegmentRaw::Element {
            start,
            end,
            expr: ImutExprRaw::Literal(LiteralRaw {
                start,
                end,
                value: Value::from(id),
            }),
        }
    }
}

impl<'script> From<ImutExprRaw<'script>> for ExprRaw<'script> {
    fn from(imut: ImutExprRaw<'script>) -> ExprRaw<'script> {
        ExprRaw::Imut(imut)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ConstPathRaw<'script> {
    pub(crate) module: Vec<IdentRaw<'script>>,
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) segments: SegmentsRaw<'script>,
}
impl_expr!(ConstPathRaw);

impl<'script> Upable<'script> for ConstPathRaw<'script> {
    type Target = LocalPath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let segments = self.segments.up(helper)?;
        let mut segments = segments.into_iter();
        if let Some(Segment::Id { mid, .. }) = segments.next() {
            let segments = segments.collect();
            let id = helper.meta.name_dflt(mid).clone();
            let mid = helper.add_meta_w_name(self.start, self.end, id.clone());
            let mut module: Vec<String> = self.module.iter().map(|m| m.id.to_string()).collect();
            module.push(id.to_string());
            if let Some(idx) = helper.is_const(&module) {
                Ok(LocalPath {
                    is_const: true,
                    idx: *idx,
                    mid,
                    segments,
                })
            } else {
                error_oops(
                    &(self.start, self.end),
                    "Only consts can be addressed inside of modules",
                    &helper.meta,
                )
            }
        } else {
            // We should never encounter this
            error_oops(&(self.start, self.end), "Empty local path", &helper.meta)
        }
    }
}
/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct LocalPathRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) segments: SegmentsRaw<'script>,
}
impl_expr!(LocalPathRaw);

impl<'script> Upable<'script> for LocalPathRaw<'script> {
    type Target = LocalPath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        const COMPILATION_UNIT_PART: u64 = 0; // FIXME cpp
        let segments = self.segments.up(helper)?;
        let mut segments = segments.into_iter();
        if let Some(Segment::Id { mid, .. }) = segments.next() {
            let segments = segments.collect();
            let id = helper.meta.name_dflt(mid).clone();
            let mid =
                helper.add_meta_w_name(self.start, self.end, id.clone(), COMPILATION_UNIT_PART);
            if let Some(idx) = helper.is_const(&vec![id.to_string()]) {
                Ok(LocalPath {
                    is_const: true,
                    idx: *idx,
                    mid,
                    segments,
                })
            } else {
                let idx = helper.var_id(&id);
                Ok(LocalPath {
                    is_const: false,
                    idx,
                    mid,
                    segments,
                })
            }
        } else {
            // We should never encounter this
            error_oops(&(self.start, self.end), "Empty local path", &helper.meta)
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MetadataPathRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) segments: SegmentsRaw<'script>,
}
impl<'script> Upable<'script> for MetadataPathRaw<'script> {
    type Target = MetadataPath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let segments = self.segments.up(helper)?;
        Ok(MetadataPath {
            mid: helper.add_meta(self.start, self.end),
            segments,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EventPathRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) segments: SegmentsRaw<'script>,
}
impl<'script> Upable<'script> for EventPathRaw<'script> {
    type Target = EventPath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let segments = self.segments.up(helper)?;
        Ok(EventPath {
            mid: helper.add_meta(self.start, self.end),
            segments,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StatePathRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub segments: SegmentsRaw<'script>,
}
impl<'script> Upable<'script> for StatePathRaw<'script> {
    type Target = StatePath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let segments = self.segments.up(helper)?;
        Ok(StatePath {
            mid: helper.add_meta(self.start, self.end),
            segments,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BinExprRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) kind: BinOpKind,
    pub(crate) lhs: ImutExprRaw<'script>,
    pub(crate) rhs: ImutExprRaw<'script>,
    // query_stream_not_defined(stmt: &Box<S>, inner: &I, name: String)
}

impl<'script> Upable<'script> for BinExprRaw<'script> {
    type Target = BinExpr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(BinExpr {
            mid: helper.add_meta(self.start, self.end),
            kind: self.kind,
            lhs: self.lhs.up(helper)?,
            rhs: self.rhs.up(helper)?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct UnaryExprRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) kind: UnaryOpKind,
    pub(crate) expr: ImutExprRaw<'script>,
}

impl<'script> Upable<'script> for UnaryExprRaw<'script> {
    type Target = UnaryExpr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(UnaryExpr {
            mid: helper.add_meta(self.start, self.end),
            kind: self.kind,
            expr: self.expr.up(helper)?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MatchRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) target: ImutExprRaw<'script>,
    pub(crate) patterns: PredicatesRaw<'script>,
}
impl_expr!(MatchRaw);

impl<'script> Upable<'script> for MatchRaw<'script> {
    type Target = Match<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let patterns: Predicates = self
            .patterns
            .into_iter()
            .map(|v| v.up(helper))
            .collect::<Result<_>>()?;

        let defaults = patterns
            .iter()
            .filter(|p| {
                p.pattern.is_default() || (p.pattern == Pattern::Default && p.guard.is_none())
            })
            .count();
        match defaults {
            0 => helper.warnings.push(Warning{
                outer: Range(self.start, self.end),
                inner: Range(self.start, self.end),
                msg: "This match expression has no default clause, if the other clauses do not cover all possibilities this will lead to events being discarded with runtime errors.".into()
            }),
            x if x > 1 => helper.warnings.push(Warning{
                outer: Range(self.start, self.end),
                inner: Range(self.start, self.end),
                msg: "A match statement with more then one default clause will never reach any but the first default clause.".into()
            }),

            _ => ()
        }

        Ok(Match {
            mid: helper.add_meta(self.start, self.end),
            target: self.target.up(helper)?,
            patterns,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutMatchRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) target: ImutExprRaw<'script>,
    pub(crate) patterns: ImutPredicatesRaw<'script>,
}

impl<'script> Upable<'script> for ImutMatchRaw<'script> {
    type Target = ImutMatch<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let patterns = self.patterns.up(helper)?;
        let defaults = patterns.iter().filter(|p| p.pattern.is_default()).count();
        match defaults {
            0 => helper.warnings.push(Warning{
                outer: Range(self.start, self.end),
                inner: Range(self.start, self.end),
                msg: "This match expression has no default clause, if the other clauses do not cover all possibilities this will lead to events being discarded with runtime errors.".into()
            }),
            x if x > 1 => helper.warnings.push(Warning{
                outer: Range(self.start, self.end),
                inner: Range(self.start, self.end),
                msg: "A match statement with more then one default clause will never reach any but the first default clause.".into()
            }),

            _ => ()
        }
        let was_leaf = helper.possible_leaf;
        helper.possible_leaf = false;
        let r = Ok(ImutMatch {
            mid: helper.add_meta(self.start, self.end),
            target: self.target.up(helper)?,
            patterns,
        });
        helper.possible_leaf = was_leaf;
        r
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct InvokeRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) module: Vec<String>,
    pub(crate) fun: String,
    pub(crate) args: ImutExprsRaw<'script>,
}
impl_expr!(InvokeRaw);

impl<'script> Upable<'script> for InvokeRaw<'script> {
    type Target = Invoke<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        if self.module.get(0) == Some(&String::from("core")) && self.module.len() == 2 {
            // we know a second module exists
            let module = self.module.get(1).cloned().unwrap_or_default();

            let invocable = helper
                .reg
                .find(&module, &self.fun)
                .map_err(|e| e.into_err(&self, &self, Some(&helper.reg), &helper.meta))?;
            let args = self.args.up(helper)?.into_iter().map(ImutExpr).collect();
            let mf = format!("{}::{}", self.module.join("::"), self.fun);
            Ok(Invoke {
                mid: helper.add_meta_w_name(self.start, self.end, mf),
                module: self.module,
                fun: self.fun,
                invocable: Invocable::Intrinsic(invocable.clone()),
                args,
            })
        } else {
            let mut module = self.module.clone();
            module.push(self.fun.clone());
            if let Some(f) = helper.functions.get(&module) {
                if let Some(f) = helper.func_vec.get(*f) {
                    let invocable = Invocable::Tremor(f.clone());
                    let args = self.args.up(helper)?.into_iter().map(ImutExpr).collect();
                    let mf = format!("{}::{}", self.module.join("::"), self.fun);
                    Ok(Invoke {
                        mid: helper.add_meta_w_name(self.start, self.end, mf),
                        module: self.module,
                        fun: self.fun,
                        invocable,
                        args,
                    })
                } else {
                    let inner: Range = (self.start, self.end).into();
                    let outer: Range = inner.expand_lines(3);
                    Err(
                        ErrorKind::MissingFunction(outer, inner, self.module, self.fun, None)
                            .into(),
                    )
                }
            } else {
                let inner: Range = (self.start, self.end).into();
                let outer: Range = inner.expand_lines(3);
                Err(ErrorKind::MissingFunction(outer, inner, self.module, self.fun, None).into())
            }
        }
    }
}

impl<'script> InvokeRaw<'script> {
    fn is_aggregate<'registry>(&self, helper: &mut Helper<'script, 'registry>) -> bool {
        if self.module.get(0) == Some(&String::from("aggr")) && self.module.len() == 2 {
            let module = self.module.get(1).cloned().unwrap_or_default();
            helper.aggr_reg.find(&module, &self.fun).is_ok()
        } else {
            false
        }
    }

    fn into_aggregate(self) -> InvokeAggrRaw<'script> {
        let module = self.module.get(1).cloned().unwrap_or_default();
        InvokeAggrRaw {
            start: self.start,
            end: self.end,
            module: module,
            fun: self.fun,
            args: self.args,
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct InvokeAggrRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) module: String,
    pub(crate) fun: String,
    pub(crate) args: ImutExprsRaw<'script>,
}
impl_expr!(InvokeAggrRaw);

impl<'script> Upable<'script> for InvokeAggrRaw<'script> {
    type Target = InvokeAggr;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        if helper.is_in_aggr {
            return Err(ErrorKind::AggrInAggr(
                self.extent(&helper.meta),
                self.extent(&helper.meta).expand_lines(2),
            )
            .into());
        };
        helper.is_in_aggr = true;
        let invocable = helper
            .aggr_reg
            .find(&self.module, &self.fun)
            .map_err(|e| e.into_err(&self, &self, Some(&helper.reg), &helper.meta))?
            .clone();
        if !invocable.valid_arity(self.args.len()) {
            return Err(ErrorKind::BadArity(
                self.extent(&helper.meta),
                self.extent(&helper.meta).expand_lines(2),
                self.module.clone(),
                self.fun.clone(),
                invocable.arity(),
                self.args.len(),
            )
            .into());
        }
        if let Some(warning) = invocable.warning() {
            helper.warnings.push(Warning {
                inner: self.extent(&helper.meta),
                outer: self.extent(&helper.meta),
                msg: warning,
            });
        }
        let aggr_id = helper.aggregates.len();
        let args = self.args.up(helper)?.into_iter().map(ImutExpr).collect();
        let mf = format!("{}::{}", self.module, self.fun);
        let invoke_meta_id = helper.add_meta_w_name(self.start, self.end, &mf);

        helper.aggregates.push(InvokeAggrFn {
            mid: invoke_meta_id,
            invocable,
            args,
            module: self.module.clone(),
            fun: self.fun.clone(),
        });
        helper.is_in_aggr = false;
        let aggr_meta_id = helper.add_meta_w_name(self.start, self.end, mf);
        Ok(InvokeAggr {
            mid: aggr_meta_id,
            module: self.module,
            fun: self.fun,
            aggr_id,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TestExprRaw {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) test: String,
}

impl<'script> Upable<'script> for TestExprRaw {
    type Target = TestExpr;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let mid = helper.add_meta(self.start, self.end);
        match Extractor::new(&self.id, &self.test) {
            Ok(ex) => Ok(TestExpr {
                id: self.id,
                test: self.test,
                extractor: ex,
                mid,
            }),
            Err(e) => Err(ErrorKind::InvalidExtractor(
                self.extent(&helper.meta).expand_lines(2),
                self.extent(&helper.meta),
                self.id,
                self.test,
                e.msg,
            )
            .into()),
        }
    }
}

pub type ExprsRaw<'script> = Vec<ExprRaw<'script>>;
pub type ImutExprsRaw<'script> = Vec<ImutExprRaw<'script>>;
pub type FieldsRaw<'script> = Vec<FieldRaw<'script>>;
pub type SegmentsRaw<'script> = Vec<SegmentRaw<'script>>;
pub type PatternFieldsRaw<'script> = Vec<PredicatePatternRaw<'script>>;
pub type PredicatesRaw<'script> = Vec<PredicateClauseRaw<'script>>;
pub type ImutPredicatesRaw<'script> = Vec<ImutPredicateClauseRaw<'script>>;
pub type PatchOperationsRaw<'script> = Vec<PatchOperationRaw<'script>>;
pub type ComprehensionCasesRaw<'script> = Vec<ComprehensionCaseRaw<'script>>;
pub type ImutComprehensionCasesRaw<'script> = Vec<ImutComprehensionCaseRaw<'script>>;
pub type ArrayPredicatePatternsRaw<'script> = Vec<ArrayPredicatePatternRaw<'script>>;
pub type WithExprsRaw<'script> = Vec<(IdentRaw<'script>, ImutExprRaw<'script>)>;
