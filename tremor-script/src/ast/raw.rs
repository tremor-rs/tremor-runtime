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

use super::upable::Upable;
use super::*;
use crate::errors::*;
use crate::impl_expr;
use crate::interpreter::{exec_binary, exec_unary};
use crate::pos::{Location, Range};
use crate::registry::{Aggr as AggrRegistry, Registry};
use crate::tilde::Extractor;
use crate::EventContext;
pub use base_expr::BaseExpr;
use halfbrown::HashMap;
pub use query::*;
use serde::Serialize;
use simd_json::value::{borrowed, Value as ValueTrait};
use simd_json::{BorrowedValue as Value, KnownKey};
use std::borrow::{Borrow, Cow};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ScriptRaw<'script> {
    pub exprs: ExprsRaw<'script>,
}

impl<'script> ScriptRaw<'script> {
    pub fn up_script<'registry>(
        self,
        reg: &'registry Registry,
        aggr_reg: &'registry AggrRegistry,
    ) -> Result<(Script<'script>, Vec<Warning>)> {
        let mut helper = Helper::new(reg, aggr_reg);
        let mut consts: Vec<Value> = vec![Value::null(), Value::null(), Value::null()];
        helper.consts.insert("window".to_owned(), WINDOW_CONST_ID);
        helper.consts.insert("group".to_owned(), GROUP_CONST_ID);
        helper.consts.insert("args".to_owned(), ARGS_CONST_ID);

        let mut exprs = vec![];
        let len = self.exprs.len();
        for (i, e) in self.exprs.into_iter().enumerate() {
            match e {
                ExprRaw::Const {
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
                            is_const: true,
                            idx: consts.len(),
                            mid: helper.add_meta_w_name(start, end, name),
                        }))
                    }

                    consts.push(reduce2(expr, &helper)?);
                }
                #[allow(unreachable_code, unused_variables)]
                #[cfg_attr(tarpaulin, skip)]
                ExprRaw::FnDecl(_f) => {
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
                            mid: p.mid(),
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
                node_meta: helper.meta,
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
                ExprRaw::Const {
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
                ExprRaw::FnDecl(f) => {
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
            mid: helper.add_meta(self.start, self.end),
            id: self.id,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FieldRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub name: StringLitRaw<'script>,
    pub value: ImutExprRaw<'script>,
}

impl<'script> Upable<'script> for FieldRaw<'script> {
    type Target = Field<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Field {
            mid: helper.add_meta(self.start, self.end),
            name: ImutExprRaw::String(self.name).up(helper)?,
            value: self.value.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RecordRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub fields: FieldsRaw<'script>,
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ListRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub exprs: ImutExprsRaw<'script>,
}
impl_expr!(ListRaw);

impl<'script> Upable<'script> for ListRaw<'script> {
    type Target = List<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(List {
            mid: helper.add_meta(self.start, self.end),
            exprs: self.exprs.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct LiteralRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub value: Value<'script>,
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StringLitRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub string: Cow<'script, str>,
    pub exprs: ImutExprsRaw<'script>,
}

pub struct StrLitElements<'script>(pub Vec<Cow<'script, str>>, pub ImutExprsRaw<'script>);

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

pub fn reduce2<'script>(expr: ImutExpr<'script>, helper: &Helper) -> Result<Value<'script>> {
    match expr {
        ImutExpr::Literal(Literal { value: v, .. }) => Ok(v),
        other => Err(ErrorKind::NotConstant(
            other.extent(&helper.meta),
            other.extent(&helper.meta).expand_lines(2),
        )
        .into()),
    }
}

// This is compile time only we don't care
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ExprRaw<'script> {
    Const {
        name: Cow<'script, str>,
        expr: ImutExprRaw<'script>,
        start: Location,
        end: Location,
    },
    MatchExpr(Box<MatchRaw<'script>>),
    Assign(Box<AssignRaw<'script>>),
    Comprehension(Box<ComprehensionRaw<'script>>),
    Drop {
        start: Location,
        end: Location,
    },
    Emit(Box<EmitExprRaw<'script>>),
    FnDecl(FnDeclRaw<'script>),
    Imut(ImutExprRaw<'script>), //Test(TestExprRaw)
}

impl<'script> Upable<'script> for ExprRaw<'script> {
    type Target = Expr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(match self {
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
                    Expr::Imut(ImutExpr::Merge(m)) => {
                        if path_eq(&path, &m.target) {
                            Expr::MergeInPlace(Box::new(*m))
                        } else {
                            Expr::Assign {
                                mid,
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
                                mid,
                                path,
                                expr: Box::new(ImutExpr::Patch(m).into()),
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
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FnDeclRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub name: IdentRaw<'script>,
    pub args: Vec<IdentRaw<'script>>,
    pub body: ExprsRaw<'script>,
}
impl_expr!(FnDeclRaw);

impl<'script> Upable<'script> for FnDeclRaw<'script> {
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
            mid: helper.add_meta(self.start, self.end),
            name: self.name.up(helper)?,
            args: self.args.up(helper)?,
            body,
            locals: locals.len(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ImutExprRaw<'script> {
    Record(Box<RecordRaw<'script>>),
    List(Box<ListRaw<'script>>),
    Patch(Box<PatchRaw<'script>>),
    Merge(Box<MergeRaw<'script>>),
    Match(Box<ImutMatchRaw<'script>>),
    Comprehension(Box<ImutComprehensionRaw<'script>>),
    Path(PathRaw<'script>),
    Binary(Box<BinExprRaw<'script>>),
    Unary(Box<UnaryExprRaw<'script>>),
    Literal(LiteralRaw<'script>),
    Invoke(InvokeRaw<'script>),
    InvokeAggr(InvokeAggrRaw<'script>),
    Present {
        path: PathRaw<'script>,
        start: Location,
        end: Location,
    },
    String(StringLitRaw<'script>),
}

impl<'script> Upable<'script> for ImutExprRaw<'script> {
    type Target = ImutExpr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(match self {
            ImutExprRaw::Binary(b) => match b.up(helper)? {
                b1 @ BinExpr {
                    lhs: ImutExpr::Literal(_),
                    rhs: ImutExpr::Literal(_),
                    ..
                } => {
                    let lhs = reduce2(b1.lhs.clone(), &helper)?;
                    let rhs = reduce2(b1.rhs.clone(), &helper)?;
                    // TODO remove duplicate params?
                    let value =
                        exec_binary(&b1, &b1, &helper.meta, b1.kind, &lhs, &rhs)?.into_owned();
                    let lit = Literal { mid: b1.mid, value };
                    ImutExpr::Literal(lit)
                }
                b1 => ImutExpr::Binary(Box::new(b1)),
            },
            ImutExprRaw::Unary(u) => match u.up(helper)? {
                u1 @ UnaryExpr {
                    expr: ImutExpr::Literal(_),
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
                    ImutExpr::Literal(lit)
                }
                u1 => ImutExpr::Unary(Box::new(u1)),
            },
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
                        module: "string".into(),
                        fun: "format".into(),
                        args,
                    })
                    .up(helper)?
                }
            }
            ImutExprRaw::Record(r) => {
                let r = r.up(helper)?;
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
                    ImutExpr::Literal(Literal {
                        mid: r.mid,
                        value: Value::from(obj?),
                    })
                } else {
                    ImutExpr::Record(r)
                }
            }
            ImutExprRaw::List(l) => {
                let l = l.up(helper)?;
                if l.exprs.iter().all(is_lit) {
                    let elements: Result<Vec<Value>> =
                        l.exprs.into_iter().map(|v| reduce2(v, &helper)).collect();
                    ImutExpr::Literal(Literal {
                        mid: l.mid,
                        value: Value::Array(elements?),
                    })
                } else {
                    ImutExpr::List(l)
                }
            }
            ImutExprRaw::Patch(p) => ImutExpr::Patch(Box::new(p.up(helper)?)),
            ImutExprRaw::Merge(m) => ImutExpr::Merge(Box::new(m.up(helper)?)),
            ImutExprRaw::Present { path, start, end } => ImutExpr::Present {
                path: path.up(helper)?,
                mid: helper.add_meta(start, end),
            },
            ImutExprRaw::Path(p) => match p.up(helper)? {
                Path::Local(LocalPath {
                    is_const,
                    mid,
                    idx,
                    ref segments,
                }) if segments.is_empty() => ImutExpr::Local { mid, idx, is_const },
                p => ImutExpr::Path(p),
            },
            ImutExprRaw::Literal(l) => ImutExpr::Literal(l.up(helper)?),
            ImutExprRaw::Invoke(i) => {
                if i.is_aggregate(helper) {
                    ImutExpr::InvokeAggr(i.into_aggregate().up(helper)?)
                } else {
                    let ex = i.extent(&helper.meta);
                    let i = i.up(helper)?;
                    if i.invocable.is_const() && i.args.iter().all(|f| is_lit(&f)) {
                        let args: Result<Vec<Value<'script>>> =
                            i.args.into_iter().map(|v| reduce2(v, &helper)).collect();
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
                            .map_err(|e| e.into_err(&ex, &ex, Some(&helper.reg), &helper.meta))?;
                        ImutExpr::Literal(Literal {
                            value: v,
                            mid: i.mid,
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
            ImutExprRaw::InvokeAggr(i) => {
                let i = i.up(helper)?;
                ImutExpr::InvokeAggr(i)
            }
            ImutExprRaw::Match(m) => ImutExpr::Match(Box::new(m.up(helper)?)),
            ImutExprRaw::Comprehension(c) => ImutExpr::Comprehension(Box::new(c.up(helper)?)),
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
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AssignRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub path: PathRaw<'script>,
    pub expr: ExprRaw<'script>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PredicateClauseRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub pattern: PatternRaw<'script>,
    pub guard: Option<ImutExprRaw<'script>>,
    pub exprs: ExprsRaw<'script>,
}

impl<'script> Upable<'script> for PredicateClauseRaw<'script> {
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
            mid: helper.add_meta(self.start, self.end),
            pattern,
            guard,
            exprs,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutPredicateClauseRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub pattern: PatternRaw<'script>,
    pub guard: Option<ImutExprRaw<'script>>,
    pub exprs: ImutExprsRaw<'script>,
}

impl<'script> Upable<'script> for ImutPredicateClauseRaw<'script> {
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
            mid: helper.add_meta(self.start, self.end),
            pattern,
            guard,
            exprs,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PatchRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExprRaw<'script>,
    pub operations: PatchOperationsRaw<'script>,
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PatchOperationRaw<'script> {
    Insert {
        ident: ImutExprRaw<'script>,
        expr: ImutExprRaw<'script>,
    },
    Upsert {
        ident: ImutExprRaw<'script>,
        expr: ImutExprRaw<'script>,
    },
    Update {
        ident: ImutExprRaw<'script>,
        expr: ImutExprRaw<'script>,
    },
    Erase {
        ident: ImutExprRaw<'script>,
    },
    Copy {
        from: ImutExprRaw<'script>,
        to: ImutExprRaw<'script>,
    },
    Move {
        from: ImutExprRaw<'script>,
        to: ImutExprRaw<'script>,
    },
    Merge {
        ident: ImutExprRaw<'script>,
        expr: ImutExprRaw<'script>,
    },
    TupleMerge {
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ComprehensionCaseRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub key_name: Cow<'script, str>,
    pub value_name: Cow<'script, str>,
    pub guard: Option<ImutExprRaw<'script>>,
    pub exprs: ExprsRaw<'script>,
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
    pub start: Location,
    pub end: Location,
    pub key_name: Cow<'script, str>,
    pub value_name: Cow<'script, str>,
    pub guard: Option<ImutExprRaw<'script>>,
    pub exprs: ImutExprsRaw<'script>,
}

impl<'script> Upable<'script> for ImutComprehensionCaseRaw<'script> {
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
            mid: helper.add_meta(self.start, self.end),
            key_name: self.key_name,
            value_name: self.value_name,
            guard,
            exprs,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PatternRaw<'script> {
    //Predicate(PredicatePatternRaw<'script>),
    Record(RecordPatternRaw<'script>),
    Array(ArrayPatternRaw<'script>),
    Expr(ImutExprRaw<'script>),
    Assign(AssignPatternRaw<'script>),
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
            Expr(expr) => Pattern::Expr(expr.up(helper)?),
            Assign(ap) => Pattern::Assign(ap.up(helper)?),
            Default => Pattern::Default,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PredicatePatternRaw<'script> {
    TildeEq {
        assign: Cow<'script, str>,
        lhs: Cow<'script, str>,
        test: TestExprRaw,
    },
    Eq {
        lhs: Cow<'script, str>,
        rhs: ImutExprRaw<'script>,
        not: bool,
    },
    RecordPatternEq {
        lhs: Cow<'script, str>,
        pattern: RecordPatternRaw<'script>,
    },
    ArrayPatternEq {
        lhs: Cow<'script, str>,
        pattern: ArrayPatternRaw<'script>,
    },
    FieldPresent {
        lhs: Cow<'script, str>,
    },
    FieldAbsent {
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
pub struct RecordPatternRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub fields: PatternFieldsRaw<'script>,
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
                    msg: format!("The field {} is checked with both present and another extractor, this is redundant as extractors imply presence. It may also oberwrite the result of th extractor.", present),
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ArrayPredicatePatternRaw<'script> {
    Expr(ImutExprRaw<'script>),
    Tilde(TestExprRaw),
    Record(RecordPatternRaw<'script>),
    //Array(ArrayPattern),
}
impl<'script> Upable<'script> for ArrayPredicatePatternRaw<'script> {
    type Target = ArrayPredicatePattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use ArrayPredicatePatternRaw::*;
        Ok(match self {
            Expr(expr) => ArrayPredicatePattern::Expr(expr.up(helper)?),
            Tilde(te) => ArrayPredicatePattern::Tilde(te.up(helper)?),
            Record(rp) => ArrayPredicatePattern::Record(rp.up(helper)?),
            //Array(ap) => ArrayPredicatePattern::Array(ap),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ArrayPatternRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub exprs: ArrayPredicatePatternsRaw<'script>,
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AssignPatternRaw<'script> {
    pub id: Cow<'script, str>,
    pub pattern: Box<PatternRaw<'script>>,
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PathRaw<'script> {
    Local(LocalPathRaw<'script>),
    Event(EventPathRaw<'script>),
    Meta(MetadataPathRaw<'script>),
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
            Event(p) => Path::Event(p.up(helper)?),
            Meta(p) => Path::Meta(p.up(helper)?),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum SegmentRaw<'script> {
    Element {
        expr: ImutExprRaw<'script>,
        start: Location,
        end: Location,
    },
    Range {
        start_lower: Location,
        range_start: ImutExprRaw<'script>,
        end_lower: Location,
        start_upper: Location,
        range_end: ImutExprRaw<'script>,
        end_upper: Location,
    },
}

impl<'script> Upable<'script> for SegmentRaw<'script> {
    type Target = Segment<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use SegmentRaw::*;
        Ok(match self {
            Element { expr, start, end } => {
                let expr = expr.up(helper)?;
                let r = expr.extent(&helper.meta);
                match expr {
                    ImutExpr::Literal(l) => match reduce2(ImutExpr::Literal(l), &helper)? {
                        Value::String(id) => {
                            let mid = helper.add_meta_w_name(start, end, id.clone());
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
                value: Value::String(id.into()),
            }),
        }
    }
}

impl<'script> From<ImutExprRaw<'script>> for ExprRaw<'script> {
    fn from(imut: ImutExprRaw<'script>) -> ExprRaw<'script> {
        ExprRaw::Imut(imut)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct LocalPathRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub segments: SegmentsRaw<'script>,
}
impl_expr!(LocalPathRaw);

impl<'script> Upable<'script> for LocalPathRaw<'script> {
    type Target = LocalPath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let segments = self.segments.up(helper)?;
        let mut segments = segments.into_iter();
        if let Some(Segment::Id { mid, .. }) = segments.next() {
            let segments = segments.collect();
            let id = helper.meta.name_dflt(mid).clone();
            let mid = helper.add_meta_w_name(self.start, self.end, id.clone());
            if let Some(idx) = helper.is_const(&id) {
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MetadataPathRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub segments: SegmentsRaw<'script>,
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EventPathRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub segments: SegmentsRaw<'script>,
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BinExprRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub kind: BinOpKind,
    pub lhs: ImutExprRaw<'script>,
    pub rhs: ImutExprRaw<'script>,
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct UnaryExprRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub kind: UnaryOpKind,
    pub expr: ImutExprRaw<'script>,
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MatchRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExprRaw<'script>,
    pub patterns: PredicatesRaw<'script>,
}

impl<'script> Upable<'script> for MatchRaw<'script> {
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
            mid: helper.add_meta(self.start, self.end),
            target: self.target.up(helper)?,
            patterns,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutMatchRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExprRaw<'script>,
    pub patterns: ImutPredicatesRaw<'script>,
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
            mid: helper.add_meta(self.start, self.end),
            target: self.target.up(helper)?,
            patterns,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct InvokeRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub module: String,
    pub fun: String,
    pub args: ImutExprsRaw<'script>,
}
impl_expr!(InvokeRaw);

impl<'script> Upable<'script> for InvokeRaw<'script> {
    type Target = Invoke<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let invocable = helper
            .reg
            .find(&self.module, &self.fun)
            .map_err(|e| e.into_err(&self, &self, Some(&helper.reg), &helper.meta))?;

        let args = self.args.up(helper)?;

        Ok(Invoke {
            mid: helper.add_meta(self.start, self.end),
            module: self.module,
            fun: self.fun,
            invocable: invocable.clone(),
            args,
        })
    }
}

impl<'script> InvokeRaw<'script> {
    fn is_aggregate<'registry>(&self, helper: &mut Helper<'script, 'registry>) -> bool {
        helper.aggr_reg.find(&self.module, &self.fun).is_ok()
    }

    fn into_aggregate(self) -> InvokeAggrRaw<'script> {
        InvokeAggrRaw {
            start: self.start,
            end: self.end,
            module: self.module,
            fun: self.fun,
            args: self.args,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct InvokeAggrRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub module: String,
    pub fun: String,
    pub args: ImutExprsRaw<'script>,
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
        let args = self.args.up(helper)?;
        let mid = helper.add_meta(self.start, self.end);
        helper.aggregates.push(InvokeAggrFn {
            mid,
            invocable,
            args,
            module: self.module.clone(),
            fun: self.fun.clone(),
        });
        helper.is_in_aggr = false;
        let mid = helper.add_meta(self.start, self.end);
        Ok(InvokeAggr {
            mid,
            module: self.module,
            fun: self.fun,
            aggr_id,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TestExprRaw {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub test: String,
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
