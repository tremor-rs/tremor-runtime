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
use crate::interpreter::Interpreter;
use crate::interpreter::ValueStack;
use crate::pos::{Location, Range};
use crate::registry::{Context, Registry, TremorFn};
use crate::tilde::Extractor;
use simd_json::value::borrowed;
use simd_json::value::owned::Map as OwnedMap;

use simd_json::OwnedValue;
use std::fmt;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Script1 {
    pub exprs: Exprs1,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Warning {
    pub outer: Range,
    pub inner: Range,
    pub msg: String,
}

pub struct Helper<'h, Ctx>
where
    Ctx: Context + 'static,
{
    reg: &'h Registry<Ctx>,
    warnings: Vec<Warning>,
}

impl<'h, Ctx> Helper<'h, Ctx>
where
    Ctx: Context + 'static,
{
    pub fn new(reg: &'h Registry<Ctx>) -> Self {
        Helper {
            reg,
            warnings: Vec::new(),
        }
    }
    pub fn into_warnings(self) -> Vec<Warning> {
        self.warnings
    }
}
impl Script1 {
    pub fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<Script<Ctx>> {
        use std::borrow::Borrow;
        let exprs: Result<Exprs<Ctx>> = self.exprs.into_iter().map(|p| p.up(helper)).collect();
        let mut exprs = exprs?;
        // We make sure the if we return `event` we turn it into `emit event`
        // While this is not required logically it allows us to
        // take advantage of the `emit event` optiisation
        if let Some(e) = exprs.pop() {
            match e.borrow() {
                Expr::Emit(_) => exprs.push(e),
                Expr::Path(Path::Event(EventPath { segments, .. })) if segments.is_empty() => {
                    let expr = EmitExpr {
                        start: e.s(),
                        end: e.e(),
                        expr: e,
                        port: None,
                    };
                    exprs.push(Expr::Emit(Box::new(expr)))
                }
                _ => exprs.push(e),
            }
        } else {
            return Err(ErrorKind::EmptyScript.into());
        }

        Ok(Script { exprs })
    }
}

#[derive(Debug, PartialEq, Serialize)]
pub struct Script<Ctx: Context + 'static> {
    pub exprs: Exprs<Ctx>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Ident {
    pub id: String,
}

pub trait BaseExpr {
    fn s(&self) -> Location;
    fn e(&self) -> Location;
    fn extent(&self) -> Range {
        Range(self.s(), self.e())
    }
}

macro_rules! impl_expr {
    ($name:ident) => {
        impl<Ctx: Context + 'static> BaseExpr for $name<Ctx> {
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
        impl BaseExpr for $name {
            fn s(&self) -> Location {
                self.start
            }

            fn e(&self) -> Location {
                self.end
            }
        }
    };
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct TodoExpr {
    pub start: Location,
    pub end: Location,
}
impl_expr1!(TodoExpr);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Field1 {
    pub start: Location,
    pub end: Location,
    pub name: String,
    pub value: Box<Expr1>,
}

impl Field1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<Field<Ctx>> {
        Ok(Field {
            start: self.start,
            end: self.end,
            name: self.name,
            value: self.value.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Field<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub name: String,
    pub value: Expr<Ctx>,
}
impl_expr!(Field);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record1 {
    pub start: Location,
    pub end: Location,
    pub fields: Fields1,
}
impl_expr1!(Record1);

impl Record1 {
    pub fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<Record<Ctx>> {
        let fields: Result<Fields<Ctx>> = self.fields.into_iter().map(|p| p.up(helper)).collect();
        Ok(Record {
            start: self.start,
            end: self.end,
            fields: fields?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Record<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub fields: Fields<Ctx>,
}
impl_expr!(Record);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Literal1 {
    pub start: Location,
    pub end: Location,
    pub value: LiteralValue1,
}

impl Literal1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<Literal<Ctx>> {
        Ok(Literal {
            start: self.start,
            end: self.end,
            value: self.value.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Literal<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub value: LiteralValue<Ctx>,
}

impl_expr!(Literal);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum LiteralValue1 {
    Native(OwnedValue),
    List(Exprs1),
}

impl LiteralValue1 {
    pub fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<LiteralValue<Ctx>> {
        use LiteralValue1::*;
        Ok(match self {
            Native(v) => LiteralValue::Native(v),
            List(exprs) => {
                let exprs: Result<Exprs<Ctx>> = exprs.into_iter().map(|p| p.up(helper)).collect();
                let exprs = exprs?;
                if exprs.iter().all(is_lit) {
                    let elements: Result<Vec<OwnedValue>> = exprs
                        .iter()
                        .map(|expr| {
                            let c = Ctx::default();;
                            let mut event = borrowed::Value::Object(borrowed::Map::new());
                            let mut meta = event.clone();
                            let mut local = event.clone();
                            let stack: ValueStack = ValueStack::default();
                            expr.run(&c, &mut event, &mut meta, &mut local, &stack)
                                .and_then(|v| v.into_value(&expr, &expr))
                                .map(|v| OwnedValue::from(v.clone()))
                        })
                        .collect();
                    LiteralValue::Native(OwnedValue::Array(elements?))
                } else {
                    LiteralValue::List(exprs)
                }
            }
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum LiteralValue<Ctx: Context + 'static> {
    Native(simd_json::OwnedValue),
    List(Exprs<Ctx>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expr1 {
    RecordExpr(Record1),
    MatchExpr(Box<Match1>),
    PatchExpr(Box<Patch1>),
    MergeExpr(Box<Merge1>),
    Path(Path1),
    Literal(Literal1),
    Binary(BinExpr1),
    Unary(UnaryExpr1),
    Invoke(Invoke1),
    //Let(Let<Ctx>),
    Assign(Assign1),
    Comprehension(Comprehension1),
    Drop {
        start: Location,
        end: Location,
    },
    Present {
        path: Path1,
        start: Location,
        end: Location,
    },
    Emit(EmitExpr1),
    Test(TestExpr1),
}

impl Expr1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<Expr<Ctx>> {
        Ok(match self {
            Expr1::RecordExpr(r) => {
                let r1 = r.up(helper)?;
                if r1.fields.iter().all(|e| is_lit(&e.value)) {
                    let obj: Result<OwnedMap> = r1
                        .fields
                        .iter()
                        .map(|f| {
                            let c = Ctx::default();;
                            let mut event = borrowed::Value::Object(borrowed::Map::new());
                            let mut meta = event.clone();
                            let mut local = event.clone();
                            let stack: ValueStack = ValueStack::default();
                            f.value
                                .run(&c, &mut event, &mut meta, &mut local, &stack)
                                .and_then(|v| v.into_value(&Expr::dummy_from(&r1), &f.value))
                                .map(|v| (f.name.clone(), OwnedValue::from(v.clone())))
                        })
                        .collect();
                    Expr::Literal(Literal {
                        start: r1.start,
                        end: r1.end,
                        value: LiteralValue::Native(OwnedValue::Object(obj?)),
                    })
                } else {
                    Expr::RecordExpr(r1)
                }
            }
            Expr1::MatchExpr(m) => Expr::MatchExpr(Box::new(m.up(helper)?)),
            Expr1::PatchExpr(p) => Expr::PatchExpr(Box::new(p.up(helper)?)),
            Expr1::MergeExpr(m) => Expr::MergeExpr(Box::new(m.up(helper)?)),
            Expr1::Present { path, start, end } => Expr::Present {
                path: path.up(helper)?,
                start,
                end,
            },
            Expr1::Path(p) => Expr::Path(p.up(helper)?),
            Expr1::Literal(l) => Expr::Literal(l.up(helper)?),
            Expr1::Binary(b) => match b.up(helper)? {
                b1 @ BinExpr {
                    lhs: Expr::Literal(_),
                    rhs: Expr::Literal(_),
                    ..
                } => {
                    let c = Ctx::default();;
                    let start = b1.start;
                    let end = b1.end;
                    let expr = Expr::Binary(Box::new(b1));
                    let mut event = borrowed::Value::Object(borrowed::Map::new());
                    let mut meta = event.clone();
                    let mut local = event.clone();
                    let stack: ValueStack = ValueStack::default();
                    let value = expr
                        .run(&c, &mut event, &mut meta, &mut local, &stack)?
                        .into_value(&expr, &expr)?;
                    let lit = Literal {
                        start,
                        end,
                        value: LiteralValue::Native(value.clone().into()),
                    };
                    Expr::Literal(lit)
                }
                b1 @ BinExpr {
                    lhs: Expr::Drop { .. },
                    ..
                } => {
                    let r: Range = Expr::Binary(Box::new(b1)).into();
                    return Err(ErrorKind::BinaryDrop(r.expand_lines(2), r).into());
                }
                b1 @ BinExpr {
                    lhs: Expr::Emit(_), ..
                } => {
                    let r: Range = Expr::Binary(Box::new(b1)).into();
                    return Err(ErrorKind::BinaryEmit(r.expand_lines(2), r).into());
                }
                b1 => Expr::Binary(Box::new(b1)),
            },
            Expr1::Unary(u) => match u.up(helper)? {
                u1 @ UnaryExpr {
                    expr: Expr::Literal(_),
                    ..
                } => {
                    let c = Ctx::default();;
                    let start = u1.start;
                    let end = u1.end;
                    let expr = Expr::Unary(Box::new(u1));
                    let mut event = borrowed::Value::Object(borrowed::Map::new());
                    let mut meta = event.clone();
                    let mut local = event.clone();
                    let stack: ValueStack = ValueStack::default();
                    let value = expr
                        .run(&c, &mut event, &mut meta, &mut local, &stack)?
                        .into_value(&expr, &expr)?;
                    let lit = Literal {
                        start,
                        end,
                        value: LiteralValue::Native(value.clone().into()),
                    };
                    Expr::Literal(lit)
                }
                u1 => Expr::Unary(Box::new(u1)),
            },
            Expr1::Invoke(i) => Expr::Invoke(i.up(helper)?),
            //Expr1::Let(l) => Expr::Let(l),
            Expr1::Assign(a) => Expr::Assign(a.up(helper)?),
            Expr1::Comprehension(c) => Expr::Comprehension(c.up(helper)?),
            Expr1::Drop { start, end } => Expr::Drop { start, end },
            Expr1::Emit(e) => Expr::Emit(Box::new(e.up(helper)?)),
            Expr1::Test(t) => Expr::Test(t.up(helper)?),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Expr<Ctx: Context + 'static> {
    RecordExpr(Record<Ctx>),
    MatchExpr(Box<Match<Ctx>>),
    PatchExpr(Box<Patch<Ctx>>),
    MergeExpr(Box<Merge<Ctx>>),
    Path(Path<Ctx>),
    Literal(Literal<Ctx>),
    Binary(Box<BinExpr<Ctx>>),
    Unary(Box<UnaryExpr<Ctx>>),
    Present {
        path: Path<Ctx>,
        start: Location,
        end: Location,
    },
    Invoke(Invoke<Ctx>),
    Let(Let<Ctx>),
    Assign(Assign<Ctx>),
    Comprehension(Comprehension<Ctx>),
    Drop {
        start: Location,
        end: Location,
    },
    Emit(Box<EmitExpr<Ctx>>),
    Test(TestExpr),
}

fn is_lit<Ctx: Context + 'static>(e: &Expr<Ctx>) -> bool {
    match e {
        Expr::Literal(_) => true,
        _ => false,
    }
}

impl<Ctx: Context + 'static> Expr<Ctx> {
    pub fn dummy(start: Location, end: Location) -> Self {
        Expr::Literal(Literal {
            value: LiteralValue::Native("dummy".into()),
            start,
            end,
        })
    }

    pub fn dummy_extend(start: Location, end: Location) -> Self {
        Expr::Literal(Literal {
            value: LiteralValue::Native("dummy".into()),
            start: start.move_up_lines(2),
            end: end.move_down_lines(2),
        })
    }

    pub fn dummy_from<T: BaseExpr>(from: &T) -> Self {
        Expr::dummy(from.s(), from.e())
    }
    pub fn dummy_extend_from<T: BaseExpr>(from: &T) -> Self {
        Expr::dummy_extend(from.s(), from.e())
    }
}

impl<Ctx: Context + 'static> BaseExpr for Expr<Ctx> {
    fn s(&self) -> Location {
        match self {
            Expr::RecordExpr(e) => e.s(),
            Expr::MatchExpr(e) => e.s(),
            Expr::PatchExpr(e) => e.s(),
            Expr::MergeExpr(e) => e.s(),
            Expr::Path(e) => e.s(),
            Expr::Literal(e) => e.s(),
            Expr::Binary(e) => e.s(),
            Expr::Unary(e) => e.s(),
            Expr::Invoke(e) => e.s(),
            Expr::Let(e) => e.s(),
            Expr::Assign(e) => e.s(),
            Expr::Comprehension(e) => e.s(),
            Expr::Drop { start, .. } => *start,
            Expr::Present { start, .. } => *start,
            Expr::Emit(e) => e.s(),
            Expr::Test(e) => e.s(),
        }
    }
    fn e(&self) -> Location {
        match self {
            Expr::RecordExpr(e) => e.e(),
            Expr::MatchExpr(e) => e.e(),
            Expr::PatchExpr(e) => e.e(),
            Expr::MergeExpr(e) => e.e(),
            Expr::Path(e) => e.e(),
            Expr::Literal(e) => e.e(),
            Expr::Binary(e) => e.e(),
            Expr::Unary(e) => e.e(),
            Expr::Invoke(e) => e.e(),
            Expr::Let(e) => e.e(),
            Expr::Assign(e) => e.e(),
            Expr::Comprehension(e) => e.e(),
            Expr::Drop { end, .. } => *end,
            Expr::Present { end, .. } => *end,
            Expr::Emit(e) => e.e(),
            Expr::Test(e) => e.e(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EmitExpr1 {
    pub start: Location,
    pub end: Location,
    pub expr: Box<Expr1>,
    pub port: Option<String>,
}
impl EmitExpr1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<EmitExpr<Ctx>> {
        Ok(EmitExpr {
            start: self.start,
            end: self.end,
            expr: self.expr.up(helper)?,
            port: self.port,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EmitExpr<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub expr: Expr<Ctx>,
    pub port: Option<String>,
}
impl_expr!(EmitExpr);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Let<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub exprs: Exprs<Ctx>,
}
impl_expr!(Let);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Assign1 {
    pub start: Location,
    pub end: Location,
    pub path: Path1,
    pub expr: Box<Expr1>,
}

impl Assign1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<Assign<Ctx>> {
        Ok(Assign {
            start: self.start,
            end: self.end,
            path: self.path.up(helper)?,
            expr: Box::new(self.expr.up(helper)?),
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Assign<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub path: Path<Ctx>,
    pub expr: Box<Expr<Ctx>>,
}
impl_expr!(Assign);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Invoke1 {
    pub start: Location,
    pub end: Location,
    pub module: String,
    pub fun: String,
    pub args: Exprs1,
}
impl_expr1!(Invoke1);

impl Invoke1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<Invoke<Ctx>> {
        let args: Result<Exprs<Ctx>> = self
            .args
            .clone()
            .into_iter()
            .map(|p| p.up(helper))
            .collect();
        let args = args?;
        let invocable = helper.reg.find(&self.module, &self.fun).map_err(|e| {
            e.into_err(
                &Expr::dummy_extend_from(&self),
                &Expr::dummy_from(&self),
                Some(&helper.reg),
            )
        })?;

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
pub struct Invoke<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub module: String,
    pub fun: String,
    #[serde(skip)]
    pub invocable: TremorFn<Ctx>,
    pub args: Exprs<Ctx>,
}
impl_expr!(Invoke);

impl<Ctx: Context + 'static> PartialEq for Invoke<Ctx> {
    fn eq(&self, other: &Self) -> bool {
        self.start == other.start
            && self.end == other.end
            && self.module == other.module
            && self.fun == other.fun
        //&& self.args == other.args FIXME why??!?
    }
}

impl<Ctx: Context + 'static> fmt::Debug for Invoke<Ctx> {
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
impl_expr1!(TestExpr);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TestExpr1 {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub test: String,
}

impl_expr1!(TestExpr1);

impl TestExpr1 {
    fn up<Ctx: Context + 'static>(self, _helper: &mut Helper<Ctx>) -> Result<TestExpr> {
        match Extractor::new(&self.id, &self.test) {
            Ok(ex) => Ok(TestExpr {
                id: self.id,
                test: self.test,
                extractor: ex,
                start: self.start,
                end: self.end,
            }),
            Err(e) => Err(ErrorKind::InvalidExtractor(
                Expr::<Ctx>::dummy_extend_from(&self).into(),
                Expr::<Ctx>::dummy_from(&self).into(),
                self.id,
                self.test,
                e.msg,
            )
            .into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Match1 {
    pub start: Location,
    pub end: Location,
    pub target: Expr1,
    pub patterns: Predicates1,
}

impl Match1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<Match<Ctx>> {
        let patterns: Result<Predicates<Ctx>> =
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
pub struct Match<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub target: Expr<Ctx>,
    pub patterns: Predicates<Ctx>,
}
impl_expr!(Match);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PredicateClause1 {
    pub start: Location,
    pub end: Location,
    pub pattern: Pattern1,
    pub guard: Option<Expr1>,
    pub exprs: Exprs1,
}

impl PredicateClause1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<PredicateClause<Ctx>> {
        let exprs: Result<Exprs<Ctx>> = self.exprs.into_iter().map(|p| p.up(helper)).collect();
        let guard = if let Some(guard) = self.guard {
            Some(guard.up(helper)?)
        } else {
            None
        };
        Ok(PredicateClause {
            start: self.start,
            end: self.end,
            pattern: self.pattern.up(helper)?,
            guard,
            exprs: exprs?,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PredicateClause<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub pattern: Pattern<Ctx>,
    pub guard: Option<Expr<Ctx>>,
    pub exprs: Exprs<Ctx>,
}
impl_expr!(PredicateClause);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Patch1 {
    pub start: Location,
    pub end: Location,
    pub target: Expr1,
    pub operations: PatchOperations1,
}

impl Patch1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<Patch<Ctx>> {
        let operations: Result<PatchOperations<Ctx>> =
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
pub struct Patch<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub target: Expr<Ctx>,
    pub operations: PatchOperations<Ctx>,
}
impl_expr!(Patch);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PatchOperation1 {
    Insert { ident: String, expr: Expr1 },
    Upsert { ident: String, expr: Expr1 },
    Update { ident: String, expr: Expr1 },
    Erase { ident: String },
    Merge { ident: String, expr: Expr1 },
    TupleMerge { expr: Expr1 },
}

impl PatchOperation1 {
    pub fn up<Ctx: Context + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<PatchOperation<Ctx>> {
        use PatchOperation1::*;
        Ok(match self {
            Insert { ident, expr } => PatchOperation::Insert {
                ident,
                expr: expr.up(helper)?,
            },
            Upsert { ident, expr } => PatchOperation::Upsert {
                ident,
                expr: expr.up(helper)?,
            },
            Update { ident, expr } => PatchOperation::Update {
                ident,
                expr: expr.up(helper)?,
            },
            Erase { ident } => PatchOperation::Erase { ident },
            Merge { ident, expr } => PatchOperation::Merge {
                ident,
                expr: expr.up(helper)?,
            },
            TupleMerge { expr } => PatchOperation::TupleMerge {
                expr: expr.up(helper)?,
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PatchOperation<Ctx: Context + 'static> {
    Insert { ident: String, expr: Expr<Ctx> },
    Upsert { ident: String, expr: Expr<Ctx> },
    Update { ident: String, expr: Expr<Ctx> },
    Erase { ident: String },
    Merge { ident: String, expr: Expr<Ctx> },
    TupleMerge { expr: Expr<Ctx> },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Merge1 {
    pub start: Location,
    pub end: Location,
    pub target: Expr1,
    pub expr: Expr1,
}
impl Merge1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<Merge<Ctx>> {
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
pub struct Merge<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub target: Expr<Ctx>,
    pub expr: Expr<Ctx>,
}
impl_expr!(Merge);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Comprehension1 {
    pub start: Location,
    pub end: Location,
    pub target: Box<Expr1>,
    pub cases: ComprehensionCases1,
}

impl Comprehension1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<Comprehension<Ctx>> {
        let cases: Result<ComprehensionCases<Ctx>> =
            self.cases.into_iter().map(|p| p.up(helper)).collect();

        Ok(Comprehension {
            start: self.start,
            end: self.end,
            target: Box::new(self.target.up(helper)?),
            cases: cases?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Comprehension<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub target: Box<Expr<Ctx>>,
    pub cases: ComprehensionCases<Ctx>,
}
impl_expr!(Comprehension);
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ComprehensionCase1 {
    pub start: Location,
    pub end: Location,
    pub key_name: String,
    pub value_name: String,
    pub guard: Option<Expr1>,
    pub expr: Box<Expr1>,
}

impl ComprehensionCase1 {
    fn up<Ctx: Context + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<ComprehensionCase<Ctx>> {
        let guard = if let Some(guard) = self.guard {
            Some(Box::new(guard.up(helper)?))
        } else {
            None
        };

        Ok(ComprehensionCase {
            start: self.start,
            end: self.end,
            key_name: self.key_name,
            value_name: self.value_name,
            guard,
            expr: Box::new(self.expr.up(helper)?),
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ComprehensionCase<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub key_name: String,
    pub value_name: String,
    pub guard: Option<Box<Expr<Ctx>>>,
    pub expr: Box<Expr<Ctx>>,
}
impl_expr!(ComprehensionCase);

// NOTE: We do not want the pain of having boxes here
// this is the script it does only rarely get allocated
// so the memory isn't that much of an issue.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Pattern1 {
    Predicate(PredicatePattern1),
    Record(RecordPattern1),
    Array(ArrayPattern1),
    Expr(Expr1),
    Assign(AssignPattern1),
    Default,
}
impl Pattern1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<Pattern<Ctx>> {
        use Pattern1::*;
        Ok(match self {
            Predicate(pp) => Pattern::Predicate(pp.up(helper)?),
            Record(rp) => Pattern::Record(rp.up(helper)?),
            Array(ap) => Pattern::Array(ap.up(helper)?),
            Expr(expr) => Pattern::Expr(expr.up(helper)?),
            Assign(ap) => Pattern::Assign(ap.up(helper)?),
            Default => Pattern::Default,
        })
    }
}

// NOTE: We do not want the pain of having boxes here
// this is the script it does only rarely get allocated
// so the memory isn't that much of an issue.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Pattern<Ctx: Context + 'static> {
    Predicate(PredicatePattern<Ctx>),
    Record(RecordPattern<Ctx>),
    Array(ArrayPattern<Ctx>),
    Expr(Expr<Ctx>),
    Assign(AssignPattern<Ctx>),
    Default,
}
impl<Ctx: Context + 'static> Pattern<Ctx> {
    fn is_default(&self) -> bool {
        if let Pattern::Default = self {
            true
        } else {
            false
        }
    }
}

pub trait BasePattern {}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PredicatePattern1 {
    TildeEq {
        assign: String,
        lhs: String,
        test: TestExpr1,
    },
    Eq {
        lhs: String,
        rhs: Expr1,
        not: bool,
    },
    RecordPatternEq {
        lhs: String,
        pattern: RecordPattern1,
    },
    ArrayPatternEq {
        lhs: String,
        pattern: ArrayPattern1,
    },
    FieldPresent {
        lhs: String,
    },
    FieldAbsent {
        lhs: String,
    },
}

impl PredicatePattern1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<PredicatePattern<Ctx>> {
        use PredicatePattern1::*;
        Ok(match self {
            TildeEq { assign, lhs, test } => PredicatePattern::TildeEq {
                assign,
                lhs,
                test: test.up(helper)?,
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
pub enum PredicatePattern<Ctx: Context + 'static> {
    TildeEq {
        assign: String,
        lhs: String,
        test: TestExpr,
    },
    Eq {
        lhs: String,
        rhs: Expr<Ctx>,
        not: bool,
    },
    RecordPatternEq {
        lhs: String,
        pattern: RecordPattern<Ctx>,
    },
    ArrayPatternEq {
        lhs: String,
        pattern: ArrayPattern<Ctx>,
    },
    FieldPresent {
        lhs: String,
    },
    FieldAbsent {
        lhs: String,
    },
}

impl<Ctx: Context + 'static> PredicatePattern<Ctx> {
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecordPattern1 {
    pub start: Location,
    pub end: Location,
    pub fields: PatternFields1,
}

impl RecordPattern1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<RecordPattern<Ctx>> {
        let fields: Result<PatternFields<Ctx>> =
            self.fields.into_iter().map(|p| p.up(helper)).collect();
        Ok(RecordPattern {
            start: self.start,
            end: self.end,
            fields: fields?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RecordPattern<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub fields: PatternFields<Ctx>,
}
impl_expr!(RecordPattern);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ArrayPredicatePattern1 {
    Expr(Expr1),
    Tilde(TestExpr1),
    Record(RecordPattern1),
    //Array(ArrayPattern),
}
impl ArrayPredicatePattern1 {
    fn up<Ctx: Context + 'static>(
        self,
        helper: &mut Helper<Ctx>,
    ) -> Result<ArrayPredicatePattern<Ctx>> {
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
pub enum ArrayPredicatePattern<Ctx: Context + 'static> {
    Expr(Expr<Ctx>),
    Tilde(TestExpr),
    Record(RecordPattern<Ctx>),
    Array(ArrayPattern<Ctx>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ArrayPattern1 {
    pub start: Location,
    pub end: Location,
    pub exprs: ArrayPredicatePatterns1,
}

impl ArrayPattern1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<ArrayPattern<Ctx>> {
        let exprs: Result<Vec<ArrayPredicatePattern<Ctx>>> =
            self.exprs.into_iter().map(|p| p.up(helper)).collect();
        Ok(ArrayPattern {
            start: self.start,
            end: self.end,
            exprs: exprs?,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ArrayPattern<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub exprs: ArrayPredicatePatterns<Ctx>,
}

impl_expr!(ArrayPattern);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssignPattern1 {
    pub id: Path1,
    pub pattern: Box<Pattern1>,
}

impl AssignPattern1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<AssignPattern<Ctx>> {
        Ok(AssignPattern {
            id: self.id.up(helper)?,
            pattern: Box::new(self.pattern.up(helper)?),
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AssignPattern<Ctx: Context + 'static> {
    pub id: Path<Ctx>,
    pub pattern: Box<Pattern<Ctx>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Path1 {
    Local(LocalPath1),
    Event(EventPath1),
    Meta(MetadataPath1),
}

impl Path1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<Path<Ctx>> {
        use Path1::*;
        Ok(match self {
            Local(p) => Path::Local(p.up(helper)?),
            Event(p) => Path::Event(p.up(helper)?),
            Meta(p) => Path::Meta(p.up(helper)?),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Path<Ctx: Context + 'static> {
    Local(LocalPath<Ctx>),
    Event(EventPath<Ctx>),
    Meta(MetadataPath<Ctx>),
}

impl<Ctx: Context + 'static> BaseExpr for Path<Ctx> {
    fn s(&self) -> Location {
        match self {
            Path::Local(e) => e.s(),
            Path::Meta(e) => e.s(),
            Path::Event(e) => e.s(),
        }
    }
    fn e(&self) -> Location {
        match self {
            Path::Local(e) => e.e(),
            Path::Meta(e) => e.e(),
            Path::Event(e) => e.e(),
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Segment1 {
    ElementSelector {
        expr: Expr1,
        start: Location,
        end: Location,
    },
    RangeSelector {
        start_lower: Location,
        range_start: Expr1,
        end_lower: Location,
        start_upper: Location,
        range_end: Expr1,
        end_upper: Location,
    },
}

impl Segment1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<Segment<Ctx>> {
        use Segment1::*;
        Ok(match self {
            ElementSelector { expr, start, end } => Segment::ElementSelector {
                start,
                end,
                expr: expr.up(helper)?,
            },
            RangeSelector {
                start_lower,
                range_start,
                end_lower,
                start_upper,
                range_end,
                end_upper,
            } => Segment::RangeSelector {
                start_lower,
                range_start: range_start.up(helper)?,
                end_lower,
                start_upper,
                range_end: range_end.up(helper)?,
                end_upper,
            },
        })
    }
    pub fn from_id(id: Ident, start: Location, end: Location) -> Self {
        Segment1::ElementSelector {
            start,
            end,
            expr: Expr1::Literal(Literal1 {
                start,
                end,
                value: LiteralValue1::Native(id.id.into()),
            }),
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Segment<Ctx: Context + 'static> {
    ElementSelector {
        expr: Expr<Ctx>,
        start: Location,
        end: Location,
    },
    RangeSelector {
        start_lower: Location,
        range_start: Expr<Ctx>,
        end_lower: Location,
        start_upper: Location,
        range_end: Expr<Ctx>,
        end_upper: Location,
    },
}

impl<Ctx: Context + 'static> BaseExpr for Segment<Ctx> {
    fn s(&self) -> Location {
        match self {
            Segment::ElementSelector { start, .. } => *start,
            Segment::RangeSelector { start_lower, .. } => *start_lower,
        }
    }
    fn e(&self) -> Location {
        match self {
            Segment::ElementSelector { end, .. } => *end,
            Segment::RangeSelector { end_upper, .. } => *end_upper,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LocalPath1 {
    pub start: Location,
    pub end: Location,
    pub segments: Segments1,
}
impl LocalPath1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<LocalPath<Ctx>> {
        let segments: Result<Segments<Ctx>> =
            self.segments.into_iter().map(|p| p.up(helper)).collect();
        Ok(LocalPath {
            start: self.start,
            end: self.end,
            segments: segments?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct LocalPath<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub segments: Segments<Ctx>,
}
impl_expr!(LocalPath);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetadataPath1 {
    pub start: Location,
    pub end: Location,
    pub segments: Segments1,
}
impl MetadataPath1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<MetadataPath<Ctx>> {
        let segments: Result<Segments<Ctx>> =
            self.segments.into_iter().map(|p| p.up(helper)).collect();
        Ok(MetadataPath {
            start: self.start,
            end: self.end,
            segments: segments?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MetadataPath<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub segments: Segments<Ctx>,
}
impl_expr!(MetadataPath);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventPath1 {
    pub start: Location,
    pub end: Location,
    pub segments: Segments1,
}
impl EventPath1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<EventPath<Ctx>> {
        let segments: Result<Segments<Ctx>> =
            self.segments.into_iter().map(|p| p.up(helper)).collect();
        Ok(EventPath {
            start: self.start,
            end: self.end,
            segments: segments?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EventPath<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub segments: Segments<Ctx>,
}
impl_expr!(EventPath);

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BinExpr1 {
    pub start: Location,
    pub end: Location,
    pub kind: BinOpKind,
    pub lhs: Box<Expr1>,
    pub rhs: Box<Expr1>,
}
impl BinExpr1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<BinExpr<Ctx>> {
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
pub struct BinExpr<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub kind: BinOpKind,
    pub lhs: Expr<Ctx>,
    pub rhs: Expr<Ctx>,
}
impl_expr!(BinExpr);

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnaryExpr1 {
    pub start: Location,
    pub end: Location,
    pub kind: UnaryOpKind,
    pub expr: Box<Expr1>,
}

impl UnaryExpr1 {
    fn up<Ctx: Context + 'static>(self, helper: &mut Helper<Ctx>) -> Result<UnaryExpr<Ctx>> {
        Ok(UnaryExpr {
            start: self.start,
            end: self.end,
            kind: self.kind,
            expr: self.expr.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct UnaryExpr<Ctx: Context + 'static> {
    pub start: Location,
    pub end: Location,
    pub kind: UnaryOpKind,
    pub expr: Expr<Ctx>,
}
impl_expr!(UnaryExpr);

pub type Exprs<Ctx> = Vec<Expr<Ctx>>;
pub type Exprs1 = Vec<Expr1>;
pub type Fields<Ctx> = Vec<Field<Ctx>>;
pub type Fields1 = Vec<Box<Field1>>;
pub type Segments<Ctx> = Vec<Segment<Ctx>>;
pub type Segments1 = Vec<Segment1>;
pub type PatternFields<Ctx> = Vec<PredicatePattern<Ctx>>;
pub type PatternFields1 = Vec<PredicatePattern1>;
pub type Predicates<Ctx> = Vec<PredicateClause<Ctx>>;
pub type Predicates1 = Vec<PredicateClause1>;
pub type PatchOperations<Ctx> = Vec<PatchOperation<Ctx>>;
pub type PatchOperations1 = Vec<PatchOperation1>;
pub type ComprehensionCases<Ctx> = Vec<ComprehensionCase<Ctx>>;
pub type ComprehensionCases1 = Vec<ComprehensionCase1>;
pub type ArrayPredicatePatterns<Ctx> = Vec<ArrayPredicatePattern<Ctx>>;
pub type ArrayPredicatePatterns1 = Vec<ArrayPredicatePattern1>;
