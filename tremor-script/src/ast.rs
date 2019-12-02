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
pub mod raw;
mod support;
pub mod upable;
use crate::errors::*;
use crate::impl_expr2;
use crate::interpreter::*;
use crate::pos::{Location, Range};
use crate::registry::{Aggr as AggrRegistry, Registry, TremorAggrFnWrapper, TremorFnWrapper};
use crate::script::Return;
use crate::stry;
use crate::tilde::Extractor;
pub use base_expr::BaseExpr;
use halfbrown::HashMap;
pub use query::*;
use serde::Serialize;
use simd_json::{BorrowedValue as Value, KnownKey};
use std::borrow::{Borrow, Cow};
use std::mem;
use upable::Upable;

#[derive(Default, Copy, Clone, Serialize, Debug, PartialEq)]
pub struct NodeMeta<'script> {
    start: Location,
    end: Location,
}

impl From<(Location, Location)> for NodeMeta<'static> {
    fn from((start, end): (Location, Location)) -> Self {
        Self { start, end }
    }
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
    pub meta: Vec<NodeMeta>,
}

impl<'script, 'registry> Helper<'script, 'registry>
where
    'script: 'registry,
{
    pub fn add_meta(&mut self, start: Location, end: Location) -> usize {
        let mid = self.meta.len();
        self.meta.push((start, end).into());
        mid
    }
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
            meta: Vec::new(),
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Script<'script> {
    pub exprs: Exprs<'script>,
    pub consts: Vec<Value<'script>>,
    pub aggregates: Vec<InvokeAggrFn<'script>>,
    pub locals: usize,
    pub node_meta: Vec<NodeMeta>,
}

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
            meta: &self.node_meta,
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
    pub mid: usize,
    pub id: Cow<'script, str>,
}
impl_expr2!(Ident);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Field<'script> {
    pub mid: usize,
    pub name: ImutExpr<'script>,
    pub value: ImutExpr<'script>,
}
impl_expr2!(Field);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Record<'script> {
    pub mid: usize,
    pub fields: Fields<'script>,
}
impl_expr2!(Record);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct List<'script> {
    pub mid: usize,
    pub exprs: ImutExprs<'script>,
}
impl_expr2!(List);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Literal<'script> {
    pub mid: usize,
    pub value: Value<'script>,
}
impl_expr2!(Literal);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FnDecl<'script> {
    pub mid: usize,
    pub name: Ident<'script>,
    pub args: Vec<Ident<'script>>,
    pub body: Exprs<'script>,
    pub locals: usize,
}
impl_expr2!(FnDecl);

fn path_eq<'script>(path: &Path<'script>, expr: &ImutExpr<'script>) -> bool {
    let path_expr: ImutExpr = ImutExpr::Path(path.clone());

    let target_expr = match expr.clone() {
        ImutExpr::Local {
            id,
            idx,
            mid,
            is_const,
        } => ImutExpr::Path(Path::Local(LocalPath {
            id,
            segments: vec![],
            idx,
            mid,
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
        mid: usize,
        path: Path<'script>,
        expr: Box<Expr<'script>>,
    },
    // Moves
    AssignMoveLocal {
        mid: usize,
        path: Path<'script>,
        idx: usize,
    },
    Comprehension(Box<Comprehension<'script>>),
    Drop {
        mid: usize,
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
        mid: usize,
        is_const: bool,
    },
    Literal(Literal<'script>),
    Present {
        path: Path<'script>,
        mid: usize,
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
pub struct EmitExpr<'script> {
    pub mid: usize,
    pub expr: ImutExpr<'script>,
    pub port: Option<ImutExpr<'script>>,
}
impl_expr2!(EmitExpr);

#[derive(Clone, Serialize)]
pub struct Invoke<'script> {
    pub mid: usize,
    pub module: String,
    pub fun: String,
    #[serde(skip)]
    pub invocable: TremorFnWrapper,
    pub args: ImutExprs<'script>,
}
impl_expr2!(Invoke);

#[derive(Clone, Serialize)]
pub struct InvokeAggr {
    pub mid: usize,
    pub module: String,
    pub fun: String,
    pub aggr_id: usize,
}

#[derive(Clone, Serialize)]
pub struct InvokeAggrFn<'script> {
    pub mid: usize,
    #[serde(skip)]
    pub invocable: TremorAggrFnWrapper,
    pub module: String,
    pub fun: String,
    pub args: ImutExprs<'script>,
}
impl_expr2!(InvokeAggrFn);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TestExpr {
    pub mid: usize,
    pub id: String,
    pub test: String,
    pub extractor: Extractor,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Match<'script> {
    pub mid: usize,
    pub target: ImutExpr<'script>,
    pub patterns: Predicates<'script>,
}
impl_expr2!(Match);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutMatch<'script> {
    pub mid: usize,
    pub target: ImutExpr<'script>,
    pub patterns: ImutPredicates<'script>,
}
impl_expr2!(ImutMatch);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PredicateClause<'script> {
    pub mid: usize,
    pub pattern: Pattern<'script>,
    pub guard: Option<ImutExpr<'script>>,
    pub exprs: Exprs<'script>,
}
impl_expr2!(PredicateClause);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutPredicateClause<'script> {
    pub mid: usize,
    pub pattern: Pattern<'script>,
    pub guard: Option<ImutExpr<'script>>,
    pub exprs: ImutExprs<'script>,
}
impl_expr2!(ImutPredicateClause);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Patch<'script> {
    pub mid: usize,
    pub target: ImutExpr<'script>,
    pub operations: PatchOperations<'script>,
}
impl_expr2!(Patch);

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
pub struct Merge<'script> {
    pub mid: usize,
    pub target: ImutExpr<'script>,
    pub expr: ImutExpr<'script>,
}
impl_expr2!(Merge);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Comprehension<'script> {
    pub mid: usize,
    pub key_id: usize,
    pub val_id: usize,
    pub target: ImutExpr<'script>,
    pub cases: ComprehensionCases<'script>,
}
impl_expr2!(Comprehension);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutComprehension<'script> {
    pub mid: usize,
    pub key_id: usize,
    pub val_id: usize,
    pub target: ImutExpr<'script>,
    pub cases: ImutComprehensionCases<'script>,
}
impl_expr2!(ImutComprehension);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ComprehensionCase<'script> {
    pub mid: usize,
    pub key_name: Cow<'script, str>,
    pub value_name: Cow<'script, str>,
    pub guard: Option<ImutExpr<'script>>,
    pub exprs: Exprs<'script>,
}
impl_expr2!(ComprehensionCase);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutComprehensionCase<'script> {
    pub mid: usize,
    pub key_name: Cow<'script, str>,
    pub value_name: Cow<'script, str>,
    pub guard: Option<ImutExpr<'script>>,
    pub exprs: ImutExprs<'script>,
}
impl_expr2!(ImutComprehensionCase);

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

    fn lhs(&self) -> &Cow<'script, str> {
        use PredicatePattern::*;
        match self {
            TildeEq { lhs, .. }
            | Eq { lhs, .. }
            | RecordPatternEq { lhs, .. }
            | ArrayPatternEq { lhs, .. }
            | FieldPresent { lhs, .. }
            | FieldAbsent { lhs, .. } => &lhs,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RecordPattern<'script> {
    pub mid: usize,
    pub fields: PatternFields<'script>,
}
impl_expr2!(RecordPattern);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ArrayPredicatePattern<'script> {
    Expr(ImutExpr<'script>),
    Tilde(TestExpr),
    Record(RecordPattern<'script>),
    Array(ArrayPattern<'script>),
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ArrayPattern<'script> {
    pub mid: usize,
    pub exprs: ArrayPredicatePatterns<'script>,
}

impl_expr2!(ArrayPattern);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AssignPattern<'script> {
    pub id: Cow<'script, str>,
    pub idx: usize,
    pub pattern: Box<Pattern<'script>>,
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

#[derive(Clone, Debug, Serialize)]
pub enum Segment<'script> {
    Id {
        id: Cow<'script, str>,
        #[serde(skip)]
        key: KnownKey<'script>,
        mid: usize,
    },
    Idx {
        idx: usize,
        mid: usize,
    },
    Element {
        expr: ImutExpr<'script>,
        mid: usize,
    },
    Range {
        lower_mid: usize,
        upper_mid: usize,
        mid: usize,
        range_start: Box<ImutExpr<'script>>,
        range_end: Box<ImutExpr<'script>>,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct LocalPath<'script> {
    pub id: Cow<'script, str>,
    pub idx: usize,
    pub is_const: bool,
    pub mid: usize,
    pub segments: Segments<'script>,
}
impl_expr2!(LocalPath);

#[derive(Clone, Debug, Serialize)]
pub struct MetadataPath<'script> {
    pub mid: usize,
    pub segments: Segments<'script>,
}
impl_expr2!(MetadataPath);

#[derive(Clone, Debug, Serialize)]
pub struct EventPath<'script> {
    pub mid: usize,
    pub segments: Segments<'script>,
}
impl_expr2!(EventPath);

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
pub struct BinExpr<'script> {
    pub mid: usize,
    pub kind: BinOpKind,
    pub lhs: ImutExpr<'script>,
    pub rhs: ImutExpr<'script>,
}
impl_expr2!(BinExpr);

#[derive(Copy, Clone, Debug, PartialEq, Serialize)]
pub enum UnaryOpKind {
    Plus,
    Minus,
    Not,
    BitNot,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct UnaryExpr<'script> {
    pub mid: usize,
    pub kind: UnaryOpKind,
    pub expr: ImutExpr<'script>,
}
impl_expr2!(UnaryExpr);

pub type Exprs<'script> = Vec<Expr<'script>>;
pub type ImutExprs<'script> = Vec<ImutExpr<'script>>;
pub type Fields<'script> = Vec<Field<'script>>;
pub type Segments<'script> = Vec<Segment<'script>>;
pub type PatternFields<'script> = Vec<PredicatePattern<'script>>;
pub type Predicates<'script> = Vec<PredicateClause<'script>>;
pub type ImutPredicates<'script> = Vec<ImutPredicateClause<'script>>;
pub type PatchOperations<'script> = Vec<PatchOperation<'script>>;
pub type ComprehensionCases<'script> = Vec<ComprehensionCase<'script>>;
pub type ImutComprehensionCases<'script> = Vec<ImutComprehensionCase<'script>>;
pub type ArrayPredicatePatterns<'script> = Vec<ArrayPredicatePattern<'script>>;
pub type Stmts<'script> = Vec<Stmt<'script>>;

fn replace_last_shadow_use<'script>(replace_idx: usize, expr: Expr<'script>) -> Expr<'script> {
    match expr {
        Expr::Assign { path, expr, mid } => match expr.borrow() {
            Expr::Imut(ImutExpr::Local { idx, .. }) if idx == &replace_idx => {
                Expr::AssignMoveLocal {
                    mid,
                    idx: *idx,
                    path,
                }
            }

            _ => Expr::Assign { path, expr, mid },
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
