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

pub(crate) mod base_expr;
/// Query AST
pub mod query;
pub(crate) mod raw;
mod support;
mod upable;
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
use simd_json::{prelude::*, BorrowedValue as Value, KnownKey};
use std::borrow::{Borrow, Cow};
use std::mem;
use upable::Upable;

#[derive(Default, Clone, Serialize, Debug, PartialEq)]
struct NodeMeta<'script> {
    start: Location,
    end: Location,
    name: Option<Cow<'script, str>>,
}

impl From<(Location, Location)> for NodeMeta<'static> {
    fn from((start, end): (Location, Location)) -> Self {
        Self {
            start,
            end,
            name: None,
        }
    }
}
/// Information about node metadata
#[derive(Default, Clone, Serialize, Debug, PartialEq)]
pub struct NodeMetas<'script>(Vec<NodeMeta<'script>>);

impl<'script> NodeMetas<'script> {
    pub(crate) fn add_meta(&mut self, start: Location, end: Location) -> usize {
        let mid = self.0.len();
        self.0.push((start, end).into());
        mid
    }
    pub(crate) fn add_meta_w_name(
        &mut self,
        start: Location,
        end: Location,
        name: Cow<'script, str>,
    ) -> usize {
        let mid = self.0.len();
        self.0.push(NodeMeta {
            start,
            end,
            name: Some(name),
        });
        mid
    }
    pub(crate) fn start(&self, idx: usize) -> Option<Location> {
        self.0.get(idx).map(|v| v.start)
    }
    pub(crate) fn end(&self, idx: usize) -> Option<Location> {
        self.0.get(idx).map(|v| v.end)
    }
    pub(crate) fn name(&self, idx: usize) -> Option<&Cow<'script, str>> {
        self.0.get(idx).map(|v| v.name.as_ref()).and_then(|v| v)
    }
    pub(crate) fn name_dflt(&self, idx: usize) -> Cow<'script, str> {
        self.name(idx)
            .cloned()
            .unwrap_or_else(|| String::from("<UNKNOWN>").into())
    }
}

#[derive(Serialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
/// A warning generated while lexing or parsing
pub struct Warning {
    /// Outer span of the warning
    pub outer: Range,
    /// Inner span of thw warning
    pub inner: Range,
    /// Warning message
    pub msg: String,
}

pub(crate) struct Helper<'script, 'registry>
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
    // TODO: Users of the `warnings` field might be helped if `warnings` were a Set. Right now,
    // some places (twice in query/raw.rs) do `append + sort + dedup`. With, e.g., a `BTreeSet`,
    // this could be achieved in a cleaner and faster way, and `Warning` already implements `Ord`
    // anyway.
    warnings: Vec<Warning>,
    shadowed_vars: Vec<String>,
    pub locals: HashMap<String, usize>,
    pub consts: HashMap<String, usize>,
    pub meta: NodeMetas<'script>,
}

impl<'script, 'registry> Helper<'script, 'registry>
where
    'script: 'registry,
{
    pub fn add_meta(&mut self, start: Location, end: Location) -> usize {
        self.meta.add_meta(start, end)
    }
    pub fn add_meta_w_name(
        &mut self,
        start: Location,
        end: Location,
        name: Cow<'script, str>,
    ) -> usize {
        self.meta.add_meta_w_name(start, end, name)
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
            meta: NodeMetas::default(),
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

    fn find_shadow_var(&self, id: &str) -> Option<String> {
        let mut r = None;
        for (i, s) in self.shadowed_vars.iter().enumerate() {
            if s == id {
                //FIXME: make sure we never overwrite this,
                r = Some(shadow_name(i))
            }
        }
        r
    }

    fn reserve_shadow(&mut self) -> usize {
        self.var_id(&shadow_name(self.shadowed_vars.len()))
    }

    fn reserve_2_shadow(&mut self) -> (usize, usize) {
        let l = self.shadowed_vars.len();
        let n1 = shadow_name(l);
        let n2 = shadow_name(l + 1);
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

/// A tremor script instance
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Script<'script> {
    /// Expressions of the script
    exprs: Exprs<'script>,
    /// Constants defined in this script
    pub consts: Vec<Value<'script>>,
    aggregates: Vec<InvokeAggrFn<'script>>,
    locals: usize,
    node_meta: NodeMetas<'script>,
}

impl<'run, 'script, 'event> Script<'script>
where
    'script: 'event,
    'event: 'run,
{
    /// Runs the script and evaluates to a resulting event
    pub fn run(
        &'script self,
        context: &'run crate::EventContext,
        aggr: AggrType,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
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
                match stry!(expr.run(opts.with_result(), &env, event, state, meta, &mut local)) {
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
                match stry!(expr.run(opts.without_result(), &env, event, state, meta, &mut local)) {
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
            value: Value::null(),
            port: None,
        })
    }
}

/// An ident
#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct Ident<'script> {
    pub(crate) mid: usize,
    /// the text of the ident
    pub id: Cow<'script, str>,
}
impl_expr2!(Ident);

impl<'script> From<&'script str> for Ident<'script> {
    fn from(id: &'script str) -> Self {
        Self {
            mid: 0,
            id: id.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct Field<'script> {
    pub mid: usize,
    pub name: ImutExprInt<'script>,
    pub value: ImutExprInt<'script>,
}
impl_expr2!(Field);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct Record<'script> {
    pub mid: usize,
    pub fields: Fields<'script>,
}
impl_expr2!(Record);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct List<'script> {
    pub mid: usize,
    pub exprs: ImutExprs<'script>,
}
impl_expr2!(List);

/// A Literal
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Literal<'script> {
    /// MetadataId of this node
    pub mid: usize,
    /// Literal Value
    pub value: Value<'script>,
}
impl_expr2!(Literal);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct FnDecl<'script> {
    pub mid: usize,
    pub name: Ident<'script>,
    pub args: Vec<Ident<'script>>,
    pub body: Exprs<'script>,
    pub locals: usize,
}
impl_expr2!(FnDecl);

fn path_eq<'script>(path: &Path<'script>, expr: &ImutExprInt<'script>) -> bool {
    let path_expr: ImutExprInt = ImutExprInt::Path(path.clone());

    let target_expr = match expr.clone() {
        ImutExprInt::Local {
            //id,
            idx,
            mid,
            is_const,
        } => ImutExprInt::Path(Path::Local(LocalPath {
            //id,
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
pub(crate) enum Expr<'script> {
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
    Imut(ImutExprInt<'script>),
}

impl<'script> From<ImutExprInt<'script>> for Expr<'script> {
    fn from(imut: ImutExprInt<'script>) -> Expr<'script> {
        Expr::Imut(imut)
    }
}

/// An immutable expression
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutExpr<'script>(pub(crate) ImutExprInt<'script>);

impl<'script> From<Literal<'script>> for ImutExpr<'script> {
    fn from(lit: Literal<'script>) -> Self {
        Self(ImutExprInt::Literal(lit))
    }
}

impl<'script> BaseExpr for ImutExpr<'script> {
    fn mid(&self) -> usize {
        self.0.mid()
    }

    fn s(&self, meta: &NodeMetas) -> Location {
        self.0.s(meta)
    }

    fn e(&self, meta: &NodeMetas) -> Location {
        self.0.e(meta)
    }

    fn extent(&self, meta: &NodeMetas) -> Range {
        self.0.extent(meta)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) enum ImutExprInt<'script> {
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
        //id: Cow<'script, str>,
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

fn is_lit<'script>(e: &ImutExprInt<'script>) -> bool {
    match e {
        ImutExprInt::Literal(_) => true,
        _ => false,
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct EmitExpr<'script> {
    pub mid: usize,
    pub expr: ImutExprInt<'script>,
    pub port: Option<ImutExprInt<'script>>,
}
impl_expr2!(EmitExpr);

#[derive(Clone, Serialize)]
pub(crate) struct Invoke<'script> {
    pub mid: usize,
    pub module: String,
    pub fun: String,
    #[serde(skip)]
    pub invocable: TremorFnWrapper,
    pub args: ImutExprs<'script>,
}
impl_expr2!(Invoke);

#[derive(Clone, Serialize)]
pub(crate) struct InvokeAggr {
    pub mid: usize,
    pub module: String,
    pub fun: String,
    pub aggr_id: usize,
}

/// A Invocable aggregate function
#[derive(Clone, Serialize)]
pub struct InvokeAggrFn<'script> {
    pub(crate) mid: usize,
    /// The invocable function
    #[serde(skip)]
    pub invocable: TremorAggrFnWrapper,
    pub(crate) module: String,
    pub(crate) fun: String,
    /// Arguments passed to the function
    pub args: ImutExprs<'script>,
}
impl_expr2!(InvokeAggrFn);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct TestExpr {
    pub mid: usize,
    pub id: String,
    pub test: String,
    pub extractor: Extractor,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct Match<'script> {
    pub mid: usize,
    pub target: ImutExprInt<'script>,
    pub patterns: Predicates<'script>,
}
impl_expr2!(Match);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct ImutMatch<'script> {
    pub mid: usize,
    pub target: ImutExprInt<'script>,
    pub patterns: ImutPredicates<'script>,
}
impl_expr2!(ImutMatch);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct PredicateClause<'script> {
    pub mid: usize,
    pub pattern: Pattern<'script>,
    pub guard: Option<ImutExprInt<'script>>,
    pub exprs: Exprs<'script>,
}
impl_expr2!(PredicateClause);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct ImutPredicateClause<'script> {
    pub mid: usize,
    pub pattern: Pattern<'script>,
    pub guard: Option<ImutExprInt<'script>>,
    pub exprs: ImutExprs<'script>,
}
impl_expr2!(ImutPredicateClause);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct Patch<'script> {
    pub mid: usize,
    pub target: ImutExprInt<'script>,
    pub operations: PatchOperations<'script>,
}
impl_expr2!(Patch);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) enum PatchOperation<'script> {
    Insert {
        ident: ImutExprInt<'script>,
        expr: ImutExprInt<'script>,
    },
    Upsert {
        ident: ImutExprInt<'script>,
        expr: ImutExprInt<'script>,
    },
    Update {
        ident: ImutExprInt<'script>,
        expr: ImutExprInt<'script>,
    },
    Erase {
        ident: ImutExprInt<'script>,
    },
    Copy {
        from: ImutExprInt<'script>,
        to: ImutExprInt<'script>,
    },
    Move {
        from: ImutExprInt<'script>,
        to: ImutExprInt<'script>,
    },
    Merge {
        ident: ImutExprInt<'script>,
        expr: ImutExprInt<'script>,
    },
    TupleMerge {
        expr: ImutExprInt<'script>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct Merge<'script> {
    pub mid: usize,
    pub target: ImutExprInt<'script>,
    pub expr: ImutExprInt<'script>,
}
impl_expr2!(Merge);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct Comprehension<'script> {
    pub mid: usize,
    pub key_id: usize,
    pub val_id: usize,
    pub target: ImutExprInt<'script>,
    pub cases: ComprehensionCases<'script>,
}
impl_expr2!(Comprehension);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct ImutComprehension<'script> {
    pub mid: usize,
    pub key_id: usize,
    pub val_id: usize,
    pub target: ImutExprInt<'script>,
    pub cases: ImutComprehensionCases<'script>,
}
impl_expr2!(ImutComprehension);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct ComprehensionCase<'script> {
    pub mid: usize,
    pub key_name: Cow<'script, str>,
    pub value_name: Cow<'script, str>,
    pub guard: Option<ImutExprInt<'script>>,
    pub exprs: Exprs<'script>,
}
impl_expr2!(ComprehensionCase);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct ImutComprehensionCase<'script> {
    pub mid: usize,
    pub key_name: Cow<'script, str>,
    pub value_name: Cow<'script, str>,
    pub guard: Option<ImutExprInt<'script>>,
    pub exprs: ImutExprs<'script>,
}
impl_expr2!(ImutComprehensionCase);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) enum Pattern<'script> {
    //Predicate(PredicatePattern<'script>),
    Record(RecordPattern<'script>),
    Array(ArrayPattern<'script>),
    Expr(ImutExprInt<'script>),
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
pub(crate) enum PredicatePattern<'script> {
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
        rhs: ImutExprInt<'script>,
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
pub(crate) struct RecordPattern<'script> {
    pub mid: usize,
    pub fields: PatternFields<'script>,
}
impl_expr2!(RecordPattern);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) enum ArrayPredicatePattern<'script> {
    Expr(ImutExprInt<'script>),
    Tilde(TestExpr),
    Record(RecordPattern<'script>),
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct ArrayPattern<'script> {
    pub mid: usize,
    pub exprs: ArrayPredicatePatterns<'script>,
}

impl_expr2!(ArrayPattern);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct AssignPattern<'script> {
    pub id: Cow<'script, str>,
    pub idx: usize,
    pub pattern: Box<Pattern<'script>>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) enum Path<'script> {
    Const(LocalPath<'script>),
    Local(LocalPath<'script>),
    Event(EventPath<'script>),
    State(StatePath<'script>),
    Meta(MetadataPath<'script>),
}

impl<'script> Path<'script> {
    pub fn segments(&self) -> &[Segment] {
        match self {
            Path::Const(path) | Path::Local(path) => &path.segments,
            Path::Meta(path) => &path.segments,
            Path::Event(path) => &path.segments,
            Path::State(path) => &path.segments,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) enum Segment<'script> {
    Id {
        #[serde(skip)]
        key: KnownKey<'script>,
        mid: usize,
    },
    Idx {
        idx: usize,
        mid: usize,
    },
    Element {
        expr: ImutExprInt<'script>,
        mid: usize,
    },
    Range {
        lower_mid: usize,
        upper_mid: usize,
        mid: usize,
        range_start: Box<ImutExprInt<'script>>,
        range_end: Box<ImutExprInt<'script>>,
    },
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct LocalPath<'script> {
    //pub id: Cow<'script, str>,
    pub idx: usize,
    pub is_const: bool,
    pub mid: usize,
    pub segments: Segments<'script>,
}
impl_expr2!(LocalPath);

#[derive(Clone, Debug, Serialize)]
pub(crate) struct MetadataPath<'script> {
    pub mid: usize,
    pub segments: Segments<'script>,
}
impl_expr2!(MetadataPath);

#[derive(Clone, Debug, Serialize)]
pub(crate) struct EventPath<'script> {
    pub mid: usize,
    pub segments: Segments<'script>,
}
impl_expr2!(EventPath);

#[derive(Clone, Debug, Serialize)]
pub(crate) struct StatePath<'script> {
    pub mid: usize,
    pub segments: Segments<'script>,
}
impl_expr2!(StatePath);

/// we're forced to make this pub because of lalrpop
#[derive(Copy, Clone, Debug, PartialEq, Serialize)]
pub enum BinOpKind {
    /// we're forced to make this pub because of lalrpop
    Or,
    /// we're forced to make this pub because of lalrpop
    Xor,
    /// we're forced to make this pub because of lalrpop
    And,

    /// we're forced to make this pub because of lalrpop
    BitOr,
    /// we're forced to make this pub because of lalrpop
    BitXor,
    /// we're forced to make this pub because of lalrpop
    BitAnd,

    /// we're forced to make this pub because of lalrpop
    Eq,
    /// we're forced to make this pub because of lalrpop
    NotEq,

    /// we're forced to make this pub because of lalrpop
    Gte,
    /// we're forced to make this pub because of lalrpop
    Gt,
    /// we're forced to make this pub because of lalrpop
    Lte,
    /// we're forced to make this pub because of lalrpop
    Lt,

    /// we're forced to make this pub because of lalrpop
    RBitShiftSigned,
    /// we're forced to make this pub because of lalrpop
    RBitShiftUnsigned,
    /// we're forced to make this pub because of lalrpop
    LBitShift,

    /// we're forced to make this pub because of lalrpop
    Add,
    /// we're forced to make this pub because of lalrpop
    Sub,
    /// we're forced to make this pub because of lalrpop
    Mul,
    /// we're forced to make this pub because of lalrpop
    Div,
    /// we're forced to make this pub because of lalrpop
    Mod,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct BinExpr<'script> {
    pub mid: usize,
    pub kind: BinOpKind,
    pub lhs: ImutExprInt<'script>,
    pub rhs: ImutExprInt<'script>,
}
impl_expr2!(BinExpr);

/// we're forced to make this pub because of lalrpop
#[derive(Copy, Clone, Debug, PartialEq, Serialize)]
pub enum UnaryOpKind {
    /// we're forced to make this pub because of lalrpop
    Plus,
    /// we're forced to make this pub because of lalrpop
    Minus,
    /// we're forced to make this pub because of lalrpop
    Not,
    /// we're forced to make this pub because of lalrpop
    BitNot,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct UnaryExpr<'script> {
    pub mid: usize,
    pub kind: UnaryOpKind,
    pub expr: ImutExprInt<'script>,
}
impl_expr2!(UnaryExpr);

pub(crate) type Exprs<'script> = Vec<Expr<'script>>;
/// A list of immutable expressions
pub type ImutExprs<'script> = Vec<ImutExpr<'script>>;
pub(crate) type Fields<'script> = Vec<Field<'script>>;
pub(crate) type Segments<'script> = Vec<Segment<'script>>;
pub(crate) type PatternFields<'script> = Vec<PredicatePattern<'script>>;
pub(crate) type Predicates<'script> = Vec<PredicateClause<'script>>;
pub(crate) type ImutPredicates<'script> = Vec<ImutPredicateClause<'script>>;
pub(crate) type PatchOperations<'script> = Vec<PatchOperation<'script>>;
pub(crate) type ComprehensionCases<'script> = Vec<ComprehensionCase<'script>>;
pub(crate) type ImutComprehensionCases<'script> = Vec<ImutComprehensionCase<'script>>;
pub(crate) type ArrayPredicatePatterns<'script> = Vec<ArrayPredicatePattern<'script>>;
/// A vector of statements
pub type Stmts<'script> = Vec<Stmt<'script>>;

fn replace_last_shadow_use<'script>(replace_idx: usize, expr: Expr<'script>) -> Expr<'script> {
    match expr {
        Expr::Assign { path, expr, mid } => match expr.borrow() {
            Expr::Imut(ImutExprInt::Local { idx, .. }) if idx == &replace_idx => {
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

            // In each pattern we can replace the use in the last assign
            for p in &mut m.patterns {
                if let Some(expr) = p.exprs.pop() {
                    p.exprs.push(replace_last_shadow_use(replace_idx, expr))
                }
            }

            Expr::Match(Box::new(m))
        }
        other => other,
    }
}

fn shadow_name(id: usize) -> String {
    format!(" __SHADOW {}__ ", id)
}
