// Copyright 2020-2021, The Tremor Team
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

use std::hash::{Hash, Hasher};

use crate::ast::optimizer::Optimizer;
use crate::ast::{BooleanBinExpr, BooleanBinOpKind};
use crate::{
    ast::{
        base_expr, query, upable::Upable, ArrayPattern, ArrayPredicatePattern, AssignPattern,
        BinExpr, BinOpKind, Bytes, BytesPart, ClauseGroup, Comprehension, ComprehensionCase,
        Costly, DefaultCase, EmitExpr, EventPath, Expr, ExprPath, Expression, Field, FnDefn,
        Helper, Ident, IfElse, ImutExpr, Invocable, Invoke, InvokeAggr, InvokeAggrFn, List,
        Literal, LocalPath, Match, Merge, MetadataPath, Patch, PatchOperation, Path, Pattern,
        PredicateClause, PredicatePattern, Record, RecordPattern, Recur, ReservedPath, Script,
        Segment, StatePath, StrLitElement, StringLit, TestExpr, TuplePattern, UnaryExpr,
        UnaryOpKind,
    },
    errors::{err_generic, error_generic, error_missing_effector, Kind as ErrorKind, Result},
    extractor::Extractor,
    impl_expr, impl_expr_exraw, impl_expr_no_lt,
    prelude::*,
    KnownKey, Value,
};
pub use base_expr::BaseExpr;
use beef::Cow;
use halfbrown::HashMap;
pub use query::*;
use serde::Serialize;

use super::{
    base_expr::Ranged,
    docs::{FnDoc, ModDoc},
    module::Manager,
    warning, Const, NodeId, NodeMeta,
};

#[derive(Clone, Debug, PartialEq, Serialize, Eq)]
pub struct UseRaw {
    pub modules: Vec<(NodeId, Option<String>)>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr_no_lt!(UseRaw);

/// A raw script we got to put this here because of silly lalrpoop focing it to be public
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ScriptRaw<'script> {
    mid: Box<NodeMeta>,
    exprs: TopLevelExprsRaw<'script>,
    doc: Option<Vec<Cow<'script, str>>>,
}

impl<'script> ScriptRaw<'script> {
    pub(crate) fn new(
        mid: Box<NodeMeta>,
        exprs: TopLevelExprsRaw<'script>,
        doc: Option<Vec<Cow<'script, str>>>,
    ) -> Self {
        Self { mid, exprs, doc }
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn up_script<'registry>(
        self,
        mut helper: &mut Helper<'script, 'registry>,
    ) -> Result<Script<'script>> {
        helper.enter_scope();
        let mut exprs = vec![];

        for e in self.exprs {
            match e {
                TopLevelExprRaw::Use(UseRaw { modules, .. }) => {
                    for (module, alias) in modules {
                        let mid = Manager::load(&module)?;
                        let alias = alias.unwrap_or_else(|| module.id.clone());
                        helper.scope().add_module_alias(alias, mid);
                    }
                }
                TopLevelExprRaw::Const(const_raw) => {
                    let c = const_raw.up(helper)?;
                    exprs.push(Expr::Imut(ImutExpr::literal(
                        c.mid.clone(),
                        c.value.clone(),
                    )));
                    helper.scope.insert_const(c)?;
                }
                TopLevelExprRaw::FnDefn(f) => {
                    let mut f = f.up(helper)?;
                    Optimizer::new(helper).walk_fn_defn(&mut f)?;

                    helper.scope.insert_function(f)?;
                }
                TopLevelExprRaw::Expr(expr) => {
                    exprs.push(expr.up(helper)?);
                }
            }
        }

        // We make sure the if we return `event` we turn it into `emit event`
        // While this is not required logically it allows us to
        // take advantage of the `emit event` optimisation
        if let Some(e) = exprs.last_mut() {
            if let Expr::Imut(ImutExpr::Path(Path::Event(p))) = e {
                if p.segments.is_empty() {
                    let expr = EmitExpr {
                        mid: Box::new(p.meta().clone()),
                        expr: ImutExpr::Path(Path::Event(p.clone())),
                        port: None,
                    };
                    *e = Expr::Emit(Box::new(expr));
                }
            }
        } else {
            let expr = EmitExpr {
                mid: self.mid.clone(),
                expr: ImutExpr::Path(Path::Event(EventPath {
                    mid: self.mid.clone(),
                    segments: vec![],
                })),
                port: None,
            };
            exprs.push(Expr::Emit(Box::new(expr)));
        }

        helper.docs.module = Some(ModDoc {
            name: "self".into(),
            doc: self
                .doc
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        });
        helper.leave_scope()?;
        Ok(Script {
            mid: self.mid,
            exprs,
            state: None,
            locals: helper.locals.len(),
            docs: helper.docs.clone(),
        })
    }
}

#[derive(Debug, PartialEq, Serialize, Clone, Copy, Eq)]
pub enum BytesDataType {
    SignedInteger,
    UnsignedInteger,
    Binary,
}

impl Default for BytesDataType {
    fn default() -> Self {
        BytesDataType::UnsignedInteger
    }
}

#[derive(Debug, PartialEq, Serialize, Clone, Copy, Eq)]
pub enum Endian {
    Little,
    Big,
}

impl Default for Endian {
    fn default() -> Self {
        Endian::Big
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct BytesPartRaw<'script> {
    pub data: ImutExprRaw<'script>,
    pub data_type: IdentRaw<'script>,
    pub bits: Option<u64>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(BytesPartRaw);

impl<'script> Upable<'script> for BytesPartRaw<'script> {
    type Target = BytesPart<'script>;
    // We allow this for casting the bits
    #[allow(clippy::cast_sign_loss)]
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let data_type: Vec<&str> = self.data_type.id.split('_').collect();
        let (data_type, endianess) = match data_type.as_slice() {
            ["binary"] => (BytesDataType::Binary, Endian::Big),
            []
            | ["" | "integer" | "big" | "unsigned"]
            | ["unsigned" | "big", "integer"]
            | ["big", "unsigned", "integer"] => (BytesDataType::UnsignedInteger, Endian::Big),
            ["signed", "integer"] | ["big", "signed", "integer"] => {
                (BytesDataType::SignedInteger, Endian::Big)
            }
            ["little"] | ["little", "integer"] | ["little", "unsigned", "integer"] => {
                (BytesDataType::UnsignedInteger, Endian::Little)
            }
            ["little", "signed", "integer"] => (BytesDataType::SignedInteger, Endian::Little),
            other => {
                return Err(error_generic(
                    &self,
                    &self,
                    &format!("Not a valid data type: '{}'", other.join("-")),
                ))
            }
        };
        let bits = if let Some(bits) = self.bits {
            if bits == 0 || bits > 64 {
                return Err(error_generic(
                    &self,
                    &self,
                    &format!("negative bits or bits > 64 are are not allowed: {}", bits),
                ));
            }
            bits as u64
        } else {
            match data_type {
                BytesDataType::SignedInteger | BytesDataType::UnsignedInteger => 8,
                BytesDataType::Binary => 0,
            }
        };

        Ok(BytesPart {
            mid: self.mid,
            data: self.data.up(helper)?,
            data_type,
            endianess,
            bits,
        })
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct BytesRaw<'script> {
    pub(crate) mid: Box<NodeMeta>,
    pub bytes: Vec<BytesPartRaw<'script>>,
}
impl_expr!(BytesRaw);

impl<'script> Upable<'script> for BytesRaw<'script> {
    type Target = Bytes<'script>;

    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Bytes {
            mid: self.mid,
            value: self
                .bytes
                .into_iter()
                .map(|b| b.up(helper))
                .collect::<Result<_>>()?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Debug, Serialize, Clone, Eq)]
pub struct IdentRaw<'script> {
    pub(crate) mid: Box<NodeMeta>,
    pub id: beef::Cow<'script, str>,
}
impl_expr!(IdentRaw);

impl PartialEq<str> for IdentRaw<'_> {
    fn eq(&self, other: &str) -> bool {
        self.id == other
    }
}

impl PartialEq for IdentRaw<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for IdentRaw<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<'script> IdentRaw<'script> {
    /// empty ident
    pub(crate) fn none(mid: Box<NodeMeta>) -> Self {
        Self {
            mid,
            id: Cow::const_str(""),
        }
    }

    /// literal ident injected at position `mid`
    pub(crate) fn literal(mid: Box<NodeMeta>, s: &'script str) -> Self {
        Self {
            mid,
            id: Cow::const_str(s),
        }
    }
}

impl<'script> ToString for IdentRaw<'script> {
    fn to_string(&self) -> String {
        self.id.to_string()
    }
}

impl<'script, 'str> PartialEq<&'str str> for IdentRaw<'script> {
    fn eq(&self, other: &&'str str) -> bool {
        self.id == *other
    }
}

impl<'script> Upable<'script> for IdentRaw<'script> {
    type Target = Ident<'script>;
    fn up<'registry>(self, _helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Self::Target {
            mid: self.mid.box_with_name(&self.id),
            id: self.id,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FieldRaw<'script> {
    pub(crate) name: StringLitRaw<'script>,
    pub(crate) value: ImutExprRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}

impl<'script> Upable<'script> for FieldRaw<'script> {
    type Target = Field<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let name = self.name.up(helper)?;
        Ok(Field {
            mid: self.mid,
            name,
            value: self.value.up(helper)?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RecordRaw<'script> {
    pub(crate) fields: FieldsRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(RecordRaw);

impl<'script> Upable<'script> for RecordRaw<'script> {
    type Target = Record<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Record {
            base: crate::Object::new(),
            mid: self.mid,
            fields: self.fields.up(helper)?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ListRaw<'script> {
    pub(crate) exprs: ImutExprsRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(ListRaw);

impl<'script> Upable<'script> for ListRaw<'script> {
    type Target = List<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(List {
            mid: self.mid,
            exprs: self.exprs.up(helper)?.into_iter().collect(),
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize, Eq)]
pub struct LiteralRaw<'script> {
    pub(crate) value: Value<'script>,
    pub(crate) mid: Box<NodeMeta>,
}

impl_expr!(LiteralRaw);
impl<'script> Upable<'script> for LiteralRaw<'script> {
    type Target = Literal<'script>;
    fn up<'registry>(self, _helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Literal {
            mid: self.mid,
            value: self.value,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StringLitRaw<'script> {
    pub(crate) elements: StrLitElementsRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}

impl<'script> Upable<'script> for StringLitRaw<'script> {
    type Target = StringLit<'script>;

    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let elements = self
            .elements
            .into_iter()
            .rev()
            .map(|e| e.up(helper))
            .collect::<Result<_>>()?;
        Ok(StringLit {
            mid: self.mid,
            elements,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum StrLitElementRaw<'script> {
    Lit(Cow<'script, str>),
    Expr(ImutExprRaw<'script>),
}
impl<'script> Upable<'script> for StrLitElementRaw<'script> {
    type Target = StrLitElement<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match self {
            StrLitElementRaw::Lit(l) => Ok(StrLitElement::Lit(l)),
            StrLitElementRaw::Expr(e) => Ok(StrLitElement::Expr(e.up(helper)?)),
        }
    }
}

impl<'script> From<ImutExprRaw<'script>> for StrLitElementRaw<'script> {
    fn from(e: ImutExprRaw<'script>) -> Self {
        StrLitElementRaw::Expr(e)
    }
}

impl<'script> From<Cow<'script, str>> for StrLitElementRaw<'script> {
    fn from(e: Cow<'script, str>) -> Self {
        StrLitElementRaw::Lit(e)
    }
}

impl<'script> From<&'script str> for StrLitElementRaw<'script> {
    fn from(e: &'script str) -> Self {
        StrLitElementRaw::Lit(e.into())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ConstRaw<'script> {
    pub name: Cow<'script, str>,
    pub expr: ImutExprRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
    /// doc comment - is put into the helper
    pub comment: Option<Vec<Cow<'script, str>>>,
}
impl_expr!(ConstRaw);

impl<'script> Upable<'script> for ConstRaw<'script> {
    type Target = Const<'script>;

    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        if self.name.to_uppercase() != self.name {
            helper.warn_with_scope(
                self.extent(),
                &"const's are canonically written in UPPER_CASE",
                warning::Class::Consistency,
            );
        }
        let expr = self.expr.up(helper)?;
        let value = expr.try_into_value(helper)?;
        helper.add_const_doc(&self.name, self.comment.clone(), value.value_type());
        Ok(Const {
            mid: self.mid.box_with_name(&self.name),
            id: self.name.to_string(),
            value,
        })
    }
}

/// we're forced to make this pub because of lalrpop
pub type StrLitElementsRaw<'script> = Vec<StrLitElementRaw<'script>>;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum TopLevelExprRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Const(ConstRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    FnDefn(AnyFnRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Use(UseRaw),
    /// we're forced to make this pub because of lalrpop
    Expr(ExprRaw<'script>),
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ExprRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    MatchExpr(Box<MatchRaw<'script, Self>>),
    /// we're forced to make this pub because of lalrpop
    Assign(Box<AssignRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Comprehension(Box<ComprehensionRaw<'script, Self>>),
    Drop {
        mid: Box<NodeMeta>,
    },
    /// we're forced to make this pub because of lalrpop
    Emit(Box<EmitExprRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Imut(ImutExprRaw<'script>),
}
impl<'script> ExpressionRaw<'script> for ExprRaw<'script> {}

fn is_one_simple_group<'script>(patterns: &[ClauseGroup<'script, Expr<'script>>]) -> bool {
    let is_one = patterns.len() == 1;
    let is_simple = patterns
        .first()
        .map(|cg| {
            if let ClauseGroup::Simple {
                precondition: None,
                patterns,
            } = cg
            {
                patterns.len() == 1
            } else {
                false
            }
        })
        .unwrap_or_default();
    is_one && is_simple
}

impl<'script> Upable<'script> for ExprRaw<'script> {
    type Target = Expr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(match self {
            ExprRaw::MatchExpr(m) => match m.up(helper)? {
                Match {
                    mid,
                    target,
                    mut patterns,
                    default,
                } if is_one_simple_group(&patterns) => {
                    if let Some(ClauseGroup::Simple {
                        precondition: None,
                        mut patterns,
                    }) = patterns.pop()
                    {
                        if let Some(if_clause) = patterns.pop() {
                            let ie = IfElse {
                                mid,
                                target,
                                if_clause,
                                else_clause: default,
                            };
                            Expr::IfElse(Box::new(ie))
                        } else {
                            return Err("Invalid group clause with 0 patterns".into());
                        }
                    } else {
                        // ALLOW: we check patterns.len() above
                        unreachable!()
                    }
                }
                m => Expr::Match(Box::new(m)),
            },
            ExprRaw::Assign(a) => {
                let path = a.path.up(helper)?;
                let mid = a.mid;
                match a.expr.up(helper)? {
                    Expr::Imut(ImutExpr::Merge(m)) => Expr::Assign {
                        mid,
                        path,
                        expr: Box::new(ImutExpr::Merge(m).into()),
                    },
                    Expr::Imut(ImutExpr::Patch(m)) => Expr::Assign {
                        mid,
                        path,
                        expr: Box::new(ImutExpr::Patch(m).into()),
                    },
                    expr => Expr::Assign {
                        mid,
                        path,
                        expr: Box::new(expr),
                    },
                }
            }
            ExprRaw::Comprehension(c) => Expr::Comprehension(Box::new(c.up(helper)?)),
            ExprRaw::Drop { mid } => {
                if !helper.can_emit {
                    return Err(ErrorKind::InvalidDrop(mid.range.expand_lines(2), mid.range).into());
                }
                Expr::Drop { mid }
            }
            ExprRaw::Emit(e) => Expr::Emit(Box::new(e.up(helper)?)),
            ExprRaw::Imut(i) => i.up(helper)?.into(),
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FnDefnRaw<'script> {
    pub(crate) name: IdentRaw<'script>,
    pub(crate) args: Vec<IdentRaw<'script>>,
    pub(crate) body: ExprsRaw<'script>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
    pub(crate) open: bool,
    pub(crate) inline: bool,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(FnDefnRaw);

impl<'script> FnDefnRaw<'script> {
    pub(crate) fn doc(&self) -> FnDoc {
        FnDoc {
            name: self.name.to_string(),
            args: self.args.iter().map(ToString::to_string).collect(),
            open: self.open,
            doc: self
                .doc
                .clone()
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        }
    }
}

impl<'script> Upable<'script> for FnDefnRaw<'script> {
    type Target = FnDefn<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let can_emit = helper.can_emit;
        let mut aggrs = Vec::new();
        // register documentation
        helper.docs.fns.push(self.doc());

        let mut locals: HashMap<_, _> = self
            .args
            .iter()
            .enumerate()
            .map(|(i, a)| (a.to_string(), i))
            .collect();

        helper.can_emit = false;
        helper.is_open = self.open;
        helper.fn_argc = self.args.len();

        helper.swap(&mut aggrs, &mut locals);
        helper.possible_leaf = true;
        let body = self.body.up(helper)?;
        helper.possible_leaf = false;
        helper.swap(&mut aggrs, &mut locals);
        helper.can_emit = can_emit;
        Ok(FnDefn {
            mid: self.mid.box_with_name(&self.name.id),
            name: self.name.id.to_string(),
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
    Match(MatchFnDefnRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Normal(FnDefnRaw<'script>),
}

impl<'script> Upable<'script> for AnyFnRaw<'script> {
    type Target = FnDefn<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match self {
            AnyFnRaw::Normal(f) => f.up(helper),
            AnyFnRaw::Match(f) => f.up(helper),
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MatchFnDefnRaw<'script> {
    pub(crate) name: IdentRaw<'script>,
    pub(crate) args: Vec<IdentRaw<'script>>,
    pub(crate) cases: Vec<PredicateClauseRaw<'script, ExprRaw<'script>>>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
    pub(crate) open: bool,
    pub(crate) inline: bool,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(MatchFnDefnRaw);

impl<'script> MatchFnDefnRaw<'script> {
    pub(crate) fn doc(&self) -> FnDoc {
        FnDoc {
            name: self.name.to_string(),
            args: self.args.iter().map(ToString::to_string).collect(),
            open: self.open,
            doc: self
                .doc
                .clone()
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        }
    }
}

impl<'script> Upable<'script> for MatchFnDefnRaw<'script> {
    type Target = FnDefn<'script>;
    fn up<'registry>(mut self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let can_emit = helper.can_emit;
        let mut aggrs = Vec::new();
        let mut locals = HashMap::new();

        helper.docs.fns.push(self.doc());

        for (i, a) in self.args.iter().enumerate() {
            locals.insert(a.to_string(), i);
        }

        helper.is_open = self.open;
        helper.fn_argc = self.args.len();

        helper.can_emit = false;

        helper.swap(&mut aggrs, &mut locals);

        let target = self
            .args
            .iter()
            .map(|root| {
                ImutExprRaw::Path(PathRaw::Local(LocalPathRaw {
                    mid: root.mid.clone(),
                    root: root.clone(),
                    segments: vec![],
                }))
            })
            .collect();

        let mut patterns = Vec::new();

        std::mem::swap(&mut self.cases, &mut patterns);

        let patterns = patterns
            .into_iter()
            .map(|mut c: PredicateClauseRaw<_>| {
                if c.pattern != PatternRaw::DoNotCare {
                    let args = self.args.iter().enumerate();
                    let mut exprs: Vec<_> = args
                        .map(|(i, root)| {
                            let mid = c.mid.clone();
                            let root_mid = root.mid.clone();
                            ExprRaw::Assign(Box::new(AssignRaw {
                                mid,
                                path: PathRaw::Local(LocalPathRaw {
                                    mid: root_mid.clone(),
                                    root: root.clone(),
                                    segments: vec![],
                                }),
                                expr: ExprRaw::Imut(ImutExprRaw::Path(PathRaw::Local(
                                    LocalPathRaw {
                                        mid: root_mid.clone(),
                                        root: IdentRaw {
                                            mid: root_mid.clone(),
                                            id: FN_RES_NAME.into(),
                                        },
                                        segments: vec![SegmentRaw::from_usize(i, root_mid)],
                                    },
                                ))),
                            }))
                        })
                        .collect();
                    exprs.append(&mut c.exprs);
                    c.exprs = exprs;
                }
                c
            })
            .collect();

        let body = ExprRaw::MatchExpr(Box::new(MatchRaw {
            mid: self.mid.clone(),
            target: ImutExprRaw::List(Box::new(ListRaw {
                mid: self.mid.clone(),
                exprs: target,
            })),
            patterns,
        }));
        helper.possible_leaf = true;
        let body = body.up(helper)?;
        helper.possible_leaf = false;
        let body = vec![body];

        helper.swap(&mut aggrs, &mut locals);
        helper.can_emit = can_emit;
        Ok(FnDefn {
            mid: self.mid.box_with_name(&self.name.id),
            name: self.name.id.to_string(),
            args: self.args.up(helper)?,
            body,
            locals: locals.len(),
            open: self.open,
            inline: self.inline,
        })
    }
}

/// A raw expression
pub trait ExpressionRaw<'script>:
    Clone + std::fmt::Debug + PartialEq + Serialize + Upable<'script>
where
    <Self as Upable<'script>>::Target: Expression + 'script,
{
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
    Match(Box<MatchRaw<'script, Self>>),
    /// we're forced to make this pub because of lalrpop
    Comprehension(Box<ComprehensionRaw<'script, Self>>),
    /// we're forced to make this pub because of lalrpop
    Path(PathRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Binary(Box<BinExprRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    BinaryBoolean(Box<BooleanBinExprRaw<'script>>),
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
        mid: Box<NodeMeta>,
    },
    /// we're forced to make this pub because of lalrpop
    String(StringLitRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Recur(RecurRaw<'script>),
    /// bytes
    Bytes(BytesRaw<'script>),
}
impl<'script> ExpressionRaw<'script> for ImutExprRaw<'script> {}

impl<'script> Upable<'script> for ImutExprRaw<'script> {
    type Target = ImutExpr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let was_leaf = helper.possible_leaf;
        helper.possible_leaf = false;
        let r = match self {
            ImutExprRaw::Recur(r) => {
                helper.possible_leaf = was_leaf;
                ImutExpr::Recur(r.up(helper)?)
            }
            ImutExprRaw::Binary(b) => ImutExpr::Binary(Box::new(b.up(helper)?)),
            ImutExprRaw::BinaryBoolean(b) => ImutExpr::BinaryBoolean(Box::new(b.up(helper)?)),
            ImutExprRaw::Unary(u) => ImutExpr::Unary(Box::new(u.up(helper)?)),
            ImutExprRaw::String(s) => ImutExpr::String(s.up(helper)?),
            ImutExprRaw::Record(r) => ImutExpr::Record(r.up(helper)?),
            ImutExprRaw::List(l) => ImutExpr::List(l.up(helper)?),
            ImutExprRaw::Patch(p) => ImutExpr::Patch(Box::new(p.up(helper)?)),
            ImutExprRaw::Merge(m) => ImutExpr::Merge(Box::new(m.up(helper)?)),
            ImutExprRaw::Present { path, mid } => ImutExpr::Present {
                path: path.up(helper)?,
                mid,
            },
            ImutExprRaw::Path(p) => match p.up(helper)? {
                Path::Local(LocalPath {
                    mid,
                    idx,
                    ref segments,
                }) if segments.is_empty() => ImutExpr::Local { mid, idx },
                p => ImutExpr::Path(p),
            },
            ImutExprRaw::Literal(l) => ImutExpr::Literal(l.up(helper)?),
            ImutExprRaw::Invoke(i) => {
                if i.is_aggregate(helper) {
                    ImutExpr::InvokeAggr(i.into_aggregate().up(helper)?)
                } else {
                    let i = i.up(helper)?;
                    if i.can_inline() {
                        i.inline()?
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
            ImutExprRaw::Match(m) => {
                helper.possible_leaf = was_leaf;
                ImutExpr::Match(Box::new(m.up(helper)?))
            }
            ImutExprRaw::Comprehension(c) => ImutExpr::Comprehension(Box::new(c.up(helper)?)),
            ImutExprRaw::Bytes(b) => ImutExpr::Bytes(b.up(helper)?),
        };
        helper.possible_leaf = was_leaf;
        Ok(r)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RecurRaw<'script> {
    pub exprs: ImutExprsRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(RecurRaw);

impl<'script> Upable<'script> for RecurRaw<'script> {
    type Target = Recur<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let was_leaf = helper.possible_leaf;
        helper.possible_leaf = false;
        if !was_leaf {
            return Err(
                ErrorKind::InvalidRecur(self.extent().expand_lines(2), self.extent()).into(),
            );
        };
        let argc = helper.fn_argc;
        let arglen = self.exprs.len();
        if (helper.is_open && argc < arglen) || (!helper.is_open && argc != arglen) {
            let m = format!("Wrong number of arguments {argc} != {arglen}");
            return err_generic(&self, &self, &m);
        }
        let exprs = self.exprs.up(helper)?.into_iter().collect();
        helper.possible_leaf = was_leaf;

        Ok(Recur {
            mid: self.mid,
            argc: helper.fn_argc,
            open: helper.is_open,
            exprs,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EmitExprRaw<'script> {
    pub expr: ImutExprRaw<'script>,
    pub port: Option<ImutExprRaw<'script>>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(EmitExprRaw);

impl<'script> Upable<'script> for EmitExprRaw<'script> {
    type Target = EmitExpr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        if !helper.can_emit {
            return Err(
                ErrorKind::InvalidEmit(self.extent().expand_lines(2), self.extent()).into(),
            );
        }
        Ok(EmitExpr {
            mid: self.mid,
            expr: self.expr.up(helper)?,
            port: self.port.up(helper)?,
        })
    }
}
/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AssignRaw<'script> {
    pub(crate) path: PathRaw<'script>,
    pub(crate) expr: ExprRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(AssignRaw);

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PredicateClauseRaw<'script, Ex>
where
    <Ex as Upable<'script>>::Target: Expression + 'script,
    Ex: ExpressionRaw<'script> + 'script,
{
    pub(crate) pattern: PatternRaw<'script>,
    pub(crate) guard: Option<ImutExprRaw<'script>>,
    pub(crate) exprs: Vec<Ex>,
    pub(crate) mid: Box<NodeMeta>,
}

impl<'script, Ex> Upable<'script> for PredicateClauseRaw<'script, Ex>
where
    <Ex as Upable<'script>>::Target: Expression + 'script,
    Ex: ExpressionRaw<'script> + 'script,
{
    type Target = PredicateClause<'script, <Ex as Upable<'script>>::Target>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let was_leaf = helper.possible_leaf;
        helper.possible_leaf = false;
        // We run the pattern first as this might reserve a local shadow
        let pattern = self.pattern.up(helper)?;
        let guard = self.guard.up(helper)?;
        helper.possible_leaf = was_leaf;
        let mut exprs = self.exprs.up(helper)?;

        if let Pattern::Assign(AssignPattern { idx, .. }) = &pattern {
            // If we are in an assign pattern we'd have created
            // a shadow variable, this needs to be undone at the end
            helper.end_shadow_var();
            if let Some(expr) = exprs.last_mut() {
                expr.replace_last_shadow_use(*idx);
            };
        }

        let span = self.mid.range;
        let last_expr = exprs
            .pop()
            .ok_or_else(|| error_missing_effector(&span, &span))?;

        Ok(PredicateClause {
            mid: self.mid,
            pattern,
            guard,
            exprs,
            last_expr,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PatchRaw<'script> {
    pub(crate) target: ImutExprRaw<'script>,
    pub(crate) operations: PatchOperationsRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}

impl<'script> Upable<'script> for PatchRaw<'script> {
    type Target = Patch<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let operations = self.operations.up(helper)?;

        Ok(Patch {
            mid: self.mid,
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
        ident: StringLitRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        mid: Box<NodeMeta>,
    },
    /// we're forced to make this pub because of lalrpop
    Upsert {
        /// we're forced to make this pub because of lalrpop
        ident: StringLitRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        mid: Box<NodeMeta>,
    },
    /// we're forced to make this pub because of lalrpop
    Update {
        /// we're forced to make this pub because of lalrpop
        ident: StringLitRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        mid: Box<NodeMeta>,
    },
    /// we're forced to make this pub because of lalrpop
    Erase {
        /// we're forced to make this pub because of lalrpop
        ident: StringLitRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        mid: Box<NodeMeta>,
    },
    /// we're forced to make this pub because of lalrpop
    Copy {
        /// we're forced to make this pub because of lalrpop
        from: StringLitRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        to: StringLitRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        mid: Box<NodeMeta>,
    },
    /// we're forced to make this pub because of lalrpop
    Move {
        /// we're forced to make this pub because of lalrpop
        from: StringLitRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        to: StringLitRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        mid: Box<NodeMeta>,
    },
    /// we're forced to make this pub because of lalrpop
    Merge {
        /// we're forced to make this pub because of lalrpop
        ident: StringLitRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        mid: Box<NodeMeta>,
    },
    /// we're forced to make this pub because of lalrpop
    MergeRecord {
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        mid: Box<NodeMeta>,
    },
    /// we're forced to make this pub because of lalrpop
    Default {
        /// we're forced to make this pub because of lalrpop
        ident: StringLitRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        mid: Box<NodeMeta>,
    },
    /// we're forced to make this pub because of lalrpop
    DefaultRecord {
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        mid: Box<NodeMeta>,
    },
}

impl<'script> Upable<'script> for PatchOperationRaw<'script> {
    type Target = PatchOperation<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use PatchOperationRaw::{Copy, Erase, Insert, Merge, MergeRecord, Move, Update, Upsert};
        Ok(match self {
            Insert { mid, ident, expr } => PatchOperation::Insert {
                mid,
                ident: ident.up(helper)?,
                expr: expr.up(helper)?,
            },
            Upsert { mid, ident, expr } => PatchOperation::Upsert {
                mid,
                ident: ident.up(helper)?,
                expr: expr.up(helper)?,
            },
            Update { mid, ident, expr } => PatchOperation::Update {
                mid,
                ident: ident.up(helper)?,
                expr: expr.up(helper)?,
            },
            Erase { mid, ident } => PatchOperation::Erase {
                mid,
                ident: ident.up(helper)?,
            },
            Copy { mid, from, to } => PatchOperation::Copy {
                mid,
                from: from.up(helper)?,
                to: to.up(helper)?,
            },
            Move { mid, from, to } => PatchOperation::Move {
                mid,
                from: from.up(helper)?,
                to: to.up(helper)?,
            },
            Merge {
                mid, expr, ident, ..
            } => PatchOperation::Merge {
                mid,
                ident: ident.up(helper)?,
                expr: expr.up(helper)?,
            },
            MergeRecord { mid, expr } => PatchOperation::MergeRecord {
                mid,
                expr: expr.up(helper)?,
            },
            PatchOperationRaw::Default { mid, ident, expr } => PatchOperation::Default {
                ident: ident.up(helper)?,
                mid,
                expr: expr.up(helper)?,
            },
            PatchOperationRaw::DefaultRecord { mid, expr } => PatchOperation::DefaultRecord {
                expr: expr.up(helper)?,
                mid,
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MergeRaw<'script> {
    pub target: ImutExprRaw<'script>,
    pub expr: ImutExprRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}

impl<'script> Upable<'script> for MergeRaw<'script> {
    type Target = Merge<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Merge {
            mid: self.mid,
            target: self.target.up(helper)?,
            expr: self.expr.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ComprehensionRaw<'script, Ex>
where
    <Ex as Upable<'script>>::Target: Expression + 'script,
    Ex: ExpressionRaw<'script> + 'script,
{
    pub target: ImutExprRaw<'script>,
    pub cases: ComprehensionCasesRaw<'script, Ex>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr_exraw!(ComprehensionRaw);

impl<'script, Ex> Upable<'script> for ComprehensionRaw<'script, Ex>
where
    <Ex as Upable<'script>>::Target: Expression + 'script,
    Ex: ExpressionRaw<'script> + 'script,
{
    type Target = Comprehension<'script, Ex::Target>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // We compute the target before shadowing the key and value

        let target = self.target.up(helper)?;

        // We know that each case will have a key and a value as a shadowed
        // variable so we reserve two ahead of time so we know what id's those
        // will be.
        let (key_id, val_id) = helper.reserve_2_shadow();

        let cases = self.cases.up(helper)?;

        Ok(Comprehension {
            mid: self.mid,
            target,
            cases,
            key_id,
            val_id,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutComprehensionRaw<'script> {
    pub target: ImutExprRaw<'script>,
    pub cases: ImutComprehensionCasesRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ComprehensionCaseRaw<'script, Ex>
where
    <Ex as Upable<'script>>::Target: Expression + 'script,
    Ex: ExpressionRaw<'script> + 'script,
{
    pub(crate) key_name: Cow<'script, str>,
    pub(crate) value_name: Cow<'script, str>,
    pub(crate) guard: Option<ImutExprRaw<'script>>,
    pub(crate) exprs: Vec<Ex>,
    pub(crate) mid: Box<NodeMeta>,
}

impl<'script, Ex> Upable<'script> for ComprehensionCaseRaw<'script, Ex>
where
    <Ex as Upable<'script>>::Target: Expression + 'script,
    Ex: ExpressionRaw<'script> + 'script,
{
    type Target = ComprehensionCase<'script, Ex::Target>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // regiter key and value as shadowed variables
        let key_idx = helper.register_shadow_var(&self.key_name);
        let val_idx = helper.register_shadow_var(&self.value_name);

        let guard = self.guard.up(helper)?;
        let mut exprs = self.exprs.up(helper)?;

        if let Some(expr) = exprs.last_mut() {
            expr.replace_last_shadow_use(key_idx);
            expr.replace_last_shadow_use(val_idx);
        };

        // unregister them again
        helper.end_shadow_var();
        helper.end_shadow_var();
        let span = self.mid.range;
        let last_expr = exprs
            .pop()
            .ok_or_else(|| error_missing_effector(&span, &span))?;
        Ok(ComprehensionCase {
            mid: self.mid,
            key_name: self.key_name,
            value_name: self.value_name,
            guard,
            exprs,
            last_expr,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutComprehensionCaseRaw<'script> {
    pub(crate) key_name: Cow<'script, str>,
    pub(crate) value_name: Cow<'script, str>,
    pub(crate) guard: Option<ImutExprRaw<'script>>,
    pub(crate) exprs: ImutExprsRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
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
    /// a test expression
    Extract(TestExprRaw),
    /// we're forced to make this pub because of lalrpop
    DoNotCare,
}

impl<'script> Upable<'script> for PatternRaw<'script> {
    type Target = Pattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use PatternRaw::{Array, Assign, DoNotCare, Expr, Extract, Record, Tuple};
        Ok(match self {
            //Predicate(pp) => Pattern::Predicate(pp.up(helper)?),
            Record(rp) => Pattern::Record(rp.up(helper)?),
            Array(ap) => Pattern::Array(ap.up(helper)?),
            Tuple(tp) => Pattern::Tuple(tp.up(helper)?),
            Expr(expr) => Pattern::Expr(expr.up(helper)?),
            Assign(ap) => Pattern::Assign(ap.up(helper)?),
            Extract(e) => Pattern::Extract(Box::new(e.up(helper)?)),
            DoNotCare => Pattern::DoNotCare,
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
    TuplePatternEq {
        /// we're forced to make this pub because of lalrpop
        lhs: Cow<'script, str>,
        /// we're forced to make this pub because of lalrpop
        pattern: TuplePatternRaw<'script>,
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
        use PredicatePatternRaw::{
            ArrayPatternEq, Bin, FieldAbsent, FieldPresent, RecordPatternEq, TildeEq,
            TuplePatternEq,
        };
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
            TuplePatternEq { lhs, pattern } => PredicatePattern::TuplePatternEq {
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
    pub(crate) fields: PatternFieldsRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
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
                helper.warn(
                    self.mid.range.expand_lines(2),
                    self.mid.range,
                    &format!("The field {} is checked with both present and another extractor, this is redundant as extractors imply presence. It may also overwrite the result of the extractor.", present),
                    warning::Class::Behaviour
                );
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
                helper.warn(
                    self.mid.range.expand_lines(2),
                    self.mid.range,
                    &format!("The field {} is checked with both absence and another extractor, this test can never be true.", absent),
                    warning::Class::Behaviour
                );
            }
        }

        Ok(RecordPattern {
            mid: self.mid,
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
        use ArrayPredicatePatternRaw::{Expr, Ignore, Record, Tilde};
        Ok(match self {
            Expr(expr) => ArrayPredicatePattern::Expr(expr.up(helper)?),
            Tilde(te) => ArrayPredicatePattern::Tilde(Box::new(te.up(helper)?)),
            Record(rp) => ArrayPredicatePattern::Record(rp.up(helper)?),
            Ignore => ArrayPredicatePattern::Ignore,
            //Array(ap) => ArrayPredicatePattern::Array(ap),
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ArrayPatternRaw<'script> {
    pub(crate) exprs: ArrayPredicatePatternsRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}

impl<'script> Upable<'script> for ArrayPatternRaw<'script> {
    type Target = ArrayPattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let exprs = self.exprs.up(helper)?;
        Ok(ArrayPattern {
            mid: self.mid,
            exprs,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TuplePatternRaw<'script> {
    pub(crate) exprs: ArrayPredicatePatternsRaw<'script>,
    pub(crate) open: bool,
    pub(crate) mid: Box<NodeMeta>,
}

impl<'script> Upable<'script> for TuplePatternRaw<'script> {
    type Target = TuplePattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let exprs = self.exprs.up(helper)?;
        Ok(TuplePattern {
            mid: self.mid,
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
    /// Special reserved path
    Reserved(ReservedPathRaw<'script>),
    /// Special reserved path an expression path
    Expr(ExprPathRaw<'script>),
}

impl<'script> Upable<'script> for PathRaw<'script> {
    type Target = Path<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use PathRaw::{Const, Event, Expr, Local, Meta, Reserved, State};
        Ok(match self {
            Local(p) => {
                // Handle local constants
                if helper.is_const_path(&p) {
                    let c: crate::ast::Const = helper
                        .get(&NodeId::from(&p.root))?
                        .ok_or("invalid constant")?;
                    let mid = p.mid.box_with_name(&p.root);
                    let var = helper.register_shadow_from_mid(&mid);
                    Path::Expr(ExprPath {
                        expr: Box::new(ImutExpr::literal(c.mid, c.value)),
                        segments: p.segments.up(helper)?,
                        var,
                        mid,
                    })
                } else {
                    let p = p.up(helper)?;
                    Path::Local(p)
                }
            }
            Const(p) => Path::Expr(p.up(helper)?),
            Event(p) => Path::Event(p.up(helper)?),
            State(p) => Path::State(p.up(helper)?),
            Meta(p) => Path::Meta(p.up(helper)?),
            Expr(p) => Path::Expr(p.up(helper)?),
            Reserved(p) => Path::Reserved(p.up(helper)?),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// we're forced to make this pub because of lalrpop
pub struct SegmentRangeRaw<'script> {
    pub(crate) range_start: ImutExprRaw<'script>,
    pub(crate) range_end: ImutExprRaw<'script>,
}

impl<'script> Upable<'script> for SegmentRangeRaw<'script> {
    type Target = Segment<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let SegmentRangeRaw {
            range_start,
            range_end,
        } = self;
        let start = range_start.up(helper)?;
        let end = range_end.up(helper)?;
        let mid = NodeMeta::new_box(start.s(), end.e());
        Ok(Segment::RangeExpr {
            start: Box::new(start),
            end: Box::new(end),
            mid,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SegmentElementRaw<'script> {
    pub(crate) expr: ImutExprRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}

impl<'script> Upable<'script> for SegmentElementRaw<'script> {
    type Target = Segment<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let SegmentElementRaw { expr, mut mid } = self;
        let expr = expr.up(helper)?;
        let r = expr.extent();
        match expr {
            ImutExpr::Literal(l) => match ImutExpr::Literal(l).try_into_value(helper)? {
                Value::String(id) => {
                    mid.set_name(&id);
                    Ok(Segment::Id {
                        key: KnownKey::from(id.clone()),
                        mid,
                    })
                }
                other => {
                    if let Some(idx) = other.as_usize() {
                        Ok(Segment::Idx { idx, mid })
                    } else {
                        let exp = vec![ValueType::I64, ValueType::String];
                        let o = r.expand_lines(2);
                        Err(ErrorKind::TypeConflict(o, r, other.value_type(), exp).into())
                    }
                }
            },
            expr => Ok(Segment::Element { mid, expr }),
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum SegmentRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Element(Box<SegmentElementRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Range(Box<SegmentRangeRaw<'script>>),
}

impl<'script> Upable<'script> for SegmentRaw<'script> {
    type Target = Segment<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match self {
            SegmentRaw::Element(e) => e.up(helper),
            SegmentRaw::Range(r) => r.up(helper),
        }
    }
}

impl<'script> From<IdentRaw<'script>> for SegmentRaw<'script> {
    fn from(id: IdentRaw<'script>) -> Self {
        SegmentRaw::Element(Box::new(SegmentElementRaw {
            mid: id.mid.clone(),
            expr: ImutExprRaw::Literal(LiteralRaw {
                mid: id.mid,
                value: Value::from(id.id),
            }),
        }))
    }
}

impl<'script> SegmentRaw<'script> {
    pub fn from_usize(id: usize, mid: Box<NodeMeta>) -> Self {
        SegmentRaw::Element(Box::new(SegmentElementRaw {
            mid: mid.clone(),
            expr: ImutExprRaw::Literal(LiteralRaw {
                mid,
                value: Value::from(id),
            }),
        }))
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
    pub(crate) root: IdentRaw<'script>,
    pub(crate) segments: SegmentsRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(ConstPathRaw);

impl<'script> Upable<'script> for ConstPathRaw<'script> {
    type Target = ExprPath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let segments = self.segments.up(helper)?;
        let id = self.root.up(helper)?;

        let mid = self.mid.box_with_name(&id);
        let node_id = NodeId {
            module: self.module.iter().map(ToString::to_string).collect(),
            id: id.to_string(),
            mid: mid.clone(),
        };

        let c: Const = helper.get(&node_id)?.ok_or_else(|| {
            let msg = format!("The constant {node_id} (absolute path) is not defined.",);
            error_generic(&mid.range, &mid.range, &msg)
        })?;
        let var = helper.register_shadow_from_mid(&mid);

        Ok(ExprPath {
            expr: Literal::boxed_expr(mid.clone(), c.value),
            segments,
            var,
            mid,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ExprPathRaw<'script> {
    pub(crate) expr: Box<ImutExprRaw<'script>>,
    pub(crate) segments: SegmentsRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}
impl<'script> Upable<'script> for ExprPathRaw<'script> {
    type Target = ExprPath<'script>;

    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let var = helper.register_shadow_from_mid(&self.mid);
        let segments = self.segments.up(helper)?;
        let expr = Box::new(self.expr.up(helper)?);
        helper.end_shadow_var();

        Ok(ExprPath {
            var,
            segments,
            expr,
            mid: self.mid,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ReservedPathRaw<'script> {
    Args {
        segments: SegmentsRaw<'script>,
        mid: Box<NodeMeta>,
    },
    Window {
        segments: SegmentsRaw<'script>,
        mid: Box<NodeMeta>,
    },
    Group {
        segments: SegmentsRaw<'script>,
        mid: Box<NodeMeta>,
    },
}

impl<'script> Upable<'script> for ReservedPathRaw<'script> {
    type Target = ReservedPath<'script>;

    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let r = match self {
            ReservedPathRaw::Args { mid, segments } => ReservedPath::Args {
                mid: mid.box_with_name(&"args"),
                segments: segments.up(helper)?,
            },
            ReservedPathRaw::Window { mid, segments } => ReservedPath::Window {
                mid: mid.box_with_name(&"window"),
                segments: segments.up(helper)?,
            },
            ReservedPathRaw::Group { mid, segments } => ReservedPath::Group {
                mid: mid.box_with_name(&"group"),
                segments: segments.up(helper)?,
            },
        };
        Ok(r)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct LocalPathRaw<'script> {
    pub(crate) root: IdentRaw<'script>,
    pub(crate) segments: SegmentsRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(LocalPathRaw);

impl<'script> Upable<'script> for LocalPathRaw<'script> {
    type Target = LocalPath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let id = self.root.up(helper)?;
        let segments = self.segments.up(helper)?;

        Ok(LocalPath {
            idx: helper.var_id(&id.id),
            mid: self.mid.box_with_name(&id),
            segments,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MetadataPathRaw<'script> {
    pub(crate) segments: SegmentsRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}
impl<'script> Upable<'script> for MetadataPathRaw<'script> {
    type Target = MetadataPath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let segments = self.segments.up(helper)?;
        Ok(MetadataPath {
            mid: self.mid,
            segments,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EventPathRaw<'script> {
    pub(crate) segments: SegmentsRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}
impl<'script> Upable<'script> for EventPathRaw<'script> {
    type Target = EventPath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let segments = self.segments.up(helper)?;
        Ok(EventPath {
            mid: self.mid,
            segments,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StatePathRaw<'script> {
    pub segments: SegmentsRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}
impl<'script> Upable<'script> for StatePathRaw<'script> {
    type Target = StatePath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let segments = self.segments.up(helper)?;
        Ok(StatePath {
            mid: self.mid,
            segments,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BinExprRaw<'script> {
    pub(crate) kind: BinOpKind,
    pub(crate) lhs: ImutExprRaw<'script>,
    pub(crate) rhs: ImutExprRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}

impl<'script> Upable<'script> for BinExprRaw<'script> {
    type Target = BinExpr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(BinExpr {
            mid: self.mid,
            kind: self.kind,
            lhs: self.lhs.up(helper)?,
            rhs: self.rhs.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BooleanBinExprRaw<'script> {
    pub(crate) kind: BooleanBinOpKind,
    pub(crate) lhs: ImutExprRaw<'script>,
    pub(crate) rhs: ImutExprRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}

impl<'script> Upable<'script> for BooleanBinExprRaw<'script> {
    type Target = BooleanBinExpr<'script>;

    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(BooleanBinExpr {
            mid: self.mid,
            kind: self.kind,
            lhs: self.lhs.up(helper)?,
            rhs: self.rhs.up(helper)?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct UnaryExprRaw<'script> {
    pub(crate) kind: UnaryOpKind,
    pub(crate) expr: ImutExprRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}

impl<'script> Upable<'script> for UnaryExprRaw<'script> {
    type Target = UnaryExpr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(UnaryExpr {
            mid: self.mid,
            kind: self.kind,
            expr: self.expr.up(helper)?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MatchRaw<'script, Ex>
where
    <Ex as Upable<'script>>::Target: Expression + 'script,
    Ex: ExpressionRaw<'script> + 'script,
{
    pub(crate) target: ImutExprRaw<'script>,
    pub(crate) patterns: PredicatesRaw<'script, Ex>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr_exraw!(MatchRaw);

// Sort clauses, move up cheaper clauses as long as they're exclusive
#[allow(
    // No clippy, we really mean j.exclusive(k) || k.exclusive(j)...
    clippy::suspicious_operation_groupings,
    // also what is wrong with you clippy ...
    clippy::blocks_in_if_conditions
)]
fn sort_clauses<Ex: Expression>(patterns: &mut [PredicateClause<Ex>]) {
    for i in (0..patterns.len()).rev() {
        for j in (1..=i).rev() {
            if patterns
                .get(j)
                .and_then(|jv| Some((jv, patterns.get(j - 1)?)))
                .map(|(j, k)| {
                    j.cost() <= k.cost() && (j.is_exclusive_to(k) || k.is_exclusive_to(j))
                })
                .unwrap_or_default()
            {
                patterns.swap(j, j - 1);
            }
        }
    }
}

const NO_DFLT: &str = "This match expression has no default clause, if the other clauses do not cover all possibilities this will lead to events being discarded with runtime errors.";
const MULT_DFLT: &str = "Any but the first `case _` statement are unreachable and will be ignored.";

// Checks for warnings in match arms
fn check_patterns<Ex>(patterns: &[PredicateClause<Ex>], mid: &NodeMeta, helper: &mut Helper)
where
    Ex: Expression,
{
    let mut seen_default = false;

    for p in patterns
        .iter()
        .filter(|p| p.pattern.is_default() && p.guard.is_none())
    {
        if seen_default {
            helper.warn(mid.range, p.extent(), &MULT_DFLT, warning::Class::Behaviour);
        } else {
            seen_default = true;
        }
    }

    if !seen_default {
        helper.warn_with_scope(mid.range, &NO_DFLT, warning::Class::Behaviour);
    };
}

impl<'script, Ex> Upable<'script> for MatchRaw<'script, Ex>
where
    <Ex as Upable<'script>>::Target: Expression + 'script,
    Ex: ExpressionRaw<'script> + 'script,
{
    type Target = Match<'script, Ex::Target>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let mut patterns: Vec<PredicateClause<_>> = self
            .patterns
            .into_iter()
            .map(|v| v.up(helper))
            .collect::<Result<_>>()?;

        check_patterns(&patterns, &self.mid, helper);

        // If the last statement is a global default we can simply remove it
        let default = if let Some(PredicateClause {
            pattern: Pattern::DoNotCare,
            guard: None,
            exprs,
            last_expr,
            ..
        }) = patterns.last_mut()
        {
            let mut es = Vec::new();
            let mut last = Ex::Target::null_lit(self.mid.clone());
            std::mem::swap(exprs, &mut es);
            std::mem::swap(last_expr, &mut last);
            if es.is_empty() {
                if last.is_null_lit() {
                    DefaultCase::Null
                } else {
                    DefaultCase::One(last)
                }
            } else {
                DefaultCase::Many {
                    exprs: es,
                    last_expr: Box::new(last),
                }
            }
        } else {
            DefaultCase::None
        };

        if default != DefaultCase::None {
            patterns.pop();
        }

        // This shortcuts for if / else style matches
        if patterns.len() == 1 {
            let patterns = patterns.into_iter().map(ClauseGroup::simple).collect();
            return Ok(Match {
                mid: self.mid,
                target: self.target.up(helper)?,
                patterns,
                default,
            });
        }
        sort_clauses(&mut patterns);
        let mut groups = Vec::new();
        let mut group: Vec<PredicateClause<_>> = Vec::new();

        for p in patterns {
            if group
                .iter()
                .all(|g| p.is_exclusive_to(g) || g.is_exclusive_to(&p))
            {
                group.push(p);
            } else {
                group.sort_by_key(Costly::cost);

                let mut g = ClauseGroup::Simple {
                    patterns: group,
                    precondition: None,
                };
                g.optimize(0);
                groups.push(g);
                group = vec![p];
            }
        }
        group.sort_by_key(Costly::cost);
        let mut g = ClauseGroup::Simple {
            patterns: group,
            precondition: None,
        };
        g.optimize(0);
        groups.push(g);

        let mut patterns: Vec<ClauseGroup<_>> = Vec::new();

        for g in groups {
            #[allow(clippy::option_if_let_else)]
            if let Some(last) = patterns.last_mut() {
                if last.combinable(&g) {
                    last.combine(g);
                } else {
                    patterns.push(g);
                }
            } else {
                patterns.push(g);
            }
        }
        Ok(Match {
            mid: self.mid,
            target: self.target.up(helper)?,
            patterns,
            default,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct InvokeRaw<'script> {
    pub(crate) module: Vec<String>,
    pub(crate) fun: String,
    pub(crate) args: ImutExprsRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(InvokeRaw);

impl<'script> Upable<'script> for InvokeRaw<'script> {
    type Target = Invoke<'script>;

    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let node_id = NodeId {
            id: self.fun,
            module: self.module,
            mid: self.mid.clone(),
        };
        let inner = self.mid.range;
        let outer = inner.expand_lines(3);
        if node_id.module.first() == Some(&String::from("core")) && node_id.module.len() == 2 {
            // we know a second module exists
            let module = node_id.module.get(1).cloned().unwrap_or_default();

            let invocable = helper
                .reg
                .find(&module, &node_id.id)
                .map_err(|e| e.into_err(&outer, &inner, Some(helper.reg)))?;
            let args = self.args.up(helper)?.into_iter().collect();
            let mf = node_id.fqn();
            if let Some((class, warning)) = invocable.warning() {
                helper.warn(outer, inner, &warning, class);
            }
            Ok(Invoke {
                mid: self.mid.box_with_name(&mf),
                node_id,
                invocable: Invocable::Intrinsic(invocable.clone()),
                args,
            })
        } else {
            // Absolute locability from without a set of nested modules

            // of the form: [mod, mod1, name] - where the list of idents is effectively a fully qualified resource name
            if let Some(f) = helper.get::<FnDefn>(&node_id)? {
                if let [Expr::Imut(
                    ImutExpr::Invoke(Invoke {
                        invocable: Invocable::Intrinsic(invocable),
                        ..
                    })
                    | ImutExpr::Invoke1(Invoke {
                        invocable: Invocable::Intrinsic(invocable),
                        ..
                    })
                    | ImutExpr::Invoke2(Invoke {
                        invocable: Invocable::Intrinsic(invocable),
                        ..
                    })
                    | ImutExpr::Invoke3(Invoke {
                        invocable: Invocable::Intrinsic(invocable),
                        ..
                    }),
                )] = f.body.as_slice()
                {
                    if let Some((class, warning)) = invocable.warning() {
                        helper.warn(outer, inner, &warning, class);
                    }
                }

                let invocable = Invocable::Tremor(f.into());

                let args = self.args.up(helper)?.into_iter().collect();
                Ok(Invoke {
                    mid: self.mid.box_with_name(&node_id.fqn()),
                    node_id,
                    invocable,
                    args,
                })
            } else {
                Err(
                    ErrorKind::MissingFunction(outer, inner, node_id.module, node_id.id, None)
                        .into(),
                )
            }
        }
    }
}

impl<'script> InvokeRaw<'script> {
    fn is_aggregate<'registry>(&self, helper: &mut Helper<'script, 'registry>) -> bool {
        if self.module.first() == Some(&String::from("aggr")) && self.module.len() == 2 {
            let module = self.module.get(1).cloned().unwrap_or_default();
            helper.aggr_reg.find(&module, &self.fun).is_ok()
        } else {
            false
        }
    }

    fn into_aggregate(self) -> InvokeAggrRaw<'script> {
        let module = self.module.get(1).cloned().unwrap_or_default();
        InvokeAggrRaw {
            mid: self.mid,
            module,
            fun: self.fun,
            args: self.args,
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct InvokeAggrRaw<'script> {
    pub(crate) module: String,
    pub(crate) fun: String,
    pub(crate) args: ImutExprsRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(InvokeAggrRaw);

impl<'script> Upable<'script> for InvokeAggrRaw<'script> {
    type Target = InvokeAggr;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        if helper.is_in_aggr {
            return Err(ErrorKind::AggrInAggr(self.extent(), self.extent().expand_lines(2)).into());
        };
        helper.is_in_aggr = true;
        let invocable = helper
            .aggr_reg
            .find(&self.module, &self.fun)
            .map_err(|e| e.into_err(&self, &self, Some(helper.reg)))?
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
        if let Some((class, warning)) = invocable.warning() {
            helper.warn_with_scope(self.extent(), &warning, class);
        }
        let aggr_id = helper.aggregates.len();
        let args = self.args.up(helper)?.into_iter().collect();
        let mf = format!("{}::{}", self.module, self.fun);
        let mid = self.mid.box_with_name(&mf);

        helper.aggregates.push(InvokeAggrFn {
            mid: mid.clone(),
            invocable,
            args,
            module: self.module.clone(),
            fun: self.fun.clone(),
        });
        helper.is_in_aggr = false;

        Ok(InvokeAggr {
            mid,
            module: self.module,
            fun: self.fun,
            aggr_id,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize, Eq)]
pub struct TestExprRaw {
    pub(crate) id: String,
    pub(crate) test: String,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr_no_lt!(TestExprRaw);

impl<'script> Upable<'script> for TestExprRaw {
    type Target = TestExpr;
    fn up<'registry>(self, _helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match Extractor::new(&self.id, &self.test) {
            Ok(ex) => Ok(TestExpr {
                id: self.id,
                test: self.test,
                extractor: ex,
                mid: self.mid,
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

pub type TopLevelExprsRaw<'script> = Vec<TopLevelExprRaw<'script>>;
pub type ExprsRaw<'script> = Vec<ExprRaw<'script>>;
pub type ImutExprsRaw<'script> = Vec<ImutExprRaw<'script>>;
pub type FieldsRaw<'script> = Vec<FieldRaw<'script>>;
pub type SegmentsRaw<'script> = Vec<SegmentRaw<'script>>;
pub type PatternFieldsRaw<'script> = Vec<PredicatePatternRaw<'script>>;
pub type PredicatesRaw<'script, Ex> = Vec<PredicateClauseRaw<'script, Ex>>;
pub type PatchOperationsRaw<'script> = Vec<PatchOperationRaw<'script>>;
pub type ComprehensionCasesRaw<'script, Ex> = Vec<ComprehensionCaseRaw<'script, Ex>>;
pub type ImutComprehensionCasesRaw<'script> = Vec<ImutComprehensionCaseRaw<'script>>;
pub type ArrayPredicatePatternsRaw<'script> = Vec<ArrayPredicatePatternRaw<'script>>;

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn default() {
        assert_eq!(Endian::default(), Endian::Big);
        assert_eq!(BytesDataType::default(), BytesDataType::UnsignedInteger);
    }
}
