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

/// Base definition for expressions
pub mod base_expr;
pub(crate) mod binary;
/// custom equality definition - checking for equivalence of different AST nodes
/// e.g. two different event paths with different metadata
pub mod eq;
/// Query AST
pub mod query;
pub(crate) mod raw;
mod support;
mod upable;
/// collection of AST visitors
pub mod visitors;
use crate::errors::{error_generic, error_no_consts, error_no_locals, ErrorKind, Result};
use crate::impl_expr_mid;
use crate::interpreter::{exec_binary, exec_unary, AggrType, Cont, Env, ExecOpts, LocalStack};
pub use crate::lexer::CompilationUnit;
use crate::pos::{Location, Range};
use crate::prelude::*;
use crate::registry::FResult;
use crate::registry::{
    Aggr as AggrRegistry, CustomFn, Registry, TremorAggrFnWrapper, TremorFnWrapper,
};
use crate::script::Return;
use crate::KnownKey;
use crate::{stry, tilde::Extractor, EventContext, Value, NO_AGGRS, NO_CONSTS};
pub use base_expr::BaseExpr;
use beef::Cow;
use halfbrown::HashMap;
pub use query::*;
use raw::reduce2;
use serde::Serialize;
use simd_json::StaticNode;
use std::mem;
use std::{borrow::Borrow, collections::BTreeSet};
use upable::Upable;

use self::{
    binary::extend_bytes_from_value,
    raw::{BytesDataType, Endian},
};

#[derive(Default, Clone, Serialize, Debug, PartialEq)]
struct NodeMeta {
    start: Location,
    end: Location,
    name: Option<String>,
    /// Id of current compilation unit part
    cu: usize,
    terminal: bool,
}

impl From<(Location, Location, usize)> for NodeMeta {
    fn from((start, end, cu): (Location, Location, usize)) -> Self {
        Self {
            start,
            end,
            name: None,
            cu,
            terminal: false,
        }
    }
}
/// Information about node metadata
#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct NodeMetas {
    nodes: Vec<NodeMeta>,
    #[serde(skip)]
    pub(crate) cus: Vec<CompilationUnit>,
}

impl<'script> NodeMetas {
    /// Initializes meta noes with a given set of
    #[must_use]
    pub fn new(cus: Vec<CompilationUnit>) -> Self {
        Self {
            nodes: Vec::new(),
            cus,
        }
    }
    pub(crate) fn add_meta(&mut self, mut start: Location, mut end: Location, cu: usize) -> usize {
        let mid = self.nodes.len();
        start.set_cu(cu);
        end.set_cu(cu);
        self.nodes.push((start, end, cu).into());
        mid
    }
    pub(crate) fn add_meta_w_name<S>(
        &mut self,
        mut start: Location,
        mut end: Location,
        name: &S,
        cu: usize,
    ) -> usize
    where
        S: ToString,
    {
        start.set_cu(cu);
        end.set_cu(cu);
        let mid = self.nodes.len();
        self.nodes.push(NodeMeta {
            start,
            end,
            cu,
            name: Some(name.to_string()),
            terminal: false,
        });
        mid
    }

    pub(crate) fn start(&self, idx: usize) -> Option<Location> {
        self.nodes.get(idx).map(|v| v.start)
    }

    pub(crate) fn end(&self, idx: usize) -> Option<Location> {
        self.nodes.get(idx).map(|v| v.end)
    }

    pub(crate) fn name(&self, idx: usize) -> Option<&str> {
        self.nodes
            .get(idx)
            .map(|v| v.name.as_ref())
            .and_then(|v| v)
            .map(String::as_str)
    }

    pub(crate) fn name_dflt(&self, idx: usize) -> &str {
        self.name(idx).unwrap_or("<UNKNOWN>")
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

impl Warning {
    fn new(inner: Range, outer: Range, msg: String) -> Self {
        Self { outer, inner, msg }
    }
    fn new_with_scope(warning_scope: Range, msg: String) -> Self {
        Self::new(warning_scope, warning_scope, msg)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Function<'script> {
    is_const: bool,
    argc: usize,
    name: Cow<'script, str>,
}

#[derive(Debug, PartialEq, Serialize, Clone)]
/// A section of a binary
pub struct BytesPart<'script> {
    /// metadata id
    pub mid: usize,
    /// data
    pub data: ImutExpr<'script>,
    /// type we want to convert this to
    pub data_type: BytesDataType,
    /// Endianness
    pub endianess: Endian,
    /// bits allocated for this
    pub bits: u64,
}
impl_expr_mid!(BytesPart);

/// Binary semiliteral
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Bytes<'script> {
    mid: usize,
    /// Bytes
    pub value: Vec<BytesPart<'script>>,
}
impl_expr_mid!(Bytes);

impl<'script> Bytes<'script> {
    fn try_reduce(mut self, helper: &Helper<'script, '_>) -> Result<ImutExprInt<'script>> {
        self.value = self
            .value
            .into_iter()
            .map(|mut v| {
                v.data = v.data.0.try_reduce(helper).map(ImutExpr)?;
                Ok(v)
            })
            .collect::<Result<Vec<BytesPart>>>()?;
        if self.value.iter().all(|v| is_lit(&v.data.0)) {
            let mut bytes: Vec<u8> = Vec::with_capacity(self.value.len());
            let outer = self.extent(&helper.meta);
            let mut used = 0;
            let mut buf = 0;

            for part in self.value {
                let inner = part.extent(&helper.meta);
                let value = reduce2(part.data.0, &helper)?;
                extend_bytes_from_value(
                    &outer,
                    &inner,
                    &helper.meta,
                    part.data_type,
                    part.endianess,
                    part.bits,
                    &mut buf,
                    &mut used,
                    &mut bytes,
                    &value,
                )?;
            }
            if used > 0 {
                bytes.push(buf >> (8 - used))
            }
            Ok(ImutExprInt::Literal(Literal {
                mid: self.mid,
                value: Value::Bytes(bytes.into()),
            }))
        } else {
            Ok(ImutExprInt::Bytes(self))
        }
    }
}

/// Documentation from constant
#[derive(Debug, Clone, PartialEq)]
pub struct ConstDoc<'script> {
    /// Constant name
    pub name: Cow<'script, str>,
    /// Constant documentation
    pub doc: Option<String>,
    /// Constant value type
    pub value_type: ValueType,
}

impl<'script> ToString for ConstDoc<'script> {
    fn to_string(&self) -> String {
        format!(
            r#"
### {}

*type*: {:?}

{}
        "#,
            self.name,
            self.value_type,
            &self.doc.clone().unwrap_or_default()
        )
    }
}

/// Documentation from function
#[derive(Debug, Clone, PartialEq)]
pub struct FnDoc<'script> {
    /// Function name
    pub name: Cow<'script, str>,
    /// Function arguments
    pub args: Vec<Cow<'script, str>>,
    /// Function documentation
    pub doc: Option<String>,
    /// Whether the function is open or not
    // TODO clarify what open exactly is
    pub open: bool,
}

/// Documentation from a module
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ModDoc<'script> {
    /// Module name
    pub name: Cow<'script, str>,
    /// Module documentation
    pub doc: Option<String>,
}

impl<'script> ModDoc<'script> {
    /// Prints the module documentation
    #[must_use]
    pub fn print_with_name(&self, name: &str) -> String {
        format!(
            r#"
# {}


{}
        "#,
            name,
            &self.doc.clone().unwrap_or_default()
        )
    }
}

impl<'script> ToString for FnDoc<'script> {
    fn to_string(&self) -> String {
        format!(
            r#"
### {}({})

{}
        "#,
            self.name,
            self.args.join(", "),
            self.doc.clone().unwrap_or_default()
        )
    }
}

/// Documentation from a module
#[derive(Debug, Clone, PartialEq)]
pub struct Docs<'script> {
    /// Constants
    pub consts: Vec<ConstDoc<'script>>,
    /// Functions
    pub fns: Vec<FnDoc<'script>>,
    /// Module level documentation
    pub module: Option<ModDoc<'script>>,
}

impl<'script> Default for Docs<'script> {
    fn default() -> Self {
        Self {
            consts: Vec::new(),
            fns: Vec::new(),
            module: None,
        }
    }
}

/// Constants and special keyword values
#[derive(Debug, Default, Clone, PartialEq, Serialize)]
pub struct Consts<'script> {
    names: HashMap<Vec<String>, usize>,
    values: Vec<Value<'script>>,
    // always present 'special' constants
    /// the `args` keyword
    pub args: Value<'script>,
    /// the `group` keyword
    pub group: Value<'script>,
    /// the `window` keyword
    pub window: Value<'script>,
}

impl<'script> Consts<'script> {
    pub(crate) fn new() -> Self {
        Consts {
            names: HashMap::new(),
            values: Vec::new(),
            args: Value::Static(StaticNode::Null),
            group: Value::Static(StaticNode::Null),
            window: Value::Static(StaticNode::Null),
        }
    }
    fn is_const(&self, id: &[String]) -> Option<&usize> {
        self.names.get(id)
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn insert(
        &mut self,
        name_v: Vec<String>,
        val: Value<'script>,
    ) -> std::result::Result<usize, usize> {
        let idx = self.values.len();

        self.names.insert(name_v, idx).map_or_else(
            || {
                self.values.push(val);
                Ok(idx)
            },
            Err,
        )
    }

    pub(crate) fn get(&self, idx: usize) -> Option<&Value<'script>> {
        self.values.get(idx)
    }
}

/// ordered collection of warnings
pub type Warnings = std::collections::BTreeSet<Warning>;

#[allow(clippy::struct_excessive_bools)]
pub(crate) struct Helper<'script, 'registry>
where
    'script: 'registry,
{
    reg: &'registry Registry,
    aggr_reg: &'registry AggrRegistry,
    can_emit: bool,
    is_in_aggr: bool,
    windows: HashMap<String, WindowDecl<'script>>,
    scripts: HashMap<String, ScriptDecl<'script>>,
    operators: HashMap<String, OperatorDecl<'script>>,
    aggregates: Vec<InvokeAggrFn<'script>>,
    pub warnings: Warnings,
    shadowed_vars: Vec<String>,
    func_vec: Vec<CustomFn<'script>>,
    pub locals: HashMap<String, usize>,
    pub functions: HashMap<Vec<String>, usize>,
    pub consts: Consts<'script>,
    pub streams: HashMap<Vec<String>, usize>,
    pub meta: NodeMetas,
    docs: Docs<'script>,
    module: Vec<String>,
    possible_leaf: bool,
    fn_argc: usize,
    is_open: bool,
    file_offset: Location,
    cu: usize,
}

impl<'script, 'registry> Helper<'script, 'registry>
where
    'script: 'registry,
{
    fn is_const(&self, id: &[String]) -> Option<&usize> {
        self.consts.is_const(id)
    }

    fn add_const_doc(
        &mut self,
        name: Cow<'script, str>,
        doc: Option<Vec<Cow<'script, str>>>,
        value_type: ValueType,
    ) {
        let doc = doc.map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n"));
        self.docs.consts.push(ConstDoc {
            name,
            doc,
            value_type,
        })
    }
    pub fn add_meta(&mut self, start: Location, end: Location) -> usize {
        self.meta
            .add_meta(start - self.file_offset, end - self.file_offset, self.cu)
    }
    pub fn add_meta_w_name<S>(&mut self, start: Location, end: Location, name: &S) -> usize
    where
        S: ToString,
    {
        self.meta.add_meta_w_name(
            start - self.file_offset,
            end - self.file_offset,
            name,
            self.cu,
        )
    }
    pub fn has_locals(&self) -> bool {
        self.locals
            .iter()
            .any(|(n, _)| !n.starts_with(" __SHADOW "))
    }

    pub fn swap(
        &mut self,
        aggregates: &mut Vec<InvokeAggrFn<'script>>,
        locals: &mut HashMap<String, usize>,
    ) {
        mem::swap(&mut self.aggregates, aggregates);
        mem::swap(&mut self.locals, locals);
    }

    pub fn new(
        reg: &'registry Registry,
        aggr_reg: &'registry AggrRegistry,
        cus: Vec<crate::lexer::CompilationUnit>,
    ) -> Self {
        Helper {
            reg,
            aggr_reg,
            can_emit: true,
            is_in_aggr: false,
            windows: HashMap::new(),
            scripts: HashMap::new(),
            operators: HashMap::new(),
            aggregates: Vec::new(),
            warnings: BTreeSet::new(),
            locals: HashMap::new(),
            consts: Consts::default(),
            streams: HashMap::new(),
            functions: HashMap::new(),
            func_vec: Vec::new(),
            shadowed_vars: Vec::new(),
            meta: NodeMetas::new(cus),
            docs: Docs::default(),
            module: Vec::new(),
            possible_leaf: false,
            fn_argc: 0,
            is_open: false,
            file_offset: Location::default(),
            cu: 0,
        }
    }

    fn register_fun(&mut self, f: CustomFn<'script>) -> Result<usize> {
        let i = self.func_vec.len();
        let mut mf = self.module.clone();
        mf.push(f.name.clone().to_string());

        if self.functions.insert(mf, i).is_none() {
            self.func_vec.push(f);
            Ok(i)
        } else {
            Err(format!("function {} already defined.", f.name).into())
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
                //TODO: make sure we never overwrite this,
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
        let id = self.find_shadow_var(id).unwrap_or_else(|| id.to_string());

        self.locals.get(id.as_str()).copied().unwrap_or_else(|| {
            self.locals.insert(id.to_string(), self.locals.len());
            self.locals.len() - 1
        })
    }

    fn warn(&mut self, warning: Warning) {
        self.warnings.insert(warning);
    }
}

/// A tremor script instance
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Script<'script> {
    /// Import definitions
    pub imports: Imports<'script>,
    /// Expressions of the script
    pub exprs: Exprs<'script>,
    /// Constants defined in this script
    pub consts: Consts<'script>,
    /// Aggregate functions
    pub aggregates: Vec<InvokeAggrFn<'script>>,
    windows: HashMap<String, WindowDecl<'script>>,
    functions: Vec<CustomFn<'script>>,
    /// Locals
    pub locals: usize,
    /// Node metadata
    pub node_meta: NodeMetas,
    #[serde(skip)]
    /// Documentation from the script
    pub docs: Docs<'script>,
}

impl<'input, 'run, 'script, 'event> Script<'script>
where
    'input: 'script,
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
            recursion_limit: crate::recursion_limit(),
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
            }
            match stry!(expr.run(opts.without_result(), &env, event, state, meta, &mut local)) {
                Cont::Drop => return Ok(Return::Drop),
                Cont::Emit(value, port) => return Ok(Return::Emit { value, port }),
                Cont::EmitEvent(port) => {
                    return Ok(Return::EmitEvent { port });
                }
                Cont::Cont(_v) => (),
            }
        }

        // We know that we never get here, sadly rust doesn't

        Ok(Return::Emit {
            value: Value::null(),
            port: None,
        })
    }
}

/// A lexical compilation unit
#[derive(Debug, PartialEq, Serialize, Clone)]
pub enum LexicalUnit<'script> {
    /// Import declaration with no alias
    NakedImportDecl(Vec<raw::IdentRaw<'script>>),
    /// Import declaration with an alias
    AliasedImportDecl(Vec<raw::IdentRaw<'script>>, raw::IdentRaw<'script>),
    /// Line directive with embedded "<string> <num> ;"
    LineDirective(Cow<'script, str>),
}
// impl_expr_mid!(Ident);

/// An ident
#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct Ident<'script> {
    pub(crate) mid: usize,
    /// the text of the ident
    pub id: beef::Cow<'script, str>,
}
impl_expr_mid!(Ident);

impl<'script> std::fmt::Display for Ident<'script> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

impl<'script> From<&'script str> for Ident<'script> {
    fn from(id: &'script str) -> Self {
        Self {
            mid: 0,
            id: id.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulation of a record structure field
pub struct Field<'script> {
    /// Id
    pub mid: usize,
    /// Name of the field
    pub name: ImutExprInt<'script>,
    /// Value expression for the field
    pub value: ImutExprInt<'script>,
}
impl_expr_mid!(Field);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulation of a record structure
pub struct Record<'script> {
    /// Id
    pub mid: usize,
    /// Fields of this record
    pub fields: Fields<'script>,
}
impl_expr_mid!(Record);
impl<'script> Record<'script> {
    fn try_reduce(self, helper: &Helper<'script, '_>) -> Result<ImutExprInt<'script>> {
        if self
            .fields
            .iter()
            .all(|f| is_lit(&f.name) && is_lit(&f.value))
        {
            let obj: Result<crate::Object> = self
                .fields
                .into_iter()
                .map(|f| {
                    reduce2(f.name.clone(), &helper).and_then(|n| {
                        // ALLOW: The grammar guarantees the key of a record is always a string
                        let n = n.as_str().unwrap_or_else(|| unreachable!());
                        reduce2(f.value, &helper).map(|v| (n.to_owned().into(), v))
                    })
                })
                .collect();
            Ok(ImutExprInt::Literal(Literal {
                mid: self.mid,
                value: Value::from(obj?),
            }))
        } else {
            Ok(ImutExprInt::Record(self))
        }
    }
    /// Tries to fetch a field from a record
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&ImutExprInt> {
        self.fields.iter().find_map(|f| {
            if let ImutExprInt::Literal(Literal { value, .. }) = &f.name {
                if value == name {
                    Some(&f.value)
                } else {
                    None
                }
            } else {
                None
            }
        })
    }
    /// Tries to fetch a literal from a record
    #[must_use]
    pub fn get_literal(&self, name: &str) -> Option<&Value> {
        if let ImutExprInt::Literal(Literal { value, .. }) = self.get(name)? {
            Some(value)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulation of a list structure
pub struct List<'script> {
    /// Id
    pub mid: usize,
    /// Value expressions for list elements of this list
    pub exprs: ImutExprs<'script>,
}
impl_expr_mid!(List);

impl<'script> List<'script> {
    fn try_reduce(self, helper: &Helper<'script, '_>) -> Result<ImutExprInt<'script>> {
        if self.exprs.iter().map(|v| &v.0).all(is_lit) {
            let elements: Result<Vec<Value>> = self
                .exprs
                .into_iter()
                .map(|v| reduce2(v.0, &helper))
                .collect();
            Ok(ImutExprInt::Literal(Literal {
                mid: self.mid,
                value: Value::from(elements?),
            }))
        } else {
            Ok(ImutExprInt::List(self))
        }
    }
}

/// A Literal
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Literal<'script> {
    /// Id
    pub mid: usize,
    /// Literal value
    pub value: Value<'script>,
}
impl_expr_mid!(Literal);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct FnDecl<'script> {
    pub mid: usize,
    pub name: Ident<'script>,
    pub args: Vec<Ident<'script>>,
    pub body: Exprs<'script>,
    pub locals: usize,
    pub open: bool,
    pub inline: bool,
}
impl_expr_mid!(FnDecl);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Legal expression forms
pub enum Expr<'script> {
    /// Match expression
    Match(Box<Match<'script>>),
    /// In place patch expression
    PatchInPlace(Box<Patch<'script>>),
    /// In place merge expression
    MergeInPlace(Box<Merge<'script>>),
    /// Assignment expression
    Assign {
        /// Id
        mid: usize,
        /// Target
        path: Path<'script>,
        /// Value expression
        expr: Box<Expr<'script>>,
    },
    /// Assignment from local expression
    AssignMoveLocal {
        /// Id
        mid: usize,
        /// Target
        path: Path<'script>,
        /// Local Index
        idx: usize,
    },
    /// A structure comprehension
    Comprehension(Box<Comprehension<'script>>),
    /// A drop expression
    Drop {
        /// Id
        mid: usize,
    },
    /// An emit expression
    Emit(Box<EmitExpr<'script>>),
    /// An immutable expression
    Imut(ImutExprInt<'script>),
}

impl<'script> From<ImutExprInt<'script>> for Expr<'script> {
    fn from(imut: ImutExprInt<'script>) -> Expr<'script> {
        Expr::Imut(imut)
    }
}

/// An immutable expression
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutExpr<'script>(pub ImutExprInt<'script>);

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
/// Encapsulates an immutable expression
pub enum ImutExprInt<'script> {
    /// Record
    Record(Record<'script>),
    /// List
    List(List<'script>),
    /// Binary operation
    Binary(Box<BinExpr<'script>>),
    /// Unary operation
    Unary(Box<UnaryExpr<'script>>),
    /// Patch
    Patch(Box<Patch<'script>>),
    /// Match
    Match(Box<ImutMatch<'script>>),
    /// Comprehension
    Comprehension(Box<ImutComprehension<'script>>),
    /// Merge
    Merge(Box<Merge<'script>>),
    /// Path
    Path(Path<'script>),
    /// A string literal
    String(StringLit<'script>),
    /// Local - local variable
    Local {
        /// Local Index
        idx: usize,
        /// Id
        mid: usize,
        /// True, if it is declared constant
        is_const: bool,
    },
    /// Literal
    Literal(Literal<'script>),
    /// Presence
    Present {
        /// Path
        path: Path<'script>,
        /// Id
        mid: usize,
    },
    /// Function invocation
    Invoke1(Invoke<'script>),
    /// Function invocation
    Invoke2(Invoke<'script>),
    /// Function invocation
    Invoke3(Invoke<'script>),
    /// Function invocation
    Invoke(Invoke<'script>),
    /// Aggregate Function invocation
    InvokeAggr(InvokeAggr),
    /// Tail-Recursion
    Recur(Recur<'script>),
    /// Bytes
    Bytes(Bytes<'script>),
}

fn is_lit(e: &ImutExprInt) -> bool {
    matches!(e, ImutExprInt::Literal(_))
}

/// A string literal with interpolation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StringLit<'script> {
    /// Id
    pub mid: usize,
    /// Elements
    pub elements: StrLitElements<'script>,
}
impl_expr_mid!(StringLit);

/// A part of a string literal with interpolation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum StrLitElement<'script> {
    /// A literal string
    Lit(Cow<'script, str>),
    /// An expression in a string interpolation
    Expr(ImutExprInt<'script>),
}

/// we're forced to make this pub because of lalrpop
pub type StrLitElements<'script> = Vec<StrLitElement<'script>>;

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates an emit expression
pub struct EmitExpr<'script> {
    /// Id
    pub mid: usize,
    /// Value expression
    pub expr: ImutExprInt<'script>,
    /// Port name
    pub port: Option<ImutExprInt<'script>>,
}
impl_expr_mid!(EmitExpr);

#[derive(Clone, Serialize)]
/// Encapsulates a function invocation expression
pub struct Invoke<'script> {
    /// Id
    pub mid: usize,
    /// Module path
    pub module: Vec<String>,
    /// Function name
    pub fun: String,
    /// Invocable implementation
    #[serde(skip)]
    pub invocable: Invocable<'script>,
    /// Arguments
    pub args: ImutExprs<'script>,
}
impl_expr_mid!(Invoke);

impl<'script> Invoke<'script> {
    fn inline(self) -> Result<ImutExprInt<'script>> {
        self.invocable.inline(self.args, self.mid)
    }
    fn can_inline(&self) -> bool {
        self.invocable.can_inline()
    }

    fn try_reduce(self, helper: &Helper<'script, '_>) -> Result<ImutExprInt<'script>> {
        if self.invocable.is_const() && self.args.iter().all(|f| is_lit(&f.0)) {
            let ex = self.extent(&helper.meta);
            let args: Result<Vec<Value<'script>>> = self
                .args
                .into_iter()
                .map(|v| reduce2(v.0, &helper))
                .collect();
            let args = args?;
            // Construct a view into `args`, since `invoke` expects a slice of references.
            let args2: Vec<&Value<'script>> = args.iter().collect();
            let env = Env {
                context: &EventContext::default(),
                consts: &NO_CONSTS,
                aggrs: &NO_AGGRS,
                meta: &helper.meta,
                recursion_limit: crate::recursion_limit(),
            };

            let v = self
                .invocable
                .invoke(&env, &args2)
                .map_err(|e| e.into_err(&ex, &ex, Some(&helper.reg), &helper.meta))?
                .into_static();
            Ok(ImutExprInt::Literal(Literal {
                value: v,
                mid: self.mid,
            }))
        } else {
            Ok(match self.args.len() {
                1 => ImutExprInt::Invoke1(self),
                2 => ImutExprInt::Invoke2(self),
                3 => ImutExprInt::Invoke3(self),
                _ => ImutExprInt::Invoke(self),
            })
        }
    }
}

#[derive(Clone)]
/// An invocable expression form
pub enum Invocable<'script> {
    /// Reference to a builtin or intrinsic function
    Intrinsic(TremorFnWrapper),
    /// A user defined or standard library function
    Tremor(CustomFn<'script>),
}

impl<'script> Invocable<'script> {
    fn inline(self, args: ImutExprs<'script>, mid: usize) -> Result<ImutExprInt<'script>> {
        match self {
            Invocable::Intrinsic(_f) => Err("can't inline intrinsic".into()),
            Invocable::Tremor(f) => f.inline(args, mid),
        }
    }
    fn can_inline(&self) -> bool {
        match self {
            Invocable::Intrinsic(_f) => false,
            Invocable::Tremor(f) => f.can_inline(),
        }
    }

    fn is_const(&self) -> bool {
        match self {
            Invocable::Intrinsic(f) => f.is_const(),
            Invocable::Tremor(f) => f.is_const(),
        }
    }
    /// Invokes this invocable
    pub fn invoke<'event, 'run>(
        &'script self,
        env: &'run Env<'run, 'event, 'script>,
        args: &'run [&'run Value<'event>],
    ) -> FResult<Value<'event>>
    where
        'script: 'event,
        'event: 'run,
    {
        match self {
            Invocable::Intrinsic(f) => f.invoke(env.context, args),
            Invocable::Tremor(f) => f.invoke(env, args),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates the tail-recursion entry-point in a tail-recursive function
pub struct Recur<'script> {
    /// Id
    pub mid: usize,
    /// Arity
    pub argc: usize,
    /// True, if supports variable arguments
    pub open: bool,
    /// Capture of argument value expressions
    pub exprs: ImutExprs<'script>,
}
impl_expr_mid!(Recur);

#[derive(Clone, Serialize, PartialEq)]
/// Encapsulates an Aggregate function invocation
pub struct InvokeAggr {
    /// Id
    pub mid: usize,
    /// Module name
    pub module: String,
    /// Function name
    pub fun: String,
    /// Unique Id of this instance
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
impl_expr_mid!(InvokeAggrFn);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a pluggable extractor expression form
pub struct TestExpr {
    /// Id
    pub mid: usize,
    /// Extractor name
    pub id: String,
    /// Extractor format
    pub test: String,
    /// Extractor plugin
    pub extractor: Extractor,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a match expression form
pub struct Match<'script> {
    /// Id
    pub mid: usize,
    /// The target of the match
    pub target: ImutExprInt<'script>,
    /// Patterns to match against the target
    pub patterns: Predicates<'script>,
}
impl_expr_mid!(Match);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates an immutable match expression form
pub struct ImutMatch<'script> {
    /// Id
    pub mid: usize,
    /// The target of the match
    pub target: ImutExprInt<'script>,
    /// The patterns against the match target
    pub patterns: ImutPredicates<'script>,
}
impl_expr_mid!(ImutMatch);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a predicate expression form
pub struct PredicateClause<'script> {
    /// Id
    pub mid: usize,
    /// Predicate pattern
    pub pattern: Pattern<'script>,
    /// Optional guard expression
    pub guard: Option<ImutExprInt<'script>>,
    /// Expressions to evaluate if predicate test and guard pass
    pub exprs: Exprs<'script>,
    /// The last expression
    pub last_expr: Expr<'script>,
}
impl_expr_mid!(PredicateClause);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates an immutable predicate expression form
pub struct ImutPredicateClause<'script> {
    /// Id
    pub mid: usize,
    /// Predicate pattern
    pub pattern: Pattern<'script>,
    /// Optional guard expression
    pub guard: Option<ImutExprInt<'script>>,
    /// Expressions to evaluate if predicate test and guard pass
    pub expr: ImutExpr<'script>,
}
impl_expr_mid!(ImutPredicateClause);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a path expression form
pub struct Patch<'script> {
    /// Id
    pub mid: usize,
    /// The patch target
    pub target: ImutExprInt<'script>,
    /// Operations to patch against the target
    pub operations: PatchOperations<'script>,
}
impl_expr_mid!(Patch);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates patch operation forms
pub enum PatchOperation<'script> {
    /// Insert only operation
    Insert {
        /// Field
        ident: ImutExprInt<'script>,
        /// Value expression
        expr: ImutExprInt<'script>,
    },
    /// Insert or update operation
    Upsert {
        /// Field
        ident: ImutExprInt<'script>,
        /// Value expression
        expr: ImutExprInt<'script>,
    },
    /// Update only operation
    Update {
        /// Field
        ident: ImutExprInt<'script>,
        /// Value expression
        expr: ImutExprInt<'script>,
    },
    /// Erase operation
    Erase {
        /// Field
        ident: ImutExprInt<'script>,
    },
    /// Copy operation
    Copy {
        /// From field
        from: ImutExprInt<'script>,
        /// To field
        to: ImutExprInt<'script>,
    },
    /// Move operation
    Move {
        /// Field from
        from: ImutExprInt<'script>,
        /// Field to
        to: ImutExprInt<'script>,
    },
    /// Merge convenience operation
    Merge {
        /// Field
        ident: ImutExprInt<'script>,
        /// Value
        expr: ImutExprInt<'script>,
    },
    /// Tuple based merge operation
    TupleMerge {
        /// Value
        expr: ImutExprInt<'script>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a merge form
pub struct Merge<'script> {
    /// Id
    pub mid: usize,
    /// Target of the merge
    pub target: ImutExprInt<'script>,
    /// Value expression computing content to merge into the target
    pub expr: ImutExprInt<'script>,
}
impl_expr_mid!(Merge);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a structure comprehension form
pub struct Comprehension<'script> {
    /// Id
    pub mid: usize,
    /// Key binding
    pub key_id: usize,
    /// Value binding
    pub val_id: usize,
    /// Target of the comprehension
    pub target: ImutExprInt<'script>,
    /// Case applications against target elements
    pub cases: ComprehensionCases<'script>,
}
impl_expr_mid!(Comprehension);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates an immutable comprehension form
pub struct ImutComprehension<'script> {
    /// Id
    pub mid: usize,
    /// Key binding
    pub key_id: usize,
    /// Value binding
    pub val_id: usize,
    /// Target of the comprehension
    pub target: ImutExprInt<'script>,
    /// Case applications against target elements
    pub cases: ImutComprehensionCases<'script>,
}
impl_expr_mid!(ImutComprehension);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a comprehension case application
pub struct ComprehensionCase<'script> {
    /// Id
    pub mid: usize,
    /// Key binding
    pub key_name: Cow<'script, str>,
    /// Value binding
    pub value_name: Cow<'script, str>,
    /// Guard expression
    pub guard: Option<ImutExprInt<'script>>,
    /// Case application against target on passing guard
    pub exprs: Exprs<'script>,
    /// Last case application against target on passing guard
    pub last_expr: Expr<'script>,
}
impl_expr_mid!(ComprehensionCase);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates an immutable comprehension case application
pub struct ImutComprehensionCase<'script> {
    /// id
    pub mid: usize,
    /// Key binding
    pub key_name: Cow<'script, str>,
    /// value binding
    pub value_name: Cow<'script, str>,
    /// Guard expression
    pub guard: Option<ImutExprInt<'script>>,
    /// Case application against target on passing guard
    pub expr: ImutExpr<'script>,
}
impl_expr_mid!(ImutComprehensionCase);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates predicate pattern form
pub enum Pattern<'script> {
    //Predicate(PredicatePattern<'script>),
    /// Record pattern
    Record(RecordPattern<'script>),
    /// Array pattern
    Array(ArrayPattern<'script>),
    /// Expression
    Expr(ImutExprInt<'script>),
    /// Assignment pattern
    Assign(AssignPattern<'script>),
    /// Tuple pattern
    Tuple(TuplePattern<'script>),
    /// Don't care condition
    DoNotCare,
    /// Gates if no other pattern matches
    Default,
}
impl<'script> Pattern<'script> {
    fn is_default(&self) -> bool {
        matches!(self, Pattern::Default)
    }
    fn is_assign(&self) -> bool {
        matches!(self, Pattern::Assign(_))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a predicate pattern form
pub enum PredicatePattern<'script> {
    /// Structural application
    TildeEq {
        /// Assignment bind point
        assign: Cow<'script, str>,
        /// Lhs
        lhs: Cow<'script, str>,
        /// Key
        #[serde(skip)]
        key: KnownKey<'script>,
        /// Predicate
        test: Box<TestExpr>,
    },
    /// Binary predicate
    Bin {
        /// Lhs
        lhs: Cow<'script, str>,
        /// Key
        #[serde(skip)]
        key: KnownKey<'script>,
        /// Rhs
        rhs: ImutExprInt<'script>,
        /// Binary operation kind
        kind: BinOpKind,
    },
    /// Record search pattern
    RecordPatternEq {
        /// Lhs
        lhs: Cow<'script, str>,
        /// Key
        #[serde(skip)]
        key: KnownKey<'script>,
        /// Predicate
        pattern: RecordPattern<'script>,
    },
    /// Array search pattern
    ArrayPatternEq {
        /// Lhs
        lhs: Cow<'script, str>,
        /// Key
        #[serde(skip)]
        key: KnownKey<'script>,
        /// Predicate
        pattern: ArrayPattern<'script>,
    },
    /// Field presence
    FieldPresent {
        /// Lhs
        lhs: Cow<'script, str>,
        /// Key
        #[serde(skip)]
        key: KnownKey<'script>,
    },
    /// Field absence
    FieldAbsent {
        /// Lhs
        lhs: Cow<'script, str>,
        /// Key
        #[serde(skip)]
        key: KnownKey<'script>,
    },
}

impl<'script> PredicatePattern<'script> {
    /// Get key
    #[must_use]
    pub fn key(&self) -> &KnownKey<'script> {
        use PredicatePattern::{
            ArrayPatternEq, Bin, FieldAbsent, FieldPresent, RecordPatternEq, TildeEq,
        };
        match self {
            TildeEq { key, .. }
            | Bin { key, .. }
            | RecordPatternEq { key, .. }
            | ArrayPatternEq { key, .. }
            | FieldPresent { key, .. }
            | FieldAbsent { key, .. } => &key,
        }
    }

    fn lhs(&self) -> &Cow<'script, str> {
        use PredicatePattern::{
            ArrayPatternEq, Bin, FieldAbsent, FieldPresent, RecordPatternEq, TildeEq,
        };
        match self {
            TildeEq { lhs, .. }
            | Bin { lhs, .. }
            | RecordPatternEq { lhs, .. }
            | ArrayPatternEq { lhs, .. }
            | FieldPresent { lhs, .. }
            | FieldAbsent { lhs, .. } => &lhs,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a record pattern
pub struct RecordPattern<'script> {
    /// Id
    pub mid: usize,
    /// Pattern fields
    pub fields: PatternFields<'script>,
}
impl_expr_mid!(RecordPattern);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates an array predicate pattern
pub enum ArrayPredicatePattern<'script> {
    /// Expression
    Expr(ImutExprInt<'script>),
    /// Tilde predicate
    Tilde(Box<TestExpr>),
    /// Nested record pattern
    Record(RecordPattern<'script>),
    /// Don't care condition
    Ignore,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates an array pattern
pub struct ArrayPattern<'script> {
    /// Id
    pub mid: usize,
    /// Predicates
    pub exprs: ArrayPredicatePatterns<'script>,
}
impl_expr_mid!(ArrayPattern);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates an assignment pattern
pub struct AssignPattern<'script> {
    /// Bind point
    pub id: Cow<'script, str>,
    /// Local index
    pub idx: usize,
    /// Nested predicate pattern
    pub pattern: Box<Pattern<'script>>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a positional tuple pattern
pub struct TuplePattern<'script> {
    /// Id
    pub mid: usize,
    /// Predicates
    pub exprs: ArrayPredicatePatterns<'script>,
    /// True, if the pattern supports variable arguments
    pub open: bool,
}
impl_expr_mid!(TuplePattern);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Represents a path-like-structure
pub enum Path<'script> {
    /// A constant path
    Const(LocalPath<'script>),
    /// A local path
    Local(LocalPath<'script>),
    /// The current event
    Event(EventPath<'script>),
    /// The captured program state, minus const and local state
    State(StatePath<'script>),
    /// Runtime type information ( meta-state )
    Meta(MetadataPath<'script>),
    /// Special reserved path
    Reserved(ReservedPath<'script>),
}

impl<'script> Path<'script> {
    /// Get segments as slice
    #[must_use]
    pub fn segments(&self) -> &[Segment] {
        match self {
            Path::Const(path) | Path::Local(path) => &path.segments,
            Path::Meta(path) => &path.segments,
            Path::Event(path) => &path.segments,
            Path::State(path) => &path.segments,
            Path::Reserved(path) => path.segments(),
        }
    }
    fn try_reduce(self, helper: &Helper<'script, '_>) -> Result<ImutExprInt<'script>> {
        match self {
            Path::Const(LocalPath {
                is_const: true,
                segments,
                idx,
                mid,
            }) if segments.is_empty() => {
                if let Some(v) = helper.consts.get(idx) {
                    let lit = Literal {
                        mid,
                        value: v.clone(),
                    };
                    Ok(ImutExprInt::Literal(lit))
                } else {
                    let r = Range::from((
                        helper.meta.start(mid).unwrap_or_default(),
                        helper.meta.end(mid).unwrap_or_default(),
                    ));
                    let e = format!(
                        "Invalid const reference to '{}'",
                        helper.meta.name_dflt(mid),
                    );
                    error_generic(&r.expand_lines(2), &r, &e, &helper.meta)
                }
            }
            other => Ok(ImutExprInt::Path(other)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// A Path segment
pub enum Segment<'script> {
    /// An identifier
    Id {
        /// Key
        #[serde(skip)]
        key: KnownKey<'script>,
        /// Id
        mid: usize,
    },
    /// A numeric index
    Idx {
        /// Index
        idx: usize,
        /// id
        mid: usize,
    },
    /// An element
    Element {
        /// Value Expression
        expr: ImutExprInt<'script>,
        /// Id
        mid: usize,
    },
    /// A range
    Range {
        /// Lower-inclusive
        lower_mid: usize,
        /// Max-exclusive
        upper_mid: usize,
        /// Id
        mid: usize,
        /// Start of range value expression
        range_start: Box<ImutExprInt<'script>>,
        /// End of range value expression
        range_end: Box<ImutExprInt<'script>>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// A path local to the current program
pub struct LocalPath<'script> {
    /// Local Index
    pub idx: usize,
    /// True, if declared const
    pub is_const: bool,
    /// Id
    pub mid: usize,
    /// Segments
    pub segments: Segments<'script>,
}
impl_expr_mid!(LocalPath);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// A metadata path
pub struct MetadataPath<'script> {
    /// Id
    pub mid: usize,
    /// Segments
    pub segments: Segments<'script>,
}
impl_expr_mid!(MetadataPath);

/// Reserved keyword path
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ReservedPath<'script> {
    /// `args` keyword
    Args {
        /// Id
        mid: usize,
        /// Segments
        segments: Segments<'script>,
    },
    /// `window` keyword
    Window {
        /// Id
        mid: usize,
        /// Segments
        segments: Segments<'script>,
    },
    /// `group` keyword
    Group {
        /// Id
        mid: usize,
        /// Segments
        segments: Segments<'script>,
    },
}

impl<'script> ReservedPath<'script> {
    fn segments(&self) -> &Segments<'script> {
        match self {
            ReservedPath::Args { segments, .. }
            | ReservedPath::Window { segments, .. }
            | ReservedPath::Group { segments, .. } => segments,
        }
    }
}

impl<'script> BaseExpr for ReservedPath<'script> {
    fn mid(&self) -> usize {
        match self {
            ReservedPath::Args { mid, .. }
            | ReservedPath::Window { mid, .. }
            | ReservedPath::Group { mid, .. } => *mid,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// The path representing the current in-flight event
pub struct EventPath<'script> {
    /// Id
    pub mid: usize,
    /// Segments
    pub segments: Segments<'script>,
}
impl_expr_mid!(EventPath);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// The path representing captured program state
pub struct StatePath<'script> {
    /// Id
    pub mid: usize,
    /// Segments
    pub segments: Segments<'script>,
}
impl_expr_mid!(StatePath);

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
/// Encapsulates a binary expression form
pub struct BinExpr<'script> {
    /// Id
    pub mid: usize,
    /// The operation kind
    pub kind: BinOpKind,
    /// The Left-hand-side operand
    pub lhs: ImutExprInt<'script>,
    /// The Right-hand-side operand
    pub rhs: ImutExprInt<'script>,
}
impl_expr_mid!(BinExpr);

impl<'script> BinExpr<'script> {
    fn try_reduce(self, helper: &Helper<'script, '_>) -> Result<ImutExprInt<'script>> {
        match self {
            b
            @
            BinExpr {
                lhs: ImutExprInt::Literal(_),
                rhs: ImutExprInt::Literal(_),
                ..
            } => {
                let lhs = reduce2(b.lhs.clone(), helper)?;
                let rhs = reduce2(b.rhs.clone(), helper)?;
                let value = exec_binary(&b, &b, &helper.meta, b.kind, &lhs, &rhs)?.into_owned();
                let lit = Literal { mid: b.mid, value };
                Ok(ImutExprInt::Literal(lit))
            }
            b => Ok(ImutExprInt::Binary(Box::new(b))),
        }
    }
}

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
/// Encapsulates a unary expression form
pub struct UnaryExpr<'script> {
    /// Id
    pub mid: usize,
    /// The operation kind
    pub kind: UnaryOpKind,
    /// The operand
    pub expr: ImutExprInt<'script>,
}
impl_expr_mid!(UnaryExpr);

impl<'script> UnaryExpr<'script> {
    fn try_reduce(self, helper: &Helper<'script, '_>) -> Result<ImutExprInt<'script>> {
        match self {
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
        }
    }
}

pub(crate) type Exprs<'script> = Vec<Expr<'script>>;
/// A list of lexical compilation units
pub type Imports<'script> = Vec<LexicalUnit<'script>>;
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

#[cfg(test)]
mod test {
    use crate::prelude::*;

    fn v(s: &'static str) -> super::ImutExprInt<'static> {
        super::ImutExprInt::Literal(super::Literal {
            mid: 0,
            value: Value::from(s),
        })
    }
    #[test]
    fn record() {
        let f1 = super::Field {
            mid: 0,
            name: v("snot"),
            value: v("badger"),
        };
        let f2 = super::Field {
            mid: 0,
            name: v("badger"),
            value: v("snot"),
        };

        let r = super::Record {
            mid: 0,
            fields: vec![f1, f2],
        };

        assert_eq!(r.get("snot"), Some(&v("badger")));
        assert_eq!(r.get("nots"), None);

        assert_eq!(
            r.get_literal("badger").and_then(ValueTrait::as_str),
            Some("snot")
        );
        assert_eq!(r.get("adgerb"), None);
    }
}
