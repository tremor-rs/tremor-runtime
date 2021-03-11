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
/// Query AST
pub mod query;
pub(crate) mod raw;
mod support;
mod upable;
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
use std::borrow::Borrow;
use std::mem;
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

impl<'script> AstEq for BytesPart<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.data_type == other.data_type
            && self.endianess == other.endianess
            && self.bits == other.bits
            && self.data.0.ast_eq(&other.data.0)
    }
}

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

impl<'script> AstEq for Bytes<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.value.len() == other.value.len()
            && self
                .value
                .iter()
                .zip(other.value.iter())
                .all(|(b1, b2)| b1.ast_eq(b2))
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
    // TODO: Users of the `warnings` field might be helped if `warnings` were a Set. Right now,
    // some places (twice in query/raw.rs) do `append + sort + dedup`. With, e.g., a `BTreeSet`,
    // this could be achieved in a cleaner and faster way, and `Warning` already implements `Ord`
    // anyway.
    warnings: Vec<Warning>,
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
            warnings: Vec::new(),
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

impl<'script> AstEq for Record<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.fields.len() == other.fields.len()
            && self
                .fields
                .iter()
                .zip(other.fields.iter())
                .all(|(f1, f2)| f1.name.ast_eq(&f2.name) && f1.value.ast_eq(&f2.value))
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

impl<'script> AstEq for List<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.exprs.len() == other.exprs.len()
            && self
                .exprs
                .iter()
                .zip(other.exprs.iter())
                .all(|(e1, e2)| e1.0.ast_eq(&e2.0))
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

impl<'script> AstEq for Literal<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

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

/// some special kind of equivalence between expressions
/// ignoring metadata ids
pub trait AstEq<T = Self> {
    /// returns true if both self and other are the same, ignoring the `mid`
    fn ast_eq(&self, other: &T) -> bool;
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

impl<'script> AstEq for ImutExprInt<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        use ImutExprInt::*;
        match (self, other) {
            (Record(r1), Record(r2)) => r1.ast_eq(r2),
            (List(l1), List(l2)) => l1.ast_eq(l2),
            (Binary(b1), Binary(b2)) => b1.ast_eq(b2),
            (Unary(u1), Unary(u2)) => u1.ast_eq(u2),
            (Patch(p1), Patch(p2)) => p1.ast_eq(p2),
            (Match(m1), Match(m2)) => m1.ast_eq(m2),
            (Comprehension(c1), Comprehension(c2)) => c1.ast_eq(c2),
            (Merge(m1), Merge(m2)) => m1.ast_eq(m2),
            (Path(p1), Path(p2)) => p1.ast_eq(p2),
            (String(s1), String(s2)) => s1.ast_eq(s2),
            (
                Local {
                    idx: idx1,
                    is_const: const1,
                    ..
                },
                Local {
                    idx: idx2,
                    is_const: const2,
                    ..
                },
            ) => idx1 == idx2 && const1 == const2,
            (Literal(l1), Literal(l2)) => l1.ast_eq(l2),
            (Present { path: path1, .. }, Present { path: path2, .. }) => path1.ast_eq(path2),
            (Invoke1(i1), Invoke1(i2)) => i1.ast_eq(i2),
            (Invoke2(i1), Invoke2(i2)) => i1.ast_eq(i2),
            (Invoke3(i1), Invoke3(i2)) => i1.ast_eq(i2),
            (Invoke(i1), Invoke(i2)) => i1.ast_eq(i2),
            (InvokeAggr(i1), InvokeAggr(i2)) => i1.ast_eq(i2),
            (Recur(r1), Recur(r2)) => r1.ast_eq(r2),
            (Bytes(b1), Bytes(b2)) => b1.ast_eq(b2),
            _ => false,
        }
    }
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

impl<'script> AstEq for StringLit<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.elements.len() == other.elements.len()
            && self
                .elements
                .iter()
                .zip(other.elements.iter())
                .all(|(e1, e2)| e1.ast_eq(e2))
    }
}

/// A part of a string literal with interpolation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum StrLitElement<'script> {
    /// A literal string
    Lit(Cow<'script, str>),
    /// An expression in a string interpolation
    Expr(ImutExprInt<'script>),
}

impl<'script> AstEq for StrLitElement<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Expr(e1), Self::Expr(e2)) => e1.ast_eq(e2),
            _ => self == other,
        }
    }
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

impl<'script> AstEq for Invoke<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.module.eq(&other.module)
            && self.fun == other.fun
            && self.invocable.ast_eq(&other.invocable)
            && self.args.len() == other.args.len()
            && self
                .args
                .iter()
                .zip(other.args.iter())
                .all(|(a1, a2)| a1.0.ast_eq(&a2.0))
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

impl<'script> AstEq for Invocable<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Intrinsic(wrapper1), Self::Intrinsic(wrapper2)) => wrapper1.ast_eq(wrapper2),
            (Self::Tremor(custom1), Self::Tremor(custom2)) => custom1.ast_eq(custom2),
            _ => false,
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

impl<'script> AstEq for Recur<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.argc == other.argc
            && self.open == other.open
            && self.exprs.len() == other.exprs.len()
            && self
                .exprs
                .iter()
                .zip(other.exprs.iter())
                .all(|(e1, e2)| e1.0.ast_eq(&e2.0))
    }
}

#[derive(Clone, Serialize)]
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

impl AstEq for InvokeAggr {
    fn ast_eq(&self, other: &Self) -> bool {
        self.aggr_id == other.aggr_id && self.module == other.module && self.fun == other.fun
    }
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

impl AstEq for TestExpr {
    fn ast_eq(&self, other: &Self) -> bool {
        self.id == other.id && self.test == other.test && self.extractor == other.extractor
    }
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

impl<'script> AstEq for ImutMatch<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.target.ast_eq(&other.target)
            && self.patterns.len() == other.patterns.len()
            && self
                .patterns
                .iter()
                .zip(other.patterns.iter())
                .all(|(p1, p2)| p1.ast_eq(&p2))
    }
}
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

impl<'script> AstEq for ImutPredicateClause<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.expr.0.ast_eq(&other.expr.0)
            && match (self.guard.as_ref(), other.guard.as_ref()) {
                (Some(expr1), Some(expr2)) => expr1.ast_eq(expr2),
                (None, None) => true,
                _ => false,
            }
            && self.pattern.ast_eq(&other.pattern)
    }
}

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

impl<'script> AstEq for Patch<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.target.ast_eq(&other.target)
            && self.operations.len() == other.operations.len()
            && self
                .operations
                .iter()
                .zip(other.operations.iter())
                .all(|(o1, o2)| o1.ast_eq(o2))
    }
}

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

impl<'script> AstEq for PatchOperation<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Insert {
                    ident: i1,
                    expr: e1,
                },
                Self::Insert {
                    ident: i2,
                    expr: e2,
                },
            ) => i1.ast_eq(i2) && e1.ast_eq(e2),
            (
                Self::Upsert {
                    ident: i1,
                    expr: e1,
                },
                Self::Upsert {
                    ident: i2,
                    expr: e2,
                },
            ) => i1.ast_eq(i2) && e1.ast_eq(e2),
            (
                Self::Update {
                    ident: i1,
                    expr: e1,
                },
                Self::Update {
                    ident: i2,
                    expr: e2,
                },
            ) => i1.ast_eq(i2) && e1.ast_eq(e2),
            (Self::Erase { ident: i1 }, Self::Erase { ident: i2 }) => i1.ast_eq(i2),
            (Self::Copy { from: f1, to: t1 }, Self::Copy { from: f2, to: t2 }) => {
                f1.ast_eq(f2) && t1.ast_eq(t2)
            }
            (Self::Move { from: f1, to: t1 }, Self::Move { from: f2, to: t2 }) => {
                f1.ast_eq(f2) && t1.ast_eq(t2)
            }
            (
                Self::Merge {
                    ident: i1,
                    expr: e1,
                },
                Self::Merge {
                    ident: i2,
                    expr: e2,
                },
            ) => i1.ast_eq(i2) && e1.ast_eq(e2),
            (Self::TupleMerge { expr: e1 }, Self::TupleMerge { expr: e2 }) => e1.ast_eq(e2),
            _ => false,
        }
    }
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

impl<'script> AstEq for Merge<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.target.ast_eq(&other.target) && self.expr.ast_eq(&other.expr)
    }
}

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

impl<'script> AstEq for ImutComprehension<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.key_id == other.key_id
            && self.val_id == other.val_id
            && self.target.ast_eq(&other.target)
            && self.cases.len() == other.cases.len()
            && self
                .cases
                .iter()
                .zip(other.cases.iter())
                .all(|(c1, c2)| c1.ast_eq(c2))
    }
}

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

impl<'script> AstEq for ImutComprehensionCase<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.key_name == other.key_name
            && self.value_name == other.value_name
            && match (self.guard.as_ref(), other.guard.as_ref()) {
                (Some(g1), Some(g2)) => g1.ast_eq(g2),
                (None, None) => true,
                _ => false,
            }
            && self.expr.0.ast_eq(&other.expr.0)
    }
}

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

impl<'script> AstEq for Pattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Record(r1), Self::Record(r2)) => r1.ast_eq(r2),
            (Self::Array(a1), Self::Array(a2)) => a1.ast_eq(a2),
            (Self::Expr(e1), Self::Expr(e2)) => e1.ast_eq(e2),
            (Self::Assign(a1), Self::Assign(a2)) => a1.ast_eq(a2),
            (Self::Tuple(t1), Self::Tuple(t2)) => t1.ast_eq(t2),
            _ => self == other,
        }
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

impl<'script> AstEq for PredicatePattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::TildeEq {
                    assign: a1,
                    lhs: l1,
                    key: k1,
                    test: t1,
                },
                Self::TildeEq {
                    assign: a2,
                    lhs: l2,
                    key: k2,
                    test: t2,
                },
            ) => a1 == a2 && l1 == l2 && k1 == k2 && t1.ast_eq(t2.as_ref()),
            (
                Self::Bin {
                    lhs: l1,
                    key: k1,
                    rhs: r1,
                    kind: kind1,
                },
                Self::Bin {
                    lhs: l2,
                    key: k2,
                    rhs: r2,
                    kind: kind2,
                },
            ) => l1 == l2 && k1 == k2 && kind1 == kind2 && r1.ast_eq(r2),
            (
                Self::RecordPatternEq {
                    lhs: l1,
                    key: k1,
                    pattern: p1,
                },
                Self::RecordPatternEq {
                    lhs: l2,
                    key: k2,
                    pattern: p2,
                },
            ) => l1 == l2 && k1 == k2 && p1.ast_eq(p2),
            (
                Self::ArrayPatternEq {
                    lhs: l1,
                    key: k1,
                    pattern: p1,
                },
                Self::ArrayPatternEq {
                    lhs: l2,
                    key: k2,
                    pattern: p2,
                },
            ) => l1 == l2 && k1 == k2 && p1.ast_eq(p2),
            _ => self == other,
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

impl<'script> AstEq for RecordPattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.fields.len() == other.fields.len()
            && self
                .fields
                .iter()
                .zip(other.fields.iter())
                .all(|(f1, f2)| f1.ast_eq(&f2))
    }
}

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

impl<'script> AstEq for ArrayPredicatePattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Expr(e1), Self::Expr(e2)) => e1.ast_eq(e2),
            (Self::Tilde(t1), Self::Tilde(t2)) => t1.ast_eq(t2.as_ref()),
            (Self::Record(r1), Self::Record(r2)) => r1.ast_eq(r2),
            _ => self == other,
        }
    }
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

impl<'script> AstEq for ArrayPattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.exprs.len() == other.exprs.len()
            && self
                .exprs
                .iter()
                .zip(other.exprs.iter())
                .all(|(e1, e2)| e1.ast_eq(e2))
    }
}

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

impl<'script> AstEq for AssignPattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.id == other.id && self.idx == other.idx && self.pattern.ast_eq(other.pattern.as_ref())
    }
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

impl<'script> AstEq for TuplePattern<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.open == other.open
            && self.exprs.len() == other.exprs.len()
            && self
                .exprs
                .iter()
                .zip(other.exprs.iter())
                .all(|(e1, e2)| e1.ast_eq(e2))
    }
}

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

impl<'script> AstEq for Path<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Const(c1), Self::Const(c2)) => c1.ast_eq(c2),
            (Self::Local(l1), Self::Local(l2)) => l1.ast_eq(l2),
            (Self::Event(e1), Self::Event(e2)) => e1.ast_eq(e2),
            (Self::State(s1), Self::State(s2)) => s1.ast_eq(s2),
            (Self::Meta(m1), Self::Meta(m2)) => m1.ast_eq(m2),
            (Self::Reserved(r1), Self::Reserved(r2)) => r1.ast_eq(r2),
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
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

impl<'script> AstEq for Segment<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Id { key: k1, .. }, Self::Id { key: k2, .. }) => k1 == k2,
            (Self::Idx { idx: i1, .. }, Self::Idx { idx: i2, .. }) => i1 == i2,
            (Self::Element { expr: e1, .. }, Self::Element { expr: e2, .. }) => e1.ast_eq(e2),
            (
                Self::Range {
                    range_start: s1,
                    range_end: e1,
                    ..
                },
                Self::Range {
                    range_start: s2,
                    range_end: e2,
                    ..
                },
            ) => s1.ast_eq(s2.as_ref()) && e1.ast_eq(e2.as_ref()),
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
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

impl<'script> AstEq for LocalPath<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.idx == other.idx
            && self.is_const == other.is_const
            && self.segments.len() == other.segments.len()
            && self
                .segments
                .iter()
                .zip(other.segments.iter())
                .all(|(s1, s2)| s1.ast_eq(s2))
    }
}

#[derive(Clone, Debug, Serialize)]
/// A metadata path
pub struct MetadataPath<'script> {
    /// Id
    pub mid: usize,
    /// Segments
    pub segments: Segments<'script>,
}
impl_expr_mid!(MetadataPath);

impl<'script> AstEq for MetadataPath<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.segments.len() == other.segments.len()
            && self
                .segments
                .iter()
                .zip(other.segments.iter())
                .all(|(s1, s2)| s1.ast_eq(s2))
    }
}

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

impl<'script> AstEq for ReservedPath<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Args { segments: s1, .. }, Self::Args { segments: s2, .. })
            | (Self::Window { segments: s1, .. }, Self::Window { segments: s2, .. })
            | (Self::Group { segments: s1, .. }, Self::Group { segments: s2, .. }) => {
                s1.len() == s2.len() && s1.iter().zip(s2.iter()).all(|(s1, s2)| s1.ast_eq(s2))
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
/// The path representing the current in-flight event
pub struct EventPath<'script> {
    /// Id
    pub mid: usize,
    /// Segments
    pub segments: Segments<'script>,
}
impl_expr_mid!(EventPath);

impl<'script> AstEq for EventPath<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.segments.len() == other.segments.len()
            && self
                .segments
                .iter()
                .zip(other.segments.iter())
                .all(|(s1, s2)| s1.ast_eq(s2))
    }
}

#[derive(Clone, Debug, Serialize)]
/// The path representing captured program state
pub struct StatePath<'script> {
    /// Id
    pub mid: usize,
    /// Segments
    pub segments: Segments<'script>,
}
impl_expr_mid!(StatePath);

impl<'script> AstEq for StatePath<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.segments.len() == other.segments.len()
            && self
                .segments
                .iter()
                .zip(other.segments.iter())
                .all(|(s1, s2)| s1.ast_eq(s2))
    }
}

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

impl<'script> AstEq for BinExpr<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.lhs.ast_eq(&other.lhs) && self.rhs.ast_eq(&other.rhs)
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

impl<'script> AstEq for UnaryExpr<'script> {
    fn ast_eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.expr.ast_eq(&other.expr)
    }
}

/// Return value from visit methods for `ImutExprIntVisitor`
/// controlling whether to continue walking the subtree or not
pub enum VisitRes {
    /// carry on walking
    Walk,
    /// stop walking
    Stop,
}

use VisitRes::*;

/// Visitor for traversing all ImutExprInts within the given ImutExprInt
///
/// Implement your custom expr visiting logic by overwriting the visit_* methods.
/// You do not need to traverse further down. This is done by the provided `walk_*` methods.
/// The walk_* methods implement walking the expression tree, those do not need to be changed.
pub trait ImutExprIntVisitor<'script> {
    /// visit a record
    fn visit_record(&mut self, _record: &mut Record<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a record
    fn walk_record(&mut self, record: &mut Record<'script>) -> Result<()> {
        for field in &mut record.fields {
            self.walk_expr(&mut field.name)?;
            self.walk_expr(&mut field.value)?;
        }
        Ok(())
    }
    /// visit a list
    fn visit_list(&mut self, _list: &mut List<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a list
    fn walk_list(&mut self, list: &mut List<'script>) -> Result<()> {
        for element in &mut list.exprs {
            self.walk_expr(&mut element.0)?;
        }
        Ok(())
    }
    /// visit a binary
    fn visit_binary(&mut self, _binary: &mut BinExpr<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a binary
    fn walk_binary(&mut self, binary: &mut BinExpr<'script>) -> Result<()> {
        self.walk_expr(&mut binary.lhs)?;
        self.walk_expr(&mut binary.rhs)
    }

    /// visit a unary expr
    fn visit_unary(&mut self, _unary: &mut UnaryExpr<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a unary
    fn walk_unary(&mut self, unary: &mut UnaryExpr<'script>) -> Result<()> {
        self.walk_expr(&mut unary.expr)
    }

    /// visit a patch expr
    fn visit_patch(&mut self, _patch: &mut Patch<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a patch expr
    fn walk_patch(&mut self, patch: &mut Patch<'script>) -> Result<()> {
        self.walk_expr(&mut patch.target)?;
        for op in &mut patch.operations {
            match op {
                PatchOperation::Insert { ident, expr } => {
                    self.walk_expr(ident)?;
                    self.walk_expr(expr)?;
                }
                PatchOperation::Copy { from, to } => {
                    self.walk_expr(from)?;
                    self.walk_expr(to)?;
                }
                PatchOperation::Erase { ident } => {
                    self.walk_expr(ident)?;
                }
                PatchOperation::Merge { ident, expr } => {
                    self.walk_expr(ident)?;
                    self.walk_expr(expr)?;
                }
                PatchOperation::Move { from, to } => {
                    self.walk_expr(from)?;
                    self.walk_expr(to)?;
                }
                PatchOperation::TupleMerge { expr } => {
                    self.walk_expr(expr)?;
                }
                PatchOperation::Update { ident, expr } => {
                    self.walk_expr(ident)?;
                    self.walk_expr(expr)?;
                }
                PatchOperation::Upsert { ident, expr } => {
                    self.walk_expr(ident)?;
                    self.walk_expr(expr)?;
                }
            }
        }
        Ok(())
    }
    /// visit a match expr
    fn visit_match(&mut self, _mmatch: &mut ImutMatch<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a match expr
    fn walk_match(&mut self, mmatch: &mut ImutMatch<'script>) -> Result<()> {
        self.walk_expr(&mut mmatch.target)?;
        for predicate in &mut mmatch.patterns {
            self.walk_match_patterns(&mut predicate.pattern)?;
            if let Some(guard) = &mut predicate.guard {
                self.walk_expr(guard)?;
            }
            self.walk_expr(&mut predicate.expr.0)?;
        }
        Ok(())
    }
    /// walk match patterns
    fn walk_match_patterns(&mut self, pattern: &mut Pattern<'script>) -> Result<()> {
        match pattern {
            Pattern::Record(record_pat) => {
                self.walk_record_pattern(record_pat)?;
            }
            Pattern::Array(array_pat) => {
                self.walk_array_pattern(array_pat)?;
            }
            Pattern::Expr(expr) => {
                self.walk_expr(expr)?;
            }
            Pattern::Assign(assign_pattern) => {
                self.walk_match_patterns(assign_pattern.pattern.as_mut())?;
            }
            Pattern::Tuple(tuple_pattern) => {
                for elem in &mut tuple_pattern.exprs {
                    match elem {
                        ArrayPredicatePattern::Expr(expr) => {
                            self.walk_expr(expr)?;
                        }
                        ArrayPredicatePattern::Record(record_pattern) => {
                            self.walk_record_pattern(record_pattern)?;
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
    /// walk a record pattern
    fn walk_record_pattern(&mut self, record_pattern: &mut RecordPattern<'script>) -> Result<()> {
        for field in &mut record_pattern.fields {
            match field {
                PredicatePattern::RecordPatternEq { pattern, .. } => {
                    self.walk_record_pattern(pattern)?;
                }
                PredicatePattern::Bin { rhs, .. } => {
                    self.walk_expr(rhs)?;
                }
                PredicatePattern::ArrayPatternEq { pattern, .. } => {
                    self.walk_array_pattern(pattern)?;
                }
                _ => {}
            }
        }
        Ok(())
    }
    /// walk an array pattern
    fn walk_array_pattern(&mut self, array_pattern: &mut ArrayPattern<'script>) -> Result<()> {
        for elem in &mut array_pattern.exprs {
            match elem {
                ArrayPredicatePattern::Expr(expr) => {
                    self.walk_expr(expr)?;
                }
                ArrayPredicatePattern::Record(record_pattern) => {
                    self.walk_record_pattern(record_pattern)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// visit a comprehension
    fn visit_comprehension(&mut self, _comp: &mut ImutComprehension<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a comprehension
    fn walk_comprehension(&mut self, comp: &mut ImutComprehension<'script>) -> Result<()> {
        self.walk_expr(&mut comp.target)?;
        for comp_case in &mut comp.cases {
            if let Some(guard) = &mut comp_case.guard {
                self.walk_expr(guard)?;
            }
            self.walk_expr(&mut comp_case.expr.0)?;
        }
        Ok(())
    }

    /// visit a merge expr
    fn visit_merge(&mut self, _merge: &mut Merge<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a merge expr
    fn walk_merge(&mut self, merge: &mut Merge<'script>) -> Result<()> {
        self.walk_expr(&mut merge.target)?;
        self.walk_expr(&mut merge.expr)
    }

    /// visit a path
    fn visit_path(&mut self, _path: &mut Path<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a path
    fn walk_path(&mut self, path: &mut Path<'script>) -> Result<()> {
        let segments = match path {
            Path::Const(LocalPath { segments, .. })
            | Path::Local(LocalPath { segments, .. })
            | Path::Event(EventPath { segments, .. })
            | Path::State(StatePath { segments, .. })
            | Path::Meta(MetadataPath { segments, .. })
            | Path::Reserved(ReservedPath::Args { segments, .. })
            | Path::Reserved(ReservedPath::Group { segments, .. })
            | Path::Reserved(ReservedPath::Window { segments, .. }) => segments,
        };
        for segment in segments {
            match segment {
                Segment::Element { expr, .. } => {
                    self.walk_expr(expr)?;
                }
                Segment::Range {
                    range_start,
                    range_end,
                    ..
                } => {
                    self.walk_expr(range_start.as_mut())?;
                    self.walk_expr(range_end.as_mut())?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// visit a string
    fn visit_string(&mut self, _string: &mut StringLit<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a string
    fn walk_string(&mut self, string: &mut StringLit<'script>) -> Result<()> {
        for element in &mut string.elements {
            match element {
                StrLitElement::Expr(expr) => self.walk_expr(expr)?,
                _ => {}
            }
        }
        Ok(())
    }

    /// visit a local
    fn visit_local(&mut self, _local_idx: &mut usize) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a present expr
    fn visit_present(&mut self, _path: &mut Path<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit an invoke expr
    fn visit_invoke(&mut self, _invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk an invoke expr
    fn walk_invoke(&mut self, invoke: &mut Invoke<'script>) -> Result<()> {
        for arg in &mut invoke.args {
            self.walk_expr(&mut arg.0)?;
        }
        Ok(())
    }

    /// visit an invoke1 expr
    fn visit_invoke1(&mut self, _invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// visit an invoke2 expr
    fn visit_invoke2(&mut self, _invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// visit an invoke3 expr
    fn visit_invoke3(&mut self, _invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit an invoke_aggr expr
    fn visit_invoke_aggr(&mut self, _invoke_aggr: &mut InvokeAggr) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a recur expr
    fn visit_recur(&mut self, _recur: &mut Recur<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a recur expr
    fn walk_recur(&mut self, recur: &mut Recur<'script>) -> Result<()> {
        for expr in &mut recur.exprs {
            self.walk_expr(&mut expr.0)?;
        }
        Ok(())
    }

    /// visit bytes
    fn visit_bytes(&mut self, _bytes: &mut Bytes<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk bytes
    fn walk_bytes(&mut self, bytes: &mut Bytes<'script>) -> Result<()> {
        for part in &mut bytes.value {
            self.walk_expr(&mut part.data.0)?;
        }
        Ok(())
    }

    /// visit a literal
    fn visit_literal(&mut self, _literal: &mut Literal<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a generic ImutExprInt (this is called before the concrete `visit_*` method)
    fn visit_expr(&mut self, _e: &mut ImutExprInt<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// entry point into this visitor - call this to start visiting the given expression `e`
    fn walk_expr(&mut self, e: &mut ImutExprInt<'script>) -> Result<()> {
        if let Walk = self.visit_expr(e)? {
            match e {
                ImutExprInt::Record(record) => {
                    if let Walk = self.visit_record(record)? {
                        self.walk_record(record)?;
                    }
                }
                ImutExprInt::List(list) => {
                    if let Walk = self.visit_list(list)? {
                        self.walk_list(list)?;
                    }
                }
                ImutExprInt::Binary(binary) => {
                    if let Walk = self.visit_binary(binary.as_mut())? {
                        self.walk_binary(binary.as_mut())?;
                    }
                }
                ImutExprInt::Unary(unary) => {
                    if let Walk = self.visit_unary(unary.as_mut())? {
                        self.walk_unary(unary.as_mut())?;
                    }
                }
                ImutExprInt::Patch(patch) => {
                    if let Walk = self.visit_patch(patch.as_mut())? {
                        self.walk_patch(patch.as_mut())?;
                    }
                }
                ImutExprInt::Match(mmatch) => {
                    if let Walk = self.visit_match(mmatch.as_mut())? {
                        self.walk_match(mmatch.as_mut())?;
                    }
                }
                ImutExprInt::Comprehension(comp) => {
                    if let Walk = self.visit_comprehension(comp.as_mut())? {
                        self.walk_comprehension(comp.as_mut())?;
                    }
                }
                ImutExprInt::Merge(merge) => {
                    if let Walk = self.visit_merge(merge.as_mut())? {
                        self.walk_merge(merge.as_mut())?;
                    }
                }
                ImutExprInt::Path(path) => {
                    if let Walk = self.visit_path(path)? {
                        self.walk_path(path)?;
                    }
                }
                ImutExprInt::String(string) => {
                    if let Walk = self.visit_string(string)? {
                        self.walk_string(string)?;
                    }
                }
                ImutExprInt::Local { idx, .. } => {
                    let _ = self.visit_local(idx)?;
                }
                ImutExprInt::Present { path, .. } => {
                    if let Walk = self.visit_present(path)? {
                        self.walk_path(path)?;
                    }
                }
                ImutExprInt::Invoke(invoke) => {
                    if let Walk = self.visit_invoke(invoke)? {
                        self.walk_invoke(invoke)?;
                    }
                }
                ImutExprInt::Invoke1(invoke1) => {
                    if let Walk = self.visit_invoke1(invoke1)? {
                        self.walk_invoke(invoke1)?;
                    }
                }
                ImutExprInt::Invoke2(invoke2) => {
                    if let Walk = self.visit_invoke2(invoke2)? {
                        self.walk_invoke(invoke2)?;
                    }
                }
                ImutExprInt::Invoke3(invoke3) => {
                    if let Walk = self.visit_invoke3(invoke3)? {
                        self.walk_invoke(invoke3)?;
                    }
                }
                ImutExprInt::InvokeAggr(invoke_aggr) => {
                    let _ = self.visit_invoke_aggr(invoke_aggr)?;
                }
                ImutExprInt::Recur(recur) => {
                    if let Walk = self.visit_recur(recur)? {
                        self.walk_recur(recur)?;
                    }
                }
                ImutExprInt::Bytes(bytes) => {
                    if let Walk = self.visit_bytes(bytes)? {
                        self.walk_bytes(bytes)?;
                    }
                }
                ImutExprInt::Literal(lit) => {
                    let _ = self.visit_literal(lit)?;
                }
            }
        }

        Ok(())
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
