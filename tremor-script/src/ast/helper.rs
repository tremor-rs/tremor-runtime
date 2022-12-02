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

pub use super::query::*;
use super::{
    docs::{ConstDoc, Docs, QueryDoc},
    module::{self, Content, GetMod, Manager},
    raw::LocalPathRaw,
    ConnectorDefinition, Const, DeployFlow, FlowDefinition, FnDefn, InvokeAggrFn, NodeId,
};
use crate::{
    errors::Result,
    pos::Span,
    prelude::*,
    registry::{Aggr as AggrRegistry, Registry},
    NodeMeta,
};
use beef::Cow;
use halfbrown::HashMap;
use std::{collections::BTreeSet, fmt::Display, mem};

/// ordered collection of warnings
pub type Warnings = std::collections::BTreeSet<Warning>;

/// Class of warning that gives additional insight into the warning's intent
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Copy)]
pub enum WarningClass {
    /// A general warning that isn't specific to a particular class
    General,
    /// A warning that is related to performance
    Performance,
    /// A warning that is related to consistency
    Consistency,
    /// A warning that is related to possibly unexpected behaviour
    Behaviour,
}

impl Display for WarningClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::General => write!(f, "general"),
            Self::Performance => write!(f, "performance"),
            Self::Consistency => write!(f, "consistency"),
            Self::Behaviour => write!(f, "behaviour"),
        }
    }
}

#[derive(Serialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
/// A warning generated while lexing or parsing
pub struct Warning {
    /// type of the warning
    pub class: WarningClass,
    /// Outer span of the warning
    pub outer: Span,
    /// Inner span of thw warning
    pub inner: Span,
    /// Warning message
    pub msg: String,
}

impl Warning {
    fn new<T: ToString>(outer: Span, inner: Span, msg: &T, class: WarningClass) -> Self {
        Self {
            class,
            outer,
            inner,
            msg: msg.to_string(),
        }
    }
    fn new_with_scope<T: ToString>(warning_scope: Span, msg: &T, class: WarningClass) -> Self {
        Self::new(warning_scope, warning_scope, msg, class)
    }
}

/// A scope
#[derive(Default, Debug, Clone, Serialize, PartialEq)]
pub struct Scope<'script> {
    /// Module of the scope
    pub(crate) modules: std::collections::BTreeMap<String, module::Index>,
    /// Content of the scope
    pub content: Content<'script>,
    pub(crate) parent: Option<Box<Scope<'script>>>,
}
impl<'script> Scope<'script> {
    pub(crate) fn get_module(&self, id: &[String]) -> Result<Option<module::Index>> {
        let (first, rest) = if let Some(r) = id.split_first() {
            r
        } else {
            return Ok(None);
        };
        let id = if let Some(i) = self.modules.get(first) {
            *i
        } else {
            return Ok(None);
        };
        Manager::find_module(id, rest)
    }
    pub(crate) fn add_module_alias(&mut self, alias: String, module_id: module::Index) {
        self.modules.insert(alias, module_id);
    }

    pub(crate) fn insert_flow(&mut self, flow: FlowDefinition<'script>) -> Result<()> {
        self.content.insert_flow(flow)
    }
    pub(crate) fn insert_connector(
        &mut self,
        connector: ConnectorDefinition<'script>,
    ) -> Result<()> {
        self.content.insert_connector(connector)
    }
    pub(crate) fn insert_const(&mut self, c: Const<'script>) -> Result<()> {
        self.content.insert_const(c)
    }
    pub(crate) fn insert_function(&mut self, f: FnDefn<'script>) -> Result<()> {
        self.content.insert_function(f)
    }
    pub(crate) fn insert_pipeline(&mut self, pipeline: PipelineDefinition<'script>) -> Result<()> {
        self.content.insert_pipeline(pipeline)
    }

    pub(crate) fn insert_window(&mut self, window: WindowDefinition<'script>) -> Result<()> {
        self.content.insert_window(window)
    }
    pub(crate) fn insert_operator(&mut self, operator: OperatorDefinition<'script>) -> Result<()> {
        self.content.insert_operator(operator)
    }
    pub(crate) fn insert_script(&mut self, script: ScriptDefinition<'script>) -> Result<()> {
        self.content.insert_script(script)
    }
}
/// Helper
#[allow(clippy::struct_excessive_bools)]
pub struct Helper<'script, 'registry>
where
    'script: 'registry,
{
    pub(crate) reg: &'registry Registry,
    pub(crate) aggr_reg: &'registry AggrRegistry,
    pub(crate) can_emit: bool,
    pub(crate) is_in_aggr: bool,
    // Troy
    pub(crate) instances: HashMap<String, DeployFlow<'script>>,
    /// Aggregates
    pub aggregates: Vec<InvokeAggrFn<'script>>,
    /// Warnings
    pub warnings: Warnings,
    pub(crate) shadowed_vars: Vec<String>,
    pub(crate) locals: HashMap<String, usize>,
    /// AST Metadata
    pub(crate) docs: Docs,
    pub(crate) possible_leaf: bool,
    pub(crate) fn_argc: usize,
    pub(crate) is_open: bool,
    /// Current scope
    pub scope: Scope<'script>,
}

impl<'script, 'registry> Helper<'script, 'registry>
where
    'script: 'registry,
{
    /// get current scope
    pub(crate) fn scope(&mut self) -> &mut Scope<'script> {
        &mut self.scope
    }
    /// Enters a new scope
    pub(crate) fn enter_scope(&mut self) {
        self.set_scope(Scope::default());
    }
    ///Prepends a given scope
    pub(crate) fn set_scope(&mut self, mut scope: Scope<'script>) {
        std::mem::swap(&mut self.scope, &mut scope);
        self.scope.parent = Some(Box::new(scope));
    }
    /// leaves a scope
    pub(crate) fn leave_scope(&mut self) -> Result<Scope<'script>> {
        if let Some(mut next) = self.scope.parent.take() {
            std::mem::swap(&mut self.scope, &mut next);
            Ok(*next)
        } else {
            Err("No parent scope".into())
        }
    }
    /// Finds something from the module script
    /// # Errors
    /// if we can't lock the module manager
    pub fn get<Target>(&self, id: &NodeId) -> Result<Option<Target>>
    where
        Target: 'script,
        Manager: module::Get<Target>,
        Content<'script>: module::GetMod<Target>,
    {
        if id.module.is_empty() {
            Ok(self.scope.content.get(&id.id))
        } else if let Some((m, n)) = self.resolve_module_alias(id)? {
            Manager::get(m, n)
        } else {
            Ok(None)
        }
    }

    /// resolves the local aliases for modules
    pub(crate) fn resolve_module_alias<'n>(
        &self,
        id: &'n NodeId,
    ) -> Result<Option<(module::Index, &'n str)>> {
        Ok(if id.module.is_empty() {
            None
        } else {
            self.scope.get_module(&id.module)?.map(|mid| (mid, id.id()))
        })
    }

    pub(crate) fn is_const(&self, id: &str) -> bool {
        self.scope.content.consts.contains_key(id)
    }

    pub(crate) fn is_const_path(&self, p: &LocalPathRaw) -> bool {
        self.is_const(&p.root.id)
    }

    pub(crate) fn add_const_doc<N: ToString>(
        &mut self,
        name: &N,
        doc: Option<Vec<Cow<'script, str>>>,
        value_type: ValueType,
    ) {
        let doc = doc.map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n"));
        self.docs.consts.push(ConstDoc {
            name: name.to_string(),
            doc,
            value_type,
        });
    }
    pub(crate) fn add_query_doc<N: ToString>(
        &mut self,
        name: &N,
        doc: Option<Vec<Cow<'script, str>>>,
    ) {
        let doc = doc.map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n"));
        self.docs.queries.push(QueryDoc {
            name: name.to_string(),
            doc,
        });
    }

    pub(crate) fn has_locals(&self) -> bool {
        self.locals
            .iter()
            .any(|(n, _)| !n.starts_with(" __SHADOW "))
    }

    pub(crate) fn swap(
        &mut self,
        aggregates: &mut Vec<InvokeAggrFn<'script>>,
        locals: &mut HashMap<String, usize>,
    ) {
        mem::swap(&mut self.aggregates, aggregates);
        mem::swap(&mut self.locals, locals);
    }

    /// Creates a new AST helper
    #[must_use]
    pub fn new(reg: &'registry Registry, aggr_reg: &'registry AggrRegistry) -> Self {
        Helper {
            reg,
            aggr_reg,
            can_emit: true,
            is_in_aggr: false,
            // Troy
            instances: HashMap::new(),
            // Trickle
            aggregates: Vec::new(),
            // Common
            warnings: BTreeSet::new(),
            locals: HashMap::new(),
            shadowed_vars: Vec::new(),
            docs: Docs::default(),
            possible_leaf: false,
            fn_argc: 0,
            is_open: false,
            scope: Scope::default(),
        }
    }

    /// id - identifier that is shadowed
    pub(crate) fn register_shadow_var(&mut self, id: &str) -> usize {
        let r = self.reserve_shadow();
        self.shadowed_vars.push(id.to_string());
        r
    }

    /// register an "anonymous" shadow variable from a AST node meta id
    pub(crate) fn register_shadow_from_mid(&mut self, mid: &NodeMeta) -> usize {
        let id = format!("{:?}", mid);
        self.register_shadow_var(&id)
    }

    /// removes the last shadow variable
    /// does not touch the locals
    pub(crate) fn end_shadow_var(&mut self) {
        self.shadowed_vars.pop();
    }

    fn find_shadow_var(&self, id: &str) -> Option<String> {
        let mut r = None;
        for (i, s) in self.shadowed_vars.iter().enumerate() {
            if s == id {
                //TODO: make sure we never overwrite this,
                r = Some(shadow_name(i));
            }
        }
        r
    }

    /// this always needs to be used with `register_shadow_var`
    fn reserve_shadow(&mut self) -> usize {
        self.var_id(&shadow_name(self.shadowed_vars.len()))
    }

    pub(crate) fn reserve_2_shadow(&mut self) -> (usize, usize) {
        let l = self.shadowed_vars.len();
        let n1 = shadow_name(l);
        let n2 = shadow_name(l + 1);
        (self.var_id(&n1), self.var_id(&n2))
    }

    pub(crate) fn var_id(&mut self, id: &str) -> usize {
        let id = self.find_shadow_var(id).unwrap_or_else(|| id.to_string());

        self.locals.get(id.as_str()).copied().unwrap_or_else(|| {
            self.locals.insert(id.to_string(), self.locals.len());
            self.locals.len() - 1
        })
    }

    pub(crate) fn warn<S: ToString>(
        &mut self,
        outer: Span,
        inner: Span,
        msg: &S,
        class: WarningClass,
    ) {
        self.warnings.insert(Warning::new(outer, inner, msg, class));
    }
    pub(crate) fn warn_with_scope<S: ToString>(&mut self, r: Span, msg: &S, class: WarningClass) {
        self.warnings.insert(Warning::new_with_scope(r, msg, class));
    }
}

/// create a unique shadow name that cannot be created by users
fn shadow_name(id: usize) -> String {
    format!(" __SHADOW {}__ ", id)
}
