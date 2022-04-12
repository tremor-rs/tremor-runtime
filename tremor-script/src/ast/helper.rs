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
    docs::{ConstDoc, Docs, QueryDeclDoc},
    module::{self, GetModule, ModuleContent, ModuleManager},
    raw::LocalPathRaw,
    ConnectorDefinition, Const, DeployFlow, FlowDefinition, FnDecl, InvokeAggrFn, NodeId,
};
use crate::{
    errors::Result,
    pos::Span,
    prelude::*,
    registry::{Aggr as AggrRegistry, Registry},
};
use beef::Cow;
use halfbrown::HashMap;
use std::{collections::BTreeSet, mem};

/// ordered collection of warnings
pub type Warnings = std::collections::BTreeSet<Warning>;

#[derive(Serialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
/// A warning generated while lexing or parsing
pub struct Warning {
    /// Outer span of the warning
    pub outer: Span,
    /// Inner span of thw warning
    pub inner: Span,
    /// Warning message
    pub msg: String,
}

impl Warning {
    fn new<T: ToString>(inner: Span, outer: Span, msg: &T) -> Self {
        Self {
            outer,
            inner,
            msg: msg.to_string(),
        }
    }
    fn new_with_scope<T: ToString>(warning_scope: Span, msg: &T) -> Self {
        Self::new(warning_scope, warning_scope, msg)
    }
}

/// A scope
#[derive(Default, Debug, Clone, Serialize, PartialEq)]
pub struct Scope<'script> {
    /// Module of the scope
    pub(crate) modules: std::collections::HashMap<String, module::Index>,
    /// Content of the scope
    pub content: ModuleContent<'script>,
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
        ModuleManager::find_module(id, rest)
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
    pub(crate) fn insert_function(&mut self, f: FnDecl<'script>) -> Result<()> {
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
    pub(crate) instances: HashMap<NodeId, DeployFlow<'script>>,
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
        let mut scope = Scope::default();
        std::mem::swap(&mut self.scope, &mut scope);
        self.scope.parent = Some(Box::new(scope));
    }
    /// leaves a scope
    pub(crate) fn leave_scope(&mut self) -> Result<()> {
        // FIXME return left scope
        if let Some(next) = self.scope.parent.take() {
            self.scope = *next;
            Ok(())
        } else {
            Err("No parent scope".into())
        }
    }
    /// Finds something from the module script
    pub fn get<Target>(&self, id: &NodeId) -> Result<Option<Target>>
    where
        Target: 'script,
        ModuleManager: module::Get<Target>,
        ModuleContent<'script>: module::GetModule<Target>,
    {
        Ok(if id.module.is_empty() {
            self.scope.content.get(&id.id)
        } else {
            self.resolve_module_alias(id)?
                .and_then(ModuleManager::get_tpl)
        })
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
    pub(crate) fn add_query_decl_doc<N: ToString>(
        &mut self,
        name: &N,
        doc: Option<Vec<Cow<'script, str>>>,
    ) {
        let doc = doc.map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n"));
        self.docs.query_decls.push(QueryDeclDoc {
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

    pub(crate) fn register_shadow_var(&mut self, id: &str) -> usize {
        let r = self.reserve_shadow();
        self.shadowed_vars.push(id.to_string());
        r
    }

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

    pub(crate) fn reserve_shadow(&mut self) -> usize {
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

    pub(crate) fn warn<S: ToString>(&mut self, inner: Span, outer: Span, msg: &S) {
        self.warnings.insert(Warning::new(inner, outer, msg));
    }
    pub(crate) fn warn_with_scope<S: ToString>(&mut self, r: Span, msg: &S) {
        self.warnings.insert(Warning::new_with_scope(r, msg));
    }
}

fn shadow_name(id: usize) -> String {
    format!(" __SHADOW {}__ ", id)
}
