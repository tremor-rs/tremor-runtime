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
    Consts, DeployFlow, FlowDefinition, InvokeAggrFn, NodeId, NodeMetas,
};
use crate::{
    errors::Result,
    pos::{Location, Range},
    prelude::*,
    registry::{Aggr as AggrRegistry, CustomFn, Registry},
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
    pub outer: Range,
    /// Inner span of thw warning
    pub inner: Range,
    /// Warning message
    pub msg: String,
}

impl Warning {
    fn new<T: ToString>(inner: Range, outer: Range, msg: &T) -> Self {
        Self {
            outer,
            inner,
            msg: msg.to_string(),
        }
    }
    fn new_with_scope<T: ToString>(warning_scope: Range, msg: &T) -> Self {
        Self::new(warning_scope, warning_scope, msg)
    }
}

/// Helper
#[allow(clippy::struct_excessive_bools)]
pub struct Helper<'script, 'registry>
where
    'script: 'registry,
{
    // NOTE We should refactor this into multiple structs as its becoming a bit of a
    // spaghetti junction over time the languages and runtime continue to evolve. What
    // we have today is no longer a helper, its essential, but badly factored for what
    // we need today and looking forward
    //
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
    pub(crate) func_vec: Vec<CustomFn<'script>>,
    pub(crate) locals: HashMap<String, usize>,
    pub(crate) functions: HashMap<Vec<String>, usize>,
    /// Runtime constant pool
    pub consts: Consts<'script>,
    /// AST Metadata
    pub meta: NodeMetas,
    pub(crate) docs: Docs,
    pub(crate) module: Vec<String>,
    pub(crate) possible_leaf: bool,
    pub(crate) fn_argc: usize,
    pub(crate) is_open: bool,
    pub(crate) file_offset: Location,
}

impl<'script, 'registry> Helper<'script, 'registry>
where
    'script: 'registry,
{
    pub(crate) fn get_flow_decls(&self, _: &NodeId) -> Option<FlowDefinition<'script>> {
        None
    }

    pub(crate) fn get_pipeline(&self, _: &NodeId) -> Option<PipelineDefinition<'script>> {
        None
    }
    pub(crate) fn get_script(&self, _: &NodeId) -> Option<ScriptDefinition<'script>> {
        None
    }

    pub(crate) fn is_const(&self, id: &[String]) -> Option<&usize> {
        self.consts.is_const(id)
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
    pub(crate) fn add_meta(&mut self, start: Location, end: Location) -> usize {
        // FIXME: cu
        self.meta
            .add_meta(start - self.file_offset, end - self.file_offset, 0)
    }
    pub(crate) fn add_meta_w_name<S>(&mut self, start: Location, end: Location, name: &S) -> usize
    where
        S: ToString,
    {
        self.meta.add_meta_w_name(
            start - self.file_offset,
            end - self.file_offset,
            name,
            0, // FIXME: self.cu,
        )
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
            // Troy
            instances: HashMap::new(),
            // Trickle
            aggregates: Vec::new(),
            // Common
            warnings: BTreeSet::new(),
            locals: HashMap::new(),
            consts: Consts::default(),
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
        }
    }

    pub(crate) fn register_fun(&mut self, f: CustomFn<'script>) -> Result<usize> {
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

    pub(crate) fn warn<S: ToString>(&mut self, inner: Range, outer: Range, msg: &S) {
        self.warnings.insert(Warning::new(inner, outer, msg));
    }
    pub(crate) fn warn_with_scope<S: ToString>(&mut self, r: Range, msg: &S) {
        self.warnings.insert(Warning::new_with_scope(r, msg));
    }
}

fn shadow_name(id: usize) -> String {
    format!(" __SHADOW {}__ ", id)
}
