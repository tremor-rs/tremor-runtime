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
#![allow(dead_code)]

use super::{
    deploy::raw::{ConnectorDefinitionRaw, FlowDefinitionRaw},
    helper::raw::{
        OperatorDefinitionRaw, PipelineDefinitionRaw, ScriptDefinitionRaw, WindowDefinitionRaw,
    },
    raw::{AnyFnRaw, ConstRaw, IdentRaw, UseRaw},
    upable::Upable,
    BaseExpr, ConnectorDefinition, Const, FlowDefinition, FnDecl, Helper, NodeId, NodeMeta,
    OperatorDefinition, PipelineDefinition, ScriptDefinition, WindowDefinition,
};
use crate::{
    arena::{self, Arena},
    errors::Result,
    impl_expr_raw,
    lexer::{Location, Tokenizer},
    path::ModulePath,
    FN_REGISTRY,
};
use beef::Cow;
use sha2::Digest;
use std::mem::transmute;
use std::{collections::HashMap, fmt::Debug};

use std::sync::RwLock;
lazy_static::lazy_static! {
    static ref MODULES: RwLock<ModuleManager> = RwLock::new(ModuleManager::default());
}
/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ModuleStmtRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Flow(FlowDefinitionRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Connector(ConnectorDefinitionRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Const(ConstRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    FnDecl(AnyFnRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Pipeline(PipelineDefinitionRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Use(UseRaw),
    /// we're forced to make this pub because of lalrpop
    Window(WindowDefinitionRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Operator(OperatorDefinitionRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Script(ScriptDefinitionRaw<'script>),
}
impl<'script> BaseExpr for ModuleStmtRaw<'script> {
    fn meta(&self) -> &NodeMeta {
        todo!()
    }

    fn s(&self) -> Location {
        match self {
            ModuleStmtRaw::Flow(e) => e.s(),
            ModuleStmtRaw::Connector(e) => e.s(),
            ModuleStmtRaw::Const(e) => e.s(),
            ModuleStmtRaw::FnDecl(e) => e.s(),
            ModuleStmtRaw::Pipeline(e) => e.s(),
            ModuleStmtRaw::Use(e) => e.s(),
            ModuleStmtRaw::Window(e) => e.s(),
            ModuleStmtRaw::Operator(e) => e.s(),
            ModuleStmtRaw::Script(e) => e.s(),
        }
    }

    fn e(&self) -> Location {
        match self {
            ModuleStmtRaw::Flow(e) => e.e(),
            ModuleStmtRaw::Connector(e) => e.e(),
            ModuleStmtRaw::Const(e) => e.e(),
            ModuleStmtRaw::FnDecl(e) => e.e(),
            ModuleStmtRaw::Pipeline(e) => e.e(),
            ModuleStmtRaw::Use(e) => e.e(),
            ModuleStmtRaw::Window(e) => e.e(),
            ModuleStmtRaw::Operator(e) => e.e(),
            ModuleStmtRaw::Script(e) => e.e(),
        }
    }
}

pub type ModuleStmtsRaw<'script> = Vec<ModuleStmtRaw<'script>>;

/// we're forced to make this pub because of lalrpop
#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct ModuleRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub name: IdentRaw<'script>,
    pub stmts: ModuleStmtsRaw<'script>,
    pub doc: Option<Vec<Cow<'script, str>>>,
}
impl_expr_raw!(ModuleRaw);

/// module id
#[derive(Debug, Clone, PartialEq)]
pub struct ModuleId(Vec<u8>);

type NamedEnteties<T> = HashMap<String, T>;

/// Content of a module
#[derive(Default, Clone, Serialize, PartialEq)]
pub struct ModuleContent<'script> {
    /// connectors in this module
    pub connectors: NamedEnteties<ConnectorDefinition<'script>>,
    /// pipelines in this module
    pub pipelines: NamedEnteties<PipelineDefinition<'script>>,
    /// windows in this module
    pub windows: NamedEnteties<WindowDefinition<'script>>,
    /// scripts in this module
    pub scripts: NamedEnteties<ScriptDefinition<'script>>,
    /// operators in this module
    pub operators: NamedEnteties<OperatorDefinition<'script>>,
    /// flows in this module
    pub flows: NamedEnteties<FlowDefinition<'script>>,
    /// consts in this module
    pub consts: NamedEnteties<Const<'script>>,
    /// functions in this module
    pub functions: NamedEnteties<FnDecl<'script>>,
}

impl<'script> Debug for ModuleContent<'script> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModuleContent")
            .field("connectors", &self.connectors.keys())
            .field("pipelines", &self.pipelines.keys())
            .field("windows", &self.windows.keys())
            .field("scripts", &self.scripts.keys())
            .field("operators", &self.operators.keys())
            .field("flows", &self.flows.keys())
            .field("consts", &self.consts.keys())
            .field("functions", &self.functions.keys())
            .finish()
    }
}

impl<'script> ModuleContent<'script> {
    pub(crate) fn insert_flow(&mut self, flow: FlowDefinition<'script>) -> Result<()> {
        let name = flow.node_id.id.clone();
        if let Some(_old) = self.flows.insert(name.clone(), flow) {
            Err("FIXME: already defined".into())
        } else {
            Ok(())
        }
    }
    pub(crate) fn insert_connector(
        &mut self,
        connector: ConnectorDefinition<'script>,
    ) -> Result<()> {
        let name = connector.node_id.id.clone();
        if let Some(_old) = self.connectors.insert(name.clone(), connector) {
            Err("FIXME: already defined".into())
        } else {
            Ok(())
        }
    }
    pub(crate) fn insert_const(&mut self, c: Const<'script>) -> Result<()> {
        let name = c.name.clone();
        if let Some(old) = self.consts.insert(name.clone(), c) {
            Err(format!("FIXME: already defined: {}", old.name).into())
        } else {
            Ok(())
        }
    }
    pub(crate) fn insert_function(&mut self, f: FnDecl<'script>) -> Result<()> {
        let name = f.name.to_string();
        if let Some(_old) = self.functions.insert(name, f) {
            Err("FIXME: already defined".into())
        } else {
            Ok(())
        }
    }
    pub(crate) fn insert_pipeline(&mut self, pipeline: PipelineDefinition<'script>) -> Result<()> {
        let name = pipeline.node_id.id.clone();
        if let Some(_old) = self.pipelines.insert(name, pipeline) {
            Err("FIXME: already defined".into())
        } else {
            Ok(())
        }
    }

    pub(crate) fn insert_window(&mut self, window: WindowDefinition<'script>) -> Result<()> {
        let name = window.node_id.id.clone();
        if let Some(_old) = self.windows.insert(name, window) {
            Err("FIXME: already defined".into())
        } else {
            Ok(())
        }
    }
    pub(crate) fn insert_operator(&mut self, operator: OperatorDefinition<'script>) -> Result<()> {
        let name = operator.node_id.id.clone();
        if let Some(_old) = self.operators.insert(name, operator) {
            Err("FIXME: already defined".into())
        } else {
            Ok(())
        }
    }
    pub(crate) fn insert_script(&mut self, script: ScriptDefinition<'script>) -> Result<()> {
        let name = script.node_id.id.clone();
        if let Some(_old) = self.scripts.insert(name, script) {
            Err("FIXME: already defined".into())
        } else {
            Ok(())
        }
    }
}

// This is a self referential struct, beware
#[derive(Debug, Clone)]
pub(crate) struct Module {
    pub(crate) name: Vec<String>,
    pub(crate) id: ModuleId,
    pub(crate) content: ModuleContent<'static>,
    pub(crate) modules: HashMap<String, Index>,
}

impl From<&[u8]> for ModuleId {
    fn from(src: &[u8]) -> Self {
        ModuleId(sha2::Sha512::digest(src).to_vec())
    }
}

#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
/// Module Index
pub struct Index(usize);

impl Module {
    pub(crate) fn insert_flow(&mut self, flow: FlowDefinition<'static>) -> Result<()> {
        self.content.insert_flow(flow)
    }
    pub(crate) fn insert_connector(
        &mut self,
        connector: ConnectorDefinition<'static>,
    ) -> Result<()> {
        self.content.insert_connector(connector)
    }

    pub fn load(
        id: ModuleId,
        aid: arena::Index,
        src: &'static str,
        name: Vec<String>,
    ) -> Result<Self> {
        let aggr_reg = crate::aggr_registry();
        let reg = &*FN_REGISTRY.read()?; // FIXME
        let mut helper = Helper::new(&reg, &aggr_reg);

        let lexemes = Tokenizer::new(&src, aid)
            .filter_map(std::result::Result::ok)
            .filter(|t| !t.value.is_ignorable());
        let raw: ModuleRaw = crate::parser::g::ModuleFileParser::new().parse(lexemes)?;
        let raw = unsafe { transmute::<ModuleRaw<'_>, ModuleRaw<'static>>(raw) };

        for s in raw.stmts {
            match s {
                ModuleStmtRaw::Use(UseRaw { alias, module, .. }) => {
                    // FIXME: prevent self inclusion
                    let mid = ModuleManager::load(&module)?;
                    let alias = alias.unwrap_or_else(|| module.id.clone());
                    helper.scope().add_module_alias(alias, mid);
                }
                ModuleStmtRaw::Flow(e) => {
                    let e = e.up(&mut helper)?;
                    helper.scope.insert_flow(e)?;
                }
                ModuleStmtRaw::Connector(e) => {
                    let e = e.up(&mut helper)?;
                    helper.scope.insert_connector(e)?;
                }
                ModuleStmtRaw::Const(e) => {
                    let e = e.up(&mut helper)?;
                    helper.scope.insert_const(e)?;
                }
                ModuleStmtRaw::FnDecl(e) => {
                    let e = e.up(&mut helper)?;
                    helper.scope.insert_function(e)?;
                }

                ModuleStmtRaw::Pipeline(e) => {
                    let e = e.up(&mut helper)?;
                    helper.scope.insert_pipeline(e)?;
                }

                ModuleStmtRaw::Window(e) => {
                    let e = e.up(&mut helper)?;
                    helper.scope.insert_window(e)?;
                }
                ModuleStmtRaw::Operator(e) => {
                    let e = e.up(&mut helper)?;
                    helper.scope.insert_operator(e)?;
                }
                ModuleStmtRaw::Script(e) => {
                    let e = e.up(&mut helper)?;
                    helper.scope.insert_script(e)?;
                }
            }
        }
        Ok(Module {
            id,
            name,
            content: helper.scope.content,
            modules: helper.scope.modules,
        })
    }
}

/// Global Module Manager
#[derive(Default, Debug)]
pub struct ModuleManager {
    path: ModulePath,
    modules: Vec<Module>,
}

// FIXME: unwraps
impl ModuleManager {
    /// Addas a module path
    pub fn add_path<S: ToString>(path: S) {
        MODULES.write().unwrap().path.add(path);
    }
    /// shows modules
    pub(crate) fn modules(&self) -> &[Module] {
        &self.modules
    }

    pub(crate) fn find_module(mut root: Index, nest: &[String]) -> Option<Index> {
        let ms = MODULES.read().unwrap();
        for k in nest {
            let m = ms.modules.get(root.0)?;
            root = *m.modules.get(k)?;
        }
        Some(root)
    }

    pub(crate) fn load(node_id: &NodeId) -> Result<Index> {
        let p = MODULES
            .read()
            .unwrap()
            .path
            .resolve_id(node_id)
            .ok_or_else(|| format!("module {} not found", node_id))?;
        let src = std::fs::read_to_string(&p)?;
        let id = ModuleId::from(src.as_bytes());

        let maybe_id = MODULES
            .read()
            .unwrap()
            .modules()
            .iter()
            .enumerate()
            .find(|(_, m)| m.id == id)
            .map(|(i, _)| i);
        if let Some(id) = maybe_id {
            Ok(Index(id))
        } else {
            let mid = node_id.to_vec();
            let (aid, src) = Arena::insert(src)?;
            let m = Module::load(id, aid, src, mid)?;
            let mut mm = MODULES.write().unwrap(); // FIXME
            let n = mm.modules.len();
            mm.modules.push(m);
            Ok(Index(n))
        }
    }
    pub(crate) fn get_flow(module: Index, name: &str) -> Option<FlowDefinition<'static>> {
        let ms = MODULES.read().unwrap();
        ms.modules().get(module.0)?.content.flows.get(name).cloned()
    }
    pub(crate) fn get_operator(module: Index, name: &str) -> Option<OperatorDefinition<'static>> {
        let ms = MODULES.read().unwrap();
        ms.modules()
            .get(module.0)?
            .content
            .operators
            .get(name)
            .cloned()
    }
    pub(crate) fn get_script(module: Index, name: &str) -> Option<ScriptDefinition<'static>> {
        let ms = MODULES.read().unwrap();
        ms.modules()
            .get(module.0)?
            .content
            .scripts
            .get(name)
            .cloned()
    }

    pub(crate) fn get_const(module: Index, name: &str) -> Option<Const<'static>> {
        let ms = MODULES.read().unwrap();
        ms.modules()
            .get(module.0)?
            .content
            .consts
            .get(name)
            .cloned()
    }
    pub(crate) fn get_connector(module: Index, name: &str) -> Option<ConnectorDefinition<'static>> {
        let ms = MODULES.read().unwrap();
        ms.modules()
            .get(module.0)?
            .content
            .connectors
            .get(name)
            .cloned()
    }
    pub(crate) fn get_pipeline(module: Index, name: &str) -> Option<PipelineDefinition<'static>> {
        let ms = MODULES.read().unwrap();
        ms.modules()
            .get(module.0)?
            .content
            .pipelines
            .get(name)
            .cloned()
    }

    pub(crate) fn get_function(module: Index, name: &str) -> Option<FnDecl<'static>> {
        let ms = MODULES.read().unwrap();
        ms.modules()
            .get(module.0)?
            .content
            .functions
            .get(name)
            .cloned()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn load_twice() -> Result<()> {
        ModuleManager::add_path("./lib");
        let id1 = ModuleManager::load(&NodeId {
            id: "string".to_string(),
            module: vec!["std".into()],
        })?;
        let id2 = ModuleManager::load(&NodeId {
            id: "string".to_string(),
            module: vec!["std".into()],
        })?;
        assert_eq!(id1, id2);
        Ok(())
    }
    #[test]
    fn load_nested() -> Result<()> {
        ModuleManager::add_path("./tests/modules");
        ModuleManager::load(&NodeId {
            id: "outside".to_string(),
            module: vec![],
        })?;
        Ok(())
    }
    #[test]
    fn load_from_id() -> Result<()> {
        ModuleManager::add_path("./lib");

        ModuleManager::load(&NodeId {
            id: "string".to_string(),
            module: vec!["std".to_string()],
        })?;
        Ok(())
    }
}
