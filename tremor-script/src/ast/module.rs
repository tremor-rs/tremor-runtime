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
    errors::{already_defined_err, Error, Kind as ErrorKind, Result},
    impl_expr,
    lexer::{Span, Tokenizer},
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
        match self {
            ModuleStmtRaw::Flow(e) => e.meta(),
            ModuleStmtRaw::Connector(e) => e.meta(),
            ModuleStmtRaw::Const(e) => e.meta(),
            ModuleStmtRaw::FnDecl(e) => e.meta(),
            ModuleStmtRaw::Pipeline(e) => e.meta(),
            ModuleStmtRaw::Use(e) => e.meta(),
            ModuleStmtRaw::Window(e) => e.meta(),
            ModuleStmtRaw::Operator(e) => e.meta(),
            ModuleStmtRaw::Script(e) => e.meta(),
        }
    }
}

pub type ModuleStmtsRaw<'script> = Vec<ModuleStmtRaw<'script>>;

/// we're forced to make this pub because of lalrpop
#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct ModuleRaw<'script> {
    pub name: IdentRaw<'script>,
    pub stmts: ModuleStmtsRaw<'script>,
    pub doc: Option<Vec<Cow<'script, str>>>,
    pub mid: Box<NodeMeta>,
}
impl_expr!(ModuleRaw);

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
        if self.flows.contains_key(&name) {
            Err(already_defined_err(&flow, "flow"))
        } else {
            self.flows.insert(name, flow);
            Ok(())
        }
    }
    pub(crate) fn insert_connector(
        &mut self,
        connector: ConnectorDefinition<'script>,
    ) -> Result<()> {
        let name = connector.node_id.id.clone();
        if self.connectors.contains_key(&name) {
            Err(already_defined_err(&connector, "connector"))
        } else {
            self.connectors.insert(name, connector);
            Ok(())
        }
    }
    pub(crate) fn insert_const(&mut self, c: Const<'script>) -> Result<()> {
        let name = c.name.clone();
        if self.consts.contains_key(&name) {
            Err(already_defined_err(&c, "constant"))
        } else {
            self.consts.insert(name, c);
            Ok(())
        }
    }
    pub(crate) fn insert_function(&mut self, f: FnDecl<'script>) -> Result<()> {
        let name = f.name.to_string();
        if self.functions.contains_key(&name) {
            Err(already_defined_err(&f, "function"))
        } else {
            self.functions.insert(name, f);
            Ok(())
        }
    }
    pub(crate) fn insert_pipeline(&mut self, pipeline: PipelineDefinition<'script>) -> Result<()> {
        let name = pipeline.node_id.id.clone();
        if self.pipelines.contains_key(&name) {
            Err(already_defined_err(&pipeline, "pipeline"))
        } else {
            self.pipelines.insert(name, pipeline);
            Ok(())
        }
    }

    pub(crate) fn insert_window(&mut self, window: WindowDefinition<'script>) -> Result<()> {
        let name = window.node_id.id.clone();
        if self.windows.contains_key(&name) {
            Err(already_defined_err(&window, "window"))
        } else {
            self.windows.insert(name, window);
            Ok(())
        }
    }
    pub(crate) fn insert_operator(&mut self, operator: OperatorDefinition<'script>) -> Result<()> {
        let name = operator.node_id.id.clone();
        if self.operators.contains_key(&name) {
            Err(already_defined_err(&operator, "operator"))
        } else {
            self.operators.insert(name, operator);
            Ok(())
        }
    }
    pub(crate) fn insert_script(&mut self, script: ScriptDefinition<'script>) -> Result<()> {
        let name = script.node_id.id.clone();
        if let Some(old) = self.scripts.insert(name, script) {
            Err(already_defined_err(&old, "script"))
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
        ids: &mut Vec<(ModuleId, String)>,
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
                ModuleStmtRaw::Use(UseRaw {
                    alias,
                    module,
                    mid: meta,
                }) => {
                    let mid = ModuleManager::load_(&module, ids);
                    match mid {
                        Err(Error(ErrorKind::CyclicUse(_, _, uses), o)) => {
                            return Err(Error(
                                ErrorKind::CyclicUse(meta.range, meta.range, uses),
                                o,
                            ));
                        }
                        Err(e) => {
                            return Err(e);
                        }
                        Ok(mid) => {
                            let alias = alias.unwrap_or_else(|| module.id.clone());
                            helper.scope().add_module_alias(alias, mid);
                        }
                    }
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

/// Get something from a module manager
pub trait Get<Target> {
    fn get(&self, m: Index, id: &str) -> Option<Target>;
    fn get_tpl(&self, (m, id): (Index, &str)) -> Option<Target> {
        self.get(m, id)
    }
}

/// Get something from a module
impl<'target, Target> Get<Target> for ModuleManager
where
    ModuleContent<'target>: GetModule<Target>,
    Target: Clone,
{
    fn get(&self, module: Index, name: &str) -> Option<Target> {
        self.modules.get(module.0)?.content.get(name)
    }
}

pub trait GetModule<Target> {
    fn get(&self, id: &str) -> Option<Target>;
}

impl<'module> GetModule<WindowDefinition<'module>> for ModuleContent<'module> {
    fn get(&self, name: &str) -> Option<WindowDefinition<'module>> {
        self.windows.get(name).cloned()
    }
}

impl<'module> GetModule<Const<'module>> for ModuleContent<'module> {
    fn get(&self, name: &str) -> Option<Const<'module>> {
        self.consts.get(name).cloned()
    }
}
impl<'module> GetModule<ConnectorDefinition<'module>> for ModuleContent<'module> {
    fn get(&self, name: &str) -> Option<ConnectorDefinition<'module>> {
        self.connectors.get(name).cloned()
    }
}
impl<'module> GetModule<ScriptDefinition<'module>> for ModuleContent<'module> {
    fn get(&self, name: &str) -> Option<ScriptDefinition<'module>> {
        self.scripts.get(name).cloned()
    }
}

impl<'module> GetModule<OperatorDefinition<'module>> for ModuleContent<'module> {
    fn get(&self, name: &str) -> Option<OperatorDefinition<'module>> {
        self.operators.get(name).cloned()
    }
}

impl<'module> GetModule<FnDecl<'module>> for ModuleContent<'module> {
    fn get(&self, name: &str) -> Option<FnDecl<'module>> {
        self.functions.get(name).cloned()
    }
}

impl<'module> GetModule<PipelineDefinition<'module>> for ModuleContent<'module> {
    fn get(&self, name: &str) -> Option<PipelineDefinition<'module>> {
        self.pipelines.get(name).cloned()
    }
}

impl<'module> GetModule<FlowDefinition<'module>> for ModuleContent<'module> {
    fn get(&self, name: &str) -> Option<FlowDefinition<'module>> {
        self.flows.get(name).cloned()
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
    /// removes all module load locations
    pub fn clear_path() -> Result<()> {
        MODULES.write()?.path.clear();
        Ok(())
    }

    /// Addas a module path
    pub fn add_path<S: ToString>(path: S) -> Result<()> {
        MODULES.write()?.path.add(path);
        Ok(())
    }
    /// shows modules
    pub(crate) fn modules(&self) -> &[Module] {
        &self.modules
    }

    pub(crate) fn find_module(mut root: Index, nest: &[String]) -> Result<Option<Index>> {
        let ms = MODULES.read()?;
        for k in nest {
            let m = if let Some(m) = ms.modules.get(root.0) {
                m
            } else {
                return Ok(None);
            };
            root = if let Some(m) = m.modules.get(k) {
                *m
            } else {
                return Ok(None);
            };
        }
        Ok(Some(root))
    }

    pub(crate) fn load(node_id: &NodeId) -> Result<Index> {
        let mut ids = Vec::new();
        ModuleManager::load_(node_id, &mut ids)
    }

    fn load_(node_id: &NodeId, ids: &mut Vec<(ModuleId, String)>) -> Result<Index> {
        let m = MODULES.read()?;
        let path = &m.path;

        let p = path.resolve_id(node_id).ok_or_else(|| {
            crate::errors::ErrorKind::ModuleNotFound(
                Span::default(),
                Span::default(),
                node_id.fqn(),
                path.mounts.clone(),
            )
        })?;
        drop(m);

        let src = std::fs::read_to_string(&p)?;
        let id = ModuleId::from(src.as_bytes());
        if ids.iter().any(|(other, _)| &id == other) {
            return Err(ErrorKind::CyclicUse(
                Span::default(),
                Span::default(),
                ids.iter().map(|v| &v.1).cloned().collect(),
            )
            .into());
        }
        ids.push((id.clone(), p.to_string_lossy().to_string()));

        let m = MODULES.read()?;
        let maybe_id = m
            .modules()
            .iter()
            .enumerate()
            .find(|(_, m)| m.id == id)
            .map(|(i, _)| i);
        drop(m);
        dbg!(maybe_id);
        if let Some(id) = maybe_id {
            Ok(Index(id))
        } else {
            let mid = node_id.to_vec();
            let (aid, src) = Arena::insert(src)?;
            let m = Module::load(id, ids, aid, src, mid)?;

            let mut mm = MODULES.write()?;

            let n = mm.modules.len();
            mm.modules.push(m);

            Ok(Index(n))
        }
    }

    pub(crate) fn get<Target>(module: Index, name: &str) -> Option<Target>
    where
        ModuleManager: Get<Target>,
    {
        let ms = MODULES.read().unwrap();
        ms.get(module, name)
    }
    pub(crate) fn get_tpl<Target>(tpl: (Index, &str)) -> Option<Target>
    where
        ModuleManager: Get<Target>,
    {
        let ms = MODULES.read().unwrap();
        ms.get_tpl(tpl)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn load_twice() -> Result<()> {
        ModuleManager::add_path("./lib")?;
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
        ModuleManager::add_path("./tests/modules")?;
        ModuleManager::load(&NodeId {
            id: "outside".to_string(),
            module: vec![],
        })?;
        Ok(())
    }
    #[test]
    fn load_from_id() -> Result<()> {
        ModuleManager::add_path("./lib")?;

        ModuleManager::load(&NodeId {
            id: "string".to_string(),
            module: vec!["std".to_string()],
        })?;
        Ok(())
    }
}
