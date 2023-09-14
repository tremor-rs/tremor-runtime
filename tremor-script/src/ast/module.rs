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
// We want to keep the names here
#![allow(clippy::module_name_repetitions)]

use super::{
    base_expr::Ranged,
    deploy::raw::{ConnectorDefinitionRaw, FlowDefinitionRaw},
    docs::{Docs, ModDoc},
    query::raw::{
        OperatorDefinitionRaw, PipelineDefinitionRaw, ScriptDefinitionRaw, WindowDefinitionRaw,
    },
    raw::{AnyFnRaw, ConstRaw, IdentRaw, UseRaw},
    upable::Upable,
    BaseExpr, ConnectorDefinition, Const, FlowDefinition, FnDefn, Helper, NodeId, NodeMeta,
    OperatorDefinition, PipelineDefinition, ScriptDefinition, WindowDefinition,
};
use crate::{
    arena::{self, Arena},
    errors::{already_defined_err, Error, Kind as ErrorKind, Result},
    impl_expr,
    lexer::{Lexer, Span},
    path::ModulePath,
    FN_REGISTRY,
};
use beef::Cow;
use sha2::Digest;
use std::collections::btree_map::Entry;
use std::{collections::BTreeMap, fmt::Debug};

use std::sync::RwLock;
lazy_static::lazy_static! {
    static ref MODULES: RwLock<Manager> = RwLock::new(Manager::default());
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
    FnDefn(AnyFnRaw<'script>),
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
            ModuleStmtRaw::FnDefn(e) => e.meta(),
            ModuleStmtRaw::Pipeline(e) => e.meta(),
            ModuleStmtRaw::Use(e) => e.meta(),
            ModuleStmtRaw::Window(e) => e.meta(),
            ModuleStmtRaw::Operator(e) => e.meta(),
            ModuleStmtRaw::Script(e) => e.meta(),
        }
    }
}

pub(crate) type ModuleStmtsRaw<'script> = Vec<ModuleStmtRaw<'script>>;

/// we're forced to make this pub because of lalrpop
#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct ModuleRaw<'script> {
    pub(crate) name: IdentRaw<'script>,
    pub(crate) stmts: ModuleStmtsRaw<'script>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(ModuleRaw);

/// module id
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Id(Vec<u8>);

type NamedEnteties<T> = BTreeMap<String, T>;

/// Content of a module
#[derive(Default, Clone, Serialize, PartialEq)]
pub struct Content<'script> {
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
    pub functions: NamedEnteties<FnDefn<'script>>,
}

impl<'script> Debug for Content<'script> {
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

impl<'script> Content<'script> {
    pub(crate) fn insert_flow(&mut self, elem: FlowDefinition<'script>) -> Result<()> {
        let name = elem.id.clone();
        if let Entry::Vacant(e) = self.flows.entry(name) {
            e.insert(elem);
            Ok(())
        } else {
            Err(already_defined_err(&elem, "flow"))
        }
    }
    pub(crate) fn insert_connector(&mut self, elem: ConnectorDefinition<'script>) -> Result<()> {
        let name = elem.id.clone();
        if let Entry::Vacant(e) = self.connectors.entry(name) {
            e.insert(elem);
            Ok(())
        } else {
            Err(already_defined_err(&elem, "connector"))
        }
    }
    pub(crate) fn insert_const(&mut self, elem: Const<'script>) -> Result<()> {
        let name = elem.id.clone();
        if let Entry::Vacant(e) = self.consts.entry(name) {
            e.insert(elem);
            Ok(())
        } else {
            Err(already_defined_err(&elem, "const"))
        }
    }
    pub(crate) fn insert_function(&mut self, elem: FnDefn<'script>) -> Result<()> {
        let name = elem.name.clone();
        if let Entry::Vacant(e) = self.functions.entry(name) {
            e.insert(elem);
            Ok(())
        } else {
            Err(already_defined_err(&elem, "function"))
        }
    }
    pub(crate) fn insert_pipeline(&mut self, elem: PipelineDefinition<'script>) -> Result<()> {
        let name = elem.id.clone();
        if let Entry::Vacant(e) = self.pipelines.entry(name) {
            e.insert(elem);
            Ok(())
        } else {
            Err(already_defined_err(&elem, "pipeline"))
        }
    }

    pub(crate) fn insert_window(&mut self, elem: WindowDefinition<'script>) -> Result<()> {
        let name = elem.id.clone();
        if let Entry::Vacant(e) = self.windows.entry(name) {
            e.insert(elem);
            Ok(())
        } else {
            Err(already_defined_err(&elem, "window"))
        }
    }
    pub(crate) fn insert_operator(&mut self, elem: OperatorDefinition<'script>) -> Result<()> {
        let name = elem.id.clone();
        if let Entry::Vacant(e) = self.operators.entry(name) {
            e.insert(elem);
            Ok(())
        } else {
            Err(already_defined_err(&elem, "operator"))
        }
    }
    pub(crate) fn insert_script(&mut self, elem: ScriptDefinition<'script>) -> Result<()> {
        let name = elem.id.clone();
        if let Entry::Vacant(e) = self.scripts.entry(name) {
            e.insert(elem);
            Ok(())
        } else {
            Err(already_defined_err(&elem, "script"))
        }
    }
}

// This is a self referential struct, beware
/// Tremor `use`-able module representation
#[derive(Debug, Clone)]
pub struct Module {
    /// module identifier
    pub id: Id,
    /// module documentation
    pub docs: Docs,
    /// module contents
    pub content: Content<'static>,
    /// additionally loaded modules
    pub modules: BTreeMap<String, Index>,
}

impl From<&[u8]> for Id {
    fn from(src: &[u8]) -> Self {
        Id(sha2::Sha512::digest(src).to_vec())
    }
}

#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
/// Module Index
pub struct Index(usize);

impl Module {
    /// Load a module
    ///
    /// this happens when a module is `use`d
    ///
    /// # Errors
    /// If parsing the module fails.
    pub fn load(
        id: Id,
        ids: &mut Vec<(Id, String)>,
        arena_idx: arena::Index,
        src: &'static str,
    ) -> Result<Self> {
        let aggr_reg = crate::aggr_registry();
        let reg = &*FN_REGISTRY.read()?;
        let mut helper = Helper::new(reg, &aggr_reg);

        let lexemes = Lexer::new(src, arena_idx)
            .filter_map(std::result::Result::ok)
            .filter(|t| !t.value.is_ignorable());
        let raw: ModuleRaw<'static> = crate::parser::g::ModuleFileParser::new().parse(lexemes)?;

        for s in raw.stmts {
            match s {
                ModuleStmtRaw::Use(UseRaw { modules, mid: meta }) => {
                    for (module, alias) in modules {
                        match Manager::load_(&module, ids) {
                            Err(Error(ErrorKind::CyclicUse(_, _, uses), o)) => {
                                return Err(Error(
                                    ErrorKind::CyclicUse(meta.range, meta.range, uses),
                                    o,
                                ));
                            }
                            Err(e) => {
                                return Err(e);
                            }
                            Ok(mod_idx) => {
                                let alias = alias.unwrap_or_else(|| module.id.clone());
                                helper.scope().add_module_alias(alias, mod_idx);
                            }
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
                ModuleStmtRaw::FnDefn(e) => {
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
        let module_docs = raw
            .doc
            .map(|docs| docs.into_iter().map(Cow::into_owned).collect::<Vec<_>>());
        let Helper {
            scope, mut docs, ..
        } = helper;
        docs.module = Some(ModDoc {
            name: raw.name.id.to_string(),
            doc: module_docs.map(|lines| lines.join("\n")),
        });
        Ok(Module {
            id,
            docs,
            content: scope.content,
            modules: scope.modules,
        })
    }
}

/// Get something from a module manager
pub trait Get<Target> {
    /// Gets an item from a module and scope
    fn get(&self, m: Index, id: &str) -> Option<Target>;
}

/// Get something from a module
impl<'target, Target> Get<Target> for Manager
where
    Content<'target>: GetMod<Target>,
    Target: Clone,
{
    fn get(&self, module: Index, name: &str) -> Option<Target> {
        self.modules.get(module.0)?.content.get(name)
    }
}

/// Something that is gettable from a module
pub trait GetMod<Target> {
    /// Gets an item from a  scope
    fn get(&self, id: &str) -> Option<Target>;
}

impl<'module> GetMod<WindowDefinition<'module>> for Content<'module> {
    fn get(&self, name: &str) -> Option<WindowDefinition<'module>> {
        self.windows.get(name).cloned()
    }
}

impl<'module> GetMod<Const<'module>> for Content<'module> {
    fn get(&self, name: &str) -> Option<Const<'module>> {
        self.consts.get(name).cloned()
    }
}
impl<'module> GetMod<ConnectorDefinition<'module>> for Content<'module> {
    fn get(&self, name: &str) -> Option<ConnectorDefinition<'module>> {
        self.connectors.get(name).cloned()
    }
}
impl<'module> GetMod<ScriptDefinition<'module>> for Content<'module> {
    fn get(&self, name: &str) -> Option<ScriptDefinition<'module>> {
        self.scripts.get(name).cloned()
    }
}

impl<'module> GetMod<OperatorDefinition<'module>> for Content<'module> {
    fn get(&self, name: &str) -> Option<OperatorDefinition<'module>> {
        self.operators.get(name).cloned()
    }
}

impl<'module> GetMod<FnDefn<'module>> for Content<'module> {
    fn get(&self, name: &str) -> Option<FnDefn<'module>> {
        self.functions.get(name).cloned()
    }
}

impl<'module> GetMod<PipelineDefinition<'module>> for Content<'module> {
    fn get(&self, name: &str) -> Option<PipelineDefinition<'module>> {
        self.pipelines.get(name).cloned()
    }
}

impl<'module> GetMod<FlowDefinition<'module>> for Content<'module> {
    fn get(&self, name: &str) -> Option<FlowDefinition<'module>> {
        self.flows.get(name).cloned()
    }
}

/// Global Module Manager
#[derive(Default, Debug)]
pub struct Manager {
    path: ModulePath,
    modules: Vec<Module>,
}

impl Manager {
    /// removes all module load locations
    /// # Errors
    /// if the module global can't be aquired
    pub fn clear_path() -> Result<()> {
        MODULES.write()?.path.clear();
        Ok(())
    }

    /// Addas a module path
    /// # Errors
    /// if the module global can't be aquired
    pub fn add_path<S: ToString>(path: &S) -> Result<()> {
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
            let Some(m) = ms.modules.get(root.0) else {
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
        Manager::load_(node_id, &mut ids)
    }

    fn load_(node_id: &NodeId, ids: &mut Vec<(Id, String)>) -> Result<Index> {
        let m = MODULES.read()?;
        let path = &m.path;

        let p = path.resolve_id(node_id).ok_or_else(|| {
            crate::errors::ErrorKind::ModuleNotFound(
                node_id.extent().expand_lines(2),
                node_id.extent(),
                node_id.fqn(),
                path.mounts.clone(),
            )
        })?;
        drop(m);

        let src = std::fs::read_to_string(&p)?;
        let id = Id::from(src.as_bytes());
        if ids.iter().any(|(other, _)| &id == other) {
            return Err(ErrorKind::CyclicUse(
                Span::yolo(),
                Span::yolo(),
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
        let r = if let Some(id) = maybe_id {
            Ok(Index(id))
        } else {
            let (arena_idx, src) = Arena::insert(&src)?;
            let m = Module::load(id, ids, arena_idx, src)?;

            let mut mm = MODULES.write()?;

            let n = mm.modules.len();
            mm.modules.push(m);

            Ok(Index(n))
        };
        ids.pop();
        r
    }

    pub(crate) fn get<Target>(module: Index, name: &str) -> Result<Option<Target>>
    where
        Manager: Get<Target>,
    {
        let ms = MODULES.read()?;
        Ok(ms.get(module, name))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn load_twice() -> Result<()> {
        Manager::add_path(&"./tests/modules")?;
        let id1 = Manager::load(&NodeId {
            id: "twice".to_string(),
            module: vec!["loading".into()],
            mid: NodeMeta::dummy(),
        })?;
        let id2 = Manager::load(&NodeId {
            id: "twice".to_string(),
            module: vec!["loading".into()],
            mid: NodeMeta::dummy(),
        })?;
        assert_eq!(id1, id2);
        Ok(())
    }
    #[test]
    fn load_nested() -> Result<()> {
        Manager::add_path(&"./tests/modules")?;
        Manager::load(&NodeId {
            id: "outside".to_string(),
            module: vec![],
            mid: NodeMeta::dummy(),
        })?;
        Ok(())
    }
    #[test]
    fn load_from_id() -> Result<()> {
        Manager::add_path(&"./lib")?;

        Manager::load(&NodeId {
            id: "string".to_string(),
            module: vec!["std".to_string()],
            mid: NodeMeta::dummy(),
        })?;
        Ok(())
    }
}
