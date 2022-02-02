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

///! FIXME
use super::{
    deploy::raw::{ConnectorDefinitionRaw, FlowDefinitionRaw},
    helper::raw::{
        OperatorDefinitionRaw, PipelineDefinitionRaw, ScriptDefinitionRaw, WindowDefinitionRaw,
    },
    raw::{AnyFnRaw, ConstRaw, IdentRaw, UseRaw},
    upable::Upable,
    BaseExpr, ConnectorDefinition, Const, FlowDefinition, FnDecl, Helper, NodeId,
    OperatorDefinition, PipelineDefinition, ScriptDefinition, WindowDefinition,
};
use crate::{
    errors::Result,
    impl_expr,
    lexer::{Location, Tokenizer},
    path::ModulePath,
};
use beef::Cow;
use sha2::Digest;
use std::mem::transmute;
use std::{
    collections::HashMap,
    fmt::Debug,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};

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
    fn mid(&self) -> usize {
        0
    }

    fn s(&self, meta: &super::NodeMetas) -> Location {
        match self {
            ModuleStmtRaw::Flow(e) => e.s(meta),
            ModuleStmtRaw::Connector(e) => e.s(meta),
            ModuleStmtRaw::Const(e) => e.s(meta),
            ModuleStmtRaw::FnDecl(e) => e.s(meta),
            ModuleStmtRaw::Pipeline(e) => e.s(meta),
            ModuleStmtRaw::Use(e) => e.s(meta),
            ModuleStmtRaw::Window(e) => e.s(meta),
            ModuleStmtRaw::Operator(e) => e.s(meta),
            ModuleStmtRaw::Script(e) => e.s(meta),
        }
    }

    fn e(&self, meta: &super::NodeMetas) -> Location {
        match self {
            ModuleStmtRaw::Flow(e) => e.e(meta),
            ModuleStmtRaw::Connector(e) => e.e(meta),
            ModuleStmtRaw::Const(e) => e.e(meta),
            ModuleStmtRaw::FnDecl(e) => e.e(meta),
            ModuleStmtRaw::Pipeline(e) => e.e(meta),
            ModuleStmtRaw::Use(e) => e.e(meta),
            ModuleStmtRaw::Window(e) => e.e(meta),
            ModuleStmtRaw::Operator(e) => e.e(meta),
            ModuleStmtRaw::Script(e) => e.e(meta),
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
impl_expr!(ModuleRaw);

/// module id
#[derive(Debug, Clone, PartialEq)]
pub struct ModuleId(Vec<u8>);

type NamedEnteties<T> = HashMap<String, T>;

#[derive(Default, Debug, Clone)]
pub(crate) struct ModuleContent<'script> {
    pub(crate) connectors: NamedEnteties<ConnectorDefinition<'script>>,
    pub(crate) pipelines: NamedEnteties<PipelineDefinition<'script>>,
    pub(crate) windows: NamedEnteties<WindowDefinition<'script>>,
    pub(crate) scripts: NamedEnteties<ScriptDefinition<'script>>,
    pub(crate) operators: NamedEnteties<OperatorDefinition<'script>>,
    pub(crate) flows: NamedEnteties<FlowDefinition<'script>>,
    pub(crate) consts: NamedEnteties<Const<'script>>,
    pub(crate) functions: NamedEnteties<FnDecl<'script>>,
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
    pub(crate) src: Arc<Pin<String>>,
    pub(crate) file_name: PathBuf,
    pub(crate) id: ModuleId,
    pub(crate) content: ModuleContent<'static>,
    pub(crate) modules: HashMap<String, usize>,
}

impl From<&[u8]> for ModuleId {
    fn from(src: &[u8]) -> Self {
        ModuleId(sha2::Sha512::digest(src).to_vec())
    }
}

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

    pub fn load<P>(
        id: ModuleId,
        file_name: P,
        src: Arc<Pin<String>>,
        name: Vec<String>,
    ) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        // FIXME this isn't a good id but good enough for testing
        // FIXME there are transmutes hers :sob:

        let aggr_reg = crate::aggr_registry();
        let reg = crate::registry(); // FIXME
        let mut helper = Helper::new(&reg, &aggr_reg, Vec::new());

        let lexemes = Tokenizer::new(&src)
            .filter_map(std::result::Result::ok)
            .filter(|t| !t.value.is_ignorable());
        let raw: ModuleRaw = crate::parser::g::ModuleFileParser::new().parse(lexemes)?;
        let raw = unsafe { transmute::<ModuleRaw<'_>, ModuleRaw<'static>>(raw) };
        let file_name: &Path = file_name.as_ref();

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
                    // The self referential nature comes into play here
                    let e = unsafe { transmute::<FlowDefinition<'_>, FlowDefinition<'static>>(e) };
                    helper.scope.insert_flow(e)?;
                }
                ModuleStmtRaw::Connector(e) => {
                    let e = e.up(&mut helper)?.into_static();
                    helper.scope.insert_connector(e)?;
                }
                ModuleStmtRaw::Const(e) => {
                    let e = e.up(&mut helper)?;
                    // The self referential nature comes into play here
                    let e = unsafe { transmute::<Const<'_>, Const<'static>>(e) };
                    helper.scope.insert_const(e)?;
                }
                ModuleStmtRaw::FnDecl(e) => {
                    let e = e.up(&mut helper)?;
                    // The self referential nature comes into play here
                    let e = unsafe { transmute::<FnDecl<'_>, FnDecl<'static>>(e) };
                    helper.scope.insert_function(e)?;
                }

                ModuleStmtRaw::Pipeline(e) => {
                    // FIXME? We can't do into static here
                    let e = e.up(&mut helper)?;
                    // The self referential nature comes into play here
                    let e = unsafe {
                        transmute::<PipelineDefinition<'_>, PipelineDefinition<'static>>(e)
                    };
                    helper.scope.insert_pipeline(e)?;
                }

                ModuleStmtRaw::Window(e) => {
                    let e = e.up(&mut helper)?.into_static();
                    helper.scope.insert_window(e)?;
                }
                ModuleStmtRaw::Operator(e) => {
                    let e = e.up(&mut helper)?.into_static();
                    helper.scope.insert_operator(e)?;
                }
                ModuleStmtRaw::Script(e) => {
                    // FIXME? We can't do into static here
                    let e = e.up(&mut helper)?;
                    // The self referential nature comes into play here
                    let e =
                        unsafe { transmute::<ScriptDefinition<'_>, ScriptDefinition<'static>>(e) };
                    helper.scope.insert_script(e)?;
                }
            }
        }
        Ok(Module {
            src,
            file_name: PathBuf::from(file_name),
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

    pub(crate) fn find_module(mut root: usize, nest: &[String]) -> Option<usize> {
        let ms = MODULES.read().unwrap();
        for k in nest {
            let m = ms.modules.get(root)?;
            root = *m.modules.get(k)?;
        }
        Some(root)
    }

    pub(crate) fn load(node_id: &NodeId) -> Result<usize> {
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
            Ok(dbg!(id))
        } else {
            let mid = node_id.to_vec();
            let src = Arc::new(Pin::new(src));
            let m = Module::load(id, p, src, mid)?;
            let mut mm = MODULES.write().unwrap(); // FIXME
            let n = mm.modules.len();
            mm.modules.push(m);
            Ok(n)
        }
    }

    pub(crate) fn get_const(module: usize, name: &str) -> Option<Const<'static>> {
        let ms = MODULES.read().unwrap();
        ms.modules().get(module)?.content.consts.get(name).cloned()
    }

    pub(crate) fn get_function(module: usize, name: &str) -> Option<FnDecl<'static>> {
        let ms = MODULES.read().unwrap();
        ms.modules()
            .get(module)?
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
