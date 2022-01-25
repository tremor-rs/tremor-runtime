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

///! FIXME
use super::{
    deploy::raw::{ConnectorDefinitionRaw, FlowDefinitionRaw},
    helper::raw::{
        OperatorDefinitionRaw, PipelineDefinitionRaw, ScriptDefinitionRaw, WindowDefinitionRaw,
    },
    raw::{AnyFnRaw, ConstRaw, IdentRaw, UseRaw},
    upable::Upable,
    BaseExpr, ConnectorDefinition, FlowDefinition, FnDecl, Helper, NodeId, OperatorDefinition,
    PipelineDefinition, ScriptDefinition, WindowDefinition,
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
use tremor_value::Value;

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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NamedEnteties<T>
where
    T: Debug + Clone + PartialEq,
{
    enteties: Vec<T>,
    names: HashMap<String, usize>,
}
impl<T> Default for NamedEnteties<T>
where
    T: Debug + Clone + PartialEq,
{
    fn default() -> Self {
        Self {
            enteties: Vec::new(),
            names: HashMap::new(),
        }
    }
}
impl<T> NamedEnteties<T>
where
    T: Debug + Clone + PartialEq,
{
    pub fn insert(&mut self, name: String, value: T) -> Result<usize> {
        if self.names.contains_key(&name) {
            return Err(format!("{name} already defined.").into());
        }
        let id = self.enteties.len();
        self.enteties.push(value);
        self.names.insert(name, id);
        Ok(id)
    }

    pub fn get_name(&self, n: &str) -> Option<&T> {
        self.names.get(n).and_then(|id| self.enteties.get(*id))
    }
    pub fn get_id(&self, id: usize) -> Option<&T> {
        self.enteties.get(id)
    }
}

// This is a self referential struct, beware
#[derive(Debug, Clone)]
pub struct Module {
    pub(crate) src: Arc<Pin<String>>,
    pub(crate) file_name: PathBuf,
    pub(crate) id: ModuleId,
    pub(crate) connectors: NamedEnteties<ConnectorDefinition<'static>>,
    pub(crate) pipelines: NamedEnteties<PipelineDefinition<'static>>,
    pub(crate) windows: NamedEnteties<WindowDefinition<'static>>,
    pub(crate) scripts: NamedEnteties<ScriptDefinition<'static>>,
    pub(crate) operators: NamedEnteties<OperatorDefinition<'static>>,
    pub(crate) flows: NamedEnteties<FlowDefinition<'static>>,
    pub(crate) consts: NamedEnteties<Value<'static>>,
    pub(crate) functions: NamedEnteties<FnDecl<'static>>,
}

impl From<&[u8]> for ModuleId {
    fn from(src: &[u8]) -> Self {
        ModuleId(sha2::Sha512::digest(src).to_vec())
    }
}

impl Module {
    pub fn load<P>(id: ModuleId, file_name: P, src: Arc<Pin<String>>) -> Result<Self>
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

        let mut flows = NamedEnteties::default();
        let mut connectors = NamedEnteties::default();
        let mut pipelines = NamedEnteties::default();
        let mut windows = NamedEnteties::default();
        let mut scripts = NamedEnteties::default();
        let mut operators = NamedEnteties::default();
        let mut functions = NamedEnteties::default();
        let mut consts = NamedEnteties::default();
        for s in raw.stmts {
            match s {
                ModuleStmtRaw::Use(_) => todo!(),
                ModuleStmtRaw::Flow(e) => {
                    let e = e.up(&mut helper)?;
                    let name = e.node_id.id.clone();
                    // The self referential nature comes into play here
                    let e = unsafe { transmute::<FlowDefinition<'_>, FlowDefinition<'static>>(e) };

                    flows.insert(name, e)?;
                }
                ModuleStmtRaw::Connector(e) => {
                    let e = e.up(&mut helper)?.into_static();
                    let name = e.node_id.id.clone();
                    connectors.insert(name, e)?;
                }
                ModuleStmtRaw::Const(e) => {
                    let e = e.up(&mut helper)?;
                    // The self referential nature comes into play here
                    let value = unsafe { transmute::<Value<'_>, Value<'static>>(e.value) };
                    consts.insert(e.name, value)?;
                }
                ModuleStmtRaw::FnDecl(e) => {
                    let e = e.up(&mut helper)?;
                    // The self referential nature comes into play here
                    let e = unsafe { transmute::<FnDecl<'_>, FnDecl<'static>>(e) };
                    let name = e.name.to_string();
                    functions.insert(name, e)?;
                }

                ModuleStmtRaw::Pipeline(e) => {
                    // FIXME? We can't do into static here
                    let e = e.up(&mut helper)?;
                    // The self referential nature comes into play here
                    let e = unsafe {
                        transmute::<PipelineDefinition<'_>, PipelineDefinition<'static>>(e)
                    };

                    let name = e.node_id.id.clone();
                    pipelines.insert(name, e)?;
                }

                ModuleStmtRaw::Window(e) => {
                    let e = e.up(&mut helper)?.into_static();
                    let name = e.node_id.id.clone();
                    windows.insert(name, e)?;
                }
                ModuleStmtRaw::Operator(e) => {
                    let e = e.up(&mut helper)?.into_static();
                    let name = e.node_id.id.clone();
                    operators.insert(name, e)?;
                }
                ModuleStmtRaw::Script(e) => {
                    // FIXME? We can't do into static here
                    let e = e.up(&mut helper)?;
                    // The self referential nature comes into play here
                    let e =
                        unsafe { transmute::<ScriptDefinition<'_>, ScriptDefinition<'static>>(e) };

                    let name = e.node_id.id.clone();
                    scripts.insert(name, e)?;
                }
            }
        }

        let file_name: &Path = file_name.as_ref();
        Ok(Module {
            src,
            file_name: PathBuf::from(file_name),
            id,
            connectors,
            pipelines,
            windows,
            scripts,
            operators,
            flows,
            consts,
            functions,
        })
    }
}

#[derive(Default, Debug)]
pub(crate) struct ModuleManager {
    path: ModulePath,
    modules: Vec<Module>,
}

impl ModuleManager {
    pub fn load_id(&mut self, id: &NodeId) -> Result<usize> {
        let f = self
            .path
            .resolve_id(id)
            .ok_or_else(|| format!("module {} not found", id))?;
        self.load(f)
    }
    pub fn load<P: AsRef<Path>>(&mut self, p: P) -> Result<usize> {
        let mut src = std::fs::read_to_string(&p)?;
        let id = ModuleId::from(src.as_bytes());

        if let Some((id, _)) = self.modules.iter().enumerate().find(|(i, m)| m.id == id) {
            Ok(id)
        } else {
            let n = self.modules.len();
            // FIXME: **sob** we still need this
            src.push('\n');
            let src = Arc::new(Pin::new(src));
            self.modules.push(Module::load(id, p, src)?);
            Ok(n)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn load() -> Result<()> {
        let mut m = ModuleManager::default();
        let id1 = m.load("./lib/std/string.tremor")?;
        let id2 = m.load("./lib/std/string.tremor")?;
        assert_eq!(id1, id2);
        // dbg!(m);
        // panic!();
        Ok(())
    }
    #[test]
    fn load_from_id() -> Result<()> {
        let mut m = ModuleManager {
            path: ModulePath {
                mounts: vec!["./lib".to_string()],
            },
            modules: Vec::new(),
        };

        m.load_id(&NodeId {
            id: "string".to_string(),
            module: vec!["std".to_string()],
        })?;
        // dbg!(m);
        // panic!();
        Ok(())
    }
}
