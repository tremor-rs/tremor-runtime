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
    BaseExpr, ConnectStmt, ConnectorDefinition, CreateStmt, CreateTargetDefinition, DeployEndpoint,
    DeployFlow, FlowDefinition, Value,
};
use crate::{
    ast::{
        base_expr::Ranged,
        docs::{FlowDoc, ModDoc},
        error_generic,
        node_id::NodeId,
        query::raw::{
            ConfigRaw, CreationalWithRaw, DefinitionalArgsRaw, DefinitionalArgsWithRaw,
            PipelineDefinitionRaw,
        },
        raw::{IdentRaw, UseRaw},
        visitors::ConstFolder,
        walkers::{ImutExprWalker, QueryWalker},
        Deploy, DeployStmt, Helper, NodeMeta, Script, Upable,
    },
    errors::{Error, Kind as ErrorKind, Result},
    impl_expr,
    module::Manager,
    AggrType, EventContext, Return,
};
use beef::Cow;
use halfbrown::HashMap;
use tremor_common::time::nanotime;
use tremor_value::literal;

/// Evaluate a script expression at compile time with an empty state context
/// for use during compile time reduction
/// # Errors
/// If evaluation of the script fails, or a legal value cannot be evaluated by result
pub fn run_script<'script>(expr: &Script<'script>) -> Result<Value<'script>> {
    // We duplicate these here as it simplifies use of the macro externally
    let ctx = EventContext::new(nanotime(), None);
    let mut event = literal!({}).into_static();
    let mut state = literal!({}).into_static();
    let mut meta = literal!({}).into_static();

    match expr.run(&ctx, AggrType::Emit, &mut event, &mut state, &mut meta) {
        Ok(Return::Emit { value, .. }) => Ok(value),
        _otherwise => error_generic(
            expr,
            expr,
            &"Failed to evaluate script at compile time".to_string(),
        ),
    }
}

#[derive(Debug, PartialEq, Serialize)]
pub struct DeployRaw<'script> {
    pub(crate) config: ConfigRaw<'script>,
    pub(crate) stmts: DeployStmtsRaw<'script>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
}
impl<'script> DeployRaw<'script> {
    pub(crate) fn up_script<'registry>(
        self,
        mut helper: &mut Helper<'script, 'registry>,
    ) -> Result<Deploy<'script>> {
        let mut stmts: Vec<DeployStmt<'script>> = vec![];
        for (_i, stmt) in self.stmts.into_iter().enumerate() {
            if let Some(stmt) = stmt.up(helper)? {
                stmts.push(stmt);
            }
        }

        helper.docs.module = Some(ModDoc {
            name: "self".into(),
            doc: self
                .doc
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        });

        let mut config = HashMap::new();
        for (k, mut v) in self.config.up(helper)? {
            ConstFolder::new(helper).walk_expr(&mut v)?;
            config.insert(k.to_string(), v.try_into_value(helper)?);
        }
        Ok(Deploy {
            config,
            stmts,
            scope: helper.scope.clone(),
            docs: helper.docs.clone(),
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum DeployStmtRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    DeployFlow(DeployFlowRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    FlowDefinition(FlowDefinitionRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Use(UseRaw),
}

impl<'script> Upable<'script> for DeployStmtRaw<'script> {
    type Target = Option<DeployStmt<'script>>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match self {
            DeployStmtRaw::Use(UseRaw { alias, module, mid }) => {
                let range = mid.range;
                let module_id = Manager::load(&module).map_err(|err| match err {
                    Error(ErrorKind::ModuleNotFound(_, _, p, exp), state) => Error(
                        ErrorKind::ModuleNotFound(range.expand_lines(2), range, p, exp),
                        state,
                    ),
                    _ => err,
                })?;

                let alias = alias.unwrap_or_else(|| module.id.clone());
                helper.scope().add_module_alias(alias, module_id);
                Ok(None)
            }
            DeployStmtRaw::FlowDefinition(stmt) => {
                helper.docs.flows.push(stmt.doc());
                let stmt: FlowDefinition<'script> = stmt.up(helper)?;
                helper.scope.insert_flow(stmt)?;
                Ok(None)
            }
            DeployStmtRaw::DeployFlow(stmt) => {
                let stmt: DeployFlow = stmt.up(helper)?;
                helper
                    .instances
                    .insert(stmt.instance_alias.clone(), stmt.clone());
                Ok(Some(DeployStmt::DeployFlowStmt(Box::new(stmt))))
            }
        }
    }
}

pub type DeployStmtsRaw<'script> = Vec<DeployStmtRaw<'script>>;

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ConnectorDefinitionRaw<'script> {
    pub(crate) id: String,
    pub(crate) kind: IdentRaw<'script>,
    pub(crate) params: DefinitionalArgsWithRaw<'script>,
    pub(crate) docs: Option<Vec<Cow<'script, str>>>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(ConnectorDefinitionRaw);

impl<'script> Upable<'script> for ConnectorDefinitionRaw<'script> {
    type Target = ConnectorDefinition<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // verify supported parameters
        for (ident, _) in &self.params.with.exprs {
            let key: &str = ident.id.as_ref();
            if !ConnectorDefinition::AVAILABLE_PARAMS.contains(&key) {
                let range = ident.mid.range;
                return Err(ErrorKind::InvalidDefinitionalWithParam(
                    range.expand_lines(2),
                    range,
                    format!("connector \"{}\"", self.id),
                    ident.id.to_string(),
                    &ConnectorDefinition::AVAILABLE_PARAMS,
                )
                .into());
            }
        }

        let query_defn = ConnectorDefinition {
            config: Value::const_null(),
            mid: self.mid.box_with_name(&self.id),
            params: self.params.up(helper)?,
            builtin_kind: self.kind.to_string(),
            id: self.id,
            docs: self
                .docs
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        };

        Ok(query_defn)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct DeployEndpointRaw<'script> {
    pub(crate) alias: IdentRaw<'script>,
    pub(crate) port: IdentRaw<'script>,
    pub(crate) mid: Box<NodeMeta>,
}

impl<'script> Upable<'script> for DeployEndpointRaw<'script> {
    type Target = DeployEndpoint;
    fn up<'registry>(self, _helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(DeployEndpoint {
            alias: self.alias.to_string(),
            port: self.port.to_string(),
            mid: self.mid,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// we're forced to make this pub because of lalrpop
pub(crate) enum ConnectStmtRaw<'script> {
    ConnectorToPipeline {
        /// The instance we're connecting to
        from: DeployEndpointRaw<'script>,
        /// The instance being connected
        to: DeployEndpointRaw<'script>,
        /// The instance we're connecting to
        mid: Box<NodeMeta>,
    },
    PipelineToConnector {
        /// The instance we're connecting to
        from: DeployEndpointRaw<'script>,
        /// The instance being connected
        to: DeployEndpointRaw<'script>,
        /// The instance we're connecting to
        mid: Box<NodeMeta>,
    },
    PipelineToPipeline {
        /// The instance we're connecting to
        from: DeployEndpointRaw<'script>,
        /// The instance being connected
        to: DeployEndpointRaw<'script>,
        /// The instance we're connecting to
        mid: Box<NodeMeta>,
    },
}
impl<'script> Upable<'script> for ConnectStmtRaw<'script> {
    type Target = ConnectStmt;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match self {
            ConnectStmtRaw::ConnectorToPipeline { mid, from, to } => {
                Ok(ConnectStmt::ConnectorToPipeline {
                    mid,
                    from: from.up(helper)?,
                    to: to.up(helper)?,
                })
            }
            ConnectStmtRaw::PipelineToConnector { mid, from, to } => {
                Ok(ConnectStmt::PipelineToConnector {
                    mid,
                    from: from.up(helper)?,
                    to: to.up(helper)?,
                })
            }
            ConnectStmtRaw::PipelineToPipeline { mid, from, to } => {
                Ok(ConnectStmt::PipelineToPipeline {
                    mid,
                    from: from.up(helper)?,
                    to: to.up(helper)?,
                })
            }
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FlowDefinitionRaw<'script> {
    pub(crate) id: String,
    pub(crate) params: DefinitionalArgsRaw<'script>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
    pub(crate) stmts: Vec<FlowStmtRaw<'script>>,
    pub(crate) mid: Box<NodeMeta>,
}

impl<'script> FlowDefinitionRaw<'script> {
    fn doc(&self) -> FlowDoc {
        FlowDoc {
            name: self.id.clone(),
            doc: self
                .doc
                .clone()
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        }
    }
}
impl_expr!(FlowDefinitionRaw);

impl<'script> Upable<'script> for FlowDefinitionRaw<'script> {
    type Target = FlowDefinition<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // NOTE As we can have module aliases and/or nested modules within script definitions
        // that are private to or inline with the script - multiple script definitions in the
        // same module scope can share the same relative function/const module paths.
        //
        // We add the script name to the scope as a means to distinguish these orthogonal
        // definitions. This is achieved with the push/pop pointcut around the up() call
        // below. The actual function registration occurs in the up() call in the usual way.

        helper.enter_scope();

        let mut connections = Vec::new();
        let mut creates = Vec::new();
        for stmt in self.stmts {
            match stmt {
                FlowStmtRaw::Use(UseRaw { alias, module, mid }) => {
                    let range = mid.range;
                    let module_id = Manager::load(&module).map_err(|err| match err {
                        Error(ErrorKind::ModuleNotFound(_, _, p, exp), state) => Error(
                            ErrorKind::ModuleNotFound(range.expand_lines(2), range, p, exp),
                            state,
                        ),
                        _ => err,
                    })?;
                    let alias = alias.unwrap_or_else(|| module.id.clone());
                    helper.scope().add_module_alias(alias, module_id);
                }
                FlowStmtRaw::ConnectorDefinition(stmt) => {
                    let stmt = stmt.up(helper)?;
                    helper.scope.insert_connector(stmt)?;
                }
                FlowStmtRaw::PipelineDefinition(stmt) => {
                    let stmt = stmt.up(helper)?;
                    helper.scope.insert_pipeline(stmt)?;
                }
                FlowStmtRaw::Connect(connect) => {
                    connections.push(connect.up(helper)?);
                }
                FlowStmtRaw::Create(stmt) => {
                    creates.push(stmt.up(helper)?);
                }
            }
        }
        let mid = self.mid.box_with_name(&self.id);
        let docs = self
            .doc
            .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n"));
        helper.leave_scope()?;
        // we need to evaluate args in the outer scope
        let params = self.params.up(helper)?;

        let flow_defn = FlowDefinition {
            mid,
            id: self.id,
            params,
            connections,
            creates,
            docs,
        };
        Ok(flow_defn)
    }
}

pub(crate) type FlowStmtsRaw<'script> = Vec<FlowStmtRaw<'script>>;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) enum FlowStmtRaw<'script> {
    ConnectorDefinition(ConnectorDefinitionRaw<'script>),
    PipelineDefinition(PipelineDefinitionRaw<'script>),
    Connect(ConnectStmtRaw<'script>),
    Create(CreateStmtRaw<'script>),
    Use(UseRaw),
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum CreateKind {
    /// Reference to a connector definition
    Connector,
    /// Reference to a pipeline definition
    Pipeline,
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CreateStmtRaw<'script> {
    pub(crate) id: IdentRaw<'script>,
    pub(crate) params: CreationalWithRaw<'script>,
    /// Id of the definition
    pub target: NodeId,
    /// Module of the definition
    pub(crate) kind: CreateKind,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(CreateStmtRaw);

impl<'script> Upable<'script> for CreateStmtRaw<'script> {
    type Target = CreateStmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let target = self.target.clone();
        let outer = self.extent();
        let inner = self.id.extent();

        let defn = match self.kind {
            CreateKind::Connector => {
                if let Some(artefact) = helper.get(&target)? {
                    CreateTargetDefinition::Connector(artefact)
                } else {
                    return Err(ErrorKind::DeployArtefactNotDefined(
                        outer,
                        inner,
                        target.to_string(),
                        vec![],
                    )
                    .into());
                }
            }
            CreateKind::Pipeline => {
                if let Some(artefact) = helper.get(&target)? {
                    CreateTargetDefinition::Pipeline(Box::new(artefact))
                } else {
                    return Err(ErrorKind::DeployArtefactNotDefined(
                        outer,
                        inner,
                        target.to_string(),
                        vec![],
                    )
                    .into());
                }
            }
        };
        let args = match defn {
            CreateTargetDefinition::Connector(ref conn) => &conn.params.args.0,
            CreateTargetDefinition::Pipeline(ref pipe) => &pipe.params.args.0,
        };
        for (ident, _) in &self.params.with.exprs {
            if !args.iter().any(|(args_ident, _)| ident.id == args_ident.id) {
                let range = ident.extent();
                let available_args = args
                    .iter()
                    .map(|(ident, _)| ident.id.to_string())
                    .collect::<Vec<String>>();
                return Err(ErrorKind::WithParamNoArg(
                    range.expand_lines(2),
                    range,
                    ident.id.to_string(),
                    self.id.id.to_string(),
                    available_args,
                )
                .into());
            }
        }

        let create_stmt = CreateStmt {
            mid: self.mid.box_with_name(&self.id.id),
            with: self.params.up(helper)?,
            instance_alias: self.id.id.to_string(),
            from_target: target,
            defn,
        };

        Ok(create_stmt)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DeployFlowRaw<'script> {
    pub(crate) id: IdentRaw<'script>,
    pub(crate) params: CreationalWithRaw<'script>,
    /// Id of the definition
    pub target: NodeId,
    pub(crate) docs: Option<Vec<Cow<'script, str>>>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(DeployFlowRaw);

impl<'script> Upable<'script> for DeployFlowRaw<'script> {
    type Target = DeployFlow<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // TODO check that names across pipeline/flow/connector definitions are unique or else hygienic error
        let target = self.target.clone();
        let mut defn = if let Some(artefact) = helper.get::<FlowDefinition>(&target)? {
            artefact.clone()
        } else {
            // TODO: smarter error when using a module target
            let defined_flows = helper
                .scope
                .content
                .flows
                .keys()
                .map(ToString::to_string)
                .collect();
            return Err(ErrorKind::DeployArtefactNotDefined(
                self.extent(),
                self.id.extent(),
                target.to_string(),
                defined_flows,
            )
            .into());
        };
        let upped_params = self.params.up(helper)?;
        defn.params.ingest_creational_with(&upped_params)?;
        ConstFolder::new(helper).walk_definitional_args(&mut defn.params)?;
        let defn_args = defn.params.render()?;

        for c in &mut defn.creates {
            c.with.substitute_args(&defn_args, helper)?;
        }

        let create_stmt = DeployFlow {
            mid: self.mid.box_with_name(&self.id.id),
            instance_alias: self.id.id.to_string(),
            from_target: self.target,
            defn,
            docs: self
                .docs
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        };

        Ok(create_stmt)
    }
}
