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

use super::ConnectorDefinition;
use super::CreateStmt;
use super::FlowDefinition;
use super::Value;
use super::{BaseExpr, DeployFlow};
use super::{ConnectStmt, DeployEndpoint};
use crate::ast::{
    docs::ModDoc, error_generic, node_id::NodeId, query::raw::ConfigRaw, raw::UseRaw, Deploy,
    DeployStmt, Helper, PipelineDefinition, Script, Upable,
};
use crate::ast::{
    query::raw::{
        CreationalWithRaw, DefinitioalArgsRaw, DefinitioalArgsWithRaw, PipelineDefinitionRaw,
    },
    visitors::ConstFolder,
};
use crate::ast::{raw::IdentRaw, walkers::ImutExprWalker};
use crate::errors::{Kind as ErrorKind, Result};
use crate::impl_expr;
use crate::pos::Location;
use crate::AggrType;
use crate::EventContext;
use beef::Cow;
use halfbrown::HashMap;
use tremor_common::time::nanotime;
use tremor_value::literal;

use crate::Return;

/// Evaluate a script expression at compile time with an empty state context
/// for use during compile time reduction
/// # Errors
/// If evaluation of the script fails, or a legal value cannot be evaluated by result
pub fn run_script<'script, 'registry>(
    helper: &Helper<'script, 'registry>,
    expr: &Script<'script>,
) -> Result<Value<'script>> {
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
            &helper.meta,
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
            if let Some(stmt) = stmt.up(&mut helper)? {
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
            config.insert(k.to_string(), v.try_into_lit(&helper.meta)?);
        }
        Ok(Deploy {
            config,
            stmts,
            connector_decls: HashMap::new(), // FIXME
            pipeline_decls: HashMap::new(),  // FIXME
            flow_decls: HashMap::new(),      // FIXME
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
            DeployStmtRaw::Use(_u) => {
                todo!()
            }
            DeployStmtRaw::FlowDefinition(stmt) => {
                let _stmt: FlowDefinition<'script> = stmt.up(helper)?;
                //FIXME: helper.flow_decls.insert(stmt.node_id.clone(), stmt.clone());
                // Ok(Some(DeployStmt::FlowDefinition(Box::new(stmt))))
                todo!();
            }
            DeployStmtRaw::DeployFlow(stmt) => {
                let stmt: DeployFlow = stmt.up(helper)?;
                helper.instances.insert(stmt.node_id.clone(), stmt.clone());
                Ok(Some(DeployStmt::DeployFlowStmt(Box::new(stmt))))
            }
        }
    }
}

pub type DeployStmtsRaw<'script> = Vec<DeployStmtRaw<'script>>;

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ConnectorDefinitionRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) kind: IdentRaw<'script>,
    pub(crate) params: DefinitioalArgsWithRaw<'script>,
    pub(crate) docs: Option<Vec<Cow<'script, str>>>,
}
impl_expr!(ConnectorDefinitionRaw);

impl<'script> Upable<'script> for ConnectorDefinitionRaw<'script> {
    type Target = ConnectorDefinition<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let query_decl = ConnectorDefinition {
            config: Value::const_null(),
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            params: self.params.up(helper)?,
            builtin_kind: self.kind.to_string(),
            node_id: NodeId::new(self.id, &helper.module),
            docs: self
                .docs
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        };

        Ok(query_decl)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// we're forced to make this pub because of lalrpop
pub struct DeployEndpointRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    pub alias: IdentRaw<'script>,
    /// we're forced to make this pub because of lalrpop
    pub port: IdentRaw<'script>,
}

impl<'script> Upable<'script> for DeployEndpointRaw<'script> {
    type Target = DeployEndpoint;
    fn up<'registry>(self, _helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(DeployEndpoint {
            alias: self.alias.to_string(),
            port: self.port.to_string(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// we're forced to make this pub because of lalrpop
pub enum ConnectStmtRaw<'script> {
    ConnectorToPipeline {
        /// The instance we're connecting to
        start: Location,
        /// The instance we're connecting to
        end: Location,
        /// The instance we're connecting to
        from: DeployEndpointRaw<'script>,
        /// The instance being connected
        to: DeployEndpointRaw<'script>,
    },
    PipelineToConnector {
        /// The instance we're connecting to
        start: Location,
        /// The instance we're connecting to
        end: Location,
        /// The instance we're connecting to
        from: DeployEndpointRaw<'script>,
        /// The instance being connected
        to: DeployEndpointRaw<'script>,
    },
    PipelineToPipeline {
        /// The instance we're connecting to
        start: Location,
        /// The instance we're connecting to
        end: Location,
        /// The instance we're connecting to
        from: DeployEndpointRaw<'script>,
        /// The instance being connected
        to: DeployEndpointRaw<'script>,
    },
}
impl<'script> Upable<'script> for ConnectStmtRaw<'script> {
    type Target = ConnectStmt;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match self {
            ConnectStmtRaw::ConnectorToPipeline {
                start,
                end,
                from,
                to,
            } => Ok(ConnectStmt::ConnectorToPipeline {
                mid: helper.add_meta(start, end),
                from: from.up(helper)?,
                to: to.up(helper)?,
            }),
            ConnectStmtRaw::PipelineToConnector {
                start,
                end,
                from,
                to,
            } => Ok(ConnectStmt::PipelineToConnector {
                mid: helper.add_meta(start, end),
                from: from.up(helper)?,
                to: to.up(helper)?,
            }),
            ConnectStmtRaw::PipelineToPipeline {
                start,
                end,
                from,
                to,
            } => Ok(ConnectStmt::PipelineToPipeline {
                mid: helper.add_meta(start, end),
                from: from.up(helper)?,
                to: to.up(helper)?,
            }),
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FlowDefinitionRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) params: DefinitioalArgsRaw<'script>,
    pub(crate) docs: Option<Vec<Cow<'script, str>>>,
    pub(crate) atoms: Vec<FlowStmtRaw<'script>>,
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
        //
        helper.module.push(self.id.clone());

        let mut connections = Vec::new();
        let mut creates = Vec::new();
        for link in self.atoms {
            match link {
                FlowStmtRaw::ConnectorDefinition(stmt) => {
                    let _stmt: ConnectorDefinition<'script> = stmt.up(helper)?;
                    todo!();
                    // helper
                    //     .connector_decls
                    //     .insert(stmt.node_id.clone(), stmt.clone());
                }
                FlowStmtRaw::PipelineDefinition(stmt) => {
                    let _stmt: PipelineDefinition<'script> = stmt.up(helper)?;
                    todo!();
                    // helper
                    //     .pipeline_decls
                    //     .insert(stmt.node_id.clone(), stmt.clone());
                }
                FlowStmtRaw::Connect(connect) => {
                    connections.push(connect.up(helper)?);
                }
                FlowStmtRaw::Create(stmt) => {
                    creates.push(stmt.up(helper)?);
                }
            }
        }
        let mid = helper.add_meta_w_name(self.start, self.end, &self.id);
        let docs = self
            .docs
            .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n"));
        let params = self.params.up(helper)?;
        helper.module.pop();
        let node_id = NodeId::new(&self.id, &helper.module);

        let flow_decl = FlowDefinition {
            mid,
            node_id,
            params,
            connections,
            creates,
            docs,
        };
        Ok(flow_decl)
    }
}

pub type FlowStmtsRaw<'script> = Vec<FlowStmtRaw<'script>>;

/// we're forced to make this pub because of lalrpop

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum FlowStmtRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    ConnectorDefinition(ConnectorDefinitionRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    PipelineDefinition(PipelineDefinitionRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Connect(ConnectStmtRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Create(CreateStmtRaw<'script>),
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
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: IdentRaw<'script>,
    pub(crate) params: CreationalWithRaw<'script>,
    /// Id of the definition
    pub target: NodeId,
    /// Module of the definition
    pub(crate) kind: CreateKind,
}
impl_expr!(CreateStmtRaw);

impl<'script> Upable<'script> for CreateStmtRaw<'script> {
    type Target = CreateStmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // TODO check that names across pipeline/flow/connector definitions are unique or else hygienic error

        let _node_id = NodeId::new(&self.id.id, &helper.module);
        let _target = self.target.clone().with_prefix(&helper.module);
        let _outer = self.extent(&helper.meta);
        let _inner = self.id.extent(&helper.meta);
        let _params = self.params.up(helper)?;
        let _decl = match self.kind {
            CreateKind::Connector => {
                todo!();
                // if let Some(artefact) = helper.connector_decls.get(&target) {
                //     CreateTargetDefinition::Connector(artefact.clone())
                // } else {
                //     return Err(ErrorKind::DeployArtefactNotDefined(
                //         outer,
                //         inner,
                //         target.to_string(),
                //         helper
                //             .connector_decls
                //             .keys()
                //             .map(ToString::to_string)
                //             .collect(),
                //     )
                //     .into());
                // }
            }
            CreateKind::Pipeline => {
                todo!();
                // if let Some(artefact) = helper.pipeline_decls.get(&target) {
                //     CreateTargetDefinition::Pipeline(artefact.clone())
                // } else {
                //     return Err(ErrorKind::DeployArtefactNotDefined(
                //         outer,
                //         inner,
                //         target.to_string(),
                //         helper
                //             .pipeline_decls
                //             .keys()
                //             .map(ToString::to_string)
                //             .collect(),
                //     )
                //     .into());
                // }
            }
        };

        // let create_stmt = CreateStmt {
        //     mid: helper.add_meta_w_name(self.start, self.end, &self.id),
        //     with: params,
        //     node_id: node_id.clone(),
        //     target,
        //     defn: decl,
        // };
        // helper.instances.insert(node_id, create_stmt.clone());

        // Ok(create_stmt)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DeployFlowRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: IdentRaw<'script>,
    pub(crate) params: CreationalWithRaw<'script>,
    /// Id of the definition
    pub target: NodeId,
    pub(crate) docs: Option<Vec<Cow<'script, str>>>,
}
impl_expr!(DeployFlowRaw);

impl<'script> Upable<'script> for DeployFlowRaw<'script> {
    type Target = DeployFlow<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // TODO check that names across pipeline/flow/connector definitions are unique or else hygienic error

        let node_id = NodeId::new(&self.id.id, &helper.module);
        let target = self.target.clone().with_prefix(&helper.module);

        let mut defn = if let Some(artefact) = helper.get_flow_decls(&target) {
            artefact.clone()
        } else {
            return Err(ErrorKind::DeployArtefactNotDefined(
                self.extent(&helper.meta),
                self.id.extent(&helper.meta),
                target.to_string(),
                Vec::new(), //FIXME; helper.flow_decls.keys().map(ToString::to_string).collect(),
            )
            .into());
        };
        let params = self.params.up(helper)?;

        defn.params.ingest_creational_with(&params)?;

        defn.apply_args(helper)?;

        let create_stmt = DeployFlow {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            node_id,
            target: self.target,
            decl: defn,
            docs: self
                .docs
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        };

        Ok(create_stmt)
    }
}
