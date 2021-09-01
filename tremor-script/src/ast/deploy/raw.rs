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

use super::BaseExpr;
use super::ConnectorDecl;
use super::CreateStmt;
use super::FlowDecl;
use super::PipelineDecl;
use super::Value;
use crate::ast::error_generic;
use crate::ast::query::raw::QueryRaw;
use crate::ast::raw::ExprRaw;
use crate::ast::raw::IdentRaw;
use crate::ast::raw::ModuleRaw;
use crate::ast::raw::ScriptRaw;
use crate::ast::raw::StringLitRaw;
use crate::ast::raw::WithExprsRaw;
use crate::ast::AggrRegistry;
use crate::ast::Deploy;
use crate::ast::DeployStmt;
use crate::ast::Helper;
use crate::ast::NodeMetas;
use crate::ast::Registry;
use crate::ast::Return;
use crate::ast::Upable;
use crate::errors::ErrorKind;
use crate::errors::Result;
use crate::impl_expr;
use crate::interpreter::Cont;
use crate::pos::Location;
use crate::AggrType;
use crate::EventContext;
use beef::Cow;
use halfbrown::HashMap;
use tremor_common::time::nanotime;
use tremor_common::url::TremorUrl;
use tremor_value::literal;
use value_trait::Mutable;

// For compile time interpretation support
use crate::interpreter::Env;
use crate::interpreter::ExecOpts;
use crate::interpreter::LocalStack;

pub type DeployWithExprsRaw<'script> = Vec<(IdentRaw<'script>, ExprRaw<'script>)>;

macro_rules! stateless_run_expr {
    ($helper:ident, $expr:expr) => {{
        let eo = ExecOpts {
            aggr: AggrType::Emit,
            result_needed: true,
        };
        let ctx = EventContext::new(nanotime(), None);
        let mut event = literal!({}).into_static();
        let mut state = literal!({}).into_static();
        let mut meta = literal!({}).into_static();

        let mut local = LocalStack::with_size(0);

        let run_consts = $helper.consts.clone();
        let run_consts = run_consts.run();
        let env = Env {
            context: &ctx,
            consts: run_consts,
            aggrs: &$helper.aggregates.clone(),
            meta: &$helper.meta.clone(),
            recursion_limit: crate::recursion_limit(),
        };
        let got = $expr.run(eo, &env, &mut event, &mut state, &mut meta, &mut local);
        match got {
            Ok(Cont::Emit(value, _port)) => value,
            Ok(Cont::Cont(value)) => value.into_owned(),
            // FIXME this deserves a proper error
            _otherwise => {
                return error_generic(
                    $expr,
                    $expr,
                    &"Illegal `with` parameter value specified".to_string(),
                    &$helper.meta,
                )
            }
        }
    }};
}

macro_rules! stateless_run_str {
    ($helper:ident, $expr:expr) => {{
        let eo = ExecOpts {
            aggr: AggrType::Emit,
            result_needed: true,
        };
        let ctx = EventContext::new(nanotime(), None);
        let event = literal!({}).into_static();
        let state = literal!({}).into_static();
        let meta = literal!({}).into_static();

        let local = LocalStack::with_size(0);

        let run_consts = $helper.consts.clone();
        let run_consts = run_consts.run();
        let env = Env {
            context: &ctx,
            consts: run_consts,
            aggrs: &$helper.aggregates.clone(),
            meta: &$helper.meta.clone(),
            recursion_limit: crate::recursion_limit(),
        };
        $expr.run(eo, &env, &event, &state, &meta, &local)
    }};
}

pub(crate) fn up_params<'script, 'registry>(
    params: &[(IdentRaw<'script>, ExprRaw<'script>)],
    helper: &mut Helper<'script, 'registry>,
) -> Result<HashMap<String, Value<'script>>> {
    let mut mapped = HashMap::new();
    for (name, value) in params {
        let name = name.clone().up(helper)?.to_string();
        let expr = value.clone().up(helper)?;
        let value = stateless_run_expr!(helper, &expr).clone_static();
        mapped.insert(name, value);
    }

    Ok(mapped)
}

pub(crate) fn up_maybe_params<'script, 'registry>(
    params: &Option<DeployWithExprsRaw<'script>>,
    helper: &mut Helper<'script, 'registry>,
) -> Result<Option<HashMap<String, Value<'script>>>> {
    params
        .as_ref()
        .map(|params| up_params(params, helper))
        .transpose()
}

#[derive(Debug, PartialEq, Serialize)]
#[allow(clippy::module_name_repetitions)]
pub struct DeployRaw<'script> {
    pub(crate) config: WithExprsRaw<'script>,
    pub(crate) stmts: DeployStmtsRaw<'script>,
}
impl<'script> DeployRaw<'script> {
    pub(crate) fn up_script<'registry>(
        self,
        mut helper: &mut Helper<'script, 'registry>,
    ) -> Result<Deploy<'script>> {
        let mut stmts = vec![];
        for (_i, e) in self.stmts.into_iter().enumerate() {
            match e {
                DeployStmtRaw::ModuleStmt(m) => {
                    m.define(helper.reg, helper.aggr_reg, &mut vec![], &mut helper)?;
                }
                other => {
                    stmts.push(other.up(&mut helper)?);
                }
            }
        }

        Ok(Deploy {
            stmts,
            connectors: helper.connector_defns.clone(),
            pipelines: helper.pipeline_defns.clone(),
            flows: helper.flow_defns.clone(),
        })
    }
}
/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum DeployStmtRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    CreateStmt(CreateStmtRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    FlowDecl(FlowDeclRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    PipelineDecl(PipelineDeclRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    ConnectorDecl(ConnectorDeclRaw<'script>),
    //    /// we're forced to make this pub because of lalrpop
    //    Select(Box<SelectRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    ModuleStmt(DeployModuleStmtRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Expr(Box<ExprRaw<'script>>),
}
impl<'script> DeployStmtRaw<'script> {
    const BAD_MODULE: &'static str = "Module in wrong place error";
    const BAD_EXPR: &'static str = "Expression in wrong place error";
}

impl<'script> Upable<'script> for DeployStmtRaw<'script> {
    type Target = DeployStmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match self {
            DeployStmtRaw::PipelineDecl(stmt) => {
                let stmt: PipelineDecl<'script> = stmt.up(helper)?;
                helper
                    .pipeline_defns
                    .insert(stmt.fqsn(&stmt.module), stmt.clone());
                Ok(DeployStmt::PipelineDecl(Box::new(stmt)))
            }
            DeployStmtRaw::ConnectorDecl(stmt) => {
                let stmt: ConnectorDecl<'script> = stmt.up(helper)?;
                helper
                    .connector_defns
                    .insert(stmt.fqsn(&stmt.module), stmt.clone());
                Ok(DeployStmt::ConnectorDecl(Box::new(stmt)))
            }
            DeployStmtRaw::FlowDecl(stmt) => {
                let stmt: FlowDecl<'script> = stmt.up(helper)?;
                helper
                    .flow_defns
                    .insert(stmt.fqsn(&stmt.module), stmt.clone());
                Ok(DeployStmt::FlowDecl(Box::new(stmt)))
            }
            DeployStmtRaw::CreateStmt(stmt) => {
                let stmt: CreateStmt<'script> = stmt.up(helper)?;
                helper
                    .instances
                    .insert(stmt.fqsn(&stmt.module), stmt.clone());
                Ok(DeployStmt::CreateStmt(Box::new(stmt)))
            }
            DeployStmtRaw::ModuleStmt(ref m) => {
                error_generic(m, m, &Self::BAD_MODULE, &helper.meta)
            }
            DeployStmtRaw::Expr(m) => error_generic(&*m, &*m, &Self::BAD_EXPR, &helper.meta),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct DeployModuleStmtRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub name: IdentRaw<'script>,
    pub stmts: DeployStmtsRaw<'script>,
    pub doc: Option<Vec<Cow<'script, str>>>,
}
impl_expr!(DeployModuleStmtRaw);

impl<'script> DeployModuleStmtRaw<'script> {
    pub(crate) fn define<'registry>(
        self,
        reg: &'registry Registry,
        aggr_reg: &'registry AggrRegistry,
        consts: &mut Vec<Value<'script>>,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<()> {
        helper.module.push(self.name.to_string());
        for e in self.stmts {
            match e {
                DeployStmtRaw::ModuleStmt(m) => {
                    m.define(reg, aggr_reg, consts, helper)?;
                }
                DeployStmtRaw::Expr(e) => {
                    // We create a 'fake' tremor script module to define
                    // expressions inside this module
                    let expr_m = ModuleRaw {
                        name: self.name.clone(),
                        start: self.start,
                        end: self.end,
                        doc: None,
                        exprs: vec![*e],
                    };
                    // since `ModuleRaw::define` also prepends the module
                    // name we got to remove it prior to calling `define` and
                    // add it back later
                    helper.module.pop();
                    expr_m.define(helper)?;
                    helper.module.push(self.name.to_string());
                }
                DeployStmtRaw::ConnectorDecl(stmt) => {
                    let stmt: ConnectorDecl<'script> = stmt.up(helper)?;
                    helper
                        .connector_defns
                        .insert(stmt.fqsn(&stmt.module), stmt.clone());
                }
                DeployStmtRaw::FlowDecl(stmt) => {
                    let stmt: FlowDecl<'script> = stmt.up(helper)?;
                    helper
                        .flow_defns
                        .insert(stmt.fqsn(&stmt.module), stmt.clone());
                }
                DeployStmtRaw::PipelineDecl(stmt) => {
                    let stmt: PipelineDecl<'script> = stmt.up(helper)?;
                    helper
                        .pipeline_defns
                        .insert(stmt.fqsn(&stmt.module), stmt.clone());
                }
                DeployStmtRaw::CreateStmt(stmt) => {
                    let stmt: CreateStmt<'script> = stmt.up(helper)?;
                    helper
                        .instances
                        .insert(stmt.fqsn(&stmt.module), stmt.clone());
                }
            }
        }
        helper.module.pop();
        Ok(())
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PipelineDeclRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) args: WithArgsRaw<'script>,
    pub(crate) params: Option<DeployWithExprsRaw<'script>>,
    pub(crate) script: Option<ScriptRaw<'script>>,
    pub(crate) query: QueryRaw<'script>,
}

fn arg_spec_resolver<'script, 'registry>(
    args_spec: WithArgsRaw<'script>,
    params_spec: &Option<DeployWithExprsRaw<'script>>,
    script_spec: &Option<ScriptRaw<'script>>,
    helper: &mut Helper<'script, 'registry>,
) -> Result<(Vec<Cow<'script, str>>, Value<'script>)> {
    let mut args: HashMap<Cow<str>, Value<'script>> = HashMap::new();
    let mut spec = vec![];

    let upper_args = helper.consts.args.clone();

    // Process explicitly declared required/optional arguments
    for arg_spec in args_spec {
        match arg_spec {
            ArgumentDeclRaw::Required(name) => {
                let moo = name.to_string();
                let moo: Cow<str> = Cow::owned(moo);
                spec.push(moo.clone());
            }
            ArgumentDeclRaw::Optional(name, value) => {
                let moo = name.to_string();
                let moo: Cow<str> = Cow::owned(moo);
                spec.push(moo.clone());
                let value = value.up(helper)?;
                let value = stateless_run_expr!(helper, &value);
                args.insert(moo, value);
            }
        }
    }
    // We inject the args here so that our `with` clause can override based on
    // our specificational arguments and their defaults
    helper.consts.args = Value::Object(Box::new(args.clone()));

    // Process defaulted arguments via `with` expressions
    if let Some(params_spec) = up_maybe_params(params_spec, helper)? {
        for (name, value) in params_spec {
            let moo = Cow::owned(name);
            if !spec.contains(&moo) {
                // FIXME TODO consider duplicate argument specification - ok or ko?
                spec.push(moo.clone());
            }
            let up_value = value;
            args.insert(moo, up_value.clone_static());
        }
        // We inject the args here so that our `script` clause can override based on
        // our `with` specificational argument overrides and their defaults
        helper.consts.args = Value::Object(Box::new(args.clone()));
    }

    match &script_spec {
        Some(script) => {
            let ctx = EventContext::new(nanotime(), None);
            let x = script.clone().up_script(helper)?.run_imut(
                &ctx,
                AggrType::Emit,
                &literal!({}),
                &literal!({}),
                &literal!({}),
            )?;
            if let Return::Emit {
                value: Value::Object(value),
                ..
            } = x
            {
                for (name, value) in value.iter() {
                    let moo = Cow::owned(name.to_string());
                    if !spec.contains(&moo) {
                        // FIXME TODO consider duplicate argument specification - ok or ko?
                        spec.push(moo.clone());
                    }
                    args.insert(moo, value.clone());
                }
                helper.consts.args = Value::Object(Box::new(args.clone()));
            }
        }
        None => {}
    }

    // NOTE We don't check for `extra args` here - this is deferred to the resolution in `arg_resolver`
    helper.consts.args = upper_args.clone();
    Ok((spec, Value::Object(Box::new(args))))
}

fn arg_resolver<'script, 'registry>(
    outer: crate::pos::Range,
    spec: &[Cow<'script, str>],
    args_as_params: &Value<'script>,
    params_spec: &Option<DeployWithExprsRaw<'script>>,
    script_spec: &Option<ScriptRaw<'script>>,
    helper: &mut Helper<'script, 'registry>,
) -> Result<Value<'script>> {
    let mut args = args_as_params.clone();
    // Process defaulted arguments via `with` expressions
    helper.consts.args = args_as_params.clone();
    if let Value::Object(args_as_params) = args_as_params {
        if let Some(params_spec) = up_maybe_params(params_spec, helper)? {
            for (name, value) in params_spec {
                let moo = Cow::owned(name);
                if spec.contains(&moo) {
                    args.insert(moo, value.clone_static())?;
                } else {
                    return Err(ErrorKind::DeployArgNotSpecified(
                        outer.extent(&helper.meta),
                        outer.extent(&helper.meta),
                        moo.to_string(),
                    )
                    .into());
                }
            }
        }

        if let Some(script_spec) = script_spec {
            let ctx = EventContext::new(nanotime(), None);
            let x = script_spec.clone().up_script(helper)?.run_imut(
                &ctx,
                AggrType::Emit,
                &literal!({}),
                &literal!({}),
                &literal!({}),
            )?;
            if let Return::Emit {
                value: Value::Object(value),
                ..
            } = x
            {
                for (name, value) in value.iter() {
                    let cow_name = Cow::owned(name.to_string());
                    if spec.contains(&cow_name) {
                        args.insert(cow_name, value.clone())?;
                    } else {
                        return Err(ErrorKind::DeployArgNotSpecified(
                            outer.extent(&helper.meta),
                            outer.extent(&helper.meta),
                            name.to_string(),
                        )
                        .into());
                    }
                }
                helper.consts.args = args.clone();
            }
        }

        // We iterate over the definitional arguments known / specified
        for (name, _value) in args_as_params.iter() {
            let cow_name = Cow::owned(name.to_string());
            if !spec.contains(&cow_name) {
                return Err(ErrorKind::DeployRequiredArgDoesNotResolve(
                    outer.extent(&helper.meta),
                    outer.extent(&helper.meta),
                    name.to_string(),
                )
                .into());
            }
        }

        // We iterate over the deployment parameters and filter/ban unknown arguments
        for name in spec.iter() {
            let cow_name = Cow::owned(name.to_string());
            if !args_as_params.contains_key(&cow_name) {
                // FIXME Add extra args hygienic error
                return Err(ErrorKind::DeployRequiredArgDoesNotResolve(
                    outer.extent(&helper.meta),
                    outer.extent(&helper.meta),
                    name.to_string(),
                )
                .into());
            }
        }
    }

    Ok(args)
}

impl<'script> Upable<'script> for PipelineDeclRaw<'script> {
    type Target = PipelineDecl<'script>;
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
        let (spec, args) = arg_spec_resolver(self.args, &self.params, &self.script, helper)?;
        let query = self.query.clone().up_script(helper)?;

        let query_decl = PipelineDecl {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            module: helper.module.clone(),
            id: self.id,
            spec: spec.clone(),
            args,
            query_raw: self.query.clone(),
            query, // The runnable is redefined by `deploy` statements which complete/finalize and seal the runtime arguments
        };
        helper.module.pop();
        let script_name = query_decl.fqsn(&helper.module);
        helper
            .pipeline_defns
            .insert(script_name, query_decl.clone());
        Ok(query_decl)
    }
}

pub type DeployStmtsRaw<'script> = Vec<DeployStmtRaw<'script>>;

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ConnectorDeclRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) kind: (Vec<IdentRaw<'script>>, IdentRaw<'script>),
    pub(crate) args: WithArgsRaw<'script>,
    pub(crate) params: Option<DeployWithExprsRaw<'script>>,
    pub(crate) script: Option<ScriptRaw<'script>>,
}

impl<'script> Upable<'script> for ConnectorDeclRaw<'script> {
    type Target = ConnectorDecl<'script>;
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
        let (spec, args) = arg_spec_resolver(self.args, &self.params, &self.script, helper)?;

        let query_decl = ConnectorDecl {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            module: helper.module.clone(),
            id: self.id,
            spec: spec.clone(),
            args,
        };
        helper.module.pop();
        let script_name = query_decl.fqsn(&helper.module);
        helper
            .connector_defns
            .insert(script_name, query_decl.clone());
        Ok(query_decl)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum ArgumentDeclRaw<'script> {
    Required(IdentRaw<'script>),
    Optional(IdentRaw<'script>, ExprRaw<'script>),
}

pub type WithArgsRaw<'script> = Vec<ArgumentDeclRaw<'script>>;

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FlowDeclRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) args: WithArgsRaw<'script>,
    pub(crate) params: Option<DeployWithExprsRaw<'script>>,
    pub(crate) script: Option<ScriptRaw<'script>>,
    pub(crate) links: DeployLinksRaw<'script>,
}

type ModularTarget<'script> = (Vec<IdentRaw<'script>>, IdentRaw<'script>);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// we're forced to make this pub because of lalrpop
pub enum DeployEndpointRaw<'script> {
    Legacy(StringLitRaw<'script>),
    // FIXME TODO modular target with optional port specification
    Troy(ModularTarget<'script>),
}

impl<'script> Upable<'script> for FlowDeclRaw<'script> {
    type Target = FlowDecl<'script>;
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
        let (spec, args) = arg_spec_resolver(self.args, &self.params, &self.script, helper)?;

        let mut links = HashMap::new();
        for link in self.links {
            let from = match link.from {
                DeployEndpointRaw::Legacy(string_url) => {
                    let raw_url = stateless_run_str!(helper, string_url.up(helper)?);
                    TremorUrl::parse(&raw_url?.to_string())?
                }
                DeployEndpointRaw::Troy(_modular_target) => {
                    // TODO FIXME modular url target resolution and validation
                    TremorUrl::parse("fake-it")?
                }
            };
            let to = match link.to {
                DeployEndpointRaw::Legacy(string_url) => {
                    let raw_url = stateless_run_str!(helper, string_url.up(helper)?);
                    TremorUrl::parse(&raw_url?.to_string())?
                }
                DeployEndpointRaw::Troy(_modular_target) => {
                    // TODO FIXME modular url target resolution and validation
                    TremorUrl::parse("fake-it")?
                }
            };
            links.insert(from, to);
        }

        let flow_decl = FlowDecl {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            module: helper.module.clone(),
            id: self.id,
            spec: spec.clone(),
            args,
            links,
        };
        helper.module.pop();
        let script_name = flow_decl.fqsn(&helper.module);
        helper.flow_defns.insert(script_name, flow_decl.clone());
        Ok(flow_decl)
    }
}

pub type DeployLinksRaw<'script> = Vec<DeployLinkRaw<'script>>;

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DeployLinkRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    pub from: DeployEndpointRaw<'script>,
    /// we're forced to make this pub because of lalrpop
    pub to: DeployEndpointRaw<'script>,
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CreateStmtRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: IdentRaw<'script>,
    pub(crate) params: Option<DeployWithExprsRaw<'script>>,
    pub(crate) script: Option<ScriptRaw<'script>>,
    /// Id of the definition
    pub target: IdentRaw<'script>,
    /// Module of the definition
    pub(crate) module: Vec<IdentRaw<'script>>,
}
impl_expr!(CreateStmtRaw);

/// A fully resolved deployable artefact
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum AtomOfDeployment<'script> {
    /// A deployable pipeline instance
    Pipeline(PipelineDecl<'script>),
    /// A deployable connector instance
    Connector(ConnectorDecl<'script>),
    /// A deployable flow instance
    Flow(FlowDecl<'script>),
}

impl<'script> Upable<'script> for CreateStmtRaw<'script> {
    type Target = CreateStmt<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // NOTE As we can have module aliases and/or nested modules within script definitions
        // that are private to or inline with the script - multiple script definitions in the
        // same module scope can share the same relative function/const module paths.
        //
        // We add the script name to the scope as a means to distinguish these orthogonal
        // definitions. This is achieved with the push/pop pointcut around the up() call
        // below. The actual function registration occurs in the up() call in the usual way.
        //
        helper.module.push(self.id.to_string());

        // TODO check that names across pipeline/flow/connector definitions are unique or else hygienic error

        let target_module = self
            .module
            .iter()
            .map(|x| format!("{}::", x.to_string()))
            .collect::<Vec<String>>()
            .join("");
        let fqsn = format!("{}{}", target_module, self.target.to_string());

        let mut h2 = helper.clone(); // Clone to save some accounting/tracking & cleanup
        let atom = if let Some(artefact) = helper.pipeline_defns.get(&fqsn) {
            let mut args = arg_resolver(
                self.extent(&helper.meta),
                &artefact.spec,
                &artefact.args,
                &self.params,
                &self.script,
                &mut h2,
            )?;

            // We use our now resolved compile time arguments to compile
            // the script to its interpretable and runnable final form with
            // the resolved arguments provides as the arguments to the underlying
            // pipeline.
            std::mem::swap(&mut h2.consts.args, &mut args);
            let mut query = artefact.query_raw.clone().up_script(&mut h2)?;
            let mut artefact = artefact.clone();
            std::mem::swap(&mut artefact.query, &mut query);

            AtomOfDeployment::Pipeline(artefact)
        } else if let Some(artefact) = helper.connector_defns.get(&fqsn) {
            let args = arg_resolver(
                self.extent(&helper.meta),
                &artefact.spec,
                &artefact.args,
                &self.params,
                &self.script,
                &mut h2,
            )?;
            let mut artefact = artefact.clone();
            artefact.args = args;
            AtomOfDeployment::Connector(artefact)
        } else if let Some(artefact) = helper.flow_defns.get(&fqsn) {
            let args = arg_resolver(
                self.extent(&helper.meta),
                &artefact.spec,
                &artefact.args,
                &self.params,
                &self.script,
                &mut h2,
            )?;
            let mut artefact = artefact.clone();
            artefact.args = args;
            AtomOfDeployment::Flow(artefact)
        } else {
            let inner = if target_module.is_empty() {
                self.id.extent(&helper.meta)
            } else {
                crate::pos::Range(self.module[0].s(&helper.meta), self.target.e(&helper.meta))
            };
            return Err(ErrorKind::DeployArtefactNotDefined(
                self.extent(&helper.meta),
                inner.extent(&helper.meta),
                fqsn,
            )
            .into());
        };

        let module = (&self.module).iter().map(ToString::to_string).collect();
        let create_stmt = CreateStmt {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            id: self.id.to_string(),
            script: match self.script {
                Some(script) => Some(script.up_script(helper)?),
                None => None,
            },
            module,
            target: self.target.to_string(),
            atom,
        };
        let script_name = create_stmt.fqsn(&helper.module);
        helper.instances.insert(script_name, create_stmt.clone());
        helper.module.pop();

        Ok(create_stmt)
    }
}
