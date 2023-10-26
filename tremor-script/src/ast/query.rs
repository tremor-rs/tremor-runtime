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

pub(crate) mod raw;

use super::{
    err_generic, error_no_locals,
    helper::Scope,
    node_id::NodeId,
    visitors::{ArgsRewriter, ConstFolder},
    walkers::QueryWalker,
    EventPath, HashMap, Helper, Ident, ImutExpr, InvokeAggrFn, NodeMeta, Path, Result, Script,
    Serialize, Stmts, Upable, Value,
};
use super::{raw::BaseExpr, Consts};
use crate::ast::optimizer::Optimizer;
use crate::ast::Literal;
use crate::{errors::error_generic, impl_expr};
use raw::WindowName;
use tremor_common::ports::Port;
use value_trait::prelude::*;

/// A Tremor query
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Query<'script> {
    /// Config for the query
    pub config: HashMap<String, Value<'script>>,
    /// Input Ports
    pub from: Vec<Ident<'script>>,
    /// Output Ports
    pub into: Vec<Ident<'script>>,
    /// Statements
    pub stmts: Stmts<'script>,
    /// Params if this is a modular query
    pub params: DefinitionalArgs<'script>,
    /// definitions
    pub scope: Scope<'script>,
    /// metadata
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(Query);

/// Query statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Stmt<'script> {
    /// A window definition
    WindowDefinition(Box<WindowDefinition<'script>>),
    /// An operator definition
    OperatorDefinition(OperatorDefinition<'script>),
    /// A script definition
    ScriptDefinition(Box<ScriptDefinition<'script>>),
    /// An pipeline definition
    PipelineDefinition(Box<PipelineDefinition<'script>>),
    /// A stream creation
    StreamCreate(StreamCreate),
    /// An operator creation
    OperatorCreate(OperatorCreate<'script>),
    /// A script creation
    ScriptCreate(ScriptCreate<'script>),
    /// An pipeline creation
    PipelineCreate(PipelineCreate<'script>),
    /// A select statement
    SelectStmt(SelectStmt<'script>),
}

// #[cfg_attr(coverage, no_coverage)] // this is a simple passthrough
impl<'script> BaseExpr for Stmt<'script> {
    fn meta(&self) -> &NodeMeta {
        match self {
            Stmt::WindowDefinition(s) => s.meta(),
            Stmt::StreamCreate(s) => s.meta(),
            Stmt::OperatorDefinition(s) => s.meta(),
            Stmt::ScriptDefinition(s) => s.meta(),
            Stmt::PipelineDefinition(s) => s.meta(),
            Stmt::PipelineCreate(s) => s.meta(),
            Stmt::OperatorCreate(s) => s.meta(),
            Stmt::ScriptCreate(s) => s.meta(),
            Stmt::SelectStmt(s) => s.meta(),
        }
    }
}

/// array of aggregate functions
pub type Aggregates<'f> = Vec<InvokeAggrFn<'f>>;
/// array of aggregate functions (as slice)
pub type AggrSlice<'f> = [InvokeAggrFn<'f>];

///
/// A Select statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SelectStmt<'script> {
    /// The select statement
    pub stmt: Box<Select<'script>>,
    /// Aggregates
    pub aggregates: Aggregates<'script>,
    /// Constants
    pub consts: Consts<'script>,
    /// Number of locals
    pub locals: usize,
}
// #[cfg_attr(coverage, no_coverage)] // this is a simple passthrough
impl<'script> BaseExpr for SelectStmt<'script> {
    fn meta(&self) -> &NodeMeta {
        self.stmt.meta()
    }
}

/// The type of a select statement
pub enum SelectType {
    /// This select statement can be turned
    /// into a passthrough node
    Passthrough,
    /// This is a simple statement without grouping
    /// or windowing
    Simple,
    /// This is a full fledged select statement
    Normal,
}

impl SelectStmt<'_> {
    /// Determine how complex a select statement is
    #[must_use]
    pub fn complexity(&self) -> SelectType {
        if matches!(
            &self.stmt.target,
            ImutExpr::Path(Path::Event(EventPath {
                segments, ..
            })) if segments.is_empty()
        ) && self.stmt.maybe_group_by.is_none()
            && self.stmt.windows.is_empty()
        {
            if self.stmt.maybe_having.is_none() && self.stmt.maybe_where.is_none() {
                SelectType::Passthrough
            } else {
                SelectType::Simple
            }
        } else {
            SelectType::Normal
        }
    }
}

/// Operator kind identifier
#[derive(Clone, Debug, PartialEq, Serialize, Eq)]
pub struct OperatorKind {
    pub(crate) mid: Box<NodeMeta>,
    /// Module of the operator
    pub module: String,
    /// Operator name
    pub operation: String,
}

impl BaseExpr for OperatorKind {
    fn meta(&self) -> &NodeMeta {
        &self.mid
    }
}

/// An operator definition
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorDefinition<'script> {
    /// The ID and Module of the Operator
    pub id: String,
    /// metadata id
    pub(crate) mid: Box<NodeMeta>,
    /// Type of the operator
    pub kind: OperatorKind,
    /// Parameters for the operator
    pub params: DefinitionalArgsWith<'script>,
}
impl_expr!(OperatorDefinition);

/// An operator creation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorCreate<'script> {
    /// The ID and Module of the Operator
    pub id: String,
    /// metadata id
    pub(crate) mid: Box<NodeMeta>,
    /// Target of the operator
    pub target: NodeId,
    /// parameters of the instance
    pub params: CreationalWith<'script>,
}
impl_expr!(OperatorCreate);

/// A script definition
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptDefinition<'script> {
    pub(crate) mid: Box<NodeMeta>,
    /// The ID and Module of the Script
    pub id: String,
    /// Parameters of a script definition
    pub params: DefinitionalArgs<'script>,
    /// The script itself
    pub script: Script<'script>,
    /// The script itself
    pub named: HashMap<Port<'script>, Script<'script>>,
}
impl_expr!(ScriptDefinition);

/// A script creation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptCreate<'script> {
    /// The ID and Module of the Script
    pub id: String,
    /// metadata id
    pub(crate) mid: Box<NodeMeta>,
    /// Target of the script
    pub target: NodeId,
    /// Parameters of the script statement
    pub params: CreationalWith<'script>,
}
impl_expr!(ScriptCreate);

/// A config
pub type Config<'script> = Vec<(String, ImutExpr<'script>)>;

/// A pipeline definition
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PipelineDefinition<'script> {
    /// The ID and Module of the PipelineDefinition
    pub id: String,
    /// metadata id
    pub(crate) mid: Box<NodeMeta>,
    /// Parameters of a subquery definition
    pub params: DefinitionalArgs<'script>,
    /// Input Ports
    pub from: Vec<Ident<'script>>,
    /// Output Ports
    pub into: Vec<Ident<'script>>,
    /// Raw config
    pub config: Config<'script>,
    /// The raw pipeline statements
    pub stmts: Stmts<'script>,
    /// The scope
    pub scope: Scope<'script>,
}
impl_expr!(PipelineDefinition);

impl<'script> PipelineDefinition<'script> {
    /// Converts a pipeline defintion into a query
    ///
    /// # Errors
    /// if translation to a query fails
    pub fn to_query<'registry>(
        &self,
        create: &CreationalWith<'script>,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<Query<'script>> {
        let mut create = create.clone();
        Optimizer::new(helper).walk_creational_with(&mut create)?;
        let mut args = create.render()?;

        let mut config = HashMap::new();

        for (k, v) in &self.config {
            let v = v.clone();
            let v = v.try_into_value(helper)?;
            config.insert(k.to_string(), v);
        }

        let scope = self.scope.clone();
        helper.set_scope(scope);

        let mut params = self.params.clone();
        for (k, v) in &mut params.args.0 {
            if let Some(new) = args.remove(k.as_str())? {
                *v = Some(*Literal::boxed_expr(Box::new(k.meta().clone()), new));
            }
        }
        if let Some(k) = args.as_object().and_then(|o| o.keys().next()) {
            return err_generic(&create, &create, &format!("Unknown parameter {k}"));
        }
        Optimizer::new(helper).walk_definitional_args(&mut params)?;
        let inner_args = params.render()?;
        let stmts = self
            .stmts
            .iter()
            .cloned()
            .map(|s| s.apply_args(&inner_args, helper, params.meta()))
            .collect::<Result<_>>()?;

        Ok(Query {
            config,
            stmts,
            from: self.from.clone(),
            into: self.into.clone(),
            params: self.params.clone(),
            scope: helper.leave_scope()?,
            mid: self.mid.clone(),
        })
    }
}

/// A pipeline creation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PipelineCreate<'script> {
    pub(crate) mid: Box<NodeMeta>,
    /// The node id of the pipeline definition we want to create
    pub target: NodeId,
    /// Map of pipeline ports and internal stream id
    pub port_stream_map: HashMap<String, String>,
    /// With arguments
    pub params: CreationalWith<'script>,
    /// local alias
    pub alias: String,
}
impl<'script> BaseExpr for PipelineCreate<'script> {
    fn meta(&self) -> &NodeMeta {
        &self.mid
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize, Eq)]
pub enum WindowKind {
    /// we're forced to make this pub because of lalrpop
    Sliding,
    /// we're forced to make this pub because of lalrpop
    Tumbling,
}

/// A window definition
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowDefinition<'script> {
    /// ID and Module of the Window
    pub id: String,
    /// metadata id
    pub(crate) mid: Box<NodeMeta>,
    /// The type of window
    pub kind: WindowKind,
    /// Parameters passed to the window
    pub params: CreationalWith<'script>,
    /// The script of the window
    pub script: Option<Script<'script>>,
    /// The script of the window during ticks
    pub tick_script: Option<Script<'script>>,
    /// initial state of the window
    pub state: Option<Value<'script>>,
}
impl_expr!(WindowDefinition);

impl<'script> WindowDefinition<'script> {
    /// `emit_empty_windows` setting
    pub const EMIT_EMPTY_WINDOWS: &'static str = "emit_empty_windows";
    /// `max_groups` setting
    pub const MAX_GROUPS: &'static str = "max_groups";
    /// `interval` setting
    pub const INTERVAL: &'static str = "interval";
    /// `size` setting
    pub const SIZE: &'static str = "size";
}

/// A select statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Select<'script> {
    /// MetadataID of the statement
    pub mid: Box<NodeMeta>,
    /// The from clause
    pub from: (Ident<'script>, Ident<'script>),
    /// The into claus
    pub into: (Ident<'script>, Ident<'script>),
    /// The target (select part)
    pub target: ImutExpr<'script>,
    /// Where clause
    pub maybe_where: Option<ImutExpr<'script>>,
    /// Having clause
    pub maybe_having: Option<ImutExpr<'script>>,
    /// Group-By clause
    pub maybe_group_by: Option<GroupBy<'script>>,
    /// Window
    pub windows: Vec<WindowName>,
}
impl_expr!(Select);

/// A group by clause
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum GroupBy<'script> {
    /// Expression based group by
    Expr {
        /// mid
        mid: Box<NodeMeta>,
        /// expr
        expr: ImutExpr<'script>,
    },
    /// `set` based group by
    Set {
        /// mid
        mid: Box<NodeMeta>,
        /// items
        items: Vec<GroupBy<'script>>,
    },
    /// `each` based group by
    Each {
        /// mid
        mid: Box<NodeMeta>,
        /// expr
        expr: ImutExpr<'script>,
    },
}

/// A `create stream` statement
#[derive(Clone, Debug, PartialEq, Serialize, Eq)]
pub struct StreamCreate {
    pub(crate) mid: Box<NodeMeta>,
    /// ID if the stream
    pub id: String,
}

impl BaseExpr for StreamCreate {
    fn meta(&self) -> &NodeMeta {
        &self.mid
    }
}

/// A with block in a creational statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CreationalWith<'script> {
    /// `with` seection
    pub with: WithExprs<'script>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(CreationalWith);

impl<'script> CreationalWith<'script> {
    pub(crate) fn substitute_args<'registry>(
        &mut self,
        args: &Value<'script>,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<()> {
        self.with.substitute_args(args, helper, &self.mid)
    }

    /// Renders a with clause into a k/v pair
    /// # Errors
    /// when a value can't be evaluated intoa literal
    pub fn render(&self) -> Result<Value<'script>> {
        let mut res = Value::object();
        for (k, v) in &self.with.0 {
            res.try_insert(k.id.clone(), v.try_as_lit()?.clone());
        }
        Ok(res)
    }
}

/// A args / with block in a definitional statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DefinitionalArgsWith<'script> {
    /// `args` section
    pub args: ArgsExprs<'script>,
    /// With section
    pub with: WithExprs<'script>,
    /// node meta
    pub mid: Box<NodeMeta>,
}

impl<'script> DefinitionalArgsWith<'script> {
    /// Combines the definitional args and with block along with the creational with block
    /// here the following happens:
    /// 1) The creational with is merged into the definitial with, overwriting defaults
    /// 2) We check if all mandatory fiends defined in the creational-args are set
    /// 3) we incoperate the merged args into the creational with - this results in the final map
    /// in the with section
    ///
    /// # Errors
    /// for unknown keys
    pub fn ingest_creational_with(&mut self, creational: &CreationalWith<'script>) -> Result<()> {
        // Ingest creational `with` into definitional `args` and error if `with` contains
        // a unknown key
        for (k, v) in &creational.with.0 {
            if let Some((_, arg_v)) = self
                .args
                .0
                .iter_mut()
                .find(|(arg_key, _)| arg_key.id == k.id)
            {
                *arg_v = Some(v.clone());
            } else {
                return err_generic(creational, k, &"Unknown key");
            }
        }

        if let Some((k, _)) = self.args.0.iter_mut().find(|(_, v)| v.is_none()) {
            err_generic(creational, k, &"Missing key")
        } else {
            Ok(())
        }
    }

    /// Generates the config
    ///
    /// # Errors
    ///   * If the config could not be generated
    pub fn generate_config<'registry>(
        &self,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<Value<'script>> {
        let args = self
            .args
            .0
            .iter()
            .map(|(k, expr)| {
                let expr = expr
                    .clone()
                    .ok_or_else(|| format!("Missing configuration variable {k}"))?;
                Ok((k.id.clone(), ConstFolder::reduce_to_val(helper, expr)?))
            })
            .collect::<Result<Value>>()?;

        let config = self
            .with
            .0
            .iter()
            .map(|(k, v)| {
                let mut expr = v.clone();
                ArgsRewriter::new(args.clone(), helper, &self.mid).rewrite_expr(&mut expr)?;
                Ok((k.id.to_string(), ConstFolder::reduce_to_val(helper, expr)?))
            })
            .collect();
        config
    }
}

/// A args block in a definitional statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DefinitionalArgs<'script> {
    /// `args` seection
    pub(crate) args: ArgsExprs<'script>,
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(DefinitionalArgs);

impl<'script> DefinitionalArgs<'script> {
    /// Combines the definitional args and with block along with the creational with block
    /// here the following happens:
    /// 1) The creational with is merged into the definitial with, overwriting defaults
    /// 2) We check if all mandatory fiends defined in the creational-args are set
    /// 3) we incoperate the merged args into the creational with - this results in the final map
    /// in the with section
    ///
    /// # Errors
    /// for unknown keys
    pub fn ingest_creational_with(&mut self, creational: &CreationalWith<'script>) -> Result<()> {
        // Ingest creational `with` into definitional `args` and error if `with` contains
        // a unknown key
        for (k, v) in &creational.with.0 {
            if let Some((_, arg_v)) = self
                .args
                .0
                .iter_mut()
                .find(|(arg_key, _)| arg_key.id == k.id)
            {
                *arg_v = Some(v.clone());
            } else {
                return err_generic(creational, k, &format!("Unknown argument: {}", k.as_str()));
            }
        }

        if let Some((k, _)) = self.args.0.iter_mut().find(|(_, v)| v.is_none()) {
            let k = k.clone();
            err_generic(self, &k, &format!("Missing required argument: {k}"))
        } else {
            Ok(())
        }
    }

    /// Renders a with clause into a k/v pair
    /// # Errors
    /// on missing keys
    pub fn render(&self) -> Result<Value<'script>> {
        let mut res = Value::object();
        for (k, v) in &self.args.0 {
            let v = v
                .as_ref()
                .ok_or_else(|| error_generic(k, k, &"Required key not provided"))?
                .try_as_lit()?
                .clone();
            res.try_insert(k.id.clone(), v);
        }
        Ok(res)
    }
}

/// a With key value pair.
pub type WithExpr<'script> = (Ident<'script>, ImutExpr<'script>);
/// list of arguments in a `with` section
#[derive(Clone, Debug, PartialEq, Serialize, Default)]
pub struct WithExprs<'script>(pub Vec<WithExpr<'script>>);

impl<'script> WithExprs<'script> {
    pub(crate) fn substitute_args<'registry>(
        &mut self,
        args: &Value<'script>,
        helper: &mut Helper<'script, 'registry>,
        mid: &NodeMeta,
    ) -> Result<()> {
        let mut old = Vec::new();
        std::mem::swap(&mut old, &mut self.0);
        self.0 = old
            .into_iter()
            .map(|(name, mut value_expr)| {
                ArgsRewriter::new(args.clone(), helper, mid).rewrite_expr(&mut value_expr)?;
                Optimizer::new(helper).walk_imut_expr(&mut value_expr)?;
                Ok((name, value_expr))
            })
            .collect::<Result<_>>()?;
        Ok(())
    }
}

/// a Args key value pair.
pub type ArgsExpr<'script> = (Ident<'script>, Option<ImutExpr<'script>>);

/// list of arguments in a `args` section
#[derive(Clone, Debug, PartialEq, Serialize, Default)]
pub struct ArgsExprs<'script>(pub Vec<ArgsExpr<'script>>);

impl<'script> Stmt<'script> {
    fn apply_args(
        mut self,
        args: &Value<'script>,
        helper: &mut Helper<'script, '_>,
        mid: &NodeMeta,
    ) -> Result<Self> {
        match &mut self {
            // For definitions, select andstreams we do not substitute incomming args
            // their args are handled via the create statements
            Stmt::WindowDefinition(_)
            | Stmt::OperatorDefinition(_)
            | Stmt::ScriptDefinition(_)
            | Stmt::PipelineDefinition(_)
            | Stmt::StreamCreate(_) => (),
            Stmt::SelectStmt(s) => {
                ArgsRewriter::new(args.clone(), helper, mid).walk_select_stmt(s)?;
                Optimizer::new(helper).walk_select_stmt(s)?;
            }
            Stmt::OperatorCreate(d) => d.params.substitute_args(args, helper)?,
            Stmt::ScriptCreate(d) => d.params.substitute_args(args, helper)?,
            Stmt::PipelineCreate(d) => d.params.substitute_args(args, helper)?,
        };
        Ok(self)
    }
}
