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

use self::raw::{ArgsExprsRaw, ConfigRaw, DefinitioalArgsRaw, QueryRaw};

use super::{
    error_generic, error_no_locals,
    helper::Scope,
    node_id::NodeId,
    raw::{IdentRaw, ImutExprRaw, LiteralRaw},
    visitors::{ArgsRewriter, ConstFolder},
    walkers::QueryWalker,
    EventPath, HashMap, Helper, Ident, ImutExpr, InvokeAggrFn, NodeMeta, Path, Result, Script,
    Serialize, Stmts, Upable, Value,
};
use super::{raw::BaseExpr, Consts};
use crate::{ast::walkers::ImutExprWalker, errors::Error};
use crate::{impl_expr, impl_fqn};
use raw::WindowDefnRaw;
use simd_json::{Builder, Mutable};

/// A Tremor query
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Query<'script> {
    /// Config for the query
    pub config: HashMap<String, Value<'script>>,
    /// Statements
    pub stmts: Stmts<'script>,
    /// Params if this is a modular query
    pub params: DefinitioalArgs<'script>,
    /// definitions
    pub scope: Scope<'script>,
    /// metadata
    pub(crate) mid: Box<NodeMeta>,
}
impl_expr!(Query);

/// Query statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Stmt<'script> {
    /// A window declaration
    WindowDefinition(Box<WindowDefinition<'script>>),
    /// An operator declaration
    OperatorDefinition(OperatorDefinition<'script>),
    /// A script declaration
    ScriptDefinition(Box<ScriptDefinition<'script>>),
    /// An pipeline declaration
    PipelineDefinition(Box<PipelineDefinition<'script>>),
    /// A stream
    StreamStmt(StreamStmt),
    /// An operator creation
    OperatorCreate(OperatorCreate<'script>),
    /// A script creation
    ScriptCreate(ScriptCreate<'script>),
    /// An pipeline creation
    PipelineCreate(PipelineCreate<'script>),
    /// A select statement
    SelectStmt(SelectStmt<'script>),
}

#[cfg(not(tarpaulin_include))] // this is a simple passthrough
impl<'script> BaseExpr for Stmt<'script> {
    fn meta(&self) -> &NodeMeta {
        match self {
            Stmt::WindowDefinition(s) => s.meta(),
            Stmt::StreamStmt(s) => s.meta(),
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
    pub aggregates: Aggregates<'static>,
    /// Constants
    pub consts: Consts<'script>,
    /// Number of locals
    pub locals: usize,
}
#[cfg(not(tarpaulin_include))] // this is a simple passthrough
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
#[derive(Clone, Debug, PartialEq, Serialize)]
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

/// An operator declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorDefinition<'script> {
    /// The ID and Module of the Operator
    pub node_id: NodeId,
    /// metadata id
    pub(crate) mid: Box<NodeMeta>,
    /// Type of the operator
    pub kind: OperatorKind,
    /// Parameters for the operator
    pub params: DefinitioalArgsWith<'script>,
}
impl_expr!(OperatorDefinition);
impl_fqn!(OperatorDefinition);

/// An operator creation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorCreate<'script> {
    /// The ID and Module of the Operator
    pub node_id: NodeId,
    /// metadata id
    pub(crate) mid: Box<NodeMeta>,
    /// Target of the operator
    pub target: NodeId,
    /// parameters of the instance
    pub params: CreationalWith<'script>,
}
impl_expr!(OperatorCreate);

/// A script declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptDefinition<'script> {
    pub(crate) mid: Box<NodeMeta>,
    /// The ID and Module of the Script
    pub node_id: NodeId,
    /// Parameters of a script declaration
    pub params: DefinitioalArgs<'script>,
    /// The script itself
    pub script: Script<'script>,
}
impl_expr!(ScriptDefinition);
impl_fqn!(ScriptDefinition);

/// A script creation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptCreate<'script> {
    /// The ID and Module of the Script
    pub node_id: NodeId,
    /// metadata id
    pub(crate) mid: Box<NodeMeta>,
    /// Target of the script
    pub target: NodeId,
    /// Parameters of the script statement
    pub params: CreationalWith<'script>,
}
impl_expr!(ScriptCreate);

/// A pipeline declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PipelineDefinition<'script> {
    /// The ID and Module of the SubqueryDecl
    pub node_id: NodeId,
    /// metadata id
    pub(crate) mid: Box<NodeMeta>,
    /// Parameters of a subquery declaration
    pub params: DefinitioalArgs<'script>,
    /// Input Ports
    pub from: Vec<Ident<'script>>,
    /// Output Ports
    pub into: Vec<Ident<'script>>,
    /// Raw config
    pub raw_config: ConfigRaw<'script>,
    /// The raw pipeline statements
    pub raw_stmts: raw::StmtsRaw<'script>,
    /// The query in it's runnable form
    pub query: Option<Query<'script>>,
}

impl<'script> PipelineDefinition<'script> {
    /// FIXME: :sob:
    pub fn apply_args<'registry>(
        &mut self,
        args_in: &Value<'script>,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<()> {
        let mut params = self.params.clone();
        params.substitute_args(args_in, helper)?;
        let args = ArgsExprsRaw(
            params
                .args
                .0
                .into_iter()
                .map(|(k, v)| {
                    Ok((
                        IdentRaw {
                            mid: k.mid,
                            id: k.id,
                        },
                        v.map(|v| -> Result<_> {
                            let mid = Box::new(v.meta().clone());
                            let value = v.try_into_lit()?;
                            Ok(ImutExprRaw::Literal(LiteralRaw { mid, value }))
                        })
                        .transpose()?,
                    ))
                })
                .collect::<Result<_>>()?,
        );
        let params = DefinitioalArgsRaw { args };
        let mut query = QueryRaw {
            config: self.raw_config.clone(),
            stmts: self.raw_stmts.clone(),
            params,
            mid: self.mid.clone(),
        }
        .up_script(helper)?;
        for stmt in &mut query.stmts {
            match stmt {
                // definitions do not need to be updated
                Stmt::WindowDefinition(_)
                | Stmt::OperatorDefinition(_)
                | Stmt::ScriptDefinition(_)
                | Stmt::PipelineDefinition(_)
                | Stmt::StreamStmt(_) => (),
                Stmt::PipelineCreate(c) => c.params.substitute_args(&args_in, helper)?,
                Stmt::OperatorCreate(op) => op.params.substitute_args(&args_in, helper)?,
                Stmt::ScriptCreate(script) => script.params.substitute_args(&args_in, helper)?,
                Stmt::SelectStmt(select) => {
                    ArgsRewriter::new(args_in.clone(), helper).walk_select(select.stmt.as_mut())?;
                }
            }
            ConstFolder::new(&helper).walk_stmt(stmt)?;
        }
        self.query = Some(query);
        Ok(())
    }
}
impl_expr!(PipelineDefinition);
impl_fqn!(PipelineDefinition);

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
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum WindowKind {
    /// we're forced to make this pub because of lalrpop
    Sliding,
    /// we're forced to make this pub because of lalrpop
    Tumbling,
}

/// A window declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowDefinition<'script> {
    /// ID and Module of the Window
    pub node_id: NodeId,
    /// metadata id
    pub(crate) mid: Box<NodeMeta>,
    /// The type of window
    pub kind: WindowKind,
    /// Parameters passed to the window
    pub params: CreationalWith<'script>,
    /// The script of the window
    pub script: Option<Script<'script>>,
}
impl_expr!(WindowDefinition);
impl_fqn!(WindowDefinition);

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
    pub windows: Vec<WindowDefnRaw>,
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

/// A stream statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StreamStmt {
    pub(crate) mid: Box<NodeMeta>,
    /// ID if the stream
    pub id: String,
}

impl BaseExpr for StreamStmt {
    fn meta(&self) -> &NodeMeta {
        &self.mid
    }
}

/// A with block in a creational statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CreationalWith<'script> {
    /// `with` seection
    pub with: WithExprs<'script>,
}
impl<'script> CreationalWith<'script> {
    pub(crate) fn substitute_args<'registry>(
        &mut self,
        args: &Value<'script>,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<()> {
        self.with.substitute_args(args, helper)
    }

    /// Renders a with clause into a k/v pair
    pub fn render(&self) -> Result<Value<'script>> {
        let mut res = Value::object();
        for (k, v) in self.with.0.iter() {
            res.insert(k.id.clone(), v.clone().try_into_lit()?.clone())?;
        }
        Ok(res)
    }
}

/// A args / with block in a definitional statement
#[derive(Clone, Debug, PartialEq, Serialize, Default)]
pub struct DefinitioalArgsWith<'script> {
    /// `args` seection
    pub args: ArgsExprs<'script>,
    /// With section
    pub with: WithExprs<'script>,
}

impl<'script> DefinitioalArgsWith<'script> {
    /// Combines the definitional args and with block along with the creational with block
    /// here the following happens:
    /// 1) The creational with is merged into the definitial with, overwriting defaults
    /// 2) We check if all mandatory fiends defined in the creational-args are set
    /// 3) we incoperate the merged args into the creational with - this results in the final map
    /// in the with section
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
                // FIXME: better error
                return Err(format!("unknown key: {}", k).into());
            }
        }

        if let Some((k, _)) = self.args.0.iter_mut().find(|(_, v)| v.is_none()) {
            Err(format!("missing key: {}", k).into())
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
                    .ok_or_else(|| format!("Missing configuration variable {}", k))?;
                Ok((k.id.clone(), ConstFolder::reduce_to_val(helper, expr)?))
            })
            .collect::<Result<Value>>()?;

        let config = self
            .with
            .0
            .iter()
            .map(|(k, v)| {
                let mut expr = v.clone();
                ArgsRewriter::new(args.clone(), helper).rewrite_expr(&mut expr)?;
                Ok((k.id.to_string(), ConstFolder::reduce_to_val(helper, expr)?))
            })
            .collect();
        config
    }
}

/// A args block in a definitional statement
#[derive(Clone, Debug, PartialEq, Serialize, Default)]
pub struct DefinitioalArgs<'script> {
    /// `args` seection
    pub args: ArgsExprs<'script>,
}

impl<'script> DefinitioalArgs<'script> {
    /// Combines the definitional args and with block along with the creational with block
    /// here the following happens:
    /// 1) The creational with is merged into the definitial with, overwriting defaults
    /// 2) We check if all mandatory fiends defined in the creational-args are set
    /// 3) we incoperate the merged args into the creational with - this results in the final map
    /// in the with section
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
                // FIXME: better error
                return Err(format!("unknown key: {}", k).into());
            }
        }

        if let Some((k, _)) = self.args.0.iter_mut().find(|(_, v)| v.is_none()) {
            Err(format!("missing key: {}", k).into())
        } else {
            Ok(())
        }
    }

    pub(crate) fn substitute_args<'registry>(
        &mut self,
        args: &Value<'script>,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<()> {
        self.args.substitute_args(args, helper)
    }

    /// Renders a with clause into a k/v pair
    pub fn render(&self) -> Result<Value<'script>> {
        let mut res = Value::object();
        for (k, v) in self.args.0.iter() {
            // FIXME: hygenic error
            res.insert(
                k.id.clone(),
                v.clone()
                    .ok_or_else(|| Error::from(format!("missing key: {}", k)))?
                    .try_into_lit()?
                    .clone(),
            )?;
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
    ) -> Result<()> {
        let mut old = Vec::new();
        std::mem::swap(&mut old, &mut self.0);
        self.0 = old
            .into_iter()
            .map(|(name, mut value_expr)| {
                ArgsRewriter::new(args.clone(), helper).rewrite_expr(&mut value_expr)?;
                ImutExprWalker::walk_expr(&mut ConstFolder::new(helper), &mut value_expr)?;
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

impl<'script> ArgsExprs<'script> {
    pub(crate) fn substitute_args<'registry>(
        &mut self,
        args: &Value<'script>,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<()> {
        let mut old = Vec::new();
        std::mem::swap(&mut old, &mut self.0);
        self.0 = old
            .into_iter()
            .map(|(name, value_expr)| {
                let value_expr = value_expr
                    .map(|mut value_expr| -> Result<_> {
                        ArgsRewriter::new(args.clone(), helper).rewrite_expr(&mut value_expr)?;
                        ImutExprWalker::walk_expr(&mut ConstFolder::new(helper), &mut value_expr)?;
                        Ok(value_expr)
                    })
                    .transpose()?;

                Ok((name, value_expr))
            })
            .collect::<Result<_>>()?;
        Ok(())
    }
}
