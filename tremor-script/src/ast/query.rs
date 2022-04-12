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

use self::raw::{ArgsExprsRaw, DefinitioalArgsRaw, QueryRaw};

use super::{
    error_generic, error_no_consts, error_no_locals,
    node_id::NodeId,
    raw::{IdentRaw, ImutExprRaw, LiteralRaw},
    visitors::{ArgsRewriter, ConstFolder},
    walkers::QueryWalker,
    EventPath, HashMap, Helper, Ident, ImutExpr, InvokeAggrFn, Location, NodeMetas, Path, Result,
    Script, Serialize, Stmts, Upable, Value,
};
use super::{raw::BaseExpr, Consts};
use crate::{
    ast::{eq::AstEq, walkers::ImutExprWalker},
    errors::Error,
};
use crate::{impl_expr_mid, impl_fqn};
use raw::WindowDefnRaw;
use simd_json::{Builder, Mutable};

/// A Tremor query
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Query<'script> {
    /// Config for the query
    pub config: HashMap<String, Value<'script>>,
    /// Statements
    pub stmts: Stmts<'script>,
    /// Query Node Metadata
    pub node_meta: NodeMetas,
    /// Window declarations
    pub windows: HashMap<String, WindowDefinition<'script>>,
    /// Script declarations
    pub scripts: HashMap<String, ScriptDefinition<'script>>,
    /// Operators declarations
    pub operators: HashMap<String, OperatorDefinition<'script>>,
    /// Query Constants
    pub consts: Consts<'script>,
    /// Params if this is a modular query
    pub params: DefinitioalArgs<'script>,
}

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
    PipelineCreate(PipelineCreate),
    /// A select statement
    SelectStmt(SelectStmt<'script>),
}

#[cfg(not(tarpaulin_include))] // this is a simple passthrough
impl<'script> BaseExpr for Stmt<'script> {
    fn mid(&self) -> usize {
        match self {
            Stmt::WindowDefinition(s) => s.mid(),
            Stmt::StreamStmt(s) => s.mid(),
            Stmt::OperatorDefinition(s) => s.mid(),
            Stmt::ScriptDefinition(s) => s.mid(),
            Stmt::PipelineDefinition(s) => s.mid(),
            Stmt::PipelineCreate(s) => s.mid(),
            Stmt::OperatorCreate(s) => s.mid(),
            Stmt::ScriptCreate(s) => s.mid(),
            Stmt::SelectStmt(s) => s.mid(),
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
    /// Node metadata nodes
    pub node_meta: NodeMetas,
}
#[cfg(not(tarpaulin_include))] // this is a simple passthrough
impl<'script> BaseExpr for SelectStmt<'script> {
    fn mid(&self) -> usize {
        self.stmt.mid()
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
        if self
            .stmt
            .target
            .ast_eq(&ImutExpr::Path(Path::Event(EventPath {
                mid: 0,
                segments: vec![],
            })))
            && self.stmt.maybe_group_by.is_none()
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
    pub(crate) mid: usize,
    /// Module of the operator
    pub module: String,
    /// Operator name
    pub operation: String,
}

impl BaseExpr for OperatorKind {
    fn mid(&self) -> usize {
        self.mid
    }
}

/// An operator declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorDefinition<'script> {
    /// The ID and Module of the Operator
    pub node_id: NodeId,
    /// metadata id
    pub(crate) mid: usize,
    /// Type of the operator
    pub kind: OperatorKind,
    /// Parameters for the operator
    pub params: DefinitioalArgsWith<'script>,
}
impl_expr_mid!(OperatorDefinition);
impl_fqn!(OperatorDefinition);

/// An operator creation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorCreate<'script> {
    /// The ID and Module of the Operator
    pub node_id: NodeId,
    /// metadata id
    pub(crate) mid: usize,
    /// Target of the operator
    pub target: NodeId,
    /// parameters of the instance
    pub params: CreationalWith<'script>,
}
impl_expr_mid!(OperatorCreate);

/// A script declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptDefinition<'script> {
    pub(crate) mid: usize,
    /// The ID and Module of the Script
    pub node_id: NodeId,
    /// Parameters of a script declaration
    pub params: DefinitioalArgs<'script>,
    /// The script itself
    pub script: Script<'script>,
}
impl_expr_mid!(ScriptDefinition);
impl_fqn!(ScriptDefinition);

/// A script creation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptCreate<'script> {
    /// The ID and Module of the Script
    pub node_id: NodeId,
    /// metadata id
    pub(crate) mid: usize,
    /// Target of the script
    pub target: NodeId,
    /// Parameters of the script statement
    pub params: CreationalWith<'script>,
}
impl_expr_mid!(ScriptCreate);

/// A pipeline declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PipelineDefinition<'script> {
    /// The ID and Module of the SubqueryDecl
    pub node_id: NodeId,
    /// metadata id
    pub(crate) mid: usize,
    /// Parameters of a subquery declaration
    pub params: DefinitioalArgs<'script>,
    /// Input Ports
    pub from: Vec<Ident<'script>>,
    /// Output Ports
    pub into: Vec<Ident<'script>>,
    /// The raw pipeline statements
    pub raw_stmts: raw::StmtsRaw<'script>,
    /// The query in it's runnable form
    pub query: Option<Query<'script>>,
}

impl<'script> PipelineDefinition<'script> {
    pub(crate) fn to_query(&self) -> Result<Query<'script>> {
        Ok(self
            .query
            .clone()
            .ok_or(format!("not a toplevel query: {}", &self.node_id.id()))?)
    }

    pub(crate) fn apply_args<'registry>(
        &mut self,
        args_in: &Value<'script>,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<()> {
        dbg!();
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
                            start: k.s(&helper.meta),
                            end: k.e(&helper.meta),
                            id: k.id,
                        },
                        v.map(|v| -> Result<_> {
                            let start = v.s(&helper.meta);
                            let end = v.e(&helper.meta);
                            let value = v.try_into_lit(&helper.meta)?;
                            Ok(ImutExprRaw::Literal(LiteralRaw { start, end, value }))
                        })
                        .transpose()?,
                    ))
                })
                .collect::<Result<_>>()?,
        );
        let params = DefinitioalArgsRaw { args };
        let mut query = QueryRaw {
            config: vec![],
            stmts: self.raw_stmts.clone(),
            params,
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
                Stmt::PipelineCreate(_) => unreachable!(),
                Stmt::OperatorCreate(op) => op.params.substitute_args(&args_in, helper)?,
                Stmt::ScriptCreate(script) => script.params.substitute_args(&args_in, helper)?,
                Stmt::SelectStmt(select) => {
                    ArgsRewriter::new(args_in.clone(), helper).walk_select(select.stmt.as_mut())?;
                }
            }
            ConstFolder::new(&helper).walk_stmt(stmt)?;
        }
        self.query = Some(query);
        dbg!();
        Ok(())
    }
}
impl_expr_mid!(PipelineDefinition);
impl_fqn!(PipelineDefinition);

/// A pipeline creation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PipelineCreate {
    pub(crate) mid: usize,
    /// Module of the pipeline
    pub module: Vec<String>,
    /// ID of the pipeline
    pub id: String,
    /// Map of pipeline ports and internal stream id
    pub port_stream_map: HashMap<String, String>,
}
impl BaseExpr for PipelineCreate {
    fn mid(&self) -> usize {
        self.mid
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
    pub(crate) mid: usize,
    /// The type of window
    pub kind: WindowKind,
    /// Parameters passed to the window
    pub params: CreationalWith<'script>,
    /// The script of the window
    pub script: Option<Script<'script>>,
}
impl_expr_mid!(WindowDefinition);
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
    pub mid: usize,
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
impl_expr_mid!(Select);

/// A group by clause
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum GroupBy<'script> {
    /// Expression based group by
    Expr {
        /// mid
        mid: usize,
        /// expr
        expr: ImutExpr<'script>,
    },
    /// `set` based group by
    Set {
        /// mid
        mid: usize,
        /// items
        items: Vec<GroupBy<'script>>,
    },
    /// `each` based group by
    Each {
        /// mid
        mid: usize,
        /// expr
        expr: ImutExpr<'script>,
    },
}

/// A stream statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StreamStmt {
    pub(crate) mid: usize,
    /// ID if the stream
    pub id: String,
}

impl BaseExpr for StreamStmt {
    fn mid(&self) -> usize {
        self.mid
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
    pub fn render(&self, meta: &NodeMetas) -> Result<Value<'script>> {
        let mut res = Value::object();
        for (k, v) in self.with.0.iter() {
            res.insert(k.id.clone(), v.clone().try_into_lit(meta)?.clone())?;
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
    pub(crate) fn substitute_args<'registry>(
        &mut self,
        args: &Value<'script>,
        helper: &mut Helper<'script, 'registry>,
    ) -> Result<()> {
        // We do NOT replace external args in the `with` part as this part will be replaced used
        // with the internal args
        //
        // ```
        //   define pipeline pipeline_name
        //   args
        //     pipeline_server_name
        //   pipeline
        //     define http connector server
        //     args
        //       server_name: args.pipeline_server_name # this gets replaced
        //     with
        //       config = {"server": args.server_name} # this does not get replaced
        //     end;
        //     # ...
        //  end
        // ```

        self.args.substitute_args(args, helper)
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
    pub fn render(&self, meta: &NodeMetas) -> Result<Value<'script>> {
        let mut res = Value::object();
        for (k, v) in self.args.0.iter() {
            // FIXME: hygenic error
            res.insert(
                k.id.clone(),
                v.clone()
                    .ok_or_else(|| Error::from(format!("missing key: {}", k)))?
                    .try_into_lit(meta)?
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
