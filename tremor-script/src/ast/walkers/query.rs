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

use crate::ast::{
    helper::raw::WindowName, module::Content, CreationalWith, DefinitionalArgs, WithExpr,
};

use super::super::visitors::prelude::*;
macro_rules! stop {
    ($e:expr, $leave_fn:expr) => {
        if $e? == VisitRes::Stop {
            return $leave_fn;
        }
    };
}

/// Visitor for traversing all `ImutExprInt`s within the given `ImutExprInt`
///
/// Implement your custom expr visiting logic by overwriting the visit_* methods.
/// You do not need to traverse further down. This is done by the provided `walk_*` methods.
/// The walk_* methods implement walking the expression tree, those do not need to be changed.
pub trait Walker<'script>: ExprWalker<'script> + QueryVisitor<'script> {
    /// walks a `GroupBy`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_group_by(&mut self, group_by: &mut GroupBy<'script>) -> Result<()> {
        stop!(self.visit_group_by(group_by), self.leave_group_by(group_by));
        match group_by {
            GroupBy::Each { expr, .. } | GroupBy::Expr { expr, .. } => {
                ImutExprWalker::walk_expr(self, expr)?;
            }
            GroupBy::Set { items, .. } => {
                for g in items {
                    self.walk_group_by(g)?;
                }
            }
        }
        self.leave_group_by(group_by)
    }
    /// walks a `Script`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_script(&mut self, script: &mut Script<'script>) -> Result<()> {
        stop!(self.visit_script(script), self.leave_script(script));
        if let Some(state) = script.state.as_mut() {
            ImutExprWalker::walk_expr(self, state)?;
        }
        for e in &mut script.exprs {
            ExprWalker::walk_expr(self, e)?;
        }
        self.leave_script(script)
    }

    /// walks a `Select`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_select(&mut self, select: &mut Select<'script>) -> Result<()> {
        stop!(self.visit_select(select), self.leave_select(select));
        ImutExprWalker::walk_expr(self, &mut select.target)?;
        if let Some(w) = select.maybe_where.as_mut() {
            ImutExprWalker::walk_expr(self, w)?;
        };
        if let Some(h) = select.maybe_having.as_mut() {
            ImutExprWalker::walk_expr(self, h)?;
        };
        if let Some(g) = select.maybe_group_by.as_mut() {
            self.walk_group_by(g)?;
        };

        for w in &mut select.windows {
            self.walk_window_name(w)?;
        }

        self.leave_select(select)
    }

    /// walks a `WindowName`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_window_name(&mut self, window: &mut WindowName) -> Result<()> {
        stop!(
            self.visit_window_name(window),
            self.leave_window_name(window)
        );
        self.leave_window_name(window)
    }

    /// walks a `WindowDefinition`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_window_defn(&mut self, defn: &mut WindowDefinition<'script>) -> Result<()> {
        stop!(self.visit_window_defn(defn), self.leave_window_defn(defn));
        self.walk_creational_with(&mut defn.params)?;
        if let Some(script) = defn.script.as_mut() {
            self.walk_script(script)?;
        }
        self.leave_window_defn(defn)
    }

    /// walks a `CreationalWith`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_creational_with(&mut self, with: &mut CreationalWith<'script>) -> Result<()> {
        stop!(
            self.visit_creational_with(with),
            self.leave_creational_with(with)
        );
        for w in &mut with.with.0 {
            self.walk_with_expr(w)?;
        }
        self.leave_creational_with(with)
    }

    /// walks a `WithExpr`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_with_expr(&mut self, with: &mut WithExpr<'script>) -> Result<()> {
        stop!(self.visit_with_expr(with), self.leave_with_expr(with));
        self.walk_ident(&mut with.0)?;
        ImutExprWalker::walk_expr(self, &mut with.1)?;
        self.leave_with_expr(with)
    }

    /// walks a `DefinitioalArgs`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_definitional_args(&mut self, args: &mut DefinitionalArgs<'script>) -> Result<()> {
        stop!(
            self.visit_definitional_args(args),
            self.leave_definitional_args(args)
        );
        for w in &mut args.args.0 {
            self.walk_args_expr(w)?;
        }
        self.leave_definitional_args(args)
    }

    /// walks a `DefinitioalArgsWith`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_definitinal_args_with(
        &mut self,
        args: &mut DefinitionalArgsWith<'script>,
    ) -> Result<()> {
        stop!(
            self.visit_definitional_args_with(args),
            self.leave_definitional_args_with(args)
        );
        for a in &mut args.args.0 {
            self.walk_args_expr(a)?;
        }
        for w in &mut args.with.0 {
            self.walk_with_expr(w)?;
        }
        self.leave_definitional_args_with(args)
    }

    /// walks a `WithExpr`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_args_expr(&mut self, args: &mut ArgsExpr<'script>) -> Result<()> {
        stop!(self.visit_args_expr(args), self.leave_args_expr(args));
        self.walk_ident(&mut args.0)?;
        if let Some(expr) = args.1.as_mut() {
            ImutExprWalker::walk_expr(self, expr)?;
        }
        self.leave_args_expr(args)
    }

    /// walks a `OperatorDefinition`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_operator_defn(&mut self, defn: &mut OperatorDefinition<'script>) -> Result<()> {
        stop!(
            self.visit_operator_defn(defn),
            self.leave_operator_defn(defn)
        );
        self.walk_definitinal_args_with(&mut defn.params)?;
        self.leave_operator_defn(defn)
    }

    /// walks a `ScriptDefinition`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_script_defn(&mut self, defn: &mut ScriptDefinition<'script>) -> Result<()> {
        stop!(self.visit_script_defn(defn), self.leave_script_defn(defn));
        self.walk_definitional_args(&mut defn.params)?;
        self.walk_script(&mut defn.script)?;
        self.leave_script_defn(defn)
    }

    /// walks a `PipelineDefinition`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_pipeline_definition(&mut self, defn: &mut PipelineDefinition<'script>) -> Result<()> {
        stop!(
            self.visit_pipeline_defn(defn),
            self.leave_pipeline_defn(defn)
        );

        self.walk_definitional_args(&mut defn.params)?;
        self.leave_pipeline_defn(defn)
    }

    /// walks a `StreamStmt`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_stream_stmt(&mut self, stmt: &mut StreamCreate) -> Result<()> {
        stop!(self.visit_stream_stmt(stmt), self.leave_stream_stmt(stmt));
        self.leave_stream_stmt(stmt)
    }

    /// walks a `OperatorCreate`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_operator_create(&mut self, stmt: &mut OperatorCreate<'script>) -> Result<()> {
        stop!(
            self.visit_operator_create(stmt),
            self.leave_operator_create(stmt)
        );
        self.walk_creational_with(&mut stmt.params)?;
        self.leave_operator_create(stmt)
    }

    /// walks a `ScriptCreate`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_script_stmt(&mut self, stmt: &mut ScriptCreate<'script>) -> Result<()> {
        stop!(
            self.visit_script_create(stmt),
            self.leave_script_create(stmt)
        );
        self.walk_creational_with(&mut stmt.params)?;
        self.leave_script_create(stmt)
    }

    /// walks a `PipelineCreate`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_pipeline_stmt(&mut self, stmt: &mut PipelineCreate) -> Result<()> {
        stop!(
            self.visit_pipeline_create(stmt),
            self.leave_pipeline_create(stmt)
        );
        self.leave_pipeline_create(stmt)
    }

    /// walks a `SelectStmt`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_select_stmt(&mut self, stmt: &mut SelectStmt<'script>) -> Result<()> {
        stop!(self.visit_select_stmt(stmt), self.leave_select_stmt(stmt));
        self.walk_select(stmt.stmt.as_mut())?;
        self.leave_select_stmt(stmt)
    }

    /// entry point into this visitor - call this to start visiting the given statment
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_stmt(&mut self, stmt: &mut Stmt<'script>) -> Result<()> {
        stop!(self.visit_stmt(stmt), self.leave_stmt(stmt));
        match stmt {
            Stmt::WindowDefinition(d) => self.walk_window_defn(d.as_mut())?,
            Stmt::OperatorDefinition(d) => self.walk_operator_defn(d)?,
            Stmt::ScriptDefinition(d) => self.walk_script_defn(d.as_mut())?,
            Stmt::PipelineDefinition(d) => self.walk_pipeline_definition(d.as_mut())?,
            Stmt::StreamCreate(s) => self.walk_stream_stmt(s)?,
            Stmt::OperatorCreate(s) => self.walk_operator_create(s)?,
            Stmt::ScriptCreate(s) => self.walk_script_stmt(s)?,
            Stmt::PipelineCreate(s) => self.walk_pipeline_stmt(s)?,
            Stmt::SelectStmt(s) => self.walk_select_stmt(s)?,
        }
        self.leave_stmt(stmt)
    }

    /// alternative entry point into this visitor - call this to start visiting the given query
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_query(&mut self, q: &mut Query<'script>) -> Result<()> {
        stop!(self.visit_query(q), self.leave_query(q));

        for s in &mut q.stmts {
            self.walk_stmt(s)?;
        }
        self.walk_module_content(&mut q.scope.content)?;
        self.leave_query(q)
    }

    /// alternative entry point into this visitor - call this to start visiting the given query
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_module_content(&mut self, m: &mut Content<'script>) -> Result<()> {
        stop!(self.visit_module_content(m), self.leave_module_content(m));
        for d in m.windows.values_mut() {
            self.walk_window_defn(d)?;
        }
        for d in m.pipelines.values_mut() {
            self.walk_pipeline_definition(d)?;
        }
        for d in m.scripts.values_mut() {
            self.walk_script_defn(d)?;
        }
        for d in m.operators.values_mut() {
            self.walk_operator_defn(d)?;
        }
        for d in m.functions.values_mut() {
            self.walk_fn_defn(d)?;
        }
        for d in m.consts.values_mut() {
            self.walk_const(d)?;
        }
        // We can't have flows and connectors in query

        self.leave_module_content(m)
    }
}
