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

use super::super::visitors::prelude::*;
use VisitRes::Walk;

/// Visitor for traversing all `Exprs`s within the given `Exprs`
///
/// Implement your custom expr visiting logic by overwriting the methods.
#[cfg(not(tarpaulin_include))]
pub trait Visitor<'script> {
    /// visit a `DefinitioalArgsWith`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_definitional_args_with(
        &mut self,
        _with: &mut DefinitioalArgsWith<'script>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `DefinitioalArgsWith`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_definitional_args_with(
        &mut self,
        _with: &mut DefinitioalArgsWith<'script>,
    ) -> Result<()> {
        Ok(())
    }

    /// visit a `DefinitioalArgs`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_definitional_args(
        &mut self,
        _with: &mut DefinitioalArgs<'script>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `DefinitioalArgs`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_definitional_args(&mut self, _with: &mut DefinitioalArgs<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `CreationalWith`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_creational_with(&mut self, _with: &mut CreationalWith<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `CreationalWith`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_creational_with(&mut self, _with: &mut CreationalWith<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `WithExpr`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_with_expr(&mut self, _with: &mut WithExpr<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `WithExpr`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_with_expr(&mut self, _with: &mut WithExpr<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `ArgsExpr`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_args_expr(&mut self, _args: &mut ArgsExpr<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `ArgsExpr`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_args_expr(&mut self, _args: &mut ArgsExpr<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `Query`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_query(&mut self, _q: &mut Query<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `Query`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_query(&mut self, _q: &mut Query<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `Stmt`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_stmt(&mut self, _stmt: &mut Stmt<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `Stmt`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_stmt(&mut self, _stmt: &mut Stmt<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `GroupBy`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_group_by(&mut self, _stmt: &mut GroupBy<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `GroupBy`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_group_by(&mut self, _stmt: &mut GroupBy<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `Script`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_script(&mut self, _script: &mut Script<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `Script`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_script(&mut self, _script: &mut Script<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `Select`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_select(&mut self, _script: &mut Select<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `Select`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_select(&mut self, _script: &mut Select<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `WindowDecl`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_window_decl(&mut self, _decl: &mut WindowDecl<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `WindowDecl`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_window_decl(&mut self, _decl: &mut WindowDecl<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `OperatorDecl`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_operator_decl(&mut self, _decl: &mut OperatorDecl<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `OperatorDecl`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_operator_decl(&mut self, _decl: &mut OperatorDecl<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `ScriptDecl`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_script_decl(&mut self, _decl: &mut ScriptDecl<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `ScriptDecl`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_script_decl(&mut self, _decl: &mut ScriptDecl<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `PipelineDecl`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_pipeline_decl(&mut self, _decl: &mut PipelineDecl<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `PipelineDecl`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_pipeline_decl(&mut self, _decl: &mut PipelineDecl<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `StreamStmt`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_stream_stmt(&mut self, _stmt: &mut StreamStmt) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `StreamStmt`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_stream_stmt(&mut self, _stmt: &mut StreamStmt) -> Result<()> {
        Ok(())
    }

    /// visit a `OperatorCreate`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_operator_stmt(&mut self, _stmt: &mut OperatorCreate<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `OperatorCreate`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_operator_stmt(&mut self, _stmt: &mut OperatorCreate<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `ScriptCreate`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_script_stmt(&mut self, _stmt: &mut ScriptCreate<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `ScriptCreate`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_script_stmt(&mut self, _stmt: &mut ScriptCreate<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `PipelineCreate`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_pipeline_stmt(&mut self, _stmt: &mut PipelineCreate) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `PipelineCreate`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_pipeline_stmt(&mut self, _stmt: &mut PipelineCreate) -> Result<()> {
        Ok(())
    }

    /// visit a `SelectStmt`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_select_stmt(&mut self, _stmt: &mut SelectStmt<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `SelectStmt`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_select_stmt(&mut self, _stmt: &mut SelectStmt<'script>) -> Result<()> {
        Ok(())
    }
}
