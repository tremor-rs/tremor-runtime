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

use super::prelude::*;
use super::VisitRes::Walk;

/// Visitor for traversing all `Exprs`s within the given `Exprs`
///
/// Implement your custom expr visiting logic by overwriting the methods.
// #[cfg_attr(coverage, no_coverage)]
pub trait Visitor<'script> {
    /// visit a generic `Expr` (this is called before the concrete `visit_*` method)
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_expr(&mut self, _e: &mut Expr<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a generic `Expr` (this is called after the concrete `leave_*` method)
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_expr(&mut self, _e: &mut Expr<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a generic `Expr` (this is called before the concrete `visit_*` method)
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_fn_defn(&mut self, _e: &mut FnDefn<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a generic `Expr` (this is called after the concrete `leave_*` method)
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_fn_defn(&mut self, _e: &mut FnDefn<'script>) -> Result<()> {
        Ok(())
    }
    /// visit a comprehension
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_comprehension(
        &mut self,
        _comp: &mut Comprehension<'script, Expr<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a comprehension
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_comprehension(
        &mut self,
        _comp: &mut Comprehension<'script, Expr<'script>>,
    ) -> Result<()> {
        Ok(())
    }

    /// visit a `EmitExpr`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_emit(&mut self, _emit: &mut EmitExpr<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `EmitExpr`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_emit(&mut self, _emit: &mut EmitExpr<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `IfElse`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_ifelse(&mut self, _mifelse: &mut IfElse<'script, Expr<'script>>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `IfElse`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_ifelse(&mut self, _mifelse: &mut IfElse<'script, Expr<'script>>) -> Result<()> {
        Ok(())
    }

    /// visit a `DefaultCase`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_default_case(
        &mut self,
        _mdefault: &mut DefaultCase<Expr<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `DefaultCase`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_default_case(&mut self, _mdefault: &mut DefaultCase<Expr<'script>>) -> Result<()> {
        Ok(())
    }

    /// visit a `Match`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_mmatch(&mut self, _mmatch: &mut Match<'script, Expr<'script>>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `Match`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_mmatch(&mut self, _mmatch: &mut Match<'script, Expr<'script>>) -> Result<()> {
        Ok(())
    }

    /// visit a `ClauseGroup`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_clause_group(
        &mut self,
        _group: &mut ClauseGroup<'script, Expr<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `ClauseGroup`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_clause_group(
        &mut self,
        _group: &mut ClauseGroup<'script, Expr<'script>>,
    ) -> Result<()> {
        Ok(())
    }

    /// visit a `PredicateClause`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_predicate_clause(
        &mut self,
        _predicate: &mut PredicateClause<'script, Expr<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `PredicateClause`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_predicate_clause(
        &mut self,
        _predicate: &mut PredicateClause<'script, Expr<'script>>,
    ) -> Result<()> {
        Ok(())
    }
}
