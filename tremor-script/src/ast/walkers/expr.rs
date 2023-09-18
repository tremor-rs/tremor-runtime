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

macro_rules! stop {
    ($e:expr, $leave_fn:expr) => {
        if $e? == VisitRes::Stop {
            return $leave_fn;
        }
    };
}

/// Walker for traversing all `Exprs`s within the given `Exprs`
///
/// You only need to impl this without overwriting any functions. It is a helper trait
/// for the `ExprVisitor`, to handle any processing on the nodes please use that trait.
pub trait Walker<'script>: ExprVisitor<'script> + ImutExprWalker<'script> {
    /// entry point into this visitor - call this to start visiting the given expression `e`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_expr(&mut self, e: &mut Expr<'script>) -> Result<()> {
        stop!(
            ExprVisitor::visit_expr(self, e),
            ExprVisitor::leave_expr(self, e)
        );
        match e {
            Expr::Match(mmatch) => {
                Walker::walk_match(self, mmatch.as_mut())?;
            }
            Expr::IfElse(ifelse) => {
                self.walk_ifelse(ifelse.as_mut())?;
            }
            Expr::Assign { path, expr, .. } => {
                self.walk_path(path)?;
                Walker::walk_expr(self, expr.as_mut())?;
            }
            Expr::AssignMoveLocal { path, .. } => {
                self.walk_path(path)?;
            }
            Expr::Comprehension(c) => {
                Walker::walk_comprehension(self, c.as_mut())?;
            }
            Expr::Drop { .. } => {}
            Expr::Emit(e) => {
                self.walk_emit(e.as_mut())?;
            }
            Expr::Imut(e) => {
                ImutExprWalker::walk_expr(self, e)?;
            }
        }
        ExprVisitor::leave_expr(self, e)
    }

    /// walk a comprehension
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_fn_defn(&mut self, defn: &mut FnDefn<'script>) -> Result<()> {
        stop!(
            ExprVisitor::visit_fn_defn(self, defn),
            ExprVisitor::leave_fn_defn(self, defn)
        );
        for e in &mut defn.body {
            Walker::walk_expr(self, e)?;
        }
        ExprVisitor::leave_fn_defn(self, defn)
    }

    /// walk a comprehension
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_comprehension(
        &mut self,
        comp: &mut Comprehension<'script, Expr<'script>>,
    ) -> Result<()> {
        stop!(
            ExprVisitor::visit_comprehension(self, comp),
            ExprVisitor::leave_comprehension(self, comp)
        );
        for comp_case in &mut comp.cases {
            if let Some(guard) = &mut comp_case.guard {
                ImutExprWalker::walk_expr(self, guard)?;
            }
            for expr in &mut comp_case.exprs {
                Walker::walk_expr(self, expr)?;
            }
            Walker::walk_expr(self, &mut comp_case.last_expr)?;
        }
        ImutExprWalker::walk_expr(self, &mut comp.target)?;
        ImutExprWalker::walk_expr(self, &mut comp.initial)?;
        ExprVisitor::leave_comprehension(self, comp)
    }

    /// walk a emit expr
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_emit(&mut self, emit: &mut EmitExpr<'script>) -> Result<()> {
        stop!(self.visit_emit(emit), self.leave_emit(emit));
        if let Some(port) = emit.port.as_mut() {
            ImutExprWalker::walk_expr(self, port)?;
        }
        ImutExprWalker::walk_expr(self, &mut emit.expr)?;
        self.leave_emit(emit)
    }

    /// walk a ifelse expr
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_ifelse(&mut self, ifelse: &mut IfElse<'script, Expr<'script>>) -> Result<()> {
        stop!(self.visit_ifelse(ifelse), self.leave_ifelse(ifelse));
        ImutExprWalker::walk_expr(self, &mut ifelse.target)?;
        Walker::walk_predicate_clause(self, &mut ifelse.if_clause)?;
        Walker::walk_default_case(self, &mut ifelse.else_clause)?;
        self.leave_ifelse(ifelse)
    }

    /// walk a `DefaultCase`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_default_case(&mut self, mdefault: &mut DefaultCase<Expr<'script>>) -> Result<()> {
        stop!(
            ExprVisitor::visit_default_case(self, mdefault),
            ExprVisitor::leave_default_case(self, mdefault)
        );
        match mdefault {
            DefaultCase::None | DefaultCase::Null => {}
            DefaultCase::Many { exprs, last_expr } => {
                for e in exprs {
                    Walker::walk_expr(self, e)?;
                }
                Walker::walk_expr(self, last_expr)?;
            }
            DefaultCase::One(last_expr) => {
                Walker::walk_expr(self, last_expr)?;
            }
        }
        ExprVisitor::leave_default_case(self, mdefault)
    }

    /// walk a match expr
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_match(&mut self, mmatch: &mut Match<'script, Expr<'script>>) -> Result<()> {
        stop!(
            ExprVisitor::visit_mmatch(self, mmatch),
            ExprVisitor::leave_mmatch(self, mmatch)
        );
        ImutExprWalker::walk_expr(self, &mut mmatch.target)?;
        for group in &mut mmatch.patterns {
            Walker::walk_clause_group(self, group)?;
        }
        Walker::walk_default_case(self, &mut mmatch.default)?;
        ExprVisitor::leave_mmatch(self, mmatch)
    }

    /// Walks a clause group
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_clause_group(&mut self, group: &mut ClauseGroup<'script, Expr<'script>>) -> Result<()> {
        stop!(
            ExprVisitor::visit_clause_group(self, group),
            ExprVisitor::leave_clause_group(self, group)
        );
        match group {
            ClauseGroup::Single {
                precondition,
                pattern,
            } => {
                if let Some(precondition) = precondition {
                    self.walk_precondition(precondition)?;
                }
                Walker::walk_predicate_clause(self, pattern)?;
            }
            ClauseGroup::Simple {
                precondition,
                patterns,
            } => {
                if let Some(precondition) = precondition {
                    self.walk_precondition(precondition)?;
                }
                for predicate in patterns {
                    Walker::walk_predicate_clause(self, predicate)?;
                }
            }
            ClauseGroup::SearchTree {
                precondition,
                tree,
                rest,
            } => {
                if let Some(precondition) = precondition {
                    self.walk_precondition(precondition)?;
                }
                for (es, e) in tree.values_mut() {
                    for e in es {
                        Walker::walk_expr(self, e)?;
                    }
                    Walker::walk_expr(self, e)?;
                }
                for predicate in rest {
                    Walker::walk_predicate_clause(self, predicate)?;
                }
            }
            ClauseGroup::Combined {
                precondition,
                groups,
            } => {
                if let Some(precondition) = precondition {
                    self.walk_precondition(precondition)?;
                }
                for g in groups {
                    Walker::walk_clause_group(self, g)?;
                }
            }
        }
        ExprVisitor::leave_clause_group(self, group)
    }

    /// Walks a predicate clause
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_predicate_clause(
        &mut self,
        predicate: &mut PredicateClause<'script, Expr<'script>>,
    ) -> Result<()> {
        stop!(
            ExprVisitor::visit_predicate_clause(self, predicate),
            ExprVisitor::leave_predicate_clause(self, predicate)
        );
        self.walk_match_pattern(&mut predicate.pattern)?;
        if let Some(guard) = &mut predicate.guard {
            ImutExprWalker::walk_expr(self, guard)?;
        }
        for expr in &mut predicate.exprs {
            Walker::walk_expr(self, expr)?;
        }
        Walker::walk_expr(self, &mut predicate.last_expr)?;
        ExprVisitor::leave_predicate_clause(self, predicate)
    }
}
