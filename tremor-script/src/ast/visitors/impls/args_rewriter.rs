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

use super::super::prelude::*;
use crate::{ast::NodeMeta, Value};

/// Rewrites a path to `args` or an element of `args` inside a script/query
/// into an expression referencing the concrete `args` values
/// in order to not leak the current `args` into other scopes upon nesting modules or subqueries, all referencing `args`
pub struct ArgsRewriter<'script, 'registry, 'meta> {
    args: ImutExpr<'script>,
    helper: &'meta mut Helper<'script, 'registry>,
}

impl<'script, 'registry, 'meta> ArgsRewriter<'script, 'registry, 'meta> {
    /// New rewriter
    pub fn new(
        args: Value<'script>,
        helper: &'meta mut Helper<'script, 'registry>,
        mid: &NodeMeta,
    ) -> Self {
        let args: ImutExpr = Literal {
            mid: Box::new(mid.clone()),
            value: args,
        }
        .into();
        Self { args, helper }
    }

    pub(crate) fn rewrite_expr(&mut self, expr: &mut ImutExpr<'script>) -> Result<()> {
        ImutExprWalker::walk_expr(self, expr)?;
        Ok(())
    }
}

impl<'script> ImutExprWalker<'script> for ArgsRewriter<'script, '_, '_> {}

impl<'script> ExprWalker<'script> for ArgsRewriter<'script, '_, '_> {}
impl<'script> QueryWalker<'script> for ArgsRewriter<'script, '_, '_> {}
impl<'script> DeployWalker<'script> for ArgsRewriter<'script, '_, '_> {}

impl<'script> ImutExprVisitor<'script> for ArgsRewriter<'script, '_, '_> {
    fn visit_path(&mut self, path: &mut Path<'script>) -> Result<VisitRes> {
        if let Path::Reserved(ReservedPath::Args { segments, mid }) = path {
            let var = self.helper.register_shadow_from_mid(mid);
            let new = ExprPath {
                expr: Box::new(self.args.clone()),
                segments: segments.clone(),
                mid: mid.clone(),
                var,
            };
            *path = Path::Expr(new);
            self.helper.end_shadow_var();
        }
        Ok(VisitRes::Walk)
    }
}

impl<'script> ExprVisitor<'script> for ArgsRewriter<'script, '_, '_> {
    fn visit_expr(&mut self, _e: &mut Expr<'script>) -> Result<VisitRes> {
        Ok(VisitRes::Walk)
    }

    fn leave_expr(&mut self, _e: &mut Expr<'script>) -> Result<()> {
        Ok(())
    }

    fn visit_fn_defn(&mut self, _e: &mut FnDefn<'script>) -> Result<VisitRes> {
        Ok(VisitRes::Walk)
    }

    fn leave_fn_defn(&mut self, _e: &mut FnDefn<'script>) -> Result<()> {
        Ok(())
    }

    fn visit_comprehension(
        &mut self,
        _comp: &mut Comprehension<'script, Expr<'script>>,
    ) -> Result<VisitRes> {
        Ok(VisitRes::Walk)
    }

    fn leave_comprehension(
        &mut self,
        _comp: &mut Comprehension<'script, Expr<'script>>,
    ) -> Result<()> {
        Ok(())
    }

    fn visit_emit(&mut self, _emit: &mut EmitExpr<'script>) -> Result<VisitRes> {
        Ok(VisitRes::Walk)
    }

    fn leave_emit(&mut self, _emit: &mut EmitExpr<'script>) -> Result<()> {
        Ok(())
    }

    fn visit_ifelse(&mut self, _mifelse: &mut IfElse<'script, Expr<'script>>) -> Result<VisitRes> {
        Ok(VisitRes::Walk)
    }

    fn leave_ifelse(&mut self, _mifelse: &mut IfElse<'script, Expr<'script>>) -> Result<()> {
        Ok(())
    }

    fn visit_default_case(
        &mut self,
        _mdefault: &mut DefaultCase<Expr<'script>>,
    ) -> Result<VisitRes> {
        Ok(VisitRes::Walk)
    }

    fn leave_default_case(&mut self, _mdefault: &mut DefaultCase<Expr<'script>>) -> Result<()> {
        Ok(())
    }

    fn visit_mmatch(&mut self, _mmatch: &mut Match<'script, Expr<'script>>) -> Result<VisitRes> {
        Ok(VisitRes::Walk)
    }

    fn leave_mmatch(&mut self, _mmatch: &mut Match<'script, Expr<'script>>) -> Result<()> {
        Ok(())
    }

    fn visit_clause_group(
        &mut self,
        _group: &mut ClauseGroup<'script, Expr<'script>>,
    ) -> Result<VisitRes> {
        Ok(VisitRes::Walk)
    }

    fn leave_clause_group(
        &mut self,
        _group: &mut ClauseGroup<'script, Expr<'script>>,
    ) -> Result<()> {
        Ok(())
    }

    fn visit_predicate_clause(
        &mut self,
        _predicate: &mut PredicateClause<'script, Expr<'script>>,
    ) -> Result<VisitRes> {
        Ok(VisitRes::Walk)
    }

    fn leave_predicate_clause(
        &mut self,
        _predicate: &mut PredicateClause<'script, Expr<'script>>,
    ) -> Result<()> {
        Ok(())
    }
}
impl<'script> QueryVisitor<'script> for ArgsRewriter<'script, '_, '_> {}
impl<'script> DeployVisitor<'script> for ArgsRewriter<'script, '_, '_> {}
