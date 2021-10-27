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

pub(crate) mod prelude;
use prelude::*;

pub(crate) mod args_rewriter;
pub(crate) mod expr_reducer;
pub(crate) mod group_by_extractor;
pub(crate) mod target_event_ref;

pub(crate) use args_rewriter::ArgsRewriter;
pub(crate) use expr_reducer::ExprReducer;
pub(crate) use group_by_extractor::GroupByExprExtractor;
pub(crate) use target_event_ref::TargetEventRef;

#[derive(Clone, Copy, PartialEq, Eq)]
/// Return value from visit methods for `ImutExprIntVisitor`
/// controlling whether to continue walking the subtree or not
pub enum VisitRes {
    /// carry on walking
    Walk,
    /// stop walking
    Stop,
}

use VisitRes::Walk;

/// Visitor for traversing all `Exprs`s within the given `Exprs`
///
/// Implement your custom expr visiting logic by overwriting the methods.
#[cfg(not(tarpaulin_include))]
pub trait ExprVisitor<'script> {
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

/// Visitor for traversing all `ImutExprInt`s within the given `ImutExprInt`
///
/// Implement your custom expr visiting logic by overwriting the methods.
#[cfg(not(tarpaulin_include))]
pub trait ImutExprVisitor<'script> {
    /// visit a `Record`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_record(&mut self, _record: &mut Record<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `Record`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_record(&mut self, _record: &mut Record<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a list
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_list(&mut self, _list: &mut List<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a list
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_list(&mut self, _list: &mut List<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a binary
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_binary(&mut self, _binary: &mut BinExpr<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a binary
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_binary(&mut self, _binary: &mut BinExpr<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `UnaryExpr`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_unary(&mut self, _unary: &mut UnaryExpr<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `UnaryExpr`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_unary(&mut self, _unary: &mut UnaryExpr<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `Patch`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_patch(&mut self, _patch: &mut Patch<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `Patch`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_patch(&mut self, _patch: &mut Patch<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `PatchOperation`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_patch_operation(&mut self, _patch: &mut PatchOperation<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `PatchOperation`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_patch_operation(&mut self, _patch: &mut PatchOperation<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a match expr
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_precondition(
        &mut self,
        _precondition: &mut super::ClausePreCondition<'script>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a match expr
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_precondition(
        &mut self,
        _precondition: &mut super::ClausePreCondition<'script>,
    ) -> Result<()> {
        Ok(())
    }

    /// visit a `DefaultCase`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_default_case(
        &mut self,
        _mdefault: &mut DefaultCase<ImutExprInt<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `DefaultCase`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_default_case(
        &mut self,
        _mdefault: &mut DefaultCase<ImutExprInt<'script>>,
    ) -> Result<()> {
        Ok(())
    }

    /// visit a `Match`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_mmatch(&mut self, _mmatch: &mut Match<'script, ImutExprInt>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `Match`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_mmatch(&mut self, _mmatch: &mut Match<'script, ImutExprInt>) -> Result<()> {
        Ok(())
    }

    /// visit a `PredicateClause`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_predicate_clause(
        &mut self,
        _predicate: &mut PredicateClause<'script, ImutExprInt<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `PredicateClause`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_predicate_clause(
        &mut self,
        _predicate: &mut PredicateClause<'script, ImutExprInt<'script>>,
    ) -> Result<()> {
        Ok(())
    }

    /// visit a `ClauseGroup`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_clause_group(
        &mut self,
        _group: &mut ClauseGroup<'script, ImutExprInt<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `ClauseGroup`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_clause_group(
        &mut self,
        _group: &mut ClauseGroup<'script, ImutExprInt<'script>>,
    ) -> Result<()> {
        Ok(())
    }

    /// visit a `Pattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_match_pattern(&mut self, _pattern: &mut Pattern<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `Pattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_match_pattern(&mut self, _pattern: &mut Pattern<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `RecordPattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_record_pattern(
        &mut self,
        _record_pattern: &mut RecordPattern<'script>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `RecordPattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_record_pattern(&mut self, _record_pattern: &mut RecordPattern<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `PredicatePattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_predicate_pattern(
        &mut self,
        _record_pattern: &mut PredicatePattern<'script>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `PredicatePattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_predicate_pattern(
        &mut self,
        _record_pattern: &mut PredicatePattern<'script>,
    ) -> Result<()> {
        Ok(())
    }

    /// visit a `ArrayPattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_array_pattern(
        &mut self,
        _array_pattern: &mut ArrayPattern<'script>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `ArrayPattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_array_pattern(&mut self, _array_pattern: &mut ArrayPattern<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `TuplePattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_tuple_pattern(&mut self, _pattern: &mut TuplePattern<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `TuplePattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_tuple_pattern(&mut self, _pattern: &mut TuplePattern<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `ArrayPredicatePattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_array_predicate_pattern(
        &mut self,
        _pattern: &mut ArrayPredicatePattern<'script>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `ArrayPredicatePattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_array_predicate_pattern(
        &mut self,
        _pattern: &mut ArrayPredicatePattern<'script>,
    ) -> Result<()> {
        Ok(())
    }

    /// visit a `TestExpr`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_test_expr(&mut self, _expr: &mut TestExpr) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `TestExpr`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_test_expr(&mut self, _expr: &mut TestExpr) -> Result<()> {
        Ok(())
    }

    /// visit a comprehension
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_comprehension(
        &mut self,
        _comp: &mut Comprehension<'script, ImutExprInt<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a comprehension
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_comprehension(
        &mut self,
        _comp: &mut Comprehension<'script, ImutExprInt<'script>>,
    ) -> Result<()> {
        Ok(())
    }

    /// visit a merge expr
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_merge(&mut self, _merge: &mut Merge<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a merge expr
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_merge(&mut self, _merge: &mut Merge<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a merge expr
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_segment(&mut self, _segment: &mut Segment<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a merge expr
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_segment(&mut self, _segment: &mut Segment<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a path
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_path(&mut self, _path: &mut Path<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a path
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_path(&mut self, _path: &mut Path<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a cow
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_string_lit(&mut self, _cow: &mut beef::Cow<'script, str>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a cow
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_string_lit(&mut self, _cow: &mut beef::Cow<'script, str>) -> Result<()> {
        Ok(())
    }

    /// visit a string
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_string(&mut self, _string: &mut StringLit<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a string
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_string(&mut self, _string: &mut StringLit<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `StrLitElement`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_string_element(&mut self, _string: &mut StrLitElement<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `StrLitElement`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_string_element(&mut self, _string: &mut StrLitElement<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a local
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_local(&mut self, _local_idx: &mut usize) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a local
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_local(&mut self, _local_idx: &mut usize) -> Result<()> {
        Ok(())
    }

    /// visit a present expr
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_present(&mut self, _path: &mut Path<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a present expr
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_present(&mut self, _path: &mut Path<'script>) -> Result<()> {
        Ok(())
    }

    /// visit an invoke expr
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_invoke(&mut self, _invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave an invoke expr
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_invoke(&mut self, _invoke: &mut Invoke<'script>) -> Result<()> {
        Ok(())
    }

    /// visit an invoke1 expr
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_invoke1(&mut self, _invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave an invoke1 expr
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_invoke1(&mut self, _invoke: &mut Invoke<'script>) -> Result<()> {
        Ok(())
    }

    /// visit an invoke2 expr
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_invoke2(&mut self, _invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave an invoke2 expr
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_invoke2(&mut self, _invoke: &mut Invoke<'script>) -> Result<()> {
        Ok(())
    }

    /// visit an invoke3 expr
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_invoke3(&mut self, _invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave an invoke3 expr
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_invoke3(&mut self, _invoke: &mut Invoke<'script>) -> Result<()> {
        Ok(())
    }

    /// visit an `invoke_aggr` expr
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_invoke_aggr(&mut self, _invoke_aggr: &mut InvokeAggr) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave an `invoke_aggr` expr
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_invoke_aggr(&mut self, _invoke_aggr: &mut InvokeAggr) -> Result<()> {
        Ok(())
    }

    /// visit a recur expr
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_recur(&mut self, _recur: &mut Recur<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a recur expr
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_recur(&mut self, _recur: &mut Recur<'script>) -> Result<()> {
        Ok(())
    }

    /// visit bytes
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_bytes(&mut self, _bytes: &mut Bytes<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave bytes
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_bytes(&mut self, _bytes: &mut Bytes<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a literal
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_literal(&mut self, _literal: &mut Literal<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a literal
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_literal(&mut self, _literal: &mut Literal<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a generic `ImutExprInt` (this is called before the concrete `visit_*` method)
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_expr(&mut self, _e: &mut ImutExprInt<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a generic `ImutExprInt` (this is called after the concrete `leave_*` method)
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_expr(&mut self, _e: &mut ImutExprInt<'script>) -> Result<()> {
        Ok(())
    }

    /// visit segments
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_segments(&mut self, _e: &mut Vec<Segment<'script>>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave segments
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_segments(&mut self, _e: &mut Vec<Segment<'script>>) -> Result<()> {
        Ok(())
    }

    /// visit a `ExprPath`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_expr_path(&mut self, _e: &mut ExprPath<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `ExprPath`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_expr_path(&mut self, _e: &mut ExprPath<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a const path (uses the `LocalPath` ast node)
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_const_path(&mut self, _e: &mut LocalPath<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a const path (uses the `LocalPath` ast node)
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_const_path(&mut self, _e: &mut LocalPath<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `LocalPath`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_local_path(&mut self, _e: &mut LocalPath<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `LocalPath`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_local_path(&mut self, _e: &mut LocalPath<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `EventPath`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_event_path(&mut self, _e: &mut EventPath<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `EventPath`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_event_path(&mut self, _e: &mut EventPath<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `StatePath`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_state_path(&mut self, _e: &mut StatePath<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `StatePath`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_state_path(&mut self, _e: &mut StatePath<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `MetadataPath`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_meta_path(&mut self, _e: &mut MetadataPath<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `MetadataPath`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_meta_path(&mut self, _e: &mut MetadataPath<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `ReservedPath`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_reserved_path(&mut self, _e: &mut ReservedPath<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `ReservedPath`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_reserved_path(&mut self, _e: &mut ReservedPath<'script>) -> Result<()> {
        Ok(())
    }
}

/// Visitor for `GroupBy`
pub(crate) trait GroupByVisitor<'script> {
    fn visit_expr(&mut self, _expr: &ImutExprInt<'script>);

    /// Walks an expression
    fn walk_group_by(&mut self, group_by: &GroupByInt<'script>) {
        match group_by {
            GroupByInt::Expr { expr, .. } | GroupByInt::Each { expr, .. } => self.visit_expr(expr),
            GroupByInt::Set { items, .. } => {
                for inner_group_by in items {
                    self.walk_group_by(&inner_group_by.0);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::ast::walkers::ExprWalker;
    use crate::path::ModulePath;
    use crate::registry::registry;
    use tremor_value::prelude::*;

    #[derive(Default)]
    struct Find42Visitor {
        found: usize,
    }
    impl<'script> ImutExprWalker<'script> for Find42Visitor {}
    impl<'script> ExprWalker<'script> for Find42Visitor {}
    impl<'script> ExprVisitor<'script> for Find42Visitor {}
    impl<'script> ImutExprVisitor<'script> for Find42Visitor {
        fn visit_literal(&mut self, literal: &mut Literal<'script>) -> Result<VisitRes> {
            if let Some(42) = literal.value.as_u64() {
                self.found += 1;
                return Ok(VisitRes::Stop);
            }
            Ok(VisitRes::Walk)
        }
    }

    fn test_walk<'script>(input: &'script str, expected_42s: usize) -> Result<()> {
        let module_path = ModulePath::load();
        let mut registry = registry();
        crate::std_lib::load(&mut registry);
        let script_script: crate::script::Script =
            crate::script::Script::parse(&module_path, "test", input.to_owned(), &registry)?;
        let script: &crate::ast::Script = script_script.script.suffix();
        let mut visitor = Find42Visitor::default();
        for expr in &script.exprs {
            let mut expr = expr.clone();
            ExprWalker::walk_expr(&mut visitor, &mut expr)?;
        }
        assert_eq!(
            expected_42s, visitor.found,
            "Did not find {} 42s only {} in: {}",
            expected_42s, visitor.found, input
        );
        Ok(())
    }

    #[test]
    fn test_visitor_walking() -> Result<()> {
        test_walk(
            r#"
            const c = {"badger": 23};
            for c[event]["s"].badger[42] of
              case (a, b) => let event = 5
              case (a, b) when a == 7 => drop
              case (a, b) => emit event
            end;
            match event.foo of
              case %{ field == " #{42 + event.foo} ", present foo, absent bar, snot ~= re|badger| } => event.bar
              case present snot => event.snot
              case absent badger => 42
              case %{ a ==  1 } => event.snot
              case %{ a ==  2 } => event.snot
              case %{ a ==  3 } => event.snot
              case %{ a ==  4 } => event.snot
              case %{ a ==  5 } => event.snot
              case %{ a ==  6 } => event.snot
              case %{ a ==  7 } => event.snot
              case %{ a ==  8 } => event.snot
              case %{ a ==  9 } => event.snot
              case %{ a == 10 } => event.snot
              case %{ a == 11 } => event.snot
              case %{ a == 12 } => event.snot
              case %{ a == 13 } => event.snot
              case %{ a == 14 } => event.snot
              case %{ a == 15 } => event.snot
              case %{ a == 16 } => event.snot
              case %{ a == 17 } => event.snot
              case %{ a == 18 } => event.snot
              case %{ a == 19 } => event.snot
              case %[42] => event.snake
              case a = %(42, _, ...) => let event = a
              default => let a = 7, let b = 9, event + a + b
            end;
            match $meta.foo of          
              case 1 => let event = a
              case _ => null
            end;
            fn hide_the_42(x) with
              let x = x + 42;
              recur(x)
            end;
            drop;
            emit event => " #{42 + event} ";
            hide_the_42(
              match event.foo of
                case %{ field == " #{42 + event.foo} ", present foo, absent bar, badger ~= %("snot", ~ re|snot|, _) } => event.bar
                case 7 => event.snot
                case %[42, _, %{}] => event.snake
                case a = %(42, ...) => a
                case ~ re|snot| => b
                default => event.snot
              end
            );
        "#,
            8,
        )
    }

    #[test]
    fn test_walk_list() -> Result<()> {
        test_walk(
            r#"
        let x = event.bla + 1;
        fn add(x, y) with
          recur(x + y, 1)
        end;
        let zero = 0;
        [
            -event.foo,
            (patch event of
                default => {"snot": 42 - zero},
                default "key" => {"snot": 42 +  zero},
                insert "snot" => 42,
                merge => {"snot": 42 - zero},
                merge "badger" => {"snot": 42 - zero},
                upsert "snot" => 42,
                copy "snot" => "snotter",
                erase "snotter"
            end),
            (merge event of {"foo": event[42:x]} end),
            "~~~ #{ state[1] } ~~~",
            x,
            x[x],
            add(event.foo, 42),
            <<event.foo:8/unsigned>>
        ]
        "#,
            8,
        )
    }

    #[test]
    fn test_walk_comprehension() -> Result<()> {
        test_walk(
            r#"
            for group[0] of
                case (i, e) when i == 0 =>
                  let event = 42 + i,
                  event + 7
              end;
            (for group[0] of
               case (i, e) =>
                 42 + i
            end)
        "#,
            2,
        )
    }
}
