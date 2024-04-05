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

use crate::ast::{ArrayAppend, BooleanBinExpr, Const};

use super::prelude::*;
use super::VisitRes::Walk;

/// Visitor for traversing all `ImutExprInt`s within the given `ImutExprInt`
///
/// Implement your custom expr visiting logic by overwriting the methods.
// #[cfg_attr(coverage, no_coverage)]
pub trait Visitor<'script> {
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
    /// visit a binary
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_binary_boolean(&mut self, _binary: &mut BooleanBinExpr<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a binary
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_binary(&mut self, _binary: &mut BinExpr<'script>) -> Result<()> {
        Ok(())
    }

    /// leave a binary
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_binary_boolean(&mut self, _binary: &mut BooleanBinExpr<'script>) -> Result<()> {
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
        _precondition: &mut ClausePreCondition<'script>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a match expr
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_precondition(
        &mut self,
        _precondition: &mut ClausePreCondition<'script>,
    ) -> Result<()> {
        Ok(())
    }

    /// visit a `DefaultCase`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_default_case(
        &mut self,
        _mdefault: &mut DefaultCase<ImutExpr<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `DefaultCase`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_default_case(&mut self, _mdefault: &mut DefaultCase<ImutExpr<'script>>) -> Result<()> {
        Ok(())
    }

    /// visit a `Match`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_mmatch(&mut self, _mmatch: &mut Match<'script, ImutExpr>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `Match`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_mmatch(&mut self, _mmatch: &mut Match<'script, ImutExpr>) -> Result<()> {
        Ok(())
    }

    /// visit a `PredicateClause`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_predicate_clause(
        &mut self,
        _predicate: &mut PredicateClause<'script, ImutExpr<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `PredicateClause`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_predicate_clause(
        &mut self,
        _predicate: &mut PredicateClause<'script, ImutExpr<'script>>,
    ) -> Result<()> {
        Ok(())
    }

    /// visit a `ClauseGroup`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_clause_group(
        &mut self,
        _group: &mut ClauseGroup<'script, ImutExpr<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `ClauseGroup`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_clause_group(
        &mut self,
        _group: &mut ClauseGroup<'script, ImutExpr<'script>>,
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
        _comp: &mut Comprehension<'script, ImutExpr<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a comprehension
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_comprehension(
        &mut self,
        _comp: &mut Comprehension<'script, ImutExpr<'script>>,
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

    /// visit a `Ident`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_ident(&mut self, _cow: &mut Ident<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `Ident`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_ident(&mut self, _cow: &mut Ident<'script>) -> Result<()> {
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
    fn visit_expr(&mut self, _e: &mut ImutExpr<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a generic `ImutExprInt` (this is called after the concrete `leave_*` method)
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_expr(&mut self, _e: &mut ImutExpr<'script>) -> Result<()> {
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

    /// visit a generic `Const`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_const(&mut self, _e: &mut Const<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// Visit an `ArrayAppend`
    ///
    /// # Errors
    /// If the walker function fails
    fn visit_array_append(&mut self, _a: &mut ArrayAppend<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// Leave an `ArrayAppend`
    ///
    /// # Errors
    /// If the walker function fails
    fn leave_array_append(&mut self, _a: &mut ArrayAppend<'script>) -> Result<()> {
        Ok(())
    }

    /// leave a generic `Const`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_const(&mut self, _e: &mut Const<'script>) -> Result<()> {
        Ok(())
    }
}
