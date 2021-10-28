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

use super::{
    ArrayPattern, ArrayPredicatePattern, BinExpr, Bytes, ClauseGroup, Comprehension, DefaultCase,
    EmitExpr, EventPath, Expr, ExprPath, IfElse, ImutExprInt, Invoke, InvokeAggr, List, Literal,
    LocalPath, Match, Merge, MetadataPath, Patch, PatchOperation, Path, Pattern, PredicateClause,
    PredicatePattern, Record, RecordPattern, Recur, ReservedPath, Segment, StatePath,
    StrLitElement, StringLit, TestExpr, TuplePattern, UnaryExpr,
};

use crate::errors::Result;

use super::visitors::{ExprVisitor, ImutExprVisitor, VisitRes};

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
pub trait ExprWalker<'script>:
    ImutExprVisitor<'script> + ExprVisitor<'script> + ImutExprWalker<'script>
{
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
                ExprWalker::walk_match(self, mmatch.as_mut())?;
            }
            Expr::IfElse(ifelse) => {
                self.walk_ifelse(ifelse.as_mut())?;
            }
            Expr::Assign { path, expr, .. } => {
                self.walk_path(path)?;
                ExprWalker::walk_expr(self, expr.as_mut())?;
            }
            Expr::AssignMoveLocal { path, .. } => {
                self.walk_path(path)?;
            }
            Expr::Comprehension(c) => {
                ExprWalker::walk_comprehension(self, c.as_mut())?;
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
                ExprWalker::walk_expr(self, expr)?;
            }
            ExprWalker::walk_expr(self, &mut comp_case.last_expr)?;
        }
        ImutExprWalker::walk_expr(self, &mut comp.target)?;
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
        ExprWalker::walk_predicate_clause(self, &mut ifelse.if_clause)?;
        ExprWalker::walk_default_case(self, &mut ifelse.else_clause)?;
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
                    ExprWalker::walk_expr(self, e)?;
                }
                ExprWalker::walk_expr(self, last_expr)?;
            }
            DefaultCase::One(last_expr) => {
                ExprWalker::walk_expr(self, last_expr)?;
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
            ExprWalker::walk_clause_group(self, group)?;
        }
        ExprWalker::walk_default_case(self, &mut mmatch.default)?;
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
                ExprWalker::walk_predicate_clause(self, pattern)?;
            }
            ClauseGroup::Simple {
                precondition,
                patterns,
            } => {
                if let Some(precondition) = precondition {
                    self.walk_precondition(precondition)?;
                }
                for predicate in patterns {
                    ExprWalker::walk_predicate_clause(self, predicate)?;
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
                for (_v, (es, e)) in tree.iter_mut() {
                    for e in es {
                        ExprWalker::walk_expr(self, e)?;
                    }
                    ExprWalker::walk_expr(self, e)?;
                }
                for predicate in rest {
                    ExprWalker::walk_predicate_clause(self, predicate)?;
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
                    ExprWalker::walk_clause_group(self, g)?;
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
            ExprWalker::walk_expr(self, expr)?;
        }
        ExprWalker::walk_expr(self, &mut predicate.last_expr)?;
        ExprVisitor::leave_predicate_clause(self, predicate)
    }
}

/// Visitor for traversing all `ImutExprInt`s within the given `ImutExprInt`
///
/// Implement your custom expr visiting logic by overwriting the visit_* methods.
/// You do not need to traverse further down. This is done by the provided `walk_*` methods.
/// The walk_* methods implement walking the expression tree, those do not need to be changed.
pub trait ImutExprWalker<'script>: ImutExprVisitor<'script> {
    /// walk a record
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_record(&mut self, record: &mut Record<'script>) -> Result<()> {
        stop!(self.visit_record(record), self.leave_record(record));
        for field in &mut record.fields {
            self.walk_string(&mut field.name)?;
            self.walk_expr(&mut field.value)?;
        }
        self.leave_record(record)
    }

    /// walk a list
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_list(&mut self, list: &mut List<'script>) -> Result<()> {
        stop!(self.visit_list(list), self.leave_list(list));
        for element in &mut list.exprs {
            self.walk_expr(&mut element.0)?;
        }
        self.leave_list(list)
    }

    /// walk a binary
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_binary(&mut self, binary: &mut BinExpr<'script>) -> Result<()> {
        stop!(self.visit_binary(binary), self.leave_binary(binary));
        self.walk_expr(&mut binary.lhs)?;
        self.walk_expr(&mut binary.rhs)?;
        self.leave_binary(binary)
    }

    /// walk a unary
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_unary(&mut self, unary: &mut UnaryExpr<'script>) -> Result<()> {
        stop!(self.visit_unary(unary), self.leave_unary(unary));
        self.walk_expr(&mut unary.expr)?;
        self.leave_unary(unary)
    }

    /// walk a `Patch`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_patch(&mut self, patch: &mut Patch<'script>) -> Result<()> {
        stop!(self.visit_patch(patch), self.leave_patch(patch));
        for op in &mut patch.operations {
            self.walk_patch_operation(op)?;
        }
        self.walk_expr(&mut patch.target)?;
        self.leave_patch(patch)
    }

    /// walk a `Patch`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_patch_operation(&mut self, op: &mut PatchOperation<'script>) -> Result<()> {
        stop!(
            self.visit_patch_operation(op),
            self.leave_patch_operation(op)
        );
        match op {
            PatchOperation::Insert { ident, expr }
            | PatchOperation::Default { ident, expr }
            | PatchOperation::Merge { ident, expr }
            | PatchOperation::Update { ident, expr }
            | PatchOperation::Upsert { ident, expr } => {
                self.walk_string(ident)?;
                self.walk_expr(expr)?;
            }
            PatchOperation::Copy { from, to } | PatchOperation::Move { from, to } => {
                self.walk_string(from)?;
                self.walk_string(to)?;
            }
            PatchOperation::Erase { ident } => {
                self.walk_string(ident)?;
            }
            PatchOperation::DefaultRecord { expr } | PatchOperation::MergeRecord { expr } => {
                self.walk_expr(expr)?;
            }
        }
        self.leave_patch_operation(op)
    }

    /// Walks a `ClausePreCondition`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_precondition(
        &mut self,
        precondition: &mut super::ClausePreCondition<'script>,
    ) -> Result<()> {
        stop!(
            self.visit_precondition(precondition),
            self.leave_precondition(precondition)
        );
        for segment in precondition.path.segments_mut() {
            self.walk_segment(segment)?;
        }
        self.leave_precondition(precondition)
    }

    /// walk a `DefaultCase`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_default_case(
        &mut self,
        mdefault: &mut DefaultCase<ImutExprInt<'script>>,
    ) -> Result<()> {
        stop!(
            self.visit_default_case(mdefault),
            self.leave_default_case(mdefault)
        );
        match mdefault {
            DefaultCase::None | DefaultCase::Null => {}
            DefaultCase::Many { exprs, last_expr } => {
                for e in exprs {
                    self.walk_expr(e)?;
                }
                self.walk_expr(last_expr)?;
            }
            DefaultCase::One(last_expr) => {
                self.walk_expr(last_expr)?;
            }
        }
        self.leave_default_case(mdefault)
    }

    /// walk a `Match`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_match(&mut self, mmatch: &mut Match<'script, ImutExprInt<'script>>) -> Result<()> {
        stop!(self.visit_mmatch(mmatch), self.leave_mmatch(mmatch));
        self.walk_expr(&mut mmatch.target)?;
        for group in &mut mmatch.patterns {
            self.walk_clause_group(group)?;
        }
        self.walk_default_case(&mut mmatch.default)?;
        self.leave_mmatch(mmatch)
    }

    /// Walks a `PredicateClause`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_predicate_clause(
        &mut self,
        predicate: &mut PredicateClause<'script, ImutExprInt<'script>>,
    ) -> Result<()> {
        stop!(
            self.visit_predicate_clause(predicate),
            self.leave_predicate_clause(predicate)
        );
        self.walk_match_pattern(&mut predicate.pattern)?;
        if let Some(guard) = &mut predicate.guard {
            self.walk_expr(guard)?;
        }
        for expr in &mut predicate.exprs {
            self.walk_expr(expr)?;
        }
        self.walk_expr(&mut predicate.last_expr)?;
        self.leave_predicate_clause(predicate)
    }

    /// Walks a `ClauseGroup`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_clause_group(
        &mut self,
        group: &mut ClauseGroup<'script, ImutExprInt<'script>>,
    ) -> Result<()> {
        stop!(
            self.visit_clause_group(group),
            self.leave_clause_group(group)
        );
        match group {
            ClauseGroup::Single {
                precondition,
                pattern,
            } => {
                if let Some(precondition) = precondition {
                    self.walk_precondition(precondition)?;
                }
                self.walk_predicate_clause(pattern)?;
            }
            ClauseGroup::Simple {
                precondition,
                patterns,
            } => {
                if let Some(precondition) = precondition {
                    self.walk_precondition(precondition)?;
                }
                for predicate in patterns {
                    self.walk_predicate_clause(predicate)?;
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
                for (_v, (es, e)) in tree.iter_mut() {
                    for e in es {
                        self.walk_expr(e)?;
                    }
                    self.walk_expr(e)?;
                }
                for predicate in rest {
                    self.walk_predicate_clause(predicate)?;
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
                    self.walk_clause_group(g)?;
                }
            }
        }
        self.leave_clause_group(group)
    }

    /// walk match `Pattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_match_pattern(&mut self, pattern: &mut Pattern<'script>) -> Result<()> {
        stop!(
            self.visit_match_pattern(pattern),
            self.leave_match_pattern(pattern)
        );
        match pattern {
            Pattern::Record(record_pat) => {
                self.walk_record_pattern(record_pat)?;
            }
            Pattern::Array(array_pat) => {
                self.walk_array_pattern(array_pat)?;
            }
            Pattern::Expr(expr) => {
                self.walk_expr(expr)?;
            }
            Pattern::Assign(ap) => {
                self.walk_match_pattern(ap.pattern.as_mut())?;
            }
            Pattern::Tuple(tuple_pattern) => {
                self.walk_tuple_pattern(tuple_pattern)?;
            }
            Pattern::Extract(e) => {
                self.walk_test_expr(e.as_mut())?;
            }
            Pattern::DoNotCare | Pattern::Default => {}
        }
        self.leave_match_pattern(pattern)
    }
    /// walk `TuplePattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_tuple_pattern(&mut self, pattern: &mut TuplePattern<'script>) -> Result<()> {
        stop!(
            self.visit_tuple_pattern(pattern),
            self.leave_tuple_pattern(pattern)
        );
        for elem in &mut pattern.exprs {
            self.walk_array_predicate_pattern(elem)?;
        }

        self.leave_tuple_pattern(pattern)
    }

    /// walk `ArrayPredicatePattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_array_predicate_pattern(
        &mut self,
        pattern: &mut ArrayPredicatePattern<'script>,
    ) -> Result<()> {
        stop!(
            self.visit_array_predicate_pattern(pattern),
            self.leave_array_predicate_pattern(pattern)
        );
        match pattern {
            ArrayPredicatePattern::Expr(expr) => {
                self.walk_expr(expr)?;
            }
            ArrayPredicatePattern::Record(rp) => {
                self.walk_record_pattern(rp)?;
            }
            ArrayPredicatePattern::Tilde(e) => {
                self.walk_test_expr(e.as_mut())?;
            }
            ArrayPredicatePattern::Ignore => {}
        }
        self.leave_array_predicate_pattern(pattern)
    }

    /// walk `TestExpr`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_test_expr(&mut self, e: &mut TestExpr) -> Result<()> {
        self.visit_test_expr(e)?;
        self.leave_test_expr(e)
    }

    /// walk a `RecordPattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_record_pattern(&mut self, record_pattern: &mut RecordPattern<'script>) -> Result<()> {
        stop!(
            self.visit_record_pattern(record_pattern),
            self.leave_record_pattern(record_pattern)
        );
        for pattern in &mut record_pattern.fields {
            self.walk_predicate_pattern(pattern)?;
        }
        self.leave_record_pattern(record_pattern)
    }

    /// walk a `PredicatePattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_predicate_pattern(&mut self, pattern: &mut PredicatePattern<'script>) -> Result<()> {
        stop!(
            self.visit_predicate_pattern(pattern),
            self.leave_predicate_pattern(pattern)
        );
        match pattern {
            PredicatePattern::RecordPatternEq { pattern, .. } => {
                self.walk_record_pattern(pattern)?;
            }
            PredicatePattern::Bin { rhs, .. } => {
                self.walk_expr(rhs)?;
            }
            PredicatePattern::ArrayPatternEq { pattern, .. } => {
                self.walk_array_pattern(pattern)?;
            }
            PredicatePattern::TuplePatternEq { pattern, .. } => {
                self.walk_tuple_pattern(pattern)?;
            }
            PredicatePattern::TildeEq { test, .. } => {
                self.walk_test_expr(test.as_mut())?;
            }
            PredicatePattern::FieldPresent { .. } | PredicatePattern::FieldAbsent { .. } => {}
        }
        self.leave_predicate_pattern(pattern)
    }

    /// walk an `ArrayPattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_array_pattern(&mut self, array_pattern: &mut ArrayPattern<'script>) -> Result<()> {
        stop!(
            self.visit_array_pattern(array_pattern),
            self.leave_array_pattern(array_pattern)
        );
        for elem in &mut array_pattern.exprs {
            self.walk_array_predicate_pattern(elem)?;
        }

        self.leave_array_pattern(array_pattern)
    }

    /// walk a `Comprehension`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_comprehension(
        &mut self,
        comp: &mut Comprehension<'script, ImutExprInt<'script>>,
    ) -> Result<()> {
        stop!(
            self.visit_comprehension(comp),
            self.leave_comprehension(comp)
        );
        for comp_case in &mut comp.cases {
            if let Some(guard) = &mut comp_case.guard {
                self.walk_expr(guard)?;
            }
            for expr in &mut comp_case.exprs {
                self.walk_expr(expr)?;
            }
            self.walk_expr(&mut comp_case.last_expr)?;
        }
        self.walk_expr(&mut comp.target)?;
        self.leave_comprehension(comp)
    }

    /// walk a `Merge`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_merge(&mut self, merge: &mut Merge<'script>) -> Result<()> {
        stop!(self.visit_merge(merge), self.leave_merge(merge));
        self.walk_expr(&mut merge.target)?;
        self.walk_expr(&mut merge.expr)?;
        self.leave_merge(merge)
    }

    /// walk a path `Segment`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_segment(&mut self, segment: &mut Segment<'script>) -> Result<()> {
        stop!(self.visit_segment(segment), self.leave_segment(segment));
        match segment {
            Segment::Element { expr, .. } => self.walk_expr(expr)?,
            Segment::Range { start, end, .. } => {
                self.walk_expr(start.as_mut())?;
                self.walk_expr(end.as_mut())?;
            }
            Segment::Id { .. } | Segment::Idx { .. } => (),
        }
        self.leave_segment(segment)
    }

    /// walk a `Path`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_path(&mut self, path: &mut Path<'script>) -> Result<()> {
        stop!(self.visit_path(path), self.leave_path(path));
        match path {
            Path::Expr(p) => {
                self.walk_expr_path(p)?;
            }
            Path::Const(p) => {
                self.walk_const_path(p)?;
            }
            Path::Local(p) => {
                self.walk_local_path(p)?;
            }
            Path::Event(p) => {
                self.walk_event_path(p)?;
            }
            Path::State(p) => {
                self.walk_state_path(p)?;
            }
            Path::Meta(p) => {
                self.walk_meta_path(p)?;
            }
            Path::Reserved(p) => {
                self.walk_reserved_path(p)?;
            }
        }
        self.leave_path(path)
    }

    /// walk a `ExprPath`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_expr_path(&mut self, path: &mut ExprPath<'script>) -> Result<()> {
        stop!(self.visit_expr_path(path), self.leave_expr_path(path));
        self.visit_expr(&mut path.expr)?;
        self.walk_segments(&mut path.segments)?;
        self.leave_expr_path(path)
    }

    /// walk a `ConstPath` (represented as `LocalPath` in the AST)
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_const_path(&mut self, path: &mut LocalPath<'script>) -> Result<()> {
        stop!(self.visit_const_path(path), self.leave_const_path(path));
        self.walk_segments(&mut path.segments)?;
        self.leave_const_path(path)
    }

    /// walk a local path
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_local_path(&mut self, path: &mut LocalPath<'script>) -> Result<()> {
        stop!(self.visit_local_path(path), self.leave_local_path(path));
        self.walk_segments(&mut path.segments)?;
        self.leave_local_path(path)
    }

    /// walk a event path
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_event_path(&mut self, path: &mut EventPath<'script>) -> Result<()> {
        stop!(self.visit_event_path(path), self.leave_event_path(path));
        self.walk_segments(&mut path.segments)?;
        self.leave_event_path(path)
    }

    /// walk a event path
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_state_path(&mut self, path: &mut StatePath<'script>) -> Result<()> {
        stop!(self.visit_state_path(path), self.leave_state_path(path));
        self.walk_segments(&mut path.segments)?;
        self.leave_state_path(path)
    }

    /// walk a metadata path
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_meta_path(&mut self, path: &mut MetadataPath<'script>) -> Result<()> {
        stop!(self.visit_meta_path(path), self.leave_meta_path(path));
        self.walk_segments(&mut path.segments)?;
        self.leave_meta_path(path)
    }

    /// walk a reserved path
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_reserved_path(&mut self, path: &mut ReservedPath<'script>) -> Result<()> {
        stop!(
            self.visit_reserved_path(path),
            self.leave_reserved_path(path)
        );
        self.walk_segments(path.segments_mut())?;
        self.leave_reserved_path(path)
    }

    /// walks a segments
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_segments(&mut self, segments: &mut Vec<Segment<'script>>) -> Result<()> {
        stop!(self.visit_segments(segments), self.leave_segments(segments));
        for segment in segments.iter_mut() {
            self.walk_segment(segment)?;
        }
        self.leave_segments(segments)
    }

    /// walks a cow
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_string_lit(&mut self, cow: &mut beef::Cow<'script, str>) -> Result<()> {
        self.visit_string_lit(cow)?;
        self.leave_string_lit(cow)
    }

    /// walk a string
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_string(&mut self, string: &mut StringLit<'script>) -> Result<()> {
        stop!(self.visit_string(string), self.leave_string(string));
        for element in &mut string.elements {
            self.walk_string_element(element)?;
        }

        self.leave_string(string)
    }

    /// walk a string element
    ///
    /// # Errors
    /// if the walker function fails

    fn walk_string_element(&mut self, element: &mut StrLitElement<'script>) -> Result<()> {
        stop!(
            self.visit_string_element(element),
            self.leave_string_element(element)
        );
        match element {
            StrLitElement::Lit(l) => {
                self.walk_string_lit(l)?;
            }
            StrLitElement::Expr(expr) => {
                self.walk_expr(expr)?;
            }
        }
        self.leave_string_element(element)
    }

    /// walks a local
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_local(&mut self, local_idx: &mut usize) -> Result<()> {
        self.visit_local(local_idx)?;
        self.leave_local(local_idx)
    }

    /// walk an invoke expr
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_invoke(&mut self, invoke: &mut Invoke<'script>) -> Result<()> {
        stop!(self.visit_invoke(invoke), self.leave_invoke(invoke));
        for arg in &mut invoke.args {
            self.walk_expr(&mut arg.0)?;
        }
        self.leave_invoke(invoke)
    }

    /// walk an invoke1 expr
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_invoke1(&mut self, invoke: &mut Invoke<'script>) -> Result<()> {
        stop!(self.visit_invoke1(invoke), self.leave_invoke1(invoke));
        for arg in &mut invoke.args {
            self.walk_expr(&mut arg.0)?;
        }
        self.leave_invoke1(invoke)
    }

    /// walk an invoke expr
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_invoke2(&mut self, invoke: &mut Invoke<'script>) -> Result<()> {
        stop!(self.visit_invoke2(invoke), self.leave_invoke2(invoke));
        for arg in &mut invoke.args {
            self.walk_expr(&mut arg.0)?;
        }
        self.leave_invoke2(invoke)
    }

    /// walk an invoke expr
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_invoke3(&mut self, invoke: &mut Invoke<'script>) -> Result<()> {
        stop!(self.visit_invoke3(invoke), self.leave_invoke3(invoke));
        for arg in &mut invoke.args {
            self.walk_expr(&mut arg.0)?;
        }
        self.leave_invoke3(invoke)
    }

    /// walk an invoke expr
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_invoke_aggr(&mut self, invoke_aggr: &mut InvokeAggr) -> Result<()> {
        self.visit_invoke_aggr(invoke_aggr)?;
        self.leave_invoke_aggr(invoke_aggr)
    }

    /// walk a recur expr
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_recur(&mut self, recur: &mut Recur<'script>) -> Result<()> {
        stop!(self.visit_recur(recur), self.leave_recur(recur));
        for expr in &mut recur.exprs {
            self.walk_expr(&mut expr.0)?;
        }
        self.leave_recur(recur)
    }

    /// walk bytes
    /// # Errors
    /// if the walker function fails
    fn walk_bytes(&mut self, bytes: &mut Bytes<'script>) -> Result<()> {
        stop!(self.visit_bytes(bytes), self.leave_bytes(bytes));
        for part in &mut bytes.value {
            self.walk_expr(&mut part.data.0)?;
        }
        self.leave_bytes(bytes)
    }

    /// walks a literal
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_literal(&mut self, literal: &mut Literal<'script>) -> Result<()> {
        self.visit_literal(literal)?;
        self.leave_literal(literal)
    }

    /// entry point into this visitor - call this to start visiting the given expression `e`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_expr(&mut self, e: &mut ImutExprInt<'script>) -> Result<()> {
        stop!(self.visit_expr(e), self.leave_expr(e));
        match e {
            ImutExprInt::Record(record) => {
                self.walk_record(record)?;
            }
            ImutExprInt::List(list) => {
                self.walk_list(list)?;
            }
            ImutExprInt::Binary(binary) => {
                self.walk_binary(binary.as_mut())?;
            }
            ImutExprInt::Unary(unary) => {
                self.walk_unary(unary.as_mut())?;
            }
            ImutExprInt::Patch(patch) => {
                self.walk_patch(patch.as_mut())?;
            }
            ImutExprInt::Match(mmatch) => {
                self.walk_match(mmatch.as_mut())?;
            }
            ImutExprInt::Comprehension(comp) => {
                self.walk_comprehension(comp.as_mut())?;
            }
            ImutExprInt::Merge(merge) => {
                self.walk_merge(merge.as_mut())?;
            }
            ImutExprInt::String(string) => {
                self.walk_string(string)?;
            }
            ImutExprInt::Local { idx, .. } => {
                self.walk_local(idx)?;
            }
            ImutExprInt::Path(path) | ImutExprInt::Present { path, .. } => {
                self.walk_path(path)?;
            }
            ImutExprInt::Invoke(invoke) => {
                self.walk_invoke(invoke)?;
            }
            ImutExprInt::Invoke1(invoke1) => {
                self.walk_invoke1(invoke1)?;
            }
            ImutExprInt::Invoke2(invoke2) => {
                self.walk_invoke2(invoke2)?;
            }
            ImutExprInt::Invoke3(invoke3) => {
                self.walk_invoke3(invoke3)?;
            }
            ImutExprInt::InvokeAggr(invoke_aggr) => {
                self.walk_invoke_aggr(invoke_aggr)?;
            }
            ImutExprInt::Recur(recur) => {
                self.walk_recur(recur)?;
            }
            ImutExprInt::Bytes(bytes) => {
                self.walk_bytes(bytes)?;
            }
            ImutExprInt::Literal(lit) => {
                self.walk_literal(lit)?;
            }
        }
        self.leave_expr(e)
    }
}
