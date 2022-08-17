use crate::ast::helper::raw::WindowName;
use crate::ast::visitors::{
    ArrayAdditionOptimizer, ConstFolder, ExprVisitor, ImutExprVisitor, QueryVisitor, VisitRes,
};
use crate::ast::walkers::expr::Walker as ExprWalker;
use crate::ast::walkers::imut_expr::Walker as IMutExprWalker;
use crate::ast::walkers::query::Walker as QueryWalker;
use crate::ast::{
    ArgsExpr, ArrayAppend, ArrayPattern, ArrayPredicatePattern, BinExpr, BooleanBinExpr, Bytes,
    ClauseGroup, ClausePreCondition, Comprehension, Const, CreationalWith, DefaultCase,
    DefinitionalArgs, DefinitionalArgsWith, EmitExpr, EventPath, Expr, ExprPath, FnDefn, GroupBy,
    Helper, Ident, IfElse, ImutExpr, Invoke, InvokeAggr, List, Literal, LocalPath, Match, Merge,
    MetadataPath, OperatorCreate, OperatorDefinition, Patch, PatchOperation, Path, Pattern,
    PipelineCreate, PipelineDefinition, PredicateClause, PredicatePattern, Query, Record,
    RecordPattern, Recur, ReservedPath, Script, ScriptCreate, ScriptDefinition, Segment, Select,
    SelectStmt, StatePath, Stmt, StrLitElement, StreamStmt, StringLit, TestExpr, TuplePattern,
    UnaryExpr, WindowDefinition, WithExpr,
};
use crate::errors::Result;
use crate::module::Content;
use beef::Cow;

struct CombinedVisitor<First, Second> {
    first: First,
    second: Second,
}

impl<'script, First, Second> ExprWalker<'script> for CombinedVisitor<First, Second>
where
    First: ExprVisitor<'script> + ImutExprVisitor<'script>,
    Second: ExprVisitor<'script> + ImutExprVisitor<'script>,
{
}
impl<'script, First, Second> ExprVisitor<'script> for CombinedVisitor<First, Second>
where
    First: ExprVisitor<'script> + ImutExprVisitor<'script>,
    Second: ExprVisitor<'script> + ImutExprVisitor<'script>,
{
    fn visit_expr(&mut self, e: &mut Expr<'script>) -> Result<VisitRes> {
        ExprVisitor::visit_expr(&mut self.first, e)?;
        ExprVisitor::visit_expr(&mut self.second, e)?;

        Ok(VisitRes::Walk)
    }

    fn leave_expr(&mut self, e: &mut Expr<'script>) -> Result<()> {
        ExprVisitor::leave_expr(&mut self.first, e)?;
        ExprVisitor::leave_expr(&mut self.second, e)?;

        Ok(())
    }

    fn visit_fn_defn(&mut self, e: &mut FnDefn<'script>) -> Result<VisitRes> {
        self.first.visit_fn_defn(e)?;
        self.second.visit_fn_defn(e)?;

        Ok(VisitRes::Walk)
    }

    fn leave_fn_defn(&mut self, e: &mut FnDefn<'script>) -> Result<()> {
        self.first.leave_fn_defn(e)?;
        self.second.visit_fn_defn(e)?;

        Ok(())
    }

    fn visit_comprehension(
        &mut self,
        comp: &mut Comprehension<'script, Expr<'script>>,
    ) -> Result<VisitRes> {
        ExprVisitor::visit_comprehension(&mut self.first, comp)?;
        ExprVisitor::visit_comprehension(&mut self.second, comp)?;

        Ok(VisitRes::Walk)
    }

    fn leave_comprehension(
        &mut self,
        comp: &mut Comprehension<'script, Expr<'script>>,
    ) -> Result<()> {
        ExprVisitor::leave_comprehension(&mut self.first, comp)?;
        ExprVisitor::leave_comprehension(&mut self.second, comp)?;

        Ok(())
    }

    fn visit_emit(&mut self, emit: &mut EmitExpr<'script>) -> Result<VisitRes> {
        self.first.visit_emit(emit)?;
        self.second.visit_emit(emit)?;

        Ok(VisitRes::Walk)
    }

    fn leave_emit(&mut self, emit: &mut EmitExpr<'script>) -> Result<()> {
        self.first.leave_emit(emit)?;
        self.second.leave_emit(emit)?;

        Ok(())
    }

    fn visit_ifelse(&mut self, mifelse: &mut IfElse<'script, Expr<'script>>) -> Result<VisitRes> {
        self.first.visit_ifelse(mifelse)?;
        self.second.visit_ifelse(mifelse)?;

        Ok(VisitRes::Walk)
    }

    fn leave_ifelse(&mut self, mifelse: &mut IfElse<'script, Expr<'script>>) -> Result<()> {
        self.first.leave_ifelse(mifelse)?;
        self.second.leave_ifelse(mifelse)?;

        Ok(())
    }

    fn visit_default_case(
        &mut self,
        mdefault: &mut DefaultCase<Expr<'script>>,
    ) -> Result<VisitRes> {
        ExprVisitor::visit_default_case(&mut self.first, mdefault)?;
        ExprVisitor::visit_default_case(&mut self.second, mdefault)?;

        Ok(VisitRes::Walk)
    }

    fn leave_default_case(&mut self, mdefault: &mut DefaultCase<Expr<'script>>) -> Result<()> {
        ExprVisitor::leave_default_case(&mut self.first, mdefault)?;
        ExprVisitor::leave_default_case(&mut self.second, mdefault)?;

        Ok(())
    }

    fn visit_mmatch(&mut self, mmatch: &mut Match<'script, Expr<'script>>) -> Result<VisitRes> {
        ExprVisitor::visit_mmatch(&mut self.first, mmatch)?;
        ExprVisitor::visit_mmatch(&mut self.second, mmatch)?;

        Ok(VisitRes::Walk)
    }

    fn leave_mmatch(&mut self, mmatch: &mut Match<'script, Expr<'script>>) -> Result<()> {
        ExprVisitor::leave_mmatch(&mut self.first, mmatch)?;
        ExprVisitor::leave_mmatch(&mut self.second, mmatch)?;

        Ok(())
    }

    fn visit_clause_group(
        &mut self,
        group: &mut ClauseGroup<'script, Expr<'script>>,
    ) -> Result<VisitRes> {
        ExprVisitor::visit_clause_group(&mut self.first, group)?;
        ExprVisitor::visit_clause_group(&mut self.second, group)?;

        Ok(VisitRes::Walk)
    }

    fn leave_clause_group(
        &mut self,
        group: &mut ClauseGroup<'script, Expr<'script>>,
    ) -> Result<()> {
        ExprVisitor::leave_clause_group(&mut self.first, group)?;
        ExprVisitor::leave_clause_group(&mut self.second, group)?;

        Ok(())
    }

    fn visit_predicate_clause(
        &mut self,
        predicate: &mut PredicateClause<'script, Expr<'script>>,
    ) -> Result<VisitRes> {
        ExprVisitor::visit_predicate_clause(&mut self.first, predicate)?;
        ExprVisitor::visit_predicate_clause(&mut self.second, predicate)?;

        Ok(VisitRes::Walk)
    }

    fn leave_predicate_clause(
        &mut self,
        predicate: &mut PredicateClause<'script, Expr<'script>>,
    ) -> Result<()> {
        ExprVisitor::leave_predicate_clause(&mut self.first, predicate)?;
        ExprVisitor::leave_predicate_clause(&mut self.second, predicate)?;

        Ok(())
    }
}
impl<'script, First, Second> IMutExprWalker<'script> for CombinedVisitor<First, Second>
where
    First: ImutExprVisitor<'script>,
    Second: ImutExprVisitor<'script>,
{
}
impl<'script, First, Second> ImutExprVisitor<'script> for CombinedVisitor<First, Second>
where
    First: ImutExprVisitor<'script>,
    Second: ImutExprVisitor<'script>,
{
    fn visit_record(&mut self, record: &mut Record<'script>) -> Result<VisitRes> {
        self.first.visit_record(record)?;
        self.second.visit_record(record)?;

        Ok(VisitRes::Walk)
    }

    fn leave_record(&mut self, record: &mut Record<'script>) -> Result<()> {
        self.first.leave_record(record)?;
        self.second.leave_record(record)?;

        Ok(())
    }

    fn visit_list(&mut self, list: &mut List<'script>) -> Result<VisitRes> {
        self.first.visit_list(list)?;
        self.second.visit_list(list)?;

        Ok(VisitRes::Walk)
    }

    fn leave_list(&mut self, list: &mut List<'script>) -> Result<()> {
        self.first.leave_list(list)?;
        self.second.leave_list(list)?;

        Ok(())
    }

    fn visit_binary(&mut self, binary: &mut BinExpr<'script>) -> Result<VisitRes> {
        self.first.visit_binary(binary)?;
        self.second.visit_binary(binary)?;

        Ok(VisitRes::Walk)
    }

    fn visit_binary_boolean(&mut self, binary: &mut BooleanBinExpr<'script>) -> Result<VisitRes> {
        self.first.visit_binary_boolean(binary)?;
        self.second.visit_binary_boolean(binary)?;

        Ok(VisitRes::Walk)
    }

    fn leave_binary(&mut self, binary: &mut BinExpr<'script>) -> Result<()> {
        self.first.leave_binary(binary)?;
        self.second.leave_binary(binary)?;

        Ok(())
    }

    fn leave_binary_boolean(&mut self, binary: &mut BooleanBinExpr<'script>) -> Result<()> {
        self.first.leave_binary_boolean(binary)?;
        self.second.leave_binary_boolean(binary)?;

        Ok(())
    }

    fn visit_unary(&mut self, unary: &mut UnaryExpr<'script>) -> Result<VisitRes> {
        self.first.visit_unary(unary)?;
        self.second.visit_unary(unary)?;

        Ok(VisitRes::Walk)
    }

    fn leave_unary(&mut self, unary: &mut UnaryExpr<'script>) -> Result<()> {
        self.first.leave_unary(unary)?;
        self.second.leave_unary(unary)?;

        Ok(())
    }

    fn visit_patch(&mut self, patch: &mut Patch<'script>) -> Result<VisitRes> {
        self.first.visit_patch(patch)?;
        self.second.visit_patch(patch)?;

        Ok(VisitRes::Walk)
    }

    fn leave_patch(&mut self, patch: &mut Patch<'script>) -> Result<()> {
        self.first.leave_patch(patch)?;
        self.second.leave_patch(patch)?;

        Ok(())
    }

    fn visit_patch_operation(&mut self, patch: &mut PatchOperation<'script>) -> Result<VisitRes> {
        self.first.visit_patch_operation(patch)?;
        self.second.visit_patch_operation(patch)?;

        Ok(VisitRes::Walk)
    }

    fn leave_patch_operation(&mut self, patch: &mut PatchOperation<'script>) -> Result<()> {
        self.first.leave_patch_operation(patch)?;
        self.second.leave_patch_operation(patch)?;

        Ok(())
    }

    fn visit_precondition(
        &mut self,
        precondition: &mut ClausePreCondition<'script>,
    ) -> Result<VisitRes> {
        self.first.visit_precondition(precondition)?;
        self.second.visit_precondition(precondition)?;

        Ok(VisitRes::Walk)
    }

    fn leave_precondition(&mut self, precondition: &mut ClausePreCondition<'script>) -> Result<()> {
        self.first.leave_precondition(precondition)?;
        self.second.leave_precondition(precondition)?;

        Ok(())
    }

    fn visit_default_case(
        &mut self,
        mdefault: &mut DefaultCase<ImutExpr<'script>>,
    ) -> Result<VisitRes> {
        self.first.visit_default_case(mdefault)?;
        self.second.visit_default_case(mdefault)?;

        Ok(VisitRes::Walk)
    }

    fn leave_default_case(&mut self, mdefault: &mut DefaultCase<ImutExpr<'script>>) -> Result<()> {
        self.first.leave_default_case(mdefault)?;
        self.second.leave_default_case(mdefault)?;

        Ok(())
    }

    fn visit_mmatch(&mut self, mmatch: &mut Match<'script, ImutExpr>) -> Result<VisitRes> {
        self.first.visit_mmatch(mmatch)?;
        self.second.visit_mmatch(mmatch)?;

        Ok(VisitRes::Walk)
    }

    fn leave_mmatch(&mut self, mmatch: &mut Match<'script, ImutExpr>) -> Result<()> {
        self.first.leave_mmatch(mmatch)?;
        self.second.leave_mmatch(mmatch)?;

        Ok(())
    }

    fn visit_predicate_clause(
        &mut self,
        predicate: &mut PredicateClause<'script, ImutExpr<'script>>,
    ) -> Result<VisitRes> {
        self.first.visit_predicate_clause(predicate)?;
        self.second.visit_predicate_clause(predicate)?;

        Ok(VisitRes::Walk)
    }

    fn leave_predicate_clause(
        &mut self,
        predicate: &mut PredicateClause<'script, ImutExpr<'script>>,
    ) -> Result<()> {
        self.first.leave_predicate_clause(predicate)?;
        self.second.leave_predicate_clause(predicate)?;

        Ok(())
    }

    fn visit_clause_group(
        &mut self,
        group: &mut ClauseGroup<'script, ImutExpr<'script>>,
    ) -> Result<VisitRes> {
        self.first.visit_clause_group(group)?;
        self.second.visit_clause_group(group)?;

        Ok(VisitRes::Walk)
    }

    fn leave_clause_group(
        &mut self,
        group: &mut ClauseGroup<'script, ImutExpr<'script>>,
    ) -> Result<()> {
        self.first.leave_clause_group(group)?;
        self.second.leave_clause_group(group)?;

        Ok(())
    }

    fn visit_match_pattern(&mut self, pattern: &mut Pattern<'script>) -> Result<VisitRes> {
        self.first.visit_match_pattern(pattern)?;
        self.second.visit_match_pattern(pattern)?;

        Ok(VisitRes::Walk)
    }

    fn leave_match_pattern(&mut self, pattern: &mut Pattern<'script>) -> Result<()> {
        self.first.leave_match_pattern(pattern)?;
        self.second.leave_match_pattern(pattern)?;

        Ok(())
    }

    fn visit_record_pattern(
        &mut self,
        record_pattern: &mut RecordPattern<'script>,
    ) -> Result<VisitRes> {
        self.first.visit_record_pattern(record_pattern)?;
        self.second.visit_record_pattern(record_pattern)?;

        Ok(VisitRes::Walk)
    }

    fn leave_record_pattern(&mut self, record_pattern: &mut RecordPattern<'script>) -> Result<()> {
        self.first.leave_record_pattern(record_pattern)?;
        self.second.leave_record_pattern(record_pattern)?;

        Ok(())
    }

    fn visit_predicate_pattern(
        &mut self,
        record_pattern: &mut PredicatePattern<'script>,
    ) -> Result<VisitRes> {
        self.first.visit_predicate_pattern(record_pattern)?;
        self.second.visit_predicate_pattern(record_pattern)?;

        Ok(VisitRes::Walk)
    }

    fn leave_predicate_pattern(
        &mut self,
        record_pattern: &mut PredicatePattern<'script>,
    ) -> Result<()> {
        self.first.leave_predicate_pattern(record_pattern)?;
        self.second.leave_predicate_pattern(record_pattern)?;

        Ok(())
    }

    fn visit_array_pattern(
        &mut self,
        array_pattern: &mut ArrayPattern<'script>,
    ) -> Result<VisitRes> {
        self.first.visit_array_pattern(array_pattern)?;
        self.second.visit_array_pattern(array_pattern)?;

        Ok(VisitRes::Walk)
    }

    fn leave_array_pattern(&mut self, array_pattern: &mut ArrayPattern<'script>) -> Result<()> {
        self.first.leave_array_pattern(array_pattern)?;
        self.second.leave_array_pattern(array_pattern)?;

        Ok(())
    }

    fn visit_tuple_pattern(&mut self, pattern: &mut TuplePattern<'script>) -> Result<VisitRes> {
        self.first.visit_tuple_pattern(pattern)?;
        self.second.visit_tuple_pattern(pattern)?;

        Ok(VisitRes::Walk)
    }

    fn leave_tuple_pattern(&mut self, pattern: &mut TuplePattern<'script>) -> Result<()> {
        self.first.leave_tuple_pattern(pattern)?;
        self.second.leave_tuple_pattern(pattern)?;

        Ok(())
    }

    fn visit_array_predicate_pattern(
        &mut self,
        pattern: &mut ArrayPredicatePattern<'script>,
    ) -> Result<VisitRes> {
        self.first.visit_array_predicate_pattern(pattern)?;
        self.second.visit_array_predicate_pattern(pattern)?;

        Ok(VisitRes::Walk)
    }

    fn leave_array_predicate_pattern(
        &mut self,
        pattern: &mut ArrayPredicatePattern<'script>,
    ) -> Result<()> {
        self.first.leave_array_predicate_pattern(pattern)?;
        self.second.leave_array_predicate_pattern(pattern)?;

        Ok(())
    }

    fn visit_test_expr(&mut self, expr: &mut TestExpr) -> Result<VisitRes> {
        self.first.visit_test_expr(expr)?;
        self.second.visit_test_expr(expr)?;

        Ok(VisitRes::Walk)
    }

    fn leave_test_expr(&mut self, expr: &mut TestExpr) -> Result<()> {
        self.first.leave_test_expr(expr)?;
        self.second.leave_test_expr(expr)?;

        Ok(())
    }

    fn visit_comprehension(
        &mut self,
        comp: &mut Comprehension<'script, ImutExpr<'script>>,
    ) -> Result<VisitRes> {
        self.first.visit_comprehension(comp)?;
        self.second.visit_comprehension(comp)?;

        Ok(VisitRes::Walk)
    }

    fn leave_comprehension(
        &mut self,
        comp: &mut Comprehension<'script, ImutExpr<'script>>,
    ) -> Result<()> {
        self.first.leave_comprehension(comp)?;
        self.second.leave_comprehension(comp)?;

        Ok(())
    }

    fn visit_merge(&mut self, merge: &mut Merge<'script>) -> Result<VisitRes> {
        self.first.visit_merge(merge)?;
        self.second.visit_merge(merge)?;

        Ok(VisitRes::Walk)
    }

    fn leave_merge(&mut self, merge: &mut Merge<'script>) -> Result<()> {
        self.first.leave_merge(merge)?;
        self.second.leave_merge(merge)?;

        Ok(())
    }

    fn visit_segment(&mut self, segment: &mut Segment<'script>) -> Result<VisitRes> {
        self.first.visit_segment(segment)?;
        self.second.visit_segment(segment)?;

        Ok(VisitRes::Walk)
    }

    fn leave_segment(&mut self, segment: &mut Segment<'script>) -> Result<()> {
        self.first.leave_segment(segment)?;
        self.second.leave_segment(segment)?;

        Ok(())
    }

    fn visit_path(&mut self, path: &mut Path<'script>) -> Result<VisitRes> {
        self.first.visit_path(path)?;
        self.second.visit_path(path)?;

        Ok(VisitRes::Walk)
    }

    fn leave_path(&mut self, path: &mut Path<'script>) -> Result<()> {
        self.first.leave_path(path)?;
        self.second.leave_path(path)?;

        Ok(())
    }

    fn visit_string_lit(&mut self, cow: &mut Cow<'script, str>) -> Result<VisitRes> {
        self.first.visit_string_lit(cow)?;
        self.second.visit_string_lit(cow)?;

        Ok(VisitRes::Walk)
    }

    fn leave_string_lit(&mut self, cow: &mut Cow<'script, str>) -> Result<()> {
        self.first.leave_string_lit(cow)?;
        self.second.leave_string_lit(cow)?;

        Ok(())
    }

    fn visit_ident(&mut self, cow: &mut Ident<'script>) -> Result<VisitRes> {
        self.first.visit_ident(cow)?;
        self.second.visit_ident(cow)?;

        Ok(VisitRes::Walk)
    }

    fn leave_ident(&mut self, cow: &mut Ident<'script>) -> Result<()> {
        self.first.leave_ident(cow)?;
        self.second.leave_ident(cow)?;

        Ok(())
    }

    fn visit_string(&mut self, string: &mut StringLit<'script>) -> Result<VisitRes> {
        self.first.visit_string(string)?;
        self.second.visit_string(string)?;

        Ok(VisitRes::Walk)
    }

    fn leave_string(&mut self, string: &mut StringLit<'script>) -> Result<()> {
        self.first.leave_string(string)?;
        self.second.leave_string(string)?;

        Ok(())
    }

    fn visit_string_element(&mut self, string: &mut StrLitElement<'script>) -> Result<VisitRes> {
        self.first.visit_string_element(string)?;
        self.second.visit_string_element(string)?;

        Ok(VisitRes::Walk)
    }

    fn leave_string_element(&mut self, string: &mut StrLitElement<'script>) -> Result<()> {
        self.first.leave_string_element(string)?;
        self.second.leave_string_element(string)?;

        Ok(())
    }

    fn visit_local(&mut self, local_idx: &mut usize) -> Result<VisitRes> {
        self.first.visit_local(local_idx)?;
        self.second.visit_local(local_idx)?;

        Ok(VisitRes::Walk)
    }

    fn leave_local(&mut self, local_idx: &mut usize) -> Result<()> {
        self.first.leave_local(local_idx)?;
        self.second.leave_local(local_idx)?;

        Ok(())
    }

    fn visit_present(&mut self, path: &mut Path<'script>) -> Result<VisitRes> {
        self.first.visit_present(path)?;
        self.second.visit_present(path)?;

        Ok(VisitRes::Walk)
    }

    fn leave_present(&mut self, path: &mut Path<'script>) -> Result<()> {
        self.first.leave_present(path)?;
        self.second.leave_present(path)?;

        Ok(())
    }

    fn visit_invoke(&mut self, invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        self.first.visit_invoke(invoke)?;
        self.second.visit_invoke(invoke)?;

        Ok(VisitRes::Walk)
    }

    fn leave_invoke(&mut self, invoke: &mut Invoke<'script>) -> Result<()> {
        self.first.leave_invoke(invoke)?;
        self.second.leave_invoke(invoke)?;

        Ok(())
    }

    fn visit_invoke_aggr(&mut self, invoke_aggr: &mut InvokeAggr) -> Result<VisitRes> {
        self.first.visit_invoke_aggr(invoke_aggr)?;
        self.second.visit_invoke_aggr(invoke_aggr)?;

        Ok(VisitRes::Walk)
    }

    fn leave_invoke_aggr(&mut self, invoke_aggr: &mut InvokeAggr) -> Result<()> {
        self.first.leave_invoke_aggr(invoke_aggr)?;
        self.second.leave_invoke_aggr(invoke_aggr)?;

        Ok(())
    }

    fn visit_recur(&mut self, recur: &mut Recur<'script>) -> Result<VisitRes> {
        self.first.visit_recur(recur)?;
        self.second.visit_recur(recur)?;

        Ok(VisitRes::Walk)
    }

    fn leave_recur(&mut self, recur: &mut Recur<'script>) -> Result<()> {
        self.first.leave_recur(recur)?;
        self.second.leave_recur(recur)?;

        Ok(())
    }

    fn visit_bytes(&mut self, bytes: &mut Bytes<'script>) -> Result<VisitRes> {
        self.first.visit_bytes(bytes)?;
        self.second.visit_bytes(bytes)?;

        Ok(VisitRes::Walk)
    }

    fn leave_bytes(&mut self, bytes: &mut Bytes<'script>) -> Result<()> {
        self.first.leave_bytes(bytes)?;
        self.second.leave_bytes(bytes)?;

        Ok(())
    }

    fn visit_literal(&mut self, literal: &mut Literal<'script>) -> Result<VisitRes> {
        self.first.visit_literal(literal)?;
        self.second.visit_literal(literal)?;

        Ok(VisitRes::Walk)
    }

    fn leave_literal(&mut self, literal: &mut Literal<'script>) -> Result<()> {
        self.first.leave_literal(literal)?;
        self.second.leave_literal(literal)?;

        Ok(())
    }

    fn visit_expr(&mut self, e: &mut ImutExpr<'script>) -> Result<VisitRes> {
        self.first.visit_expr(e)?;
        self.second.visit_expr(e)?;

        Ok(VisitRes::Walk)
    }

    fn leave_expr(&mut self, e: &mut ImutExpr<'script>) -> Result<()> {
        self.first.leave_expr(e)?;
        self.second.leave_expr(e)?;

        Ok(())
    }

    fn visit_segments(&mut self, e: &mut Vec<Segment<'script>>) -> Result<VisitRes> {
        self.first.visit_segments(e)?;
        self.second.visit_segments(e)?;

        Ok(VisitRes::Walk)
    }

    fn leave_segments(&mut self, e: &mut Vec<Segment<'script>>) -> Result<()> {
        self.first.leave_segments(e)?;
        self.second.leave_segments(e)?;

        Ok(())
    }

    fn visit_expr_path(&mut self, e: &mut ExprPath<'script>) -> Result<VisitRes> {
        self.first.visit_expr_path(e)?;
        self.second.visit_expr_path(e)?;

        Ok(VisitRes::Walk)
    }

    fn leave_expr_path(&mut self, e: &mut ExprPath<'script>) -> Result<()> {
        self.first.leave_expr_path(e)?;
        self.second.leave_expr_path(e)?;

        Ok(())
    }

    fn visit_const_path(&mut self, e: &mut LocalPath<'script>) -> Result<VisitRes> {
        self.first.visit_const_path(e)?;
        self.second.visit_const_path(e)?;

        Ok(VisitRes::Walk)
    }

    fn leave_const_path(&mut self, e: &mut LocalPath<'script>) -> Result<()> {
        self.first.leave_const_path(e)?;
        self.second.leave_const_path(e)?;

        Ok(())
    }

    fn visit_local_path(&mut self, e: &mut LocalPath<'script>) -> Result<VisitRes> {
        self.first.visit_local_path(e)?;
        self.second.visit_local_path(e)?;

        Ok(VisitRes::Walk)
    }

    fn leave_local_path(&mut self, e: &mut LocalPath<'script>) -> Result<()> {
        self.first.leave_local_path(e)?;
        self.second.leave_local_path(e)?;

        Ok(())
    }

    fn visit_event_path(&mut self, e: &mut EventPath<'script>) -> Result<VisitRes> {
        self.first.visit_event_path(e)?;
        self.second.visit_event_path(e)?;

        Ok(VisitRes::Walk)
    }

    fn leave_event_path(&mut self, e: &mut EventPath<'script>) -> Result<()> {
        self.first.leave_event_path(e)?;
        self.second.leave_event_path(e)?;

        Ok(())
    }

    fn visit_state_path(&mut self, e: &mut StatePath<'script>) -> Result<VisitRes> {
        self.first.visit_state_path(e)?;
        self.second.visit_state_path(e)?;

        Ok(VisitRes::Walk)
    }

    fn leave_state_path(&mut self, e: &mut StatePath<'script>) -> Result<()> {
        self.first.leave_state_path(e)?;
        self.second.leave_state_path(e)?;

        Ok(())
    }

    fn visit_meta_path(&mut self, e: &mut MetadataPath<'script>) -> Result<VisitRes> {
        self.first.visit_meta_path(e)?;
        self.second.visit_meta_path(e)?;

        Ok(VisitRes::Walk)
    }

    fn leave_meta_path(&mut self, e: &mut MetadataPath<'script>) -> Result<()> {
        self.first.leave_meta_path(e)?;
        self.second.leave_meta_path(e)?;

        Ok(())
    }

    fn visit_reserved_path(&mut self, e: &mut ReservedPath<'script>) -> Result<VisitRes> {
        self.first.visit_reserved_path(e)?;
        self.second.visit_reserved_path(e)?;

        Ok(VisitRes::Walk)
    }

    fn leave_reserved_path(&mut self, e: &mut ReservedPath<'script>) -> Result<()> {
        self.first.leave_reserved_path(e)?;
        self.second.leave_reserved_path(e)?;

        Ok(())
    }

    fn visit_const(&mut self, e: &mut Const<'script>) -> Result<VisitRes> {
        self.first.visit_const(e)?;
        self.second.visit_const(e)?;

        Ok(VisitRes::Walk)
    }

    fn visit_array_append(&mut self, a: &mut ArrayAppend<'script>) -> Result<VisitRes> {
        self.first.visit_array_append(a)?;
        self.second.visit_array_append(a)?;

        Ok(VisitRes::Walk)
    }

    fn leave_array_append(&mut self, a: &mut ArrayAppend<'script>) -> Result<()> {
        self.first.leave_array_append(a)?;
        self.second.leave_array_append(a)?;

        Ok(())
    }

    fn leave_const(&mut self, e: &mut Const<'script>) -> Result<()> {
        self.first.leave_const(e)?;
        self.second.leave_const(e)?;

        Ok(())
    }
}
impl<'script, First, Second> QueryVisitor<'script> for CombinedVisitor<First, Second>
where
    First: QueryVisitor<'script>,
    Second: QueryVisitor<'script>,
{
    fn visit_definitional_args_with(
        &mut self,
        with: &mut DefinitionalArgsWith<'script>,
    ) -> Result<VisitRes> {
        self.first.visit_definitional_args_with(with)?;
        self.second.visit_definitional_args_with(with)?;

        Ok(VisitRes::Walk)
    }

    fn leave_definitional_args_with(
        &mut self,
        with: &mut DefinitionalArgsWith<'script>,
    ) -> Result<()> {
        self.first.leave_definitional_args_with(with)?;
        self.second.leave_definitional_args_with(with)?;

        Ok(())
    }

    fn visit_definitional_args(
        &mut self,
        with: &mut DefinitionalArgs<'script>,
    ) -> Result<VisitRes> {
        self.first.visit_definitional_args(with)?;
        self.second.visit_definitional_args(with)?;

        Ok(VisitRes::Walk)
    }

    fn leave_definitional_args(&mut self, with: &mut DefinitionalArgs<'script>) -> Result<()> {
        self.first.leave_definitional_args(with)?;
        self.second.leave_definitional_args(with)?;

        Ok(())
    }

    fn visit_creational_with(&mut self, with: &mut CreationalWith<'script>) -> Result<VisitRes> {
        self.first.visit_creational_with(with)?;
        self.second.visit_creational_with(with)?;

        Ok(VisitRes::Walk)
    }

    fn leave_creational_with(&mut self, with: &mut CreationalWith<'script>) -> Result<()> {
        self.first.leave_creational_with(with)?;
        self.second.leave_creational_with(with)?;

        Ok(())
    }

    fn visit_with_expr(&mut self, with: &mut WithExpr<'script>) -> Result<VisitRes> {
        self.first.visit_with_expr(with)?;
        self.second.visit_with_expr(with)?;

        Ok(VisitRes::Walk)
    }

    fn leave_with_expr(&mut self, with: &mut WithExpr<'script>) -> Result<()> {
        self.first.leave_with_expr(with)?;
        self.second.leave_with_expr(with)?;

        Ok(())
    }

    fn visit_args_expr(&mut self, args: &mut ArgsExpr<'script>) -> Result<VisitRes> {
        self.first.visit_args_expr(args)?;
        self.second.visit_args_expr(args)?;

        Ok(VisitRes::Walk)
    }

    fn leave_args_expr(&mut self, args: &mut ArgsExpr<'script>) -> Result<()> {
        self.first.leave_args_expr(args)?;
        self.second.leave_args_expr(args)?;

        Ok(())
    }

    fn visit_query(&mut self, q: &mut Query<'script>) -> Result<VisitRes> {
        self.first.visit_query(q)?;
        self.second.visit_query(q)?;

        Ok(VisitRes::Walk)
    }

    fn leave_query(&mut self, q: &mut Query<'script>) -> Result<()> {
        self.first.leave_query(q)?;
        self.second.leave_query(q)?;

        Ok(())
    }

    fn visit_module_content(&mut self, q: &mut Content<'script>) -> Result<VisitRes> {
        self.first.visit_module_content(q)?;
        self.second.visit_module_content(q)?;

        Ok(VisitRes::Walk)
    }

    fn leave_module_content(&mut self, q: &mut Content<'script>) -> Result<()> {
        self.first.leave_module_content(q)?;
        self.second.leave_module_content(q)?;

        Ok(())
    }

    fn visit_stmt(&mut self, stmt: &mut Stmt<'script>) -> Result<VisitRes> {
        self.first.visit_stmt(stmt)?;
        self.second.visit_stmt(stmt)?;

        Ok(VisitRes::Walk)
    }

    fn leave_stmt(&mut self, stmt: &mut Stmt<'script>) -> Result<()> {
        self.first.leave_stmt(stmt)?;
        self.second.leave_stmt(stmt)?;

        Ok(())
    }

    fn visit_group_by(&mut self, stmt: &mut GroupBy<'script>) -> Result<VisitRes> {
        self.first.visit_group_by(stmt)?;
        self.second.visit_group_by(stmt)?;

        Ok(VisitRes::Walk)
    }

    fn leave_group_by(&mut self, stmt: &mut GroupBy<'script>) -> Result<()> {
        self.first.leave_group_by(stmt)?;
        self.second.leave_group_by(stmt)?;

        Ok(())
    }

    fn visit_script(&mut self, script: &mut Script<'script>) -> Result<VisitRes> {
        self.first.visit_script(script)?;
        self.second.visit_script(script)?;

        Ok(VisitRes::Walk)
    }

    fn leave_script(&mut self, script: &mut Script<'script>) -> Result<()> {
        self.first.leave_script(script)?;
        self.second.leave_script(script)?;

        Ok(())
    }

    fn visit_select(&mut self, script: &mut Select<'script>) -> Result<VisitRes> {
        self.first.visit_select(script)?;
        self.second.visit_select(script)?;

        Ok(VisitRes::Walk)
    }

    fn leave_select(&mut self, script: &mut Select<'script>) -> Result<()> {
        self.first.leave_select(script)?;
        self.second.leave_select(script)?;

        Ok(())
    }

    fn visit_window_defn(&mut self, defn: &mut WindowDefinition<'script>) -> Result<VisitRes> {
        self.first.visit_window_defn(defn)?;
        self.second.visit_window_defn(defn)?;

        Ok(VisitRes::Walk)
    }

    fn leave_window_defn(&mut self, defn: &mut WindowDefinition<'script>) -> Result<()> {
        self.first.leave_window_defn(defn)?;
        self.second.leave_window_defn(defn)?;

        Ok(())
    }

    fn visit_window_name(&mut self, defn: &mut WindowName) -> Result<VisitRes> {
        self.first.visit_window_name(defn)?;
        self.second.visit_window_name(defn)?;

        Ok(VisitRes::Walk)
    }

    fn leave_window_name(&mut self, defn: &mut WindowName) -> Result<()> {
        self.first.leave_window_name(defn)?;
        self.second.leave_window_name(defn)?;

        Ok(())
    }

    fn visit_operator_defn(&mut self, defn: &mut OperatorDefinition<'script>) -> Result<VisitRes> {
        self.first.visit_operator_defn(defn)?;
        self.second.visit_operator_defn(defn)?;

        Ok(VisitRes::Walk)
    }

    fn leave_operator_defn(&mut self, defn: &mut OperatorDefinition<'script>) -> Result<()> {
        self.first.leave_operator_defn(defn)?;
        self.second.leave_operator_defn(defn)?;

        Ok(())
    }

    fn visit_script_defn(&mut self, defn: &mut ScriptDefinition<'script>) -> Result<VisitRes> {
        self.first.visit_script_defn(defn)?;
        self.second.visit_script_defn(defn)?;

        Ok(VisitRes::Walk)
    }

    fn leave_script_defn(&mut self, defn: &mut ScriptDefinition<'script>) -> Result<()> {
        self.first.leave_script_defn(defn)?;
        self.second.leave_script_defn(defn)?;

        Ok(())
    }

    fn visit_pipeline_defn(&mut self, defn: &mut PipelineDefinition<'script>) -> Result<VisitRes> {
        self.first.visit_pipeline_defn(defn)?;
        self.second.visit_pipeline_defn(defn)?;

        Ok(VisitRes::Walk)
    }

    fn leave_pipeline_defn(&mut self, defn: &mut PipelineDefinition<'script>) -> Result<()> {
        self.first.leave_pipeline_defn(defn)?;
        self.second.leave_pipeline_defn(defn)?;

        Ok(())
    }

    fn visit_stream_stmt(&mut self, stmt: &mut StreamStmt) -> Result<VisitRes> {
        self.first.visit_stream_stmt(stmt)?;
        self.second.visit_stream_stmt(stmt)?;

        Ok(VisitRes::Walk)
    }

    fn leave_stream_stmt(&mut self, stmt: &mut StreamStmt) -> Result<()> {
        self.first.leave_stream_stmt(stmt)?;
        self.second.leave_stream_stmt(stmt)?;

        Ok(())
    }

    fn visit_operator_create(&mut self, stmt: &mut OperatorCreate<'script>) -> Result<VisitRes> {
        self.first.visit_operator_create(stmt)?;
        self.second.visit_operator_create(stmt)?;

        Ok(VisitRes::Walk)
    }

    fn leave_operator_create(&mut self, stmt: &mut OperatorCreate<'script>) -> Result<()> {
        self.first.leave_operator_create(stmt)?;
        self.second.leave_operator_create(stmt)?;

        Ok(())
    }

    fn visit_script_create(&mut self, stmt: &mut ScriptCreate<'script>) -> Result<VisitRes> {
        self.first.visit_script_create(stmt)?;
        self.second.visit_script_create(stmt)?;

        Ok(VisitRes::Walk)
    }

    fn leave_script_create(&mut self, stmt: &mut ScriptCreate<'script>) -> Result<()> {
        self.first.leave_script_create(stmt)?;
        self.second.leave_script_create(stmt)?;

        Ok(())
    }

    fn visit_pipeline_create(&mut self, stmt: &mut PipelineCreate) -> Result<VisitRes> {
        self.first.visit_pipeline_create(stmt)?;
        self.second.visit_pipeline_create(stmt)?;

        Ok(VisitRes::Walk)
    }

    fn leave_pipeline_create(&mut self, stmt: &mut PipelineCreate) -> Result<()> {
        self.first.leave_pipeline_create(stmt)?;
        self.second.leave_pipeline_create(stmt)?;

        Ok(())
    }

    fn visit_select_stmt(&mut self, stmt: &mut SelectStmt<'script>) -> Result<VisitRes> {
        self.first.visit_select_stmt(stmt)?;
        self.second.visit_select_stmt(stmt)?;

        Ok(VisitRes::Walk)
    }

    fn leave_select_stmt(&mut self, stmt: &mut SelectStmt<'script>) -> Result<()> {
        self.first.leave_select_stmt(stmt)?;
        self.second.leave_select_stmt(stmt)?;

        Ok(())
    }
}
impl<'script, First, Second> QueryWalker<'script> for CombinedVisitor<First, Second>
where
    First: ExprVisitor<'script> + ImutExprVisitor<'script> + QueryVisitor<'script>,
    Second: ExprVisitor<'script> + ImutExprVisitor<'script> + QueryVisitor<'script>,
{
}

/// Runs optimisers on a given AST element
pub struct Optimizer<'run, 'script>
where
    'script: 'run,
{
    visitor: CombinedVisitor<ConstFolder<'run, 'script>, ArrayAdditionOptimizer>,
}

impl<'run, 'script, 'registry> Optimizer<'run, 'script>
where
    'script: 'run,
{
    #[must_use]
    /// Create a new instance
    pub fn new(helper: &'run Helper<'script, 'registry>) -> Self {
        Self {
            visitor: CombinedVisitor {
                first: ConstFolder::new(helper),
                second: ArrayAdditionOptimizer {},
            },
        }
    }

    /// Walk `ImutExpr`
    /// # Errors
    /// When the walker fails
    pub fn walk_imut_expr(&mut self, expr: &mut ImutExpr<'script>) -> Result<()> {
        IMutExprWalker::walk_expr(&mut self.visitor, expr)?;

        Ok(())
    }

    /// Walk `Script`
    /// # Errors
    /// When the walker fails
    pub fn walk_script(&mut self, e: &mut Script<'script>) -> Result<()> {
        self.visitor.walk_script(e)?;

        Ok(())
    }

    /// Walk `Query`
    /// # Errors
    /// When the walker fails
    pub fn walk_query(&mut self, e: &mut Query<'script>) -> Result<()> {
        self.visitor.walk_query(e)?;

        Ok(())
    }

    /// Walk `FnDefn`
    /// # Errors
    /// When the walker fails
    pub fn walk_fn_defn(&mut self, e: &mut FnDefn<'script>) -> Result<()> {
        self.visitor.walk_fn_defn(e)?;

        Ok(())
    }

    /// Walk `SelectStmt`
    /// # Errors
    /// When the walker fails
    pub fn walk_select_stmt(&mut self, e: &mut SelectStmt<'script>) -> Result<()> {
        self.visitor.walk_select_stmt(e)?;

        Ok(())
    }

    /// Walk `DefinitionalArgs`
    /// # Errors
    /// When the walker fails
    pub fn walk_definitional_args(&mut self, e: &mut DefinitionalArgs<'script>) -> Result<()> {
        self.visitor.walk_definitional_args(e)?;

        Ok(())
    }

    /// Walk `CreationalWith`
    /// # Errors
    /// When the walker fails
    pub fn walk_creational_with(&mut self, e: &mut CreationalWith<'script>) -> Result<()> {
        self.visitor.walk_creational_with(e)?;

        Ok(())
    }

    /// Walk `Stmt`
    /// # Errors
    /// When the walker fails
    pub fn walk_stmt(&mut self, e: &mut Stmt<'script>) -> Result<()> {
        self.visitor.walk_stmt(e)?;

        Ok(())
    }

    /// Walk `WindowDefinition`
    /// # Errors
    /// When the walker fails
    pub fn walk_window_defn(&mut self, e: &mut WindowDefinition<'script>) -> Result<()> {
        self.visitor.walk_window_defn(e)?;

        Ok(())
    }

    /// Walk `ScriptDefinition`
    /// # Errors
    /// When the walker fails
    pub fn walk_script_defn(&mut self, e: &mut ScriptDefinition<'script>) -> Result<()> {
        self.visitor.walk_script_defn(e)?;

        Ok(())
    }
}
