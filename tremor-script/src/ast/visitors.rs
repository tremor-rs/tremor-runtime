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

use super::{base_expr::BaseExpr, ClauseGroup, Comprehension};
use super::{eq::AstEq, PredicateClause};
use super::{
    ArrayPattern, ArrayPredicatePattern, BinExpr, Bytes, EventPath, GroupBy, GroupByInt,
    ImutExprInt, Invoke, InvokeAggr, List, Literal, LocalPath, Match, Merge, MetadataPath,
    NodeMetas, Patch, PatchOperation, Path, Pattern, PredicatePattern, Record, RecordPattern,
    Recur, ReservedPath, Segment, StatePath, StrLitElement, StringLit, UnaryExpr,
};
use crate::errors::{error_event_ref_not_allowed, Result};
/// Return value from visit methods for `ImutExprIntVisitor`
/// controlling whether to continue walking the subtree or not
pub enum VisitRes {
    /// carry on walking
    Walk,
    /// stop walking
    Stop,
}

use VisitRes::Walk;

/// Visitor for traversing all `ImutExprInt`s within the given `ImutExprInt`
///
/// Implement your custom expr visiting logic by overwriting the visit_* methods.
/// You do not need to traverse further down. This is done by the provided `walk_*` methods.
/// The walk_* methods implement walking the expression tree, those do not need to be changed.
pub trait ImutExprIntVisitor<'script> {
    /// visit a record
    fn visit_record(&mut self, _record: &mut Record<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a record
    fn walk_record(&mut self, record: &mut Record<'script>) -> Result<()> {
        for field in &mut record.fields {
            self.walk_expr(&mut field.name)?;
            self.walk_expr(&mut field.value)?;
        }
        Ok(())
    }
    /// visit a list
    fn visit_list(&mut self, _list: &mut List<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a list
    fn walk_list(&mut self, list: &mut List<'script>) -> Result<()> {
        for element in &mut list.exprs {
            self.walk_expr(&mut element.0)?;
        }
        Ok(())
    }
    /// visit a binary
    fn visit_binary(&mut self, _binary: &mut BinExpr<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a binary
    fn walk_binary(&mut self, binary: &mut BinExpr<'script>) -> Result<()> {
        self.walk_expr(&mut binary.lhs)?;
        self.walk_expr(&mut binary.rhs)
    }

    /// visit a unary expr
    fn visit_unary(&mut self, _unary: &mut UnaryExpr<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a unary
    fn walk_unary(&mut self, unary: &mut UnaryExpr<'script>) -> Result<()> {
        self.walk_expr(&mut unary.expr)
    }

    /// visit a patch expr
    fn visit_patch(&mut self, _patch: &mut Patch<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a match expr
    fn visit_match(&mut self, _mmatch: &mut Match<'script, ImutExprInt>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a patch expr
    fn walk_patch(&mut self, patch: &mut Patch<'script>) -> Result<()> {
        self.walk_expr(&mut patch.target)?;
        for op in &mut patch.operations {
            match op {
                PatchOperation::Insert { ident, expr }
                | PatchOperation::Merge { ident, expr }
                | PatchOperation::Update { ident, expr }
                | PatchOperation::Upsert { ident, expr } => {
                    self.walk_expr(ident)?;
                    self.walk_expr(expr)?;
                }
                PatchOperation::Copy { from, to } | PatchOperation::Move { from, to } => {
                    self.walk_expr(from)?;
                    self.walk_expr(to)?;
                }
                PatchOperation::Erase { ident } => {
                    self.walk_expr(ident)?;
                }
                PatchOperation::TupleMerge { expr } => {
                    self.walk_expr(expr)?;
                }
            }
        }
        Ok(())
    }

    /// Walks a precondition
    fn walk_precondition(
        &mut self,
        precondition: &mut super::ClausePreCondition<'script>,
    ) -> Result<()> {
        for segment in &mut precondition.segments {
            self.walk_segment(segment)?;
        }
        Ok(())
    }

    /// walk a match expr
    fn walk_match(&mut self, mmatch: &mut Match<'script, ImutExprInt<'script>>) -> Result<()> {
        self.walk_expr(&mut mmatch.target)?;
        for group in &mut mmatch.patterns {
            self.walk_clause_group(group)?;
        }
        Ok(())
    }

    /// Walks a predicate clause
    fn walk_predicate_clause(
        &mut self,
        predicate: &mut PredicateClause<'script, ImutExprInt<'script>>,
    ) -> Result<()> {
        self.walk_match_patterns(&mut predicate.pattern)?;
        if let Some(guard) = &mut predicate.guard {
            self.walk_expr(guard)?;
        }
        for expr in &mut predicate.exprs {
            self.walk_expr(expr)?;
        }
        self.walk_expr(&mut predicate.last_expr)?;
        Ok(())
    }

    /// Walks a clause group
    fn walk_clause_group(
        &mut self,
        group: &mut ClauseGroup<'script, ImutExprInt<'script>>,
    ) -> Result<()> {
        match group {
            ClauseGroup::Single {
                precondition,
                pattern,
            } => {
                if let Some(precondition) = precondition {
                    self.walk_precondition(precondition)?;
                }
                self.walk_predicate_clause(pattern)
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
                Ok(())
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
                Ok(())
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
                Ok(())
            }
        }
    }
    /// walk match patterns
    fn walk_match_patterns(&mut self, pattern: &mut Pattern<'script>) -> Result<()> {
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
            Pattern::Assign(assign_pattern) => {
                self.walk_match_patterns(assign_pattern.pattern.as_mut())?;
            }
            Pattern::Tuple(tuple_pattern) => {
                for elem in &mut tuple_pattern.exprs {
                    match elem {
                        ArrayPredicatePattern::Expr(expr) => {
                            self.walk_expr(expr)?;
                        }
                        ArrayPredicatePattern::Record(record_pattern) => {
                            self.walk_record_pattern(record_pattern)?;
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
    /// walk a record pattern
    fn walk_record_pattern(&mut self, record_pattern: &mut RecordPattern<'script>) -> Result<()> {
        for field in &mut record_pattern.fields {
            match field {
                PredicatePattern::RecordPatternEq { pattern, .. } => {
                    self.walk_record_pattern(pattern)?;
                }
                PredicatePattern::Bin { rhs, .. } => {
                    self.walk_expr(rhs)?;
                }
                PredicatePattern::ArrayPatternEq { pattern, .. } => {
                    self.walk_array_pattern(pattern)?;
                }
                _ => {}
            }
        }
        Ok(())
    }
    /// walk an array pattern
    fn walk_array_pattern(&mut self, array_pattern: &mut ArrayPattern<'script>) -> Result<()> {
        for elem in &mut array_pattern.exprs {
            match elem {
                ArrayPredicatePattern::Expr(expr) => {
                    self.walk_expr(expr)?;
                }
                ArrayPredicatePattern::Record(record_pattern) => {
                    self.walk_record_pattern(record_pattern)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// visit a comprehension
    fn visit_comprehension(
        &mut self,
        _comp: &mut Comprehension<'script, ImutExprInt<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a comprehension
    fn walk_comprehension(
        &mut self,
        comp: &mut Comprehension<'script, ImutExprInt<'script>>,
    ) -> Result<()> {
        self.walk_expr(&mut comp.target)?;
        for comp_case in &mut comp.cases {
            if let Some(guard) = &mut comp_case.guard {
                self.walk_expr(guard)?;
            }
            for expr in &mut comp_case.exprs {
                self.walk_expr(expr)?;
            }
            self.walk_expr(&mut comp_case.last_expr)?;
        }
        Ok(())
    }

    /// visit a merge expr
    fn visit_merge(&mut self, _merge: &mut Merge<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a merge expr
    fn walk_merge(&mut self, merge: &mut Merge<'script>) -> Result<()> {
        self.walk_expr(&mut merge.target)?;
        self.walk_expr(&mut merge.expr)
    }

    /// walk a path segment
    fn walk_segment(&mut self, segment: &mut Segment<'script>) -> Result<()> {
        match segment {
            Segment::Element { expr, .. } => self.walk_expr(expr),
            Segment::Range {
                range_start,
                range_end,
                ..
            } => {
                self.walk_expr(range_start.as_mut())?;
                self.walk_expr(range_end.as_mut())
            }
            _ => Ok(()),
        }
    }

    /// visit a path
    fn visit_path(&mut self, _path: &mut Path<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// walk a path
    fn walk_path(&mut self, path: &mut Path<'script>) -> Result<()> {
        let segments = match path {
            Path::Const(LocalPath { segments, .. })
            | Path::Local(LocalPath { segments, .. })
            | Path::Event(EventPath { segments, .. })
            | Path::State(StatePath { segments, .. })
            | Path::Meta(MetadataPath { segments, .. })
            | Path::Reserved(ReservedPath::Args { segments, .. })
            | Path::Reserved(ReservedPath::Group { segments, .. })
            | Path::Reserved(ReservedPath::Window { segments, .. }) => segments,
        };
        for segment in segments {
            self.walk_segment(segment)?
        }
        Ok(())
    }

    /// visit a string
    fn visit_string(&mut self, _string: &mut StringLit<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a string
    fn walk_string(&mut self, string: &mut StringLit<'script>) -> Result<()> {
        for element in &mut string.elements {
            if let StrLitElement::Expr(expr) = element {
                self.walk_expr(expr)?
            }
        }
        Ok(())
    }

    /// visit a local
    fn visit_local(&mut self, _local_idx: &mut usize) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a present expr
    fn visit_present(&mut self, _path: &mut Path<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit an invoke expr
    fn visit_invoke(&mut self, _invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk an invoke expr
    fn walk_invoke(&mut self, invoke: &mut Invoke<'script>) -> Result<()> {
        for arg in &mut invoke.args {
            self.walk_expr(&mut arg.0)?;
        }
        Ok(())
    }

    /// visit an invoke1 expr
    fn visit_invoke1(&mut self, _invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// visit an invoke2 expr
    fn visit_invoke2(&mut self, _invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// visit an invoke3 expr
    fn visit_invoke3(&mut self, _invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit an `invoke_aggr` expr
    fn visit_invoke_aggr(&mut self, _invoke_aggr: &mut InvokeAggr) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a recur expr
    fn visit_recur(&mut self, _recur: &mut Recur<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk a recur expr
    fn walk_recur(&mut self, recur: &mut Recur<'script>) -> Result<()> {
        for expr in &mut recur.exprs {
            self.walk_expr(&mut expr.0)?;
        }
        Ok(())
    }

    /// visit bytes
    fn visit_bytes(&mut self, _bytes: &mut Bytes<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// walk bytes
    fn walk_bytes(&mut self, bytes: &mut Bytes<'script>) -> Result<()> {
        for part in &mut bytes.value {
            self.walk_expr(&mut part.data.0)?;
        }
        Ok(())
    }

    /// visit a literal
    fn visit_literal(&mut self, _literal: &mut Literal<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a generic `ImutExprInt` (this is called before the concrete `visit_*` method)
    fn visit_expr(&mut self, _e: &mut ImutExprInt<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// entry point into this visitor - call this to start visiting the given expression `e`
    fn walk_expr(&mut self, e: &mut ImutExprInt<'script>) -> Result<()> {
        if let Walk = self.visit_expr(e)? {
            match e {
                ImutExprInt::Record(record) => {
                    if let Walk = self.visit_record(record)? {
                        self.walk_record(record)?;
                    }
                }
                ImutExprInt::List(list) => {
                    if let Walk = self.visit_list(list)? {
                        self.walk_list(list)?;
                    }
                }
                ImutExprInt::Binary(binary) => {
                    if let Walk = self.visit_binary(binary.as_mut())? {
                        self.walk_binary(binary.as_mut())?;
                    }
                }
                ImutExprInt::Unary(unary) => {
                    if let Walk = self.visit_unary(unary.as_mut())? {
                        self.walk_unary(unary.as_mut())?;
                    }
                }
                ImutExprInt::Patch(patch) => {
                    if let Walk = self.visit_patch(patch.as_mut())? {
                        self.walk_patch(patch.as_mut())?;
                    }
                }
                ImutExprInt::Match(mmatch) => {
                    if let Walk = self.visit_match(mmatch.as_mut())? {
                        self.walk_match(mmatch.as_mut())?;
                    }
                }
                ImutExprInt::Comprehension(comp) => {
                    if let Walk = self.visit_comprehension(comp.as_mut())? {
                        self.walk_comprehension(comp.as_mut())?;
                    }
                }
                ImutExprInt::Merge(merge) => {
                    if let Walk = self.visit_merge(merge.as_mut())? {
                        self.walk_merge(merge.as_mut())?;
                    }
                }
                ImutExprInt::Path(path) => {
                    if let Walk = self.visit_path(path)? {
                        self.walk_path(path)?;
                    }
                }
                ImutExprInt::String(string) => {
                    if let Walk = self.visit_string(string)? {
                        self.walk_string(string)?;
                    }
                }
                ImutExprInt::Local { idx, .. } => {
                    self.visit_local(idx)?;
                }
                ImutExprInt::Present { path, .. } => {
                    if let Walk = self.visit_present(path)? {
                        self.walk_path(path)?;
                    }
                }
                ImutExprInt::Invoke(invoke) => {
                    if let Walk = self.visit_invoke(invoke)? {
                        self.walk_invoke(invoke)?;
                    }
                }
                ImutExprInt::Invoke1(invoke1) => {
                    if let Walk = self.visit_invoke1(invoke1)? {
                        self.walk_invoke(invoke1)?;
                    }
                }
                ImutExprInt::Invoke2(invoke2) => {
                    if let Walk = self.visit_invoke2(invoke2)? {
                        self.walk_invoke(invoke2)?;
                    }
                }
                ImutExprInt::Invoke3(invoke3) => {
                    if let Walk = self.visit_invoke3(invoke3)? {
                        self.walk_invoke(invoke3)?;
                    }
                }
                ImutExprInt::InvokeAggr(invoke_aggr) => {
                    self.visit_invoke_aggr(invoke_aggr)?;
                }
                ImutExprInt::Recur(recur) => {
                    if let Walk = self.visit_recur(recur)? {
                        self.walk_recur(recur)?;
                    }
                }
                ImutExprInt::Bytes(bytes) => {
                    if let Walk = self.visit_bytes(bytes)? {
                        self.walk_bytes(bytes)?;
                    }
                }
                ImutExprInt::Literal(lit) => {
                    self.visit_literal(lit)?;
                }
            }
        }

        Ok(())
    }
}

pub(crate) trait GroupByVisitor<'script> {
    fn visit_expr(&mut self, expr: &ImutExprInt<'script>);

    fn walk_group_by(&mut self, group_by: &GroupByInt<'script>) {
        match group_by {
            GroupByInt::Expr { expr, .. } | GroupByInt::Each { expr, .. } => self.visit_expr(expr),
            GroupByInt::Set { items, .. } => {
                for inner_group_by in items {
                    self.walk_group_by(&inner_group_by.0)
                }
            }
        }
    }
}

/// analyze the select target expr if it references the event outside of an aggregate function
/// rewrite what we can to group references
///
/// at a later stage we will only allow expressions with event references, if they are
/// also in the group by clause - so we can simply rewrite those to reference `group` and thus we dont need to copy.
pub(crate) struct TargetEventRefVisitor<'script, 'meta> {
    rewritten: bool,
    meta: &'meta NodeMetas,
    group_expressions: Vec<ImutExprInt<'script>>,
}

impl<'script, 'meta> TargetEventRefVisitor<'script, 'meta> {
    pub(crate) fn new(
        group_expressions: Vec<ImutExprInt<'script>>,
        meta: &'meta NodeMetas,
    ) -> Self {
        Self {
            rewritten: false,
            meta,
            group_expressions,
        }
    }

    pub(crate) fn rewrite_target(&mut self, target: &mut ImutExprInt<'script>) -> Result<bool> {
        self.walk_expr(target)?;
        Ok(self.rewritten)
    }
}
impl<'script, 'meta> ImutExprIntVisitor<'script> for TargetEventRefVisitor<'script, 'meta> {
    fn visit_expr(&mut self, e: &mut ImutExprInt<'script>) -> Result<VisitRes> {
        for (idx, group_expr) in self.group_expressions.iter().enumerate() {
            // check if we have an equivalent expression :)
            if e.ast_eq(group_expr) {
                // rewrite it:
                *e = ImutExprInt::Path(Path::Reserved(crate::ast::ReservedPath::Group {
                    mid: e.mid(),
                    segments: vec![crate::ast::Segment::Idx { mid: e.mid(), idx }],
                }));
                self.rewritten = true;
                // we do not need to visit this expression further, we already replaced it.
                return Ok(VisitRes::Stop);
            }
        }
        Ok(VisitRes::Walk)
    }
    fn visit_path(&mut self, path: &mut Path<'script>) -> Result<VisitRes> {
        match path {
            // these are the only exprs that can get a hold of the event payload or its metadata
            Path::Event(_) | Path::Meta(_) => {
                // fail if we see an event or meta ref in the select target
                return error_event_ref_not_allowed(path, path, self.meta);
            }
            _ => {}
        }
        Ok(VisitRes::Walk)
    }
}

pub(crate) struct GroupByExprExtractor<'script> {
    pub(crate) expressions: Vec<ImutExprInt<'script>>,
}

impl<'script> GroupByExprExtractor<'script> {
    pub(crate) fn new() -> Self {
        Self {
            expressions: vec![],
        }
    }

    pub(crate) fn extract_expressions(&mut self, group_by: &GroupBy<'script>) {
        self.walk_group_by(&group_by.0);
    }
}

impl<'script> GroupByVisitor<'script> for GroupByExprExtractor<'script> {
    fn visit_expr(&mut self, expr: &ImutExprInt<'script>) {
        self.expressions.push(expr.clone()); // take this, lifetimes (yes, i am stupid)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::ast::Expr;
    use crate::errors::Result;
    use crate::path::ModulePath;
    use crate::registry::registry;
    use simd_json::Value;

    #[derive(Default)]
    struct Find42Visitor {
        found: usize,
    }

    impl<'script> ImutExprIntVisitor<'script> for Find42Visitor {
        fn visit_literal(&mut self, literal: &mut Literal<'script>) -> Result<VisitRes> {
            if let Some(42) = literal.value.as_u64() {
                self.found += 1;
                return Ok(VisitRes::Stop);
            }
            Ok(VisitRes::Walk)
        }
    }

    fn test_walk<'script>(script: &'script str, expected_42s: usize) -> Result<()> {
        let module_path = ModulePath::load();
        let mut registry = registry();
        crate::std_lib::load(&mut registry);
        let script_script: crate::script::Script =
            crate::script::Script::parse(&module_path, "test", script.to_owned(), &registry)?;
        let script: &crate::ast::Script = script_script.script.suffix();
        let mut imut_expr = script
            .exprs
            .iter()
            .filter_map(|e| {
                if let Expr::Imut(expr) = e {
                    Some(expr)
                } else {
                    None
                }
            })
            .cloned()
            .last()
            .unwrap();
        let mut visitor = Find42Visitor::default();
        visitor.walk_expr(&mut imut_expr)?;
        assert_eq!(
            expected_42s, visitor.found,
            "Did not find {} 42s in {:?}, only {}",
            expected_42s, imut_expr, visitor.found
        );
        Ok(())
    }

    #[test]
    fn test_visitor_walking() -> Result<()> {
        test_walk(
            r#"
            fn hide_the_42(x) with
                x + 1
            end;
            hide_the_42(
                match event.foo of
                  case %{ field == " #{42 + event.foo} ", present foo, absent bar } => event.bar
                  case %[42] => event.snake
                  case a = %(42, ...) => a
                  default => event.snot
                end
            );
        "#,
            3,
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
            6,
        )
    }

    #[test]
    fn test_walk_comprehension() -> Result<()> {
        test_walk(
            r#"
            (for group[0] of
              case (i, e) =>
                42 + i
            end
            )
        "#,
            1,
        )
    }

    #[test]
    fn test_group_expr_extractor() -> Result<()> {
        let mut visitor = GroupByExprExtractor::new();
        let lit_42 = ImutExprInt::Literal(Literal {
            mid: 3,
            value: tremor_value::Value::from(42),
        });
        let false_array = ImutExprInt::List(List {
            mid: 5,
            exprs: vec![crate::ast::ImutExpr(ImutExprInt::Literal(Literal {
                mid: 6,
                value: tremor_value::Value::from(false),
            }))],
        });
        let group_by = GroupBy(GroupByInt::Set {
            mid: 1,
            items: vec![
                GroupBy(GroupByInt::Expr {
                    mid: 2,
                    expr: lit_42.clone(),
                }),
                GroupBy(GroupByInt::Each {
                    mid: 4,
                    expr: false_array.clone(),
                }),
            ],
        });
        visitor.extract_expressions(&group_by);
        assert_eq!(2, visitor.expressions.len());
        assert_eq!(&[lit_42, false_array], visitor.expressions.as_slice());
        Ok(())
    }
}
