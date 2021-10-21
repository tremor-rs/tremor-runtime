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
    fn expr(&mut self, _e: &mut Expr<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a comprehension
    ///
    /// # Errors
    /// if the walker function fails
    fn comprehension(
        &mut self,
        _comp: &mut Comprehension<'script, Expr<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `EmitExpr`
    ///
    /// # Errors
    /// if the walker function fails
    fn emit(&mut self, _emit: &mut EmitExpr<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `IfElse`
    ///
    /// # Errors
    /// if the walker function fails
    fn ifelse(&mut self, _mifelse: &mut IfElse<'script, Expr<'script>>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `DefaultCase`
    ///
    /// # Errors
    /// if the walker function fails
    fn default_case(&mut self, _mdefault: &mut DefaultCase<Expr<'script>>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `Match`
    ///
    /// # Errors
    /// if the walker function fails
    fn mmatch(&mut self, _mmatch: &mut Match<'script, Expr<'script>>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `ClauseGroup`
    ///
    /// # Errors
    /// if the walker function fails
    fn clause_group(
        &mut self,
        _group: &mut ClauseGroup<'script, Expr<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `PredicateClause`
    ///
    /// # Errors
    /// if the walker function fails
    fn predicate_clause(
        &mut self,
        _group: &mut PredicateClause<'script, Expr<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
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
    fn record(&mut self, _record: &mut Record<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a list
    ///
    /// # Errors
    /// if the walker function fails
    fn list(&mut self, _list: &mut List<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a binary
    ///
    /// # Errors
    /// if the walker function fails
    fn binary(&mut self, _binary: &mut BinExpr<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `UnaryExpr`
    ///
    /// # Errors
    /// if the walker function fails
    fn unary(&mut self, _unary: &mut UnaryExpr<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `Patch`
    ///
    /// # Errors
    /// if the walker function fails
    fn patch(&mut self, _patch: &mut Patch<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `PatchOperation`
    ///
    /// # Errors
    /// if the walker function fails
    fn patch_operation(&mut self, _patch: &mut PatchOperation<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a match expr
    ///
    /// # Errors
    /// if the walker function fails
    fn precondition(
        &mut self,
        _precondition: &mut super::ClausePreCondition<'script>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `Match`
    ///
    /// # Errors
    /// if the walker function fails
    fn mmatch(&mut self, _mmatch: &mut Match<'script, ImutExprInt>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `PredicateClause`
    ///
    /// # Errors
    /// if the walker function fails
    fn predicate_clause(
        &mut self,
        _predicate: &mut PredicateClause<'script, ImutExprInt<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `ClauseGroup`
    ///
    /// # Errors
    /// if the walker function fails
    fn clause_group(
        &mut self,
        _group: &mut ClauseGroup<'script, ImutExprInt<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `Pattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn match_pattern(&mut self, _pattern: &mut Pattern<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `RecordPattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn record_pattern(&mut self, _record_pattern: &mut RecordPattern<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `PredicatePattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn predicate_pattern(
        &mut self,
        _record_pattern: &mut PredicatePattern<'script>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `ArrayPattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn array_pattern(&mut self, _array_pattern: &mut ArrayPattern<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `TuplePattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn tuple_pattern(&mut self, _pattern: &mut TuplePattern<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `ArrayPredicatePattern`
    ///
    /// # Errors
    /// if the walker function fails
    fn array_predicate_pattern(
        &mut self,
        _pattern: &mut ArrayPredicatePattern<'script>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `TestExpr`
    ///
    /// # Errors
    /// if the walker function fails
    fn test_expr(&mut self, _expr: &mut TestExpr) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// visit a comprehension
    ///
    /// # Errors
    /// if the walker function fails
    fn comprehension(
        &mut self,
        _comp: &mut Comprehension<'script, ImutExprInt<'script>>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a merge expr
    ///
    /// # Errors
    /// if the walker function fails
    fn merge(&mut self, _merge: &mut Merge<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a merge expr
    ///
    /// # Errors
    /// if the walker function fails
    fn segment(&mut self, _segment: &mut Segment<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a path
    ///
    /// # Errors
    /// if the walker function fails
    fn path(&mut self, _path: &mut Path<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a cow
    ///
    /// # Errors
    /// if the walker function fails
    fn string_lit(&mut self, _cow: &mut beef::Cow<'script, str>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a string
    ///
    /// # Errors
    /// if the walker function fails
    fn string(&mut self, _string: &mut StringLit<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `StrLitElement`
    ///
    /// # Errors
    /// if the walker function fails
    fn string_element(&mut self, _string: &mut StrLitElement<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a local
    ///
    /// # Errors
    /// if the walker function fails
    fn local(&mut self, _local_idx: &mut usize) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a present expr
    ///
    /// # Errors
    /// if the walker function fails
    fn present(&mut self, _path: &mut Path<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit an invoke expr
    ///
    /// # Errors
    /// if the walker function fails
    fn invoke(&mut self, _invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit an invoke1 expr
    ///
    /// # Errors
    /// if the walker function fails
    fn invoke1(&mut self, _invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit an invoke2 expr
    ///
    /// # Errors
    /// if the walker function fails
    fn invoke2(&mut self, _invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit an invoke3 expr
    ///
    /// # Errors
    /// if the walker function fails
    fn invoke3(&mut self, _invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// visit an `invoke_aggr` expr
    ///
    /// # Errors
    /// if the walker function fails
    fn invoke_aggr(&mut self, _invoke_aggr: &mut InvokeAggr) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a recur expr
    ///
    /// # Errors
    /// if the walker function fails
    fn recur(&mut self, _recur: &mut Recur<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit bytes
    ///
    /// # Errors
    /// if the walker function fails
    fn bytes(&mut self, _bytes: &mut Bytes<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a literal
    ///
    /// # Errors
    /// if the walker function fails
    fn literal(&mut self, _literal: &mut Literal<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a generic `ImutExprInt` (this is called before the concrete `visit_*` method)
    ///
    /// # Errors
    /// if the walker function fails
    fn expr(&mut self, _e: &mut ImutExprInt<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `StatePath`
    ///
    /// # Errors
    /// if the walker function fails
    fn segments(&mut self, _e: &mut Vec<Segment<'script>>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// visit a `ExprPath`
    ///
    /// # Errors
    /// if the walker function fails
    fn expr_path(&mut self, _e: &mut ExprPath<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a const path (uses the `LocalPath` ast node)
    ///
    /// # Errors
    /// if the walker function fails
    fn const_path(&mut self, _e: &mut LocalPath<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `LocalPath`
    ///
    /// # Errors
    /// if the walker function fails
    fn local_path(&mut self, _e: &mut LocalPath<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `EventPath`
    ///
    /// # Errors
    /// if the walker function fails
    fn event_path(&mut self, _e: &mut EventPath<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `StatePath`
    ///
    /// # Errors
    /// if the walker function fails
    fn state_path(&mut self, _e: &mut StatePath<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// visit a `MetadataPath`
    ///
    /// # Errors
    /// if the walker function fails
    fn meta_path(&mut self, _e: &mut MetadataPath<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
    /// visit a `ReservedPath`
    ///
    /// # Errors
    /// if the walker function fails
    fn reserved_path(&mut self, _e: &mut ReservedPath<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }
}

/// Visitor for `GroupBy`
pub(crate) trait GroupByVisitor<'script> {
    /// visits an expression
    fn visit_expr(&mut self, expr: &ImutExprInt<'script>);

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
        fn literal(&mut self, literal: &mut Literal<'script>) -> Result<VisitRes> {
            if let Some(42) = literal.value.as_u64() {
                self.found += 1;
                return Ok(VisitRes::Stop);
            }
            Ok(VisitRes::Walk)
        }
    }

    fn test_walk<'script>(script: &'script str, expected_42s: usize) -> Result<()> {
        test_walk_imut(script, expected_42s)?;
        test_walk_mut(script, expected_42s)?;
        Ok(())
    }

    fn test_walk_imut<'script>(script: &'script str, expected_42s: usize) -> Result<()> {
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
            .unwrap()
            .into_static();
        let mut visitor = Find42Visitor::default();
        ImutExprWalker::walk_expr(&mut visitor, &mut imut_expr)?;
        assert_eq!(
            expected_42s, visitor.found,
            "Did not find {} 42s in {:?}, only {}",
            expected_42s, imut_expr, visitor.found
        );
        Ok(())
    }

    fn test_walk_mut<'script>(script: &'script str, expected_42s: usize) -> Result<()> {
        let module_path = ModulePath::load();
        let mut registry = registry();
        crate::std_lib::load(&mut registry);
        let script_script: crate::script::Script =
            crate::script::Script::parse(&module_path, "test", script.to_owned(), &registry)?;
        let script: &crate::ast::Script = script_script.script.suffix();
        let mut imut_expr = script.exprs.last().cloned().unwrap().into_static();
        let mut visitor = Find42Visitor::default();
        ExprWalker::walk_expr(&mut visitor, &mut imut_expr)?;
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
}
