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

/// deployments
pub mod deploy;
/// expressions
pub mod expr;
mod impls;
/// imutable expressions
pub mod imut_expr;
/// queries
pub mod query;

pub use impls::args_rewriter::ArgsRewriter;
pub use impls::array_addition_optimizer::ArrayAdditionOptimizer;
pub use impls::const_folder::ConstFolder;
pub(crate) use impls::group_by_extractor::GroupByExprExtractor;
pub(crate) use impls::is_const::IsConstFn;
pub(crate) use impls::target_event_ref::TargetEventRef;

pub(crate) use deploy::Visitor as DeployVisitor;
pub(crate) use expr::Visitor as ExprVisitor;
pub(crate) use imut_expr::Visitor as ImutExprVisitor;
pub(crate) use query::Visitor as QueryVisitor;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
/// Return value from visit methods for `ImutExprIntVisitor`
/// controlling whether to continue walking the subtree or not
pub enum VisitRes {
    /// carry on walking
    Walk,
    /// stop walking
    Stop,
}

/// Visitor for `GroupBy`
pub(crate) trait GroupByVisitor<'script> {
    fn visit_expr(&mut self, _expr: &ImutExpr<'script>);

    /// Walks an expression
    fn walk_group_by(&mut self, group_by: &GroupBy<'script>) {
        match group_by {
            GroupBy::Expr { expr, .. } | GroupBy::Each { expr, .. } => self.visit_expr(expr),
            GroupBy::Set { items, .. } => {
                for inner_group_by in items {
                    self.walk_group_by(inner_group_by);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::ast::walkers::ExprWalker;
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
        let mut registry = registry();
        crate::std_lib::load(&mut registry);
        let script_script: crate::script::Script = crate::script::Script::parse(input, &registry)?;
        let script: &crate::ast::Script = &script_script.script;
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
              default => let a = 7; let b = 9; event + a + b
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
                default => {"snot": 42 - zero};
                default "key" => {"snot": 42 +  zero};
                insert "snot" => 42;
                merge => {"snot": 42 - zero};
                merge "badger" => {"snot": 42 - zero};
                upsert "snot" => 42;
                copy "snot" => "snotter";
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
                  let event = 42 + i;
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
