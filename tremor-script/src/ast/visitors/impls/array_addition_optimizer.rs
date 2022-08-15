use crate::ast::visitors::query::Visitor;
use crate::ast::visitors::{ExprVisitor, ImutExprVisitor, QueryVisitor};
use crate::ast::walkers::{ExprWalker, ImutExprWalker, QueryWalker};
use crate::ast::{BinOpKind, ImutExpr, List};

/// Optimizes array addition
pub struct ArrayAdditionOptimizer {}

impl<'script> ImutExprWalker<'script> for ArrayAdditionOptimizer {}

impl<'script> ExprVisitor<'script> for ArrayAdditionOptimizer {}
impl<'script, 'registry, 'meta> ExprWalker<'script> for ArrayAdditionOptimizer {}

impl<'script> QueryVisitor<'script> for ArrayAdditionOptimizer {}

impl<'script> QueryWalker<'script> for ArrayAdditionOptimizer {}

impl<'script, 'registry, 'meta> ImutExprVisitor<'script> for ArrayAdditionOptimizer {
    fn leave_expr(&mut self, e: &mut ImutExpr<'script>) -> crate::Result<()> {
        if let ImutExpr::Binary(binary) = e {
            if binary.kind != BinOpKind::Add {
                return Ok(());
            }

            if let ImutExpr::List(List { ref exprs, mid: _ }) = binary.rhs {
                // FIXME: get rid of the clones!
                *e = ImutExpr::ArrayAppend {
                    left: Box::new(binary.lhs.clone()),
                    right: exprs.clone(),
                    mid: binary.mid.clone(),
                };

                return Ok(());
            }
        }

        return Ok(());
    }
}
