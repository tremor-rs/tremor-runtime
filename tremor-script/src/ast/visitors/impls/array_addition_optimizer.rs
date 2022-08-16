use crate::ast::visitors::{ExprVisitor, ImutExprVisitor, QueryVisitor};
use crate::ast::walkers::{ExprWalker, ImutExprWalker, QueryWalker};
use crate::ast::{BaseExpr, BinOpKind, ImutExpr, List, Literal};
use tremor_value::Value;

/// Optimizes array addition
pub struct ArrayAdditionOptimizer {}

impl<'script> ImutExprWalker<'script> for ArrayAdditionOptimizer {}

impl<'script> ExprVisitor<'script> for ArrayAdditionOptimizer {}
impl<'script> ExprWalker<'script> for ArrayAdditionOptimizer {}

impl<'script> QueryVisitor<'script> for ArrayAdditionOptimizer {}

impl<'script> QueryWalker<'script> for ArrayAdditionOptimizer {}

impl<'script> ImutExprVisitor<'script> for ArrayAdditionOptimizer {
    fn leave_expr(&mut self, e: &mut ImutExpr<'script>) -> crate::Result<()> {
        let mut buf = ImutExpr::Literal(Literal {
            value: Value::const_null(),
            mid: Box::new(e.meta().clone()),
        });
        std::mem::swap(&mut buf, e);

        *e = match buf {
            ImutExpr::Binary(binary) if binary.kind == BinOpKind::Add => {
                if let ImutExpr::List(List { exprs, mid: _ }) = binary.rhs {
                    ImutExpr::ArrayAppend {
                        left: Box::new(binary.lhs),
                        right: exprs,
                        mid: binary.mid,
                    }
                } else {
                    ImutExpr::Binary(binary)
                }
            }
            other => other,
        };

        Ok(())
    }
}
