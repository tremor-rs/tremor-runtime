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

use crate::ast::visitors::{DeployVisitor, ExprVisitor, ImutExprVisitor, QueryVisitor};
use crate::ast::walkers::{DeployWalker, ExprWalker, ImutExprWalker, QueryWalker};
use crate::ast::{ArrayAppend, BaseExpr, BinOpKind, ImutExpr, List, Literal};
use tremor_value::Value;

/// Optimizes array addition
pub struct ArrayAdditionOptimizer {}

impl<'script> DeployWalker<'script> for ArrayAdditionOptimizer {}
impl<'script> QueryWalker<'script> for ArrayAdditionOptimizer {}
impl<'script> ExprWalker<'script> for ArrayAdditionOptimizer {}
impl<'script> ImutExprWalker<'script> for ArrayAdditionOptimizer {}
impl<'script> DeployVisitor<'script> for ArrayAdditionOptimizer {}
impl<'script> QueryVisitor<'script> for ArrayAdditionOptimizer {}
impl<'script> ExprVisitor<'script> for ArrayAdditionOptimizer {}

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
                    ImutExpr::ArrayAppend(ArrayAppend {
                        left: Box::new(binary.lhs),
                        right: exprs,
                        mid: binary.mid,
                    })
                } else {
                    ImutExpr::Binary(binary)
                }
            }
            other => other,
        };

        Ok(())
    }
}
