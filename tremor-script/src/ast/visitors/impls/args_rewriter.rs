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

use super::super::prelude::*;
use crate::{ast::NodeMeta, Value};

/// Rewrites a path to `args` or an element of `args` inside a script/query
/// into an expression referencing the concrete `args` values
/// in order to not leak the current `args` into other scopes upon nesting modules or subqueries, all referencing `args`
pub(crate) struct ArgsRewriter<'script, 'registry, 'meta> {
    args: ImutExpr<'script>,
    helper: &'meta mut Helper<'script, 'registry>,
}

impl<'script, 'registry, 'meta> ArgsRewriter<'script, 'registry, 'meta> {
    pub(crate) fn new(args: Value<'script>, helper: &'meta mut Helper<'script, 'registry>) -> Self {
        let args: ImutExpr = Literal {
            mid: Box::new(NodeMeta::default()),
            value: args,
        }
        .into();
        Self { args, helper }
    }

    pub(crate) fn rewrite_expr(&mut self, expr: &mut ImutExpr<'script>) -> Result<()> {
        ImutExprWalker::walk_expr(self, expr)?;
        Ok(())
    }

    // pub(crate) fn rewrite_group_by(&mut self, group_by: &mut GroupBy<'script>) -> Result<()> {
    //     match group_by {
    //         GroupBy::Expr { expr, .. } | GroupBy::Each { expr, .. } => {
    //             self.rewrite_expr(expr)?;
    //         }
    //         GroupBy::Set { items, .. } => {
    //             for inner_group_by in items {
    //                 self.rewrite_group_by(inner_group_by)?;
    //             }
    //         }
    //     }
    //     Ok(())
    // }
}

impl<'script, 'registry, 'meta> ImutExprWalker<'script>
    for ArgsRewriter<'script, 'registry, 'meta>
{
}

impl<'script, 'registry, 'meta> ExprWalker<'script> for ArgsRewriter<'script, 'registry, 'meta> {}
impl<'script, 'registry, 'meta> QueryWalker<'script> for ArgsRewriter<'script, 'registry, 'meta> {}

impl<'script, 'registry, 'meta> ImutExprVisitor<'script>
    for ArgsRewriter<'script, 'registry, 'meta>
{
    fn visit_path(&mut self, path: &mut Path<'script>) -> Result<VisitRes> {
        if let Path::Reserved(ReservedPath::Args { segments, mid }) = path {
            // FIXME: add mid to args here
            let new = ExprPath {
                expr: Box::new(self.args.clone()),
                segments: segments.clone(),
                mid: mid.clone(),
                var: self.helper.reserve_shadow(),
            };
            *path = Path::Expr(new);
            self.helper.end_shadow_var();
        }
        Ok(VisitRes::Walk)
    }
}

impl<'script, 'registry, 'meta> ExprVisitor<'script> for ArgsRewriter<'script, 'registry, 'meta> {}
impl<'script, 'registry, 'meta> QueryVisitor<'script> for ArgsRewriter<'script, 'registry, 'meta> {}
