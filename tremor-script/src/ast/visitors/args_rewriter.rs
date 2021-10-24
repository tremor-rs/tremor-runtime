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

use super::prelude::*;
use crate::Value;

pub(crate) struct ArgsRewriter<'script, 'registry, 'meta> {
    args: ImutExprInt<'script>,
    helper: &'meta mut Helper<'script, 'registry>,
}

impl<'script, 'registry, 'meta> ArgsRewriter<'script, 'registry, 'meta> {
    pub(crate) fn new(args: Value<'script>, helper: &'meta mut Helper<'script, 'registry>) -> Self {
        let args: ImutExpr = Literal {
            mid: 0,
            value: args,
        }
        .into();
        Self {
            args: args.0,
            helper,
        }
    }

    pub(crate) fn rewrite_expr(&mut self, expr: &mut ImutExprInt<'script>) -> Result<()> {
        self.walk_expr(expr)?;
        Ok(())
    }

    pub(crate) fn rewrite_group_by(&mut self, group_by: &mut GroupByInt<'script>) -> Result<()> {
        match group_by {
            GroupByInt::Expr { expr, .. } | GroupByInt::Each { expr, .. } => {
                self.rewrite_expr(expr)?;
            }
            GroupByInt::Set { items, .. } => {
                for inner_group_by in items {
                    self.rewrite_group_by(&mut inner_group_by.0)?;
                }
            }
        }
        Ok(())
    }
}

impl<'script, 'registry, 'meta> ImutExprWalker<'script>
    for ArgsRewriter<'script, 'registry, 'meta>
{
}

impl<'script, 'registry, 'meta> ImutExprVisitor<'script>
    for ArgsRewriter<'script, 'registry, 'meta>
{
    fn visit_path(&mut self, path: &mut Path<'script>) -> Result<VisitRes> {
        if let Path::Reserved(ReservedPath::Args { segments, mid }) = path {
            let new = ExprPath {
                expr: Box::new(self.args.clone()),
                segments: segments.clone(),
                mid: *mid,
                var: self.helper.reserve_shadow(),
            };
            *path = Path::Expr(new);
            self.helper.end_shadow_var();
        }
        Ok(VisitRes::Walk)
    }
}
