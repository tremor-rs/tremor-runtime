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

pub(crate) struct ExprReducer<'script, 'registry, 'meta> {
    visits: u64,
    helper: &'meta mut Helper<'script, 'registry>,
}

impl<'script, 'registry, 'meta> ExprReducer<'script, 'registry, 'meta> {
    pub(crate) fn new(helper: &'meta mut Helper<'script, 'registry>) -> Self {
        Self { helper, visits: 0 }
    }

    pub(crate) fn reduce(&mut self, expr: &'meta mut ImutExprInt<'script>) -> Result<()> {
        // Counts the number of visits needed to walk the expr.
        self.walk_expr(expr)?;

        // TODO: This is slow
        let loops = self.visits;
        for _ in 1..loops {
            self.walk_expr(expr)?;
        }
        Ok(())
    }
}

impl<'script, 'registry, 'meta> ImutExprWalker<'script> for ExprReducer<'script, 'registry, 'meta> {}

impl<'script, 'registry, 'meta> ImutExprVisitor<'script>
    for ExprReducer<'script, 'registry, 'meta>
{
    fn expr(&mut self, e: &mut ImutExprInt<'script>) -> Result<VisitRes> {
        self.visits += 1;
        *e = e.clone().try_reduce(self.helper)?;
        Ok(VisitRes::Walk)
    }
}
