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
use crate::errors::error_event_ref_not_allowed;

/// analyze the select target expr if it references the event outside of an aggregate function
/// rewrite what we can to group references
///
/// at a later stage we will only allow expressions with event references, if they are
/// also in the group by clause - so we can simply rewrite those to reference `group` and thus we dont need to copy.
pub(crate) struct TargetEventRef<'script, 'meta> {
    rewritten: bool,
    meta: &'meta NodeMetas,
    group_expressions: Vec<ImutExprInt<'script>>,
}

impl<'script, 'meta> TargetEventRef<'script, 'meta> {
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
impl<'script, 'meta> ImutExprWalker<'script> for TargetEventRef<'script, 'meta> {}
impl<'script, 'meta> ImutExprVisitor<'script> for TargetEventRef<'script, 'meta> {
    fn expr(&mut self, e: &mut ImutExprInt<'script>) -> Result<VisitRes> {
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
    fn path(&mut self, path: &mut Path<'script>) -> Result<VisitRes> {
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
