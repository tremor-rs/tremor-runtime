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

use crate::ast::visitors::prelude::*;

pub(crate) struct IsConstFn {
    is_const: bool,
}

impl Default for IsConstFn {
    fn default() -> Self {
        Self { is_const: true }
    }
}

impl IsConstFn {
    pub(crate) fn is_const(es: &mut Exprs) -> Result<bool> {
        let mut walker = Self::default();
        for e in es {
            walkers::expr::Walker::walk_expr(&mut walker, e)?;
        }
        Ok(walker.is_const)
    }
}

impl<'script> walkers::imut_expr::Walker<'script> for IsConstFn {}
impl<'script> visitors::imut_expr::Visitor<'script> for IsConstFn {
    fn visit_invoke(&mut self, invoke: &mut Invoke<'script>) -> Result<VisitRes> {
        self.is_const |= invoke.invocable.is_const();
        Ok(VisitRes::Walk)
    }
}

impl<'script> walkers::expr::Walker<'script> for IsConstFn {}
impl<'script> visitors::expr::Visitor<'script> for IsConstFn {}
