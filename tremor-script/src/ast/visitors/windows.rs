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

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct OnlyMutState {}

impl OnlyMutState {
    fn validate_path(path: &Path) -> Result<()> {
        match path {
            Path::Event(_) => Err("Can not assign to `event` here".into()),
            Path::Meta(_) => Err("Can not assign to `$` here".into()),
            Path::Local(_) | Path::Expr(_) | Path::State(_) | Path::Reserved(_) => Ok(()),
        }
    }
    // Validates that only state is every mutated
    pub fn validate(e: &mut Expr) -> Result<()> {
        let mut v = Self::default();
        ExprWalker::walk_expr(&mut v, e)
    }
}

impl<'script> ExprWalker<'script> for OnlyMutState {}
impl<'script> ImutExprWalker<'script> for OnlyMutState {}

impl<'script> ExprVisitor<'script> for OnlyMutState {
    fn visit_expr(&mut self, e: &mut Expr<'script>) -> Result<VisitRes> {
        if let Expr::Assign { path, .. } = e {
            Self::validate_path(path)?;
        }
        Ok(VisitRes::Walk)
    }
}
impl<'script> ImutExprVisitor<'script> for OnlyMutState {}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct NoEventAccess {}

impl NoEventAccess {
    // Validates that only state is every mutated
    pub fn validate(e: &mut Expr) -> Result<()> {
        let mut v = Self::default();
        ExprWalker::walk_expr(&mut v, e)
    }
}

impl<'script> ExprWalker<'script> for NoEventAccess {}
impl<'script> ImutExprWalker<'script> for NoEventAccess {}

impl<'script> ExprVisitor<'script> for NoEventAccess {}
impl<'script> ImutExprVisitor<'script> for NoEventAccess {
    fn visit_path(&mut self, p: &mut Path<'script>) -> Result<VisitRes> {
        match p {
            Path::Event(_) => Err("`event` isn't accessible in this context".into()),
            Path::Meta(_) => Err("`$` isn't accessible in this context".into()),
            Path::Local(_) | Path::State(_) | Path::Expr(_) | Path::Reserved(_) => {
                Ok(VisitRes::Walk)
            }
        }
    }
}
