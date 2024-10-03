// Copyright 2020-2024, The Tremor Team
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
use crate::{
    ast::{EmitExpr, Expr, Path},
    errors::Result,
    vm::{
        compiler::{Compilable, Compiler},
        Op,
    },
};

use super::compile_segment_path;

impl<'script> Compilable<'script> for Expr<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        match self {
            Expr::Match(m) => m.compile(compiler)?,
            Expr::IfElse(ie) => ie.compile(compiler)?,
            Expr::Assign { mid: _, path, expr } => {
                compile_assign(compiler, path, *expr)?;
            }
            Expr::AssignMoveLocal {
                mid: _,
                path: _,
                idx: _,
            } => {}
            Expr::Comprehension(c) => c.compile(compiler)?,
            Expr::Drop { mid } => compiler.emit(Op::Drop, &mid),
            Expr::Emit(e) => e.compile(compiler)?,
            Expr::Imut(e) => e.compile(compiler)?,
        }
        Ok(())
    }
}

impl<'script> Compilable<'script> for EmitExpr<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        self.expr.compile(compiler)?;
        let dflt = if let Some(port) = self.port {
            port.compile(compiler)?;
            false
        } else {
            true
        };
        compiler.emit(Op::Emit { dflt }, &self.mid);
        Ok(())
    }
}

#[allow(clippy::cast_possible_truncation)]
fn compile_assign<'script>(
    compiler: &mut Compiler<'script>,
    mut path: Path<'script>,
    expr: Expr<'script>,
) -> Result<()> {
    expr.compile(compiler)?;
    let elements = u16::try_from(path.len())?;
    for s in path.segments_mut().drain(..) {
        compile_segment_path(compiler, s)?;
    }
    match path {
        Path::Local(p) => {
            compiler.max_locals = compiler.max_locals.max(p.idx);
            compiler.emit(
                Op::StoreLocal {
                    idx: u32::try_from(p.idx)?,
                    elements,
                },
                &p.mid,
            );
        }
        Path::Event(p) => {
            compiler.emit(Op::StoreEvent { elements }, &p.mid);
        }
        Path::State(_p) => todo!(),
        Path::Meta(_p) => todo!(),
        Path::Expr(_p) => todo!(),
        Path::Reserved(_p) => todo!(),
    }
    Ok(())
}
