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
    ast::{EmitExpr, Expr, Path, Segment},
    errors::{err_generic, Result},
    prelude::Ranged as _,
    vm::{
        compiler::{Compilable, Compiler},
        Op,
    },
    NodeMeta,
};

impl<'script> Compilable<'script> for Expr<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        match self {
            Expr::Match(m) => m.compile(compiler)?,
            Expr::IfElse(ie) => ie.compile(compiler)?,
            Expr::Assign { mid, path, expr } => {
                compile_assign(compiler, &mid, path, *expr)?;
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

fn compile_segment_path<'script>(
    compiler: &mut Compiler<'script>,
    segmetn: Segment<'script>,
) -> Result<()> {
    match segmetn {
        Segment::Id { mid, key } => compiler.emit_const(key.key().to_string(), &mid),
        Segment::Element { expr, mid: _ } => expr.compile(compiler)?,
        Segment::Idx { idx, mid } => compiler.emit_const(idx, &mid),
        Segment::Range { mid, .. } | Segment::RangeExpr { mid, .. } => {
            return err_generic(
                &mid.extent(),
                &mid.extent(),
                &"range segment can't be assigned",
            )
        }
    }
    Ok(())
}
#[allow(clippy::cast_possible_truncation)]
fn compile_assign<'script>(
    compiler: &mut Compiler<'script>,
    _mid: &NodeMeta,
    path: Path<'script>,
    expr: Expr<'script>,
) -> Result<()> {
    expr.compile(compiler)?;
    match path {
        Path::Local(p) => {
            let elements: u16 = u16::try_from(p.segments.len())?;
            for s in p.segments.into_iter().rev() {
                compile_segment_path(compiler, s)?;
            }
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
            let elements: u16 = p.segments.len() as u16;
            for s in p.segments.into_iter().rev() {
                compile_segment_path(compiler, s)?;
            }
            compiler.emit(Op::StoreEvent { elements }, &p.mid);
        }
        Path::State(_p) => todo!(),
        Path::Meta(_p) => todo!(),
        Path::Expr(_p) => todo!(),
        Path::Reserved(_p) => todo!(),
    }
    Ok(())
}
