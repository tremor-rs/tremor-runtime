use crate::{
    ast::{EmitExpr, Expr, Path},
    errors::Result,
    vm::{
        compiler::{Compilable, Compiler},
        Op,
    },
};

impl<'script> Compilable<'script> for Expr<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        match self {
            Expr::Match(m) => m.compile(compiler)?,
            Expr::IfElse(ie) => ie.compile(compiler)?,
            #[allow(clippy::cast_possible_truncation)]
            Expr::Assign { mid: _, path, expr } => {
                let Path::Local(p) = path else {
                    todo!("we only allow local pasth?");
                };
                if !p.segments.is_empty() {
                    todo!("we only allow non nested asignments pasth?");
                }
                expr.compile(compiler)?;
                compiler.max_locals = compiler.max_locals.max(p.idx);
                compiler.emit(Op::StoreLocal { idx: p.idx as u32 }, &p.mid);
            }
            Expr::AssignMoveLocal {
                mid: _,
                path: _,
                idx: _,
            } => {}
            Expr::Comprehension(_) => todo!(),
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
