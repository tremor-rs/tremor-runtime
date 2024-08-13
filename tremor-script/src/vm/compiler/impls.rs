mod imut_expr;
mod mut_expr;

use crate::{
    ast::{
        ClauseGroup, Comprehension, DefaultCase, Expression, IfElse, Match, Path, PredicateClause,
        Script,
    },
    errors::Result,
    vm::{
        compiler::{Compilable, Compiler},
        Op,
    },
    NodeMeta,
};
impl<'script> Compilable<'script> for Script<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        for e in self.exprs {
            e.compile(compiler)?;
        }
        Ok(())
    }
}

impl<'script, Ex> Compilable<'script> for PredicateClause<'script, Ex>
where
    Ex: Expression + Compilable<'script>,
{
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let PredicateClause {
            mid,
            pattern,
            guard,
            exprs,
            last_expr,
        } = self;
        let end_dst = compiler.end_dst()?;
        let dst = compiler.new_jump_point();
        compiler.comment("Predicate Clause");
        compiler.emit(Op::CopyV1, &mid);
        pattern.compile_to_b(compiler)?;
        compiler.comment("Jump to the next pattern");
        compiler.emit(Op::JumpFalse { dst }, &mid);
        if let Some(guard) = guard {
            compiler.comment("Predicate Clause Guard");
            guard.compile_to_b(compiler)?;
            compiler.emit(Op::JumpFalse { dst }, &mid);
        }
        compiler.comment("Predicate Clause Body");
        for e in exprs {
            e.compile(compiler)?;
        }
        last_expr.compile(compiler)?;
        // we were successful so we jump to the end
        compiler.comment("Jump to the end of the matching statement since we were successful");
        compiler.emit(Op::Jump { dst: end_dst }, &mid);
        compiler.set_jump_target(dst);
        Ok(())
    }
}

impl<'script, Ex> Compilable<'script> for DefaultCase<Ex>
where
    Ex: Compilable<'script> + Expression,
{
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        match self {
            DefaultCase::None => {
                compiler.emit(Op::Nop, &NodeMeta::dummy());
            }
            DefaultCase::Null => {
                compiler.emit(Op::Null, &NodeMeta::dummy());
            }
            DefaultCase::Many { exprs, last_expr } => {
                for e in exprs {
                    e.compile(compiler)?;
                }
                last_expr.compile(compiler)?;
            }
            DefaultCase::One(e) => e.compile(compiler)?,
        }
        Ok(())
    }
}
impl<'script, Ex> Compilable<'script> for ClauseGroup<'script, Ex>
where
    Ex: Compilable<'script> + Expression,
{
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let next = compiler.new_jump_point();
        match self {
            ClauseGroup::Simple {
                precondition,
                patterns,
            } => {
                if let Some(precondition) = precondition {
                    compiler.comment("Match Group Preconditions");
                    precondition.compile_to_b(compiler)?;
                    compiler.comment("Jump to next case if precondition is false");
                    compiler.emit(Op::JumpFalse { dst: next }, &NodeMeta::dummy());
                    // FIXME
                }
                for p in patterns {
                    p.compile(compiler)?;
                }
            }
            ClauseGroup::SearchTree {
                precondition,
                tree: _,
                rest,
            } => {
                if let Some(precondition) = precondition {
                    compiler.comment("Match Tree Preconditions");
                    precondition.compile_to_b(compiler)?;
                    compiler.comment("Jump to next case if precondition is false");
                    compiler.emit(Op::JumpFalse { dst: next }, &NodeMeta::dummy());
                    // FIXME
                }
                for r in rest {
                    r.compile(compiler)?;
                }
                todo!("the tree has to go before therest!");
            }
            ClauseGroup::Combined {
                precondition,
                groups,
            } => {
                if let Some(precondition) = precondition {
                    compiler.comment("Match Combined Preconditions");
                    precondition.compile_to_b(compiler)?;
                    compiler.comment("Jump to next case if precondition is false");
                    compiler.emit(Op::JumpFalse { dst: next }, &NodeMeta::dummy());
                    // FIXME
                }
                for g in groups {
                    g.compile(compiler)?;
                }
            }
            ClauseGroup::Single {
                precondition,
                pattern,
            } => {
                if let Some(precondition) = precondition {
                    compiler.comment("Match Single Preconditions");
                    precondition.compile_to_b(compiler)?;
                    compiler.comment("Jump to next case if precondition is false");
                    compiler.emit(Op::JumpFalse { dst: next }, &NodeMeta::dummy());
                }
                pattern.compile(compiler)?;
            }
        }
        compiler.set_jump_target(next);
        Ok(())
    }
}

impl<'script, Ex> Compilable<'script> for Match<'script, Ex>
where
    Ex: Compilable<'script> + Expression,
{
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let Match {
            mid,
            target,
            patterns,
            default,
        } = self;
        compiler.comment("Match statement");
        compiler.new_end_target();
        // Save r1 to ensure we can restore it after the patch operation
        compiler.emit(Op::StoreV1, &mid);
        // Save r1 to ensure we can restore it after the patch operation
        target.compile(compiler)?;
        // load target to v1
        compiler.emit(Op::LoadV1, &mid);

        for c in patterns {
            c.compile(compiler)?;
        }
        default.compile(compiler)?;
        compiler.set_end_target()?;
        // restore r1 and ensure the result is on top of the stack
        // Load the result of the match into r1
        compiler.emit(Op::LoadV1, &mid);
        // restore r1 and ensure the result is on top of the stack
        compiler.emit(Op::SwapV1, &mid);
        Ok(())
    }
}

impl<'script, Ex> Compilable<'script> for IfElse<'script, Ex>
where
    Ex: Compilable<'script> + Expression,
{
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let IfElse {
            mid,
            target,
            if_clause,
            else_clause,
        }: IfElse<Ex> = self;
        // jump point for the else clause
        let else_dst = compiler.new_jump_point();
        // jump point for the end of the if expression
        compiler.new_end_target();
        // store v1
        compiler.emit(Op::StoreV1, &mid);

        // compile the target and store the result in the B1 register
        target.compile(compiler)?;
        // load target in register 1;
        compiler.emit(Op::LoadV1, &mid);
        // compile the if clause
        if_clause.compile(compiler)?;
        // this is the jump destionaion for the else clause
        compiler.set_jump_target(else_dst);
        else_clause.compile(compiler)?;
        // this is the jump destionaion for the end of the if expression
        compiler.set_end_target()?;
        // load the result in of the expression to v1 so we can restore the old value
        compiler.emit(Op::LoadV1, &mid);
        // restore original value
        compiler.emit(Op::SwapV1, &mid);

        Ok(())
    }
}

impl<'script> Compilable<'script> for Path<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        match self {
            Path::Local(p) => {
                compiler.max_locals = compiler.max_locals.max(p.idx);
                compiler.emit(
                    Op::LoadLocal {
                        idx: u32::try_from(p.idx)?,
                    },
                    &p.mid,
                );
                for s in p.segments {
                    s.compile(compiler)?;
                }
            }
            Path::Event(p) => {
                compiler.emit(Op::LoadEvent, &p.mid);
                for s in p.segments {
                    s.compile(compiler)?;
                }
            }
            Path::Expr(p) => {
                p.expr.compile(compiler)?;
                for s in p.segments {
                    s.compile(compiler)?;
                }
            }
            Path::Meta(_p) => todo!(),
            Path::Reserved(_p) => todo!(),
            Path::State(_p) => todo!(),
        }
        Ok(())
    }
}
impl<'script, Ex> Compilable<'script> for Comprehension<'script, Ex>
where
    Ex: Compilable<'script> + Expression,
{
    fn compile(self, _compiler: &mut Compiler<'script>) -> Result<()> {
        todo!()
    }
}
