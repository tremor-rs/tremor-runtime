use tremor_value::Value;

use crate::{
    ast::{
        raw::{BytesDataType, Endian},
        ArrayPredicatePattern, BaseExpr, BinOpKind, BooleanBinExpr, BooleanBinOpKind, BytesPart,
        ClausePreCondition, Field, ImutExpr, Invoke, List, Merge, Patch, PatchOperation, Pattern,
        PredicatePattern, Record, Segment, StrLitElement, StringLit,
    },
    errors::Result,
    vm::{
        compiler::{Compilable, Compiler},
        Op,
    },
    NodeMeta,
};

#[allow(clippy::too_many_lines)]
impl<'script> Compilable<'script> for ImutExpr<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        match self {
            ImutExpr::Record(r) => r.compile(compiler)?,
            ImutExpr::List(l) => l.compile(compiler)?,
            ImutExpr::Binary(b) => {
                b.lhs.compile(compiler)?;
                b.rhs.compile(compiler)?;
                compiler.emit(Op::Binary { op: b.kind }, &b.mid);
            }
            ImutExpr::BinaryBoolean(b) => {
                b.compile(compiler)?;
            }
            ImutExpr::Unary(u) => {
                u.expr.compile(compiler)?;
                compiler.emit(Op::Unary { op: u.kind }, &u.mid);
            }
            ImutExpr::Patch(p) => p.compile(compiler)?,
            ImutExpr::Match(m) => m.compile(compiler)?,
            ImutExpr::Comprehension(c) => c.compile(compiler)?,
            ImutExpr::Merge(m) => m.compile(compiler)?,
            ImutExpr::Path(p) => p.compile(compiler)?,
            ImutExpr::String(s) => s.compile(compiler)?,
            ImutExpr::Local { idx, mid } => {
                compiler.max_locals = compiler.max_locals.max(idx);
                compiler.emit(
                    Op::LoadLocal {
                        idx: u32::try_from(idx)?,
                    },
                    &mid,
                );
            }
            ImutExpr::Literal(l) => {
                compiler.emit_const(l.value, &l.mid);
            }
            ImutExpr::Present {
                path: _path,
                mid: _mid,
            } => todo!(),
            ImutExpr::Invoke1(i) => i.compile(compiler)?,
            ImutExpr::Invoke2(i) => i.compile(compiler)?,
            ImutExpr::Invoke3(i) => i.compile(compiler)?,
            ImutExpr::Invoke(i) => i.compile(compiler)?,
            ImutExpr::InvokeAggr(_) => todo!(),
            ImutExpr::Recur(_) => todo!(),
            ImutExpr::Bytes(b) => {
                let size = u32::try_from(b.value.len())?;
                let mid = b.mid;
                // we modify r1 in the parts so we need to store it
                compiler.emit(Op::StoreV1, &mid);
                for b in b.value {
                    b.compile(compiler)?;
                }
                compiler.emit(Op::Bytes { size }, &mid);
                // once the bytes are crated we restore it
                compiler.emit(Op::LoadV1, &mid);
            }
            ImutExpr::ArrayAppend(a) => {
                let size = u32::try_from(a.right.len())?;
                for r in a.right {
                    r.compile(compiler)?;
                }
                a.left.compile(compiler)?;
                compiler.emit(Op::Array { size }, &a.mid);
            }
        }
        Ok(())
    }
}

impl<'script> Compilable<'script> for Field<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        self.name.compile(compiler)?;
        self.value.compile(compiler)?;
        Ok(())
    }
}

impl<'script> Compilable<'script> for StringLit<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let size = u32::try_from(self.elements.len())?;
        for e in self.elements {
            match e {
                StrLitElement::Lit(s) => {
                    compiler.emit_const(s, &self.mid);
                }
                StrLitElement::Expr(e) => {
                    e.compile(compiler)?;
                }
            }
        }
        compiler.emit(Op::String { size }, &self.mid);
        Ok(())
    }
}

impl<'script> Compilable<'script> for PatchOperation<'script> {
    #[allow(unused_variables)]
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        match self {
            PatchOperation::Insert { ident, expr, mid } => {
                ident.compile(compiler)?;
                compiler.emit(Op::TestRecortPresent, &mid);
                let dst = compiler.new_jump_point();
                compiler.emit(Op::JumpFalse { dst }, &mid);
                compiler.emit_const("Key already present", &mid);
                compiler.emit(Op::Error, &mid);
                compiler.set_jump_target(dst);
                expr.compile(compiler)?;
                compiler.emit(Op::RecordSet, &mid);
            }
            PatchOperation::Upsert { ident, expr, mid } => {
                ident.compile(compiler)?;
                expr.compile(compiler)?;
                compiler.emit(Op::RecordSet, &mid);
            }
            PatchOperation::Update { ident, expr, mid } => {
                ident.compile(compiler)?;
                compiler.emit(Op::TestRecortPresent, &mid);
                let dst = compiler.new_jump_point();
                compiler.emit(Op::JumpTrue { dst }, &mid);
                compiler.emit_const("Key already present", &mid);
                compiler.emit(Op::Error, &mid);
                compiler.set_jump_target(dst);
                expr.compile(compiler)?;
                compiler.emit(Op::RecordSet, &mid);
            }
            PatchOperation::Erase { ident, mid } => {
                ident.compile(compiler)?;
                compiler.emit(Op::RecordRemove, &mid);
                compiler.emit(Op::Pop, &mid);
            }
            PatchOperation::Copy { from, to, mid } => {
                from.compile(compiler)?;
                compiler.emit(Op::RecordGet, &mid);
                to.compile(compiler)?;
                compiler.emit(Op::Swap, &mid);
                compiler.emit(Op::RecordSet, &mid);
            }
            PatchOperation::Move { from, to, mid } => {
                from.compile(compiler)?;
                compiler.emit(Op::RecordRemove, &mid);
                to.compile(compiler)?;
                compiler.emit(Op::Swap, &mid);
                compiler.emit(Op::RecordSet, &mid);
            }
            PatchOperation::Merge { ident, expr, mid } => {
                expr.compile(compiler)?;
                ident.compile(compiler)?;
                compiler.emit(Op::RecordMergeKey, &mid);
            }
            PatchOperation::MergeRecord { expr, mid } => {
                expr.compile(compiler)?;
                compiler.emit(Op::RecordMerge, &mid);
            }
            PatchOperation::Default { ident, expr, mid } => {
                ident.compile(compiler)?;
                compiler.emit(Op::TestRecortPresent, &mid);
                let dst = compiler.new_jump_point();
                compiler.emit(Op::JumpTrue { dst }, &mid);
                expr.compile(compiler)?;
                compiler.emit(Op::RecordSet, &mid);
                compiler.set_jump_target(dst);
            }
            PatchOperation::DefaultRecord { expr, mid } => {
                expr.compile(compiler)?;
                let empty_dst = compiler.new_jump_point();
                let start_dst = compiler.new_jump_point();
                let skip_dst = compiler.new_jump_point();

                // start of the loop
                compiler.set_jump_target(start_dst);
                // pull the record from the stack in v1 to test for empy
                // FIXME: this is not great
                compiler.emit(Op::SwapV1, &mid);
                compiler.emit(Op::TestRecordIsEmpty, &mid);
                // return the original v1
                compiler.emit(Op::SwapV1, &mid);
                // if the record is empty we are done
                compiler.emit(Op::JumpTrue { dst: empty_dst }, &mid);
                // take the top record
                compiler.emit(Op::RecordPop, &mid);

                // Test if the key is present
                compiler.emit(Op::TestRecortPresent, &mid);
                // if it is we don't need to set r
                compiler.emit(Op::JumpTrue { dst: skip_dst }, &mid);
                // we insert the missing key
                // ne need to swap the elements as RecordSewt expects the value on top of the stack
                compiler.emit(Op::Swap, &mid);
                compiler.emit(Op::RecordSet, &mid);
                // next itteration
                compiler.emit(Op::Jump { dst: start_dst }, &mid);

                // If we skip we need to pop the key and value
                compiler.set_jump_target(skip_dst);
                compiler.emit(Op::Pop, &mid);
                compiler.emit(Op::Pop, &mid);
                // next itteration
                compiler.emit(Op::Jump { dst: start_dst }, &mid);
                compiler.set_jump_target(empty_dst);
            }
        }
        Ok(())
    }
}

impl<'script> Compilable<'script> for Segment<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        match self {
            Segment::Id { mid, key } => {
                let key = compiler.add_key(key);
                compiler.emit(Op::GetKey { key }, &mid);
            }
            Segment::Element { expr, mid } => {
                expr.compile(compiler)?;
                compiler.emit(Op::Get, &mid);
            }
            Segment::Idx { idx, mid } => {
                if let Ok(idx) = u32::try_from(idx) {
                    compiler.emit(Op::IndexFast { idx }, &mid);
                } else {
                    compiler.emit_const(idx, &mid);
                    compiler.emit(Op::Index, &mid);
                }
            }
            Segment::Range { mid, start, end } => {
                if let Some((start, end)) = u16::try_from(start).ok().zip(u16::try_from(end).ok()) {
                    compiler.emit(Op::RangeFast { start, end }, &mid);
                } else {
                    compiler.emit_const(start, &mid);
                    compiler.emit_const(end, &mid);
                    compiler.emit(Op::Range, &mid);
                }
            }

            Segment::RangeExpr { mid, start, end } => {
                start.compile(compiler)?;
                end.compile(compiler)?;
                compiler.emit(Op::Range, &mid);
            }
        }
        Ok(())
    }
}

impl<'script> Compilable<'script> for BooleanBinExpr<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let mid = self.mid.clone();
        self.compile_to_b(compiler)?;
        compiler.emit(Op::StoreRB, &mid);
        Ok(())
    }
    fn compile_to_b(self, compiler: &mut Compiler<'script>) -> Result<()> {
        self.lhs.compile_to_b(compiler)?;
        match self.kind {
            #[allow(clippy::cast_sign_loss)]
            BooleanBinOpKind::Or => {
                let dst = compiler.new_jump_point();
                compiler.emit(Op::JumpTrue { dst }, &self.mid);
                self.rhs.compile_to_b(compiler)?;
                compiler.set_jump_target(dst);
            }
            BooleanBinOpKind::Xor => {
                self.rhs.compile(compiler)?;
                compiler.emit(Op::Xor, &self.mid);
            }
            #[allow(clippy::cast_sign_loss)]
            BooleanBinOpKind::And => {
                let dst = compiler.new_jump_point();
                compiler.emit(Op::JumpFalse { dst }, &self.mid);
                self.rhs.compile_to_b(compiler)?;
                compiler.set_jump_target(dst);
            }
        }
        Ok(())
    }
}

impl From<Endian> for u8 {
    fn from(v: Endian) -> Self {
        match v {
            Endian::Big => 0,
            Endian::Little => 1,
        }
    }
}

impl From<u8> for Endian {
    fn from(v: u8) -> Self {
        match v {
            0 => Endian::Big,
            _ => Endian::Little,
        }
    }
}

impl From<BytesDataType> for u8 {
    fn from(v: BytesDataType) -> Self {
        match v {
            BytesDataType::SignedInteger => 0,
            BytesDataType::UnsignedInteger => 1,
            BytesDataType::Binary => 2,
        }
    }
}
impl From<u8> for BytesDataType {
    fn from(v: u8) -> Self {
        match v {
            0 => BytesDataType::SignedInteger,
            1 => BytesDataType::UnsignedInteger,
            _ => BytesDataType::Binary,
        }
    }
}

impl<'script> Compilable<'script> for BytesPart<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let BytesPart {
            mid,
            data,
            data_type,
            endianess,
            bits,
        }: BytesPart = self;

        // format is bits:48 |  data_type:2 |endianess:1
        let mut format = bits;
        format = format << 2 | u64::from(u8::from(data_type)) & 0b11;
        format = format << 1 | u64::from(u8::from(endianess)) & 0b1;

        data.compile(compiler)?;
        let dst = compiler.new_jump_point();
        // load the value into r1 so we can test it
        compiler.emit(Op::LoadV1, &mid);
        match data_type {
            BytesDataType::SignedInteger => compiler.emit(Op::TestIsI64, &mid),
            BytesDataType::UnsignedInteger => compiler.emit(Op::TestIsU64, &mid),
            BytesDataType::Binary => compiler.emit(Op::TestIsBytes, &mid),
        }
        compiler.emit(Op::JumpFalse { dst }, &mid);
        compiler.emit_const("invalid type conversion for binary", &mid);
        compiler.emit(Op::Error, &mid);
        compiler.set_jump_target(dst);
        // save the value back to the stack
        compiler.emit(Op::StoreV1, &mid);
        compiler.emit_const(format, &mid);
        Ok(())
    }
}

impl<'script> Compilable<'script> for PredicatePattern<'script> {
    fn compile(self, _compiler: &mut Compiler<'script>) -> Result<()> {
        todo!()
    }
}
impl<'script> Compilable<'script> for ArrayPredicatePattern<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        match self {
            ArrayPredicatePattern::Expr(e) => {
                let mid = e.meta().clone();
                e.compile(compiler)?;
                compiler.emit(Op::Binary { op: BinOpKind::Eq }, &mid);
                compiler.emit(Op::LoadRB, &mid);
            }
            ArrayPredicatePattern::Tilde(_) => todo!(),
            ArrayPredicatePattern::Record(_) => todo!(),
            ArrayPredicatePattern::Ignore => todo!(),
        }
        Ok(())
    }
}

impl<'script> Compilable<'script> for Pattern<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        self.compile_to_b(compiler)
    }
    /// will return the match state in registers.B
    fn compile_to_b(self, compiler: &mut Compiler<'script>) -> Result<()> {
        match self {
            Pattern::Record(r) => {
                compiler.emit(Op::TestIsRecord, &r.mid);
                let dst = compiler.new_jump_point();
                compiler.emit(Op::JumpFalse { dst }, &r.mid);

                for f in r.fields {
                    f.compile(compiler)?;
                }

                compiler.set_jump_target(dst);
            }
            Pattern::Array(a) => {
                let mid = *a.mid;
                compiler.emit(Op::TestIsArray, &mid);
                let dst = compiler.new_jump_point();
                compiler.emit(Op::JumpFalse { dst }, &mid);
                for e in a.exprs {
                    e.compile(compiler)?;
                    todo!("we need to look at all the array elements :sob:")
                }
                compiler.set_jump_target(dst);
            }
            Pattern::Expr(e) => {
                let mid = e.meta().clone();
                e.compile(compiler)?;
                compiler.emit(Op::TestEq, &mid);
            }

            Pattern::Assign(_) => todo!(),
            Pattern::Tuple(t) => {
                compiler.comment("Tuple pattern");
                let mid = *t.mid;
                compiler.emit(Op::TestIsArray, &mid);
                let dst_next = compiler.new_jump_point();
                compiler.emit(Op::JumpFalse { dst: dst_next }, &mid);
                compiler.comment("Check if the array is long enough");
                compiler.emit(Op::InspectLen, &mid);
                compiler.emit_const(t.exprs.len(), &mid);

                if t.open {
                    compiler.emit(Op::Binary { op: BinOpKind::Gte }, &mid);
                } else {
                    compiler.emit(Op::Binary { op: BinOpKind::Eq }, &mid);
                }
                compiler.emit(Op::LoadRB, &mid);
                let end_and_pop = compiler.new_jump_point();

                compiler.emit(Op::JumpFalse { dst: dst_next }, &mid);

                compiler.comment("Save array for itteration and reverse it");
                compiler.emit(Op::CopyV1, &mid);
                compiler.emit(Op::ArrayReverse, &mid);
                for (i, e) in t.exprs.into_iter().enumerate() {
                    compiler.comment(&format!("Test tuple element {}", i));
                    compiler.emit(Op::ArrayPop, &mid);
                    e.compile(compiler)?;
                    compiler.comment("Jump on no match");
                    compiler.emit(Op::JumpFalse { dst: end_and_pop }, &mid);
                }
                // remove the array from the stack
                compiler.comment("Remove the array from the stack");
                compiler.set_jump_target(end_and_pop);
                compiler.emit(Op::Pop, &mid);
                compiler.set_jump_target(dst_next);
            }
            Pattern::Extract(_) => todo!(),
            Pattern::DoNotCare => {
                compiler.emit(Op::True, &NodeMeta::dummy());
                compiler.emit(Op::LoadRB, &NodeMeta::dummy());
            }
        }
        Ok(())
    }
}

impl<'script> Compilable<'script> for ClausePreCondition<'script> {
    fn compile(self, _compiler: &mut Compiler<'script>) -> Result<()> {
        todo!()
    }
    fn compile_to_b(self, _compiler: &mut Compiler<'script>) -> Result<()> {
        todo!()
    }
}

impl<'script> Compilable<'script> for Patch<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let Patch {
            mid,
            target,
            operations,
        } = self;
        // Save r1 to ensure we can restore it after the patch operation
        compiler.emit(Op::StoreV1, &mid);
        // compile the target
        target.compile(compiler)?;
        // load the target into the register
        compiler.emit(Op::LoadV1, &mid);
        for op in operations {
            op.compile(compiler)?;
        }
        // restore r1 and ensure the result is on top of the stack
        compiler.emit(Op::SwapV1, &mid);
        Ok(())
    }
}

impl<'script> Compilable<'script> for Merge<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let Merge { mid, target, expr } = self;
        // Save r1 to ensure we can restore it after the patch operation
        compiler.emit(Op::StoreV1, &mid);
        // compile the target
        target.compile(compiler)?;
        // load the target into the register
        compiler.emit(Op::LoadV1, &mid);
        expr.compile(compiler)?;
        compiler.emit(Op::RecordMerge, &mid);
        // restore r1 and ensure the result is on top of the stack
        compiler.emit(Op::SwapV1, &mid);
        Ok(())
    }
}

impl<'script> Compilable<'script> for List<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let List { mid, exprs } = self;
        let size = u32::try_from(exprs.len())?;
        for e in exprs {
            e.compile(compiler)?;
        }
        compiler.emit_const(Value::array(), &mid);
        compiler.emit(Op::Array { size }, &mid);
        Ok(())
    }
}
impl<'script> Compilable<'script> for Record<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let Record { mid, base, fields } = self;
        let size = u32::try_from(fields.len())?;
        for f in fields {
            f.compile(compiler)?;
        }
        compiler.emit_const(Value::from(base), &mid);
        compiler.emit(Op::Record { size }, &mid);

        Ok(())
    }
}

impl<'script> Compilable<'script> for Invoke<'script> {
    fn compile(self, _compiler: &mut Compiler<'script>) -> Result<()> {
        todo!()
    }
}
