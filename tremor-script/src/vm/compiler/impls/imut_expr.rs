use tremor_value::Value;

use crate::{
    ast::{
        raw::{BytesDataType, Endian},
        BaseExpr, BinOpKind, BooleanBinExpr, BooleanBinOpKind, BytesPart, ClausePreCondition,
        Field, ImutExpr, List, Merge, Patch, PatchOperation, Pattern, Record, Segment,
        StrLitElement, StringLit,
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
            ImutExpr::Comprehension(_) => todo!(),
            ImutExpr::Merge(m) => m.compile(compiler)?,
            ImutExpr::Path(p) => p.compile(compiler)?,
            ImutExpr::String(s) => s.compile(compiler)?,
            #[allow(clippy::cast_possible_truncation)]
            ImutExpr::Local { idx, mid } => {
                compiler.max_locals = compiler.max_locals.max(idx);
                compiler.emit(Op::LoadLocal { idx: idx as u32 }, &mid);
            }
            ImutExpr::Literal(l) => {
                compiler.emit_const(l.value, &l.mid);
            }
            ImutExpr::Present {
                path: _path,
                mid: _mid,
            } => todo!(),
            ImutExpr::Invoke1(_) => todo!(),
            ImutExpr::Invoke2(_) => todo!(),
            ImutExpr::Invoke3(_) => todo!(),
            ImutExpr::Invoke(_) => todo!(),
            ImutExpr::InvokeAggr(_) => todo!(),
            ImutExpr::Recur(_) => todo!(),
            #[allow(clippy::cast_possible_truncation)]
            ImutExpr::Bytes(b) => {
                let size = b.value.len() as u32;
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
            #[allow(clippy::cast_possible_truncation)]
            ImutExpr::ArrayAppend(a) => {
                let size = a.right.len() as u32;
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
    #[allow(clippy::cast_possible_truncation)]
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let size = self.elements.len() as u32;
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
            PatchOperation::Merge { ident, expr, mid } => todo!(),
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
            PatchOperation::DefaultRecord { expr, mid } => todo!(),
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
            #[allow(clippy::cast_possible_truncation)]
            Segment::Idx { idx, mid } => {
                if let Ok(idx) = u32::try_from(idx) {
                    compiler.emit(Op::IndexFast { idx }, &mid);
                } else {
                    compiler.emit_const(idx, &mid);
                    compiler.emit(Op::Index, &mid);
                }
            }
            #[allow(clippy::cast_possible_truncation)]
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
                if r.fields.is_empty() {
                } else {
                    todo!()
                }
                compiler.set_jump_target(dst);
            }
            Pattern::Array(a) => {
                let mid = *a.mid;
                compiler.emit(Op::TestIsArray, &mid);
                let dst = compiler.new_jump_point();
                compiler.emit(Op::JumpFalse { dst }, &mid);
                if a.exprs.is_empty() {
                } else {
                    todo!()
                }
                compiler.set_jump_target(dst);
            }
            Pattern::Expr(e) => {
                let mid = e.meta().clone();
                e.compile(compiler)?;
                compiler.emit(Op::Binary { op: BinOpKind::Eq }, &mid);
                compiler.emit(Op::LoadRB, &NodeMeta::dummy());
            }

            Pattern::Assign(_) => todo!(),
            Pattern::Tuple(_) => todo!(),
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
    #[allow(clippy::cast_possible_truncation)]
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let List { mid, exprs } = self;
        let size = exprs.len() as u32;
        for e in exprs {
            e.compile(compiler)?;
        }
        compiler.emit_const(Value::array(), &mid);
        compiler.emit(Op::Array { size }, &mid);
        Ok(())
    }
}
impl<'script> Compilable<'script> for Record<'script> {
    #[allow(clippy::cast_possible_truncation)]
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let Record { mid, base, fields } = self;
        let size = fields.len() as u32;
        for f in fields {
            f.compile(compiler)?;
        }
        compiler.emit_const(Value::from(base), &mid);
        compiler.emit(Op::Record { size }, &mid);

        Ok(())
    }
}
