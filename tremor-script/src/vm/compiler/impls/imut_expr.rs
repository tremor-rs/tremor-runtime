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
use tremor_value::Value;

use crate::{
    ast::{
        raw::{BytesDataType, Endian},
        ArrayPattern, ArrayPredicatePattern, AssignPattern, BaseExpr, BinOpKind, BooleanBinExpr,
        BooleanBinOpKind, BytesPart, ClausePreCondition, EventPath, Field, ImutExpr, Invoke,
        InvokeAggr, List, LocalPath, Merge, Patch, PatchOperation, Path, Pattern, PredicatePattern,
        Record, RecordPattern, Segment, StrLitElement, StringLit, TestExpr, TuplePattern,
    },
    errors::{err_generic, Result},
    vm::{
        compiler::{Compilable, Compiler},
        Op,
    },
    NodeMeta,
};

use super::compile_segment_path;

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
            ImutExpr::Present { path, mid } => {
                compile_present(compiler, &mid, path)?;
            }
            ImutExpr::Invoke1(i) => i.compile(compiler)?,
            ImutExpr::Invoke2(i) => i.compile(compiler)?,
            ImutExpr::Invoke3(i) => i.compile(compiler)?,
            ImutExpr::Invoke(i) => i.compile(compiler)?,
            ImutExpr::InvokeAggr(a) => a.compile(compiler)?,
            ImutExpr::Recur(_r) => todo!(),
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
    #[allow(unused_variables)]
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let key = compiler.add_key(self.key().clone());
        let mid = NodeMeta::dummy(); // FIXME
        compiler.comment("Test if the record contains the key");
        compiler.emit(Op::TestRecordContainsKey { key }, &mid);
        // handle the absent patter ndifferently
        if let PredicatePattern::FieldAbsent { key: _, lhs: _ } = self {
            compiler.emit(Op::NotRB, &mid);
        } else if let PredicatePattern::FieldPresent { key: _, lhs: _ } = self {
        } else {
            let dst = compiler.new_jump_point();
            compiler.emit(Op::JumpFalse { dst }, &mid);
            compiler.comment("Fetch key to test");
            compiler.emit(Op::GetKeyRegV1 { key }, &mid);
            compiler.comment("Swap value in the register");
            compiler.emit(Op::SwapV1, &mid);
            match self {
                PredicatePattern::TildeEq {
                    lhs: _,
                    key: _,
                    test,
                } => {
                    let dst = compiler.new_jump_point();
                    compiler.comment("Run tilde pattern");
                    test.compile(compiler)?;
                    compiler.emit(Op::JumpFalse { dst }, &mid);
                    compiler.comment("Swap the value of the tilde pattern and the stored register");
                    compiler.emit(Op::Swap, &mid);
                    compiler.set_jump_target(dst);
                }
                PredicatePattern::Bin {
                    lhs: _,
                    key: _,
                    rhs,
                    kind,
                } => {
                    compiler.comment("Run binary pattern");
                    rhs.compile(compiler)?;
                    match kind {
                        BinOpKind::Eq => compiler.emit(Op::TestEq, &mid),
                        BinOpKind::NotEq => compiler.emit(Op::TestNeq, &mid),
                        BinOpKind::Gte => compiler.emit(Op::TestGte, &mid),
                        BinOpKind::Gt => compiler.emit(Op::TestGt, &mid),
                        BinOpKind::Lte => compiler.emit(Op::TestLte, &mid),
                        BinOpKind::Lt => compiler.emit(Op::TestLt, &mid),
                        BinOpKind::BitXor
                        | BinOpKind::BitAnd
                        | BinOpKind::RBitShiftSigned
                        | BinOpKind::RBitShiftUnsigned
                        | BinOpKind::LBitShift
                        | BinOpKind::Add
                        | BinOpKind::Sub
                        | BinOpKind::Mul
                        | BinOpKind::Div
                        | BinOpKind::Mod => {
                            return Err(format!("Invalid operator {kind:?} in predicate").into());
                        }
                    }
                    compiler.comment("Remove the value from the stack");
                    compiler.emit(Op::Pop, &mid);
                }
                PredicatePattern::RecordPatternEq {
                    lhs,
                    key: _,
                    pattern,
                } => {
                    compiler.comment("Run record pattern");
                    pattern.compile(compiler)?;
                }
                PredicatePattern::ArrayPatternEq { lhs, key, pattern } => {
                    compiler.comment("Run array  pattern");
                    pattern.compile(compiler)?;
                }
                PredicatePattern::TuplePatternEq {
                    lhs,
                    key: _,
                    pattern,
                } => {
                    compiler.comment("Run tuple  pattern");
                    pattern.compile(compiler)?;
                }
                PredicatePattern::FieldPresent { .. } | PredicatePattern::FieldAbsent { .. } => {
                    unreachable!("FieldAbsent and  FieldPresent should be handled earlier");
                }
            }
            compiler.comment("Restore the register");
            compiler.emit(Op::LoadV1, &mid);
            compiler.set_jump_target(dst);
        }

        Ok(())
    }
}
impl<'script> Compilable<'script> for ArrayPredicatePattern<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        match self {
            ArrayPredicatePattern::Expr(e) => {
                let mid = e.meta().clone();
                e.compile(compiler)?;
                compiler.emit(Op::TestEq, &mid);
            }
            ArrayPredicatePattern::Tilde(p) => p.compile(compiler)?,
            ArrayPredicatePattern::Record(p) => p.compile(compiler)?,
            ArrayPredicatePattern::Ignore => compiler.emit(Op::TrueRB, &NodeMeta::dummy()),
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
            Pattern::Record(p) => p.compile(compiler)?,
            Pattern::Array(p) => p.compile(compiler)?,
            Pattern::Expr(e) => {
                let mid = e.meta().clone();
                e.compile(compiler)?;
                compiler.emit(Op::TestEq, &mid);
            }
            Pattern::Assign(p) => p.compile(compiler)?,
            Pattern::Tuple(p) => p.compile(compiler)?,
            Pattern::Extract(p) => p.compile(compiler)?,
            Pattern::DoNotCare => compiler.emit(Op::TrueRB, &NodeMeta::dummy()),
        }
        Ok(())
    }
}

impl<'script> Compilable<'script> for ClausePreCondition<'script> {
    fn compile_to_b(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let ClausePreCondition { mut path } = self;
        assert!(path.len() == 1);
        if let Some(Segment::Id { key, mid }) = path.segments_mut().pop() {
            let key = compiler.add_key(key);
            compiler.emit(Op::TestRecordContainsKey { key }, &mid);
            Ok(())
        } else {
            Err("Invalid path in pre condition".into())
        }
    }

    fn compile(self, _compiler: &mut Compiler<'script>) -> Result<()> {
        err_generic(
            &self.path,
            &self.path,
            &"Pre conditions are not supported in this context",
        )
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
impl<'script> Compilable<'script> for InvokeAggr {
    fn compile(self, _compiler: &mut Compiler<'script>) -> Result<()> {
        todo!()
    }
}

impl<'script> Compilable<'script> for RecordPattern<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let RecordPattern { mid, fields } = self;
        compiler.emit(Op::TestIsRecord, &mid);
        let dst = compiler.new_jump_point();
        compiler.emit(Op::JumpFalse { dst }, &mid);
        for f in fields {
            f.compile(compiler)?;
        }
        compiler.set_jump_target(dst);
        Ok(())
    }
}

impl<'script> Compilable<'script> for ArrayPattern<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let ArrayPattern { mid, exprs } = self;
        compiler.emit(Op::TestIsArray, &mid);
        let dst = compiler.new_jump_point();
        compiler.emit(Op::JumpFalse { dst }, &mid);
        for e in exprs {
            e.compile(compiler)?;
        }
        compiler.set_jump_target(dst);
        todo!("we need to look at all the array elements :sob:");
        // Ok(())
    }
}

impl<'script> Compilable<'script> for AssignPattern<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let AssignPattern {
            id: _,
            idx,
            pattern,
        } = self;
        // FIXME: want a MID
        let mid = NodeMeta::dummy();
        compiler.comment("Assign pattern");
        let is_extract = matches!(pattern.as_ref(), &Pattern::Extract(_));
        pattern.compile(compiler)?;
        let dst = compiler.new_jump_point();
        compiler.comment("Jump on no match");
        compiler.emit(Op::JumpFalse { dst }, &mid);
        // extract patterns will store the value in the local stack if they match
        if !is_extract {
            compiler.comment("Store the value in the local");
            compiler.emit(Op::CopyV1, &mid);
        }
        compiler.emit(
            Op::StoreLocal {
                idx: u32::try_from(idx)?,
                elements: 0,
            },
            &mid,
        );
        compiler.set_jump_target(dst);
        Ok(())
    }
}

impl<'script> Compilable<'script> for TuplePattern<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let TuplePattern { mid, exprs, open } = self;
        compiler.comment("Tuple pattern");
        compiler.emit(Op::TestIsArray, &mid);
        let dst_next = compiler.new_jump_point();
        compiler.emit(Op::JumpFalse { dst: dst_next }, &mid);
        compiler.comment("Check if the array is long enough");
        compiler.emit(Op::InspectLen, &mid);
        compiler.emit_const(exprs.len(), &mid);

        if open {
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
        for (i, e) in exprs.into_iter().enumerate() {
            compiler.comment(&format!("Test tuple element {i}"));
            compiler.emit(Op::ArrayPop, &mid);
            compiler.comment("Load value in register to test");
            compiler.emit(Op::SwapV1, &mid);
            e.compile(compiler)?;
            compiler.comment("restore original test value");
            compiler.emit(Op::LoadV1, &mid);
            compiler.comment("Jump on no match");
            compiler.emit(Op::JumpFalse { dst: end_and_pop }, &mid);
        }
        // remove the array from the stack
        compiler.comment("Remove the array from the stack");
        compiler.set_jump_target(end_and_pop);
        compiler.emit(Op::Pop, &mid);
        compiler.set_jump_target(dst_next);
        Ok(())
    }
}

impl<'script> Compilable<'script> for TestExpr {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let TestExpr {
            mid,
            id: _,
            test: _,
            extractor,
        } = self;
        let extractor = compiler.add_extractor(extractor);
        compiler.emit(Op::TestExtractor { extractor }, &mid);
        Ok(())
    }
}

#[allow(clippy::cast_possible_truncation)]
fn compile_present<'script>(
    compiler: &mut Compiler<'script>,
    mid: &NodeMeta,
    path: Path<'script>,
) -> Result<()> {
    let elements = u32::try_from(path.len())?;
    let segments = match path {
        Path::Local(LocalPath { idx, mid, segments }) => {
            compiler.comment("Store local on stack");
            compiler.emit(
                Op::LoadLocal {
                    idx: u32::try_from(idx)?,
                },
                &mid,
            );
            segments
        }
        Path::Event(EventPath { mid, segments }) => {
            compiler.comment("Store event on stack");
            compiler.emit(Op::LoadEvent, &mid);
            segments
        }
        Path::State(_p) => todo!(),
        Path::Meta(_p) => todo!(),
        Path::Expr(_p) => todo!(),
        Path::Reserved(_p) => todo!(),
    };
    dbg!(&segments);
    compiler.comment("Swap in V1 to save the current value and load the base for the test");
    compiler.emit(Op::SwapV1, mid);
    if !segments.is_empty() {
        compiler.comment("Load the path");
    }
    for s in segments {
        compile_segment_path(compiler, s)?;
    }
    compiler.comment("Test if the value is present");
    compiler.emit(Op::TestPresent { elements }, mid);
    compiler.comment("Restore V1");
    compiler.emit(Op::LoadV1, mid);
    compiler.comment("Save result");
    compiler.emit(Op::StoreRB, mid);
    Ok(())
}
