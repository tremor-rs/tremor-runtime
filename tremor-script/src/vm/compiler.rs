use crate::{
    ast::{
        raw::{BytesDataType, Endian},
        BaseExpr, BinOpKind, BooleanBinExpr, BooleanBinOpKind, BytesPart, ClauseGroup,
        ClausePreCondition, DefaultCase, EmitExpr, Expr, Expression, Field, ImutExpr, Match,
        PatchOperation, Path, Pattern, PredicateClause, Script, Segment, StrLitElement, StringLit,
    },
    errors::Result,
    NodeMeta,
};
use std::{collections::HashMap, mem};
use tremor_value::Value;

use super::{Op, Program};

#[derive(Debug)]
/// A compiler for tremor script
pub struct Compiler<'script> {
    opcodes: Vec<Op>,
    meta: Vec<NodeMeta>,
    jump_table: Vec<u32>,
    end_table: Vec<u32>,
    consts: Vec<Value<'script>>,
    keys: Vec<tremor_value::KnownKey<'script>>,
    max_locals: usize,
}

trait Compilable<'script>: Sized {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()>;
    fn compile_to_b(self, compiler: &mut Compiler<'script>) -> Result<()> {
        self.compile(compiler)?;
        compiler.emit(Op::LoadRB, *NodeMeta::dummy());
        Ok(())
    }
}

impl<'script> Compiler<'script> {
    /// compiles a script into a program
    /// # Errors
    /// if the script can't be compiled
    pub fn compile(&mut self, script: Script<'script>) -> Result<Program<'script>> {
        script.compile(self)?;
        let mut opcodes = mem::take(&mut self.opcodes);
        let meta = mem::take(&mut self.meta);
        let consts = mem::take(&mut self.consts);
        let mut jump_table = HashMap::with_capacity(self.jump_table.len());
        let keys = mem::take(&mut self.keys);

        for op in &mut opcodes {
            match op {
                Op::JumpTrue { dst, .. } | Op::JumpFalse { dst, .. } | Op::Jump { dst, .. } => {
                    *dst = self.jump_table[*dst as usize];
                }
                _ => (),
            }
        }
        for (idx, dst) in mem::take(&mut self.jump_table).into_iter().enumerate() {
            jump_table.insert(dst as usize, idx);
        }

        Ok(Program {
            opcodes,
            meta,
            jump_table,
            consts,
            keys,
            max_locals: self.max_locals,
        })
    }

    /// creates a new compiler
    #[must_use]
    pub fn new() -> Self {
        Self {
            opcodes: Vec::new(),
            meta: Vec::new(),
            jump_table: Vec::new(),
            end_table: Vec::new(),
            consts: Vec::new(),
            keys: Vec::new(),
            max_locals: 0,
        }
    }

    // #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    // fn last_code_offset(&self) -> u32 {
    //     assert!(!self.opcodes.is_empty());
    //     self.opcodes.len() as u32 - 1
    // }

    pub(crate) fn emit(&mut self, op: Op, mid: NodeMeta) {
        self.opcodes.push(op);
        self.meta.push(mid);
    }

    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn add_const(&mut self, value: Value<'script>) -> u32 {
        for (idx, v) in self.consts.iter().enumerate() {
            if v == &value {
                return idx as u32;
            }
        }
        self.consts.push(value);
        (self.consts.len() - 1) as u32
    }
    pub(crate) fn emit_const<T: Into<Value<'script>>>(&mut self, v: T, mid: NodeMeta) {
        let idx = self.add_const(v.into());
        self.emit(Op::Const { idx }, mid);
    }

    #[allow(clippy::cast_possible_truncation)]
    fn add_key(&mut self, key: tremor_value::KnownKey<'script>) -> u32 {
        for (idx, v) in self.keys.iter().enumerate() {
            if v == &key {
                return idx as u32;
            }
        }
        self.keys.push(key);
        (self.keys.len() - 1) as u32
    }

    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn new_jump_point(&mut self) -> u32 {
        self.jump_table.push(0);
        self.jump_table.len() as u32 - 1
    }
    #[deny(clippy::cast_possible_truncation)]
    pub(crate) fn set_jump_target(&mut self, jump_point: u32) {
        self.jump_table[jump_point as usize] = self.opcodes.len() as u32;
    }

    fn new_end_target(&mut self) {
        let end = self.new_jump_point();
        self.end_table.push(end);
    }

    fn set_end_target(&mut self) -> Result<()> {
        if let Some(end) = self.end_table.pop() {
            self.set_jump_target(end);
            Ok(())
        } else {
            Err("No jump destination found".into())
        }
    }

    fn end_dst(&self) -> Result<u32> {
        if let Some(dst) = self.end_table.last() {
            Ok(*dst)
        } else {
            Err("No jump destination found".into())
        }
    }
}

impl<'script> Default for Compiler<'script> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'script> Compilable<'script> for Script<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        for e in self.exprs {
            e.compile(compiler)?;
        }
        Ok(())
    }
}

impl<'script> Compilable<'script> for Expr<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        match self {
            Expr::Match(m) => m.compile(compiler)?,
            Expr::IfElse(ie) => {
                let mid = *ie.mid.clone();
                // jump point for the else clause
                let else_dst = compiler.new_jump_point();
                // jump point for the end of the if expression
                compiler.new_end_target();
                // store v1
                compiler.emit(Op::StoreV1, mid.clone());

                // compile the target and store the result in the B1 register
                ie.target.compile(compiler)?;
                // load target in register 1;
                compiler.emit(Op::LoadV1, mid.clone());
                // compile the if clause
                ie.if_clause.compile(compiler)?;
                // this is the jump destionaion for the else clause
                compiler.set_jump_target(else_dst);
                ie.else_clause.compile(compiler)?;
                // this is the jump destionaion for the end of the if expression
                compiler.set_end_target()?;
                // load the result in of the expression to v1 so we can restore the old value
                compiler.emit(Op::LoadV1, *ie.mid.clone());
                // restore original value
                compiler.emit(Op::SwapV1, *ie.mid.clone());
            }
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
                compiler.emit(Op::StoreLocal { idx: p.idx as u32 }, *p.mid);
            }
            Expr::AssignMoveLocal {
                mid: _,
                path: _,
                idx: _,
            } => {}
            Expr::Comprehension(_) => todo!(),
            Expr::Drop { mid } => compiler.emit(Op::Drop, *mid),
            Expr::Emit(e) => e.compile(compiler)?,
            Expr::Imut(e) => e.compile(compiler)?,
        }
        Ok(())
    }
}

#[allow(clippy::too_many_lines)]
impl<'script> Compilable<'script> for ImutExpr<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        match self {
            #[allow(clippy::cast_possible_truncation)]
            ImutExpr::Record(r) => {
                let size = r.fields.len() as u32;
                for f in r.fields {
                    f.compile(compiler)?;
                }
                compiler.emit(Op::Record { size }, *r.mid);
            }
            #[allow(clippy::cast_possible_truncation)]
            ImutExpr::List(l) => {
                let size = l.exprs.len() as u32;
                for e in l.exprs {
                    e.compile(compiler)?;
                }
                compiler.emit_const(Value::array(), *l.mid.clone());
                compiler.emit(Op::Array { size }, *l.mid);
            }
            ImutExpr::Binary(b) => {
                b.lhs.compile(compiler)?;
                b.rhs.compile(compiler)?;
                compiler.emit(Op::Binary { op: b.kind }, *b.mid);
            }
            ImutExpr::BinaryBoolean(b) => {
                b.compile(compiler)?;
            }
            ImutExpr::Unary(u) => {
                u.expr.compile(compiler)?;
                compiler.emit(Op::Unary { op: u.kind }, *u.mid);
            }
            ImutExpr::Patch(p) => {
                let mid = *p.mid.clone();
                // Save r1 to ensure we can restore it after the patch operation
                compiler.emit(Op::StoreV1, mid.clone());
                // compile the target
                p.target.compile(compiler)?;
                // load the target into the register
                compiler.emit(Op::LoadV1, mid.clone());
                for op in p.operations {
                    op.compile(compiler)?;
                }
                // restore r1 and ensure the result is on top of the stack
                compiler.emit(Op::SwapV1, mid);
            }
            ImutExpr::Match(m) => m.compile(compiler)?,
            ImutExpr::Comprehension(_) => todo!(),
            ImutExpr::Merge(m) => {
                m.target.compile(compiler)?;
                m.expr.compile(compiler)?;
                compiler.emit(Op::Merge, *m.mid);
            }
            ImutExpr::Path(p) => p.compile(compiler)?,
            ImutExpr::String(s) => s.compile(compiler)?,
            #[allow(clippy::cast_possible_truncation)]
            ImutExpr::Local { idx, mid } => {
                compiler.max_locals = compiler.max_locals.max(idx);
                compiler.emit(Op::LoadLocal { idx: idx as u32 }, *mid);
            }
            ImutExpr::Literal(l) => {
                compiler.emit_const(l.value, *l.mid);
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
                let mid = *b.mid;
                // we modify r1 in the parts so we need to store it
                compiler.emit(Op::StoreV1, mid.clone());
                for b in b.value {
                    b.compile(compiler)?;
                }
                compiler.emit(Op::Bytes { size }, mid.clone());
                // once the bytes are crated we restore it
                compiler.emit(Op::LoadV1, mid);
            }
            #[allow(clippy::cast_possible_truncation)]
            ImutExpr::ArrayAppend(a) => {
                let size = a.right.len() as u32;
                for r in a.right {
                    r.compile(compiler)?;
                }
                a.left.compile(compiler)?;
                compiler.emit(Op::Array { size }, *a.mid);
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
                    compiler.emit_const(s, *self.mid.clone());
                }
                StrLitElement::Expr(e) => {
                    e.compile(compiler)?;
                }
            }
        }
        compiler.emit(Op::String { size }, *self.mid);
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
        compiler.emit(Op::Emit { dflt }, *self.mid);
        Ok(())
    }
}

impl<'script> Compilable<'script> for PatchOperation<'script> {
    #[allow(unused_variables)]
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        match self {
            PatchOperation::Insert { ident, expr, mid } => {
                ident.compile(compiler)?;
                compiler.emit(Op::TestRecortPresent, *mid.clone());
                let dst = compiler.new_jump_point();
                compiler.emit(Op::JumpFalse { dst }, *mid.clone());
                compiler.emit_const("Key already present", *mid.clone());
                compiler.emit(Op::Error, *mid.clone());
                compiler.set_jump_target(dst);
                expr.compile(compiler)?;
                compiler.emit(Op::RecordSet, *mid);
            }
            PatchOperation::Upsert { ident, expr, mid } => {
                ident.compile(compiler)?;
                expr.compile(compiler)?;
                compiler.emit(Op::RecordSet, *mid);
            }
            PatchOperation::Update { ident, expr, mid } => {
                ident.compile(compiler)?;
                compiler.emit(Op::TestRecortPresent, *mid.clone());
                let dst = compiler.new_jump_point();
                compiler.emit(Op::JumpTrue { dst }, *mid.clone());
                compiler.emit_const("Key already present", *mid.clone());
                compiler.emit(Op::Error, *mid.clone());
                compiler.set_jump_target(dst);
                expr.compile(compiler)?;
                compiler.emit(Op::RecordSet, *mid);
            }
            PatchOperation::Erase { ident, mid } => {
                ident.compile(compiler)?;
                compiler.emit(Op::RecordRemove, *mid.clone());
                compiler.emit(Op::Pop, *mid);
            }
            PatchOperation::Copy { from, to, mid } => {
                from.compile(compiler)?;
                compiler.emit(Op::RecordGet, *mid.clone());
                to.compile(compiler)?;
                compiler.emit(Op::Swap, *mid.clone());
                compiler.emit(Op::RecordSet, *mid);
            }
            PatchOperation::Move { from, to, mid } => {
                from.compile(compiler)?;
                compiler.emit(Op::RecordRemove, *mid.clone());
                to.compile(compiler)?;
                compiler.emit(Op::Swap, *mid.clone());
                compiler.emit(Op::RecordSet, *mid);
            }
            PatchOperation::Merge { ident, expr, mid } => todo!(),
            PatchOperation::MergeRecord { expr, mid } => todo!(),
            PatchOperation::Default { ident, expr, mid } => {
                ident.compile(compiler)?;
                compiler.emit(Op::TestRecortPresent, *mid.clone());
                let dst = compiler.new_jump_point();
                compiler.emit(Op::JumpTrue { dst }, *mid.clone());
                expr.compile(compiler)?;
                compiler.emit(Op::RecordSet, *mid);
                compiler.set_jump_target(dst);
            },
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
                compiler.emit(Op::GetKey { key }, *mid);
            }
            Segment::Element { expr, mid } => {
                expr.compile(compiler)?;
                compiler.emit(Op::Get, *mid);
            }
            #[allow(clippy::cast_possible_truncation)]
            Segment::Idx { idx, mid } => {
                if let Ok(idx) = u32::try_from(idx) {
                    compiler.emit(Op::IndexFast { idx }, *mid);
                } else {
                    compiler.emit_const(idx, *mid.clone());
                    compiler.emit(Op::Index, *mid);
                }
            }
            #[allow(clippy::cast_possible_truncation)]
            Segment::Range { mid, start, end } => {
                if let Some((start, end)) = u16::try_from(start).ok().zip(u16::try_from(end).ok()) {
                    compiler.emit(Op::RangeFast { start, end }, *mid);
                } else {
                    compiler.emit_const(start, *mid.clone());
                    compiler.emit_const(end, *mid.clone());
                    compiler.emit(Op::Range, *mid);
                }
            }

            Segment::RangeExpr { mid, start, end } => {
                start.compile(compiler)?;
                end.compile(compiler)?;
                compiler.emit(Op::Range, *mid);
            }
        }
        Ok(())
    }
}

impl<'script> Compilable<'script> for BooleanBinExpr<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        let mid = *self.mid.clone();
        self.compile_to_b(compiler)?;
        compiler.emit(Op::StoreRB, mid);
        Ok(())
    }
    fn compile_to_b(self, compiler: &mut Compiler<'script>) -> Result<()> {
        self.lhs.compile_to_b(compiler)?;
        match self.kind {
            #[allow(clippy::cast_sign_loss)]
            BooleanBinOpKind::Or => {
                let dst = compiler.new_jump_point();
                compiler.emit(Op::JumpTrue { dst }, *self.mid.clone());
                self.rhs.compile_to_b(compiler)?;
                compiler.set_jump_target(dst);
            }
            BooleanBinOpKind::Xor => {
                self.rhs.compile(compiler)?;
                compiler.emit(Op::Xor, *self.mid);
            }
            #[allow(clippy::cast_sign_loss)]
            BooleanBinOpKind::And => {
                let dst = compiler.new_jump_point();
                compiler.emit(Op::JumpFalse { dst }, *self.mid);
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

        let mid = *mid;
        data.compile(compiler)?;
        let dst = compiler.new_jump_point();
        // load the value into r1 so we can test it
        compiler.emit(Op::LoadV1, mid.clone());
        match data_type {
            BytesDataType::SignedInteger => compiler.emit(Op::TestIsI64, mid.clone()),
            BytesDataType::UnsignedInteger => compiler.emit(Op::TestIsU64, mid.clone()),
            BytesDataType::Binary => compiler.emit(Op::TestIsBytes, mid.clone()),
        }
        compiler.emit(Op::JumpFalse { dst }, mid.clone());
        compiler.emit_const("invalid type conversion for binary", mid.clone());
        compiler.emit(Op::Error, mid.clone());
        compiler.set_jump_target(dst);
        // save the value back to the stack
        compiler.emit(Op::StoreV1, mid.clone());
        compiler.emit_const(format, mid);
        Ok(())
    }
}

impl<'script> Compilable<'script> for Path<'script> {
    fn compile(self, compiler: &mut Compiler<'script>) -> Result<()> {
        match self {
            #[allow(clippy::cast_possible_truncation)]
            Path::Local(p) => {
                compiler.max_locals = compiler.max_locals.max(p.idx);
                compiler.emit(Op::LoadLocal { idx: p.idx as u32 }, *p.mid);
                for s in p.segments {
                    s.compile(compiler)?;
                }
            }
            Path::Event(p) => {
                compiler.emit(Op::LoadEvent, *p.mid);
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

        compiler.emit(Op::CopyV1, *mid.clone());
        pattern.compile_to_b(compiler)?;
        compiler.emit(Op::JumpFalse { dst }, *mid.clone());
        if let Some(guard) = guard {
            guard.compile_to_b(compiler)?;
            compiler.emit(Op::JumpFalse { dst }, *mid.clone());
        }
        for e in exprs {
            e.compile(compiler)?;
        }
        last_expr.compile(compiler)?;
        // we were successful so we jump to the end
        compiler.emit(Op::Jump { dst: end_dst }, *mid);
        compiler.set_jump_target(dst);
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
                let mid = *r.mid;
                compiler.emit(Op::TestIsRecord, mid.clone());
                let dst = compiler.new_jump_point();
                compiler.emit(Op::JumpFalse { dst }, mid.clone());
                if r.fields.is_empty() {
                } else {
                    todo!()
                }
                compiler.set_jump_target(dst);
            }
            Pattern::Array(a) => {
                let mid = *a.mid;
                compiler.emit(Op::TestIsArray, mid.clone());
                let dst = compiler.new_jump_point();
                compiler.emit(Op::JumpFalse { dst }, mid.clone());
                if a.exprs.is_empty() {
                } else {
                    todo!()
                }
                compiler.set_jump_target(dst);
            }
            Pattern::Expr(e) => {
                let mid = e.meta().clone();
                e.compile(compiler)?;
                compiler.emit(Op::Binary { op: BinOpKind::Eq }, mid);
                compiler.emit(Op::LoadRB, *NodeMeta::dummy());
            }

            Pattern::Assign(_) => todo!(),
            Pattern::Tuple(_) => todo!(),
            Pattern::Extract(_) => todo!(),
            Pattern::DoNotCare => {
                compiler.emit(Op::True, *NodeMeta::dummy());
                compiler.emit(Op::LoadRB, *NodeMeta::dummy());
            }
        }
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
                compiler.emit(Op::Nop, *NodeMeta::dummy());
            }
            DefaultCase::Null => {
                compiler.emit(Op::Null, *NodeMeta::dummy());
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
                    precondition.compile_to_b(compiler)?;
                    compiler.emit(Op::JumpFalse { dst: next }, *NodeMeta::dummy());
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
                    precondition.compile_to_b(compiler)?;
                    compiler.emit(Op::JumpFalse { dst: next }, *NodeMeta::dummy());
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
                    precondition.compile_to_b(compiler)?;
                    compiler.emit(Op::JumpFalse { dst: next }, *NodeMeta::dummy());
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
                    precondition.compile_to_b(compiler)?;
                    compiler.emit(Op::JumpFalse { dst: next }, *NodeMeta::dummy());
                }
                pattern.compile(compiler)?;
            }
        }
        compiler.set_jump_target(next);
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
        let mid = *mid;
        compiler.new_end_target();
        // Save r1 to ensure we can restore it after the patch operation
        compiler.emit(Op::StoreV1, mid.clone());
        // Save r1 to ensure we can restore it after the patch operation
        target.compile(compiler)?;
        // load target to v1
        compiler.emit(Op::LoadV1, mid.clone());

        for c in patterns {
            c.compile(compiler)?;
        }
        default.compile(compiler)?;
        compiler.set_end_target()?;
        // restore r1 and ensure the result is on top of the stack
        // Load the result of the match into r1
        compiler.emit(Op::LoadV1, mid.clone());
        // restore r1 and ensure the result is on top of the stack
        compiler.emit(Op::SwapV1, mid);
        Ok(())
    }
}
