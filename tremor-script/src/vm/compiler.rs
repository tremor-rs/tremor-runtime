mod impls;

use crate::{ast::Script, errors::Result, NodeMeta};
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
        compiler.emit(Op::LoadRB, &NodeMeta::dummy());
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

    pub(crate) fn emit(&mut self, op: Op, mid: &NodeMeta) {
        self.opcodes.push(op);
        self.meta.push(mid.clone());
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
    pub(crate) fn emit_const<T: Into<Value<'script>>>(&mut self, v: T, mid: &NodeMeta) {
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

    #[allow(clippy::cast_possible_truncation)]
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
