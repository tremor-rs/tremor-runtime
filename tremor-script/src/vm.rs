use std::{borrow::Cow, collections::HashMap, fmt::Display};

use simd_json::{prelude::*, ValueBuilder};
use tremor_value::Value;

use crate::{
    ast::{
        binary::write_bits,
        raw::{BytesDataType, Endian},
        BinOpKind, UnaryOpKind,
    },
    errors::{error_generic, Result},
    interpreter::{exec_binary, exec_unary, merge_values},
    prelude::Ranged,
    NodeMeta, Return,
};

pub(super) mod compiler;

#[derive(Debug, PartialEq, Copy, Clone, Default, Eq)]
pub(crate) enum Op {
    /// do absolutely nothing
    #[default]
    Nop,
    /// take the top most value from the stack and delete it
    Pop,
    /// swap the top two values on the stack
    Swap,
    /// duplicate the top of the stack
    #[allow(dead_code)]
    Duplicate,
    /// Puts the event on the stack
    LoadEvent,
    /// puts a variable on the stack
    LoadLocal {
        idx: u32,
    },
    StoreLocal {
        idx: u32,
    },
    /// emits an error
    Error,
    #[allow(dead_code)]
    Begin,
    #[allow(dead_code)]
    End,
    /// emits the top of the stack
    Emit {
        dflt: bool,
    },
    /// drops the event
    Drop,
    /// jumps to the given offset if the top of the stack is true does not op the stack
    JumpTrue {
        dst: u32,
    },
    /// jumps to the given offset if the top of the stack is true does not op the stack
    JumpFalse {
        dst: u32,
    },
    Const {
        idx: u32,
    },

    // Values
    #[allow(dead_code)]
    True,
    #[allow(dead_code)]
    False,
    Record {
        size: u32,
    },
    Array {
        size: u32,
    },
    String {
        size: u32,
    },
    Bytes {
        size: u32,
    },
    // Logical XOP
    Xor,

    Binary {
        op: BinOpKind,
    },
    Unary {
        op: UnaryOpKind,
    },

    GetKey {
        key: u32,
    },
    Get,
    Index,
    IndexFast {
        idx: u32,
    },
    Range,
    RangeFast {
        start: u16,
        end: u16,
    },

    // Tests
    TestRecortPresent,
    TestIsU64,
    TestIsI64,
    TestIsBytes,

    // Patch
    RecordSet,
    RecordRemove,
    RecordGet,
    // Merge
    Merge,
}

impl Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::Nop => write!(f, "nop"),
            Op::Pop => write!(f, "pop"),
            Op::Swap => write!(f, "swap"),
            Op::Duplicate => write!(f, "duplicate"),
            Op::Error => write!(f, "error"),
            Op::LoadEvent => write!(f, "laod_event"),
            Op::LoadLocal { idx } => write!(f, "{:30} {}", "load_local", idx),
            Op::StoreLocal { idx } => write!(f, "{:30} {}", "store_local", idx),
            Op::Begin => write!(f, "begin"),
            Op::End => write!(f, "end"),
            Op::Emit { dflt } => write!(f, "{:30} {dflt}", "emit"),
            Op::Drop => write!(f, "drop"),
            Op::JumpTrue { dst } => write!(f, "{:30} {}", "jump_true", dst),
            Op::JumpFalse { dst } => write!(f, "{:30} {}", "jump_false", dst),
            Op::True => write!(f, "true"),
            Op::False => write!(f, "false"),
            Op::Const { idx } => write!(f, "{:30} {}", "const", idx),
            Op::Record { size } => write!(f, "{:30} {}", "record", size),
            Op::Array { size } => write!(f, "{:30} {}", "array", size),
            Op::String { size } => write!(f, "{:30} {}", "string", size),
            Op::Bytes { size } => write!(f, "{:30} {}", "bytes", size),
            Op::Xor => write!(f, "xor"),
            Op::Binary { op } => write!(f, "{:30} {:?}", "binary", op),
            Op::Unary { op } => write!(f, "{:30} {:?}", "unary", op),
            Op::GetKey { key } => write!(f, "{:30} {}", "lookup_key", key),
            Op::Get => write!(f, "lookup"),
            Op::Index => write!(f, "idx"),
            Op::IndexFast { idx } => write!(f, "{:30} {}", "idx_fast", idx),
            Op::Range => write!(f, "range"),
            Op::RangeFast { start, end } => write!(f, "{:30} {} {}", "range_fast", start, end),
            Op::TestRecortPresent => write!(f, "test_record_present"),
            Op::TestIsU64 => write!(f, "test_is_u64"),
            Op::TestIsI64 => write!(f, "test_is_i64"),
            Op::TestIsBytes => write!(f, "test_is_bytes"),
            Op::RecordSet => write!(f, "record_set"),
            Op::RecordRemove => write!(f, "record_remove"),
            Op::RecordGet => write!(f, "record_get"),
            Op::Merge => write!(f, "merge"),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Default, Eq)]
/// A compiler for tremor script
pub struct Program<'script> {
    opcodes: Vec<Op>,
    meta: Vec<NodeMeta>,
    jump_table: HashMap<usize, usize>,
    consts: Vec<Value<'script>>,
    keys: Vec<tremor_value::KnownKey<'script>>,
    max_locals: usize,
}

impl Display for Program<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (idx, op) in self.opcodes.iter().enumerate() {
            if let Some(dst) = self.jump_table.get(&idx).copied() {
                writeln!(f, "JMP<{dst:03}>")?;
            }
            match op {
                Op::JumpTrue { dst } => writeln!(
                    f,
                    "          {idx:04}: {:30} JMP<{:03}>",
                    "jump_true",
                    self.jump_table
                        .get(&(*dst as usize))
                        .copied()
                        .unwrap_or_default()
                )?,
                Op::JumpFalse { dst } => writeln!(
                    f,
                    "          {idx:04}: {:30} JMP<{:03}>",
                    "jump_false",
                    self.jump_table
                        .get(&(*dst as usize))
                        .copied()
                        .unwrap_or_default()
                )?,
                _ => writeln!(f, "          {idx:04}: {op}")?,
            }
        }
        Ok(())
    }
}

#[allow(dead_code)]
pub struct Vm {}

pub struct Scope<'run, 'event> {
    // value: &'run mut Value<'event>,
    keys: &'run [tremor_value::KnownKey<'event>],
}
#[allow(dead_code)]
impl Vm {
    pub fn new() -> Self {
        Self {}
    }
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::unused_self)]
    pub fn run<'run, 'prog, 'event>(
        &self,
        event: &mut Value<'event>,
        program: &Program<'prog>,
    ) -> Result<Return<'event>>
    where
        'prog: 'event,
    {
        // our locals start at zero, so we need to allocate a length of max_locals + 1
        let mut locals = Vec::with_capacity(program.max_locals + 1);
        for _ in 0..=program.max_locals {
            locals.push(None);
        }
        // value: &mut null,

        let mut root = Scope {
            keys: &program.keys,
        };
        let mut pc = 0;

        // ensure that the opcodes and meta are the same length
        assert_eq!(program.opcodes.len(), program.meta.len());
        root.run(event, &mut locals, program, &mut pc)
    }
}

impl<'run, 'event> Scope<'run, 'event> {
    #[allow(clippy::too_many_lines)]
    pub fn run<'prog>(
        &mut self,
        event: &mut Value<'event>,
        locals: &mut [Option<Value<'event>>],
        program: &Program<'prog>,
        pc: &mut usize,
    ) -> Result<Return<'event>>
    where
        'prog: 'event,
    {
        let mut stack: Vec<Cow<'_, Value>> = Vec::with_capacity(8);

        while *pc < program.opcodes.len() {
            let mid = &program.meta[*pc];
            // ALLOW: we test that pc is always in bounds in the while loop above
            match unsafe { *program.opcodes.get_unchecked(*pc) } {
                Op::Nop => continue,
                // Loads
                Op::LoadEvent => stack.push(Cow::Borrowed(event)),
                Op::LoadLocal { idx } => {
                    let idx = idx as usize;
                    stack.push(Cow::Owned(
                        locals[idx].as_ref().ok_or("Local not set")?.clone(),
                    ));
                }
                Op::StoreLocal { idx } => {
                    let idx = idx as usize;
                    locals[idx] = Some(stack.pop().ok_or("Stack underflow")?.into_owned());
                }

                Op::True => stack.push(Cow::Owned(Value::const_true())),
                Op::False => stack.push(Cow::Owned(Value::const_false())),
                Op::Const { idx } => stack.push(Cow::Borrowed(&program.consts[idx as usize])),
                Op::Begin => {
                    let _value: &mut Value = stack.last_mut().ok_or("Stack underflow")?.to_mut();
                    // let mut scopte = Scope {
                    //     value,
                    //     keys: self.keys,
                    // };
                    *pc += 1;
                    // scopte.run(event, program, pc)?;
                }
                Op::End => return Ok(Return::EmitEvent { port: None }),
                Op::Pop => {
                    stack.pop().ok_or("Stack underflow")?;
                }
                Op::Swap => {
                    let a = stack.pop().ok_or("Stack underflow")?;
                    let b = stack.pop().ok_or("Stack underflow")?;
                    stack.push(a);
                    stack.push(b);
                }
                Op::Duplicate => {
                    let a = stack.last().ok_or("Stack underflow")?;
                    stack.push(a.clone());
                }
                Op::Error => {
                    let msg = stack.pop().ok_or("Stack underflow")?;
                    let mid = mid.clone();
                    return Err(error_generic(
                        &mid.extent(),
                        &mid.extent(),
                        &msg.to_string(),
                    ));
                }
                Op::Emit { dflt: true } => {
                    let value = stack.pop().ok_or("Stack underflow")?;
                    return Ok(Return::Emit {
                        value: value.into_owned(),
                        port: None,
                    });
                }
                Op::Emit { dflt: false } => {
                    let port = stack.pop().ok_or("Stack underflow")?;
                    let value = stack.pop().ok_or("Stack underflow")?;
                    return Ok(Return::Emit {
                        value: value.into_owned(),
                        port: Some(port.try_as_str()?.to_string().into()),
                    });
                }
                Op::Drop => {
                    return Ok(Return::Drop);
                }
                #[allow(clippy::cast_abs_to_unsigned)]
                Op::JumpTrue { dst } => {
                    if stack.last().ok_or("Stack underflow")?.try_as_bool()? {
                        *pc = dst as usize;
                    }
                }
                #[allow(clippy::cast_abs_to_unsigned)]
                Op::JumpFalse { dst } => {
                    if !stack.last().ok_or("Stack underflow")?.try_as_bool()? {
                        *pc = dst as usize;
                    }
                }
                Op::Record { size } => {
                    let size = size as usize;
                    let mut record = Value::object_with_capacity(size);
                    for _ in 0..size {
                        let value = stack.pop().ok_or("Stack underflow")?;
                        let key = stack.pop().ok_or("Stack underflow")?;
                        // FIXME: we can do better than clone here
                        let key = key.into_owned().try_into_string()?;
                        record.try_insert(key, value.into_owned());
                    }
                    stack.push(Cow::Owned(record));
                }
                Op::Array { size } => {
                    let size = size as usize;
                    let mut v = stack.pop().ok_or("Stack underflow")?;
                    let array = v.to_mut().as_array_mut().ok_or("Not an array")?;
                    array.reserve(size);
                    for value in stack.drain(stack.len() - size..) {
                        array.push(value.into_owned());
                    }
                    stack.push(v);
                }
                Op::String { size } => {
                    let size = size as usize;
                    // FIXME: is this a good heuristic?
                    let mut builder = String::with_capacity(size * 16);
                    for value in stack.drain(stack.len() - size..) {
                        builder.push_str(value.try_as_str()?);
                    }
                    stack.push(Cow::Owned(builder.into()));
                }
                #[allow(
                    clippy::cast_lossless,
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss
                )]
                Op::Bytes { size } => {
                    let size = size as usize;
                    let mut bytes = Vec::with_capacity(size * 8);
                    let mut pending = 0;
                    let mut buf = 0;
                    for _ in 0..size {
                        let mut format = stack.pop().ok_or("Stack underflow")?.try_as_i64()?;
                        let endianess = Endian::from(format as u8 & 0b1);
                        format >>= 1;
                        let data_type = BytesDataType::from(format as u8 & 0b11);
                        let bits = (format >> 2) as u8;
                        let value = stack.pop().ok_or("Stack underflow")?;
                        match data_type {
                            BytesDataType::UnsignedInteger => write_bits(
                                &mut bytes,
                                bits,
                                endianess,
                                &mut buf,
                                &mut pending,
                                value.try_as_u64()?,
                            )?,
                            BytesDataType::SignedInteger => write_bits(
                                &mut bytes,
                                bits,
                                endianess,
                                &mut buf,
                                &mut pending,
                                value.try_as_i64()? as u64,
                            )?,
                            BytesDataType::Binary => {
                                let mut b = value.try_as_bytes()?;
                                if bits > 0 {
                                    b = b
                                        .get(..(bits as usize))
                                        .ok_or("Not a long enough binary")?;
                                }
                                if pending == 0 {
                                    bytes.extend_from_slice(b);
                                } else {
                                    for v in b {
                                        write_bits(
                                            &mut bytes,
                                            8,
                                            endianess,
                                            &mut buf,
                                            &mut pending,
                                            *v as u64,
                                        )?;
                                    }
                                }
                            }
                        }
                    }
                    stack.push(Cow::Owned(Value::Bytes(bytes.into())));
                }

                Op::Binary { op } => {
                    let rhs = stack.pop().ok_or("Stack underflow")?;
                    let lhs = stack.pop().ok_or("Stack underflow")?;
                    stack.push(exec_binary(mid, mid, op, &lhs, &rhs)?);
                }
                Op::Unary { op } => {
                    let value = stack.pop().ok_or("Stack underflow")?;
                    stack.push(exec_unary(mid, mid, op, &value)?);
                }
                Op::Xor => {
                    let rhs = stack.pop().ok_or("Stack underflow")?;
                    let lhs = stack.pop().ok_or("Stack underflow")?;
                    stack.push(Cow::Owned(Value::from(
                        lhs.try_as_bool()? ^ rhs.try_as_bool()?,
                    )));
                }
                // tests
                Op::TestRecortPresent => {
                    let key = stack.pop().ok_or("Stack underflow")?;
                    let r = stack.last().ok_or("Stack underflow")?;
                    let present = r.contains_key(key.try_as_str()?);
                    stack.push(key);
                    stack.push(Cow::Owned(Value::from(present)));
                } // record operations on scope

                Op::TestIsU64 => {
                    let v = stack.last().ok_or("Stack underflow")?;
                    let is_u64 = v.is_u64();
                    stack.push(Cow::Owned(Value::from(is_u64)));
                }
                Op::TestIsI64 => {
                    let v = stack.last().ok_or("Stack underflow")?;
                    let is_i64 = v.is_i64();
                    stack.push(Cow::Owned(Value::from(is_i64)));
                }
                Op::TestIsBytes => {
                    let v = stack.last().ok_or("Stack underflow")?;
                    let is_bytes = v.is_bytes();
                    stack.push(Cow::Owned(Value::from(is_bytes)));
                }
                Op::RecordSet => {
                    let value = stack.pop().ok_or("Stack underflow")?;
                    let key = stack.pop().ok_or("Stack underflow")?;
                    let r = stack.last_mut().ok_or("Stack underflow")?;
                    // FIXME: we can do better than clone here
                    let key = key.into_owned().try_into_string()?;
                    r.to_mut().insert(key, value.into_owned())?;
                }
                Op::RecordRemove => {
                    let key = stack.pop().ok_or("Stack underflow")?;
                    let r = stack.last_mut().ok_or("Stack underflow")?;
                    let key = key.try_as_str()?;
                    let v = r.to_mut().remove(key)?.unwrap_or_default();
                    stack.push(Cow::Owned(v));
                }
                Op::RecordGet => {
                    let key = stack.pop().ok_or("Stack underflow")?;
                    let r = stack.last().ok_or("Stack underflow")?;
                    let key = key.try_as_str()?;
                    // FIXME: can we avoid this clone here
                    let v = r.get(key).ok_or("not a record")?.clone();
                    stack.push(Cow::Owned(v));
                }
                // merge
                Op::Merge => {
                    let arg = stack.pop().ok_or("Stack underflow")?;
                    let mut target = stack.pop().ok_or("Stack underflow")?;
                    merge_values(target.to_mut(), &arg)?;
                    stack.push(target);
                }
                // Path
                Op::GetKey { key } => {
                    let key = &self.keys[key as usize];
                    let v = stack.pop().ok_or("Stack underflow")?;
                    // FIXME: can we avoid this clone here
                    let res = key.lookup(&v).ok_or("Missing Key FIXME")?.clone();
                    stack.push(Cow::Owned(res));
                }
                Op::Get => {
                    let key = stack.pop().ok_or("Stack underflow")?;
                    let v = stack.pop().ok_or("Stack underflow")?;
                    let v = if let Some(key) = key.as_str() {
                        v.get(key).ok_or("not a record")?.clone()
                    } else if let Some(idx) = key.as_usize() {
                        v.get_idx(idx).ok_or("Index out of bounds")?.clone()
                    } else {
                        return Err(error_generic(mid, mid, &"Invalid key type"));
                    };
                    // FIXME: can we avoid this clone here
                    stack.push(Cow::Owned(v));
                }
                Op::Index => {
                    let idx = stack.pop().ok_or("Stack underflow")?;
                    let idx = idx.try_as_usize()?;
                    let v = stack.pop().ok_or("Stack underflow")?;
                    let v = v.get_idx(idx).ok_or("Index out of bounds")?.clone();
                    stack.push(Cow::Owned(v));
                }
                Op::IndexFast { idx } => {
                    let idx = idx as usize;
                    let v = stack.pop().ok_or("Stack underflow")?;
                    let v = v.get_idx(idx).ok_or("Index out of bounds")?.clone();
                    stack.push(Cow::Owned(v));
                }
                Op::Range => {
                    let end = stack.pop().ok_or("Stack underflow")?;
                    let start = stack.pop().ok_or("Stack underflow")?;
                    let end = end.try_as_usize()?;
                    let start = start.try_as_usize()?;
                    let v = stack.pop().ok_or("Stack underflow")?;
                    let v = v
                        .try_as_array()?
                        .get(start..end)
                        .ok_or("Index out of bounds")?
                        .to_vec();
                    stack.push(Cow::Owned(v.into()));
                }
                Op::RangeFast { start, end } => {
                    let start = start as usize;
                    let end = end as usize;
                    let v = stack.pop().ok_or("Stack underflow")?;
                    let v = v
                        .try_as_array()?
                        .get(start..end)
                        .ok_or("Index out of bounds")?
                        .to_vec();
                    stack.push(Cow::Owned(v.into()));
                }
            }
            *pc += 1;
        }

        let value = stack.pop().ok_or("Stack underflow")?;
        Ok(Return::Emit {
            value: value.into_owned(),
            port: None,
        })
    }
}

#[cfg(test)]
mod tests;
