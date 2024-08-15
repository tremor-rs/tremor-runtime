use std::{borrow::Cow, mem};

use compiler::Program;
use simd_json::{prelude::*, ValueBuilder};
use tremor_value::Value;

use crate::{
    ast::{
        binary::write_bits,
        raw::{BytesDataType, Endian},
    },
    errors::{error_generic, Result},
    interpreter::{exec_binary, exec_unary, merge_values},
    prelude::Ranged,
    NodeMeta, Return,
};

pub(super) mod compiler;
mod op;
use op::Op;

#[allow(dead_code)]
pub struct Vm {}

#[derive(Debug)]
struct Registers<'run, 'event> {
    /// Value register 1
    v1: Cow<'run, Value<'event>>,
    /// Boolean register 1
    b1: bool,
}

pub struct Scope<'run, 'event> {
    // value: &'run mut Value<'event>,
    program: &'run Program<'event>,
    reg: &'run mut Registers<'run, 'event>,
    locals: &'run mut [Option<Value<'event>>],
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
        program: &'run Program<'prog>,
    ) -> Result<Return<'event>>
    where
        'prog: 'event,
    {
        // our locals start at zero, so we need to allocate a length of max_locals + 1
        let mut locals = Vec::with_capacity(program.max_locals + 1);
        let mut registers: Registers = Registers {
            v1: Cow::Owned(Value::null()),
            b1: false,
        };
        for _ in 0..=program.max_locals {
            locals.push(None);
        }
        // value: &mut null,

        let mut root = Scope {
            program,
            locals: &mut locals,
            reg: &mut registers,
        };
        let mut pc = 0;
        let mut cc = 0;

        // ensure that the opcodes and meta are the same length
        assert_eq!(program.opcodes.len(), program.meta.len());
        root.run(event, &mut pc, &mut cc)
    }
}

impl<'run, 'event> Scope<'run, 'event> {
    #[allow(clippy::too_many_lines)]
    pub fn run<'prog>(
        &mut self,
        event: &'run mut Value<'event>,
        pc: &mut usize,
        cc: &mut usize,
    ) -> Result<Return<'event>>
    where
        'prog: 'event,
    {
        let mut stack: Vec<Cow<'_, Value>> = Vec::with_capacity(8);

        while *pc < self.program.opcodes.len() {
            *cc += 1;
            let mid = &self.program.meta[*pc];
            // ALLOW: we test that pc is always in bounds in the while loop above
            let op = unsafe { *self.program.opcodes.get_unchecked(*pc) };
            // if let Some(comment) = self.program.comments.get(pc) {
            //     println!("# {comment}");
            // }
            // dbg!(stack.last(), &self.reg);
            // println!("{pc:3}: {op}");
            match op {
                Op::Nop => continue,
                // Loads
                Op::LoadV1 => self.reg.v1 = pop(&mut stack, *pc, *cc)?,
                Op::StoreV1 => stack.push(mem::take(&mut self.reg.v1)),
                Op::CopyV1 => stack.push(self.reg.v1.clone()),
                Op::SwapV1 => {
                    mem::swap(&mut self.reg.v1, last_mut(&mut stack, *pc, *cc)?);
                }
                Op::LoadRB => {
                    self.reg.b1 = pop(&mut stack, *pc, *cc)?.try_as_bool()?;
                }
                Op::StoreRB => stack.push(Cow::Owned(Value::from(self.reg.b1))),
                Op::NotRB => self.reg.b1 = !self.reg.b1,
                Op::TrueRB => self.reg.b1 = true,
                Op::FalseRB => self.reg.b1 = false,
                Op::LoadEvent => stack.push(Cow::Owned(event.clone())),
                Op::StoreEvent { elements } => unsafe {
                    let mut tmp = event as *mut Value;
                    nested_assign(elements, &mut stack, &mut tmp, mid, *pc, *cc)?;
                    let r: &mut Value = tmp
                        .as_mut()
                        .ok_or("this is nasty, we have a null pointer")?;
                    *r = pop(&mut stack, *pc, *cc)?.into_owned();
                },
                Op::LoadLocal { idx } => {
                    let idx = idx as usize;
                    stack.push(Cow::Owned(
                        self.locals[idx].as_ref().ok_or("Local not set")?.clone(),
                    ));
                }
                Op::StoreLocal { elements, idx } => unsafe {
                    let idx = idx as usize;
                    if let Some(var) = self.locals[idx].as_mut() {
                        let mut tmp = var as *mut Value;
                        nested_assign(elements, &mut stack, &mut tmp, mid, *pc, *cc)?;
                        let r: &mut Value = tmp
                            .as_mut()
                            .ok_or("this is nasty, we have a null pointer")?;

                        *r = pop(&mut stack, *pc, *cc)?.into_owned();
                    } else if elements == 0 {
                        self.locals[idx] = Some(pop(&mut stack, *pc, *cc)?.into_owned());
                    } else {
                        return Err("nested assign into unset variable".into());
                    }
                },

                Op::True => stack.push(Cow::Owned(Value::const_true())),
                Op::False => stack.push(Cow::Owned(Value::const_false())),
                Op::Null => stack.push(Cow::Owned(Value::null())),
                Op::Const { idx } => stack.push(Cow::Borrowed(&self.program.consts[idx as usize])),
                Op::Pop => {
                    pop(&mut stack, *pc, *cc)?;
                }
                Op::Swap => {
                    let a = pop(&mut stack, *pc, *cc)?;
                    let b = pop(&mut stack, *pc, *cc)?;
                    stack.push(a);
                    stack.push(b);
                }
                Op::Duplicate => {
                    let a = last(&stack, *pc, *cc)?;
                    stack.push(a.clone());
                }
                Op::Error => {
                    let msg = pop(&mut stack, *pc, *cc)?;
                    let mid = mid.clone();
                    return Err(error_generic(
                        &mid.extent(),
                        &mid.extent(),
                        &msg.to_string(),
                    ));
                }
                Op::Emit { dflt: true } => {
                    let value = pop(&mut stack, *pc, *cc)?;
                    return Ok(Return::Emit {
                        value: value.into_owned(),
                        port: None,
                    });
                }
                Op::Emit { dflt: false } => {
                    let port = pop(&mut stack, *pc, *cc)?;
                    let value = pop(&mut stack, *pc, *cc)?;
                    return Ok(Return::Emit {
                        value: value.into_owned(),
                        port: Some(port.try_as_str()?.to_string().into()),
                    });
                }
                Op::Drop => {
                    return Ok(Return::Drop);
                }
                Op::JumpTrue { dst } => {
                    if self.reg.b1 {
                        *pc = dst as usize;
                        continue;
                    }
                }
                Op::JumpFalse { dst } => {
                    if !self.reg.b1 {
                        *pc = dst as usize;
                        continue;
                    }
                }
                Op::Jump { dst } => {
                    *pc = dst as usize;
                    continue;
                }
                Op::Record { size } => {
                    let size = size as usize;
                    let mut v = pop(&mut stack, *pc, *cc)?;
                    let record = v.to_mut();
                    for _ in 0..size {
                        let value = pop(&mut stack, *pc, *cc)?;
                        let key = pop(&mut stack, *pc, *cc)?;
                        // FIXME: we can do better than clone here
                        let key = key.into_owned().try_into_string()?;
                        record.try_insert(key, value.into_owned());
                    }
                    stack.push(v);
                }
                Op::Array { size } => {
                    let size = size as usize;
                    let mut v = pop(&mut stack, *pc, *cc)?;
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
                        let mut format = pop(&mut stack, *pc, *cc)?.try_as_i64()?;
                        let endianess = Endian::from(format as u8 & 0b1);
                        format >>= 1;
                        let data_type = BytesDataType::from(format as u8 & 0b11);
                        let bits = (format >> 2) as u8;
                        let value = pop(&mut stack, *pc, *cc)?;
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

                // Compairsons ops that store in the B1 register
                // Op::Binary {
                //     op:
                //         op @ (BinOpKind::Eq
                //         | BinOpKind::NotEq
                //         | BinOpKind::Gte
                //         | BinOpKind::Gt
                //         | BinOpKind::Lte
                //         | BinOpKind::Lt),
                // } => {
                //     let rhs = pop(&mut stack, *pc, *cc)?;
                //     let lhs = pop(&mut stack, *pc, *cc)?;
                //     self.registers.b1 = exec_binary(mid, mid, op, &lhs, &rhs)?.try_as_bool()?;
                // }
                Op::Binary { op } => {
                    let rhs = pop(&mut stack, *pc, *cc)?;
                    let lhs = pop(&mut stack, *pc, *cc)?;
                    stack.push(exec_binary(mid, mid, op, &lhs, &rhs)?);
                }
                Op::Unary { op } => {
                    let value = pop(&mut stack, *pc, *cc)?;
                    stack.push(exec_unary(mid, mid, op, &value)?);
                }
                Op::Xor => {
                    let rhs = pop(&mut stack, *pc, *cc)?;
                    stack.push(Cow::Owned(Value::from(self.reg.b1 ^ rhs.try_as_bool()?)));
                }
                // tests
                Op::TestRecortPresent => {
                    let key = last(&stack, *pc, *cc)?;
                    self.reg.b1 = self.reg.v1.contains_key(key.try_as_str()?);
                } // record operations on scope

                Op::TestIsU64 => {
                    self.reg.b1 = self.reg.v1.is_u64();
                }
                Op::TestIsI64 => {
                    self.reg.b1 = self.reg.v1.is_i64();
                }
                Op::TestIsBytes => {
                    self.reg.b1 = self.reg.v1.is_bytes();
                }
                Op::TestIsRecord => {
                    self.reg.b1 = self.reg.v1.is_object();
                }
                Op::TestIsArray => {
                    self.reg.b1 = self.reg.v1.is_array();
                }
                Op::TestArrayIsEmpty => {
                    self.reg.b1 = self.reg.v1.as_array().map_or(true, Vec::is_empty);
                }
                Op::TestEq => {
                    let rhs = pop(&mut stack, *pc, *cc)?;
                    self.reg.b1 =
                        exec_binary(mid, mid, crate::ast::BinOpKind::Eq, &self.reg.v1, &rhs)?
                            .try_as_bool()?;
                }
                Op::TestNeq => {
                    let rhs = last(&stack, *pc, *cc)?;
                    self.reg.b1 =
                        exec_binary(mid, mid, crate::ast::BinOpKind::NotEq, &self.reg.v1, rhs)?
                            .try_as_bool()?;
                }
                Op::TestGt => {
                    let rhs = last(&stack, *pc, *cc)?;
                    self.reg.b1 =
                        exec_binary(mid, mid, crate::ast::BinOpKind::Gt, &self.reg.v1, rhs)?
                            .try_as_bool()?;
                }
                Op::TestLt => {
                    let rhs = last(&stack, *pc, *cc)?;
                    self.reg.b1 =
                        exec_binary(mid, mid, crate::ast::BinOpKind::Lt, &self.reg.v1, rhs)?
                            .try_as_bool()?;
                }
                Op::TestGte => {
                    let rhs = last(&stack, *pc, *cc)?;
                    self.reg.b1 =
                        exec_binary(mid, mid, crate::ast::BinOpKind::Gte, &self.reg.v1, rhs)?
                            .try_as_bool()?;
                }
                Op::TestLte => {
                    let rhs = last(&stack, *pc, *cc)?;
                    self.reg.b1 =
                        exec_binary(mid, mid, crate::ast::BinOpKind::Gte, &self.reg.v1, rhs)?
                            .try_as_bool()?;
                }
                Op::TestRecordIsEmpty => {
                    self.reg.b1 = self
                        .reg
                        .v1
                        .as_object()
                        .map_or(true, halfbrown::SizedHashMap::is_empty);
                }
                Op::TestRecordContainsKey { key } => {
                    let key = &self.program.keys[key as usize];
                    self.reg.b1 = key.lookup(&self.reg.v1).is_some();
                }
                // Inspect
                Op::InspectLen => {
                    let len = if let Some(v) = self.reg.v1.as_array() {
                        v.len()
                    } else if let Some(v) = self.reg.v1.as_object() {
                        v.len()
                    } else {
                        return Err("Not an array or object".into());
                    };
                    stack.push(Cow::Owned(Value::from(len)));
                }

                // Records
                Op::RecordSet => {
                    let value = pop(&mut stack, *pc, *cc)?;
                    let key = pop(&mut stack, *pc, *cc)?;
                    // FIXME: we can do better than clone here
                    let key = key.into_owned().try_into_string()?;
                    self.reg.v1.to_mut().insert(key, value.into_owned())?;
                }
                Op::RecordRemove => {
                    let key = pop(&mut stack, *pc, *cc)?;
                    let key = key.try_as_str()?;
                    let v = self.reg.v1.to_mut().remove(key)?.unwrap_or_default();
                    stack.push(Cow::Owned(v));
                }
                Op::RecordGet => {
                    let key = pop(&mut stack, *pc, *cc)?;
                    let key = key.try_as_str()?;
                    // FIXME: can we avoid this clone here
                    let v = self.reg.v1.get(key).ok_or("not a record")?.clone();
                    stack.push(Cow::Owned(v));
                }
                Op::RecordMergeKey => {
                    let key_val = pop(&mut stack, *pc, *cc)?;
                    let key = key_val.try_as_str()?;
                    let arg = pop(&mut stack, *pc, *cc)?;

                    let obj = self.reg.v1.to_mut().as_object_mut().ok_or("needs object")?;

                    let target = obj
                        .entry(key.to_string().into())
                        .or_insert_with(|| Value::object_with_capacity(32));
                    merge_values(target, &arg)?;
                }
                Op::RecordMerge => {
                    let arg = pop(&mut stack, *pc, *cc)?;
                    merge_values(self.reg.v1.to_mut(), &arg)?;
                }
                // FIXME: this is kind akeward, we use the stack here instead of the register
                Op::RecordPop => {
                    let obj = last_mut(&mut stack, *pc, *cc)?
                        .to_mut()
                        .as_object_mut()
                        .ok_or("needs object")?;
                    let key = obj.keys().next().ok_or("Empty object")?.clone();
                    let v = obj.remove(&key).unwrap_or_default();
                    stack.push(Cow::Owned(v));
                    stack.push(Cow::Owned(key.into()));
                }
                // Path
                Op::GetKey { key } => {
                    let key = &self.program.keys[key as usize];
                    let v = pop(&mut stack, *pc, *cc)?;
                    // FIXME: can we avoid this clone here
                    let res = key.lookup(&v).ok_or("Missing Key FIXME")?.clone();
                    stack.push(Cow::Owned(res));
                }
                Op::GetKeyRegV1 { key } => {
                    let key = &self.program.keys[key as usize];
                    // FIXME: can we avoid this clone here
                    let res = key.lookup(&self.reg.v1).ok_or("Missing Key FIXME")?.clone();
                    stack.push(Cow::Owned(res));
                }
                Op::Get => {
                    let key = pop(&mut stack, *pc, *cc)?;
                    let v = pop(&mut stack, *pc, *cc)?;
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
                    let idx = pop(&mut stack, *pc, *cc)?;
                    let idx = idx.try_as_usize()?;
                    let v = pop(&mut stack, *pc, *cc)?;
                    let v = v.get_idx(idx).ok_or("Index out of bounds")?.clone();
                    stack.push(Cow::Owned(v));
                }
                Op::IndexFast { idx } => {
                    let idx = idx as usize;
                    let v = pop(&mut stack, *pc, *cc)?;
                    let v = v.get_idx(idx).ok_or("Index out of bounds")?.clone();
                    stack.push(Cow::Owned(v));
                }
                Op::Range => {
                    let end = pop(&mut stack, *pc, *cc)?;
                    let start = pop(&mut stack, *pc, *cc)?;
                    let end = end.try_as_usize()?;
                    let start = start.try_as_usize()?;
                    let v = pop(&mut stack, *pc, *cc)?;
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
                    let v = pop(&mut stack, *pc, *cc)?;
                    let v = v
                        .try_as_array()?
                        .get(start..end)
                        .ok_or("Index out of bounds")?
                        .to_vec();
                    stack.push(Cow::Owned(v.into()));
                }

                // Array
                // FIXME: this is kind akeward, we use the stack here instead of the register
                Op::ArrayPop => {
                    let arr = last_mut(&mut stack, *pc, *cc)?
                        .to_mut()
                        .as_array_mut()
                        .ok_or("needs array")?;
                    let v = arr
                        .pop()
                        .ok_or_else(|| error_generic(mid, mid, &"Empty array"))?;
                    stack.push(Cow::Owned(v));
                }
                Op::ArrayReverse => {
                    let v = last_mut(&mut stack, *pc, *cc)?;
                    let arr = v.to_mut().as_array_mut().ok_or("needs array")?;
                    arr.reverse();
                }
            }
            *pc += 1;
        }

        let value = pop(&mut stack, *pc, *cc)?;
        Ok(Return::Emit {
            value: value.into_owned(),
            port: None,
        })
    }
}

/// This function is unsafe since it works with pointers.
/// It remains safe since we never leak the pointer and just
/// traverse the nested value a pointer at a time.
unsafe fn nested_assign(
    elements: u16,
    stack: &mut Vec<Cow<Value>>,
    tmp: &mut *mut Value,
    mid: &NodeMeta,
    pc: usize,
    cc: usize,
) -> Result<()> {
    for _ in 0..elements {
        let target = pop(stack, pc, cc)?;
        if let Some(idx) = target.as_usize() {
            let array = tmp
                .as_mut()
                .ok_or("this is nasty, we have a null pointer")?
                .as_array_mut()
                .ok_or("needs object")?;

            *tmp = std::ptr::from_mut::<Value>(match array.get_mut(idx) {
                Some(v) => v,
                None => return Err("Index out of bounds".into()),
            });
        } else if let Some(key) = target.as_str() {
            let map = tmp
                .as_mut()
                .ok_or("this is nasty, we have a null pointer")?
                .as_object_mut()
                .ok_or("needs object")?;
            *tmp = std::ptr::from_mut::<Value>(match map.get_mut(key) {
                Some(v) => v,
                None => map
                    .entry(key.to_string().into())
                    .or_insert_with(|| Value::object_with_capacity(32)),
            });
        } else {
            return Err(error_generic(mid, mid, &"Invalid key type"));
        }
    }
    Ok(())
}

#[inline]
fn pop<'run, 'event>(
    stack: &mut Vec<Cow<'run, Value<'event>>>,
    pc: usize,
    cc: usize,
) -> Result<Cow<'run, Value<'event>>> {
    Ok(stack
        .pop()
        .ok_or_else(|| format!("Stack underflow @{pc}:{cc}"))?)
}

#[inline]
fn last<'call, 'run, 'event>(
    stack: &'call [Cow<'run, Value<'event>>],
    pc: usize,
    cc: usize,
) -> Result<&'call Cow<'run, Value<'event>>> {
    Ok(stack
        .last()
        .ok_or_else(|| format!("Stack underflow @{pc}:{cc}"))?)
}
#[inline]
fn last_mut<'call, 'run, 'event>(
    stack: &'call mut [Cow<'run, Value<'event>>],
    pc: usize,
    cc: usize,
) -> Result<&'call mut Cow<'run, Value<'event>>> {
    Ok(stack
        .last_mut()
        .ok_or_else(|| format!("Stack underflow @{pc}:{cc}",))?)
}

#[cfg(test)]
mod tests;
