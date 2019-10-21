// Copyright 2018-2019, Wayfair GmbH
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

// NOTE: we use a lot of arguments here, we are aware of that but tough luck
// FIXME: investigate if re-writing would make code better
#![allow(clippy::too_many_arguments)]

// FIXME possible optimisations:
// * P001 [x] re-write `let x = merge x of ... end` to a mutable merge that does not require cloing `x`
// * P002 [x] don't construct data for expressions that return value is never used
// * P003 [x] don't clone as part of a match statement (we should never ever mutate in case or when)
// * P004 [x] turn local variables into a pre-defined vector to improve access
// * P005 [x] turn literals into BorrowedValues so we don't need to re-cast them - constants could be pre-laoded
// * P008 [x] We should not need to clone values in the for comprehension twice.

// FIXME todo
// * 101 [ ] `%{x > 3}` and other comparisons
// * 102 [x] Remove the need for `()` around when clauses that contain binary ops

mod expr;
mod imut_expr;

use crate::ast::*;
use crate::errors::*;
use crate::registry::Context;
use crate::stry;
use halfbrown::hashmap;
use halfbrown::HashMap;
use simd_json::borrowed::Value;
use simd_json::value::ValueTrait;
use std::borrow::Borrow;
use std::borrow::Cow;
use std::default::Default;
use std::iter::Iterator;

pub use self::expr::Cont;

const TRUE: Value<'static> = Value::Bool(true);
const FALSE: Value<'static> = Value::Bool(false);
const NULL: Value<'static> = Value::Null;

macro_rules! static_bool {
    ($e:expr) => {
        if $e {
            Cow::Borrowed(&TRUE)
        } else {
            Cow::Borrowed(&FALSE)
        }
    };
}

#[derive(Clone, Debug)]
pub struct LocalValue<'value> {
    v: Value<'value>,
}
#[derive(Default, Debug)]
pub struct LocalStack<'stack> {
    values: Vec<Option<LocalValue<'stack>>>,
}

impl<'stack> LocalStack<'stack> {
    pub fn with_size(size: usize) -> Self {
        Self {
            values: vec![None; size],
        }
    }
}

impl<'value> Default for LocalValue<'value> {
    fn default() -> Self {
        Self { v: Value::Null }
    }
}

#[inline]
fn val_eq<'event>(lhs: &Value<'event>, rhs: &Value<'event>) -> bool {
    use Value::*;
    let error = std::f64::EPSILON;
    match (lhs, rhs) {
        (Object(l), Object(r)) => {
            if l.len() == r.len() {
                l.iter()
                    .all(|(k, lv)| r.get(k).map(|rv| val_eq(lv, rv)) == Some(true))
            } else {
                false
            }
        }
        (Array(l), Array(r)) => {
            if l.len() == r.len() {
                l.iter().zip(r.iter()).all(|(l, r)| val_eq(l, r))
            } else {
                false
            }
        }
        (Bool(l), Bool(r)) => *l == *r,
        (String(l), String(r)) => *l == *r,
        (I64(l), I64(r)) => *l == *r,
        (I64(l), F64(r)) => ((*l as f64) - *r).abs() < error,
        (F64(l), I64(r)) => (*l - (*r as f64)).abs() < error,
        (F64(l), F64(r)) => (*l - *r).abs() < error,
        _ => false, // Error case
    }
}

#[allow(clippy::cognitive_complexity)]
#[inline]
pub fn exec_binary<'run, 'event: 'run>(
    op: BinOpKind,
    lhs: &Value<'event>,
    rhs: &Value<'event>,
) -> Option<Cow<'run, Value<'event>>> {
    // Lazy Heinz doesn't want to write that 10000 times
    // - snot badger - Darach
    use BinOpKind::*;
    use Value::*;
    match (&op, lhs, rhs) {
        (Eq, Null, Null) => Some(static_bool!(true)),
        (NotEq, Null, Null) => Some(static_bool!(false)),
        (And, Bool(l), Bool(r)) => Some(static_bool!(*l && *r)),
        (Or, Bool(l), Bool(r)) => Some(static_bool!(*l || *r)),

        (BitAnd, I64(l), I64(r)) => Some(Cow::Owned(I64(*l & *r))),
        (BitAnd, Bool(l), Bool(r)) => Some(static_bool!(*l & *r)),
        (BitOr, I64(l), I64(r)) => Some(Cow::Owned(I64(*l | *r))),
        (BitOr, Bool(l), Bool(r)) => Some(static_bool!(*l | *r)),
        (BitXor, I64(l), I64(r)) => Some(Cow::Owned(I64(*l ^ *r))),
        (BitXor, Bool(l), Bool(r)) => Some(static_bool!(*l ^ *r)),

        // FIXME - do we want this?
        // This is to make sure that == in a expression
        // and a record pattern behaves the same.
        (Eq, l, r) => Some(static_bool!(val_eq(l, r))),
        (NotEq, l, r) => Some(static_bool!(!val_eq(l, r))),

        (Gte, I64(l), I64(r)) => Some(static_bool!(*l >= *r)),
        (Gte, I64(l), F64(r)) => Some(static_bool!((*l as f64) >= *r)),
        (Gte, F64(l), I64(r)) => Some(static_bool!(*l >= (*r as f64))),
        (Gte, F64(l), F64(r)) => Some(static_bool!(*l >= *r)),
        (Gte, String(l), String(r)) => Some(static_bool!(l >= r)),
        (Gt, I64(l), I64(r)) => Some(static_bool!(*l > *r)),
        (Gt, I64(l), F64(r)) => Some(static_bool!((*l as f64) > *r)),
        (Gt, F64(l), I64(r)) => Some(static_bool!(*l > (*r as f64))),
        (Gt, F64(l), F64(r)) => Some(static_bool!(*l > *r)),
        (Gt, String(l), String(r)) => Some(static_bool!(l > r)),
        (Lt, I64(l), I64(r)) => Some(static_bool!(*l < *r)),
        (Lt, I64(l), F64(r)) => Some(static_bool!((*l as f64) < *r)),
        (Lt, F64(l), I64(r)) => Some(static_bool!(*l < (*r as f64))),
        (Lt, F64(l), F64(r)) => Some(static_bool!(*l < *r)),
        (Lt, String(l), String(r)) => Some(static_bool!(l < r)),
        (Lte, I64(l), I64(r)) => Some(static_bool!(*l <= *r)),
        (Lte, I64(l), F64(r)) => Some(static_bool!((*l as f64) <= *r)),
        (Lte, F64(l), I64(r)) => Some(static_bool!(*l <= (*r as f64))),
        (Lte, F64(l), F64(r)) => Some(static_bool!(*l <= *r)),
        (Lte, String(l), String(r)) => Some(static_bool!(l <= r)),

        // TODO use shl functions in https://doc.rust-lang.org/std/primitive.i64.html
        (RBitShiftSigned, I64(l), I64(r)) => Some(Cow::Owned(I64(*l >> *r))),
        (RBitShiftUnsigned, I64(l), I64(r)) => Some(Cow::Owned(I64((*l as u64 >> *r) as i64))),
        (LBitShift, I64(l), I64(r)) => Some(Cow::Owned(I64(*l << *r))),

        (Add, String(l), String(r)) => Some(Cow::Owned(format!("{}{}", *l, *r).into())),
        (Add, I64(l), I64(r)) => Some(Cow::Owned(I64(*l + *r))),
        (Add, I64(l), F64(r)) => Some(Cow::Owned(F64((*l as f64) + *r))),
        (Add, F64(l), I64(r)) => Some(Cow::Owned(F64(*l + (*r as f64)))),
        (Add, F64(l), F64(r)) => Some(Cow::Owned(F64(*l + *r))),
        (Sub, I64(l), I64(r)) => Some(Cow::Owned(I64(*l - *r))),
        (Sub, I64(l), F64(r)) => Some(Cow::Owned(F64((*l as f64) - *r))),
        (Sub, F64(l), I64(r)) => Some(Cow::Owned(F64(*l - (*r as f64)))),
        (Sub, F64(l), F64(r)) => Some(Cow::Owned(F64(*l - *r))),
        (Mul, I64(l), I64(r)) => Some(Cow::Owned(I64(*l * *r))),
        (Mul, I64(l), F64(r)) => Some(Cow::Owned(F64((*l as f64) * *r))),
        (Mul, F64(l), I64(r)) => Some(Cow::Owned(F64(*l * (*r as f64)))),
        (Mul, F64(l), F64(r)) => Some(Cow::Owned(F64(*l * *r))),
        (Div, I64(l), I64(r)) => Some(Cow::Owned(F64((*l as f64) / (*r as f64)))),
        (Div, I64(l), F64(r)) => Some(Cow::Owned(F64((*l as f64) / *r))),
        (Div, F64(l), I64(r)) => Some(Cow::Owned(F64(*l / (*r as f64)))),
        (Div, F64(l), F64(r)) => Some(Cow::Owned(F64(*l / *r))),
        (Mod, I64(l), I64(r)) => Some(Cow::Owned(I64(*l % *r))),
        _ => None,
    }
}

#[inline]
pub fn exec_unary<'run, 'event: 'run>(
    op: UnaryOpKind,
    val: &Value<'event>,
) -> Option<Cow<'run, Value<'event>>> {
    // Lazy Heinz doesn't want to write that 10000 times
    // - snot badger - Darach
    use UnaryOpKind::*;
    use Value::*;
    match (&op, val) {
        (Minus, I64(x)) => Some(Cow::Owned(I64(-*x))),
        (Minus, F64(x)) => Some(Cow::Owned(F64(-*x))),
        (Plus, I64(x)) => Some(Cow::Owned(I64(*x))),
        (Plus, F64(x)) => Some(Cow::Owned(F64(*x))),
        (Not, Bool(true)) => Some(static_bool!(false)), // This is not true
        (Not, Bool(false)) => Some(static_bool!(true)), // this is not false
        (BitNot, I64(x)) => Some(Cow::Owned(I64(!x))),
        (BitNot, Bool(true)) => Some(static_bool!(false)), // This is not true
        (BitNot, Bool(false)) => Some(static_bool!(true)), // this is not false
        _ => None,
    }
}

#[inline]
pub fn resolve<'run, 'event, 'script, Ctx, Expr>(
    outer: &'script Expr,
    context: &'run Ctx,
    event: &'run Value<'event>,
    meta: &'run Value<'event>,
    local: &'run LocalStack<'event>,
    consts: &'run [Value<'event>],
    path: &'script Path<Ctx>,
) -> Result<Cow<'run, Value<'event>>>
where
    Expr: BaseExpr,
    Ctx: Context,
    'script: 'event,
    'event: 'run,
{
    let mut subrange: Option<(usize, usize)> = None;
    let mut current: &Value = match path {
        Path::Local(lpath) => match local.values.get(lpath.idx) {
            Some(Some(l)) => &l.v,
            Some(None) => {
                return error_bad_key(outer, lpath, &path, lpath.id.to_string(), vec![]);
            }

            _ => return error_oops(outer),
        },
        Path::Const(lpath) => match consts.get(lpath.idx) {
            Some(v) => v,
            _ => return error_oops(outer),
        },
        Path::Meta(_path) => meta,
        Path::Event(_path) => event,
    };

    for segment in path.segments() {
        match segment {
            Segment::IdSelector { id, .. } => {
                if let Some(o) = current.as_object() {
                    if let Some(c) = o.get(id) {
                        current = c;
                        subrange = None;
                        continue;
                    } else {
                        return error_bad_key(
                            outer,
                            segment, //&Expr::dummy(*start, *end),
                            &path,
                            id.to_string(),
                            o.keys().map(|v| v.to_string()).collect(),
                        );
                    }
                } else {
                    return error_type_conflict(outer, segment, current.kind(), ValueType::Object);
                }
            }
            Segment::IdxSelector { idx, .. } => {
                if let Some(a) = current.as_array() {
                    let (start, end) = if let Some((start, end)) = subrange {
                        // We check range on setting the subrange!
                        (start, end)
                    } else {
                        (0, a.len())
                    };
                    let idx = *idx as usize + start;
                    if idx >= end {
                        // We exceed the sub range
                        return Ok(Cow::Borrowed(&FALSE));
                    }

                    if let Some(c) = a.get(idx) {
                        current = c;
                        subrange = None;
                        continue;
                    } else {
                        return error_array_out_of_bound(outer, segment, &path, idx..idx);
                    }
                } else {
                    return error_type_conflict(outer, segment, current.kind(), ValueType::Array);
                }
            }
            Segment::ElementSelector { expr, .. } => {
                let key = stry!(expr.run(context, event, meta, local, consts));

                match (current, key.borrow()) {
                    (Value::Object(o), Value::String(id)) => {
                        if let Some(v) = o.get(id) {
                            current = v;
                            subrange = None;
                            continue;
                        } else {
                            return error_bad_key(
                                outer,
                                segment,
                                &path,
                                id.to_string(),
                                o.keys().map(|v| v.to_string()).collect(),
                            );
                        }
                    }
                    (Value::Object(_), other) => {
                        return error_type_conflict(outer, segment, other.kind(), ValueType::String)
                    }
                    (Value::Array(a), Value::I64(idx)) => {
                        let (start, end) = if let Some((start, end)) = subrange {
                            // We check range on setting the subrange!
                            (start, end)
                        } else {
                            (0, a.len())
                        };
                        let idx = *idx as usize + start;
                        if idx >= end {
                            // We exceed the sub range
                            return Ok(Cow::Borrowed(&FALSE));
                        }

                        if let Some(v) = a.get(idx) {
                            current = v;
                            subrange = None;
                            continue;
                        } else {
                            return error_array_out_of_bound(outer, segment, &path, idx..idx);
                        }
                    }
                    (Value::Array(_), other) => {
                        return error_type_conflict(outer, segment, other.kind(), ValueType::I64)
                    }
                    (other, Value::String(_)) => {
                        return error_type_conflict(outer, segment, other.kind(), ValueType::Object)
                    }
                    (other, Value::I64(_)) => {
                        return error_type_conflict(outer, segment, other.kind(), ValueType::Array)
                    }
                    _ => return error_oops(outer),
                }
            }

            Segment::RangeSelector {
                range_start,
                range_end,
                ..
            } => {
                if let Some(a) = current.as_array() {
                    let (start, end) = if let Some((start, end)) = subrange {
                        // We check range on setting the subrange!
                        (start, end)
                    } else {
                        (0, a.len())
                    };
                    let s = stry!(range_start.run(context, event, meta, local, consts));
                    if let Some(range_start) = s.as_u64() {
                        let range_start = range_start as usize + start;

                        let e = stry!(range_end.run(context, event, meta, local, consts));
                        if let Some(range_end) = e.as_u64() {
                            let range_end = range_end as usize + end;
                            if range_end >= end {
                                return error_array_out_of_bound(
                                    outer,
                                    segment,
                                    &path,
                                    range_start..range_end,
                                );
                            } else {
                                subrange = Some((range_start, range_end));
                                continue;
                            }
                        } else {
                            let re: &ImutExpr<Ctx> = range_end.borrow();
                            return error_type_conflict(outer, re, e.kind(), ValueType::I64);
                        }
                    } else {
                        let rs: &ImutExpr<Ctx> = range_start.borrow();
                        return error_type_conflict(outer, rs.borrow(), s.kind(), ValueType::I64);
                    }
                } else {
                    return error_type_conflict(outer, segment, current.kind(), ValueType::Array);
                }
            }
        }
    }
    if let Some((start, end)) = subrange {
        if let Some(a) = current.as_array() {
            // We check the range whemn we set it;
            let sub = unsafe { a.get_unchecked(start..end).to_vec() };
            Ok(Cow::Owned(Value::Array(sub)))
        } else {
            // We check this when we set the subrange!
            unreachable!();
        }
    } else {
        Ok(Cow::Borrowed(current))
    }
}

fn merge_values<'run, 'event, 'script, Outer, Inner>(
    outer: &'script Outer,
    inner: &Inner,
    value: &'run mut Value<'event>,
    replacement: &'run Value<'event>,
) -> Result<()>
where
    Outer: BaseExpr,
    Inner: BaseExpr,
    'script: 'event,
    'event: 'run,
{
    if !replacement.is_object() {
        //NOTE: We got to clone here since we're duplicating values
        *value = replacement.clone();
        return Ok(());
    }

    if !value.is_object() {
        *value = Value::Object(hashmap! {});
    }

    match value {
        Value::Object(ref mut map) => {
            match replacement {
                Value::Object(rep) => {
                    for (k, v) in rep {
                        if v.is_null() {
                            map.remove(k);
                        } else {
                            match map.get_mut(k) {
                                Some(k) => stry!(merge_values(outer, inner, k, v)),
                                None => {
                                    //NOTE: We got to clone here since we're duplicating values
                                    map.insert(k.clone(), v.clone());
                                }
                            }
                        }
                    }
                }
                other => return error_type_conflict(outer, inner, other.kind(), ValueType::Object),
            }
        }
        other => return error_type_conflict(outer, inner, other.kind(), ValueType::Object),
    }

    Ok(())
}

#[inline]
fn patch_value<'run, 'event, 'script, Ctx, Expr>(
    outer: &'script Expr,
    context: &'run Ctx,
    event: &'run Value<'event>,
    meta: &'run Value<'event>,
    local: &'run LocalStack<'event>,
    consts: &'run [Value<'event>],
    value: &'run mut Value<'event>,
    expr: &'script Patch<Ctx>,
) -> Result<()>
where
    Expr: BaseExpr,
    Ctx: Context,
    'script: 'event,
    'event: 'run,
{
    for op in &expr.operations {
        // NOTE: This if is inside the for loop to prevent obj to be updated
        // between iterations and possibly lead to dangling pointers
        if let Value::Object(ref mut obj) = value {
            match op {
                PatchOperation::Insert { ident, expr } => {
                    let new_key = stry!(ident.eval_to_string(context, event, meta, local, consts));
                    let new_value = stry!(expr.run(context, event, meta, local, consts));
                    if obj.contains_key(&new_key) {
                        return error_patch_key_exists(outer, expr, new_key.to_string());
                    } else {
                        obj.insert(new_key, new_value.into_owned());
                    }
                }
                PatchOperation::Update { ident, expr } => {
                    let new_key = stry!(ident.eval_to_string(context, event, meta, local, consts));
                    let new_value = stry!(expr.run(context, event, meta, local, consts));
                    if obj.contains_key(&new_key) {
                        obj.insert(new_key, new_value.into_owned());
                    } else {
                        return error_patch_update_key_missing(outer, expr, new_key.to_string());
                    }
                }
                PatchOperation::Upsert { ident, expr } => {
                    let new_key = stry!(ident.eval_to_string(context, event, meta, local, consts));
                    let new_value = stry!(expr.run(context, event, meta, local, consts));
                    obj.insert(new_key, new_value.into_owned());
                }
                PatchOperation::Erase { ident } => {
                    let new_key = stry!(ident.eval_to_string(context, event, meta, local, consts));
                    obj.remove(&new_key);
                }
                PatchOperation::Move { from, to } => {
                    let from = stry!(from.eval_to_string(context, event, meta, local, consts));
                    let to = stry!(to.eval_to_string(context, event, meta, local, consts));

                    if obj.contains_key(&to) {
                        return error_patch_key_exists(outer, expr, to.to_string());
                    }
                    if let Some(old) = obj.remove(&from) {
                        obj.insert(to, old);
                    }
                }
                PatchOperation::Copy { from, to } => {
                    let from = stry!(from.eval_to_string(context, event, meta, local, consts));
                    let to = stry!(to.eval_to_string(context, event, meta, local, consts));

                    if obj.contains_key(&to) {
                        return error_patch_key_exists(outer, expr, to.to_string());
                    }
                    if let Some(old) = obj.get(&from) {
                        let old = old.clone();
                        obj.insert(to, old);
                    }
                }
                PatchOperation::Merge { ident, expr } => {
                    let new_key = stry!(ident.eval_to_string(context, event, meta, local, consts));
                    let merge_spec = stry!(expr.run(context, event, meta, local, consts));

                    match obj.get_mut(&new_key) {
                        Some(value @ Value::Object(_)) => {
                            stry!(merge_values(outer, expr, value, &merge_spec));
                        }
                        Some(other) => {
                            return error_patch_merge_type_conflict(
                                outer,
                                expr,
                                new_key.to_string(),
                                &other,
                            );
                        }
                        None => {
                            let mut new_value = Value::Object(hashmap! {});
                            stry!(merge_values(outer, expr, &mut new_value, &merge_spec));
                            obj.insert(new_key, new_value);
                        }
                    }
                }
                PatchOperation::TupleMerge { expr } => {
                    let merge_spec = stry!(expr.run(context, event, meta, local, consts));

                    stry!(merge_values(outer, expr, value, &merge_spec));
                }
            }
        } else {
            return error_type_conflict(outer, &expr.target, value.kind(), ValueType::Object);
        }
    }
    Ok(())
}

#[inline]
fn test_guard<'run, 'event, 'script, Ctx, Expr>(
    outer: &'script Expr,
    context: &'run Ctx,
    event: &'run Value<'event>,
    meta: &'run Value<'event>,
    local: &'run LocalStack<'event>,
    consts: &'run [Value<'event>],
    guard: &'script Option<ImutExpr<'script, Ctx>>,
) -> Result<bool>
where
    Expr: BaseExpr,
    Ctx: Context,
    'script: 'event,
    'event: 'run,
{
    if let Some(guard) = guard {
        let test = stry!(guard.run(context, event, meta, local, consts));
        match test.borrow() {
            Value::Bool(b) => Ok(*b),
            other => error_guard_not_bool(outer, guard, other),
        }
    } else {
        Ok(true)
    }
}

#[inline]
fn test_predicate_expr<'run, 'event, 'script, Ctx, Expr>(
    outer: &'script Expr,
    context: &'run Ctx,
    event: &'run Value<'event>,
    meta: &'run Value<'event>,
    local: &'run LocalStack<'event>,
    consts: &'run [Value<'event>],
    target: &'run Value<'event>,
    pattern: &'script Pattern<'script, Ctx>,
    guard: &'run Option<ImutExpr<'script, Ctx>>,
) -> Result<bool>
where
    Expr: BaseExpr,
    Ctx: Context,
    'script: 'event,
    'event: 'run,
{
    match pattern {
        Pattern::Record(ref rp) => {
            if stry!(match_rp_expr(
                outer, false, context, event, meta, local, consts, &target, &rp
            ))
            .is_some()
            {
                test_guard(outer, context, event, meta, local, consts, guard)
            } else {
                Ok(false)
            }
        }
        Pattern::Array(ref ap) => {
            if stry!(match_ap_expr(
                outer, false, context, event, meta, local, consts, &target, &ap
            ))
            .is_some()
            {
                test_guard(outer, context, event, meta, local, consts, guard)
            } else {
                Ok(false)
            }
        }
        Pattern::Expr(ref expr) => {
            let v = stry!(expr.run(context, event, meta, local, consts));
            let vb: &Value = v.borrow();
            if val_eq(target, vb) {
                test_guard(outer, context, event, meta, local, consts, guard)
            } else {
                Ok(false)
            }
        }
        Pattern::Assign(ref a) => {
            match *a.pattern {
                Pattern::Array(ref ap) => {
                    if let Some(v) = stry!(match_ap_expr(
                        outer, true, context, event, meta, local, consts, &target, &ap,
                    )) {
                        // we need to assign prior to the guard so we can cehck
                        // against the pattern expressions
                        stry!(set_local_shadow(outer, local, a.idx, v));
                        test_guard(outer, context, event, meta, local, consts, guard)
                    } else {
                        Ok(false)
                    }
                }
                Pattern::Record(ref rp) => {
                    if let Some(v) = stry!(match_rp_expr(
                        outer, true, context, event, meta, local, consts, &target, &rp,
                    )) {
                        // we need to assign prior to the guard so we can cehck
                        // against the pattern expressions
                        stry!(set_local_shadow(outer, local, a.idx, v));

                        test_guard(outer, context, event, meta, local, consts, guard)
                    } else {
                        Ok(false)
                    }
                }
                Pattern::Expr(ref expr) => {
                    let v = stry!(expr.run(context, event, meta, local, consts));
                    let vb: &Value = v.borrow();
                    if val_eq(target, vb) {
                        // we need to assign prior to the guard so we can cehck
                        // against the pattern expressions
                        stry!(set_local_shadow(outer, local, a.idx, v.into_owned()));

                        test_guard(outer, context, event, meta, local, consts, guard)
                    } else {
                        Ok(false)
                    }
                }
                _ => error_oops(outer),
            }
        }
        Pattern::Default => Ok(true),
    }
}

#[inline]
fn match_rp_expr<'run, 'event, 'script, Ctx, Expr>(
    outer: &'script Expr,
    result_needed: bool,
    context: &'run Ctx,
    event: &'run Value<'event>,
    meta: &'run Value<'event>,
    local: &'run LocalStack<'event>,
    consts: &'run [Value<'event>],
    target: &'run Value<'event>,
    rp: &'script RecordPattern<Ctx>,
) -> Result<Option<Value<'event>>>
where
    Expr: BaseExpr,
    Ctx: Context,
    'script: 'event,
    'event: 'run,
{
    let mut acc = HashMap::with_capacity(if result_needed { rp.fields.len() } else { 0 });
    for pp in &rp.fields {
        let key: Cow<str> = pp.lhs().into();

        match pp {
            PredicatePattern::TildeEq { test, .. } => {
                let testee = match target.as_object() {
                    Some(ref o) => {
                        if let Some(v) = o.get(&key) {
                            v
                        } else {
                            return Ok(None);
                        }
                    }
                    _ => return Ok(None),
                };
                if let Ok(x) = test.extractor.extract(result_needed, &testee, context) {
                    if result_needed {
                        acc.insert(key, x);
                    }
                } else {
                    return Ok(None);
                }
            }
            PredicatePattern::Eq { rhs, not, .. } => {
                let testee = match target.as_object() {
                    Some(ref o) => {
                        if let Some(v) = o.get(&key) {
                            v
                        } else {
                            return Ok(None);
                        }
                    }
                    _ => return Ok(None),
                };
                let rhs = stry!(rhs.run(context, event, meta, local, consts));
                let vb: &Value = rhs.borrow();
                let r = val_eq(testee, vb);
                let m = if *not { !r } else { r };

                if m {
                    continue;
                } else {
                    return Ok(None);
                }
            }
            PredicatePattern::FieldPresent { .. } => match target.as_object() {
                Some(ref o) => {
                    if let Some(v) = o.get(&key) {
                        if result_needed {
                            acc.insert(key, v.clone());
                        }
                        continue;
                    } else {
                        return Ok(None);
                    }
                }
                _ => return Ok(None),
            },
            PredicatePattern::FieldAbsent { .. } => match target.as_object() {
                Some(ref o) => {
                    if !o.contains_key(&key) {
                        continue;
                    } else {
                        return Ok(None);
                    }
                }
                _ => return Ok(None),
            },
            PredicatePattern::RecordPatternEq { pattern, .. } => {
                let testee = match target.as_object() {
                    Some(ref o) => {
                        if let Some(v) = o.get(&key) {
                            v
                        } else {
                            return Ok(None);
                        }
                    }
                    _ => return Ok(None),
                };
                if testee.is_object() {
                    match stry!(match_rp_expr(
                        outer,
                        result_needed,
                        context,
                        event,
                        meta,
                        local,
                        consts,
                        testee,
                        pattern,
                    )) {
                        Some(m) => {
                            if result_needed {
                                acc.insert(key, m);
                            }
                            continue;
                        }
                        _ => {
                            return Ok(None);
                        }
                    }
                } else {
                    return Ok(None);
                }
            }
            PredicatePattern::ArrayPatternEq { pattern, .. } => {
                let testee = match target.as_object() {
                    Some(ref o) => {
                        if let Some(v) = o.get(&key) {
                            v
                        } else {
                            return Ok(None);
                        }
                    }
                    _ => return Ok(None),
                };
                if testee.is_array() {
                    match stry!(match_ap_expr(
                        outer,
                        result_needed,
                        context,
                        event,
                        meta,
                        local,
                        consts,
                        testee,
                        pattern,
                    )) {
                        Some(r) => {
                            if result_needed {
                                acc.insert(key, r);
                            }

                            continue;
                        }
                        None => {
                            return Ok(None);
                        }
                    }
                } else {
                    return Ok(None);
                }
            }
        }
    }

    Ok(Some(Value::Object(acc)))
}

#[inline]
fn match_ap_expr<'run, 'event, 'script, Ctx, Expr>(
    outer: &'script Expr,
    result_needed: bool,
    context: &'run Ctx,
    event: &'run Value<'event>,
    meta: &'run Value<'event>,
    local: &'run LocalStack<'event>,
    consts: &'run [Value<'event>],
    target: &'run Value<'event>,
    ap: &'script ArrayPattern<Ctx>,
) -> Result<Option<Value<'event>>>
where
    Expr: BaseExpr,
    Ctx: Context,
    'script: 'event,
    'event: 'run,
{
    match target {
        Value::Array(ref a) => {
            let mut acc = Vec::with_capacity(if result_needed { a.len() } else { 0 });
            let mut idx = 0;
            for candidate in a {
                'inner: for expr in &ap.exprs {
                    match expr {
                        ArrayPredicatePattern::Expr(e) => {
                            let r = stry!(e.run(context, event, meta, local, consts));
                            let vb: &Value = r.borrow();

                            // NOTE: We are creating a new value here so we have to clone
                            if val_eq(candidate, vb) && result_needed {
                                acc.push(Value::Array(vec![Value::Array(vec![
                                    Value::I64(idx),
                                    r.into_owned(),
                                ])]));
                            }
                        }
                        ArrayPredicatePattern::Tilde(test) => {
                            if let Ok(r) =
                                test.extractor.extract(result_needed, &candidate, context)
                            {
                                if result_needed {
                                    acc.push(Value::Array(vec![Value::Array(vec![
                                        Value::I64(idx),
                                        r,
                                    ])]));
                                }
                            } else {
                                continue 'inner;
                            }
                        }
                        ArrayPredicatePattern::Record(rp) => {
                            if let Some(r) = stry!(match_rp_expr(
                                outer,
                                result_needed,
                                context,
                                event,
                                meta,
                                local,
                                consts,
                                candidate,
                                rp,
                            )) {
                                if result_needed {
                                    acc.push(Value::Array(vec![Value::Array(vec![
                                        Value::I64(idx),
                                        r,
                                    ])]))
                                };
                            } else {
                                continue 'inner;
                            }
                        }
                        ArrayPredicatePattern::Array(ap) => {
                            if let Some(r) = stry!(match_ap_expr(
                                outer,
                                result_needed,
                                context,
                                event,
                                meta,
                                local,
                                consts,
                                candidate,
                                ap,
                            )) {
                                if result_needed {
                                    acc.push(Value::Array(vec![Value::Array(vec![
                                        Value::I64(idx),
                                        r,
                                    ])]));
                                }
                            } else {
                                continue 'inner; // return Ok(Cont::Drop(Value::Bool(true))),
                            }
                        }
                    }
                }
                idx += 1;
            }
            Ok(Some(Value::Array(acc)))
        }
        _ => Ok(None),
    }
}

#[inline]
pub fn set_local_shadow<'run, 'event, 'script, Expr>(
    outer: &'script Expr,
    local: &'run LocalStack<'event>,
    idx: usize,
    v: Value<'event>,
) -> Result<()>
where
    Expr: BaseExpr,
    'script: 'event,
    'event: 'run,
{
    use std::mem;
    // This is icky do we want it?
    // it is only used
    #[allow(mutable_transmutes)]
    #[allow(clippy::transmute_ptr_to_ptr)]
    let local: &mut LocalStack = unsafe { mem::transmute(local) };
    if let Some(d) = local.values.get_mut(idx) {
        *d = Some(LocalValue { v });
        Ok(())
    } else {
        error_oops(outer)
    }
}
