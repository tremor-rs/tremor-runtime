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

// NOTE: For env / end
#![allow(clippy::similar_names)]

mod expr;
mod imut_expr;

pub use self::expr::Cont;
use crate::ast::*;
use crate::errors::*;
use crate::stry;
use crate::EventContext;
use simd_json::borrowed::{Object, Value};
use simd_json::value::{MutableValue, Value as ValueTrait};
use simd_json::StaticNode;
use std::borrow::Borrow;
use std::borrow::Cow;
use std::convert::TryInto;
use std::iter::Iterator;

pub const TRUE: Value<'static> = Value::Static(StaticNode::Bool(true));
pub const FALSE: Value<'static> = Value::Static(StaticNode::Bool(false));
pub const NULL: Value<'static> = Value::Static(StaticNode::Null);

macro_rules! static_bool {
    ($e:expr) => {
        if $e {
            Cow::Borrowed(&TRUE)
        } else {
            Cow::Borrowed(&FALSE)
        }
    };
}

pub struct Env<'run, 'event, 'script>
where
    'script: 'event,
    'event: 'run,
{
    pub context: &'run EventContext,
    pub consts: &'run [Value<'event>],
    pub aggrs: &'run [InvokeAggrFn<'script>],
    pub meta: &'run NodeMetas<'script>,
}

#[derive(Default, Debug)]
pub struct LocalStack<'stack> {
    pub values: Vec<Option<Value<'stack>>>,
}

impl<'stack> LocalStack<'stack> {
    pub fn with_size(size: usize) -> Self {
        Self {
            values: vec![None; size],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AggrType {
    Tick,
    Emit,
}
#[derive(Clone, Copy, Debug)]
pub struct ExecOpts {
    pub result_needed: bool,
    pub aggr: AggrType,
}

impl ExecOpts {
    pub fn without_result(mut self) -> Self {
        self.result_needed = false;
        self
    }
    pub fn with_result(mut self) -> Self {
        self.result_needed = true;
        self
    }
}

#[inline]
#[allow(clippy::cast_precision_loss)]
fn val_eq<'event>(lhs: &Value<'event>, rhs: &Value<'event>) -> bool {
    // FIXME Consider Tony Garnock-Jones perserves w.r.t. forcing a total ordering
    // across builtin types if/when extending for 'lt' and 'gt' variants
    //
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
        (Static(StaticNode::Bool(l)), Static(StaticNode::Bool(r))) => *l == *r,
        (String(l), String(r)) => *l == *r,
        (l, r) => {
            if let (Some(l), Some(r)) = (l.as_u64(), r.as_u64()) {
                l == r
            } else if let (Some(l), Some(r)) = (l.as_i64(), r.as_i64()) {
                l == r
            } else if let (Some(l), Some(r)) = (l.cast_f64(), r.cast_f64()) {
                (l - r).abs() < error
            } else {
                false
            }
        }
    }
}

#[allow(clippy::cognitive_complexity, clippy::cast_precision_loss)]
#[inline]
pub fn exec_binary<'run, 'event, OuterExpr, InnerExpr>(
    outer: &'run OuterExpr,
    inner: &'run InnerExpr,
    node_meta: &'run NodeMetas,
    op: BinOpKind,
    lhs: &Value<'event>,
    rhs: &Value<'event>,
) -> Result<Cow<'run, Value<'event>>>
where
    OuterExpr: BaseExpr,
    InnerExpr: BaseExpr,
    'event: 'run,
{
    // Lazy Heinz doesn't want to write that 10000 times
    // - snot badger - Darach
    use BinOpKind::*;
    use Value::*;
    match (&op, lhs, rhs) {
        (Eq, Static(StaticNode::Null), Static(StaticNode::Null)) => Ok(static_bool!(true)),
        (NotEq, Static(StaticNode::Null), Static(StaticNode::Null)) => Ok(static_bool!(false)),

        // FIXME - do we want this?
        // This is to make sure that == in a expression
        // and a record pattern behaves the same.
        (Eq, l, r) => Ok(static_bool!(val_eq(l, r))),

        (NotEq, l, r) =>
        {
            #[allow(clippy::if_not_else)]
            Ok(static_bool!(!val_eq(l, r)))
        }

        (op, Static(StaticNode::Bool(l)), Static(StaticNode::Bool(r))) => match op {
            And => Ok(static_bool!(*l && *r)),
            Or => Ok(static_bool!(*l || *r)),
            #[allow(clippy::if_not_else)]
            Xor =>
            {
                #[allow(clippy::if_not_else)]
                Ok(static_bool!(*l != *r))
            }
            BitAnd => Ok(static_bool!(*l & *r)),
            BitOr => Ok(static_bool!(*l | *r)),
            BitXor => Ok(static_bool!(*l ^ *r)),
            _ => error_invalid_binary(outer, inner, *op, lhs, rhs, &node_meta),
        },
        (op, String(l), String(r)) => match op {
            Gt => Ok(static_bool!(l > r)),
            Gte => Ok(static_bool!(l >= r)),
            Lt => Ok(static_bool!(l < r)),
            Lte => Ok(static_bool!(l <= r)),
            Add => Ok(Cow::Owned(format!("{}{}", *l, *r).into())),
            _ => error_invalid_binary(outer, inner, *op, lhs, rhs, &node_meta),
        },
        (op, l, r) => {
            if let (Some(l), Some(r)) = (l.as_u64(), r.as_u64()) {
                match op {
                    BitAnd => Ok(Cow::Owned(Value::from(l & r))),
                    BitOr => Ok(Cow::Owned(Value::from(l | r))),
                    BitXor => Ok(Cow::Owned(Value::from(l ^ r))),
                    Gt => Ok(static_bool!(l > r)),
                    Gte => Ok(static_bool!(l >= r)),
                    Lt => Ok(static_bool!(l < r)),
                    Lte => Ok(static_bool!(l <= r)),
                    Add => Ok(Cow::Owned(Value::from(l + r))),
                    Sub => Ok(Cow::Owned(Value::from(l - r))),
                    Mul => Ok(Cow::Owned(Value::from(l * r))),
                    Div => Ok(Cow::Owned(Value::from((l as f64) / (r as f64)))),
                    Mod => Ok(Cow::Owned(Value::from(l % r))),
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    RBitShiftSigned => match (l).checked_shr(r as u32) {
                        Some(n) => Ok(Cow::Owned(Value::from(n))),
                        None => error_invalid_bitshift(outer, inner, &node_meta),
                    },
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    RBitShiftUnsigned => match (l as u64).checked_shr(r as u32) {
                        #[allow(clippy::cast_possible_wrap)]
                        Some(n) => Ok(Cow::Owned(Value::from(n as i64))),
                        None => error_invalid_bitshift(outer, inner, &node_meta),
                    },
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    LBitShift => match (l).checked_shl(r as u32) {
                        Some(n) => Ok(Cow::Owned(Value::from(n))),
                        None => error_invalid_bitshift(outer, inner, &node_meta),
                    },
                    _ => error_invalid_binary(outer, inner, *op, lhs, rhs, &node_meta),
                }
            } else if let (Some(l), Some(r)) = (l.as_i64(), r.as_i64()) {
                match op {
                    BitAnd => Ok(Cow::Owned(Value::from(l & r))),
                    BitOr => Ok(Cow::Owned(Value::from(l | r))),
                    BitXor => Ok(Cow::Owned(Value::from(l ^ r))),
                    Gt => Ok(static_bool!(l > r)),
                    Gte => Ok(static_bool!(l >= r)),
                    Lt => Ok(static_bool!(l < r)),
                    Lte => Ok(static_bool!(l <= r)),
                    Add => Ok(Cow::Owned(Value::from(l + r))),
                    Sub => Ok(Cow::Owned(Value::from(l - r))),
                    Mul => Ok(Cow::Owned(Value::from(l * r))),
                    Div => Ok(Cow::Owned(Value::from((l as f64) / (r as f64)))),
                    Mod => Ok(Cow::Owned(Value::from(l % r))),
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    RBitShiftSigned => match (l).checked_shr(r as u32) {
                        Some(n) => Ok(Cow::Owned(Value::from(n))),
                        None => error_invalid_bitshift(outer, inner, &node_meta),
                    },
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    RBitShiftUnsigned => match (l as u64).checked_shr(r as u32) {
                        #[allow(clippy::cast_possible_wrap)]
                        Some(n) => Ok(Cow::Owned(Value::from(n as i64))),
                        None => error_invalid_bitshift(outer, inner, &node_meta),
                    },
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    LBitShift => match (l).checked_shl(r as u32) {
                        Some(n) => Ok(Cow::Owned(Value::from(n))),
                        None => error_invalid_bitshift(outer, inner, &node_meta),
                    },
                    _ => error_invalid_binary(outer, inner, *op, lhs, rhs, &node_meta),
                }
            } else if let (Some(l), Some(r)) = (l.cast_f64(), r.cast_f64()) {
                match op {
                    Gte => Ok(static_bool!(l >= r)),
                    Gt => Ok(static_bool!(l > r)),
                    Lt => Ok(static_bool!(l < r)),
                    Lte => Ok(static_bool!(l <= r)),
                    Add => Ok(Cow::Owned(Value::from(l + r))),
                    Sub => Ok(Cow::Owned(Value::from(l - r))),
                    Mul => Ok(Cow::Owned(Value::from(l * r))),
                    Div => Ok(Cow::Owned(Value::from(l / r))),
                    _ => error_invalid_binary(outer, inner, *op, lhs, rhs, &node_meta),
                }
            } else {
                error_invalid_binary(outer, inner, *op, lhs, rhs, &node_meta)
            }
        }
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
    if let Some(x) = val.as_u64() {
        match &op {
            Minus => x
                .try_into()
                .ok()
                .and_then(|x: i64| x.checked_neg())
                .map(|x| Cow::Owned(Value::from(x))),
            Plus => Some(Cow::Owned(Value::from(x))),
            BitNot => Some(Cow::Owned(Value::from(!x))),
            _ => None,
        }
    } else if let Some(x) = val.as_i64() {
        match &op {
            Minus => x
                .try_into()
                .ok()
                .and_then(|x: i64| x.checked_neg())
                .map(|x| Cow::Owned(Value::from(x))),
            Plus => Some(Cow::Owned(Value::from(x))),
            BitNot => Some(Cow::Owned(Value::from(!x))),
            _ => None,
        }
    } else if let Some(x) = val.as_bool() {
        match &op {
            BitNot | Not => Some(static_bool!(x)),
            _ => None,
        }
    } else {
        None
    }
}

#[inline]
pub fn resolve<'run, 'event, 'script, Expr>(
    outer: &'script Expr,
    opts: ExecOpts,
    env: &'run Env<'run, 'event, 'script>,
    event: &'run Value<'event>,
    meta: &'run Value<'event>,
    local: &'run LocalStack<'event>,
    path: &'script Path,
) -> Result<Cow<'run, Value<'event>>>
where
    Expr: BaseExpr,
    'script: 'event,
    'event: 'run,
{
    let mut subrange: Option<(usize, usize)> = None;
    let mut current: &Value = match path {
        Path::Local(lpath) => match local.values.get(lpath.idx) {
            Some(Some(l)) => l,
            Some(None) => {
                return error_bad_key(
                    outer,
                    lpath,
                    &path,
                    env.meta.name_dflt(lpath.mid).to_string(),
                    vec![],
                    &env.meta,
                );
            }

            _ => return error_oops(outer, "Use of unknown local value", &env.meta),
        },
        Path::Const(lpath) => match env.consts.get(lpath.idx) {
            Some(v) => v,
            _ => return error_oops(outer, "Use of uninitalized constant", &env.meta),
        },
        Path::Meta(_path) => meta,
        Path::Event(_path) => event,
    };

    for segment in path.segments() {
        match segment {
            Segment::Id { mid, key, .. } => {
                if let Some(c) = key.lookup(current) {
                    current = c;
                    subrange = None;
                    continue;
                } else if let Some(o) = current.as_object() {
                    return error_bad_key(
                        outer,
                        segment, //&Expr::dummy(*start, *end),
                        &path,
                        env.meta.name_dflt(*mid).to_string(),
                        o.keys().map(ToString::to_string).collect(),
                        &env.meta,
                    );
                } else {
                    return error_need_obj(outer, segment, current.value_type(), &env.meta);
                }
            }
            Segment::Idx { idx, .. } => {
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
                        let bad_idx = idx - start;
                        return error_array_out_of_bound(
                            outer,
                            segment,
                            &path,
                            bad_idx..bad_idx,
                            &env.meta,
                        );
                    }

                    if let Some(c) = a.get(idx) {
                        current = c;
                        subrange = None;
                        continue;
                    } else {
                        return error_array_out_of_bound(
                            outer,
                            segment,
                            &path,
                            idx..idx,
                            &env.meta,
                        );
                    }
                } else {
                    return error_need_arr(outer, segment, current.value_type(), &env.meta);
                }
            }
            Segment::Element { expr, .. } => {
                let key = stry!(expr.run(opts, env, event, meta, local));

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
                                o.keys().map(ToString::to_string).collect(),
                                &env.meta,
                            );
                        }
                    }
                    (Value::Object(_), other) => {
                        return error_need_str(outer, segment, other.value_type(), &env.meta)
                    }
                    (Value::Array(a), idx) => {
                        if let Some(idx) = idx.as_usize() {
                            let (start, end) = if let Some((start, end)) = subrange {
                                // We check range on setting the subrange!
                                (start, end)
                            } else {
                                (0, a.len())
                            };
                            let idx = idx + start;
                            if idx >= end {
                                // We exceed the sub range
                                let bad_idx = idx - start;
                                return error_array_out_of_bound(
                                    outer,
                                    segment,
                                    &path,
                                    bad_idx..bad_idx,
                                    &env.meta,
                                );
                            }

                            if let Some(v) = a.get(idx) {
                                current = v;
                                subrange = None;
                                continue;
                            } else {
                                return error_array_out_of_bound(
                                    outer,
                                    segment,
                                    &path,
                                    idx..idx,
                                    &env.meta,
                                );
                            }
                        } else {
                            return error_need_int(outer, segment, idx.value_type(), &env.meta);
                        }
                    }
                    (other, k) => {
                        return if key.is_str() {
                            error_need_obj(outer, segment, other.value_type(), &env.meta)
                        } else if k.is_usize() {
                            error_need_arr(outer, segment, other.value_type(), &env.meta)
                        } else {
                            error_oops(outer, "Bad path segments", &env.meta)
                        }
                    }
                }
            }

            Segment::Range {
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
                    let s = stry!(range_start.run(opts, env, event, meta, local));
                    if let Some(range_start) = s.as_usize() {
                        let range_start = range_start + start;
                        let e = stry!(range_end.run(opts, env, event, meta, local));
                        if let Some(range_end) = e.as_usize() {
                            let range_end = range_end;
                            if range_end > end {
                                return error_array_out_of_bound(
                                    outer,
                                    segment,
                                    &path,
                                    // Compensate for range inclusiveness
                                    range_start..(range_end - 1),
                                    &env.meta,
                                );
                            } else {
                                subrange = Some((range_start, range_end));
                                continue;
                            }
                        } else {
                            let re: &ImutExpr = range_end.borrow();
                            return error_need_int(outer, re, e.value_type(), &env.meta);
                        }
                    } else {
                        let rs: &ImutExpr = range_start.borrow();
                        return error_need_int(outer, rs.borrow(), s.value_type(), &env.meta);
                    }
                } else {
                    return error_need_arr(outer, segment, current.value_type(), &env.meta);
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
            error_need_arr(outer, outer, current.value_type(), &env.meta)
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
    if let (Some(rep), Some(map)) = (replacement.as_object(), value.as_object_mut()) {
        for (k, v) in rep {
            if v.is_null() {
                map.remove(k);
            } else if let Some(k) = map.get_mut(k) {
                stry!(merge_values(outer, inner, k, v))
            } else {
                //NOTE: We got to clone here since we're duplicating values
                map.insert(k.clone(), v.clone());
            }
        }
    } else {
        // If one of the two isn't a map we can't merge so we simplhy
        // write the replacement into the target.
        // NOTE: We got to clone here since we're duplicating values
        *value = replacement.clone();
    }
    Ok(())
}

#[inline]
fn patch_value<'run, 'event, 'script, Expr>(
    _outer: &'script Expr,
    opts: ExecOpts,
    env: &'run Env<'run, 'event, 'script>,
    event: &'run Value<'event>,
    meta: &'run Value<'event>,
    local: &'run LocalStack<'event>,
    value: &'run mut Value<'event>,
    expr: &'script Patch,
) -> Result<()>
where
    Expr: BaseExpr,

    'script: 'event,
    'event: 'run,
{
    let patch_expr = expr;
    for op in &expr.operations {
        // NOTE: This if is inside the for loop to prevent obj to be updated
        // between iterations and possibly lead to dangling pointers
        if let Some(ref mut obj) = value.as_object_mut() {
            match op {
                PatchOperation::Insert { ident, expr } => {
                    let new_key = stry!(ident.eval_to_string(opts, env, event, meta, local));
                    let new_value = stry!(expr.run(opts, env, event, meta, local));
                    if obj.contains_key(&new_key) {
                        return error_patch_key_exists(
                            patch_expr,
                            ident,
                            new_key.to_string(),
                            &env.meta,
                        );
                    } else {
                        obj.insert(new_key, new_value.into_owned());
                    }
                }
                PatchOperation::Update { ident, expr } => {
                    let new_key = stry!(ident.eval_to_string(opts, env, event, meta, local));
                    let new_value = stry!(expr.run(opts, env, event, meta, local));
                    if obj.contains_key(&new_key) {
                        obj.insert(new_key, new_value.into_owned());
                    } else {
                        return error_patch_update_key_missing(
                            patch_expr,
                            expr,
                            new_key.to_string(),
                            &env.meta,
                        );
                    }
                }
                PatchOperation::Upsert { ident, expr } => {
                    let new_key = stry!(ident.eval_to_string(opts, env, event, meta, local));
                    let new_value = stry!(expr.run(opts, env, event, meta, local));
                    obj.insert(new_key, new_value.into_owned());
                }
                PatchOperation::Erase { ident } => {
                    let new_key = stry!(ident.eval_to_string(opts, env, event, meta, local));
                    obj.remove(&new_key);
                }
                PatchOperation::Move { from, to } => {
                    let from = stry!(from.eval_to_string(opts, env, event, meta, local));
                    let to = stry!(to.eval_to_string(opts, env, event, meta, local));

                    if obj.contains_key(&to) {
                        return error_patch_key_exists(patch_expr, expr, to.to_string(), &env.meta);
                    }
                    if let Some(old) = obj.remove(&from) {
                        obj.insert(to, old);
                    }
                }
                PatchOperation::Copy { from, to } => {
                    let from = stry!(from.eval_to_string(opts, env, event, meta, local));
                    let to = stry!(to.eval_to_string(opts, env, event, meta, local));

                    if obj.contains_key(&to) {
                        return error_patch_key_exists(patch_expr, expr, to.to_string(), &env.meta);
                    }
                    if let Some(old) = obj.get(&from) {
                        let old = old.clone();
                        obj.insert(to, old);
                    }
                }
                PatchOperation::Merge { ident, expr } => {
                    let new_key = stry!(ident.eval_to_string(opts, env, event, meta, local));
                    let merge_spec = stry!(expr.run(opts, env, event, meta, local));

                    match obj.get_mut(&new_key) {
                        Some(value @ Value::Object(_)) => {
                            stry!(merge_values(patch_expr, expr, value, &merge_spec));
                        }
                        Some(other) => {
                            return error_patch_merge_type_conflict(
                                patch_expr,
                                ident,
                                new_key.to_string(),
                                &other,
                                &env.meta,
                            );
                        }
                        None => {
                            let mut new_value = Value::from(Object::new());
                            stry!(merge_values(patch_expr, expr, &mut new_value, &merge_spec));
                            obj.insert(new_key, new_value);
                        }
                    }
                }
                PatchOperation::TupleMerge { expr } => {
                    let merge_spec = stry!(expr.run(opts, env, event, meta, local));

                    stry!(merge_values(patch_expr, expr, value, &merge_spec));
                }
            }
        } else {
            return error_need_obj(patch_expr, &expr.target, value.value_type(), &env.meta);
        }
    }
    Ok(())
}

#[inline]
fn test_guard<'run, 'event, 'script, Expr>(
    outer: &'script Expr,
    opts: ExecOpts,
    env: &'run Env<'run, 'event, 'script>,
    event: &'run Value<'event>,
    meta: &'run Value<'event>,
    local: &'run LocalStack<'event>,
    guard: &'script Option<ImutExpr<'script>>,
) -> Result<bool>
where
    Expr: BaseExpr,

    'script: 'event,
    'event: 'run,
{
    if let Some(guard) = guard {
        let test = stry!(guard.run(opts, env, event, meta, local));
        if let Some(b) = test.as_bool() {
            Ok(b)
        } else {
            error_guard_not_bool(outer, guard, &test, &env.meta)
        }
    } else {
        Ok(true)
    }
}

#[inline]
fn test_predicate_expr<'run, 'event, 'script, Expr>(
    outer: &'script Expr,
    opts: ExecOpts,
    env: &'run Env<'run, 'event, 'script>,
    event: &'run Value<'event>,
    meta: &'run Value<'event>,
    local: &'run LocalStack<'event>,
    target: &'run Value<'event>,
    pattern: &'script Pattern<'script>,
    guard: &'run Option<ImutExpr<'script>>,
) -> Result<bool>
where
    Expr: BaseExpr,

    'script: 'event,
    'event: 'run,
{
    match pattern {
        Pattern::Record(ref rp) => {
            if stry!(match_rp_expr(
                outer,
                opts.without_result(),
                env,
                event,
                meta,
                local,
                &target,
                &rp
            ))
            .is_some()
            {
                test_guard(outer, opts, env, event, meta, local, guard)
            } else {
                Ok(false)
            }
        }
        Pattern::Array(ref ap) => {
            if stry!(match_ap_expr(
                outer,
                opts.without_result(),
                env,
                event,
                meta,
                local,
                &target,
                &ap
            ))
            .is_some()
            {
                test_guard(outer, opts, env, event, meta, local, guard)
            } else {
                Ok(false)
            }
        }
        Pattern::Expr(ref expr) => {
            let v = stry!(expr.run(opts, env, event, meta, local));
            let vb: &Value = v.borrow();
            if val_eq(target, vb) {
                test_guard(outer, opts, env, event, meta, local, guard)
            } else {
                Ok(false)
            }
        }
        Pattern::Assign(ref a) => {
            match *a.pattern {
                Pattern::Array(ref ap) => {
                    if let Some(v) = stry!(match_ap_expr(
                        outer,
                        opts.with_result(),
                        env,
                        event,
                        meta,
                        local,
                        &target,
                        &ap,
                    )) {
                        // we need to assign prior to the guard so we can cehck
                        // against the pattern expressions
                        stry!(set_local_shadow(outer, local, &env.meta, a.idx, v));
                        test_guard(outer, opts, env, event, meta, local, guard)
                    } else {
                        Ok(false)
                    }
                }
                Pattern::Record(ref rp) => {
                    if let Some(v) = stry!(match_rp_expr(
                        outer,
                        opts.with_result(),
                        env,
                        event,
                        meta,
                        local,
                        &target,
                        &rp,
                    )) {
                        // we need to assign prior to the guard so we can cehck
                        // against the pattern expressions
                        stry!(set_local_shadow(outer, local, &env.meta, a.idx, v));

                        test_guard(outer, opts, env, event, meta, local, guard)
                    } else {
                        Ok(false)
                    }
                }
                Pattern::Expr(ref expr) => {
                    let v = stry!(expr.run(opts, env, event, meta, local));
                    let vb: &Value = v.borrow();
                    if val_eq(target, vb) {
                        // we need to assign prior to the guard so we can cehck
                        // against the pattern expressions
                        stry!(set_local_shadow(
                            outer,
                            local,
                            &env.meta,
                            a.idx,
                            v.into_owned(),
                        ));

                        test_guard(outer, opts, env, event, meta, local, guard)
                    } else {
                        Ok(false)
                    }
                }
                _ => error_oops(outer, "Unimplemented pattern", &env.meta),
            }
        }
        Pattern::Default => Ok(true),
    }
}

#[inline]
fn match_rp_expr<'run, 'event, 'script, Expr>(
    outer: &'script Expr,
    opts: ExecOpts,
    env: &'run Env<'run, 'event, 'script>,
    event: &'run Value<'event>,
    meta: &'run Value<'event>,
    local: &'run LocalStack<'event>,
    target: &'run Value<'event>,
    rp: &'script RecordPattern,
) -> Result<Option<Value<'event>>>
where
    Expr: BaseExpr,

    'script: 'event,
    'event: 'run,
{
    let mut acc = Value::from(Object::with_capacity(if opts.result_needed {
        rp.fields.len()
    } else {
        0
    }));
    for pp in &rp.fields {
        let known_key = pp.key();

        match pp {
            PredicatePattern::FieldPresent { .. } => {
                if let Some(v) = known_key.lookup(target) {
                    if opts.result_needed {
                        known_key.insert(&mut acc, v.clone())?;
                    }
                    continue;
                } else {
                    return Ok(None);
                }
            }
            PredicatePattern::FieldAbsent { .. } => {
                if known_key.lookup(target).is_some() {
                    return Ok(None);
                } else {
                    continue;
                }
            }
            PredicatePattern::TildeEq { test, .. } => {
                let testee = if let Some(v) = known_key.lookup(target) {
                    v
                } else {
                    return Ok(None);
                };
                if let Ok(x) = test
                    .extractor
                    .extract(opts.result_needed, &testee, &env.context)
                {
                    if opts.result_needed {
                        known_key.insert(&mut acc, x)?;
                    }
                } else {
                    return Ok(None);
                }
            }
            PredicatePattern::Eq { rhs, not, .. } => {
                let testee = if let Some(v) = known_key.lookup(target) {
                    v
                } else {
                    return Ok(None);
                };

                let rhs = stry!(rhs.run(opts, env, event, meta, local));
                let vb: &Value = rhs.borrow();
                let r = val_eq(testee, vb);
                let m = if *not { !r } else { r };

                if m {
                    continue;
                } else {
                    return Ok(None);
                }
            }
            PredicatePattern::RecordPatternEq { pattern, .. } => {
                let testee = if let Some(v) = known_key.lookup(target) {
                    v
                } else {
                    return Ok(None);
                };

                if testee.is_object() {
                    if let Some(m) = stry!(match_rp_expr(
                        outer, opts, env, event, meta, local, testee, pattern,
                    )) {
                        if opts.result_needed {
                            known_key.insert(&mut acc, m)?;
                        }
                        continue;
                    } else {
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            }
            PredicatePattern::ArrayPatternEq { pattern, .. } => {
                let testee = if let Some(v) = known_key.lookup(target) {
                    v
                } else {
                    return Ok(None);
                };

                if testee.is_array() {
                    if let Some(r) = stry!(match_ap_expr(
                        outer, opts, env, event, meta, local, testee, pattern,
                    )) {
                        if opts.result_needed {
                            known_key.insert(&mut acc, r)?;
                        }

                        continue;
                    } else {
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            }
        }
    }

    Ok(Some(acc))
}

#[inline]
fn match_ap_expr<'run, 'event, 'script, Expr>(
    outer: &'script Expr,
    opts: ExecOpts,
    env: &'run Env<'run, 'event, 'script>,
    event: &'run Value<'event>,
    meta: &'run Value<'event>,
    local: &'run LocalStack<'event>,
    target: &'run Value<'event>,
    ap: &'script ArrayPattern,
) -> Result<Option<Value<'event>>>
where
    Expr: BaseExpr,

    'script: 'event,
    'event: 'run,
{
    if let Some(a) = target.as_array() {
        let mut acc = Vec::with_capacity(if opts.result_needed { a.len() } else { 0 });
        let mut idx: u64 = 0;
        for candidate in a {
            'inner: for expr in &ap.exprs {
                match expr {
                    ArrayPredicatePattern::Expr(e) => {
                        let r = stry!(e.run(opts, env, event, meta, local));
                        let vb: &Value = r.borrow();

                        // NOTE: We are creating a new value here so we have to clone
                        if val_eq(candidate, vb) && opts.result_needed {
                            acc.push(Value::Array(vec![Value::from(idx), r.into_owned()]));
                        }
                    }
                    ArrayPredicatePattern::Tilde(test) => {
                        if let Ok(r) =
                            test.extractor
                                .extract(opts.result_needed, &candidate, &env.context)
                        {
                            if opts.result_needed {
                                acc.push(Value::Array(vec![Value::from(idx), r]));
                            }
                        } else {
                            continue 'inner;
                        }
                    }
                    ArrayPredicatePattern::Record(rp) => {
                        if let Some(r) = stry!(match_rp_expr(
                            outer, opts, env, event, meta, local, candidate, rp,
                        )) {
                            if opts.result_needed {
                                acc.push(Value::Array(vec![Value::from(idx), r]))
                            };
                        } else {
                            continue 'inner;
                        }
                    }
                    ArrayPredicatePattern::Array(ap) => {
                        if let Some(r) = stry!(match_ap_expr(
                            outer, opts, env, event, meta, local, candidate, ap,
                        )) {
                            if opts.result_needed {
                                acc.push(Value::Array(vec![Value::from(idx), r]));
                            }
                        } else {
                            continue 'inner; // return Ok(Cont::Drop(Value::from(true))),
                        }
                    }
                }
            }
            idx += 1;
        }
        Ok(Some(Value::Array(acc)))
    } else {
        Ok(None)
    }
}

#[inline]
#[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr)]
pub fn set_local_shadow<'run, 'event, 'script, Expr>(
    outer: &'script Expr,
    local: &'run LocalStack<'event>,
    node_meta: &'run NodeMetas,
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
    let local: &mut LocalStack = unsafe { mem::transmute(local) };
    if let Some(d) = local.values.get_mut(idx) {
        *d = Some(v);
        Ok(())
    } else {
        error_oops(outer, "Unknown local variable", &node_meta)
    }
}

//FIXME Do we really want this here?
impl<'script> GroupBy<'script> {
    pub fn generate_groups<'run, 'event>(
        &'script self,
        ctx: &'run EventContext,
        event: &'run Value<'event>,
        node_meta: &'run NodeMetas<'script>,
        meta: &'run Value<'event>,
        groups: &'run mut Vec<Vec<Value<'event>>>,
    ) -> Result<()>
    where
        'script: 'event,
        'event: 'run,
    {
        const NO_AGGRS: [InvokeAggrFn<'static>; 0] = [];
        let opts = ExecOpts {
            result_needed: true,
            aggr: AggrType::Emit,
        };
        // FIXME
        let consts = vec![];
        let local_stack = LocalStack::with_size(0);
        let env = Env {
            consts: &consts,
            context: ctx,
            aggrs: &NO_AGGRS,
            meta: node_meta,
        };
        match self {
            GroupBy::Expr { expr, .. } => {
                let v = expr
                    .run(opts, &env, event, meta, &local_stack)?
                    .into_owned();
                if groups.is_empty() {
                    groups.push(vec![v]);
                } else {
                    groups.iter_mut().for_each(|g| g.push(v.clone()));
                };
                Ok(())
            }

            GroupBy::Set { items, .. } => {
                for item in items {
                    item.generate_groups(ctx, event, node_meta, meta, groups)?
                }

                // set(event.measurement, each(record::keys(event.fields)))
                // GroupBy::Set(items: [GroupBy::Expr(..), GroupBy::Each(GroupBy::Expr(..))])
                // [[7]]
                // [[7, "a"], [7, "b"]]

                // GroupBy::Set(items: [GroupBy::Each(GroupBy::Expr(..)), GroupBy::Expr(..)])
                // [["a"], ["b"]]
                // [["a", 7], ["b", 7]]

                // GroupBy::Set(items: [GroupBy::Each(GroupBy::Expr(..)), GroupBy::Each(GroupBy::Expr(..))])
                // [["a"], ["b"]]
                // [["a", 7], ["b", 7], ["a", 8], ["b", 8]]
                Ok(())
            }
            GroupBy::Each { expr, .. } => {
                let v = expr
                    .run(opts, &env, event, meta, &local_stack)?
                    .into_owned();
                if let Some(each) = v.as_array() {
                    if groups.is_empty() {
                        for e in each {
                            groups.push(vec![e.clone()]);
                        }
                    } else {
                        let mut new_groups = Vec::with_capacity(each.len() * groups.len());
                        for g in groups.drain(..) {
                            for e in each {
                                let mut g = g.clone();
                                g.push(e.clone());
                                new_groups.push(g);
                            }
                        }
                        std::mem::swap(groups, &mut new_groups);
                    }
                    Ok(())
                } else {
                    error_need_arr(self, self, v.value_type(), &env.meta)
                }
            }
        }
    }
}
