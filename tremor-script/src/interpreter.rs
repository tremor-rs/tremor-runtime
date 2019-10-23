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

use crate::ast::*;
use crate::errors::*;
use crate::stry;
use crate::EventContext;
use halfbrown::hashmap;
use simd_json::borrowed::{Object, Value};
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

pub struct Env<'run, 'event, 'script>
where
    'script: 'event,
    'event: 'run,
{
    pub context: &'run EventContext,
    pub consts: &'run [Value<'event>],
    pub aggrs: &'run [InvokeAggrFn<'script>],
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
    #[allow(clippy::cast_precision_loss)]
    match (&op, lhs, rhs) {
        (Eq, Null, Null) => Some(static_bool!(true)),
        (NotEq, Null, Null) => Some(static_bool!(false)),
        (And, Bool(l), Bool(r)) => Some(static_bool!(*l && *r)),
        (Or, Bool(l), Bool(r)) => Some(static_bool!(*l || *r)),
        // FIXME - do we want this?
        // This is to make sure that == in a expression
        // and a record pattern behaves the same.
        (Eq, l, r) => Some(static_bool!(val_eq(l, r))),

        (NotEq, l, r) =>
        {
            #[allow(clippy::if_not_else)]
            Some(static_bool!(!val_eq(l, r)))
        }
        (Gt, I64(l), I64(r)) => Some(static_bool!(*l > *r)),
        (Gt, String(l), String(r)) => Some(static_bool!(l > r)),
        (Gte, I64(l), I64(r)) => Some(static_bool!(*l >= *r)),
        (Gte, String(l), String(r)) => Some(static_bool!(l >= r)),
        (Lt, I64(l), I64(r)) => Some(static_bool!(*l < *r)),
        (Lt, String(l), String(r)) => Some(static_bool!(l < r)),
        (Lte, I64(l), I64(r)) => Some(static_bool!(*l <= *r)),
        (Lte, String(l), String(r)) => Some(static_bool!(l <= r)),
        (Add, String(l), String(r)) => Some(Cow::Owned(format!("{}{}", *l, *r).into())),
        (Add, I64(l), I64(r)) => Some(Cow::Owned(I64(*l + *r))),
        (Sub, I64(l), I64(r)) => Some(Cow::Owned(I64(*l - *r))),
        (Mul, I64(l), I64(r)) => Some(Cow::Owned(I64(*l * *r))),
        (Mod, I64(l), I64(r)) => Some(Cow::Owned(I64(*l % *r))),

        (op, l, r) => {
            if let (Some(l), Some(r)) = (l.cast_f64(), r.cast_f64()) {
                match op {
                    Gte => Some(static_bool!(l >= r)),
                    Gt => Some(static_bool!(l > r)),
                    Lt => Some(static_bool!(l < r)),
                    Lte => Some(static_bool!(l <= r)),
                    Add => Some(Cow::Owned(F64(l + r))),
                    Sub => Some(Cow::Owned(F64(l - r))),
                    Mul => Some(Cow::Owned(F64(l * r))),
                    Div => Some(Cow::Owned(F64(l / r))),
                    _ => None,
                }
            } else {
                None
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
    use Value::*;
    match (&op, val) {
        (Minus, I64(x)) => Some(Cow::Owned(I64(-*x))),
        (Minus, F64(x)) => Some(Cow::Owned(F64(-*x))),
        (Plus, I64(x)) => Some(Cow::Owned(I64(*x))),
        (Plus, F64(x)) => Some(Cow::Owned(F64(*x))),
        (Not, Bool(true)) => Some(static_bool!(false)), // This is not true
        (Not, Bool(false)) => Some(static_bool!(true)), // this is not false
        _ => None,
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
                return error_bad_key(outer, lpath, &path, lpath.id.to_string(), vec![]);
            }

            _ => return error_oops(outer, "Use of unknown local value"),
        },
        Path::Const(lpath) => match env.consts.get(lpath.idx) {
            Some(v) => v,
            _ => return error_oops(outer, "Use of uninitalized constant"),
        },
        Path::Meta(_path) => meta,
        Path::Event(_path) => event,
    };

    for segment in path.segments() {
        match segment {
            Segment::Id { id, key, .. } => {
                if let Some(c) = key.lookup(current) {
                    current = c;
                    subrange = None;
                    continue;
                } else if let Some(o) = current.as_object() {
                    return error_bad_key(
                        outer,
                        segment, //&Expr::dummy(*start, *end),
                        &path,
                        id.to_string(),
                        o.keys().map(ToString::to_string).collect(),
                    );
                } else {
                    return error_type_conflict(
                        outer,
                        segment,
                        current.value_type(),
                        ValueType::Object,
                    );
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
                    return error_type_conflict(
                        outer,
                        segment,
                        current.value_type(),
                        ValueType::Array,
                    );
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
                            );
                        }
                    }
                    (Value::Object(_), other) => {
                        return error_type_conflict(
                            outer,
                            segment,
                            other.value_type(),
                            ValueType::String,
                        )
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
                                return Ok(Cow::Borrowed(&FALSE));
                            }

                            if let Some(v) = a.get(idx) {
                                current = v;
                                subrange = None;
                                continue;
                            } else {
                                return error_array_out_of_bound(outer, segment, &path, idx..idx);
                            }
                        } else {
                            return error_type_conflict(
                                outer,
                                segment,
                                idx.value_type(),
                                ValueType::I64,
                            );
                        }
                    }
                    (other, Value::String(_)) => {
                        return error_type_conflict(
                            outer,
                            segment,
                            other.value_type(),
                            ValueType::Object,
                        )
                    }
                    (other, Value::I64(_)) => {
                        return error_type_conflict(
                            outer,
                            segment,
                            other.value_type(),
                            ValueType::Array,
                        )
                    }
                    _ => return error_oops(outer, "Bad path segments"),
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
                            let range_end = range_end + range_start;
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
                            let re: &ImutExpr = range_end.borrow();
                            return error_type_conflict(outer, re, e.value_type(), ValueType::I64);
                        }
                    } else {
                        let rs: &ImutExpr = range_start.borrow();
                        return error_type_conflict(
                            outer,
                            rs.borrow(),
                            s.value_type(),
                            ValueType::I64,
                        );
                    }
                } else {
                    return error_type_conflict(
                        outer,
                        segment,
                        current.value_type(),
                        ValueType::Array,
                    );
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
            error_type_conflict(outer, outer, current.value_type(), ValueType::Array)
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
                        } else if let Some(k) = map.get_mut(k) {
                            stry!(merge_values(outer, inner, k, v))
                        } else {
                            //NOTE: We got to clone here since we're duplicating values
                            map.insert(k.clone(), v.clone());
                        }
                    }
                }
                other => {
                    return error_type_conflict(outer, inner, other.value_type(), ValueType::Object)
                }
            }
        }
        other => return error_type_conflict(outer, inner, other.value_type(), ValueType::Object),
    }

    Ok(())
}

#[inline]
fn patch_value<'run, 'event, 'script, Expr>(
    outer: &'script Expr,
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
    for op in &expr.operations {
        // NOTE: This if is inside the for loop to prevent obj to be updated
        // between iterations and possibly lead to dangling pointers
        if let Value::Object(ref mut obj) = value {
            match op {
                PatchOperation::Insert { ident, expr } => {
                    let new_key = stry!(ident.eval_to_string(opts, env, event, meta, local));
                    let new_value = stry!(expr.run(opts, env, event, meta, local));
                    if obj.contains_key(&new_key) {
                        return error_patch_key_exists(outer, expr, new_key.to_string());
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
                        return error_patch_update_key_missing(outer, expr, new_key.to_string());
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
                        return error_patch_key_exists(outer, expr, to.to_string());
                    }
                    if let Some(old) = obj.remove(&from) {
                        obj.insert(to, old);
                    }
                }
                PatchOperation::Copy { from, to } => {
                    let from = stry!(from.eval_to_string(opts, env, event, meta, local));
                    let to = stry!(to.eval_to_string(opts, env, event, meta, local));

                    if obj.contains_key(&to) {
                        return error_patch_key_exists(outer, expr, to.to_string());
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
                    let merge_spec = stry!(expr.run(opts, env, event, meta, local));

                    stry!(merge_values(outer, expr, value, &merge_spec));
                }
            }
        } else {
            return error_type_conflict(outer, &expr.target, value.value_type(), ValueType::Object);
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
        match test.borrow() {
            Value::Bool(b) => Ok(*b),
            other => error_guard_not_bool(outer, guard, other),
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
                        stry!(set_local_shadow(outer, local, a.idx, v));
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
                        stry!(set_local_shadow(outer, local, a.idx, v));

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
                        stry!(set_local_shadow(outer, local, a.idx, v.into_owned()));

                        test_guard(outer, opts, env, event, meta, local, guard)
                    } else {
                        Ok(false)
                    }
                }
                _ => error_oops(outer, "Unimplemented pattern"),
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
    let mut acc = Value::Object(Object::with_capacity(if opts.result_needed {
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
        let mut idx = 0;
        for candidate in a {
            'inner: for expr in &ap.exprs {
                match expr {
                    ArrayPredicatePattern::Expr(e) => {
                        let r = stry!(e.run(opts, env, event, meta, local));
                        let vb: &Value = r.borrow();

                        // NOTE: We are creating a new value here so we have to clone
                        if val_eq(candidate, vb) && opts.result_needed {
                            acc.push(Value::Array(vec![Value::I64(idx), r.into_owned()]));
                        }
                    }
                    ArrayPredicatePattern::Tilde(test) => {
                        if let Ok(r) =
                            test.extractor
                                .extract(opts.result_needed, &candidate, &env.context)
                        {
                            if opts.result_needed {
                                acc.push(Value::Array(vec![Value::I64(idx), r]));
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
                                acc.push(Value::Array(vec![Value::I64(idx), r]))
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
                                acc.push(Value::Array(vec![Value::I64(idx), r]));
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
    } else {
        Ok(None)
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
        *d = Some(v);
        Ok(())
    } else {
        error_oops(outer, "Unknown local variable")
    }
}

//FIXME Do we really want this here?
impl<'script> GroupBy<'script> {
    pub fn generate_groups<'run, 'event>(
        &'script self,
        ctx: &'run EventContext,
        event: &'run Value<'event>,
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
                    item.generate_groups(ctx, event, meta, groups)?
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
                    error_type_conflict(self, self, v.value_type(), ValueType::Array)
                }
            }
        }
    }
}
