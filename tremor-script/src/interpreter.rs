// Copyright 2020-2021, The Tremor Team
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
// NOTE: investigate if re-writing would make code better
// NOTE possible optimisations:
// * P001 [x] re-write `let x = merge x of ... end` to a mutable merge that does not require cloing `x`
// * P002 [x] don't construct data for expressions that return value is never used
// * P003 [x] don't clone as part of a match statement (we should never ever mutate in case or when)
// * P004 [x] turn local variables into a pre-defined vector to improve access
// * P005 [x] turn literals into BorrowedValues so we don't need to re-cast them - constants could be pre-laoded
// * P008 [x] We should not need to clone values in the for comprehension twice.

// NOTE todo
// * 101 [x] `%{x > 3}` and other comparisons
// * 102 [x] Remove the need for `()` around when clauses that contain binary ops

#![allow(clippy::too_many_arguments)]
// NOTE: For env / end
#![allow(clippy::similar_names)]

/// Runtime interpreter support for expressions
pub mod expr;
mod imut_expr;

pub use self::expr::Cont;
use crate::{
    ast::{
        ArrayPattern, ArrayPredicatePattern, BaseExpr, BinOpKind, ExprPath, GroupBy, ImutExpr,
        InvokeAggrFn, NodeMeta, Patch, PatchOperation, Path, Pattern, PredicatePattern,
        RecordPattern, ReservedPath, RunConsts, Segment, TuplePattern, UnaryOpKind,
    },
    ctx::NO_CONTEXT,
    errors::{
        err_need_obj, error_array_out_of_bound, error_bad_array_index, error_bad_key,
        error_bad_key_err, error_decreasing_range, error_division_by_zero, error_guard_not_bool,
        error_invalid_binary, error_invalid_bitshift, error_need_arr, error_need_int,
        error_need_obj, error_need_str, error_oops, error_overflow, error_patch_key_exists,
        error_patch_merge_type_conflict, error_patch_update_key_missing, unknown_local, Result,
    },
    prelude::*,
    stry, EventContext, Value, NO_AGGRS, NO_CONSTS,
};
use simd_json::StaticNode;
use std::{
    borrow::{Borrow, Cow},
    convert::TryInto,
    iter::Iterator,
};

/// constant `true` value
pub const TRUE: Value<'static> = Value::Static(StaticNode::Bool(true));
/// constant `false` value
pub const FALSE: Value<'static> = Value::Static(StaticNode::Bool(false));
/// constant `null` value
pub const NULL: Value<'static> = Value::Static(StaticNode::Null);

#[macro_export]
/// Static boolean `Value` from Rust `bool`
macro_rules! static_bool {
    ($e:expr) => {
        #[allow(clippy::if_not_else)]
        {
            if $e {
                Cow::Borrowed(&TRUE)
            } else {
                Cow::Borrowed(&FALSE)
            }
        }
    };
}

/// Interpreter environment
pub struct Env<'run, 'event>
where
    'event: 'run,
{
    /// Context of the event
    pub context: &'run EventContext<'run>,
    /// Constants
    pub consts: RunConsts<'run, 'event>,
    /// Aggregates
    pub aggrs: &'run [InvokeAggrFn<'event>],
    /// Maximal recursion depth in custom functions
    pub recursion_limit: u32,
}
impl Default for Env<'static, 'static> {
    fn default() -> Self {
        Self {
            context: &NO_CONTEXT,
            consts: NO_CONSTS.run(),
            aggrs: &NO_AGGRS,
            recursion_limit: crate::recursion_limit(),
        }
    }
}

/// Local variable stack
#[derive(Default, Debug)]
pub struct LocalStack<'stack> {
    pub(crate) values: Vec<Option<Value<'stack>>>,
}

impl<'stack> LocalStack<'stack> {
    /// Creates a stack with a given size
    #[must_use]
    pub fn with_size(size: usize) -> Self {
        Self {
            values: vec![None; size],
        }
    }

    /// Fetches a local variable
    ///
    /// # Errors
    /// if the variable isn't known
    pub fn get<O>(&self, idx: usize, o: &O, m: &NodeMeta) -> Result<&Option<Value<'stack>>>
    where
        O: BaseExpr,
    {
        self.values.get(idx).ok_or_else(|| unknown_local(o, m))
    }

    /// Fetches a local variable
    ///
    /// # Errors
    /// if the variable isn't known
    pub fn get_mut<O>(
        &mut self,
        idx: usize,
        o: &O,
        m: &NodeMeta,
    ) -> Result<&mut Option<Value<'stack>>>
    where
        O: BaseExpr,
    {
        self.values.get_mut(idx).ok_or_else(|| unknown_local(o, m))
    }
}

/// The type of an aggregation
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggrType {
    /// This is a normal execution
    Tick,
    /// This is an emit event
    Emit,
}

/// Execution options for a script.
#[derive(Clone, Copy, Debug)]
pub struct ExecOpts {
    /// Is a result needed
    pub result_needed: bool,
    /// If this is an aggregation or a normal execution
    pub aggr: AggrType,
}

impl ExecOpts {
    pub(crate) fn without_result(mut self) -> Self {
        self.result_needed = false;
        self
    }
    pub(crate) fn with_result(mut self) -> Self {
        self.result_needed = true;
        self
    }
}

#[inline]
#[allow(clippy::cast_precision_loss)]
pub(crate) fn val_eq<'event>(lhs: &Value<'event>, rhs: &Value<'event>) -> bool {
    // TODO Consider Tony Garnock-Jones perserves w.r.t. forcing a total ordering
    // across builtin types if/when extending for 'lt' and 'gt' variants
    //
    use Value::{Array, Bytes, Object, Static, String};
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
        (Static(StaticNode::Null), Static(StaticNode::Null)) => true,
        (String(l), String(r)) => *l == *r,
        (Bytes(l), Bytes(r)) => *l == *r,
        (String(l), Bytes(r)) => *l.as_bytes() == *r,
        (Bytes(l), String(r)) => *l == *r.as_bytes(),
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

/// Casts the `&Value` to an index, i.e., a `usize`, or returns the appropriate error indicating
/// why the `Value` is not an index.
///
/// # Note
/// This method explicitly *does not* check whether the resulting index is in range of the array.
#[inline]
fn value_to_index<OuterExpr, InnerExpr>(
    outer: &OuterExpr,
    inner: &InnerExpr,
    val: &Value,
    path: &Path,
    array: &[Value],
) -> Result<usize>
where
    OuterExpr: BaseExpr,
    InnerExpr: BaseExpr,
{
    match val.as_usize() {
        Some(n) => Ok(n),
        None if val.is_integer() => error_bad_array_index(outer, inner, path, val, array.len()),
        None => error_need_int(outer, inner, val.value_type()),
    }
}

fn try_math<'run, 'event, T, O, I>(
    f: impl Fn(T, T) -> Option<T>,
    l: T,
    r: T,
    outer: &O,
    inner: &I,
    op: BinOpKind,
) -> Result<Cow<'run, Value<'event>>>
where
    O: Ranged,
    I: Ranged,
    Value<'static>: From<T>,
    'event: 'run,
{
    f(l, r).map_or_else(
        || error_overflow(outer, inner, op),
        |n| Ok(Cow::Owned(Value::from(n))),
    )
}

#[inline]
#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
fn exec_binary_numeric<'run, 'event, OuterExpr, InnerExpr>(
    outer: &OuterExpr,
    inner: &InnerExpr,
    op: BinOpKind,
    lhs: &Value<'event>,
    rhs: &Value<'event>,
) -> Result<Cow<'run, Value<'event>>>
where
    OuterExpr: BaseExpr,
    InnerExpr: BaseExpr,
    'event: 'run,
{
    use BinOpKind::{
        Add, BitAnd, BitXor, Div, Gt, Gte, LBitShift, Lt, Lte, Mod, Mul, RBitShiftSigned,
        RBitShiftUnsigned, Sub,
    };
    if let (Some(l), Some(r)) = (lhs.as_u64(), rhs.as_u64()) {
        match op {
            BitAnd => Ok(Cow::Owned(Value::from(l & r))),
            // BitOr => Ok(Cow::Owned(Value::from(l | r))),
            BitXor => Ok(Cow::Owned(Value::from(l ^ r))),
            Gt => Ok(static_bool!(l > r)),
            Gte => Ok(static_bool!(l >= r)),
            Lt => Ok(static_bool!(l < r)),
            Lte => Ok(static_bool!(l <= r)),
            Add => try_math(u64::checked_add, l, r, outer, inner, op),
            Sub if l >= r => Ok(Cow::Owned(Value::from(l - r))), // we check this ahead of time anyway so no reason to add an extra error
            Sub => {
                // Handle substraction that would turn this into a negative
                // to do that we calculate r-i (the inverse) and then
                // try to turn this into a i64 and negate it;
                let d = r - l;

                d.try_into().ok().and_then(i64::checked_neg).map_or_else(
                    || error_overflow(outer, inner, op),
                    |res| Ok(Cow::Owned(Value::from(res))),
                )
            }
            Mul => try_math(u64::checked_mul, l, r, outer, inner, op),
            Div if r > 0 => Ok(Cow::Owned(Value::from((l as f64) / (r as f64)))),
            Mod if r > 0 => Ok(Cow::Owned(Value::from(l % r))),
            Mod | Div => error_division_by_zero(outer, inner, op),
            RBitShiftUnsigned | RBitShiftSigned => u32::try_from(r)
                .ok()
                .and_then(|r| l.checked_shr(r))
                .map_or_else(
                    || error_invalid_bitshift(outer, inner),
                    |n| Ok(Cow::Owned(Value::from(n))),
                ),
            LBitShift => u32::try_from(r)
                .ok()
                .and_then(|r| l.checked_shl(r))
                .map_or_else(
                    || error_invalid_bitshift(outer, inner),
                    |n| Ok(Cow::Owned(Value::from(n))),
                ),
            _ => error_invalid_binary(outer, inner, op, lhs, rhs),
        }
    } else if let (Some(l), Some(r)) = (lhs.as_i64(), rhs.as_i64()) {
        match op {
            BitAnd => Ok(Cow::Owned(Value::from(l & r))),
            // BitOr => Ok(Cow::Owned(Value::from(l | r))),
            BitXor => Ok(Cow::Owned(Value::from(l ^ r))),
            Gt => Ok(static_bool!(l > r)),
            Gte => Ok(static_bool!(l >= r)),
            Lt => Ok(static_bool!(l < r)),
            Lte => Ok(static_bool!(l <= r)),
            Add => try_math(i64::checked_add, l, r, outer, inner, op),
            Sub => try_math(i64::checked_sub, l, r, outer, inner, op),
            Mul => try_math(i64::checked_mul, l, r, outer, inner, op),
            Div if r != 0 => Ok(Cow::Owned(Value::from((l as f64) / (r as f64)))),
            Mod if r != 0 => Ok(Cow::Owned(Value::from(l % r))),
            Mod | Div => error_division_by_zero(outer, inner, op),
            RBitShiftSigned => u32::try_from(r)
                .ok()
                .and_then(|r| l.checked_shr(r))
                .map_or_else(
                    || error_invalid_bitshift(outer, inner),
                    |n| Ok(Cow::Owned(Value::from(n))),
                ),
            // This is a bit a bit tricky, for unsigned shift right we need to turnj
            // our left hand side into a u64 so that the shift right operation does no
            // preserve signness then turn the result back inot a i64 to match the original
            // type. We can't use try_from for the u64 <-> i64 conversions
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                clippy::cast_possible_wrap
            )]
            RBitShiftUnsigned => u32::try_from(r)
                .ok()
                .and_then(|r| (l as u64).checked_shr(r))
                .map_or_else(
                    || error_invalid_bitshift(outer, inner),
                    |n| Ok(Cow::Owned(Value::from(n as i64))),
                ),
            LBitShift => u32::try_from(r)
                .ok()
                .and_then(|r| l.checked_shl(r))
                .map_or_else(
                    || error_invalid_bitshift(outer, inner),
                    |n| Ok(Cow::Owned(Value::from(n))),
                ),
            _ => error_invalid_binary(outer, inner, op, lhs, rhs),
        }
    } else if let (Some(l), Some(r)) = (lhs.cast_f64(), rhs.cast_f64()) {
        match op {
            Gte => Ok(static_bool!(l >= r)),
            Gt => Ok(static_bool!(l > r)),
            Lt => Ok(static_bool!(l < r)),
            Lte => Ok(static_bool!(l <= r)),
            Add => Ok(Cow::Owned(Value::from(l + r))),
            Sub => Ok(Cow::Owned(Value::from(l - r))),
            Mul => Ok(Cow::Owned(Value::from(l * r))),
            Div => Ok(Cow::Owned(Value::from(l / r))),
            _ => error_invalid_binary(outer, inner, op, lhs, rhs),
        }
    } else {
        error_invalid_binary(outer, inner, op, lhs, rhs)
    }
}
#[inline]
pub(crate) fn exec_binary<'run, 'event, OuterExpr, InnerExpr>(
    outer: &OuterExpr,
    inner: &InnerExpr,
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
    use BinOpKind::{Add, BitAnd, BitXor, Eq, Gt, Gte, Lt, Lte, NotEq};
    use StaticNode::Bool;
    use Value::{Array, Bytes, Object, Static, String};
    match (op, lhs, rhs) {
        (Eq, Static(StaticNode::Null), Static(StaticNode::Null)) => Ok(static_bool!(true)),
        (NotEq, Static(StaticNode::Null), Static(StaticNode::Null)) => Ok(static_bool!(false)),

        (Eq, l, r) => Ok(static_bool!(val_eq(l, r))),

        (NotEq, l, r) => Ok(static_bool!(!val_eq(l, r))),

        // Bool
        (BitAnd, Static(Bool(l)), Static(Bool(r))) => Ok(static_bool!(*l && *r)),
        // error_invalid_bitshift(outer, inner) missing as we don't have it implemented
        (BitXor, Static(Bool(l)), Static(Bool(r))) => Ok(static_bool!(*l != *r)),

        // Binary
        (Gt, Bytes(l), Bytes(r)) => Ok(static_bool!(l > r)),
        (Gte, Bytes(l), Bytes(r)) => Ok(static_bool!(l >= r)),
        (Lt, Bytes(l), Bytes(r)) => Ok(static_bool!(l < r)),
        (Lte, Bytes(l), Bytes(r)) => Ok(static_bool!(l <= r)),

        // Binary String
        // we have to reverse the comparison here because of types
        (Gt, Bytes(l), String(r)) => {
            let l: &[u8] = l;
            Ok(static_bool!(l > r.as_bytes()))
        }
        (Gte, Bytes(l), String(r)) => {
            let l: &[u8] = l;
            Ok(static_bool!(l >= r.as_bytes()))
        }
        (Lt, Bytes(l), String(r)) => {
            let l: &[u8] = l;
            Ok(static_bool!(r.as_bytes() > l))
        }
        (Lte, Bytes(l), String(r)) => {
            let l: &[u8] = l;
            Ok(static_bool!(r.as_bytes() >= l))
        }

        // String Binary
        (Gt, String(l), Bytes(r)) => {
            let r: &[u8] = r;
            Ok(static_bool!(l.as_bytes() > r))
        }
        (Gte, String(l), Bytes(r)) => {
            let r: &[u8] = r;
            Ok(static_bool!(l.as_bytes() >= r))
        }
        (Lt, String(l), Bytes(r)) => {
            let r: &[u8] = r;
            Ok(static_bool!(l.as_bytes() < r))
        }
        (Lte, String(l), Bytes(r)) => {
            let r: &[u8] = r;
            Ok(static_bool!(l.as_bytes() <= r))
        }

        // String
        (Gt, String(l), String(r)) => Ok(static_bool!(l > r)),
        (Gte, String(l), String(r)) => Ok(static_bool!(l >= r)),
        (Lt, String(l), String(r)) => Ok(static_bool!(l < r)),
        (Lte, String(l), String(r)) => Ok(static_bool!(l <= r)),
        (Add, String(l), String(r)) => Ok(Cow::Owned(format!("{}{}", *l, *r).into())),

        // Array
        (Add, Array(l), Array(r)) => {
            let mut result = l.clone();
            result.extend_from_slice(r);
            Ok(Cow::Owned(Value::from(result)))
        }

        // Record
        (Add, Object(l), Object(r)) => {
            // We need to clone all values anyhow, so we clone the RHS values as we know we keep them
            let mut result = r.clone();
            // then we iterate thorugh the LHS and clone only those not included in the RHS already
            for (k, v) in l.iter() {
                result
                    .raw_entry_mut()
                    .from_key(k)
                    .or_insert_with(|| (k.clone(), v.clone()));
            }
            Ok(Cow::Owned(Value::Object(result)))
        }

        // Errors
        (op, Bytes(_) | String(_), Bytes(_) | String(_))
        | (op, Static(Bool(_)), Static(Bool(_))) => {
            error_invalid_binary(outer, inner, op, lhs, rhs)
        }

        // numeric
        (op, l, r) => exec_binary_numeric(outer, inner, op, l, r),
    }
}

#[inline]
pub(crate) fn exec_unary<'run, 'event: 'run>(
    op: UnaryOpKind,
    val: &Value<'event>,
) -> Option<Cow<'run, Value<'event>>> {
    // Lazy Heinz doesn't want to write that 10000 times
    // - snot badger - Darach
    use UnaryOpKind::{BitNot, Minus, Not, Plus};
    if let Some(x) = val.as_f64() {
        match &op {
            Minus => Some(Cow::Owned(Value::from(-x))),
            Plus => Some(Cow::Owned(Value::from(x))),
            _ => None,
        }
    } else if let Some(x) = val.as_u64() {
        match &op {
            Minus => {
                if x == 9_223_372_036_854_775_808 {
                    Some(Cow::Owned(Value::from(i64::MIN)))
                } else {
                    x.try_into()
                        .ok()
                        .and_then(i64::checked_neg)
                        .map(Value::from)
                        .map(Cow::Owned)
                }
            }
            Plus => Some(Cow::Owned(Value::from(x))),
            BitNot => Some(Cow::Owned(Value::from(!x))),
            Not => None,
        }
    } else if let Some(x) = val.as_i64() {
        match &op {
            Minus => x.checked_neg().map(Value::from).map(Cow::Owned),
            Plus => Some(Cow::Owned(Value::from(x))),
            BitNot => Some(Cow::Owned(Value::from(!x))),
            Not => None,
        }
    } else if let Some(x) = val.as_bool() {
        match &op {
            BitNot | Not => Some(static_bool!(!x)),
            _ => None,
        }
    } else {
        None
    }
}

#[inline]
#[allow(clippy::too_many_lines)]
pub(crate) fn resolve<'run, 'event, Expr>(
    outer: &'run Expr,
    opts: ExecOpts,
    env: &'run Env<'run, 'event>,
    event: &'run Value<'event>,
    state: &'run Value<'static>,
    meta: &'run Value<'event>,
    local: &'run LocalStack<'event>,
    path: &'run Path<'event>,
) -> Result<Cow<'run, Value<'event>>>
where
    Expr: BaseExpr,
    'event: 'run,
{
    // Fetch the base of the path
    // TODO: Extract this into a method on `Path`?
    let base_value: &Value = match path {
        Path::Local(lpath) => {
            if let Some(l) = stry!(local.get(lpath.idx, outer, &lpath.mid)) {
                l
            } else {
                let key = lpath.mid.name_dflt().to_string();
                return error_bad_key(outer, lpath, path, key, vec![]);
            }
        }
        Path::Meta(_path) => meta,
        Path::Event(_path) => event,
        Path::State(_path) => state,
        Path::Expr(ExprPath { expr, var, .. }) => {
            // If the expression is already borrowed we can refer to it
            // if not we've to store it in a local shadow variable
            match expr.run(opts, env, event, state, meta, local)? {
                Cow::Borrowed(p) => p,
                Cow::Owned(o) => set_local_shadow(outer, local, *var, o)?,
            }
        }
        Path::Reserved(ReservedPath::Args { .. }) => env.consts.args,
        Path::Reserved(ReservedPath::Group { .. }) => env.consts.group,
        Path::Reserved(ReservedPath::Window { .. }) => env.consts.window,
    };
    resolve_value(
        outer, opts, env, event, state, meta, local, path, base_value,
    )
}

#[inline]
#[allow(clippy::too_many_lines)]
pub(crate) fn resolve_value<'run, 'event, Expr>(
    outer: &'run Expr,
    opts: ExecOpts,
    env: &'run Env<'run, 'event>,
    event: &'run Value<'event>,
    state: &'run Value<'static>,
    meta: &'run Value<'event>,
    local: &'run LocalStack<'event>,
    path: &'run Path<'event>,
    base_value: &'run Value<'event>,
) -> Result<Cow<'run, Value<'event>>>
where
    Expr: BaseExpr,
    'event: 'run,
{
    // Resolve the targeted value by applying all path segments
    let mut subrange: Option<&[Value<'event>]> = None;
    // The current value
    let mut current: &'run Value<'event> = base_value;
    for segment in path.segments() {
        match segment {
            // Next segment is an identifier: lookup the identifier on `current`, if it's an object
            Segment::Id { key, .. } => {
                subrange = None;

                current = stry!(key.lookup(current).ok_or_else(|| {
                    current.as_object().map_or_else(
                        || err_need_obj(outer, segment, current.value_type()),
                        |o| {
                            let key = key.key().to_string();
                            let options = o.keys().map(ToString::to_string).collect();
                            error_bad_key_err(
                                outer, segment, //&Expr::dummy(*start, *end),
                                path, key, options,
                            )
                        },
                    )
                }));
                continue;
            }
            // Next segment is an index: index into `current`, if it's an array
            Segment::Idx { idx, .. } => {
                if let Some(a) = current.as_array() {
                    let range_to_consider = subrange.unwrap_or(a.as_slice());
                    let idx = *idx;

                    if let Some(c) = range_to_consider.get(idx) {
                        current = c;
                        subrange = None;
                        continue;
                    }
                    let r = idx..idx;
                    let l = range_to_consider.len();
                    return error_array_out_of_bound(outer, segment, path, r, l);
                }
                return error_need_arr(outer, segment, current.value_type());
            }
            // Next segment is an index range: index into `current`, if it's an array
            Segment::RangeExpr { start, end, .. } => {
                if let Some(a) = current.as_array() {
                    let array = subrange.unwrap_or(a.as_slice());
                    let start = stry!(start
                        .eval_to_index(outer, opts, env, event, state, meta, local, path, array));
                    let end = stry!(
                        end.eval_to_index(outer, opts, env, event, state, meta, local, path, array)
                    );

                    if end < start {
                        return error_decreasing_range(outer, segment, path, start, end);
                    } else if end > array.len() {
                        let r = start..end;
                        let l = array.len();
                        return error_array_out_of_bound(outer, segment, path, r, l);
                    }
                    subrange = array.get(start..end);
                    continue;
                };
                return error_need_arr(outer, segment, current.value_type());
            }
            // Next segment is an index range: index into `current`, if it's an array
            Segment::Range { start, end, .. } => {
                if let Some(a) = current.as_array() {
                    let array = subrange.unwrap_or(a.as_slice());
                    let start = *start;
                    let end = *end;

                    if end > array.len() {
                        let r = start..end;
                        let l = array.len();
                        return error_array_out_of_bound(outer, segment, path, r, l);
                    }
                    subrange = array.get(start..end);
                    continue;
                };
                return error_need_arr(outer, segment, current.value_type());
            }
            // Next segment is an expression: run `expr` to know which key it signifies at runtime
            Segment::Element { expr, .. } => {
                let key = stry!(expr.run(opts, env, event, state, meta, local));

                match (current, key.borrow()) {
                    // The segment resolved to an identifier, and `current` is an object: lookup
                    (Value::Object(o), Value::String(id)) => {
                        if let Some(v) = o.get(id) {
                            current = v;
                            subrange = None;
                            continue;
                        };
                        let key = id.to_string();
                        let options = o.keys().map(ToString::to_string).collect();
                        return error_bad_key(outer, segment, path, key, options);
                    }
                    // The segment did not resolve to an identifier, but `current` is an object: err
                    (Value::Object(_), other) => {
                        return error_need_str(outer, segment, other.value_type())
                    }
                    // If `current` is an array, the segment has to be an index
                    (Value::Array(a), idx) => {
                        let array = subrange.unwrap_or(a.as_slice());
                        let idx = stry!(value_to_index(outer, segment, idx, path, array));

                        if let Some(v) = array.get(idx) {
                            current = v;
                            subrange = None;
                            continue;
                        };
                        let r = idx..idx;
                        let l = array.len();
                        return error_array_out_of_bound(outer, segment, path, r, l);
                    }
                    // The segment resolved to an identifier, but `current` isn't an object: err
                    (other, key) if key.is_str() => {
                        return error_need_obj(outer, segment, other.value_type());
                    }
                    // The segment resolved to an index, but `current` isn't an array: err
                    (other, key) if key.is_usize() => {
                        return error_need_arr(outer, segment, other.value_type());
                    }
                    // Anything else: err
                    _ => return error_oops(outer, 0xdead_0003, "Bad path segments"),
                }
            }
        }
    }

    Ok(subrange.map_or_else(
        || Cow::Borrowed(current),
        |range_to_consider| Cow::Owned(Value::from(range_to_consider.to_vec())),
    ))
}

fn merge_values<'event>(value: &mut Value<'event>, replacement: &Value<'event>) -> Result<()> {
    if let Some((rep, map)) = replacement.as_object().zip(value.as_object_mut()) {
        for (k, v) in rep {
            if let Some(k) = map.get_mut(k) {
                stry!(merge_values(k, v));
            } else {
                //NOTE: We got to clone here since we're duplicating values
                map.insert(k.clone(), v.clone());
            }
        }
    } else {
        // If one of the two isn't a map we can't merge so we simply
        // write the replacement into the target.
        // We do this here since we check types on the entry point
        // NOTE: We got to clone here since we're duplicating values
        *value = replacement.clone();
    }
    Ok(())
}

/// enum mirroring `PatchOperation` carrying evaluated elements
/// as we need to evaluate expressions in patch operations
/// in one go, before we do the actual in-place manipulations
/// in order to not expose temporary states of the patched object to intermittent operations
/// example:
///
/// let event = patch event of
///   insert "a" => event
///   insert "b" => event
///   insert "c" => event
/// end
///
enum PreEvaluatedPatchOperation<'event, 'run> {
    Insert {
        cow: beef::Cow<'event, str>,
        value: Value<'event>,
        mid: &'run NodeMeta,
    },
    Update {
        cow: beef::Cow<'event, str>,
        value: Value<'event>,
        mid: &'run NodeMeta,
    },
    Upsert {
        cow: beef::Cow<'event, str>,
        value: Value<'event>,
    },
    Erase {
        cow: beef::Cow<'event, str>,
    },
    Copy {
        from: beef::Cow<'event, str>,
        to: beef::Cow<'event, str>,
        mid: &'run NodeMeta,
    },
    Move {
        from: beef::Cow<'event, str>,
        to: beef::Cow<'event, str>,
        mid: &'run NodeMeta,
    },
    Merge {
        cow: beef::Cow<'event, str>,
        mvalue: Value<'event>,
        mid: &'run NodeMeta,
    },
    MergeRecord {
        mvalue: Value<'event>,
        mid: &'run NodeMeta,
    },
    Default {
        cow: beef::Cow<'event, str>,
        expr: &'run ImutExpr<'event>,
    },
    DefaultRecord {
        expr: &'run ImutExpr<'event>,
        mid: &'run NodeMeta,
    },
}

impl<'event, 'run> PreEvaluatedPatchOperation<'event, 'run> {
    /// evaulate the `PatchOperation` into constant parts
    fn from(
        patch_op: &'run PatchOperation<'event>,
        opts: ExecOpts,
        env: &Env<'run, 'event>,
        event: &Value<'event>,
        state: &Value<'static>,
        meta: &Value<'event>,
        local: &LocalStack<'event>,
    ) -> Result<Self> {
        Ok(match patch_op {
            PatchOperation::Insert { ident, expr, mid } => PreEvaluatedPatchOperation::Insert {
                cow: stry!(ident.run(opts, env, event, state, meta, local)),
                value: stry!(expr.run(opts, env, event, state, meta, local)).into_owned(),
                mid,
            },
            PatchOperation::Update { ident, expr, mid } => PreEvaluatedPatchOperation::Update {
                cow: stry!(ident.run(opts, env, event, state, meta, local)),
                value: stry!(expr.run(opts, env, event, state, meta, local)).into_owned(),
                mid,
            },
            PatchOperation::Upsert { ident, expr, .. } => PreEvaluatedPatchOperation::Upsert {
                cow: stry!(ident.run(opts, env, event, state, meta, local)),
                value: stry!(expr.run(opts, env, event, state, meta, local)).into_owned(),
            },
            PatchOperation::Erase { ident, .. } => PreEvaluatedPatchOperation::Erase {
                cow: stry!(ident.run(opts, env, event, state, meta, local)),
            },
            PatchOperation::Copy { from, to, mid } => PreEvaluatedPatchOperation::Copy {
                from: stry!(from.run(opts, env, event, state, meta, local)),
                to: stry!(to.run(opts, env, event, state, meta, local)),
                mid,
            },
            PatchOperation::Move { from, to, mid } => PreEvaluatedPatchOperation::Move {
                from: stry!(from.run(opts, env, event, state, meta, local)),
                to: stry!(to.run(opts, env, event, state, meta, local)),
                mid,
            },
            PatchOperation::Merge { ident, expr, mid } => PreEvaluatedPatchOperation::Merge {
                cow: stry!(ident.run(opts, env, event, state, meta, local)),
                mvalue: stry!(expr.run(opts, env, event, state, meta, local)).into_owned(),
                mid,
            },
            PatchOperation::MergeRecord { expr, mid } => PreEvaluatedPatchOperation::MergeRecord {
                mvalue: stry!(expr.run(opts, env, event, state, meta, local)).into_owned(),
                mid,
            },
            PatchOperation::Default { ident, expr, .. } => PreEvaluatedPatchOperation::Default {
                cow: stry!(ident.run(opts, env, event, state, meta, local)),
                // PERF: this is slow, we might not need to evaluate it
                expr,
            },
            PatchOperation::DefaultRecord { expr, mid } => {
                PreEvaluatedPatchOperation::DefaultRecord { expr, mid }
            }
        })
    }
}

#[inline]
#[allow(clippy::too_many_lines)]
fn patch_value<'event>(
    opts: ExecOpts,
    env: &Env<'_, 'event>,
    event: &Value<'event>,
    state: &Value<'static>,
    meta: &Value<'event>,
    local: &LocalStack<'event>,
    target: &mut Value<'event>,
    expr: &Patch<'event>,
) -> Result<()> {
    use PreEvaluatedPatchOperation::{
        self as Pepo, Copy, Default, DefaultRecord, Erase, Insert, Merge, MergeRecord, Move,
        Update, Upsert,
    };
    let patch_expr = expr;
    let mut evaluated: Vec<_> = Vec::with_capacity(expr.operations.len());
    // first pass over the operations, evaluating them
    // and (IMPORTANT) get it into an owned, possibly cloned value, so we reference
    // the target value in the state before any patch operation has been executed.
    for op in &expr.operations {
        evaluated.push(stry!(Pepo::from(op, opts, env, event, state, meta, local,)));
    }

    // second pass over pre-evaluated operations
    // executing them against the actual target value
    for const_op in evaluated {
        // moved inside the loop as we need to borrow it mutably in the tuple-merge case
        let t = target.value_type();
        let obj = target
            .as_object_mut()
            .ok_or_else(|| err_need_obj(patch_expr, &expr.target, t))?;
        match const_op {
            Insert {
                cow, value, mid, ..
            } => {
                if obj.contains_key(&cow) {
                    let key = cow.to_string();
                    return error_patch_key_exists(patch_expr, mid, key);
                };
                obj.insert(cow, value);
            }
            Update {
                cow, value, mid, ..
            } => {
                if obj.contains_key(&cow) {
                    obj.insert(cow, value);
                } else {
                    let key = cow.to_string();
                    return error_patch_update_key_missing(patch_expr, mid, key);
                }
            }
            Upsert { cow, value, .. } => {
                obj.insert(cow, value);
            }
            Erase { cow, .. } => {
                obj.remove(&cow);
            }
            Copy { from, to, mid } => {
                if obj.contains_key(&to) {
                    return error_patch_key_exists(patch_expr, mid, to.to_string());
                }
                if let Some(old) = obj.get(&from) {
                    let old = old.clone();
                    obj.insert(to, old);
                }
            }
            Move { from, to, mid } => {
                if obj.contains_key(&to) {
                    return error_patch_key_exists(patch_expr, mid, to.to_string());
                }
                if let Some(old) = obj.remove(&from) {
                    obj.insert(to, old);
                }
            }
            Merge {
                cow, mvalue, mid, ..
            } => match obj.get_mut(&cow) {
                Some(value) if value.is_object() && mvalue.is_object() => {
                    stry!(merge_values(value, &mvalue));
                }
                Some(other) => {
                    let key = cow.to_string();
                    return error_patch_merge_type_conflict(patch_expr, mid, key, other);
                }
                None => {
                    let mut new_value = Value::object();
                    stry!(merge_values(&mut new_value, &mvalue));
                    obj.insert(cow, new_value);
                }
            },
            MergeRecord { mvalue, mid, .. } => {
                if mvalue.is_object() {
                    stry!(merge_values(target, &mvalue));
                } else {
                    return error_patch_merge_type_conflict(
                        patch_expr,
                        mid,
                        "<target>".into(),
                        &mvalue,
                    );
                }
            }
            Default { cow, expr, .. } => {
                if !obj.contains_key(&cow) {
                    let default_value = stry!(expr.run(opts, env, event, state, meta, local));
                    obj.insert(cow, default_value.into_owned());
                };
            }
            DefaultRecord { expr: inner, mid } => {
                let default_value = stry!(inner.run(opts, env, event, state, meta, local));
                if let Some(dflt) = default_value.as_object() {
                    apply_default(obj, dflt);
                } else {
                    return error_need_obj(expr, mid, default_value.value_type());
                }
            }
        }
    }
    Ok(())
}

fn apply_default<'event>(target: &mut Object<'event>, dflt: &Object<'event>) {
    for (k, v) in dflt {
        if !target.contains_key(k) {
            target.insert(k.clone(), v.clone());
        } else if let Some((target, dflt)) = target
            .get_mut(k)
            .and_then(Value::as_object_mut)
            .zip(v.as_object())
        {
            apply_default(target, dflt);
        }
    }
}

#[inline]
fn test_guard<Expr>(
    outer: &Expr,
    opts: ExecOpts,
    env: &Env,
    event: &Value,
    state: &Value<'static>,
    meta: &Value,
    local: &LocalStack,
    guard: &Option<ImutExpr>,
) -> Result<bool>
where
    Expr: BaseExpr,
{
    guard.as_ref().map_or_else(
        || Ok(true),
        |guard| {
            let test = stry!(guard.run(opts, env, event, state, meta, local));
            test.as_bool()
                .map_or_else(|| error_guard_not_bool(outer, guard, &test), Result::Ok)
        },
    )
}

#[inline]
#[allow(clippy::too_many_lines)]
pub(crate) fn test_predicate_expr<Expr>(
    outer: &Expr,
    opts: ExecOpts,
    env: &Env,
    event: &Value,
    state: &Value<'static>,
    meta: &Value,
    local: &LocalStack,
    target: &Value,
    pattern: &Pattern,
    guard: &Option<ImutExpr>,
) -> Result<bool>
where
    Expr: BaseExpr,
{
    match pattern {
        Pattern::Extract(test) => {
            if test
                .extractor
                .extract(false, target, env.context)
                .is_match()
            {
                test_guard(outer, opts, env, event, state, meta, local, guard)
            } else {
                Ok(false)
            }
        }
        Pattern::DoNotCare => test_guard(outer, opts, env, event, state, meta, local, guard),
        Pattern::Tuple(ref tp) => {
            let opts_wo = opts.without_result();
            let res = match_tp_expr(outer, opts_wo, env, event, state, meta, local, target, tp);
            if stry!(res).is_some() {
                test_guard(outer, opts, env, event, state, meta, local, guard)
            } else {
                Ok(false)
            }
        }
        Pattern::Record(ref rp) => {
            let opts_wo = opts.without_result();
            let res = match_rp_expr(outer, opts_wo, env, event, state, meta, local, target, rp);
            if stry!(res).is_some() {
                test_guard(outer, opts, env, event, state, meta, local, guard)
            } else {
                Ok(false)
            }
        }
        Pattern::Array(ref ap) => {
            let opts_wo = opts.without_result();
            let res = match_ap_expr(outer, opts_wo, env, event, state, meta, local, target, ap);
            if stry!(res).is_some() {
                test_guard(outer, opts, env, event, state, meta, local, guard)
            } else {
                Ok(false)
            }
        }
        Pattern::Expr(ref expr) => {
            let v = stry!(expr.run(opts, env, event, state, meta, local));
            let vb: &Value = v.borrow();
            if val_eq(target, vb) {
                test_guard(outer, opts, env, event, state, meta, local, guard)
            } else {
                Ok(false)
            }
        }
        Pattern::Assign(ref a) => {
            let o_w = opts.with_result();

            match *a.pattern {
                Pattern::Extract(ref test) => {
                    if let Some(v) = test
                        .extractor
                        .extract(true, target, env.context)
                        .into_match()
                    {
                        // we need to assign prior to the guard so we can check
                        // against the pattern expressions
                        stry!(set_local_shadow(outer, local, a.idx, v));
                        test_guard(outer, opts, env, event, state, meta, local, guard)
                    } else {
                        Ok(false)
                    }
                }
                Pattern::DoNotCare => {
                    let v = target.clone();
                    stry!(set_local_shadow(outer, local, a.idx, v));
                    test_guard(outer, opts, env, event, state, meta, local, guard)
                }
                Pattern::Array(ref ap) => {
                    let res = match_ap_expr(outer, o_w, env, event, state, meta, local, target, ap);
                    stry!(res).map_or(Ok(false), |v| {
                        // we need to assign prior to the guard so we can check
                        // against the pattern expressions
                        stry!(set_local_shadow(outer, local, a.idx, v));

                        test_guard(outer, opts, env, event, state, meta, local, guard)
                    })
                }
                Pattern::Record(ref rp) => {
                    let res = match_rp_expr(outer, o_w, env, event, state, meta, local, target, rp);
                    stry!(res).map_or(Ok(false), |v| {
                        // we need to assign prior to the guard so we can check
                        // against the pattern expressions
                        stry!(set_local_shadow(outer, local, a.idx, v));

                        test_guard(outer, opts, env, event, state, meta, local, guard)
                    })
                }
                Pattern::Expr(ref expr) => {
                    let v = stry!(expr.run(opts, env, event, state, meta, local));
                    let vb: &Value = v.borrow();
                    if val_eq(target, vb) {
                        // we need to assign prior to the guard so we can check
                        // against the pattern expressions
                        let v = v.into_owned();
                        stry!(set_local_shadow(outer, local, a.idx, v));

                        test_guard(outer, opts, env, event, state, meta, local, guard)
                    } else {
                        Ok(false)
                    }
                }
                Pattern::Tuple(ref tp) => {
                    let res = match_tp_expr(outer, o_w, env, event, state, meta, local, target, tp);
                    stry!(res).map_or(Ok(false), |v| {
                        // we need to assign prior to the guard so we can cehck
                        // against the pattern expressions
                        stry!(set_local_shadow(outer, local, a.idx, v));
                        test_guard(outer, opts, env, event, state, meta, local, guard)
                    })
                }
                Pattern::Assign(_) => error_oops(outer, 0xdead_0004, "nested assign pattern"),
            }
        }
    }
}

/// A record pattern matches a target if the target is a record that contains **at least all
/// keys** and the tests for **each of the key** match.
#[inline]
#[allow(clippy::too_many_lines)]
fn match_rp_expr<'event, Expr>(
    outer: &Expr,
    opts: ExecOpts,
    env: &Env<'_, 'event>,
    event: &Value<'event>,
    state: &Value<'static>,
    meta: &Value<'event>,
    local: &LocalStack<'event>,
    target: &Value<'event>,
    rp: &RecordPattern<'event>,
) -> Result<Option<Value<'event>>>
where
    Expr: BaseExpr,
{
    let res = if let Some(record) = target.as_object() {
        let mut acc: Value<'event> = Value::object_with_capacity(if opts.result_needed {
            rp.fields.len()
        } else {
            0
        });

        for pp in &rp.fields {
            let known_key = pp.key();

            match pp {
                PredicatePattern::FieldPresent { .. } => {
                    if let Some(v) = known_key.map_lookup(record) {
                        if opts.result_needed {
                            known_key.insert(&mut acc, v.clone())?;
                        };
                    } else {
                        return Ok(None);
                    }
                }
                PredicatePattern::FieldAbsent { .. } => {
                    if known_key.map_lookup(record).is_some() {
                        return Ok(None);
                    }
                }
                PredicatePattern::TildeEq { test, .. } => {
                    let Some(testee) = known_key.map_lookup(record) else {
                        return Ok(None);
                    };
                    if let Some(x) = test
                        .extractor
                        .extract(opts.result_needed, testee, env.context)
                        .into_match()
                    {
                        if opts.result_needed {
                            known_key.insert(&mut acc, x)?;
                        };
                    } else {
                        return Ok(None);
                    }
                }
                PredicatePattern::Bin { rhs, kind, .. } => {
                    let Some(testee) = known_key.map_lookup(record) else {
                        return Ok(None);
                    };

                    let rhs = stry!(rhs.run(opts, env, event, state, meta, local));
                    let vb: &Value = rhs.borrow();
                    let r = stry!(exec_binary(outer, outer, *kind, testee, vb));

                    if !r.as_bool().unwrap_or_default() {
                        return Ok(None);
                    }
                }
                PredicatePattern::RecordPatternEq { pattern, .. } => {
                    let Some(testee) = known_key.map_lookup(record) else {
                        return Ok(None);
                    };

                    if testee.is_object() {
                        if let Some(m) = stry!(match_rp_expr(
                            outer, opts, env, event, state, meta, local, testee, pattern,
                        )) {
                            if opts.result_needed {
                                known_key.insert(&mut acc, m)?;
                            };
                        } else {
                            return Ok(None);
                        }
                    } else {
                        return Ok(None);
                    }
                }
                PredicatePattern::ArrayPatternEq { pattern, .. } => {
                    let Some(testee) = known_key.map_lookup(record) else {
                        return Ok(None);
                    };

                    if testee.is_array() {
                        if let Some(r) = stry!(match_ap_expr(
                            outer, opts, env, event, state, meta, local, testee, pattern,
                        )) {
                            if opts.result_needed {
                                known_key.insert(&mut acc, r)?;
                            };
                        } else {
                            return Ok(None);
                        }
                    } else {
                        return Ok(None);
                    }
                }
                PredicatePattern::TuplePatternEq { pattern, .. } => {
                    let Some(testee) = known_key.map_lookup(record) else {
                        return Ok(None);
                    };

                    if testee.is_array() {
                        if let Some(r) = stry!(match_tp_expr(
                            outer, opts, env, event, state, meta, local, testee, pattern,
                        )) {
                            if opts.result_needed {
                                known_key.insert(&mut acc, r)?;
                            };
                        } else {
                            return Ok(None);
                        }
                    } else {
                        return Ok(None);
                    }
                }
            }
        }
        Some(acc)
    } else {
        None
    };
    Ok(res)
}

/// An *array pattern* matches a target value if the *target* is an array and **each** test in the
/// pattern matches **at least for one** element in the *target* indiscriminate of their positions.
///
/// %[ _ ] ~= [] = false
/// %[ _ ] ~= [1] = true
/// %[ _ ] ~= [x, y, z] = true
#[inline]
// TODO this is a bit of a mess, we should probably think about array patterns
fn match_ap_expr<'event, Expr>(
    outer: &Expr,
    opts: ExecOpts,
    env: &Env<'_, 'event>,
    event: &Value<'event>,
    state: &Value<'static>,
    meta: &Value<'event>,
    local: &LocalStack<'event>,
    target: &Value<'event>,
    ap: &ArrayPattern<'event>,
) -> Result<Option<Value<'event>>>
where
    Expr: BaseExpr,
{
    let res = if let Some(a) = target.as_array() {
        // %[] - matches if target is an array
        if ap.exprs.is_empty() {
            Some(Value::array_with_capacity(0))
        } else {
            let mut acc = Vec::with_capacity(if opts.result_needed { a.len() } else { 0 });
            for expr in &ap.exprs {
                let mut matched = false;
                match expr {
                    ArrayPredicatePattern::Ignore => {
                        // _ matches any element
                        matched = !a.is_empty();
                    }
                    ArrayPredicatePattern::Expr(e) => {
                        'inner_expr: for (idx, candidate) in a.iter().enumerate() {
                            let r = stry!(e.run(opts, env, event, state, meta, local));
                            let vb: &Value = r.borrow();
                            let expr_matches = val_eq(candidate, vb);
                            matched |= expr_matches;
                            if expr_matches {
                                if opts.result_needed {
                                    // NOTE: We are creating a new value here so we have to clone
                                    acc.push(Value::from(vec![Value::from(idx), r.into_owned()]));
                                } else {
                                    // if we don't need the results, we can abort here as we have a match
                                    break 'inner_expr;
                                }
                            }
                        }
                    }
                    ArrayPredicatePattern::Tilde(test) => {
                        'inner_tilde: for (idx, candidate) in a.iter().enumerate() {
                            if let Some(r) = test
                                .extractor
                                .extract(opts.result_needed, candidate, env.context)
                                .into_match()
                            {
                                matched |= true;
                                if opts.result_needed {
                                    acc.push(Value::from(vec![Value::from(idx), r]));
                                } else {
                                    // if we don't need the results, we can abort here as we have a match
                                    break 'inner_tilde;
                                }
                            }
                        }
                    }
                    ArrayPredicatePattern::Record(rp) => {
                        'inner_rec: for (idx, candidate) in a.iter().enumerate() {
                            if let Some(r) = stry!(match_rp_expr(
                                outer, opts, env, event, state, meta, local, candidate, rp,
                            )) {
                                matched |= true;
                                if opts.result_needed {
                                    acc.push(Value::from(vec![Value::from(idx), r]));
                                } else {
                                    // if we don't need the results, we can abort here as we have a match
                                    break 'inner_rec;
                                };
                            }
                        }
                    }
                }
                // we did find a match for 1 pattern expression, we have no match at all ;-(
                // short circuit here
                if !matched {
                    return Ok(None);
                }
            }
            Some(Value::from(acc))
        }
    } else {
        // not an array
        None
    };
    Ok(res)
}

#[inline]
fn match_tp_expr<'event, Expr>(
    outer: &Expr,
    opts: ExecOpts,
    env: &Env<'_, 'event>,
    event: &Value<'event>,
    state: &Value<'static>,
    meta: &Value<'event>,
    local: &LocalStack<'event>,
    target: &Value<'event>,
    tp: &TuplePattern<'event>,
) -> Result<Option<Value<'event>>>
where
    Expr: BaseExpr,
{
    if let Some(a) = target.as_array() {
        if (tp.open && a.len() < tp.exprs.len()) || (!tp.open && a.len() != tp.exprs.len()) {
            return Ok(None);
        }
        let mut acc = Vec::with_capacity(if opts.result_needed { a.len() } else { 0 });
        let cases = tp.exprs.iter().zip(a.iter());
        for (case, candidate) in cases {
            match case {
                ArrayPredicatePattern::Ignore => {
                    if opts.result_needed {
                        acc.push(candidate.clone());
                    }
                }
                ArrayPredicatePattern::Expr(e) => {
                    let r = stry!(e.run(opts, env, event, state, meta, local));
                    let vb: &Value = r.borrow();

                    // NOTE: We are creating a new value here so we have to clone
                    if val_eq(candidate, vb) {
                        if opts.result_needed {
                            acc.push(r.into_owned());
                        }
                    } else {
                        return Ok(None);
                    }
                }
                ArrayPredicatePattern::Tilde(test) => {
                    if let Some(r) = test
                        .extractor
                        .extract(opts.result_needed, candidate, env.context)
                        .into_match()
                    {
                        if opts.result_needed {
                            acc.push(r);
                        }
                    } else {
                        return Ok(None);
                    }
                }
                ArrayPredicatePattern::Record(rp) => {
                    if let Some(r) = stry!(match_rp_expr(
                        outer, opts, env, event, state, meta, local, candidate, rp,
                    )) {
                        if opts.result_needed {
                            acc.push(r);
                        };
                    } else {
                        return Ok(None);
                    }
                }
            }
        }
        Ok(Some(Value::from(acc)))
    } else {
        Ok(None)
    }
}

#[inline]
// ALLOW: https://github.com/tremor-rs/tremor-runtime/issues/1029
#[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr)]
fn set_local_shadow<'local, 'event, Expr>(
    outer: &Expr,
    local: &LocalStack<'event>,
    idx: usize,
    v: Value<'event>,
) -> Result<&'local mut Value<'event>>
where
    Expr: BaseExpr,
{
    use std::mem;
    // ALLOW: https://github.com/tremor-rs/tremor-runtime/issues/1029
    let local: &mut LocalStack<'event> = unsafe { mem::transmute(local) };
    local.values.get_mut(idx).map_or_else(
        || {
            error_oops(
                outer,
                0xdead_0006,
                "Unknown local variable in set_local_shadow",
            )
        },
        |d| Ok(d.insert(v)),
    )
}

impl<'script> GroupBy<'script> {
    /// Creates groups based on an event.
    ///
    /// # Errors
    /// if the group can not be generated from the provided event, state and meta
    pub fn generate_groups<'event>(
        &self,
        ctx: &EventContext,
        event: &Value<'event>,
        meta: &Value<'event>,
    ) -> Result<Vec<Vec<Value<'static>>>>
    where
        'script: 'event,
    {
        let mut groups = Vec::with_capacity(16);
        stry!(self.generate_groups_inner(ctx, event, &NULL, meta, &mut groups));
        Ok(groups)
    }
    fn generate_groups_inner<'event>(
        &self,
        ctx: &EventContext,
        event: &Value<'event>,
        state: &Value<'static>,
        meta: &Value<'event>,
        groups: &mut Vec<Vec<Value<'static>>>,
    ) -> Result<()>
    where
        'script: 'event,
    {
        let opts = ExecOpts {
            result_needed: true,
            aggr: AggrType::Emit,
        };
        let local_stack = LocalStack::with_size(0);
        let env = Env {
            consts: NO_CONSTS.run(),
            context: ctx,
            aggrs: &NO_AGGRS,
            recursion_limit: crate::recursion_limit(),
        };
        match self {
            GroupBy::Expr { expr, .. } => {
                let v = stry!(expr.run(opts, &env, event, state, meta, &local_stack));
                if let Some((last_group, other_groups)) = groups.split_last_mut() {
                    other_groups
                        .iter_mut()
                        .for_each(|g| g.push(v.clone_static()));
                    last_group.push(v.clone_static());
                } else {
                    // No last group existed, i.e, `groups` was empty. Push a new group:
                    groups.push(vec![v.clone_static()]);
                }
                Ok(())
            }

            GroupBy::Set { items, .. } => {
                for item in items {
                    stry!(item.generate_groups_inner(ctx, event, state, meta, groups));
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
                let v = stry!(expr.run(opts, &env, event, state, meta, &local_stack));
                if let Some(each) = v.as_array() {
                    if groups.is_empty() {
                        for e in each {
                            groups.push(vec![e.clone_static()]);
                        }
                    } else {
                        let mut new_groups = Vec::with_capacity(each.len() * groups.len());
                        for mut g in groups.drain(..) {
                            if let Some((last, rest)) = each.split_last() {
                                for e in rest {
                                    let mut g = g.clone();
                                    g.push(e.clone_static());
                                    new_groups.push(g);
                                }
                                g.push(last.clone_static());
                                new_groups.push(g);
                            }
                        }
                        std::mem::swap(groups, &mut new_groups);
                    }
                    Ok(())
                } else {
                    error_need_arr(self, self, v.value_type())
                }
            }
        }
    }
}
