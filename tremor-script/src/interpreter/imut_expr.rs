// Copyright 2018-2020, Wayfair GmbH
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

use super::{
    exec_binary, exec_unary, merge_values, patch_value, resolve, set_local_shadow, test_guard,
    test_predicate_expr, AggrType, Env, ExecOpts, LocalStack, FALSE, TRUE,
};

use crate::ast::*;
use crate::errors::*;
use crate::interpreter::value_to_index;
use crate::registry::{Registry, TremorAggrFnWrapper};
use crate::stry;
use simd_json::prelude::*;
use simd_json::value::borrowed::{Object, Value};
use std::borrow::Borrow;
use std::borrow::Cow;
use std::mem;

impl<'run, 'event, 'script> ImutExpr<'script>
where
    'script: 'event,
    'event: 'run,
{
    /// Evaluates the expression
    pub fn run(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
    ) -> Result<Cow<'run, Value<'event>>> {
        self.0.run(opts, env, event, state, meta, local)
    }
}
impl<'run, 'event, 'script> ImutExprInt<'script>
where
    'script: 'event,
    'event: 'run,
{
    /// Evaluates the expression to a string.
    #[inline]
    pub fn eval_to_string(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
    ) -> Result<Cow<'event, str>> {
        let value = stry!(self.run(opts, env, event, state, meta, local));
        if let Some(s) = value.as_str() {
            Ok(Cow::from(s.to_owned()))
        } else {
            error_need_str(self, self, value.value_type(), &env.meta)
        }
    }

    /// Evaluates the expression to an index, i.e., a `usize`, or returns the appropriate error
    /// indicating why the resulting `Value` — if any — is not an index.
    ///
    /// # Note
    /// This method explicitly *does not* check whether the resulting index is in range of the array.
    #[inline]
    pub fn eval_to_index<Expr>(
        &'script self,
        outer: &'run Expr,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        path: &'script Path,
        array: &[Value],
    ) -> Result<usize>
    where
        Expr: BaseExpr,
    {
        let val = stry!(self.run(opts, env, event, state, meta, local));
        value_to_index(outer, self, val.borrow(), env, path, array)
    }

    #[inline]
    pub fn run(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
    ) -> Result<Cow<'run, Value<'event>>> {
        match self {
            ImutExprInt::Literal(literal) => Ok(Cow::Borrowed(&literal.value)),
            ImutExprInt::Path(path) => resolve(self, opts, env, event, state, meta, local, path),
            ImutExprInt::Present { path, .. } => {
                self.present(opts, env, event, state, meta, local, path)
            }
            ImutExprInt::Record(ref record) => {
                let mut object: Object = Object::with_capacity(record.fields.len());

                for field in &record.fields {
                    let result = stry!(field.value.run(opts, env, event, state, meta, local));
                    let name = stry!(field
                        .name
                        .eval_to_string(opts, env, event, state, meta, local));
                    object.insert(name, result.into_owned());
                }

                Ok(Cow::Owned(Value::from(object)))
            }
            ImutExprInt::List(ref list) => {
                let mut r: Vec<Value<'event>> = Vec::with_capacity(list.exprs.len());
                for expr in &list.exprs {
                    r.push(stry!(expr.run(opts, env, event, state, meta, local)).into_owned());
                }
                Ok(Cow::Owned(Value::Array(r)))
            }
            ImutExprInt::Invoke1(ref call) => {
                self.invoke1(opts, env, event, state, meta, local, call)
            }
            ImutExprInt::Invoke2(ref call) => {
                self.invoke2(opts, env, event, state, meta, local, call)
            }
            ImutExprInt::Invoke3(ref call) => {
                self.invoke3(opts, env, event, state, meta, local, call)
            }
            ImutExprInt::Invoke(ref call) => {
                self.invoke(opts, env, event, state, meta, local, call)
            }
            ImutExprInt::InvokeAggr(ref call) => self.emit_aggr(opts, env, call),
            ImutExprInt::Patch(ref expr) => self.patch(opts, env, event, state, meta, local, expr),
            ImutExprInt::Merge(ref expr) => self.merge(opts, env, event, state, meta, local, expr),
            ImutExprInt::Local {
                idx,
                mid,
                is_const: false,
            } => match local.values.get(*idx) {
                Some(Some(l)) => Ok(Cow::Borrowed(l)),
                Some(None) => {
                    let path: Path = Path::Local(LocalPath {
                        is_const: false,
                        idx: *idx,
                        mid: *mid,
                        segments: vec![],
                    });
                    //TODO: get root key
                    error_bad_key(
                        self,
                        self,
                        &path,
                        env.meta.name_dflt(*mid).to_string(),
                        vec![],
                        &env.meta,
                    )
                }

                _ => error_oops(self, "Unknown local variable", &env.meta),
            },
            ImutExprInt::Local {
                idx,
                is_const: true,
                ..
            } => match env.consts.get(*idx) {
                Some(v) => Ok(Cow::Borrowed(v)),
                _ => error_oops(self, "Unknown const variable", &env.meta),
            },
            ImutExprInt::Unary(ref expr) => self.unary(opts, env, event, state, meta, local, expr),
            ImutExprInt::Binary(ref expr) => {
                self.binary(opts, env, event, state, meta, local, expr)
            }
            ImutExprInt::Match(ref expr) => {
                self.match_expr(opts, env, event, state, meta, local, expr)
            }
            ImutExprInt::Comprehension(ref expr) => {
                self.comprehension(opts, env, event, state, meta, local, expr)
            }
        }
    }

    fn comprehension(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script ImutComprehension,
    ) -> Result<Cow<'run, Value<'event>>> {
        let mut value_vec = vec![];
        let target = &expr.target;
        let cases = &expr.cases;
        let target_value = stry!(target.run(opts, env, event, state, meta, local));

        if let Some(target_map) = target_value.as_object() {
            // Record comprehension case

            value_vec.reserve(target_map.len());

            'comprehension_outer: for (k, v) in target_map {
                let k = Value::from(k.clone());
                let v = v.clone();
                stry!(set_local_shadow(self, local, &env.meta, expr.key_id, k));
                stry!(set_local_shadow(self, local, &env.meta, expr.val_id, v));

                for e in cases {
                    if stry!(test_guard(
                        self, opts, env, event, state, meta, local, &e.guard
                    )) {
                        let v = stry!(self
                            .execute_effectors(opts, env, event, state, meta, local, e, &e.exprs,));
                        // NOTE: We are creating a new value so we have to clone;
                        value_vec.push(v.into_owned());
                        continue 'comprehension_outer;
                    }
                }
            }
        } else if let Some(target_array) = target_value.as_array() {
            // Array comprehension case

            value_vec.reserve(target_array.len());

            let mut count = 0;
            'comp_array_outer: for x in target_array {
                let k = count.into();
                let v = x.clone();
                stry!(set_local_shadow(self, local, &env.meta, expr.key_id, k));
                stry!(set_local_shadow(self, local, &env.meta, expr.val_id, v));

                for e in cases {
                    if stry!(test_guard(
                        self, opts, env, event, state, meta, local, &e.guard
                    )) {
                        let v = stry!(self
                            .execute_effectors(opts, env, event, state, meta, local, e, &e.exprs,));

                        value_vec.push(v.into_owned());
                        count += 1;
                        continue 'comp_array_outer;
                    }
                }
                count += 1;
            }
        }
        Ok(Cow::Owned(Value::Array(value_vec)))
    }

    #[inline]
    fn execute_effectors<T: BaseExpr>(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        inner: &'script T,
        effectors: &'script [ImutExpr<'script>],
    ) -> Result<Cow<'run, Value<'event>>> {
        // Since we don't have side effects we don't need to run anything but the last effector!
        if let Some(effector) = effectors.last() {
            effector.run(opts, env, event, state, meta, local)
        } else {
            error_missing_effector(self, inner, &env.meta)
        }
    }

    fn match_expr(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script ImutMatch,
    ) -> Result<Cow<'run, Value<'event>>> {
        let target = stry!(expr.target.run(opts, env, event, state, meta, local));

        for predicate in &expr.patterns {
            if stry!(test_predicate_expr(
                self,
                opts,
                env,
                event,
                state,
                meta,
                local,
                &target,
                &predicate.pattern,
                &predicate.guard,
            )) {
                return self.execute_effectors(
                    opts,
                    env,
                    event,
                    state,
                    meta,
                    local,
                    predicate,
                    &predicate.exprs,
                );
            }
        }
        error_no_clause_hit(self, &env.meta)
    }

    fn binary(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script BinExpr<'script>,
    ) -> Result<Cow<'run, Value<'event>>> {
        let lhs = stry!(expr.lhs.run(opts, env, event, state, meta, local));
        let rhs = stry!(expr.rhs.run(opts, env, event, state, meta, local));
        exec_binary(self, expr, &env.meta, expr.kind, &lhs, &rhs)
    }

    fn unary(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script UnaryExpr<'script>,
    ) -> Result<Cow<'run, Value<'event>>> {
        let rhs = stry!(expr.expr.run(opts, env, event, state, meta, local));
        // TODO align this implemenation to be similar to exec_binary?
        match exec_unary(expr.kind, &rhs) {
            Some(v) => Ok(v),
            None => error_invalid_unary(self, &expr.expr, expr.kind, &rhs, &env.meta),
        }
    }

    #[allow(clippy::too_many_lines)]
    // FIXME: Quite some overlap with `interpreter::resolve` (and some with `expr::assign`)
    fn present(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        path: &'script Path,
    ) -> Result<Cow<'run, Value<'event>>> {
        // Fetch the base of the path
        // TODO: Extract this into a method on `Path`?
        let base_value: &Value = match path {
            Path::Local(path) => match local.values.get(path.idx) {
                Some(Some(l)) => l,
                Some(None) => return Ok(Cow::Borrowed(&FALSE)),
                _ => return error_oops(self, "Unknown local variable", &env.meta),
            },
            Path::Const(path) => match env.consts.get(path.idx) {
                Some(v) => v,
                _ => return error_oops(self, "Unknown constant variable", &env.meta),
            },
            Path::Meta(_path) => meta,
            Path::Event(_path) => event,
            Path::State(_path) => state,
        };

        // Resolve the targeted value by applying all path segments
        let mut subrange: Option<&[Value]> = None;
        let mut current = base_value;
        for segment in path.segments() {
            match segment {
                // Next segment is an identifier: lookup the identifier on `current`, if it's an object
                Segment::Id { key, .. } => {
                    if let Some(c) = key.lookup(current) {
                        current = c;
                        subrange = None;
                        continue;
                    } else {
                        // No field for that id: not present
                        return Ok(Cow::Borrowed(&FALSE));
                    }
                }
                // Next segment is an index: index into `current`, if it's an array
                Segment::Idx { idx, .. } => {
                    if let Some(a) = current.as_array() {
                        let range_to_consider = subrange.unwrap_or_else(|| a.as_slice());
                        let idx = *idx;

                        if let Some(c) = range_to_consider.get(idx) {
                            current = c;
                            subrange = None;
                            continue;
                        } else {
                            // No element at the index: not present
                            return Ok(Cow::Borrowed(&FALSE));
                        }
                    } else {
                        // FIXME: Indexing into something that isn't an array: should this return
                        // false, or return an error?
                        return Ok(Cow::Borrowed(&FALSE));
                    }
                }
                // Next segment is an index range: index into `current`, if it's an array
                Segment::Range {
                    range_start,
                    range_end,
                    ..
                } => {
                    if let Some(a) = current.as_array() {
                        let array = subrange.unwrap_or_else(|| a.as_slice());
                        let start_idx = stry!(range_start.eval_to_index(
                            self, opts, env, event, state, meta, local, path, &array,
                        ));
                        let end_idx = stry!(range_end.eval_to_index(
                            self, opts, env, event, state, meta, local, path, &array,
                        ));

                        if end_idx < start_idx {
                            return error_decreasing_range(
                                self, segment, &path, start_idx, end_idx, &env.meta,
                            );
                        } else if end_idx > array.len() {
                            // Index is out of array bounds: not present
                            return Ok(Cow::Borrowed(&FALSE));
                        } else {
                            subrange = Some(&array[start_idx..end_idx]);
                            continue;
                        }
                    } else {
                        // FIXME: Indexing into something that isn't an array: should this return
                        // false, or return an error? (Should probably have the same answer as the
                        // same question a couple of lines higher in this function.)
                        return Ok(Cow::Borrowed(&FALSE));
                    }
                }
                // Next segment is an expression: run `expr` to know which key it signifies at runtime
                Segment::Element { expr, .. } => {
                    let key = stry!(expr.run(opts, env, event, state, meta, local));

                    let next = match (current, key.borrow()) {
                        // The segment resolved to an identifier, and `current` is an object: lookup
                        (Value::Object(o), Value::String(id)) => o.get(id),
                        // If `current` is an array, the segment has to be an index
                        (Value::Array(a), idx) => {
                            let array = subrange.unwrap_or_else(|| a.as_slice());
                            let idx = stry!(value_to_index(self, segment, idx, env, path, array));
                            array.get(idx)
                        }
                        // Anything else: not present
                        _other => None,
                    };
                    if let Some(next) = next {
                        current = next;
                        subrange = None;
                        continue;
                    } else {
                        return Ok(Cow::Borrowed(&FALSE));
                    }
                }
            }
        }

        Ok(Cow::Borrowed(&TRUE))
    }

    // TODO: Can we convince Rust to generate the 3 or 4 versions of this method from one template?
    fn invoke1(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script Invoke,
    ) -> Result<Cow<'run, Value<'event>>> {
        unsafe {
            let v = stry!(expr
                .args
                .get_unchecked(0)
                .run(opts, env, event, state, meta, local));
            expr.invocable
                .invoke(&env.context, &[v.borrow()])
                .map(Cow::Owned)
                .map_err(|e| {
                    let r: Option<&Registry> = None;
                    e.into_err(self, self, r, &env.meta)
                })
        }
    }

    fn invoke2(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script Invoke,
    ) -> Result<Cow<'run, Value<'event>>> {
        unsafe {
            let v1 = stry!(expr
                .args
                .get_unchecked(0)
                .run(opts, env, event, state, meta, local));
            let v2 = stry!(expr
                .args
                .get_unchecked(1)
                .run(opts, env, event, state, meta, local));
            expr.invocable
                .invoke(&env.context, &[v1.borrow(), v2.borrow()])
                .map(Cow::Owned)
                .map_err(|e| {
                    let r: Option<&Registry> = None;
                    e.into_err(self, self, r, &env.meta)
                })
        }
    }

    fn invoke3(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script Invoke,
    ) -> Result<Cow<'run, Value<'event>>> {
        unsafe {
            let v1 = stry!(expr
                .args
                .get_unchecked(0)
                .run(opts, env, event, state, meta, local));
            let v2 = stry!(expr
                .args
                .get_unchecked(1)
                .run(opts, env, event, state, meta, local));
            let v3 = stry!(expr
                .args
                .get_unchecked(2)
                .run(opts, env, event, state, meta, local));
            expr.invocable
                .invoke(&env.context, &[v1.borrow(), v2.borrow(), v3.borrow()])
                .map(Cow::Owned)
                .map_err(|e| {
                    let r: Option<&Registry> = None;
                    e.into_err(self, self, r, &env.meta)
                })
        }
    }

    fn invoke(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script Invoke,
    ) -> Result<Cow<'run, Value<'event>>> {
        let argv: Vec<Cow<'run, _>> = expr
            .args
            .iter()
            .map(|arg| arg.run(opts, env, event, state, meta, local))
            .collect::<Result<_>>()?;

        // Construct a view into `argv`, since `invoke` expects a slice of references, not Cows.
        let argv1: Vec<&Value> = argv.iter().map(Cow::borrow).collect();

        expr.invocable
            .invoke(&env.context, &argv1)
            .map(Cow::Owned)
            .map_err(|e| {
                let r: Option<&Registry> = None;
                e.into_err(self, self, r, &env.meta)
            })
    }

    #[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr)]
    fn emit_aggr(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        expr: &'script InvokeAggr,
    ) -> Result<Cow<'run, Value<'event>>> {
        if opts.aggr != AggrType::Emit {
            return error_oops(
                self,
                "Trying to emit aggreagate outside of emit context",
                &env.meta,
            );
        }

        unsafe {
            // FIXME?
            let invocable: &mut TremorAggrFnWrapper =
                mem::transmute(&env.aggrs[expr.aggr_id].invocable);
            let r = invocable.emit().map(Cow::Owned).map_err(|e| {
                let r: Option<&Registry> = None;
                e.into_err(self, self, r, &env.meta)
            })?;
            Ok(r)
        }
    }

    fn patch(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script Patch,
    ) -> Result<Cow<'run, Value<'event>>> {
        // NOTE: We clone this since we patch it - this should be not mutated but cloned

        let mut value = stry!(expr.target.run(opts, env, event, state, meta, local)).into_owned();
        stry!(patch_value(
            self, opts, env, event, state, meta, local, &mut value, expr,
        ));
        Ok(Cow::Owned(value))
    }

    fn merge(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script Merge,
    ) -> Result<Cow<'run, Value<'event>>> {
        // NOTE: We got to clone here since we're going to change the value
        let value = stry!(expr.target.run(opts, env, event, state, meta, local));

        if value.is_object() {
            // Make sure we clone the data so we don't mutate it in place
            let mut value = value.into_owned();
            let replacement = stry!(expr.expr.run(opts, env, event, state, meta, local));

            if replacement.is_object() {
                stry!(merge_values(self, &expr.expr, &mut value, &replacement));
                Ok(Cow::Owned(value))
            } else {
                error_need_obj(self, &expr.expr, replacement.value_type(), &env.meta)
            }
        } else {
            error_need_obj(self, &expr.target, value.value_type(), &env.meta)
        }
    }
}
