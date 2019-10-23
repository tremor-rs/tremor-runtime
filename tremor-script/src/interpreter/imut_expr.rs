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

use super::{
    exec_binary, exec_unary, merge_values, patch_value, resolve, set_local_shadow, test_guard,
    test_predicate_expr, AggrType, Env, ExecOpts, LocalStack, FALSE, TRUE,
};

use crate::ast::*;
use crate::errors::*;
use crate::registry::{Registry, TremorAggrFnWrapper};
use crate::stry;
use simd_json::value::borrowed::{Object, Value};
use simd_json::value::ValueTrait;
use std::borrow::Borrow;
use std::borrow::Cow;
use std::mem;

impl<'run, 'event, 'script> ImutExpr<'script>
where
    'script: 'event,
    'event: 'run,
{
    #[inline]
    pub fn eval_to_string(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
    ) -> Result<Cow<'event, str>> {
        match stry!(self.run(opts, env, event, meta, local)).borrow() {
            Value::String(s) => Ok(s.clone()),
            other => error_need_obj(self, self, other.value_type()),
        }
    }

    #[inline]
    pub fn run(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
    ) -> Result<Cow<'run, Value<'event>>> {
        match self {
            ImutExpr::Literal(literal) => Ok(Cow::Borrowed(&literal.value)),
            ImutExpr::Path(path) => resolve(self, opts, env, event, meta, local, path),
            ImutExpr::Present { path, .. } => self.present(opts, env, event, meta, local, path),
            ImutExpr::Record(ref record) => {
                let mut object: Object = Object::with_capacity(record.fields.len());

                for field in &record.fields {
                    let result = stry!(field.value.run(opts, env, event, meta, local));
                    let name = stry!(field.name.eval_to_string(opts, env, event, meta, local));
                    object.insert(name, result.into_owned());
                }

                Ok(Cow::Owned(Value::Object(object)))
            }
            ImutExpr::List(ref list) => {
                let mut r: Vec<Value<'event>> = Vec::with_capacity(list.exprs.len());
                for expr in &list.exprs {
                    r.push(stry!(expr.run(opts, env, event, meta, local)).into_owned());
                }
                Ok(Cow::Owned(Value::Array(r)))
            }
            ImutExpr::Invoke1(ref call) => self.invoke1(opts, env, event, meta, local, call),
            ImutExpr::Invoke2(ref call) => self.invoke2(opts, env, event, meta, local, call),
            ImutExpr::Invoke3(ref call) => self.invoke3(opts, env, event, meta, local, call),
            ImutExpr::Invoke(ref call) => self.invoke(opts, env, event, meta, local, call),
            ImutExpr::InvokeAggr(ref call) => self.emit_aggr(opts, env, call),
            ImutExpr::Patch(ref expr) => self.patch(opts, env, event, meta, local, expr),
            ImutExpr::Merge(ref expr) => self.merge(opts, env, event, meta, local, expr),
            ImutExpr::Local {
                idx,
                start,
                end,
                is_const: false,
                id,
            } => match local.values.get(*idx) {
                Some(Some(l)) => Ok(Cow::Borrowed(l)),
                Some(None) => {
                    let path: Path = Path::Local(LocalPath {
                        id: id.clone(),
                        is_const: false,
                        idx: *idx,
                        start: *start,
                        end: *end,
                        segments: vec![],
                    });
                    //TODO: get root key
                    error_bad_key(self, self, &path, id.to_string(), vec![])
                }

                _ => error_oops(self, "Unknown local variable"),
            },
            ImutExpr::Local {
                idx,
                is_const: true,
                ..
            } => match env.consts.get(*idx) {
                Some(v) => Ok(Cow::Borrowed(v)),
                _ => error_oops(self, "Unknown local variable"),
            },
            ImutExpr::Unary(ref expr) => self.unary(opts, env, event, meta, local, expr),
            ImutExpr::Binary(ref expr) => self.binary(opts, env, event, meta, local, expr),
            ImutExpr::Match(ref expr) => self.match_expr(opts, env, event, meta, local, expr),
            ImutExpr::Comprehension(ref expr) => {
                self.comprehension(opts, env, event, meta, local, expr)
            }
        }
    }

    fn comprehension(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script ImutComprehension,
    ) -> Result<Cow<'run, Value<'event>>> {
        //use std::borrow::Cow;
        let mut value_vec = vec![];
        let target = &expr.target;
        let cases = &expr.cases;
        let target_value = stry!(target.run(opts, env, event, meta, local));

        if let Some(target_map) = target_value.as_object() {
            // Record comprehension case
            value_vec.reserve(target_map.len());
            // NOTE: Since we we are going to create new data from this
            // object we are cloning it.
            // This is also required since we might mutate. If we restruct
            // mutation in the future we could get rid of this.

            'comprehension_outer: for (k, v) in target_map.clone() {
                stry!(set_local_shadow(self, local, expr.key_id, Value::String(k)));
                stry!(set_local_shadow(self, local, expr.val_id, v));
                for e in cases {
                    // FIXME: use test_gaurd_expr
                    if stry!(test_guard(self, opts, env, event, meta, local, &e.guard)) {
                        let v = stry!(
                            self.execute_effectors(opts, env, event, meta, local, e, &e.exprs,)
                        );
                        // NOTE: We are creating a new value so we have to clone;
                        value_vec.push(v.into_owned());
                        continue 'comprehension_outer;
                    }
                }
            }
        } else if let Some(target_array) = target_value.as_array() {
            // Array comprehension case

            value_vec.reserve(target_array.len());

            // NOTE: Since we we are going to create new data from this
            // object we are cloning it.
            // This is also required since we might mutate. If we restruct
            // mutation in the future we could get rid of this.

            let mut count = 0;
            'comp_array_outer: for x in target_array.clone() {
                stry!(set_local_shadow(self, local, expr.key_id, count.into()));
                stry!(set_local_shadow(self, local, expr.val_id, x));

                for e in cases {
                    // FIXME: use test_gaurd_expr
                    if stry!(test_guard(self, opts, env, event, meta, local, &e.guard)) {
                        let v = stry!(
                            self.execute_effectors(opts, env, event, meta, local, e, &e.exprs,)
                        );

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
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        inner: &'script T,
        effectors: &'script [ImutExpr<'script>],
    ) -> Result<Cow<'run, Value<'event>>> {
        if effectors.is_empty() {
            return error_missing_effector(self, inner);
        }
        // Since we don't have side effects we don't need to run anything but the last effector!
        let effector = &effectors[effectors.len() - 1];
        effector.run(opts, env, event, meta, local)
    }

    fn match_expr(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script ImutMatch,
    ) -> Result<Cow<'run, Value<'event>>> {
        let target = stry!(expr.target.run(opts, env, event, meta, local));

        for predicate in &expr.patterns {
            if stry!(test_predicate_expr(
                self,
                opts,
                env,
                event,
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
                    meta,
                    local,
                    predicate,
                    &predicate.exprs,
                );
            }
        }
        error_no_clause_hit(self)
    }

    fn binary(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script BinExpr<'script>,
    ) -> Result<Cow<'run, Value<'event>>> {
        let lhs = stry!(expr.lhs.run(opts, env, event, meta, local));
        let rhs = stry!(expr.rhs.run(opts, env, event, meta, local));
        match exec_binary(expr.kind, &lhs, &rhs) {
            Some(v) => Ok(v),
            None => error_invalid_binary(self, &expr.lhs, expr.kind, &lhs, &rhs),
        }
    }

    fn unary(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script UnaryExpr<'script>,
    ) -> Result<Cow<'run, Value<'event>>> {
        let rhs = stry!(expr.expr.run(opts, env, event, meta, local));
        match exec_unary(expr.kind, &rhs) {
            Some(v) => Ok(v),
            None => error_invalid_unary(self, &expr.expr, expr.kind, &rhs),
        }
    }

    fn present(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        path: &'script Path,
    ) -> Result<Cow<'run, Value<'event>>> {
        let mut subrange: Option<(usize, usize)> = None;
        let mut current: &Value = match path {
            Path::Local(path) => match local.values.get(path.idx) {
                Some(Some(l)) => l,
                Some(None) => return Ok(Cow::Borrowed(&FALSE)),
                _ => return error_oops(self, "Unknown local variable"),
            },
            Path::Const(path) => match env.consts.get(path.idx) {
                Some(v) => v,
                _ => return error_oops(self, "Unknown constant variable"),
            },
            Path::Meta(_path) => meta,
            Path::Event(_path) => event,
        };

        for segment in path.segments() {
            match segment {
                Segment::Id { id, .. } => {
                    if let Some(o) = current.as_object() {
                        if let Some(c) = o.get(id) {
                            current = c;
                            subrange = None;
                            continue;
                        } else {
                            return Ok(Cow::Borrowed(&FALSE));
                        }
                    } else {
                        return Ok(Cow::Borrowed(&FALSE));
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
                            return Ok(Cow::Borrowed(&FALSE));
                        }
                    } else {
                        return Ok(Cow::Borrowed(&FALSE));
                    }
                }

                Segment::Element { expr, .. } => {
                    let next = match (
                        current,
                        stry!(expr.run(opts, env, event, meta, local)).borrow(),
                    ) {
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
                                a.get(idx)
                            } else {
                                return Ok(Cow::Borrowed(&FALSE));
                            }
                        }
                        (Value::Object(o), Value::String(id)) => o.get(id),
                        _other => return Ok(Cow::Borrowed(&FALSE)),
                    };
                    if let Some(next) = next {
                        current = next;
                        subrange = None;
                        continue;
                    } else {
                        return Ok(Cow::Borrowed(&FALSE));
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
                                let range_end = range_end + start;
                                // We're exceeding the erray
                                if range_end >= end {
                                    return Ok(Cow::Borrowed(&FALSE));
                                } else {
                                    subrange = Some((range_start, range_end));
                                    continue;
                                }
                            }
                        }
                        return Ok(Cow::Borrowed(&FALSE));
                    } else {
                        return Ok(Cow::Borrowed(&FALSE));
                    }
                }
            }
        }

        Ok(Cow::Borrowed(&TRUE))
    }

    fn invoke1(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script Invoke,
    ) -> Result<Cow<'run, Value<'event>>> {
        unsafe {
            let v = stry!(expr
                .args
                .get_unchecked(0)
                .run(opts, env, event, meta, local));
            expr.invocable
                .invoke(&env.context, &[v.borrow()])
                .map(Cow::Owned)
                .map_err(|e| {
                    let r: Option<&Registry> = None;
                    e.into_err(self, self, r)
                })
        }
    }

    fn invoke2(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script Invoke,
    ) -> Result<Cow<'run, Value<'event>>> {
        unsafe {
            let v1 = stry!(expr
                .args
                .get_unchecked(0)
                .run(opts, env, event, meta, local));
            let v2 = stry!(expr
                .args
                .get_unchecked(1)
                .run(opts, env, event, meta, local));
            expr.invocable
                .invoke(&env.context, &[v1.borrow(), v2.borrow()])
                .map(Cow::Owned)
                .map_err(|e| {
                    let r: Option<&Registry> = None;
                    e.into_err(self, self, r)
                })
        }
    }

    fn invoke3(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script Invoke,
    ) -> Result<Cow<'run, Value<'event>>> {
        unsafe {
            let v1 = stry!(expr
                .args
                .get_unchecked(0)
                .run(opts, env, event, meta, local));
            let v2 = stry!(expr
                .args
                .get_unchecked(1)
                .run(opts, env, event, meta, local));
            let v3 = stry!(expr
                .args
                .get_unchecked(2)
                .run(opts, env, event, meta, local));
            expr.invocable
                .invoke(&env.context, &[v1.borrow(), v2.borrow(), v3.borrow()])
                .map(Cow::Owned)
                .map_err(|e| {
                    let r: Option<&Registry> = None;
                    e.into_err(self, self, r)
                })
        }
    }

    fn invoke(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script Invoke,
    ) -> Result<Cow<'run, Value<'event>>> {
        let mut argv: Vec<Cow<'run, Value<'event>>> = Vec::with_capacity(expr.args.len());
        let mut argv1: Vec<&Value> = Vec::with_capacity(expr.args.len());
        for arg in &expr.args {
            let result = stry!(arg.run(opts, env, event, meta, local));
            argv.push(result);
        }
        unsafe {
            for i in 0..argv.len() {
                argv1.push(argv.get_unchecked(i));
            }
        }
        expr.invocable
            .invoke(&env.context, &argv1)
            .map(Cow::Owned)
            .map_err(|e| {
                let r: Option<&Registry> = None;
                e.into_err(self, self, r)
            })
    }

    fn emit_aggr(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        expr: &'script InvokeAggr,
    ) -> Result<Cow<'run, Value<'event>>> {
        if opts.aggr != AggrType::Emit {
            return error_oops(self, "Trying to emit aggreagate outside of emit context");
        }

        unsafe {
            // FIXME?
            #[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr)]
            let invocable: &mut TremorAggrFnWrapper =
                mem::transmute(&env.aggrs[expr.aggr_id].invocable);
            let r = invocable.emit().map(Cow::Owned).map_err(|e| {
                let r: Option<&Registry> = None;
                e.into_err(self, self, r)
            })?;
            invocable.init();
            Ok(r)
        }
    }

    fn patch(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script Patch,
    ) -> Result<Cow<'run, Value<'event>>> {
        // NOTE: We clone this since we patch it - this should be not mutated but cloned

        let mut value = stry!(expr.target.run(opts, env, event, meta, local)).into_owned();
        stry!(patch_value(
            self, opts, env, event, meta, local, &mut value, expr,
        ));
        Ok(Cow::Owned(value))
    }

    fn merge(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script Merge,
    ) -> Result<Cow<'run, Value<'event>>> {
        // NOTE: We got to clone here since we're are going
        // to change the value
        let value = stry!(expr.target.run(opts, env, event, meta, local));

        if value.is_object() {
            // Make sure we clone the data so we don't muate it in place
            let mut value = value.into_owned();
            let replacement = stry!(expr.expr.run(opts, env, event, meta, local));

            if replacement.is_object() {
                stry!(merge_values(self, &expr.expr, &mut value, &replacement));
                Ok(Cow::Owned(value))
            } else {
                error_need_obj(self, &expr.expr, replacement.value_type())
            }
        } else {
            error_need_obj(self, &expr.target, value.value_type())
        }
    }
}
