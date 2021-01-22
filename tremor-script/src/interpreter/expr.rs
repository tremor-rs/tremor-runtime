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

use super::{
    merge_values, patch_value, resolve, set_local_shadow, test_guard, test_predicate_expr, Env,
    ExecOpts, LocalStack, NULL,
};
use crate::ast::{
    BaseExpr, Comprehension, EmitExpr, EventPath, Expr, ImutExprInt, Match, Merge, Patch, Path,
    Segment,
};
use crate::errors::{
    error_assign_array, error_assign_to_const, error_bad_key, error_invalid_assign_target,
    error_need_obj, error_no_clause_hit, error_oops, Result,
};
use crate::prelude::*;
use crate::registry::RECUR_PTR;
use crate::{stry, Value};
use matches::matches;
use std::borrow::{Borrow, Cow};
use std::mem;

#[derive(Debug)]
/// Continuation context to control program flow
pub enum Cont<'run, 'event>
where
    'event: 'run,
{
    /// Continue
    Cont(Cow<'run, Value<'event>>),
    /// Emit with default system supplied port
    Emit(Value<'event>, Option<String>),
    /// Drop
    Drop,
    /// Emit with user supplied port
    EmitEvent(Option<String>),
}

macro_rules! demit {
    ($data:expr) => {
        match stry!($data) {
            Cont::Cont(r) => r,
            Cont::Emit(v, p) => return Ok(Cont::Emit(v, p)),
            Cont::Drop => return Ok(Cont::Drop),
            Cont::EmitEvent(p) => return Ok(Cont::EmitEvent(p)),
        }
    };
}

impl<'script, 'event, 'run> Expr<'script>
where
    'script: 'event,
    'event: 'run,
{
    #[inline]
    fn execute_effectors(
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        effectors: &'script [Expr<'script>],
        last_effector: &'script Expr<'script>,
    ) -> Result<Cont<'run, 'event>> {
        for effector in effectors {
            demit!(effector.run(opts.without_result(), env, event, state, meta, local));
        }
        Ok(Cont::Cont(demit!(
            last_effector.run(opts, env, event, state, meta, local)
        )))
    }

    #[inline]
    fn match_expr(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        expr: &'script Match,
    ) -> Result<Cont<'run, 'event>> {
        let target = stry!(expr.target.run(opts, env, event, state, meta, local));

        for predicate in &expr.patterns {
            let p = &predicate.pattern;
            let g = &predicate.guard;
            if stry!(test_predicate_expr(
                self, opts, env, event, state, meta, local, &target, p, g,
            )) {
                let e = &predicate.exprs;
                let l = &predicate.last_expr;
                return Self::execute_effectors(opts, env, event, state, meta, local, e, l);
            }
        }
        error_no_clause_hit(self, &env.meta)
    }

    #[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr)]
    fn patch_in_place(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'script Patch,
    ) -> Result<Cow<'run, Value<'event>>> {
        // This function is called when we encounter code that consumes a value
        // to patch it. So the following code:
        // ```tremor
        // let event = patch event of insert "key" => "value" end
        // ```
        // When executed on it's own would clone the event, add a key and
        // overwrite original event.
        //
        // We optimise this as:
        // ```
        // patch_in_place event of insert "key" => "value" end
        // ```
        //
        // This code is generated in impl Upable for ExprRaw where the following
        // checks are performed:
        //
        // 1) the patch is on the RHS of an assignment
        // 2) the path of the assigned value and the path of the patched
        //    expression are identical.
        //
        // In turn this guarantees (at compile time):
        //
        // 1) The target (`expr`) is a path lookup
        // 2) The target is not a known constant as otherwise the assignment
        //    will complan
        // 3) this leave the `expr` to be either a local, the event, the state,
        //    metadata or a subkey thereof.
        //
        // And the following guarantees at run time:
        //
        // 1) the `expr` is an existing key of the mentioned categories,
        //    otherwise `expr.target.run` will error.
        // 2) `value` will never be owned (however the resolve function is
        //    generic so it needs to return a Cow)

        let value: Cow<'run, Value<'event>> =
            stry!(expr.target.run(opts, env, event, state, meta, local));
        debug_assert!(
            !matches!(value, Cow::Owned(_)),
            "We should never see a owned value here as patch_in_place is only ever called on existing data in event, state, meta or local"
        );
        let v: &Value<'event> = value.borrow();
        let v: &mut Value<'event> = unsafe { mem::transmute(v) };
        stry!(patch_value(
            self, opts, env, event, state, meta, local, v, expr
        ));
        Ok(value)
    }

    #[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr)]
    fn merge_in_place(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        expr: &'script Merge,
    ) -> Result<Cow<'run, Value<'event>>> {
        // Please see the soundness reasoning in `patch_in_place` for details
        // those functions perform the same function just with slighty different
        // operations.
        let value_cow: Cow<'run, Value<'event>> =
            stry!(expr.target.run(opts, env, event, state, meta, local));
        debug_assert!(
            !matches!(value_cow, Cow::Owned(_)),
            "We should never see a owned value here as merge_in_place is only ever called on existing data in event, state, meta or local"
        );

        if value_cow.is_object() {
            let value: &Value<'event> = value_cow.borrow();
            let value: &mut Value<'event> = unsafe { mem::transmute(value) };
            let replacement = stry!(expr.expr.run(opts, env, event, state, meta, local,));

            if replacement.is_object() {
                stry!(merge_values(self, &expr.expr, value, &replacement));
                Ok(value_cow)
            } else {
                error_need_obj(self, &expr.expr, replacement.value_type(), &env.meta)
            }
        } else {
            error_need_obj(self, &expr.target, value_cow.value_type(), &env.meta)
        }
    }

    // TODO: Quite some overlap with `ImutExprInt::comprehension`
    fn comprehension(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        expr: &'script Comprehension,
    ) -> Result<Cont<'run, 'event>> {
        let mut value_vec = vec![];
        let target = &expr.target;
        let cases = &expr.cases;
        let target_value = stry!(target.run(opts, env, event, state, meta, local,));

        let (l, items): (usize, Box<dyn Iterator<Item = (Value, Value)>>) =
            target_value.as_object().map_or_else(
                || {
                    target_value
                        .as_array()
                        .map_or_else::<(usize, Box<dyn Iterator<Item = (Value, Value)>>), _, _>(
                            || (0, Box::new(std::iter::empty())),
                            |target_array| {
                                (
                                    target_array.len(),
                                    Box::new(
                                        target_array
                                            .clone()
                                            .into_iter()
                                            .enumerate()
                                            .map(|(k, v)| (Value::from(k), v)),
                                    ),
                                )
                            },
                        )
                },
                |target_map| {
                    (
                        target_map.len(),
                        Box::new(
                            target_map
                                .clone()
                                .into_iter()
                                .map(|(k, v)| (Value::from(k), v)),
                        ),
                    )
                },
            );

        if opts.result_needed {
            value_vec.reserve(l);
        }

        'outer: for (k, v) in items {
            stry!(set_local_shadow(self, local, &env.meta, expr.key_id, k));
            stry!(set_local_shadow(self, local, &env.meta, expr.val_id, v));

            for e in cases {
                if stry!(test_guard(
                    self, opts, env, event, state, meta, local, &e.guard
                )) {
                    let es = &e.exprs;
                    let l = &e.last_expr;
                    let v = demit!(Self::execute_effectors(
                        opts, env, event, state, meta, local, es, l,
                    ));
                    // NOTE: We are creating a new value so we have to clone;
                    if opts.result_needed {
                        value_vec.push(v.into_owned());
                    }
                    continue 'outer;
                }
            }
        }

        Ok(Cont::Cont(Cow::Owned(Value::from(value_vec))))
    }

    #[inline]
    fn assign(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        path: &'script Path,
        value: Value<'event>,
    ) -> Result<Cow<'run, Value<'event>>> {
        if path.segments().is_empty() {
            self.assign_direct(opts, env, event, state, meta, local, path, value)
        } else {
            self.assign_nested(opts, env, event, state, meta, local, path, value)
        }
    }

    #[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr)]
    fn assign_nested(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        path: &'script Path,
        mut value: Value<'event>,
    ) -> Result<Cow<'run, Value<'event>>> {
        /* NOTE
         * This function is icky we got to do some trickery here.
         * Since it's dangerous and icky it deserves some explanation
         * What we do here is we borrow the target we want to set
         * as immutable and mem::transmute it to mutable where needed.
         *
         * We do this since there is no way to tell rust that it's safe
         * to borrow immutable out of something that's mutable even if
         * we clone data out.
         *
         * This is safe because:
         *
         * We only borrow Cow<'event, str> out of the host. So the
         * reference points to either the event or script and we
         * never mutate strings only ever replace them.
         * So even if the map the Cow originally came from we won't
         * lose the referenced data. (Famous last words)
         */
        let segments = path.segments();

        let mut current: &Value = match path {
            Path::Const(p) => {
                let name = env.meta.name_dflt(p.mid).to_string();
                return error_assign_to_const(self, name, &env.meta);
            }
            Path::Local(lpath) => {
                if let Some(l) = stry!(local.get(lpath.idx, self, lpath.mid(), &env.meta)) {
                    let l: &mut Value<'event> = unsafe { mem::transmute(l) };
                    l
                } else {
                    let key = env.meta.name_dflt(lpath.mid).to_string();
                    return error_bad_key(self, lpath, &path, key, vec![], &env.meta);
                }
            }
            Path::Meta(_path) => meta,
            Path::Event(_path) => event,
            Path::State(_path) => {
                // Extend the lifetime of value to be static (also forces all strings and
                // object keys in value to be owned COW's). This ensures that the current
                // value is kept as part of state across subsequent state assignments (if
                // users choose to do so).
                value = value.into_static();
                state
            }
        };
        for segment in segments {
            match segment {
                Segment::Id { key, .. } => {
                    current = if let Ok(next) = key.lookup_or_insert_mut(
                        unsafe { mem::transmute::<&Value, &mut Value>(current) },
                        || Value::object_with_capacity(halfbrown::VEC_LIMIT_UPPER),
                    ) {
                        next
                    } else {
                        return error_need_obj(self, segment, current.value_type(), &env.meta);
                    };
                }
                Segment::Element { expr, .. } => {
                    let id = stry!(expr.eval_to_string(opts, env, event, state, meta, local));
                    let v: &mut Value<'event> = unsafe { mem::transmute(current) };
                    if let Some(map) = v.as_object_mut() {
                        current = match map.get_mut(&id) {
                            Some(v) => v,
                            None => map
                                .entry(id)
                                .or_insert_with(|| Value::object_with_capacity(32)),
                        }
                    } else {
                        return error_need_obj(self, segment, current.value_type(), &env.meta);
                    }
                }
                Segment::Idx { .. } | Segment::Range { .. } => {
                    return error_assign_array(self, segment, &env.meta)
                }
            }
        }
        unsafe {
            *mem::transmute::<&Value<'event>, &mut Value<'event>>(current) = value;
        }
        if opts.result_needed {
            resolve(self, opts, env, event, state, meta, local, path)
        } else {
            Ok(Cow::Borrowed(&NULL))
        }
    }

    #[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr)]
    fn assign_direct(
        &'script self,
        _opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        _meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        path: &'script Path,
        mut value: Value<'event>,
    ) -> Result<Cow<'run, Value<'event>>> {
        match path {
            Path::Const(p) => {
                error_assign_to_const(self, env.meta.name_dflt(p.mid).into(), &env.meta)
            }
            Path::Local(lpath) => match stry!(local.get(lpath.idx, self, lpath.mid(), &env.meta)) {
                Some(l) => {
                    let l: &mut Value<'event> = unsafe { mem::transmute(l) };
                    *l = value;
                    Ok(Cow::Borrowed(l))
                }
                d => {
                    let d: &mut Option<Value<'event>> = unsafe { mem::transmute(d) };
                    *d = Some(value);
                    if let Some(l) = d {
                        Ok(Cow::Borrowed(l))
                    } else {
                        // ALLOW: we assign d in the line above the access, we know it is Some
                        unreachable!()
                    }
                }
            },
            Path::Meta(_path) => error_invalid_assign_target(self, &env.meta),
            Path::Event(_path) => {
                *event = value;
                Ok(Cow::Borrowed(event))
            }
            Path::State(_path) => {
                // Extend the lifetime of value to be static (also forces all strings and
                // object keys in value to be owned COW's). This ensures that the current
                // value is kept as part of state across subsequent state assignments (if
                // users choose to do so).
                value = value.into_static();
                // for the compiler, the type of value still has 'event as the lifetime
                // so we transmute it to conform with the lifetime of state ('static).
                *state = unsafe { mem::transmute(value) };
                Ok(Cow::Borrowed(state))
            }
        }
    }

    #[inline]
    /// Invokes expression
    pub fn run(
        &'script self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event, 'script>,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
    ) -> Result<Cont<'run, 'event>> {
        match self {
            Expr::Emit(expr) => match expr.borrow() {
                EmitExpr {
                    expr: ImutExprInt::Path(Path::Event(EventPath { segments, .. })),
                    port,
                    ..
                } if segments.is_empty() => {
                    let port = if let Some(port) = port {
                        Some(
                            stry!(port.eval_to_string(opts, env, event, state, meta, local))
                                .to_string(),
                        )
                    } else {
                        None
                    };
                    Ok(Cont::EmitEvent(port))
                }
                expr => {
                    let port = if let Some(port) = &expr.port {
                        Some(
                            stry!(port.eval_to_string(opts, env, event, state, meta, local))
                                .to_string(),
                        )
                    } else {
                        None
                    };
                    Ok(Cont::Emit(
                        stry!(expr.expr.run(opts, env, event, state, meta, local)).into_owned(),
                        port,
                    ))
                }
            },
            Expr::Drop { .. } => Ok(Cont::Drop),
            Expr::AssignMoveLocal { idx, path, .. } => {
                // This is a special case when we know that we'll not use
                // this local variable again, it allows os to
                // move the variable instead of cloning it

                let value = if let Some(v) = local.values.get_mut(*idx) {
                    let mut opt: Option<Value> = None;
                    std::mem::swap(v, &mut opt);
                    if let Some(v) = opt {
                        v
                    } else {
                        return error_oops(
                            self,
                            0xdead_000c,
                            "Unknown local variable in Expr::AssignMoveLocal",
                            &env.meta,
                        );
                    }
                } else {
                    return error_oops(
                        self,
                        0xdead_000b,
                        "Unknown local variable in Expr::AssignMoveLocal",
                        &env.meta,
                    );
                };
                self.assign(opts, env, event, state, meta, local, &path, value)
                    .map(Cont::Cont)
            }
            Expr::Assign { expr, path, .. } => {
                // NOTE Since we are assigning a new value we do cline here.
                // This is intended behaviour
                let value = demit!(expr.run(opts.with_result(), env, event, state, meta, local))
                    .into_owned();
                self.assign(opts, env, event, state, meta, local, &path, value)
                    .map(Cont::Cont)
            }
            Expr::Match(ref expr) => self.match_expr(opts, env, event, state, meta, local, expr),
            Expr::MergeInPlace(ref expr) => self
                .merge_in_place(opts, env, event, state, meta, local, expr)
                .map(Cont::Cont),
            Expr::PatchInPlace(ref expr) => self
                .patch_in_place(opts, env, event, state, meta, local, expr)
                .map(Cont::Cont),
            Expr::Comprehension(ref expr) => {
                self.comprehension(opts, env, event, state, meta, local, expr)
            }
            Expr::Imut(expr) => {
                // If we don't need the result of a immutable value then we
                // don't need to evaluate it.
                let r = if opts.result_needed {
                    expr.run(opts, env, event, state, meta, local)?
                } else {
                    Cow::Borrowed(&NULL)
                };
                if let Cow::Borrowed(v) = r {
                    let this_ptr = v.as_str().map(str::as_ptr);
                    if this_ptr == RECUR_PTR {
                        // NOTE: we abuse drop here to imply recursion - yes it
                        // makes no sense!
                        return Ok(Cont::Drop);
                    }
                };
                Ok(Cont::Cont(r))
            }
        }
    }
}
