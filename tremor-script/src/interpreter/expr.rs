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
    merge_values, patch_value, resolve, set_local_shadow, test_guard, test_predicate_expr,
    ExecOpts, LocalStack, LocalValue, NULL,
};
use crate::ast::*;
use crate::errors::*;
use crate::stry;
use crate::EventContext;
use simd_json::value::{
    borrowed::{Map, Value},
    ValueTrait,
};
use std::borrow::{Borrow, Cow};

#[derive(Debug)]
pub enum Cont<'run, 'event>
where
    'event: 'run,
{
    Cont(Cow<'run, Value<'event>>),
    Emit(Value<'event>, Option<String>),
    Drop,
    EmitEvent(Option<String>),
}

impl<'run, 'event, 'script> Cont<'run, 'event>
where
    'script: 'event,
    'event: 'run,
{
    pub fn into_value<O: BaseExpr, I: BaseExpr>(
        self,
        outer: &O,
        inner: &I,
    ) -> Result<Cow<'run, Value<'event>>> {
        match self {
            Cont::Cont(v) => Ok(v),
            Cont::EmitEvent(_p) => {
                Err(ErrorKind::InvalidEmit(outer.extent(), inner.extent()).into())
            }
            Cont::Emit(_v, _p) => Err(ErrorKind::InvalidEmit(outer.extent(), inner.extent()).into()),
            Cont::Drop => Err(ErrorKind::InvalidDrop(outer.extent(), inner.extent()).into()),
        }
    }
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
    fn execute_effectors<T: BaseExpr>(
        &'script self,
        opts: ExecOpts,
        context: &'run EventContext,
        aggrs: &'run [InvokeAggrFn<'script>],
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        consts: &'run [Value<'event>],
        inner: &'script T,
        effectors: &'script [Expr<'script>],
    ) -> Result<Cont<'run, 'event>> {
        if effectors.is_empty() {
            return error_missing_effector(self, inner);
        }
        // We know we have at least one element so [] access is safe!
        for effector in &effectors[..effectors.len() - 1] {
            demit!(effector.run(
                opts.without_result(),
                context,
                aggrs,
                event,
                meta,
                local,
                consts
            ));
        }
        let effector = &effectors[effectors.len() - 1];
        Ok(Cont::Cont(demit!(
            effector.run(opts, context, aggrs, event, meta, local, consts)
        )))
    }

    #[inline]
    fn match_expr(
        &'script self,
        opts: ExecOpts,
        context: &'run EventContext,
        aggrs: &'run [InvokeAggrFn<'script>],
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        consts: &'run [Value<'event>],
        expr: &'script Match,
    ) -> Result<Cont<'run, 'event>> {
        let target = stry!(expr
            .target
            .run(opts, context, aggrs, event, meta, local, consts));

        for predicate in &expr.patterns {
            if stry!(test_predicate_expr(
                self,
                opts,
                context,
                aggrs,
                event,
                meta,
                local,
                consts,
                &target,
                &predicate.pattern,
                &predicate.guard,
            )) {
                return self.execute_effectors(
                    opts,
                    context,
                    aggrs,
                    event,
                    meta,
                    local,
                    consts,
                    predicate,
                    &predicate.exprs,
                );
            }
        }
        error_no_clause_hit(self)
    }

    fn patch_in_place(
        &'script self,
        opts: ExecOpts,
        context: &'run EventContext,
        aggrs: &'run [InvokeAggrFn<'script>],
        event: &'run Value<'event>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        consts: &'run [Value<'event>],
        expr: &'script Patch,
    ) -> Result<Cow<'run, Value<'event>>> {
        use std::mem;
        // NOTE: Is this good? I don't like it.
        let value = stry!(expr
            .target
            .run(opts, context, aggrs, event, meta, local, consts));
        let v: &Value = value.borrow();
        #[allow(mutable_transmutes)]
        #[allow(clippy::transmute_ptr_to_ptr)]
        let v: &mut Value = unsafe { mem::transmute(v) };
        stry!(patch_value(
            self, opts, context, aggrs, event, meta, local, consts, v, expr
        ));
        Ok(Cow::Borrowed(v))
    }

    fn merge_in_place(
        &'script self,
        opts: ExecOpts,
        context: &'run EventContext,
        aggrs: &'run [InvokeAggrFn<'script>],
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        consts: &'run [Value<'event>],
        expr: &'script Merge,
    ) -> Result<Cow<'run, Value<'event>>> {
        use std::mem;
        // NOTE: Is this good? I don't like it.
        let value_cow = stry!(expr
            .target
            .run(opts, context, aggrs, event, meta, local, consts));
        let value: &Value = value_cow.borrow();
        #[allow(mutable_transmutes)]
        #[allow(clippy::transmute_ptr_to_ptr)]
        let value: &mut Value = unsafe { mem::transmute(value) };

        if value.is_object() {
            let replacement = stry!(expr
                .expr
                .run(opts, context, aggrs, event, meta, local, consts));

            if replacement.is_object() {
                stry!(merge_values(self, &expr.expr, value, &replacement));
                Ok(Cow::Borrowed(value))
            } else {
                error_type_conflict(self, &expr.expr, replacement.kind(), ValueType::Object)
            }
        } else {
            error_type_conflict(self, &expr.target, value.kind(), ValueType::Object)
        }
    }

    fn comprehension(
        &'script self,
        opts: ExecOpts,
        context: &'run EventContext,
        aggrs: &'run [InvokeAggrFn<'script>],
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        consts: &'run [Value<'event>],
        expr: &'script Comprehension,
    ) -> Result<Cont<'run, 'event>> {
        //use std::borrow::Cow;
        let mut value_vec = vec![];
        let target = &expr.target;
        let cases = &expr.cases;
        let target_value = stry!(target.run(opts, context, aggrs, event, meta, local, consts));

        if let Some(target_map) = target_value.as_object() {
            // Record comprehension case
            value_vec.reserve(if opts.result_needed {
                target_map.len()
            } else {
                0
            });
            // NOTE: Since we we are going to create new data from this
            // object we are cloning it.
            // This is also required since we might mutate. If we restruct
            // mutation in the future we could get rid of this.

            'comprehension_outer: for (k, v) in target_map.clone() {
                stry!(set_local_shadow(self, local, expr.key_id, Value::String(k)));
                stry!(set_local_shadow(self, local, expr.val_id, v));
                for e in cases {
                    if stry!(test_guard(
                        self, opts, context, aggrs, event, meta, local, consts, &e.guard
                    )) {
                        let v = demit!(self.execute_effectors(
                            opts, context, aggrs, event, meta, local, consts, e, &e.exprs,
                        ));
                        // NOTE: We are creating a new value so we have to clone;
                        if opts.result_needed {
                            value_vec.push(v.into_owned());
                        }
                        continue 'comprehension_outer;
                    }
                }
            }
        } else if let Some(target_array) = target_value.as_array() {
            // Array comprehension case

            value_vec.reserve(if opts.result_needed {
                target_array.len()
            } else {
                0
            });

            // NOTE: Since we we are going to create new data from this
            // object we are cloning it.
            // This is also required since we might mutate. If we restruct
            // mutation in the future we could get rid of this.

            let mut count = 0;
            'comp_array_outer: for x in target_array.clone() {
                stry!(set_local_shadow(self, local, expr.key_id, count.into()));
                stry!(set_local_shadow(self, local, expr.val_id, x));

                for e in cases {
                    if stry!(test_guard(
                        self, opts, context, aggrs, event, meta, local, consts, &e.guard
                    )) {
                        let v = demit!(self.execute_effectors(
                            opts, context, aggrs, event, meta, local, consts, e, &e.exprs,
                        ));

                        if opts.result_needed {
                            value_vec.push(v.into_owned());
                            count += 1;
                        }
                        continue 'comp_array_outer;
                    }
                }
                count += 1;
            }
        }
        Ok(Cont::Cont(Cow::Owned(Value::Array(value_vec))))
    }

    #[allow(mutable_transmutes)]
    #[allow(clippy::transmute_ptr_to_ptr)]
    fn assign(
        &'script self,
        opts: ExecOpts,
        context: &'run EventContext,
        aggrs: &'run [InvokeAggrFn<'script>],
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        consts: &'run [Value<'event>],
        path: &'script Path,
        value: Value<'event>,
    ) -> Result<Cow<'run, Value<'event>>> {
        /* NOTE
         * This function is icky we got to do some trickery here.
         * Since it's dangerous and icky it deserves some explenation
         * What we do here is we borrow the target we want to set
         * as imutable and mem::transmute it to mutable where needed.
         *
         * We do this since there is no way to tell rust that it's safe
         * to borrow imuatble out of something that' mutable even if
         * we clone data out.
         *
         * This is safe because:
         *
         * We only borrow Cow<'event, str> out of the host. So the
         * reference points to either the event or script and we.
         * never mutate strings only ever replace them.
         * So even if the map the Cow orriginally came form we won't
         * loose the refferenced data. (Famous last words)
         */
        use std::mem;
        let segments = path.segments();

        if segments.is_empty() {
            if let Path::Event(segments) = path {
                if segments.segments.is_empty() {
                    *event = value;
                    return Ok(Cow::Borrowed(event));
                }
            }
        }
        let mut current: &Value = unsafe {
            match path {
                Path::Const(p) => return error_assign_to_const(self, p.id.to_string()),
                Path::Local(lpath) => match local.values.get(lpath.idx) {
                    Some(Some(l)) => {
                        let l: &mut LocalValue = mem::transmute(l);
                        if segments.is_empty() {
                            l.v = value;
                            return Ok(Cow::Borrowed(&l.v));
                        }

                        &l.v
                    }
                    Some(d) => {
                        let d: &mut Option<LocalValue> = mem::transmute(d);
                        if segments.is_empty() {
                            *d = Some(LocalValue { v: value });
                            if let Some(l) = d {
                                return Ok(Cow::Borrowed(&l.v));
                            } else {
                                return error_oops(self);
                            }
                        }
                        return error_bad_key(self, lpath, &path, lpath.id.to_string(), vec![]);
                    }

                    _ => return error_oops(self),
                },
                Path::Meta(_path) => {
                    if segments.is_empty() {
                        return error_invalid_assign_target(self);
                    }
                    meta
                }
                Path::Event(_path) => {
                    if segments.is_empty() {
                        *event = value;
                        return Ok(Cow::Borrowed(event));
                    };
                    event
                }
            }
        };

        for segment in segments {
            unsafe {
                match segment {
                    Segment::IdSelector { id, .. } => {
                        if let Value::Object(ref mut map) =
                            mem::transmute::<&Value, &mut Value>(current)
                        {
                            current = match map.get_mut(id) {
                                Some(v) => v,
                                None => {
                                    map.insert(id.clone(), Value::Object(Map::with_capacity(32)));
                                    // NOTE this is safe because we just added this element
                                    // to the map.
                                    map.get_mut(id).unwrap_or_else(|| unreachable!())
                                }
                            }
                        } else {
                            return error_type_conflict(
                                self,
                                segment,
                                current.kind(),
                                ValueType::Object,
                            );
                        }
                    }
                    Segment::ElementSelector { expr, .. } => {
                        let id =
                            stry!(expr
                                .eval_to_string(opts, context, aggrs, event, meta, local, consts));
                        if let Value::Object(ref mut map) =
                            mem::transmute::<&Value, &mut Value>(current)
                        {
                            current = match map.get_mut(&id) {
                                Some(v) => v,
                                None => {
                                    map.insert(id.clone(), Value::Object(Map::with_capacity(32)));
                                    // NOTE this is safe because we just added this element
                                    // to the map.
                                    map.get_mut(&id).unwrap_or_else(|| unreachable!())
                                }
                            }
                        } else {
                            return error_type_conflict(
                                self,
                                segment,
                                current.kind(),
                                ValueType::Object,
                            );
                        }
                    }
                    Segment::IdxSelector { .. } => return error_assign_array(self, segment),
                    Segment::RangeSelector { .. } => return error_assign_array(self, segment),
                }
            }
        }
        unsafe {
            *mem::transmute::<&Value, &mut Value>(current) = value;
        }
        if opts.result_needed {
            //Ok(Cow::Borrowed(current))
            resolve(self, opts, context, aggrs, event, meta, local, consts, path)
        } else {
            Ok(Cow::Borrowed(&NULL))
        }
    }

    #[inline(always)]
    pub fn run(
        &'script self,
        opts: ExecOpts,
        context: &'run EventContext,
        aggrs: &'run [InvokeAggrFn<'script>],
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        consts: &'run [Value<'event>],
    ) -> Result<Cont<'run, 'event>> {
        match self {
            Expr::Emit(expr) => match expr.borrow() {
                EmitExpr {
                    expr: ImutExpr::Path(Path::Event(EventPath { segments, .. })),
                    port,
                    ..
                } if segments.is_empty() => {
                    let port = if let Some(port) = port {
                        Some(
                            stry!(port
                                .eval_to_string(opts, context, aggrs, event, meta, local, consts))
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
                            stry!(port
                                .eval_to_string(opts, context, aggrs, event, meta, local, consts))
                            .to_string(),
                        )
                    } else {
                        None
                    };
                    Ok(Cont::Emit(
                        stry!(expr
                            .expr
                            .run(opts, context, aggrs, event, meta, local, consts))
                        .into_owned(),
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
                    let mut opt: Option<LocalValue> = None;
                    std::mem::swap(v, &mut opt);
                    if let Some(v) = opt {
                        v.v
                    } else {
                        return error_oops(self);
                    }
                } else {
                    return error_oops(self);
                };
                self.assign(
                    opts, context, aggrs, event, meta, local, consts, &path, value,
                )
                .map(Cont::Cont)
            }
            Expr::Assign { expr, path, .. } => {
                // NOTE Since we are assigning a new value we do cline here.
                // This is intended behaviour
                let value = demit!(expr.run(
                    opts.with_result(),
                    context,
                    aggrs,
                    event,
                    meta,
                    local,
                    consts
                ))
                .into_owned();
                self.assign(
                    opts, context, aggrs, event, meta, local, consts, &path, value,
                )
                .map(Cont::Cont)
            }
            Expr::Match(ref expr) => {
                self.match_expr(opts, context, aggrs, event, meta, local, consts, expr)
            }
            Expr::MergeInPlace(ref expr) => self
                .merge_in_place(opts, context, aggrs, event, meta, local, consts, expr)
                .map(Cont::Cont),
            Expr::PatchInPlace(ref expr) => self
                .patch_in_place(opts, context, aggrs, event, meta, local, consts, expr)
                .map(Cont::Cont),
            Expr::Comprehension(ref expr) => {
                self.comprehension(opts, context, aggrs, event, meta, local, consts, expr)
            }
            Expr::Imut(expr) => {
                // If we don't need the result of a imutable value then we
                // don't need to evalute it.
                if opts.result_needed {
                    expr.run(opts, context, aggrs, event, meta, local, consts)
                        .map(Cont::Cont)
                } else {
                    Ok(Cont::Cont(Cow::Borrowed(&NULL)))
                }
            }
        }
    }
}
