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
    resolve, resolve_value, set_local_shadow, test_guard, test_predicate_expr, Env, LocalStack,
};
use crate::stry;
use crate::{
    ast::BinOpKind,
    errors::{
        err_invalid_fold, err_need_obj, error_assign_array, error_assign_to_const,
        error_bad_key_err, error_invalid_assign_target, error_invalid_bool_op, error_no_clause_hit,
        AddSpan, Result,
    },
};
use crate::{
    ast::{BooleanBinOpKind, ComprehensionFoldOp},
    prelude::*,
};
use crate::{
    ast::{
        ClauseGroup, ClausePreCondition, Comprehension, DefaultCase, EmitExpr, EventPath, Expr,
        IfElse, ImutExpr, Match, Path, Segment,
    },
    errors::error_oops_err,
};
use crate::{interpreter::exec_binary_numeric, registry::RECUR_PTR};
use std::mem;
use std::{
    borrow::{Borrow, Cow},
    iter,
};
use tremor_common::ports::Port;

pub(crate) type ComprehensionIter<'event, 'run> =
    Box<dyn Iterator<Item = (Value<'event>, Value<'event>)> + 'run>;
pub(crate) type ComprehensionItem<'event, 'run> = (usize, ComprehensionIter<'event, 'run>);

#[derive(Debug)]
/// Continuation context to control program flow
// TODO, can we avoid the static here?
pub enum Cont<'run, 'event>
where
    'event: 'run,
{
    /// Continue
    Cont(Cow<'run, Value<'event>>),
    /// Emit with default system supplied port
    Emit(Value<'event>, Option<Port<'static>>),
    /// Drop
    Drop,
    /// Emit with user supplied port
    EmitEvent(Option<Port<'static>>),
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

impl<'script> Expr<'script> {
    #[inline]
    pub(crate) fn execute_effectors<'run, 'event>(
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        effectors: &'run [Expr<'event>],
        last_effector: &'run Expr<'event>,
    ) -> Result<Cont<'run, 'event>> {
        for effector in effectors {
            demit!(effector.run(opts.without_result(), env, event, state, meta, local));
        }
        Ok(Cont::Cont(demit!(
            last_effector.run(opts, env, event, state, meta, local)
        )))
    }

    #[inline]
    #[allow(clippy::too_many_lines)]
    fn match_expr<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        expr: &'run Match<Expr<'event>>,
    ) -> Result<Cont<'run, 'event>> {
        // use super::DUMMY_PATH as D;
        let target = stry!(expr.target.run(opts, env, event, state, meta, local));
        for cg in &expr.patterns {
            let maybe_target = if let Some(ClausePreCondition { path, .. }) = cg.precondition() {
                let v = resolve_value(self, opts, env, event, state, meta, local, path, &target);
                if let Ok(target) = v {
                    Some(target)
                } else {
                    // We couldn't look up the value, it doesn't exist so we look at the next group
                    continue;
                }
            } else {
                None
            };
            let target: &Value = maybe_target.as_ref().map_or(&target, |target| target);
            macro_rules! execute {
                ($predicate:ident) => {{
                    let pat = &$predicate.pattern;
                    let grd = &$predicate.guard;
                    if stry!(test_predicate_expr(
                        expr, opts, env, event, state, meta, local, &target, pat, grd,
                    )) {
                        let expr = &$predicate.exprs;
                        let last = &$predicate.last_expr;
                        return Expr::execute_effectors(
                            opts, env, event, state, meta, local, expr, last,
                        );
                    }
                }};
            }
            match cg {
                ClauseGroup::Single { pattern, .. } => {
                    execute!(pattern);
                }
                ClauseGroup::Simple { patterns, .. } => {
                    for predicate in patterns {
                        execute!(predicate);
                    }
                }
                ClauseGroup::SearchTree { tree, rest, .. } => {
                    if let Some((e, l)) = tree.get(target) {
                        return Expr::execute_effectors(opts, env, event, state, meta, local, e, l);
                    }
                    for predicate in rest {
                        execute!(predicate);
                    }
                }
                ClauseGroup::Combined { groups, .. } => {
                    // we have to inline this
                    for cg in groups {
                        match cg {
                            ClauseGroup::Single { pattern, .. } => {
                                execute!(pattern);
                            }
                            ClauseGroup::Simple { patterns, .. } => {
                                for predicate in patterns {
                                    execute!(predicate);
                                }
                            }
                            ClauseGroup::SearchTree { tree, rest, .. } => {
                                if let Some((e, l)) = tree.get(target) {
                                    return Expr::execute_effectors(
                                        opts, env, event, state, meta, local, e, l,
                                    );
                                }
                                for predicate in rest {
                                    execute!(predicate);
                                }
                            }
                            ClauseGroup::Combined { .. } => {
                                return Err(
                                    "Nested combined clause groups are not permitted!".into()
                                )
                            }
                        }
                    }
                }
            };
        }
        match &expr.default {
            DefaultCase::None => error_no_clause_hit(self),
            DefaultCase::Null => Ok(Cont::Cont(Cow::Borrowed(&NULL))),
            DefaultCase::Many { exprs, last_expr } => {
                Expr::execute_effectors(opts, env, event, state, meta, local, exprs, last_expr)
            }
            DefaultCase::One(last_expr) => {
                Expr::execute_effectors(opts, env, event, state, meta, local, &[], last_expr)
            }
        }
    }

    #[inline]
    fn if_expr<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        expr: &'run IfElse<'event, Expr<'event>>,
    ) -> Result<Cont<'run, 'event>> {
        let target = stry!(expr.target.run(opts, env, event, state, meta, local));
        let p = &expr.if_clause.pattern;
        let g = &expr.if_clause.guard;
        let p = test_predicate_expr(expr, opts, env, event, state, meta, local, &target, p, g);
        if stry!(p) {
            let e = &expr.if_clause.exprs;
            let l = &expr.if_clause.last_expr;
            Expr::execute_effectors(opts, env, event, state, meta, local, e, l)
        } else {
            match &expr.else_clause {
                DefaultCase::None => error_no_clause_hit(self),
                DefaultCase::Null => Ok(Cont::Cont(Cow::Borrowed(&NULL))),
                DefaultCase::Many { exprs, last_expr } => {
                    Expr::execute_effectors(opts, env, event, state, meta, local, exprs, last_expr)
                }
                DefaultCase::One(last_expr) => {
                    Expr::execute_effectors(opts, env, event, state, meta, local, &[], last_expr)
                }
            }
        }
    }

    fn comprehension_array<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        expr: &'run Comprehension<'event, Expr>,
        items: ComprehensionIter<'event, 'run>,
        mut result: Vec<Value<'event>>,
    ) -> Result<Cont<'run, 'event>> {
        let cases = &expr.cases;

        'outer: for (k, v) in items {
            stry!(set_local_shadow(self, local, expr.key_id, k));
            stry!(set_local_shadow(self, local, expr.val_id, v));

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
                        let v = v.into_owned();
                        result.push(v);
                    }
                    continue 'outer;
                }
            }
        }

        Ok(Cont::Cont(Cow::Owned(Value::Array(result))))
    }
    fn comprehension_record<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        expr: &'run Comprehension<'event, Expr>,
        items: ComprehensionIter<'event, 'run>,
        mut result: Object<'event>,
    ) -> Result<Cont<'run, 'event>> {
        let cases = &expr.cases;
        let no_res = opts.without_result();

        'outer: for (k, v) in items {
            stry!(set_local_shadow(self, local, expr.key_id, k));
            stry!(set_local_shadow(self, local, expr.val_id, v));

            for e in cases {
                if stry!(test_guard(
                    self, opts, env, event, state, meta, local, &e.guard
                )) {
                    let l = &e.last_expr;
                    if let Expr::Imut(ImutExpr::Record(r)) = l {
                        for effector in &e.exprs {
                            demit!(effector.run(no_res, env, event, state, meta, local));
                        }

                        if opts.result_needed {
                            for (k, v) in &r.base {
                                result.insert(k.clone(), v.clone());
                            }
                            for f in &r.fields {
                                let k = stry!(f.name.run(opts, env, event, state, meta, local));
                                let v = stry!(f.value.run(opts, env, event, state, meta, local));
                                result.insert(k.clone(), v.into_owned());
                            }
                        } else {
                            for f in &r.fields {
                                if opts.result_needed {
                                    let k = stry!(f.name.run(opts, env, event, state, meta, local));
                                    let v =
                                        stry!(f.value.run(opts, env, event, state, meta, local));

                                    result.insert(k.clone(), v.into_owned());
                                } else {
                                    stry!(f.value.run(opts, env, event, state, meta, local));
                                }
                            }
                        }
                    } else {
                        let v = demit!(Self::execute_effectors(
                            opts, env, event, state, meta, local, &e.exprs, l,
                        ));
                        // NOTE: We are creating a new value so we have to clone;
                        if opts.result_needed {
                            let v = v.into_owned();

                            for (k, v) in v.try_into_object().add_span(expr, l)? {
                                result.insert(k, v);
                            }
                        }
                    }
                    continue 'outer;
                }
            }
        }

        Ok(Cont::Cont(Cow::Owned(Value::Object(Box::new(result)))))
    }
    // TODO: Quite some overlap with `ImutExprInt::comprehension`
    #[allow(clippy::too_many_lines)]
    fn comprehension<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        expr: &'run Comprehension<'event, Expr>,
    ) -> Result<Cont<'run, 'event>> {
        fn kv<'k, K>((k, v): (K, Value)) -> (Value<'k>, Value)
        where
            K: 'k,
            Value<'k>: From<K>,
        {
            (k.into(), v)
        }

        let target = &expr.target;
        let t = stry!(target.run(opts, env, event, state, meta, local,));

        let (l, items): ComprehensionItem = t.as_object().map_or_else(
            || {
                t.as_array().map_or_else::<ComprehensionItem, _, _>(
                    || (0, Box::new(iter::empty())),
                    |t| (t.len(), Box::new(t.clone().into_iter().enumerate().map(kv))),
                )
            },
            |t| (t.len(), Box::new(t.clone().into_iter().map(kv))),
        );
        let mut result = if opts.result_needed {
            let mut r = stry!(expr.initial.run(opts, env, event, state, meta, local)).into_owned();
            if let Some(v) = r.as_array_mut() {
                v.reserve(l);
            } else if let Some(v) = r.as_object_mut() {
                v.reserve(l);
            }
            r
        } else {
            Value::const_null()
        };
        let fold = expr.fold.unwrap_or_default();
        // short circuite for common cases
        if fold == ComprehensionFoldOp::Arith(BinOpKind::Add) {
            if result.is_array() {
                let a = result.into_array().unwrap_or_default();
                return self
                    .comprehension_array(opts, env, event, state, meta, local, expr, items, a);
            } else if result.is_object() {
                let o = result.into_object().unwrap_or_default();
                return self
                    .comprehension_record(opts, env, event, state, meta, local, expr, items, o);
            }
        }
        let cases = &expr.cases;

        'outer: for (k, v) in items {
            stry!(set_local_shadow(self, local, expr.key_id, k));
            stry!(set_local_shadow(self, local, expr.val_id, v));

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
                        let v = v.into_owned();
                        if let Some(lhs) = result.as_array_mut() {
                            match fold {
                                ComprehensionFoldOp::Arith(BinOpKind::Add) => {
                                    lhs.push(v);
                                }
                                op => {
                                    return err_invalid_fold(expr, l, op);
                                }
                            }
                        } else if let Some(lhs) = result.as_object_mut() {
                            match fold {
                                ComprehensionFoldOp::Arith(BinOpKind::Add) => {
                                    for (k, v) in v.try_into_object().add_span(expr, l)? {
                                        lhs.insert(k, v);
                                    }
                                }
                                op => {
                                    return err_invalid_fold(expr, l, op);
                                }
                            }
                        } else {
                            match fold {
                                ComprehensionFoldOp::Arith(op) => {
                                    result = stry!(exec_binary_numeric(self, e, op, &result, &v))
                                        .into_owned();
                                }
                                ComprehensionFoldOp::Logic(op) => {
                                    let lval = result.try_as_bool().map_err(|e| {
                                        error_invalid_bool_op(expr, l, op, e.got, None)
                                    })?;
                                    let rval = v.try_as_bool().map_err(|e| {
                                        error_invalid_bool_op(expr, l, op, e.got, None)
                                    })?;

                                    match op {
                                        BooleanBinOpKind::Or => {
                                            result = (lval || rval).into();
                                            if result.as_bool() == Some(true) {
                                                return Ok(Cont::Cont(Cow::Owned(result)));
                                            }
                                        }
                                        BooleanBinOpKind::And => {
                                            result = (lval && rval).into();
                                            if result.as_bool() == Some(false) {
                                                return Ok(Cont::Cont(Cow::Owned(result)));
                                            }
                                        }
                                        BooleanBinOpKind::Xor => result = (lval ^ rval).into(),
                                    }
                                }
                            }
                        }
                    }
                    continue 'outer;
                }
            }
        }

        Ok(Cont::Cont(Cow::Owned(result)))
    }

    #[inline]
    fn assign<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        path: &'run Path<'event>,
        value: Value<'event>,
    ) -> Result<Cow<'run, Value<'event>>> {
        if path.segments().is_empty() {
            self.assign_direct(opts, event, state, meta, local, path, value)
        } else {
            self.assign_nested(opts, env, event, state, meta, local, path, value)
        }
    }

    // ALLOW: https://github.com/tremor-rs/tremor-runtime/issues/1033
    #[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr)]
    fn assign_nested<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        path: &'run Path<'event>,
        mut value: Value<'event>,
    ) -> Result<Cow<'run, Value<'event>>> {
        /* NOTE
         * This function is icky we got to do some trickery here.
         * Since it's dangerous and icky it deserves some explanation
         * What we do here is we borrow the target we want to set
         * as immutable and turn it to mutable where needed.
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
        let segments: &'run [Segment<'event>] = path.segments();

        let mut current: &Value = match path {
            Path::Reserved(_) | Path::Expr(_) => {
                return error_assign_to_const(self);
            }

            Path::Local(lpath) => {
                stry!(local.get(lpath.idx, self, lpath.meta()).and_then(|o| {
                    o.as_ref().ok_or_else(|| {
                        let key = lpath.mid.name_dflt().to_string();
                        error_bad_key_err(self, lpath, path, key, vec![])
                    })
                }))
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
                    current = stry!(key
                        .lookup_or_insert_mut(
                            // ALLOW: https://github.com/tremor-rs/tremor-runtime/issues/1033
                            unsafe { mem::transmute::<&Value, &mut Value>(current) },
                            || Value::object_with_capacity(tremor_value::VEC_LIMIT_UPPER),
                        )
                        .map_err(|_| err_need_obj(self, segment, current.value_type())));
                }
                Segment::Element { expr, .. } => {
                    let id = stry!(expr.eval_to_string(opts, env, event, state, meta, local));
                    // ALLOW: https://github.com/tremor-rs/tremor-runtime/issues/1033
                    let v: &mut Value<'event> = unsafe { mem::transmute(current) };
                    let map = stry!(v.as_object_mut().ok_or_else(|| err_need_obj(
                        self,
                        segment,
                        current.value_type(),
                    )));

                    current = match map.get_mut(&id) {
                        Some(v) => v,
                        None => map
                            .entry(id)
                            .or_insert_with(|| Value::object_with_capacity(32)),
                    };
                }
                Segment::Idx { .. } | Segment::Range { .. } | Segment::RangeExpr { .. } => {
                    return error_assign_array(self, segment)
                }
            }
        }
        unsafe {
            // ALLOW: https://github.com/tremor-rs/tremor-runtime/issues/1033
            *mem::transmute::<&Value<'event>, &mut Value<'event>>(current) = value;
        }
        if opts.result_needed {
            resolve(self, opts, env, event, state, meta, local, path)
        } else {
            Ok(Cow::Borrowed(&NULL))
        }
    }

    fn assign_direct<'run, 'event>(
        &'run self,
        _opts: ExecOpts,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        _meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
        path: &'run Path<'event>,
        value: Value<'event>,
    ) -> Result<Cow<'run, Value<'event>>> {
        match path {
            Path::Reserved(_) | Path::Expr(_) => error_assign_to_const(self),
            Path::Local(lpath) => {
                let o = stry!(local.get_mut(lpath.idx, self, lpath.meta()));
                Ok(Cow::Borrowed(o.insert(value)))
            }
            Path::Meta(_path) => error_invalid_assign_target(self),
            Path::Event(_path) => {
                *event = value;
                Ok(Cow::Borrowed(event))
            }
            Path::State(_path) => {
                // Extend the lifetime of value to be static (also forces all strings and
                // object keys in value to be owned COW's). This ensures that the current
                // value is kept as part of state across subsequent state assignments (if
                // users choose to do so).
                *state = value.into_static();
                Ok(Cow::Borrowed(state))
            }
        }
    }

    #[inline]
    /// Invokes expression
    ///
    /// # Errors
    /// if evaluation fails
    pub fn run<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
        local: &'run mut LocalStack<'event>,
    ) -> Result<Cont<'run, 'event>>
    where
        'script: 'event,
    {
        match self {
            Expr::Emit(expr) => match expr.borrow() {
                EmitExpr {
                    expr: ImutExpr::Path(Path::Event(EventPath { segments, .. })),
                    port,
                    ..
                } if segments.is_empty() => port
                    .as_ref()
                    .map(|port| {
                        port.eval_to_string(opts, env, event, state, meta, local)
                            .map(|s| Port::from(s.to_string()))
                    })
                    .transpose()
                    .map(Cont::EmitEvent),
                expr => expr
                    .port
                    .as_ref()
                    .map(|port| {
                        port.eval_to_string(opts, env, event, state, meta, local)
                            .map(|p| Port::from(p.to_string()))
                    })
                    .transpose()
                    .and_then(|port| {
                        expr.expr
                            .run(opts, env, event, state, meta, local)
                            .map(|v| Cont::Emit(v.into_owned(), port))
                    }),
            },
            Expr::Drop { .. } => Ok(Cont::Drop),
            Expr::AssignMoveLocal { idx, path, .. } => {
                // This is a special case when we know that we'll not use
                // this local variable again, it allows os to
                // move the variable instead of cloning it

                let value = stry!(local
                    .values
                    .get_mut(*idx)
                    .ok_or_else(|| error_oops_err(
                        self,
                        0xdead_000b,
                        "Unknown local variable in Expr::AssignMoveLocal",
                    ))
                    .and_then(|v| {
                        let mut opt: Option<Value> = None;
                        std::mem::swap(v, &mut opt);
                        opt.ok_or_else(|| {
                            error_oops_err(
                                self,
                                0xdead_000c,
                                "Unknown local variable in Expr::AssignMoveLocal",
                            )
                        })
                    }));
                self.assign(opts, env, event, state, meta, local, path, value)
                    .map(Cont::Cont)
            }
            Expr::Assign { expr, path, .. } => {
                // NOTE Since we are assigning a new value we do cline here.
                // This is intended behaviour
                let value = demit!(expr.run(opts.with_result(), env, event, state, meta, local))
                    .into_owned();
                self.assign(opts, env, event, state, meta, local, path, value)
                    .map(Cont::Cont)
            }
            Expr::Match(ref expr) => self.match_expr(opts, env, event, state, meta, local, expr),
            Expr::IfElse(ref expr) => self.if_expr(opts, env, event, state, meta, local, expr),
            Expr::Comprehension(ref expr) => {
                self.comprehension(opts, env, event, state, meta, local, expr)
            }
            Expr::Imut(expr) => {
                // If we don't need the result of a immutable value then we
                // don't need to evaluate it.
                let r = if opts.result_needed {
                    stry!(expr.run(opts, env, event, state, meta, local))
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
