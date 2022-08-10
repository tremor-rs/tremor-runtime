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

use crate::ast::{BooleanBinExpr, BooleanBinOpKind};
use crate::static_bool;
use crate::{
    ast::{
        base_expr::Ranged, binary::extend_bytes_from_value, BaseExpr, BinExpr, Comprehension,
        ExprPath, ImutExpr, Invoke, InvokeAggr, Literal, LocalPath, Match, Merge, Patch, Path,
        Recur, ReservedPath, Segment, UnaryExpr,
    },
    errors::Kind as ErrorKind,
    errors::{
        error_bad_key, error_decreasing_range, error_invalid_unary, error_need_obj, error_need_str,
        error_no_clause_hit, error_oops, error_oops_err, Result,
    },
    interpreter::{
        exec_binary, exec_unary, merge_values, patch_value, resolve, set_local_shadow, test_guard,
        test_predicate_expr, value_to_index, AggrType, Env, ExecOpts, LocalStack, FALSE, TRUE,
    },
    lexer::Span,
    prelude::*,
    registry::{Registry, TremorAggrFnWrapper, RECUR_REF},
    stry, Object, Value,
};
use std::{
    borrow::{Borrow, Cow},
    iter, mem,
};

fn owned_val<'val, T>(v: T) -> Cow<'val, Value<'val>>
where
    T: 'val,
    Value<'val>: From<T>,
{
    Cow::Owned(Value::from(v))
}

type Bi<'v, 'r> = (usize, Box<dyn Iterator<Item = (Value<'v>, Value<'v>)> + 'r>);

impl<'script> ImutExpr<'script> {
    /// Checks if the expression is a literal expression
    #[inline]
    #[must_use]
    pub fn is_lit(&self) -> bool {
        matches!(self, ImutExpr::Literal(_))
    }

    /// Checks if the expression is a literal expression
    #[inline]
    #[must_use]
    pub fn into_lit(self) -> Option<Value<'script>> {
        if let ImutExpr::Literal(Literal { value, .. }) = self {
            Some(value)
        } else {
            None
        }
    }

    /// Checks if the expression is a literal expression
    #[inline]
    #[must_use]
    pub fn as_lit(&self) -> Option<&Value<'script>> {
        if let ImutExpr::Literal(Literal { value, .. }) = self {
            Some(value)
        } else {
            None
        }
    }
    /// Checks if the expression is a literal expression
    ///
    /// # Errors
    ///   * If this is not a literal
    #[inline]
    pub fn try_as_lit(&self) -> Result<&Value<'script>> {
        self.as_lit().ok_or_else(|| {
            ErrorKind::NotConstant(self.extent(), self.extent().expand_lines(2)).into()
        })
    }

    /// Evaluates the expression to a string.
    /// # Errors
    /// if the resulting value can not be represented as a str or the evaluation fails

    #[inline]
    pub fn eval_to_string<'event>(
        &self,
        opts: ExecOpts,
        env: &Env<'_, 'event>,
        event: &Value<'event>,
        state: &Value<'static>,
        meta: &Value<'event>,
        local: &LocalStack<'event>,
    ) -> Result<beef::Cow<'event, str>> {
        let value = stry!(self.run(opts, env, event, state, meta, local));
        value.as_str().map_or_else(
            || error_need_str(self, self, value.value_type()),
            |s| Ok(beef::Cow::from(s.to_owned())),
        )
    }

    /// Evaluates the expression to an index, i.e., a `usize`, or returns the appropriate error
    /// indicating why the resulting `Value` — if any — is not an index.
    ///
    /// # Note
    /// This method explicitly *does not* check whether the resulting index is in range of the array.
    ///
    /// # Errors
    /// if the resulting value can not be represented as a usize or the evaluation fails
    #[inline]
    pub fn eval_to_index<'event, Expr>(
        &self,
        outer: &Expr,
        opts: ExecOpts,
        env: &Env<'_, 'event>,
        event: &Value<'event>,
        state: &Value<'static>,
        meta: &Value<'event>,
        local: &LocalStack<'event>,
        path: &Path<'script>,
        array: &[Value],
    ) -> Result<usize>
    where
        Expr: BaseExpr,
    {
        let val = stry!(self.run(opts, env, event, state, meta, local));
        value_to_index(outer, self, val.borrow(), path, array)
    }

    #[allow(clippy::too_many_lines)]
    #[inline]
    /// Invokes expression
    ///
    /// # Errors
    /// on any runtime error
    pub fn run<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
    ) -> Result<Cow<'run, Value<'event>>>
    where
        'script: 'event,
    {
        match self {
            ImutExpr::String(s) => s.run(opts, env, event, state, meta, local).map(owned_val),
            ImutExpr::Recur(Recur { exprs, argc, .. }) => {
                // We need to pre calculate that to ensure that we don't overwrite
                // local variables that are not used
                let mut next = Vec::with_capacity(*argc);
                for (i, e) in exprs.iter().enumerate() {
                    let r = stry!(e.run(opts, env, event, state, meta, local));
                    if i < *argc {
                        next.push((i, r.into_owned()));
                    }
                }
                for (i, v) in next.drain(..) {
                    set_local_shadow(self, local, i, v)?;
                }

                Ok(Cow::Borrowed(RECUR_REF))
            }
            ImutExpr::Literal(literal) => Ok(Cow::Borrowed(&literal.value)),
            ImutExpr::Path(path) => resolve(self, opts, env, event, state, meta, local, path),
            ImutExpr::Present { path, .. } => {
                self.present(opts, env, event, state, meta, local, path)
            }
            ImutExpr::Record(ref record) => {
                let mut object: Object = record.base.clone();
                object.reserve(record.fields.len());

                for field in &record.fields {
                    let result = stry!(field.value.run(opts, env, event, state, meta, local));
                    let name = stry!(field.name.run(opts, env, event, state, meta, local));
                    object.insert(name, result.into_owned());
                }

                Ok(owned_val(object))
            }
            ImutExpr::Bytes(ref bytes) => {
                let mut bs: Vec<u8> = Vec::with_capacity(bytes.value.len());
                let mut used = 0;
                let mut buf = 0;
                for part in &bytes.value {
                    let value = stry!(part.data.run(opts, env, event, state, meta, local));
                    let tpe = part.data_type;
                    let end = part.endianess;
                    let ex = extend_bytes_from_value(
                        self, part, tpe, end, part.bits, &mut buf, &mut used, &mut bs, &value,
                    );
                    stry!(ex);
                }
                if used > 0 {
                    bs.push(buf >> (8 - used));
                }

                Ok(Cow::Owned(Value::Bytes(bs.into())))
            }

            ImutExpr::List(ref list) => {
                let mut r: Vec<Value<'event>> = Vec::with_capacity(list.exprs.len());
                for expr in &list.exprs {
                    r.push(stry!(expr.run(opts, env, event, state, meta, local)).into_owned());
                }
                Ok(owned_val(r))
            }
            ImutExpr::Invoke1(ref call) => self.invoke1(opts, env, event, state, meta, local, call),
            ImutExpr::Invoke2(ref call) => self.invoke2(opts, env, event, state, meta, local, call),
            ImutExpr::Invoke3(ref call) => self.invoke3(opts, env, event, state, meta, local, call),
            ImutExpr::Invoke(ref call) => self.invoke(opts, env, event, state, meta, local, call),
            ImutExpr::InvokeAggr(ref call) => self.emit_aggr(opts, env, call),
            ImutExpr::Patch(ref expr) => Self::patch(opts, env, event, state, meta, local, expr),
            ImutExpr::Merge(ref expr) => self.merge(opts, env, event, state, meta, local, expr),
            ImutExpr::Local { idx, mid, .. } => {
                if let Some(l) = stry!(local.get(*idx, self, mid)) {
                    Ok(Cow::Borrowed(l))
                } else {
                    let path: Path = Path::Local(LocalPath {
                        idx: *idx,
                        mid: mid.clone(),
                        segments: vec![],
                    });
                    let key = mid.name_dflt().to_string();
                    //TODO: get root key
                    error_bad_key(self, self, &path, key, vec![])
                }
            }

            ImutExpr::Unary(ref expr) => self.unary(opts, env, event, state, meta, local, expr),
            ImutExpr::Binary(ref expr) => self.binary(opts, env, event, state, meta, local, expr),
            ImutExpr::BinaryBoolean(ref expr) => {
                Self::binary_boolean(opts, env, event, state, meta, local, expr)
            }
            ImutExpr::Match(ref expr) => {
                self.match_expr(opts, env, event, state, meta, local, expr)
            }
            ImutExpr::Comprehension(ref expr) => {
                self.comprehension(opts, env, event, state, meta, local, expr)
            }
        }
    }

    fn comprehension<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'run Comprehension<'event, ImutExpr<'event>>,
    ) -> Result<Cow<'run, Value<'event>>>
    where
        'script: 'event,
    {
        fn kv<'v, K>((k, v): (K, &Value<'v>)) -> (Value<'v>, Value<'v>)
        where
            K: 'v + Clone,
            Value<'v>: From<K>,
        {
            (k.into(), v.clone())
        }

        let mut value_vec = vec![];
        let target = &expr.target;
        let t = stry!(target.run(opts, env, event, state, meta, local));

        let (l, items): Bi = t.as_object().map_or_else(
            || {
                t.as_array().map_or_else::<Bi, _, _>(
                    || (0, Box::new(iter::empty())),
                    |t| (t.len(), Box::new(t.iter().enumerate().map(kv))),
                )
            },
            |t| {
                (
                    t.len(),
                    Box::new(t.iter().map(|(k, v)| (k.clone().into(), v.clone()))),
                )
            },
        );

        if opts.result_needed {
            value_vec.reserve(l);
        }

        let cases = &expr.cases;

        'outer: for (k, v) in items {
            stry!(set_local_shadow(self, local, expr.key_id, k));
            stry!(set_local_shadow(self, local, expr.val_id, v));

            for e in cases {
                if stry!(test_guard(
                    self, opts, env, event, state, meta, local, &e.guard
                )) {
                    let l = &e.last_expr;
                    let v = stry!(Self::execute_effectors(
                        opts, env, event, state, meta, local, l
                    ));
                    value_vec.push(v.into_owned());
                    // NOTE: We are creating a new value so we have to clone;
                    continue 'outer;
                }
            }
        }
        Ok(owned_val(value_vec))
    }

    #[inline]
    fn execute_effectors<'run, 'event>(
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        effector: &'run ImutExpr<'event>,
    ) -> Result<Cow<'run, Value<'event>>>
    where
        'script: 'event,
    {
        effector.run(opts, env, event, state, meta, local)
    }

    #[allow(clippy::too_many_lines)]
    fn match_expr<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'run Match<'event, ImutExpr<'event>>,
    ) -> Result<Cow<'run, Value<'event>>>
    where
        'script: 'event,
    {
        use super::resolve_value;
        use crate::ast::{ClauseGroup, ClausePreCondition, DefaultCase};
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
                    let p = &$predicate.pattern;
                    let g = &$predicate.guard;
                    if stry!(test_predicate_expr(
                        expr, opts, env, event, state, meta, local, &target, p, g,
                    )) {
                        let l = &$predicate.last_expr;
                        return Self::execute_effectors(opts, env, event, state, meta, local, l);
                    }
                }};
            }
            match cg {
                ClauseGroup::Simple { patterns, .. } => {
                    for predicate in patterns {
                        execute!(predicate);
                    }
                }
                ClauseGroup::SearchTree { tree, rest, .. } => {
                    if let Some((_, l)) = tree.get(target) {
                        return Self::execute_effectors(opts, env, event, state, meta, local, l);
                    }
                    for predicate in rest {
                        execute!(predicate);
                    }
                }
                ClauseGroup::Combined { groups, .. } => {
                    // we have to inline this
                    for cg in groups {
                        match cg {
                            ClauseGroup::Simple { patterns, .. } => {
                                for predicate in patterns {
                                    execute!(predicate);
                                }
                            }
                            ClauseGroup::SearchTree { tree, rest, .. } => {
                                if let Some((_, l)) = tree.get(target) {
                                    return Self::execute_effectors(
                                        opts, env, event, state, meta, local, l,
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
                            ClauseGroup::Single { pattern, .. } => {
                                execute!(pattern);
                            }
                        }
                    }
                }
                ClauseGroup::Single { pattern, .. } => {
                    execute!(pattern);
                }
            };
        }

        match &expr.default {
            DefaultCase::None => error_no_clause_hit(self),
            DefaultCase::Null => Ok(Cow::Borrowed(&NULL)),
            DefaultCase::Many { last_expr, .. } => {
                Self::execute_effectors(opts, env, event, state, meta, local, last_expr)
            }
            DefaultCase::One(last_expr) => {
                Self::execute_effectors(opts, env, event, state, meta, local, last_expr)
            }
        }
    }

    fn binary<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'run BinExpr<'event>,
    ) -> Result<Cow<'run, Value<'event>>>
    where
        'script: 'event,
    {
        let lhs = stry!(expr.lhs.run(opts, env, event, state, meta, local));
        let rhs = stry!(expr.rhs.run(opts, env, event, state, meta, local));
        exec_binary(self, expr, expr.kind, &lhs, &rhs)
    }

    fn binary_boolean<'run, 'event>(
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'run BooleanBinExpr<'event>,
    ) -> Result<Cow<'run, Value<'event>>>
    where
        'script: 'event,
    {
        let lval = stry!(expr.lhs.run(opts, env, event, state, meta, local)).try_as_bool()?;

        match expr.kind {
            BooleanBinOpKind::Or if lval => Ok(static_bool!(true)),
            BooleanBinOpKind::Or => {
                let rval =
                    stry!(expr.rhs.run(opts, env, event, state, meta, local)).try_as_bool()?;

                Ok(static_bool!(lval || rval))
            }
            BooleanBinOpKind::And if !lval => Ok(static_bool!(false)),
            BooleanBinOpKind::And => {
                let rval =
                    stry!(expr.rhs.run(opts, env, event, state, meta, local)).try_as_bool()?;

                Ok(static_bool!(lval && rval))
            }
            BooleanBinOpKind::Xor => {
                let rval =
                    stry!(expr.rhs.run(opts, env, event, state, meta, local)).try_as_bool()?;

                Ok(static_bool!(lval ^ rval))
            }
        }
    }

    fn unary<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'run UnaryExpr<'event>,
    ) -> Result<Cow<'run, Value<'event>>>
    where
        'script: 'event,
    {
        let rhs = stry!(expr.expr.run(opts, env, event, state, meta, local));
        // TODO align this implemenation to be similar to exec_binary?
        match exec_unary(expr.kind, &rhs) {
            Some(v) => Ok(v),
            None => error_invalid_unary(self, &expr.expr, expr.kind, &rhs),
        }
    }

    // TODO: Quite some overlap with `interpreter::resolve` (and some with `expr::assign`)
    #[allow(clippy::too_many_lines)]
    fn present<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        path: &'run Path<'event>,
    ) -> Result<Cow<'run, Value<'event>>>
    where
        'script: 'event,
    {
        // Fetch the base of the path
        // TODO: Extract this into a method on `Path`?
        let base_value: &Value = match path {
            Path::Local(path) => {
                if let Some(l) = stry!(local.get(path.idx, self, &path.mid)) {
                    l
                } else {
                    return Ok(Cow::Borrowed(&FALSE));
                }
            }
            Path::Expr(ExprPath { expr, var, .. }) => {
                match expr.run(opts, env, event, state, meta, local)? {
                    Cow::Borrowed(p) => p,
                    Cow::Owned(o) => set_local_shadow(self, local, *var, o)?,
                }
            }
            Path::Meta(_path) => meta,
            Path::Event(_path) => event,
            Path::State(_path) => state,
            Path::Reserved(ReservedPath::Args { .. }) => env.consts.args,
            Path::Reserved(ReservedPath::Group { .. }) => env.consts.group,
            Path::Reserved(ReservedPath::Window { .. }) => env.consts.window,
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
                    }
                    // No field for that id: not present
                    return Ok(Cow::Borrowed(&FALSE));
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
                        // No element at the index: not present
                        return Ok(Cow::Borrowed(&FALSE));
                    }
                    return Ok(Cow::Borrowed(&FALSE));
                }
                // Next segment is an index range: index into `current`, if it's an array
                Segment::RangeExpr { start, end, .. } => {
                    if let Some(a) = current.as_array() {
                        let array = subrange.unwrap_or(a.as_slice());
                        let start_idx = stry!(start.eval_to_index(
                            self, opts, env, event, state, meta, local, path, array,
                        ));
                        let end_idx = stry!(end.eval_to_index(
                            self, opts, env, event, state, meta, local, path, array,
                        ));

                        if end_idx < start_idx {
                            return error_decreasing_range(self, segment, path, start_idx, end_idx);
                        } else if end_idx > array.len() {
                            // Index is out of array bounds: not present
                            return Ok(Cow::Borrowed(&FALSE));
                        }
                        subrange = array.get(start_idx..end_idx);
                        continue;
                    }
                    return Ok(Cow::Borrowed(&FALSE));
                }
                // Next segment is an index range: index into `current`, if it's an array
                Segment::Range { start, end, .. } => {
                    if let Some(a) = current.as_array() {
                        let array = subrange.unwrap_or(a.as_slice());
                        let start_idx = *start;
                        let end_idx = *end;

                        if end_idx > array.len() {
                            // Index is out of array bounds: not present
                            return Ok(Cow::Borrowed(&FALSE));
                        }
                        subrange = array.get(start_idx..end_idx);
                        continue;
                    }
                    return Ok(Cow::Borrowed(&FALSE));
                }
                // Next segment is an expression: run `expr` to know which key it signifies at runtime
                Segment::Element { expr, .. } => {
                    let key = stry!(expr.run(opts, env, event, state, meta, local));

                    let next = match (current, key.borrow()) {
                        // The segment resolved to an identifier, and `current` is an object: lookup
                        (Value::Object(o), Value::String(id)) => o.get(id),
                        // If `current` is an array, the segment has to be an index
                        (Value::Array(a), idx) => {
                            let array = subrange.unwrap_or(a.as_slice());
                            let idx = stry!(value_to_index(self, segment, idx, path, array));
                            array.get(idx)
                        }
                        // Anything else: not present
                        _other => None,
                    };
                    if let Some(next) = next {
                        current = next;
                        subrange = None;
                        continue;
                    }
                    return Ok(Cow::Borrowed(&FALSE));
                }
            }
        }

        Ok(Cow::Borrowed(&TRUE))
    }

    // TODO: Can we convince Rust to generate the 3 or 4 versions of this method from one template?
    fn invoke1<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'run Invoke<'event>,
    ) -> Result<Cow<'run, Value<'event>>>
    where
        'script: 'event,
    {
        let arg = unsafe { expr.args.get_unchecked(0) };
        let v = stry!(eval_for_fn_arg(opts, env, event, state, meta, local, arg));
        expr.invocable
            .invoke(env, &[v.borrow()])
            .map(Cow::Owned)
            .map_err(|e| {
                let r: Option<&Registry> = None;
                let outer: Span = self.extent().expand_lines(2);
                e.into_err(&outer, self, r)
            })
    }

    fn invoke2<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'run Invoke<'event>,
    ) -> Result<Cow<'run, Value<'event>>>
    where
        'script: 'event,
    {
        let arg = unsafe { (expr.args.get_unchecked(0), expr.args.get_unchecked(1)) };
        let v1 = stry!(eval_for_fn_arg(opts, env, event, state, meta, local, arg.0));
        let v2 = stry!(eval_for_fn_arg(opts, env, event, state, meta, local, arg.1));
        expr.invocable
            .invoke(env, &[v1.borrow(), v2.borrow()])
            .map(Cow::Owned)
            .map_err(|e| {
                let r: Option<&Registry> = None;
                let outer: Span = self.extent().expand_lines(2);
                e.into_err(&outer, self, r)
            })
    }

    fn invoke3<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'run Invoke<'event>,
    ) -> Result<Cow<'run, Value<'event>>>
    where
        'script: 'event,
    {
        let arg = unsafe {
            (
                expr.args.get_unchecked(0),
                expr.args.get_unchecked(1),
                expr.args.get_unchecked(2),
            )
        };

        let v1 = stry!(eval_for_fn_arg(opts, env, event, state, meta, local, arg.0));
        let v2 = stry!(eval_for_fn_arg(opts, env, event, state, meta, local, arg.1));
        let v3 = stry!(eval_for_fn_arg(opts, env, event, state, meta, local, arg.2));
        expr.invocable
            .invoke(env, &[v1.borrow(), v2.borrow(), v3.borrow()])
            .map(Cow::Owned)
            .map_err(|e| {
                let r: Option<&Registry> = None;
                let outer: Span = self.extent().expand_lines(2);
                e.into_err(&outer, self, r)
            })
    }

    fn invoke<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'run Invoke<'event>,
    ) -> Result<Cow<'run, Value<'event>>>
    where
        'script: 'event,
    {
        let argv: Vec<Cow<'run, _>> = stry!(expr
            .args
            .iter()
            .map(|arg| eval_for_fn_arg(opts, env, event, state, meta, local, arg))
            .collect::<Result<_>>());

        // Construct a view into `argv`, since `invoke` expects a slice of references, not Cows.
        let argv1: Vec<&Value> = argv.iter().map(Cow::borrow).collect();

        expr.invocable
            .invoke(env, &argv1)
            .map(Cow::Owned)
            .map_err(|e| {
                let r: Option<&Registry> = None;
                let outer: Span = self.extent().expand_lines(2);
                e.into_err(&outer, self, r)
            })
    }

    fn emit_aggr<'run, 'event>(
        &'run self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        expr: &'run InvokeAggr,
    ) -> Result<Cow<'run, Value<'event>>> {
        if opts.aggr != AggrType::Emit {
            return error_oops(
                self,
                0xdead_0011,
                "Trying to emit aggreagate outside of emit context",
            );
        }

        let inv = stry!(env
            .aggrs
            .get(expr.aggr_id)
            .map(|a| &a.invocable)
            .ok_or_else(|| {
                error_oops_err(
                    self,
                    0xdead_0012,
                    &format!("Unknown aggregate function {}", expr.aggr_id),
                )
            }));
        // ALLOW: https://github.com/tremor-rs/tremor-runtime/issues/1035
        #[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr)]
        // ALLOW: https://github.com/tremor-rs/tremor-runtime/issues/1035
        let invocable: &mut TremorAggrFnWrapper = unsafe { mem::transmute(inv) };
        let r = stry!(invocable.emit().map(Cow::Owned).map_err(|e| {
            let r: Option<&Registry> = None;
            e.into_err(self, self, r)
        }));
        Ok(r)
    }

    fn patch<'run, 'event>(
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'run Patch<'event>,
    ) -> Result<Cow<'run, Value<'event>>>
    where
        'script: 'event,
    {
        // NOTE: We clone this since we patch it - this should be not mutated but cloned

        let mut value = stry!(expr.target.run(opts, env, event, state, meta, local)).into_owned();
        stry!(patch_value(
            opts, env, event, state, meta, local, &mut value, expr,
        ));
        Ok(Cow::Owned(value))
    }

    fn merge<'run, 'event>(
        &self,
        opts: ExecOpts,
        env: &'run Env<'run, 'event>,
        event: &'run Value<'event>,
        state: &'run Value<'static>,
        meta: &'run Value<'event>,
        local: &'run LocalStack<'event>,
        expr: &'run Merge<'event>,
    ) -> Result<Cow<'run, Value<'event>>>
    where
        'script: 'event,
    {
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
                error_need_obj(self, &expr.expr, replacement.value_type())
            }
        } else {
            error_need_obj(self, &expr.target, value.value_type())
        }
    }
}

/// We need to handles arguments that are directly derived from `args` in a
/// special form since args gets cleared and overwritten inside of functions.
#[inline]
fn eval_for_fn_arg<'run, 'event>(
    opts: ExecOpts,
    env: &'run Env<'run, 'event>,
    event: &'run Value<'event>,
    state: &'run Value<'static>,
    meta: &'run Value<'event>,
    local: &'run LocalStack<'event>,
    arg: &'run ImutExpr<'event>,
) -> Result<Cow<'run, Value<'event>>>
where
    'event: 'run,
{
    match arg {
        ImutExpr::Path(Path::Reserved(ReservedPath::Args { .. })) => arg
            .run(opts, env, event, state, meta, local)
            .map(|v| Cow::Owned(v.clone_static())),
        _ => arg.run(opts, env, event, state, meta, local),
    }
}
