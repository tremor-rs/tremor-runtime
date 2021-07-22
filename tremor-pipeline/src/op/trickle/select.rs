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

// [x] PERF0001: handle select without grouping or windows easier.

#[cfg(test)]
mod test;

use std::mem;

use super::window::{self, Group, Window};
use crate::op::prelude::trickle::window::{GroupWindow, SelectCtx, Trait};
use crate::{errors::Result, SignalKind};
use crate::{op::prelude::*, EventIdGenerator};
use crate::{Event, EventId, Operator};
use halfbrown::Entry;
use tremor_common::stry;
use tremor_script::ast;
use tremor_script::{
    self,
    ast::{InvokeAggrFn, Select, SelectStmt},
    errors::Result as TSResult,
    interpreter::Env,
    prelude::*,
    srs,
    utils::sorted_serialize,
    Value,
};

#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct TrickleSelect {
    pub id: String,
    pub(crate) select: srs::Select,
    pub windows: Vec<Window>,
    pub groups: HashMap<String, Group>,
    pub event_id_gen: EventIdGenerator,
    recursion_limit: u32,
    dflt_group: Group,
}

pub(crate) const NO_AGGRS: [InvokeAggrFn<'static>; 0] = [];

impl TrickleSelect {
    pub fn with_stmt(
        operator_uid: u64,
        id: String,
        windows: Vec<(String, window::Impl)>,
        stmt: &srs::Stmt,
    ) -> Result<Self> {
        let windows: Vec<_> = windows
            .into_iter()
            .map(|(fqwn, window_impl)| Window {
                module: Window::module_path(&fqwn),
                name: Window::ident_name(&fqwn).to_string(),
                window_impl,
            })
            .collect();
        let select = srs::Select::try_new_from_stmt(stmt)?;
        let event_id_gen = EventIdGenerator::new(operator_uid);
        if let ast::Stmt::Select(SelectStmt { aggregates, .. }) = stmt.suffix() {
            let windows_itr = windows.iter();
            let dflt_group = Group {
                value: Value::const_null(),
                aggrs: aggregates.clone(),
                windows: GroupWindow::from_windows(aggregates, &EventId::default(), windows_itr),
            };
            Ok(Self {
                id,
                windows,
                select,
                groups: HashMap::new(),
                event_id_gen,
                recursion_limit: tremor_script::recursion_limit(),
                dflt_group,
            })
        } else {
            Err("Wrong type of statement".into())
        }
    }
    fn opts() -> ExecOpts {
        ExecOpts {
            result_needed: true,
            aggr: AggrType::Emit,
        }
    }
}

/// execute the select clause of the statement and filter results by having clause, if provided
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_select_and_having(
    ctx: &SelectCtx,
    env: &Env,
    data: &ValueAndMeta,
) -> TSResult<Option<(Cow<'static, str>, Event)>> {
    let (event_payload, event_meta) = data.parts();

    let value = ctx.select.target.run(
        ctx.opts,
        env,
        event_payload,
        ctx.state,
        event_meta,
        ctx.local_stack,
    )?;

    // check having clause
    let result = value.into_owned();
    if let Some(guard) = &ctx.select.maybe_having {
        let test = guard.run(ctx.opts, env, &result, ctx.state, &NULL, ctx.local_stack)?;
        if let Some(test) = test.as_bool() {
            if !test {
                return Ok(None);
            }
        } else {
            return Err(tremor_script::errors::query_guard_not_bool_err(
                ctx.select,
                guard,
                &test,
                ctx.node_meta,
            ));
        }
    }
    Ok(Some((
        OUT,
        Event {
            id: ctx.event_id.clone(),
            ingest_ns: ctx.ingest_ns,
            origin_uri: ctx.origin_uri.clone(),
            // TODO: this will ignore op_metas from all other events this one is based upon and might break operators requiring this
            op_meta: ctx.op_meta.clone(),
            is_batch: false,
            data: (result.into_static(), event_meta.clone_static()).into(),
            transactional: ctx.transactional,
            ..Event::default()
        },
    )))
}

impl Operator for TrickleSelect {
    #[allow(clippy::too_many_lines)]
    fn on_event(
        &mut self,
        _uid: u64,
        _port: &str,
        state: &mut Value<'static>,
        mut event: Event,
    ) -> Result<EventAndInsights> {
        /// Simple enum to decide what we return
        enum Res {
            Event,
            None,
            Data(EventAndInsights),
        }

        let Self {
            select,
            windows,
            event_id_gen,
            groups,
            recursion_limit,
            dflt_group,
            ..
        } = self;
        let recursion_limit = *recursion_limit;

        let Event {
            ingest_ns,
            ref mut data,
            ref id,
            ref origin_uri,
            ref op_meta,
            transactional,
            ..
        } = event;

        let ctx = EventContext::new(ingest_ns, origin_uri.as_ref());

        let opts = Self::opts();

        let res = data.apply_select(select, |data, stmt| -> TSResult<Res> {
            let SelectStmt {
                stmt: ref select,
                ref mut consts,
                ref locals,
                ref node_meta,
                ..
            } = stmt;

            let select: &Select = select;

            let Select {
                target,
                maybe_where,
                maybe_having,
                maybe_group_by,
                ..
            } = select;

            let local_stack = tremor_script::interpreter::LocalStack::with_size(*locals);

            //
            // Before any select processing, we filter by where clause
            //
            if let Some(guard) = maybe_where {
                let (unwind_event, event_meta) = data.parts();
                let env = Env {
                    context: &ctx,
                    consts: consts.run(),
                    aggrs: &NO_AGGRS,
                    meta: &node_meta,
                    recursion_limit,
                };
                let test = stry!(guard
                    .run(opts, &env, unwind_event, state, event_meta, &local_stack)
                    .and_then(|test| {
                        test.as_bool().ok_or_else(|| {
                            tremor_script::errors::query_guard_not_bool_err(
                                select, guard, &test, &node_meta,
                            )
                        })
                    }));
                if !test {
                    return Ok(Res::None);
                };
            }

            let mut group_values = if let Some(group_by) = &maybe_group_by {
                stry!(group_by.generate_groups(&ctx, data.value(), state, &node_meta, data.meta()))
            } else {
                //
                // select without group by or windows
                // event stays the same, only the value might change based on select clause
                // and we might drop it altogether based on having clause.
                //
                if windows.is_empty() {
                    consts.group = Value::from(vec![Value::const_null(), Value::from("[null]")]);

                    let (unwind_event, event_meta): (&mut Value, &mut Value) = data.parts_mut();

                    let env = Env {
                        context: &ctx,
                        consts: consts.run(),
                        aggrs: &NO_AGGRS,
                        meta: &node_meta,
                        recursion_limit,
                    };
                    let value = stry!(target.run(
                        opts,
                        &env,
                        unwind_event,
                        state,
                        event_meta,
                        &local_stack
                    ));

                    let result = value.into_owned();

                    // evaluate having clause, if one exists
                    #[allow(clippy::option_if_let_else)] // The borrow checker prevents map_or
                    return if let Some(guard) = maybe_having {
                        let test =
                            stry!(guard.run(opts, &env, &result, state, &NULL, &local_stack));
                        #[allow(clippy::option_if_let_else)] // The borrow checker prevents map_or
                        if let Some(test) = test.as_bool() {
                            if test {
                                *unwind_event = result;
                                Ok(Res::Event)
                            } else {
                                Ok(Res::None)
                            }
                        } else {
                            Err(tremor_script::errors::query_guard_not_bool_err(
                                select, guard, &test, &node_meta,
                            ))
                        }
                    } else {
                        *unwind_event = result;
                        Ok(Res::Event)
                    };
                };
                vec![]
            };

            if group_values.is_empty() {
                group_values.push(vec![Value::null()])
            };

            let group_values: Vec<Value> = group_values.into_iter().map(Value::from).collect();

            // Usually one or two windows emit, this is the common case so we don't pre-allocate
            // for the entire window depth
            let mut events = Vec::with_capacity(group_values.len() * 2);

            for group_value in group_values {
                let group_str = stry!(sorted_serialize(&group_value));

                let ctx = SelectCtx {
                    select,
                    local_stack: &local_stack,
                    node_meta,
                    opts,
                    ctx: &ctx,
                    state,
                    event_id: id.clone(),
                    event_id_gen,
                    ingest_ns,
                    op_meta,
                    origin_uri,
                    transactional,
                    recursion_limit,
                };

                match groups.entry(group_str) {
                    Entry::Occupied(mut o) => {
                        let del = stry!(o.get_mut().on_event(ctx, consts, data, &mut events));
                        if del {
                            o.remove();
                        }
                    }
                    Entry::Vacant(v) => {
                        dflt_group.value = group_value;
                        dflt_group.value.try_push(v.key().to_string());

                        let del = stry!(dflt_group.on_event(ctx, consts, data, &mut events));
                        if !del {
                            v.insert(dflt_group.clone());
                            dflt_group.reset();
                        }
                    }
                }
            }
            Ok(Res::Data(events.into()))
        })?;

        match res {
            Res::Event => Ok(event.into()),
            Res::None => Ok(EventAndInsights::default()),
            Res::Data(data) => Ok(data),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn on_signal(
        &mut self,
        _uid: u64,
        state: &mut Value<'static>, // we only reference state here immutably, no chance to change it here
        signal: &mut Event,
    ) -> Result<EventAndInsights> {
        // we only react on ticks and when we have windows
        let Self {
            select,
            windows,
            event_id_gen,
            groups,
            recursion_limit,
            ..
        } = self;
        let recursion_limit = *recursion_limit;

        if signal.kind != Some(SignalKind::Tick) || windows.is_empty() {
            return Ok(EventAndInsights::default());
        }
        let ingest_ns = signal.ingest_ns;

        let opts = Self::opts();
        select.rent_mut(|stmt| {
            let SelectStmt {
                stmt: select,
                consts,
                locals,
                node_meta,
                ..
            } = stmt;
            let mut res = EventAndInsights::default();

            let data: ValueAndMeta = (Value::const_null(), Value::object()).into();
            let op_meta = OpMeta::default();
            let local_stack = tremor_script::interpreter::LocalStack::with_size(*locals);

            consts.window = Value::const_null();
            consts.group = Value::const_null();
            consts.args = Value::const_null();

            let ctx = EventContext::new(ingest_ns, None);

            let mut to_remove = vec![];
            for (group_str, g) in groups.iter_mut() {
                if let Some(w) = &mut g.windows {
                    let mut outgoing_event_id = event_id_gen.next_id();
                    mem::swap(&mut w.id, &mut outgoing_event_id);

                    let mut run = consts.run();
                    run.group = &g.value;
                    run.window = &w.name;
                    let window_event = w.window.on_tick(ingest_ns)?;
                    let mut can_remove = window_event.emit;

                    if window_event.emit {
                        // push
                        let env = Env {
                            context: &ctx,
                            consts: run,
                            aggrs: &w.aggrs,
                            meta: &node_meta,
                            recursion_limit,
                        };

                        let mut outgoing_event_id = event_id_gen.next_id();

                        mem::swap(&mut outgoing_event_id, &mut w.id);

                        let mut ctx = SelectCtx {
                            select,
                            local_stack: &local_stack,
                            node_meta,
                            opts,
                            ctx: &ctx,
                            state,
                            event_id: outgoing_event_id,
                            event_id_gen,
                            ingest_ns,
                            op_meta: &op_meta,
                            origin_uri: &None,
                            transactional: w.transactional,
                            recursion_limit,
                        };

                        if let Some(port_and_event) =
                            super::select::execute_select_and_having(&ctx, &env, &data)?
                        {
                            res.events.push(port_and_event);
                        };
                        // re-initialize aggr state for new window
                        // reset transactional state for outgoing events

                        if let Some(next) = &mut w.next {
                            can_remove = next.on_event(
                                &mut ctx,
                                run,
                                &data,
                                &mut res.events,
                                Some(&w.aggrs),
                                can_remove,
                            )?;
                        }
                        w.reset();
                    }
                    if can_remove {
                        to_remove.push(group_str.clone());
                    }
                }
            }
            for g in to_remove {
                groups.remove(&g);
            }
            Ok(res)
        })
    }

    fn handles_signal(&self) -> bool {
        true
    }
}
