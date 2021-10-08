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

use tremor_script::{
    self,
    ast::{self, ImutExpr, InvokeAggrFn, NodeMetas, RunConsts, SelectStmt},
    errors::Result as TSResult,
    interpreter::{Env, LocalStack},
    prelude::*,
    srs,
    utils::sorted_serialize,
    Value,
};

#[derive(Debug)]
pub struct Select {
    pub id: String,
    pub(crate) select: srs::Select,
    pub windows: Vec<Window>,
    pub groups: HashMap<String, Group>,
    pub event_id_gen: EventIdGenerator,
    recursion_limit: u32,
    dflt_group: Group,
    max_groups: usize,
}

pub(crate) const NO_AGGRS: [InvokeAggrFn<'static>; 0] = [];

impl Select {
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
                windows: GroupWindow::from_windows(aggregates, &EventId::default(), windows_itr),
            };
            let windows_itr = windows.iter();
            let max_groups = windows_itr
                .map(|w| w.window_impl.max_groups())
                .min()
                .unwrap_or(0) as usize;
            Ok(Self {
                id,
                windows,
                select,
                groups: HashMap::new(),
                event_id_gen,
                recursion_limit: tremor_script::recursion_limit(),
                dflt_group,
                max_groups,
            })
        } else {
            Err("Wrong type of statement".into())
        }
    }
    const fn opts() -> ExecOpts {
        ExecOpts {
            result_needed: true,
            aggr: AggrType::Emit,
        }
    }
}

/// execute the select clause of the statement and filter results by having clause, if provided
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
        &NULL,
        event_meta,
        ctx.local_stack,
    )?;

    // check having clause
    let result = value.into_owned();
    let having = stry!(run_guard(
        ctx.select,
        &ctx.select.maybe_having,
        ctx.opts,
        env,
        &result,
        ctx.local_stack,
        ctx.node_meta,
    ));
    if having {
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
    } else {
        Ok(None)
    }
}

fn env<'run, 'script>(
    context: &'run EventContext<'run>,
    consts: RunConsts<'run, 'script>,
    meta: &'run NodeMetas,
    recursion_limit: u32,
) -> Env<'run, 'script> {
    Env {
        context,
        consts,
        aggrs: &NO_AGGRS,
        meta,
        recursion_limit,
    }
}

/// Simple enum to decide what we return
enum Res {
    Event,
    None,
    Data(EventAndInsights),
}

impl Res {
    /// Turn a result into `EventAndInsights`
    fn into_insights(self, event: Event) -> EventAndInsights {
        match self {
            Res::Event => event.into(),
            Res::None => EventAndInsights::default(),
            Res::Data(data) => data,
        }
    }
}

impl Operator for Select {
    // Note: we don't use state in this function as select does not allow mutation
    // so the state can never be changed.
    fn on_event(
        &mut self,
        _uid: u64,
        _port: &str,
        _state: &mut Value<'static>,
        mut event: Event,
    ) -> Result<EventAndInsights> {
        let Self {
            select,
            windows,
            event_id_gen,
            groups,
            recursion_limit,
            dflt_group,
            max_groups,
            ..
        } = self;

        let Event {
            ingest_ns,
            ref mut data,
            ref id,
            ref origin_uri,
            ref op_meta,
            transactional,
            ..
        } = event;

        let mut ctx = EventContext::new(ingest_ns, origin_uri.as_ref());
        ctx.cardinality = groups.len();

        let opts = Self::opts();

        let res = data.apply_select(
            select,
            |event,
             SelectStmt {
                 stmt: select,
                 consts,
                 locals,
                 node_meta,
                 ..
             }|
             -> TSResult<Res> {
                let locals = tremor_script::interpreter::LocalStack::with_size(*locals);

                let (data, meta) = event.parts_mut();
                //
                // Before any select processing, we filter by where clause
                //

                let guard = &select.maybe_where;
                let e = env(&ctx, consts.run(), node_meta, *recursion_limit);
                let w_guard = run_guard(select, guard, opts, &e, data, &locals, node_meta);
                if !stry!(w_guard) {
                    return Ok(Res::None);
                };

                let group_values = if let Some(group_by) = &select.maybe_group_by {
                    let groups = stry!(group_by.generate_groups(&ctx, data, node_meta, meta));
                    groups.into_iter().map(Value::from).collect()
                } else if windows.is_empty() {
                    //
                    // select without group by or windows
                    // event stays the same, only the value might change based on select clause
                    // and we might drop it altogether based on having clause.
                    //
                    consts.group = Value::from(vec![Value::const_null(), Value::from("[null]")]);

                    let e = env(&ctx, consts.run(), node_meta, *recursion_limit);
                    let value = stry!(select.target.run(opts, &e, data, &NULL, meta, &locals));
                    let h_guard = &select.maybe_having;
                    let h_guard = run_guard(select, h_guard, opts, &e, &value, &locals, node_meta);
                    return if stry!(h_guard) {
                        *data = value.into_owned();
                        Ok(Res::Event)
                    } else {
                        Ok(Res::None)
                    };
                } else {
                    vec![Value::from(vec![Value::const_null()])]
                };

                // Usually one or two windows emit, this is the common case so we don't pre-allocate
                // for the entire window depth
                let mut events = Vec::with_capacity(group_values.len() * 2);

                // with the `each` grouping an event could be in more then one group, so we
                // iterate over all groups we found
                for group_value in group_values {
                    let group_str = stry!(sorted_serialize(&group_value));

                    ctx.cardinality = groups.len();

                    let sel_ctx = SelectCtx {
                        select,
                        local_stack: &locals,
                        node_meta,
                        opts,
                        ctx: &ctx,
                        event_id: id.clone(),
                        event_id_gen,
                        ingest_ns,
                        op_meta,
                        origin_uri,
                        transactional,
                        recursion_limit: *recursion_limit,
                    };

                    // see if we know the group already, we use the `entry` here so we don't
                    // need to add / remove from the groups unenessessarily
                    match groups.entry(group_str) {
                        Entry::Occupied(mut o) => {
                            // If we found a group execute it, and remove it if it is not longer
                            // needed
                            if stry!(o.get_mut().on_event(sel_ctx, consts, event, &mut events)) {
                                o.remove();
                            }
                        }
                        Entry::Vacant(v) => {
                            // If we didn't find a group re-use the statements default group and set
                            // the group value of it
                            dflt_group.value = group_value;
                            dflt_group.value.try_push(v.key().to_string());

                            // execute it
                            if !stry!(dflt_group.on_event(sel_ctx, consts, event, &mut events)) {
                                // if we can't delete it check if we're having too many groups,
                                // if so, error.
                                if ctx.cardinality >= *max_groups {
                                    return Err(format!("Maxmimum amount of groups reached ({}). Ignoring group [{}]", max_groups, *max_groups+1).into());
                                }
                                // otherwise we clone the default group (this is a cost we got to pay)
                                // and reset it . If we didn't clone here we'd need to allocate a new
                                // group for every event we haven't seen yet
                                v.insert(dflt_group.clone());
                                dflt_group.reset();
                            }
                        }
                    }
                }
                Ok(Res::Data(events.into()))
            },
        )?;

        Ok(res.into_insights(event))
    }

    fn on_signal(
        &mut self,
        _uid: u64,
        _state: &mut Value<'static>,
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

        // if it isn't a tick or we do not have any windows, or have no
        // recorded groups, we can just return
        if signal.kind != Some(SignalKind::Tick) || windows.is_empty() || groups.is_empty() {
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

            let mut data: ValueAndMeta = (Value::const_null(), Value::object()).into();
            let op_meta = OpMeta::default();
            let local_stack = tremor_script::interpreter::LocalStack::with_size(*locals);

            consts.window = Value::const_null();
            consts.group = Value::const_null();
            consts.args = Value::const_null();

            let mut ctx = EventContext::new(ingest_ns, None);
            ctx.cardinality = groups.len();

            let mut to_remove = vec![];
            for (group_str, g) in groups.iter_mut() {
                if let Some(w) = &mut g.windows {
                    let mut outgoing_event_id = event_id_gen.next_id();
                    mem::swap(&mut w.id, &mut outgoing_event_id);

                    let mut run = consts.run();
                    run.group = &g.value;
                    run.window = &w.name;
                    let window_event = w.window.on_tick(ingest_ns);
                    let mut can_remove = window_event.emit;

                    if window_event.emit {
                        // push
                        let mut env = env(&ctx, run, node_meta, recursion_limit);
                        env.aggrs = &w.aggrs;

                        let mut outgoing_event_id = event_id_gen.next_id();

                        mem::swap(&mut outgoing_event_id, &mut w.id);

                        let mut ctx = SelectCtx {
                            select,
                            local_stack: &local_stack,
                            node_meta,
                            opts,
                            ctx: &ctx,
                            event_id: outgoing_event_id,
                            event_id_gen,
                            ingest_ns,
                            op_meta: &op_meta,
                            origin_uri: &None,
                            transactional: w.transactional,
                            recursion_limit,
                        };
                        if w.holds_data {
                            if let Some(port_and_event) =
                                super::select::execute_select_and_having(&ctx, &env, &data)?
                            {
                                res.events.push(port_and_event);
                            };
                        }
                        // re-initialize aggr state for new window
                        // reset transactional state for outgoing events

                        if let Some(next) = &mut w.next {
                            can_remove = next.on_event(
                                &mut ctx,
                                run,
                                &mut data,
                                &mut res.events,
                                Some((w.holds_data, &w.aggrs)),
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

fn run_guard(
    select: &ast::Select,
    guard: &Option<ImutExpr>,
    opts: ExecOpts,
    env: &Env,
    result: &Value,
    local_stack: &LocalStack,
    node_meta: &NodeMetas,
) -> TSResult<bool> {
    if let Some(guard) = guard {
        let test = stry!(guard.run(opts, env, result, &NULL, &NULL, local_stack));
        test.as_bool().ok_or_else(|| {
            tremor_script::errors::query_guard_not_bool_err(select, guard, &test, node_meta)
        })
    } else {
        Ok(true)
    }
}
