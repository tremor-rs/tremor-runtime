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

use super::window::{self, accumulate, get_or_create_group, Trait, Window, WindowEvent};
use crate::{errors::Result, EventId, SignalKind};
use crate::{op::prelude::*, EventIdGenerator};
use crate::{Event, Operator};
use std::mem;
use tremor_common::stry;
use tremor_script::{
    self,
    ast::{InvokeAggrFn, NodeMetas, Select, SelectStmt},
    interpreter::Env,
    interpreter::LocalStack,
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
    pub event_id_gen: EventIdGenerator,
}

pub(crate) const NO_AGGRS: [InvokeAggrFn<'static>; 0] = [];

impl TrickleSelect {
    pub fn with_stmt(
        operator_uid: u64,
        id: String,
        dims: &window::Groups,
        windows: Vec<(String, window::Impl)>,
        stmt: &srs::Stmt,
    ) -> Result<Self> {
        let windows = windows
            .into_iter()
            .map(|(fqwn, window_impl)| Window {
                dims: dims.clone(),
                last_dims: dims.clone(),
                module: Window::module_path(&fqwn),
                name: Window::ident_name(&fqwn).to_string(),
                window_impl,
            })
            .collect();
        let select = srs::Select::try_new_from_stmt(stmt)?;
        Ok(Self {
            id,
            windows,
            select,
            event_id_gen: EventIdGenerator::new(operator_uid),
        })
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
fn execute_select_and_having(
    stmt: &Select,
    node_meta: &NodeMetas,
    opts: ExecOpts,
    local_stack: &LocalStack,
    state: &Value<'static>,
    env: &Env,
    event_id: EventId,
    data: &ValueAndMeta,
    ingest_ns: u64,
    op_meta: &OpMeta,
    origin_uri: Option<EventOriginUri>,
    transactional: bool,
) -> Result<Option<(Cow<'static, str>, Event)>> {
    let (event_payload, event_meta) = data.parts();

    let value = stmt
        .target
        .run(opts, env, event_payload, state, event_meta, local_stack)?;

    // check having clause
    let result = value.into_owned();
    if let Some(guard) = &stmt.maybe_having {
        let test = guard.run(opts, env, &result, state, &NULL, local_stack)?;
        if let Some(test) = test.as_bool() {
            if !test {
                return Ok(None);
            }
        } else {
            let s: &Select = &stmt;
            return Err(tremor_script::errors::query_guard_not_bool_err(
                s, guard, &test, node_meta,
            )
            .into());
        }
    }
    Ok(Some((
        OUT,
        Event {
            id: event_id,
            ingest_ns,
            origin_uri,
            // TODO: this will ignore op_metas from all other events this one is based upon and might break operators requiring this
            op_meta: op_meta.clone(),
            is_batch: false,
            data: (result.into_static(), event_meta.clone_static()).into(),
            transactional,
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

        // TODO avoid origin_uri clone here
        let ctx = EventContext::new(ingest_ns, origin_uri.clone());

        let opts = Self::opts();

        let res = data.apply_select(select, |data, stmt| -> Result<Res> {
            let SelectStmt {
                stmt: ref select,
                ref mut aggregates,
                ref mut aggregate_scratches,
                ref mut consts,
                ref locals,
                ref node_meta,
            } = stmt;

            consts.window = Value::const_null();
            consts.group = Value::const_null();
            consts.args = Value::const_null();

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
                    recursion_limit: tremor_script::recursion_limit(),
                };
                let test = guard.run(opts, &env, unwind_event, state, event_meta, &local_stack)?;
                if let Some(test) = test.as_bool() {
                    if !test {
                        return Ok(Res::None);
                    };
                } else {
                    let s: &Select = &select;
                    return Err(tremor_script::errors::query_guard_not_bool_err(
                        s, guard, &test, &node_meta,
                    )
                    .into());
                };
            }

            let mut events = Vec::with_capacity(1);

            let mut group_values = if let Some(group_by) = &maybe_group_by {
                group_by.generate_groups(&ctx, data.value(), state, &node_meta, data.meta())?
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
                        recursion_limit: tremor_script::recursion_limit(),
                    };
                    let value =
                        target.run(opts, &env, unwind_event, state, event_meta, &local_stack)?;

                    let result = value.into_owned();

                    // evaluate having clause, if one exists
                    #[allow(clippy::option_if_let_else)] // The borrow checker prevents map_or
                    return if let Some(guard) = maybe_having {
                        let test = guard.run(opts, &env, &result, state, &NULL, &local_stack)?;
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
                            )
                            .into())
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

            // Handle eviction
            // retire the group data that didnt receive an event in `eviction_ns()` nanoseconds
            // if no event came after `2 * eviction_ns()` this group is finally cleared out
            windows.iter_mut().for_each(|w| w.maybe_evict(ingest_ns));

            let group_values: Vec<Value> = group_values.into_iter().map(Value::Array).collect();

            for group_value in group_values {
                let group_str = sorted_serialize(&group_value)?;

                consts.group = group_value.clone_static();
                consts.group.push(group_str.clone())?;

                match windows.len() {
                    0 => {
                        // group by without windows
                        // otherwise we just pass it through the select portion of the statement

                        // evaluate select clause
                        let env = Env {
                            context: &ctx,
                            consts: consts.run(),
                            aggrs: &NO_AGGRS,
                            meta: &node_meta,
                            recursion_limit: tremor_script::recursion_limit(),
                        };
                        // here we have a 1:1 between input and output event and thus can keep the origin uri
                        if let Some(port_and_event) = stry!(execute_select_and_having(
                            &select,
                            &node_meta,
                            opts,
                            &local_stack,
                            state,
                            &env,
                            id.clone(),
                            &data,
                            ingest_ns,
                            op_meta,
                            origin_uri.clone(),
                            transactional // no windows, we can safely pass through the events field
                        )) {
                            events.push(port_and_event);
                        };
                    }
                    1 => {
                        // simple case of single window
                        // window_event = on_event(event)
                        // if emit && !include
                        //   push
                        //   init
                        // accumulate
                        // if emit && include
                        //   push
                        //   init

                        // ALLOW: we verified that an element exists
                        let window = &mut windows[0];
                        consts.window = Value::from(window.name().to_string());

                        // get current window group
                        let this_group = stry!(get_or_create_group(
                            window,
                            event_id_gen,
                            aggregates,
                            &group_str,
                            &group_value,
                        ));
                        let window_event =
                            stry!(this_group.window.on_event(&data, ingest_ns, origin_uri));

                        // If we include the event we accumulate before we emit
                        if window_event.include {
                            stry!(accumulate(
                                &ctx,
                                consts.run(),
                                opts,
                                &node_meta,
                                &local_stack,
                                state,
                                this_group,
                                data,
                                id,
                                transactional
                            ));
                        }
                        if window_event.emit {
                            // push
                            let env = Env {
                                context: &ctx,
                                consts: consts.run(),
                                aggrs: &this_group.aggrs,
                                meta: &node_meta,
                                recursion_limit: tremor_script::recursion_limit(),
                            };
                            let mut outgoing_event_id = event_id_gen.next_id();
                            mem::swap(&mut outgoing_event_id, &mut this_group.id);
                            if let Some(port_and_event) = stry!(execute_select_and_having(
                                &select,
                                &node_meta,
                                opts,
                                &local_stack,
                                state,
                                &env,
                                outgoing_event_id,
                                &data,
                                ingest_ns,
                                op_meta,
                                None,
                                this_group.transactional
                            )) {
                                events.push(port_and_event);
                            };
                            // re-initialize aggr state for new window
                            // reset transactional state for outgoing events
                            this_group.reset();
                        }

                        // If we do not include the event we accumulate after we emit
                        if !window_event.include {
                            stry!(accumulate(
                                &ctx,
                                consts.run(),
                                opts,
                                &node_meta,
                                &local_stack,
                                state,
                                this_group,
                                data,
                                id,
                                transactional
                            ));
                        }
                    }
                    _ => {
                        // multiple windows
                        // emit_window_events = while emit { on_event() }
                        // if !emit || include {
                        //    accumulate
                        // }
                        // for window in emit_windows {
                        //   if include
                        //      this.aggrs.merge(scratch_last_window);
                        //      push event
                        //      swap(scratch_last_window, aggrs)
                        //      aggrs.init() // init next window
                        //   else
                        //      push event
                        //      if first {
                        //          swap(scratch_last_window, aggrs)
                        //          this.aggrs.init() // no merge needed
                        //      } else {
                        //          scratch2 = this.aggrs.clone()
                        //          swap(scratch_last_window, scratch2) // store old aggrs state from last window
                        //          swap(this.aggrs, scratch_last_window) // store aggrs state from this before init
                        //          this.aggrs.init()
                        //          this.aggrs.merge(scratch2);
                        //      }
                        // }
                        // if emit && !include { // if the first window is !include we postpone accumulation until here
                        //    accumulate
                        // }
                        // // if we have a window after the last emit one, we need to propagate the event/aggregation data to it
                        // if let Some(non_emit_window) = self.windows.get(emit_window_events.len()) {
                        //      this.aggrs.merge(scratch)
                        // }
                        //
                        // The issue with the windows is the following:
                        // We emit on the first event of the next windows, this works well for the initial frame
                        // on a second frame, we have the coordinate with the first we have a problem as we
                        // merge the higher resolution data into the lower resolution data before we get a chance
                        // to emit the lower res frame - causing us to have an element too much
                        // problems get worse as we now have windows that might emit before the current event
                        // is accumulated into the aggregation state or after
                        // we need to store the state before re-init in a scratch, so the next window can pull it whenever
                        // it is ready to do so, before or after accumulating
                        //
                        // event | size 2 | tilt   | size 3 |
                        //     1 | [1]    |        |
                        //     2 | [1, 2] |        |
                        //     3 | emit   | [1, 2] | [1, 2]
                        //       | [3]    |        | [1, 2]
                        //     4 | [3, 4] |        | [1, 2]
                        //     5 | emit   | [3, 4] | [1, 2, 3, 4]
                        //       | [5]    |        | [1, 2, 3, 4]
                        //     6 | [5, 6] |        | [1, 2, 3, 4]
                        //     7 | emit   | [5, 6] | [1, 2, 3, 4, 5, 6]
                        //       | [7]    |        | [1, 2, 3, 4, 5, 6]
                        //     8 | [7, 8] |        | [1, 2, 3, 4, 5, 6]
                        //     9 | emit   | [7, 8] | [1, 2, 3, 4, 5, 6, 7, 8] // this is where things break
                        //       | [9]    |        | [1, 2, 3, 4, 5, 6, 7, 8] // since we tilt up before we check
                        //       |        |        | emit                     // the next window we collect one too many elements

                        // we need scratches for multiple window handling, to avoid allocating on the hot path here
                        let (scratch_last_window, scratch2) = aggregate_scratches;
                        // scratches for passing on the event id between windows to track all events contributing to an outgoing windowed event
                        // cannot put them in AggregateScratch, as this is in tremor_script and doesnt know about EventId
                        // cannot put AggregateScratch in TrickleSelect as it has lifetimes which we tried to avoid here
                        let mut event_id_scratch_last_window = EventId::default();
                        let mut event_id_scratch2 = EventId::default();

                        // gather window events
                        let mut emit_window_events = Vec::with_capacity(windows.len());
                        let step1_iter = windows.iter_mut();
                        for window in step1_iter {
                            consts.window = Value::from(window.name().to_string());
                            // get current window group
                            let this_group = stry!(get_or_create_group(
                                window,
                                event_id_gen,
                                aggregates,
                                &group_str,
                                &group_value,
                            ));
                            let window_event =
                                stry!(this_group.window.on_event(&data, ingest_ns, origin_uri));
                            if window_event.emit {
                                emit_window_events.push(window_event);
                            } else {
                                break;
                            }
                        }

                        // accumulate if we do not emit anything or the first emitting window includes the current event into its state (window_event.include == true)
                        let pending_accumulate = if let Some(WindowEvent {
                            emit: true,
                            include: false,
                            ..
                        }) = emit_window_events.first()
                        {
                            // postpone
                            true
                        } else if let Some(first_window) = windows.first_mut() {
                            consts.window = Value::from(first_window.name().to_string());

                            // get current window group
                            let this_group = stry!(get_or_create_group(
                                first_window,
                                event_id_gen,
                                aggregates,
                                &group_str,
                                &group_value,
                            ));

                            // accumulate the current event
                            stry!(accumulate(
                                &ctx,
                                consts.run(),
                                opts,
                                &node_meta,
                                &local_stack,
                                state,
                                this_group,
                                data,
                                id,
                                transactional
                            ));
                            false
                        } else {
                            // should not happen
                            false
                        };

                        // merge previous aggr state, push window event out, re-initialize for new window
                        let mut first = true;
                        let emit_window_iter = emit_window_events.iter().zip(windows.iter_mut());
                        for (window_event, window) in emit_window_iter {
                            consts.window = Value::from(window.name().to_string());

                            // get current window group
                            let this_group = stry!(get_or_create_group(
                                window,
                                event_id_gen,
                                aggregates,
                                &group_str,
                                &group_value,
                            ));

                            if window_event.include {
                                // add this event to the aggr state **BEFORE** emit and propagate to next windows
                                // merge with scratch
                                if !first {
                                    for (this, prev) in this_group
                                        .aggrs
                                        .iter_mut()
                                        .zip(scratch_last_window.aggregates.iter())
                                    {
                                        this.invocable.merge(&prev.invocable).map_err(|e| {
                                            let r: Option<&Registry> = None;
                                            e.into_err(prev, prev, r, &node_meta)
                                        })?;
                                    }
                                    this_group
                                        .track_transactional(scratch_last_window.transactional);
                                    this_group.id.track(&event_id_scratch_last_window);
                                }
                                // push event
                                let env = Env {
                                    context: &ctx,
                                    consts: consts.run(),
                                    aggrs: &this_group.aggrs,
                                    meta: &node_meta,
                                    recursion_limit: tremor_script::recursion_limit(),
                                };

                                let mut outgoing_event_id = event_id_gen.next_id();
                                event_id_scratch_last_window = this_group.id.clone(); // store the id for to be tracked by the next window
                                mem::swap(&mut outgoing_event_id, &mut this_group.id);
                                if let Some(port_and_event) = stry!(execute_select_and_having(
                                    &select,
                                    &node_meta,
                                    opts,
                                    &local_stack,
                                    state,
                                    &env,
                                    outgoing_event_id,
                                    &data,
                                    ingest_ns,
                                    op_meta,
                                    None,
                                    this_group.transactional
                                )) {
                                    events.push(port_and_event);
                                };

                                // swap(aggrs, scratch) - store state before init to scratch as we need it for the next window
                                std::mem::swap(
                                    &mut scratch_last_window.aggregates,
                                    &mut this_group.aggrs,
                                );
                                // store transactional state for mixing into the next window
                                scratch_last_window.transactional = this_group.transactional;

                                // aggrs.init() and reset transactional state
                                this_group.reset();
                            } else {
                                // add this event to the aggr state **AFTER** emit and propagate to next windows

                                // push event
                                let env = Env {
                                    context: &ctx,
                                    consts: consts.run(),
                                    aggrs: &this_group.aggrs,
                                    meta: &node_meta,
                                    recursion_limit: tremor_script::recursion_limit(),
                                };

                                let mut outgoing_event_id = event_id_gen.next_id();
                                if !first {
                                    // store old event_ids state from last window into scratch2
                                    std::mem::swap(
                                        &mut event_id_scratch_last_window,
                                        &mut event_id_scratch2,
                                    );
                                }
                                event_id_scratch_last_window = this_group.id.clone(); // store the id for to be tracked by the next window
                                mem::swap(&mut outgoing_event_id, &mut this_group.id);
                                if let Some(port_and_event) = stry!(execute_select_and_having(
                                    &select,
                                    &node_meta,
                                    opts,
                                    &local_stack,
                                    state,
                                    &env,
                                    outgoing_event_id,
                                    &data,
                                    ingest_ns,
                                    op_meta,
                                    None,
                                    this_group.transactional
                                )) {
                                    events.push(port_and_event);
                                };

                                if first {
                                    // first window
                                    //          swap(scratch, aggrs)
                                    std::mem::swap(
                                        &mut scratch_last_window.aggregates,
                                        &mut this_group.aggrs,
                                    ); // store current state before init
                                    scratch_last_window.transactional = this_group.transactional;
                                    //          aggrs.init() and reset transactional state
                                    this_group.reset();
                                } else {
                                    // not first window
                                    // store old aggrs state from last window into scratch2

                                    std::mem::swap(scratch2, scratch_last_window);
                                    //          swap(aggrs, scratch)
                                    // store aggrs state before init() into scratch1 for next window
                                    std::mem::swap(
                                        &mut scratch_last_window.aggregates,
                                        &mut this_group.aggrs,
                                    );
                                    scratch_last_window.transactional = this_group.transactional;
                                    //          aggrs.init() and reset transactional state
                                    // scratch2 <=> scratch_last_window
                                    // scratch_last_window <=> this
                                    //
                                    // scratchg 2 == last window
                                    // scratch_last_window == this
                                    // this == empty
                                    this_group.reset();

                                    //          merge state from last window into this one
                                    for (this, prev) in
                                        this_group.aggrs.iter_mut().zip(scratch2.aggregates.iter())
                                    {
                                        this.invocable.merge(&prev.invocable).map_err(|e| {
                                            let r: Option<&Registry> = None;
                                            e.into_err(prev, prev, r, &node_meta)
                                        })?;
                                    }
                                    this_group.track_transactional(scratch2.transactional);
                                    this_group.id.track(&event_id_scratch2);
                                }
                            }

                            first = false;
                        }

                        // execute the pending accumulate (if window_event.include == false of first window)
                        if pending_accumulate {
                            if let Some(first_window) = windows.first_mut() {
                                consts.window = Value::from(first_window.name().to_string());
                                // get current window group
                                let this_group = stry!(get_or_create_group(
                                    first_window,
                                    event_id_gen,
                                    aggregates,
                                    &group_str,
                                    &group_value,
                                ));

                                // accumulate the current event
                                stry!(accumulate(
                                    &ctx,
                                    consts.run(),
                                    opts,
                                    &node_meta,
                                    &local_stack,
                                    state,
                                    this_group,
                                    data,
                                    id,
                                    transactional
                                ));
                            }
                        }

                        // merge state from last emit window into next non-emit window if there is any
                        if !emit_window_events.is_empty() {
                            if let Some(non_emit_window) = windows.get_mut(emit_window_events.len())
                            {
                                consts.window = Value::from(non_emit_window.name().to_string());

                                // get current window group
                                let this_group = stry!(get_or_create_group(
                                    non_emit_window,
                                    event_id_gen,
                                    aggregates,
                                    &group_str,
                                    &group_value,
                                ));
                                for (this, prev) in this_group
                                    .aggrs
                                    .iter_mut()
                                    .zip(scratch_last_window.aggregates.iter())
                                {
                                    this.invocable.merge(&prev.invocable).map_err(|e| {
                                        let r: Option<&Registry> = None;
                                        e.into_err(prev, prev, r, &node_meta)
                                    })?;
                                }
                                this_group.track_transactional(scratch_last_window.transactional);
                                this_group.id.track(&event_id_scratch_last_window);
                            }
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
        state: &Value<'static>, // we only reference state here immutably, no chance to change it here
        signal: &mut Event,
    ) -> Result<EventAndInsights> {
        // we only react on ticks and when we have windows
        let Self {
            select,
            windows,
            event_id_gen,
            ..
        } = self;

        if signal.kind != Some(SignalKind::Tick) || windows.is_empty() {
            return Ok(EventAndInsights::default());
        }
        let ingest_ns = signal.ingest_ns;
        // Handle eviction
        // retire the group data that didnt receive an event in `eviction_ns()` nanoseconds
        // if no event came after `2 * eviction_ns()` this group is finally cleared out
        windows.iter_mut().for_each(|w| w.maybe_evict(ingest_ns));

        let opts = Self::opts();
        select.rent_mut(|stmt| {
            let SelectStmt {
                stmt,
                aggregates,
                aggregate_scratches,
                consts,
                locals,
                node_meta,
            } = stmt;
            let mut res = EventAndInsights::default();

            let vm: ValueAndMeta = (Value::null(), Value::object()).into();
            let op_meta = OpMeta::default();
            let local_stack = tremor_script::interpreter::LocalStack::with_size(*locals);

            consts.window = Value::const_null();
            consts.group = Value::const_null();
            consts.args = Value::const_null();

            let ctx = EventContext::new(ingest_ns, None);
            if windows.len() == 1 {
                for window in windows.iter_mut() {
                    consts.window = Value::from(window.name().to_string());
                    // iterate all groups, including the ones from last_dims
                    for (group_str, group_data) in
                        window.dims.iter_mut().chain(window.last_dims.iter_mut())
                    {
                        consts.group = group_data.group.clone_static();
                        consts.group.push(group_str.clone())?;

                        let window_event = stry!(group_data.window.on_tick(ingest_ns));
                        if window_event.emit {
                            // evaluate the event and push
                            let env = Env {
                                context: &ctx,
                                consts: consts.run(),
                                aggrs: &group_data.aggrs,
                                meta: &node_meta,
                                recursion_limit: tremor_script::recursion_limit(),
                            };
                            let mut outgoing_event_id = event_id_gen.next_id();
                            mem::swap(&mut group_data.id, &mut outgoing_event_id);
                            let executed = stry!(execute_select_and_having(
                                &stmt,
                                node_meta,
                                opts,
                                &local_stack,
                                state,
                                &env,
                                outgoing_event_id,
                                &vm,
                                ingest_ns,
                                &op_meta,
                                None,
                                group_data.transactional
                            ));
                            if let Some(port_and_event) = executed {
                                res.events.push(port_and_event);
                            }

                            // init aggregates and reset transactional status
                            group_data.reset();
                        }
                    }
                }
            } else {
                // complicated logic
                // get a set of groups
                //   for each window
                //      get the corresponding group data
                //      on_event ... do the magic dance with scratches etc
                // we need scratches for multiple window handling

                // we get away with just checking the first window as there can be no group in a further window
                // that is not also in the first.
                let distinct_groups: Vec<(String, Value)> = windows
                    .first()
                    .iter()
                    .flat_map(|window| (*window).dims.iter().chain(window.last_dims.iter()))
                    .map(|(group_str, group_data)| {
                        (group_str.clone(), group_data.group.clone_static())
                    })
                    .collect();

                for (group_str, group_value) in distinct_groups {
                    consts.group = group_value.clone();
                    consts.group.push(group_str.clone())?;

                    let (scratch1, scratch2) = aggregate_scratches;
                    // scratches for passing on the event id between windows to track all events contributing to an outgoing windowed event
                    let mut event_id_scratch1 = EventId::default();
                    let mut event_id_scratch2 = EventId::default();

                    // gather window events
                    let mut emit_window_events = Vec::with_capacity(windows.len());
                    let step1_iter = windows.iter_mut();
                    for window in step1_iter {
                        consts.window = Value::from(window.name().to_string());
                        // get current window group
                        let this_group = stry!(get_or_create_group(
                            window,
                            event_id_gen,
                            aggregates,
                            &group_str,
                            &group_value,
                        ));
                        let window_event = stry!(this_group.window.on_tick(ingest_ns));
                        if window_event.emit {
                            emit_window_events.push(window_event);
                        } else {
                            break;
                        }
                    }

                    // merge previous aggr state, push window event out, re-initialize for new window
                    let mut first = true;
                    let emit_window_iter = emit_window_events.iter().zip(windows.iter_mut());
                    for (window_event, window) in emit_window_iter {
                        consts.window = Value::from(window.name().to_string());

                        // get current window group
                        let this_group = stry!(get_or_create_group(
                            window,
                            event_id_gen,
                            aggregates,
                            &group_str,
                            &group_value,
                        ));

                        if window_event.include {
                            // add this event to the aggr state **BEFORE** emit and propagate to next windows
                            // merge with scratch
                            if !first {
                                for (this, prev) in
                                    this_group.aggrs.iter_mut().zip(scratch1.aggregates.iter())
                                {
                                    this.invocable.merge(&prev.invocable).map_err(|e| {
                                        let r: Option<&Registry> = None;
                                        e.into_err(prev, prev, r, &node_meta)
                                    })?;
                                }
                                this_group.track_transactional(scratch1.transactional);
                                this_group.id.track(&event_id_scratch1);
                            }
                            // push event
                            let env = Env {
                                context: &ctx,
                                consts: consts.run(),
                                aggrs: &this_group.aggrs,
                                meta: &node_meta,
                                recursion_limit: tremor_script::recursion_limit(),
                            };

                            let mut outgoing_event_id = event_id_gen.next_id();
                            event_id_scratch1 = this_group.id.clone(); // store event_id in a scratch for tracking it in the next window in the tilt-frame
                            mem::swap(&mut this_group.id, &mut outgoing_event_id);
                            if let Some(port_and_event) = stry!(execute_select_and_having(
                                &stmt,
                                &node_meta,
                                opts,
                                &local_stack,
                                state,
                                &env,
                                outgoing_event_id,
                                &vm,
                                ingest_ns,
                                &op_meta,
                                None,
                                this_group.transactional
                            )) {
                                res.events.push(port_and_event);
                            };

                            // swap(aggrs, scratch) - store state before init to scratch as we need it for the next window
                            std::mem::swap(&mut scratch1.aggregates, &mut this_group.aggrs);
                            scratch1.transactional = this_group.transactional;
                            // aggrs.init() and reset transactional state
                            this_group.reset();
                        } else {
                            // add this event to the aggr state **AFTER** emit and propagate to next windows

                            // push event
                            let env = Env {
                                context: &ctx,
                                consts: consts.run(),
                                aggrs: &this_group.aggrs,
                                meta: &node_meta,
                                recursion_limit: tremor_script::recursion_limit(),
                            };

                            let mut outgoing_event_id = event_id_gen.next_id();
                            if !first {
                                // store previous window event_id in scratch2
                                std::mem::swap(&mut event_id_scratch1, &mut event_id_scratch2);
                            }
                            event_id_scratch1 = this_group.id.clone(); // store event id before swapping it out, for tracking in next window
                            mem::swap(&mut this_group.id, &mut outgoing_event_id);
                            if let Some(port_and_event) = stry!(execute_select_and_having(
                                &stmt,
                                &node_meta,
                                opts,
                                &local_stack,
                                state,
                                &env,
                                outgoing_event_id,
                                &vm,
                                ingest_ns,
                                &op_meta,
                                None,
                                this_group.transactional
                            )) {
                                res.events.push(port_and_event);
                            };

                            if first {
                                // first window
                                //          swap(scratch, aggrs)
                                std::mem::swap(&mut scratch1.aggregates, &mut this_group.aggrs); // store current state before init
                                scratch1.transactional = this_group.transactional;
                                //          aggrs.init() and reset transactional state
                                this_group.reset();
                            } else {
                                // not first window
                                // store old aggrs state from last window into scratch2
                                std::mem::swap(scratch1, scratch2);
                                //          swap(aggrs, scratch) // store aggrs state before init() into scratch1 for next window
                                std::mem::swap(&mut this_group.aggrs, &mut scratch1.aggregates);
                                scratch1.transactional = this_group.transactional;

                                //          aggrs.init() and reset transactional state
                                this_group.reset();
                                //          merge state from last window into this one
                                for (this, prev) in
                                    this_group.aggrs.iter_mut().zip(scratch2.aggregates.iter())
                                {
                                    this.invocable.merge(&prev.invocable).map_err(|e| {
                                        let r: Option<&Registry> = None;
                                        e.into_err(prev, prev, r, &node_meta)
                                    })?;
                                }
                                this_group.track_transactional(scratch2.transactional);
                                this_group.id.track(&event_id_scratch2);
                            }
                        }

                        first = false;
                    }

                    // merge state from last emit window into next non-emit window if there is any
                    if !emit_window_events.is_empty() {
                        if let Some(non_emit_window) = windows.get_mut(emit_window_events.len()) {
                            consts.window = Value::from(non_emit_window.name().to_string());

                            // get current window group
                            let this_group = stry!(get_or_create_group(
                                non_emit_window,
                                event_id_gen,
                                aggregates,
                                &group_str,
                                &group_value,
                            ));
                            for (this, prev) in
                                this_group.aggrs.iter_mut().zip(scratch1.aggregates.iter())
                            {
                                this.invocable.merge(&prev.invocable).map_err(|e| {
                                    let r: Option<&Registry> = None;
                                    e.into_err(prev, prev, r, &node_meta)
                                })?;
                            }
                            this_group.track_transactional(scratch1.transactional);
                            this_group.id.track(&event_id_scratch1);
                        }
                    }
                }
            };
            Ok(res)
        })
    }

    fn handles_signal(&self) -> bool {
        true
    }
}
