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

use crate::{
    errors::{Error, ErrorKind, Result},
    EventId, SignalKind,
};
use crate::{op::prelude::*, EventIdGenerator};
use crate::{Event, Operator};
use halfbrown::{HashMap, RawEntryMut};
use std::borrow::Cow as SCow;
use std::mem;
use std::sync::Arc;
use tremor_common::stry;
use tremor_script::interpreter::Env;
use tremor_script::{
    self,
    ast::{Aggregates, InvokeAggrFn, Select, SelectStmt, WindowDecl},
    prelude::*,
    query::StmtRental,
    Value,
};
use tremor_script::{ast::NodeMetas, utils::sorted_serialize};
use tremor_script::{interpreter::LocalStack, query::StmtRentalWrapper};

#[derive(Debug, Clone)]
pub struct GroupData<'groups> {
    group: Value<'static>,
    window: WindowImpl,
    aggrs: Aggregates<'groups>,
    id: EventId,
}
type Groups<'groups> = HashMap<String, GroupData<'groups>>;
rental! {
    pub mod rentals {
        use std::sync::Arc;
        use halfbrown::HashMap;
        use super::*;

        #[rental(covariant, debug)]
        pub struct Select {
            stmt: Arc<StmtRental>,
            select: tremor_script::ast::SelectStmt<'stmt>,
        }
        #[rental(covariant, debug, clone)]
        pub struct Window {
            stmt: Arc<StmtRental>,
            window: WindowDecl<'stmt>,
        }
    }
}

/// Select dimensions
#[derive(Debug, Clone)]
pub struct Dims {
    query: Arc<StmtRental>,
    groups: Groups<'static>,
}

impl Dims {
    pub fn new(query: Arc<StmtRental>) -> Self {
        Self {
            query,
            groups: HashMap::new(),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct TrickleSelect {
    pub id: String,
    pub select: rentals::Select,
    pub windows: Vec<Window>,
    pub event_id_gen: EventIdGenerator,
}

pub trait WindowTrait: std::fmt::Debug {
    fn on_event(&mut self, event: &Event) -> Result<WindowEvent>;
    /// handle a tick with the current time in nanoseconds as `ns` argument
    fn on_tick(&mut self, _ns: u64) -> Result<WindowEvent> {
        Ok(WindowEvent {
            opened: false,
            include: false,
            emit: false,
        })
    }
    fn eviction_ns(&self) -> Option<u64>;
    /// maximum number of groups to keep around simultaneously
    /// a value of `u64::MAX` allows as much simultaneous groups as possible
    /// decreasing this value will guard against runwaway memory growth
    /// when faced with unexpected huge cardinalities for grouping dimensions
    fn max_groups(&self) -> u64;
}

#[derive(Debug)]
pub struct Window {
    window_impl: WindowImpl,
    module: Vec<String>,
    name: String,
    dims: Dims,
    last_dims: Dims,
    next_swap: u64,
}

impl Window {
    pub(crate) fn module_path(fqwn: &str) -> Vec<String> {
        let segments: Vec<_> = fqwn.split("::").collect();
        if let Some((_, rest)) = segments.split_last() {
            rest.iter().map(|v| (*v).to_string()).collect()
        } else {
            vec![]
        }
    }

    pub(crate) fn ident_name(fqwn: &str) -> &str {
        fqwn.split("::").last().map_or(fqwn, |last| last)
    }
}

// We allow this since No is barely ever used.
#[derive(Debug, Clone)]
pub enum WindowImpl {
    TumblingCountBased(TumblingWindowOnNumber),
    TumblingTimeBased(TumblingWindowOnTime),
    No(NoWindow),
}

impl WindowImpl {
    // allow all the groups we can take by default
    // this preserves backward compatibility
    pub const DEFAULT_MAX_GROUPS: u64 = u64::MAX;

    // do not emit empty windows by default
    // this preserves backward compatibility
    pub const DEFAULT_EMIT_EMPTY_WINDOWS: bool = false;
}

impl std::default::Default for WindowImpl {
    fn default() -> Self {
        TumblingWindowOnTime {
            interval: 15_000_000_000,
            emit_empty_windows: Self::DEFAULT_EMIT_EMPTY_WINDOWS,
            max_groups: Self::DEFAULT_MAX_GROUPS,
            next_window: None,
            events: 0,
            script: None,
            ttl: None,
        }
        .into()
    }
}

impl WindowTrait for WindowImpl {
    fn on_event(&mut self, event: &Event) -> Result<WindowEvent> {
        match self {
            Self::TumblingTimeBased(w) => w.on_event(event),
            Self::TumblingCountBased(w) => w.on_event(event),
            Self::No(w) => w.on_event(event),
        }
    }

    fn on_tick(&mut self, ns: u64) -> Result<WindowEvent> {
        match self {
            Self::TumblingTimeBased(w) => w.on_tick(ns),
            Self::TumblingCountBased(w) => w.on_tick(ns),
            Self::No(w) => w.on_tick(ns),
        }
    }

    fn eviction_ns(&self) -> Option<u64> {
        match self {
            Self::TumblingTimeBased(w) => w.eviction_ns(),
            Self::TumblingCountBased(w) => w.eviction_ns(),
            Self::No(w) => w.eviction_ns(),
        }
    }
    fn max_groups(&self) -> u64 {
        match self {
            Self::TumblingTimeBased(w) => w.max_groups(),
            Self::TumblingCountBased(w) => w.max_groups(),
            Self::No(w) => w.max_groups(),
        }
    }
}

impl From<NoWindow> for WindowImpl {
    fn from(w: NoWindow) -> Self {
        Self::No(w)
    }
}

impl From<TumblingWindowOnNumber> for WindowImpl {
    fn from(w: TumblingWindowOnNumber) -> Self {
        Self::TumblingCountBased(w)
    }
}
impl From<TumblingWindowOnTime> for WindowImpl {
    fn from(w: TumblingWindowOnTime) -> Self {
        Self::TumblingTimeBased(w)
    }
}

#[derive(Debug, PartialEq)]
pub struct WindowEvent {
    // New window has opened
    opened: bool,
    /// Include the current event in the window event to be emitted
    include: bool,
    /// Emit a window event
    emit: bool,
}

#[derive(Default, Debug, Clone)]
pub struct NoWindow {
    open: bool,
}

impl WindowTrait for NoWindow {
    fn eviction_ns(&self) -> Option<u64> {
        None
    }
    fn on_event(&mut self, _event: &Event) -> Result<WindowEvent> {
        self.open = true;
        Ok(WindowEvent {
            opened: true,
            include: true,
            emit: true,
        })
    }
    fn max_groups(&self) -> u64 {
        u64::MAX
    }
}

#[derive(Default, Debug, Clone)]
pub struct TumblingWindowOnTime {
    next_window: Option<u64>,
    emit_empty_windows: bool,
    max_groups: u64,
    events: u64,
    interval: u64,
    ttl: Option<u64>,
    script: Option<rentals::Window>,
}
impl TumblingWindowOnTime {
    pub fn from_stmt(
        interval: u64,
        emit_empty_windows: bool,
        max_groups: u64,
        ttl: Option<u64>,
        script: Option<&WindowDecl>,
        stmt: &StmtRentalWrapper,
    ) -> Self {
        let script = script.map(|s| {
            rentals::Window::new(stmt.stmt.clone(), |_| unsafe {
                // This is sound since stmt.stmt is an arc that holds
                // the borrowed data
                mem::transmute::<WindowDecl<'_>, WindowDecl<'static>>(s.clone())
            })
        });
        Self {
            next_window: None,
            emit_empty_windows,
            max_groups,
            events: 0,
            interval,
            ttl,
            script,
        }
    }

    fn get_window_event(&mut self, time: u64) -> WindowEvent {
        match self.next_window {
            None => {
                self.next_window = Some(time + self.interval);
                WindowEvent {
                    opened: true,
                    include: false,
                    emit: false,
                }
            }
            Some(next_window) if next_window <= time => {
                let emit = self.events > 0 || self.emit_empty_windows;
                self.next_window = Some(time + self.interval);
                self.events = 0;
                WindowEvent {
                    opened: true,   // this event has been put into the newly opened window
                    include: false, // event is beyond the current window, put it into the next
                    emit,           // only emit if we had any events in this interval
                }
            }
            Some(_) => WindowEvent {
                opened: false,
                include: false,
                emit: false,
            },
        }
    }
}

impl WindowTrait for TumblingWindowOnTime {
    fn eviction_ns(&self) -> Option<u64> {
        self.ttl
    }
    fn max_groups(&self) -> u64 {
        self.max_groups
    }
    fn on_event(&mut self, event: &Event) -> Result<WindowEvent> {
        self.events += 1; // count events to check if we should emit, as we avoid to emit if we have no events
        let time = self
            .script
            .as_ref()
            .and_then(|script| script.suffix().script.as_ref())
            .map(|script| {
                // TODO avoid origin_uri clone here
                let context = EventContext::new(event.ingest_ns, event.origin_uri.clone());
                let (mut unwind_event, mut event_meta) = event.data.parts();
                let value = script.run(
                    &context,
                    AggrType::Emit,
                    &mut unwind_event,  // event
                    &mut Value::null(), // state for the window
                    &mut event_meta,    // $
                )?;
                let data = match value {
                    Return::Emit { value, .. } => value.as_u64(),
                    Return::EmitEvent { .. } => unwind_event.as_u64(),
                    Return::Drop { .. } => None,
                };
                data.ok_or_else(|| Error::from("Data based window didn't provide a valid value"))
            })
            .unwrap_or(Ok(event.ingest_ns))?;
        Ok(self.get_window_event(time))
    }

    fn on_tick(&mut self, ns: u64) -> Result<WindowEvent> {
        if self.script.is_none() {
            Ok(self.get_window_event(ns))
        } else {
            // we basically ignore ticks when we have a script with a custom timestamp
            Ok(WindowEvent {
                opened: false,
                include: false,
                emit: false,
            })
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct TumblingWindowOnNumber {
    count: u64,
    max_groups: u64,
    size: u64,
    ttl: Option<u64>,
    script: Option<rentals::Window>,
}

impl TumblingWindowOnNumber {
    pub fn from_stmt(
        size: u64,
        max_groups: u64,
        ttl: Option<u64>,
        script: Option<&WindowDecl>,
        stmt: &StmtRentalWrapper,
    ) -> Self {
        let script = script.map(|s| {
            rentals::Window::new(stmt.stmt.clone(), |_| unsafe {
                // This is safe since `stmt.stmt` is an Arc that
                // hods the referenced data and we clone it into the rental.
                // This ensures referenced data isn't dropped until the rental
                // is dropped.
                mem::transmute::<WindowDecl<'_>, WindowDecl<'static>>(s.clone())
            })
        });
        Self {
            count: 0,
            max_groups,
            size,
            script,
            ttl,
        }
    }
}
impl WindowTrait for TumblingWindowOnNumber {
    fn eviction_ns(&self) -> Option<u64> {
        self.ttl
    }
    fn max_groups(&self) -> u64 {
        self.max_groups
    }
    fn on_event(&mut self, event: &Event) -> Result<WindowEvent> {
        let count = self
            .script
            .as_ref()
            .and_then(|script| script.suffix().script.as_ref())
            .map_or(Ok(1), |script| {
                // TODO avoid origin_uri clone here
                let context = EventContext::new(event.ingest_ns, event.origin_uri.clone());
                let (mut unwind_event, mut event_meta) = event.data.parts();
                let value = script.run(
                    &context,
                    AggrType::Emit,
                    &mut unwind_event,  // event
                    &mut Value::null(), // state for the window
                    &mut event_meta,    // $
                )?;
                let data = match value {
                    Return::Emit { value, .. } => value.as_u64(),
                    Return::EmitEvent { .. } => unwind_event.as_u64(),
                    Return::Drop { .. } => None,
                };
                data.ok_or_else(|| Error::from("Data based window didn't provide a valid value"))
            })?;

        // If we're above count we emit and  set the new count to 1
        // ( we emit on the ) previous event
        let new_count = self.count + count;
        if new_count >= self.size {
            self.count = new_count - self.size;
            Ok(WindowEvent {
                opened: true,
                include: true, // we can emit now, including this event
                emit: true,
            })
        } else {
            self.count = new_count;
            Ok(WindowEvent {
                opened: false,
                include: false,
                emit: false,
            })
        }
    }
}

const NO_AGGRS: [InvokeAggrFn<'static>; 0] = [];

impl TrickleSelect {
    pub fn with_stmt(
        operator_uid: u64,
        id: String,
        dims: &Dims,
        windows: Vec<(String, WindowImpl)>,
        stmt_rentwrapped: &tremor_script::query::StmtRentalWrapper,
    ) -> Result<Self> {
        let select = match stmt_rentwrapped.suffix() {
            tremor_script::ast::Stmt::Select(ref select) => select.clone(),
            _ => {
                return Err(ErrorKind::PipelineError(
                    "Trying to turn a non select into a select operator".into(),
                )
                .into())
            }
        };

        let windows = windows
            .into_iter()
            .map(|(fqwn, window_impl)| Window {
                dims: dims.clone(),
                last_dims: dims.clone(),
                module: Window::module_path(&fqwn),
                name: Window::ident_name(&fqwn).to_string(),
                window_impl,
                next_swap: 0,
            })
            .collect();
        Ok(Self {
            id,
            windows,
            select: rentals::Select::new(stmt_rentwrapped.stmt.clone(), move |_| unsafe {
                // This is safe since `stmt_rentwrapped.stmt` is an Arc that
                // hods the referenced data and we clone it into the rental.
                // This ensures referenced data isn't dropped until the rental
                // is dropped.
                mem::transmute::<SelectStmt<'_>, SelectStmt<'static>>(select)
            }),
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
#[allow(clippy::clippy::too_many_arguments)]
fn execute_select_and_having(
    stmt: &Select,
    node_meta: &NodeMetas,
    opts: ExecOpts,
    local_stack: &LocalStack,
    state: &Value<'static>,
    env: &Env,
    event_id_gen: &mut EventIdGenerator,
    event: &Event,
    origin_uri: Option<EventOriginUri>,
) -> Result<Option<(Cow<'static, str>, Event)>> {
    let (event_payload, event_meta) = event.data.parts();

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
    let mut event_id = event_id_gen.next_id();
    event_id.track(&event.id);
    Ok(Some((
        OUT,
        Event {
            id: event_id,
            ingest_ns: event.ingest_ns,
            // TODO avoid origin_uri clone here
            origin_uri,
            is_batch: false,
            data: (result.into_static(), event_meta.clone_static()).into(),
            ..Event::default()
        },
    )))
}

/// accumulate the given `event` into the current `group`s aggregates
fn accumulate(
    opts: ExecOpts,
    node_meta: &NodeMetas,
    env: &Env,
    local_stack: &LocalStack,
    state: &mut Value<'static>,
    group: &mut GroupData,
    event: &Event,
) -> Result<()> {
    // we incorporate the event into this group below, so track it here
    group.id.track(&event.id);

    let (event_data, event_meta) = event.data.parts();
    for aggr in &mut group.aggrs {
        let invocable = &mut aggr.invocable;
        let mut argv: Vec<SCow<Value>> = Vec::with_capacity(aggr.args.len());
        let mut argv1: Vec<&Value> = Vec::with_capacity(aggr.args.len());
        for arg in &aggr.args {
            let result = arg.run(opts, env, event_data, state, event_meta, &local_stack)?;
            argv.push(result);
        }
        for arg in &argv {
            argv1.push(arg);
        }

        invocable.accumulate(argv1.as_slice()).map_err(|e| {
            // TODO nice error
            let r: Option<&Registry> = None;
            e.into_err(aggr, aggr, r, node_meta)
        })?;
    }
    Ok(())
}

/// get a mutable reference to the group of this window identified by `group_str` and `group_value`
fn get_or_create_group<'window>(
    window: &'window mut Window,
    idgen: &mut EventIdGenerator,
    aggregates: &[InvokeAggrFn<'static>],
    group_str: &str,
    group_value: &Value,
) -> Result<&'window mut GroupData<'static>> {
    let this_groups = &mut window.dims.groups;
    let groups_len = this_groups.len() as u64;
    let last_groups = &mut window.last_dims.groups;
    let window_impl = &window.window_impl;
    let (_, this_group) = match this_groups.raw_entry_mut().from_key(group_str) {
        // avoid double-clojure
        RawEntryMut::Occupied(e) => e.into_key_value(),
        RawEntryMut::Vacant(e) if groups_len < window.window_impl.max_groups() => {
            let k = group_str.to_string();
            let aggrs = aggregates.to_vec();
            let v = last_groups.remove(group_str).unwrap_or_else(|| {
                GroupData {
                    window: window_impl.clone(),
                    aggrs,
                    group: group_value.clone_static(),
                    id: idgen.next_id(), // after all this is a new event
                }
            });
            e.insert(k, v)
        }
        RawEntryMut::Vacant(_) => {
            return Err(max_groups_reached(
                window.window_impl.max_groups(),
                group_str,
            ))
        }
    };
    Ok(this_group)
}

impl Operator for TrickleSelect {
    #[allow(
        mutable_transmutes,
        clippy::transmute_ptr_to_ptr,
        clippy::too_many_lines
    )]
    fn on_event(
        &mut self,
        _uid: u64,
        _port: &str,
        state: &mut Value<'static>,
        event: Event,
    ) -> Result<EventAndInsights> {
        let opts = Self::opts();
        // We guarantee at compile time that select in itself can't have locals, so this is safe

        // NOTE We are unwrapping our rental wrapped stmt

        // TODO: reason about soundness
        let SelectStmt {
            stmt,
            aggregates,
            aggregate_scratches,
            consts,
            locals,
            node_meta,
        }: &mut SelectStmt = unsafe { mem::transmute(self.select.suffix()) };
        let local_stack = tremor_script::interpreter::LocalStack::with_size(*locals);
        consts.window = Value::null();
        consts.group = Value::null();
        consts.args = Value::null();
        // TODO avoid origin_uri clone here
        let ctx = EventContext::new(event.ingest_ns, event.origin_uri.clone());

        //
        // Before any select processing, we filter by where clause
        //
        if let Some(guard) = &stmt.maybe_where {
            let (unwind_event, event_meta) = event.data.parts();
            let env = Env {
                context: &ctx,
                consts: &consts,
                aggrs: &NO_AGGRS,
                meta: &node_meta,
                recursion_limit: tremor_script::recursion_limit(),
            };
            let test = guard.run(opts, &env, unwind_event, state, event_meta, &local_stack)?;
            if let Some(test) = test.as_bool() {
                if !test {
                    return Ok(EventAndInsights::default());
                };
            } else {
                let s: &Select = &stmt;
                return Err(tremor_script::errors::query_guard_not_bool_err(
                    s, guard, &test, &node_meta,
                )
                .into());
            };
        }

        let mut events = vec![];

        let mut group_values = {
            let data = event.data.suffix();
            if let Some(group_by) = &stmt.maybe_group_by {
                group_by.generate_groups(&ctx, data.value(), state, &node_meta, data.meta())?
            } else {
                vec![]
            }
        };

        //
        // select without group by or windows
        // event stays the same, only the value might change based on select clause
        // and we might drop it altogether based on having clause.
        //
        if self.windows.is_empty() && group_values.is_empty() {
            let group_value = Value::from(vec![()]);
            let group_str = sorted_serialize(&group_value)?;

            let data = event.data.suffix();
            // This is sound since we're transmuting immutable to mutable
            // We can't specify the 'lifetime' of the event or it would be
            // `&'run mut Value<'event>`
            let unwind_event = unsafe { data.force_value_mut() };
            let event_meta = data.meta();

            consts.group = group_value.clone_static();
            consts.group.push(group_str)?;

            let env = Env {
                context: &ctx,
                consts: &consts,
                aggrs: &NO_AGGRS,
                meta: &node_meta,
                recursion_limit: tremor_script::recursion_limit(),
            };
            let value =
                stmt.target
                    .run(opts, &env, unwind_event, state, event_meta, &local_stack)?;

            let result = value.into_owned();
            // evaluate having clause, if one exists
            if let Some(guard) = &stmt.maybe_having {
                let test = guard.run(opts, &env, &result, state, &NULL, &local_stack)?;
                if let Some(test) = test.as_bool() {
                    if !test {
                        return Ok(EventAndInsights::default());
                    }
                } else {
                    let s: &Select = &stmt;
                    return Err(tremor_script::errors::query_guard_not_bool_err(
                        s, guard, &test, &node_meta,
                    )
                    .into());
                }
            }
            *unwind_event = result;
            // We manually drop this here to inform rust that we no longer
            // borrow values from event
            drop(group_values);
            return Ok(event.into());
        }
        if group_values.is_empty() {
            group_values.push(vec![Value::null()])
        };

        // Handle eviction
        // retire the group data that didnt receive an event in `eviction_ns()` nanoseconds
        // if no event came after `2 * eviction_ns()` this group is finally cleared out
        for window in &mut self.windows {
            if let Some(eviction_ns) = window.window_impl.eviction_ns() {
                if window.next_swap < event.ingest_ns {
                    window.next_swap = event.ingest_ns + eviction_ns;
                    window.last_dims.groups.clear();
                    std::mem::swap(&mut window.dims, &mut window.last_dims);
                }
            }
        }

        let group_values: Vec<Value> = group_values.into_iter().map(Value::Array).collect();

        let scratches = aggregate_scratches.as_mut();

        for group_value in group_values {
            let group_str = sorted_serialize(&group_value)?;

            consts.group = group_value.clone_static();
            consts.group.push(group_str.clone())?;

            match self.windows.len() {
                0 => {
                    // group by without windows
                    // otherwise we just pass it through the select portion of the statement

                    // evaluate select clause
                    let env = Env {
                        context: &ctx,
                        consts: &consts,
                        aggrs: &NO_AGGRS,
                        meta: &node_meta,
                        recursion_limit: tremor_script::recursion_limit(),
                    };
                    // here we have a 1:1 between input and output event and thus can keep the origin uri
                    if let Some(port_and_event) = stry!(execute_select_and_having(
                        &stmt,
                        &node_meta,
                        opts,
                        &local_stack,
                        state,
                        &env,
                        &mut self.event_id_gen,
                        &event,
                        event.origin_uri.clone(),
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
                    let window = &mut self.windows[0];
                    consts.window = Value::from(window.name.to_string());

                    // get current window group
                    let this_group = stry!(get_or_create_group(
                        window,
                        &mut self.event_id_gen,
                        aggregates,
                        &group_str,
                        &group_value,
                    ));

                    let window_event = stry!(this_group.window.on_event(&event));
                    if window_event.emit && !window_event.include {
                        // push
                        let env = Env {
                            context: &ctx,
                            consts: &consts,
                            aggrs: &this_group.aggrs,
                            meta: &node_meta,
                            recursion_limit: tremor_script::recursion_limit(),
                        };
                        if let Some(port_and_event) = stry!(execute_select_and_having(
                            &stmt,
                            &node_meta,
                            opts,
                            &local_stack,
                            state,
                            &env,
                            &mut self.event_id_gen,
                            &event,
                            None,
                        )) {
                            events.push(port_and_event);
                        };
                        // re-initialize aggr state for new window
                        for aggr in &mut this_group.aggrs {
                            aggr.invocable.init();
                        }
                    }

                    // accumulate

                    // accumulate the current event
                    let env = Env {
                        context: &ctx,
                        consts: &consts,
                        aggrs: &NO_AGGRS,
                        meta: &node_meta,
                        recursion_limit: tremor_script::recursion_limit(),
                    };
                    stry!(accumulate(
                        opts,
                        &node_meta,
                        &env,
                        &local_stack,
                        state,
                        this_group,
                        &event,
                    ));

                    if window_event.emit && window_event.include {
                        // push
                        let env = Env {
                            context: &ctx,
                            consts: &consts,
                            aggrs: &this_group.aggrs,
                            meta: &node_meta,
                            recursion_limit: tremor_script::recursion_limit(),
                        };
                        if let Some(port_and_event) = stry!(execute_select_and_having(
                            &stmt,
                            &node_meta,
                            opts,
                            &local_stack,
                            state,
                            &env,
                            &mut self.event_id_gen,
                            &event,
                            None,
                        )) {
                            events.push(port_and_event);
                        };
                        // re-initialize aggr state for new window
                        for aggr in &mut this_group.aggrs {
                            aggr.invocable.init();
                        }
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
                    //      this.aggrs.merge(scratch);
                    //      push event
                    //      swap(scratch, aggrs)
                    //      aggrs.init() // init next window
                    //   else
                    //      push event
                    //      if first {
                    //          swap(scratch, aggrs)
                    //          aggrs.init() // no merge needed
                    //      } else {
                    //          local_scratch = aggregates.clone()
                    //          swap(scratch, local_scratch) // store old aggrs state from last window
                    //          swap(aggrs, scratch) // store aggrs state from this before init
                    //          aggrs.init()
                    //          this.aggrs.merge(local_scratch);
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
                    // event | size 2 | tilt | size 3 |
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

                    // we need scratches for multiple window handling
                    if let Some((scratch1, scratch2)) = scratches {
                        // gather window events
                        let mut emit_window_events = Vec::with_capacity(self.windows.len());
                        let step1_iter = self.windows.iter_mut();
                        for window in step1_iter {
                            consts.window = Value::from(window.name.to_string());
                            // get current window group
                            let this_group = stry!(get_or_create_group(
                                window,
                                &mut self.event_id_gen,
                                aggregates,
                                &group_str,
                                &group_value,
                            ));
                            let window_event = stry!(this_group.window.on_event(&event));
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
                        } else if let Some(first_window) = self.windows.first_mut() {
                            consts.window = Value::from(first_window.name.to_string());

                            // get current window group
                            let this_group = stry!(get_or_create_group(
                                first_window,
                                &mut self.event_id_gen,
                                aggregates,
                                &group_str,
                                &group_value,
                            ));

                            // accumulate the current event
                            let env = Env {
                                context: &ctx,
                                consts: &consts,
                                aggrs: &NO_AGGRS,
                                meta: &node_meta,
                                recursion_limit: tremor_script::recursion_limit(),
                            };
                            stry!(accumulate(
                                opts,
                                &node_meta,
                                &env,
                                &local_stack,
                                state,
                                this_group,
                                &event,
                            ));
                            false
                        } else {
                            // should not happen
                            false
                        };

                        // merge previous aggr state, push window event out, re-initialize for new window
                        let mut first = true;
                        let emit_window_iter =
                            emit_window_events.iter().zip(self.windows.iter_mut());
                        for (window_event, window) in emit_window_iter {
                            consts.window = Value::from(window.name.to_string());

                            // get current window group
                            let this_group = stry!(get_or_create_group(
                                window,
                                &mut self.event_id_gen,
                                aggregates,
                                &group_str,
                                &group_value,
                            ));

                            if window_event.include {
                                // add this event to the aggr state **BEFORE** emit and propagate to next windows
                                // merge with scratch
                                if !first {
                                    for (this, prev) in
                                        this_group.aggrs.iter_mut().zip(scratch1.iter())
                                    {
                                        this.invocable.merge(&prev.invocable).map_err(|e| {
                                            let r: Option<&Registry> = None;
                                            e.into_err(prev, prev, r, &node_meta)
                                        })?;
                                    }
                                }
                                // push event
                                let env = Env {
                                    context: &ctx,
                                    consts: &consts,
                                    aggrs: &this_group.aggrs,
                                    meta: &node_meta,
                                    recursion_limit: tremor_script::recursion_limit(),
                                };

                                if let Some(port_and_event) = stry!(execute_select_and_having(
                                    &stmt,
                                    &node_meta,
                                    opts,
                                    &local_stack,
                                    state,
                                    &env,
                                    &mut self.event_id_gen,
                                    &event,
                                    None,
                                )) {
                                    events.push(port_and_event);
                                };

                                // swap(aggrs, scratch) - store state before init to scratch as we need it for the next window
                                std::mem::swap(scratch1, &mut this_group.aggrs);
                                // aggrs.init()
                                for aggr in &mut this_group.aggrs {
                                    aggr.invocable.init();
                                }
                            } else {
                                // add this event to the aggr state **AFTER** emit and propagate to next windows

                                // push event
                                let env = Env {
                                    context: &ctx,
                                    consts: &consts,
                                    aggrs: &this_group.aggrs,
                                    meta: &node_meta,
                                    recursion_limit: tremor_script::recursion_limit(),
                                };
                                if let Some(port_and_event) = stry!(execute_select_and_having(
                                    &stmt,
                                    &node_meta,
                                    opts,
                                    &local_stack,
                                    state,
                                    &env,
                                    &mut self.event_id_gen,
                                    &event,
                                    None,
                                )) {
                                    events.push(port_and_event);
                                };

                                if first {
                                    // first window
                                    //          swap(scratch, aggrs)
                                    std::mem::swap(scratch1, &mut this_group.aggrs); // store current state before init
                                                                                     //          aggrs.init()
                                    for aggr in &mut this_group.aggrs {
                                        aggr.invocable.init();
                                    }
                                } else {
                                    // not first window
                                    // store old aggrs state from last window into scratch2
                                    std::mem::swap(scratch1, scratch2);
                                    //          swap(aggrs, scratch) // store aggrs state before init() into scratch1 for next window
                                    std::mem::swap(&mut this_group.aggrs, scratch1);
                                    //          aggrs.init()
                                    for aggr in &mut this_group.aggrs {
                                        aggr.invocable.init();
                                    }
                                    //          merge state from last wiundow into this one
                                    for (this, prev) in
                                        this_group.aggrs.iter_mut().zip(scratch2.iter())
                                    {
                                        this.invocable.merge(&prev.invocable).map_err(|e| {
                                            let r: Option<&Registry> = None;
                                            e.into_err(prev, prev, r, &node_meta)
                                        })?;
                                    }
                                }
                            }

                            first = false;
                        }

                        // execute the pending accumulate (if window_event.include == false of first window)
                        if pending_accumulate {
                            if let Some(first_window) = self.windows.first_mut() {
                                consts.window = Value::from(first_window.name.to_string());
                                // get current window group
                                let this_group = stry!(get_or_create_group(
                                    first_window,
                                    &mut self.event_id_gen,
                                    aggregates,
                                    &group_str,
                                    &group_value,
                                ));

                                // accumulate the current event
                                let env = Env {
                                    context: &ctx,
                                    consts: &consts,
                                    aggrs: &NO_AGGRS,
                                    meta: &node_meta,
                                    recursion_limit: tremor_script::recursion_limit(),
                                };
                                stry!(accumulate(
                                    opts,
                                    &node_meta,
                                    &env,
                                    &local_stack,
                                    state,
                                    this_group,
                                    &event,
                                ));
                            }
                        }

                        // merge state from last emit window into next non-emit window if there is any
                        if !emit_window_events.is_empty() {
                            if let Some(non_emit_window) =
                                self.windows.get_mut(emit_window_events.len())
                            {
                                consts.window = Value::from(non_emit_window.name.to_string());

                                // get current window group
                                let this_group = stry!(get_or_create_group(
                                    non_emit_window,
                                    &mut self.event_id_gen,
                                    aggregates,
                                    &group_str,
                                    &group_value,
                                ));
                                for (this, prev) in this_group.aggrs.iter_mut().zip(scratch1.iter())
                                {
                                    this.invocable.merge(&prev.invocable).map_err(|e| {
                                        let r: Option<&Registry> = None;
                                        e.into_err(prev, prev, r, &node_meta)
                                    })?;
                                }
                            }
                        }
                    } else {
                        // if this happens, we fucked up internally.
                        return Err("Internal Error. Cannot execute select with multiple windows, no scratches available. This is bad!".into());
                    }
                }
            }
        }
        Ok(events.into())
    }

    #[allow(
        mutable_transmutes,
        clippy::clippy::too_many_lines,
        clippy::transmute_ptr_to_ptr
    )]
    fn on_signal(
        &mut self,
        _uid: u64,
        state: &Value<'static>, // we only reference state here immutably, no chance to change it here
        signal: &mut Event,
    ) -> Result<EventAndInsights> {
        // we only react on ticks and when we have windows
        if signal.kind == Some(SignalKind::Tick) && !self.windows.is_empty() {
            // Handle eviction
            // retire the group data that didnt receive an event in `eviction_ns()` nanoseconds
            // if no event came after `2 * eviction_ns()` this group is finally cleared out
            for window in &mut self.windows {
                if let Some(eviction_ns) = window.window_impl.eviction_ns() {
                    if window.next_swap < signal.ingest_ns {
                        window.next_swap = signal.ingest_ns + eviction_ns;
                        window.last_dims.groups.clear();
                        std::mem::swap(&mut window.dims, &mut window.last_dims);
                    }
                }
            }

            let ingest_ns = signal.ingest_ns;
            let opts = Self::opts();
            // TODO: reason about soundness
            let SelectStmt {
                stmt,
                aggregates,
                aggregate_scratches,
                consts,
                locals,
                node_meta,
            }: &mut SelectStmt = unsafe { mem::transmute(self.select.suffix()) };
            let mut res = EventAndInsights::default();
            // artificial event
            let artificial_event = Event {
                id: signal.id.clone(),
                ingest_ns,
                data: (Value::null(), Value::null()).into(),
                ..Event::default()
            };
            let local_stack = tremor_script::interpreter::LocalStack::with_size(*locals);

            consts.window = Value::null();
            consts.group = Value::null();
            consts.args = Value::null();

            match self.windows.len() {
                0 => {} // we shouldnt get here
                1 => {
                    let ctx = EventContext::new(signal.ingest_ns, None);
                    for window in &mut self.windows {
                        consts.window = Value::from(window.name.to_string());
                        // iterate all groups, including the ones from last_dims
                        for (group_str, group_data) in window
                            .dims
                            .groups
                            .iter_mut()
                            .chain(window.last_dims.groups.iter_mut())
                        {
                            consts.group = group_data.group.clone_static();
                            consts.group.push(group_str.clone())?;

                            let window_event = stry!(group_data.window.on_tick(ingest_ns));
                            if window_event.emit {
                                // evaluate the event and push
                                let env = Env {
                                    context: &ctx,
                                    consts: &consts,
                                    aggrs: &group_data.aggrs,
                                    meta: &node_meta,
                                    recursion_limit: tremor_script::recursion_limit(),
                                };
                                let executed = stry!(execute_select_and_having(
                                    &stmt,
                                    node_meta,
                                    opts,
                                    &local_stack,
                                    state,
                                    &env,
                                    &mut self.event_id_gen,
                                    &artificial_event,
                                    None,
                                ));
                                if let Some(port_and_event) = executed {
                                    res.events.push(port_and_event);
                                }

                                // init
                                for aggr in &mut group_data.aggrs {
                                    aggr.invocable.init();
                                }
                            }
                        }
                    }
                }
                _ => {
                    // complicated logic
                    // get a set of groups
                    //   for each window
                    //      get the corresponding group data
                    //      on_event ... do the magic dance with scratches etc
                    // we need scratches for multiple window handling

                    let ctx = EventContext::new(ingest_ns, None);

                    // we get away with just checking the first window as there can be no group in a further window
                    // that is not also in the first.
                    let distinct_groups: Vec<(&String, &Value)> = self
                        .windows
                        .get(0)
                        .iter()
                        .flat_map(|window| {
                            (*window)
                                .dims
                                .groups
                                .iter()
                                .chain(window.last_dims.groups.iter())
                        })
                        .map(|(group_str, group_data)| {
                            // trick the borrow checker, so we can reference self.windows mutably below
                            // this is safe, as we only ever work on group_data.window and group_data.aggregates
                            // we might move a group_data struct from one map to another, but there is no deletion
                            // ALLOW: we never touch neither the group value nor the group_str during the fiddling below
                            let g_str: &'static String = unsafe { std::mem::transmute(group_str) };
                            // ALLOW: we never touch neither the group value nor the group_str during the fiddling below
                            let group_value: &'static Value =
                                unsafe { std::mem::transmute(&group_data.group) }; // lose the reference to window
                            (g_str, group_value)
                        })
                        .collect();

                    for (group_str, group_value) in distinct_groups {
                        consts.group = group_value.clone_static();
                        consts.group.push(group_str.clone())?;

                        if let Some((scratch1, scratch2)) = aggregate_scratches.as_mut() {
                            // gather window events
                            let mut emit_window_events = Vec::with_capacity(self.windows.len());
                            let step1_iter = self.windows.iter_mut();
                            for window in step1_iter {
                                consts.window = Value::from(window.name.to_string());
                                // get current window group
                                let this_group = stry!(get_or_create_group(
                                    window,
                                    &mut self.event_id_gen,
                                    aggregates,
                                    group_str,
                                    group_value,
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
                            let emit_window_iter =
                                emit_window_events.iter().zip(self.windows.iter_mut());
                            for (window_event, window) in emit_window_iter {
                                consts.window = Value::from(window.name.to_string());

                                // get current window group
                                let this_group = stry!(get_or_create_group(
                                    window,
                                    &mut self.event_id_gen,
                                    aggregates,
                                    group_str,
                                    group_value,
                                ));

                                if window_event.include {
                                    // add this event to the aggr state **BEFORE** emit and propagate to next windows
                                    // merge with scratch
                                    if !first {
                                        for (this, prev) in
                                            this_group.aggrs.iter_mut().zip(scratch1.iter())
                                        {
                                            this.invocable.merge(&prev.invocable).map_err(|e| {
                                                let r: Option<&Registry> = None;
                                                e.into_err(prev, prev, r, &node_meta)
                                            })?;
                                        }
                                    }
                                    // push event
                                    let env = Env {
                                        context: &ctx,
                                        consts: &consts,
                                        aggrs: &this_group.aggrs,
                                        meta: &node_meta,
                                        recursion_limit: tremor_script::recursion_limit(),
                                    };

                                    if let Some(port_and_event) = stry!(execute_select_and_having(
                                        &stmt,
                                        &node_meta,
                                        opts,
                                        &local_stack,
                                        state,
                                        &env,
                                        &mut self.event_id_gen,
                                        &artificial_event,
                                        None,
                                    )) {
                                        res.events.push(port_and_event);
                                    };

                                    // swap(aggrs, scratch) - store state before init to scratch as we need it for the next window
                                    std::mem::swap(scratch1, &mut this_group.aggrs);
                                    // aggrs.init()
                                    for aggr in &mut this_group.aggrs {
                                        aggr.invocable.init();
                                    }
                                } else {
                                    // add this event to the aggr state **AFTER** emit and propagate to next windows

                                    // push event
                                    let env = Env {
                                        context: &ctx,
                                        consts: &consts,
                                        aggrs: &this_group.aggrs,
                                        meta: &node_meta,
                                        recursion_limit: tremor_script::recursion_limit(),
                                    };
                                    if let Some(port_and_event) = stry!(execute_select_and_having(
                                        &stmt,
                                        &node_meta,
                                        opts,
                                        &local_stack,
                                        state,
                                        &env,
                                        &mut self.event_id_gen,
                                        &artificial_event,
                                        None,
                                    )) {
                                        res.events.push(port_and_event);
                                    };

                                    if first {
                                        // first window
                                        //          swap(scratch, aggrs)
                                        std::mem::swap(scratch1, &mut this_group.aggrs); // store current state before init
                                                                                         //          aggrs.init()
                                        for aggr in &mut this_group.aggrs {
                                            aggr.invocable.init();
                                        }
                                    } else {
                                        // not first window
                                        // store old aggrs state from last window into scratch2
                                        std::mem::swap(scratch1, scratch2);
                                        //          swap(aggrs, scratch) // store aggrs state before init() into scratch1 for next window
                                        std::mem::swap(&mut this_group.aggrs, scratch1);
                                        //          aggrs.init()
                                        for aggr in &mut this_group.aggrs {
                                            aggr.invocable.init();
                                        }
                                        //          merge state from last wiundow into this one
                                        for (this, prev) in
                                            this_group.aggrs.iter_mut().zip(scratch2.iter())
                                        {
                                            this.invocable.merge(&prev.invocable).map_err(|e| {
                                                let r: Option<&Registry> = None;
                                                e.into_err(prev, prev, r, &node_meta)
                                            })?;
                                        }
                                    }
                                }

                                first = false;
                            }

                            // merge state from last emit window into next non-emit window if there is any
                            if !emit_window_events.is_empty() {
                                if let Some(non_emit_window) =
                                    self.windows.get_mut(emit_window_events.len())
                                {
                                    consts.window = Value::from(non_emit_window.name.to_string());

                                    // get current window group
                                    let this_group = stry!(get_or_create_group(
                                        non_emit_window,
                                        &mut self.event_id_gen,
                                        aggregates,
                                        group_str,
                                        group_value,
                                    ));
                                    for (this, prev) in
                                        this_group.aggrs.iter_mut().zip(scratch1.iter())
                                    {
                                        this.invocable.merge(&prev.invocable).map_err(|e| {
                                            let r: Option<&Registry> = None;
                                            e.into_err(prev, prev, r, &node_meta)
                                        })?;
                                    }
                                }
                            }
                        } else {
                            // if this happens, we fucked up internally.
                            return Err("Internal Error. Cannot execute select with multiple windows, no scratches available. This is bad!".into());
                        }
                    }
                }
            }
            // lets reset the
            Ok(res)
        } else {
            Ok(EventAndInsights::default())
        }
    }

    fn handles_signal(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod test {
    #![allow(clippy::float_cmp)]
    use crate::query::window_decl_to_impl;

    use super::*;
    use rental::RentalError;
    use simd_json::{json, StaticNode};
    use tremor_script::{ast::Consts, Value};
    use tremor_script::{ast::Stmt, Query};
    use tremor_script::{
        ast::{self, Ident, ImutExpr, Literal},
        path::ModulePath,
    };

    fn test_target<'test>() -> ast::ImutExpr<'test> {
        let target: ast::ImutExpr<'test> = ImutExpr::from(ast::Literal {
            mid: 0,
            value: Value::from(42),
        });
        target
    }

    fn test_stmt(target: ast::ImutExpr) -> ast::Select {
        tremor_script::ast::Select {
            mid: 0,
            from: (Ident::from("in"), Ident::from("out")),
            into: (Ident::from("out"), Ident::from("in")),
            target,
            maybe_where: Some(ImutExpr::from(ast::Literal {
                mid: 0,
                value: Value::from(true),
            })),
            windows: vec![],
            maybe_group_by: None,
            maybe_having: None,
        }
    }

    fn test_query(stmt: ast::Stmt) -> ast::Query {
        ast::Query {
            stmts: vec![stmt.clone()],
            node_meta: ast::NodeMetas::new(Vec::new()),
            windows: HashMap::new(),
            scripts: HashMap::new(),
            operators: HashMap::new(),
            config: HashMap::new(),
        }
    }

    fn test_event(s: u64) -> Event {
        Event {
            id: (0, 0, s).into(),
            ingest_ns: s * 1_000_000_000,
            data: Value::from(json!({
               "h2g2" : 42,
            }))
            .into(),
            ..Event::default()
        }
    }

    use std::sync::Arc;

    fn test_select(
        uid: u64,
        stmt: tremor_script::query::StmtRentalWrapper,
    ) -> Result<TrickleSelect> {
        let groups = Dims::new(stmt.stmt.clone());
        let windows = vec![
            (
                "w15s".into(),
                TumblingWindowOnTime {
                    emit_empty_windows: false,
                    ttl: None,
                    max_groups: WindowImpl::DEFAULT_MAX_GROUPS,
                    interval: 15_000_000_000,
                    next_window: None,
                    events: 0,
                    script: None,
                }
                .into(),
            ),
            (
                "w30s".into(),
                TumblingWindowOnTime {
                    emit_empty_windows: false,
                    ttl: None,
                    max_groups: WindowImpl::DEFAULT_MAX_GROUPS,
                    interval: 30_000_000_000,
                    next_window: None,
                    events: 0,
                    script: None,
                }
                .into(),
            ),
        ];
        let id = "select".to_string();
        TrickleSelect::with_stmt(uid, id, &groups, windows, &stmt)
    }

    fn try_enqueue(
        op: &mut TrickleSelect,
        event: Event,
    ) -> Result<Option<(Cow<'static, str>, Event)>> {
        let mut state = Value::null();
        let mut action = op.on_event(0, "in", &mut state, event)?;
        let first = action.events.pop();
        if action.events.is_empty() {
            Ok(first)
        } else {
            Ok(None)
        }
    }

    fn try_enqueue_two(
        op: &mut TrickleSelect,
        event: Event,
    ) -> Result<Option<[(Cow<'static, str>, Event); 2]>> {
        let mut state = Value::null();
        let mut action = op.on_event(0, "in", &mut state, event)?;
        let r = action
            .events
            .pop()
            .and_then(|second| Some([action.events.pop()?, second]));
        if action.events.is_empty() {
            Ok(r)
        } else {
            Ok(None)
        }
    }

    fn parse_query(
        file_name: String,
        query: &str,
    ) -> Result<crate::op::trickle::select::TrickleSelect> {
        let reg = tremor_script::registry();
        let aggr_reg = tremor_script::aggr_registry();
        let module_path = tremor_script::path::load();
        let cus = vec![];
        let query = tremor_script::query::Query::parse(
            &module_path,
            &file_name,
            query,
            cus,
            &reg,
            &aggr_reg,
        )
        .map_err(tremor_script::errors::CompilerError::error)?;
        let stmt_rental = tremor_script::query::StmtRental::try_new(Arc::new(query.clone()), |q| {
            q.suffix()
                .stmts
                .get(0)
                .cloned()
                .ok_or_else(|| Error::from("Invalid query"))
        })
        .map_err(|e| e.0)?;
        let stmt = tremor_script::query::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };
        Ok(test_select(1, stmt)?)
    }

    #[test]
    fn test_sum() -> Result<()> {
        let mut op = parse_query(
            "test.trickle".to_string(),
            "select aggr::stats::sum(event.h2g2) from in[w15s, w30s] into out;",
        )?;
        assert!(try_enqueue(&mut op, test_event(0))?.is_none());
        assert!(try_enqueue(&mut op, test_event(1))?.is_none());
        let (out, event) =
            try_enqueue(&mut op, test_event(15))?.expect("no event emitted after aggregation");
        assert_eq!("out", out);

        assert_eq!(*event.data.suffix().value(), 84.0);
        Ok(())
    }

    #[test]
    fn test_count() -> Result<()> {
        let mut op = parse_query(
            "test.trickle".to_string(),
            "select aggr::stats::count() from in[w15s, w30s] into out;",
        )?;
        assert!(try_enqueue(&mut op, test_event(0))?.is_none());
        assert!(try_enqueue(&mut op, test_event(1))?.is_none());
        let (out, event) = try_enqueue(&mut op, test_event(15))?.expect("no event");
        assert_eq!("out", out);

        assert_eq!(*event.data.suffix().value(), 2);
        Ok(())
    }

    fn select_stmt_from_query(query_str: &str) -> Result<TrickleSelect> {
        let reg = tremor_script::registry();
        let aggr_reg = tremor_script::aggr_registry();
        let module_path = tremor_script::path::load();
        let cus = vec![];
        let query = tremor_script::query::Query::parse(
            &module_path,
            "fake",
            query_str,
            cus,
            &reg,
            &aggr_reg,
        )
        .map_err(tremor_script::errors::CompilerError::error)?;
        let window_decls: Vec<WindowDecl<'_>> = query
            .suffix()
            .stmts
            .iter()
            .filter_map(|stmt| match stmt {
                Stmt::WindowDecl(wd) => Some(wd.as_ref().clone()),
                _ => None,
            })
            .collect();
        let stmt_rental = tremor_script::query::StmtRental::try_new(Arc::new(query.clone()), |q| {
            q.suffix()
                .stmts
                .iter()
                .find(|stmt| matches!(*stmt, Stmt::Select(_)))
                .cloned()
                .ok_or_else(|| Error::from("Invalid query, expected only 1 select statement"))
        })
        .map_err(|e: RentalError<Error, Arc<Query>>| e.0)?;
        let stmt = tremor_script::query::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };
        let windows: Vec<(String, WindowImpl)> = window_decls
            .iter()
            .enumerate()
            .map(|(i, window_decl)| {
                (
                    i.to_string(),
                    window_decl_to_impl(window_decl, &stmt).unwrap(), // yes, indeed!
                )
            })
            .collect();
        let groups = Dims::new(stmt.stmt.clone());

        let id = "select".to_string();
        Ok(TrickleSelect::with_stmt(42, id, &groups, windows, &stmt)?)
    }

    fn test_tick(ns: u64) -> Event {
        Event {
            id: EventId::new(1, 1, ns),
            kind: Some(SignalKind::Tick),
            ingest_ns: ns,
            ..Event::default()
        }
    }

    #[test]
    fn select_single_win_with_script_on_signal() -> Result<()> {
        let mut select = select_stmt_from_query(
            r#"
        define tumbling window window1
        with
            interval = 5
        script
            event.time
        end;
        select aggr::stats::count() from in[window1] group by event.g into out having event > 0;
        "#,
        )?;
        let uid = 42;
        let mut state = Value::null();
        let mut tick1 = test_tick(1);
        let mut eis = select.on_signal(uid, &state, &mut tick1)?;
        assert!(eis.insights.is_empty());
        assert_eq!(0, eis.events.len());

        let event = Event {
            id: (1, 1, 300).into(),
            ingest_ns: 1_000,
            data: Value::from(json!({
               "time" : 4,
               "g": "group"
            }))
            .into(),
            ..Event::default()
        };
        eis = select.on_event(uid, "IN", &mut state, event)?;
        assert!(eis.insights.is_empty());
        assert_eq!(0, eis.events.len());

        // no emit on signal, although window interval would be passed
        let mut tick2 = test_tick(10);
        eis = select.on_signal(uid, &state, &mut tick2)?;
        assert!(eis.insights.is_empty());
        assert_eq!(0, eis.events.len());

        // only emit on event
        let event = Event {
            id: (1, 1, 300).into(),
            ingest_ns: 3_000,
            data: Value::from(json!({
               "time" : 11,
               "g": "group"
            }))
            .into(),
            ..Event::default()
        };
        eis = select.on_event(uid, "IN", &mut state, event)?;
        assert!(eis.insights.is_empty());
        assert_eq!(1, eis.events.len());
        assert_eq!("1", sorted_serialize(eis.events[0].1.data.parts().0)?);
        Ok(())
    }

    #[test]
    fn select_single_win_on_signal() -> Result<()> {
        let mut select = select_stmt_from_query(
            r#"
        define tumbling window window1
        with
            interval = 2
        end;
        select aggr::win::collect_flattened(event) from in[window1] group by event.g into out;
        "#,
        )?;
        let uid = 42;
        let mut state = Value::null();
        let mut tick1 = test_tick(1);
        let mut eis = select.on_signal(uid, &state, &mut tick1)?;
        assert!(eis.insights.is_empty());
        assert_eq!(0, eis.events.len());

        let event = Event {
            id: (1, 1, 300).into(),
            ingest_ns: 2,
            data: Value::from(json!({
               "g": "group"
            }))
            .into(),
            ..Event::default()
        };
        eis = select.on_event(uid, "IN", &mut state, event)?;
        assert!(eis.insights.is_empty());
        assert_eq!(0, eis.events.len());

        // no emit yet
        let mut tick2 = test_tick(3);
        eis = select.on_signal(uid, &state, &mut tick2)?;
        assert!(eis.insights.is_empty());
        assert_eq!(0, eis.events.len());

        // now emit
        let mut tick3 = test_tick(4);
        eis = select.on_signal(uid, &state, &mut tick3)?;
        assert!(eis.insights.is_empty());
        assert_eq!(1, eis.events.len());
        assert_eq!(
            r#"[{"g":"group"}]"#,
            sorted_serialize(eis.events[0].1.data.parts().0)?
        );
        Ok(())
    }

    #[test]
    fn select_multiple_wins_on_signal() -> Result<()> {
        let mut select = select_stmt_from_query(
            r#"
        define tumbling window window1
        with
            interval = 100
        end;
        define tumbling window window2
        with
            size = 2
        end;
        select aggr::win::collect_flattened(event) from in[window1, window2] group by event.cat into out;
        "#,
        )?;
        let uid = 42;
        let mut state = Value::null();
        let mut tick1 = test_tick(1);
        let mut eis = select.on_signal(uid, &state, &mut tick1)?;
        assert!(eis.insights.is_empty());
        assert_eq!(0, eis.events.len());

        let mut tick2 = test_tick(100);
        eis = select.on_signal(uid, &state, &mut tick2)?;
        assert!(eis.insights.is_empty());
        assert_eq!(0, eis.events.len());

        // we have no groups, so no emit yet
        let mut tick3 = test_tick(201);
        eis = select.on_signal(uid, &state, &mut tick3)?;
        assert!(eis.insights.is_empty());
        assert_eq!(0, eis.events.len());

        // insert first event
        let event = Event {
            id: (1, 1, 300).into(),
            ingest_ns: 300,
            data: Value::from(json!({
               "cat" : 42,
            }))
            .into(),
            ..Event::default()
        };
        eis = select.on_event(uid, "IN", &mut state, event)?;
        assert!(eis.insights.is_empty());
        assert_eq!(0, eis.events.len());

        // finish the window with the next tick
        let mut tick4 = test_tick(401);
        eis = select.on_signal(uid, &state, &mut tick4)?;
        assert!(eis.insights.is_empty());
        assert_eq!(1, eis.events.len());
        let (_port, event) = eis.events.remove(0);
        assert_eq!(r#"[{"cat":42}]"#, sorted_serialize(event.data.parts().0)?);

        let mut tick5 = test_tick(499);
        eis = select.on_signal(uid, &state, &mut tick5)?;
        assert!(eis.insights.is_empty());
        assert_eq!(0, eis.events.len());

        Ok(())
    }

    #[test]
    fn count_tilt() -> Result<()> {
        // Windows are 15s and 30s
        let mut op = select_stmt_from_query(
            r#"
        define tumbling window w15s
        with
          interval = 15 * 1000000000
        end;
        define tumbling window w30s
        with
          interval = 30 * 1000000000
        end;
        select aggr::stats::count() from in [w15s, w30s] into out;
        "#,
        )?;

        // Insert two events prior to 15
        assert!(try_enqueue(&mut op, test_event(0))?.is_none());
        assert!(try_enqueue(&mut op, test_event(1))?.is_none());
        // Add one event at 15, this flushes the prior two and set this to one.
        // This is the first time the second frame sees an event so it's timer
        // starts at 15.
        let (out, event) = try_enqueue(&mut op, test_event(15))?.expect("no event 1");
        assert_eq!("out", out);
        assert_eq!(*event.data.suffix().value(), 2);
        // Add another event prior to 30
        assert!(try_enqueue(&mut op, test_event(16))?.is_none());
        // This emits only the initial event since the rollup
        // will only be emitted once it gets the first event
        // of the next window
        let (out, event) = try_enqueue(&mut op, test_event(30))?.expect("no event 2");
        assert_eq!("out", out);
        assert_eq!(*event.data.suffix().value(), 2);
        // Add another event prior to 45 to the first window
        assert!(try_enqueue(&mut op, test_event(31))?.is_none());

        // At 45 the 15s window emits the rollup with the previous data
        let [(out1, event1), (out2, event2)] =
            try_enqueue_two(&mut op, test_event(45))?.expect("no event 3");

        assert_eq!("out", out1);
        assert_eq!("out", out2);
        assert_eq!(*event1.data.suffix().value(), 2);
        assert_eq!(*event2.data.suffix().value(), 4);
        assert!(try_enqueue(&mut op, test_event(46))?.is_none());

        // Add 60 only the 15s window emits
        let (out, event) = try_enqueue(&mut op, test_event(60))?.expect("no event 4");
        assert_eq!("out", out);
        assert_eq!(*event.data.suffix().value(), 2);
        Ok(())
    }

    #[test]
    fn select_nowin_nogrp_nowhr_nohav() -> Result<()> {
        let target = test_target();
        let stmt_ast = test_stmt(target);

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::query::QueryRental::new(script_box, |_| {
            test_query(stmt_ast.clone())
        }));

        let query = tremor_script::query::Query {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::query::StmtRental::new(Arc::new(query.clone()), |_| stmt_ast);

        let stmt = tremor_script::query::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };

        let mut op = test_select(1, stmt)?;
        assert!(try_enqueue(&mut op, test_event(0))?.is_none());
        assert!(try_enqueue(&mut op, test_event(1))?.is_none());
        let (out, event) = try_enqueue(&mut op, test_event(15))?.expect("no event");
        assert_eq!("out", out);

        assert_eq!(*event.data.suffix().value(), 42);
        Ok(())
    }

    #[test]
    fn select_nowin_nogrp_whrt_nohav() -> Result<()> {
        let target = test_target();
        let mut stmt_ast = test_stmt(target);

        stmt_ast.maybe_where = Some(ImutExpr::from(ast::Literal {
            mid: 0,
            value: Value::from(true),
        }));

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::query::QueryRental::new(script_box, |_| {
            test_query(stmt_ast.clone())
        }));

        let query = tremor_script::query::Query {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::query::StmtRental::new(Arc::new(query.clone()), |_| stmt_ast);

        let stmt = tremor_script::query::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };

        let mut op = test_select(2, stmt)?;

        assert!(try_enqueue(&mut op, test_event(0))?.is_none());

        let (out, event) = try_enqueue(&mut op, test_event(15))?.expect("no event");
        assert_eq!("out", out);

        assert_eq!(*event.data.suffix().value(), 42);
        Ok(())
    }

    #[test]
    fn select_nowin_nogrp_whrf_nohav() -> Result<()> {
        let target = test_target();
        let mut stmt_ast = test_stmt(target);

        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        stmt_ast.maybe_where = Some(ImutExpr::from(ast::Literal {
            mid: 0,
            value: Value::from(false),
        }));
        let stmt_ast = test_select_stmt(stmt_ast);

        let query_rental = Arc::new(tremor_script::query::QueryRental::new(script_box, |_| {
            test_query(stmt_ast.clone())
        }));

        let query = tremor_script::query::Query {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::query::StmtRental::new(Arc::new(query.clone()), |_| stmt_ast);

        let stmt = tremor_script::query::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };

        let mut op = test_select(3, stmt)?;
        let next = try_enqueue(&mut op, test_event(0))?;
        assert_eq!(None, next);
        Ok(())
    }

    #[test]
    fn select_nowin_nogrp_whrbad_nohav() -> Result<()> {
        let target = test_target();
        let mut stmt_ast = test_stmt(target);
        stmt_ast.maybe_where = Some(ImutExpr::from(ast::Literal {
            mid: 0,
            value: Value::from("snot"),
        }));

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::query::QueryRental::new(script_box, |_| {
            test_query(stmt_ast.clone())
        }));

        let query = tremor_script::query::Query {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::query::StmtRental::new(Arc::new(query.clone()), |_| stmt_ast);

        let stmt = tremor_script::query::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };

        let mut op = test_select(4, stmt)?;

        assert!(try_enqueue(&mut op, test_event(0)).is_err());

        Ok(())
    }

    #[test]
    fn select_nowin_nogrp_whrt_havt() -> Result<()> {
        let target = test_target();
        let mut stmt_ast = test_stmt(target);
        stmt_ast.maybe_where = Some(ImutExpr::from(ast::Literal {
            mid: 0,
            value: Value::from(true),
        }));
        stmt_ast.maybe_having = Some(ImutExpr::from(ast::Literal {
            mid: 0,
            value: Value::from(true),
        }));

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::query::QueryRental::new(script_box, |_| {
            test_query(stmt_ast.clone())
        }));

        let query = tremor_script::query::Query {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::query::StmtRental::new(Arc::new(query.clone()), |_| stmt_ast);

        let stmt = tremor_script::query::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };

        let mut op = test_select(5, stmt)?;

        let event = test_event(0);
        assert!(try_enqueue(&mut op, event)?.is_none());

        let event = test_event(15);
        let (out, event) = try_enqueue(&mut op, event)?.expect("no event");
        assert_eq!("out", out);
        assert_eq!(*event.data.suffix().value(), 42);
        Ok(())
    }

    #[test]
    fn select_nowin_nogrp_whrt_havf() -> Result<()> {
        let target = test_target();
        let mut stmt_ast = test_stmt(target);
        stmt_ast.maybe_where = Some(ImutExpr::from(ast::Literal {
            mid: 0,
            value: Value::from(true),
        }));
        stmt_ast.maybe_having = Some(ImutExpr::from(ast::Literal {
            mid: 0,
            value: Value::from(false),
        }));

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::query::QueryRental::new(script_box, |_| {
            test_query(stmt_ast.clone())
        }));

        let query = tremor_script::query::Query {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::query::StmtRental::new(Arc::new(query.clone()), |_| stmt_ast);

        let stmt = tremor_script::query::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };

        let mut op = test_select(6, stmt)?;
        let event = test_event(0);

        let next = try_enqueue(&mut op, event)?;

        assert_eq!(None, next);
        Ok(())
    }

    fn test_select_stmt(stmt: tremor_script::ast::Select) -> tremor_script::ast::Stmt {
        let aggregates = vec![];
        let aggregate_scratches = Some((aggregates.clone(), aggregates.clone()));
        ast::Stmt::Select(SelectStmt {
            stmt: Box::new(stmt),
            aggregates,
            aggregate_scratches,
            consts: Consts::default(),
            locals: 0,
            node_meta: ast::NodeMetas::new(vec![]),
        })
    }
    #[test]
    fn select_nowin_nogrp_whrt_havbad() -> Result<()> {
        use halfbrown::hashmap;
        let target = test_target();
        let mut stmt_ast = test_stmt(target);
        stmt_ast.maybe_where = Some(ImutExpr::from(ast::Literal {
            mid: 0,
            value: Value::from(true),
        }));
        stmt_ast.maybe_having = Some(ImutExpr::from(Literal {
            mid: 0,
            value: Value::from(hashmap! {
                "snot".into() => "badger".into(),
            }),
        }));

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::query::QueryRental::new(script_box, |_| {
            test_query(stmt_ast.clone())
        }));

        let query = tremor_script::query::Query {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::query::StmtRental::new(Arc::new(query.clone()), |_| stmt_ast);

        let stmt = tremor_script::query::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };

        let mut op = test_select(7, stmt)?;
        let event = test_event(0);

        let next = try_enqueue(&mut op, event)?;

        assert_eq!(None, next);
        Ok(())
    }

    // get a stmt rental for a stupid script
    fn stmt_rental() -> Result<StmtRentalWrapper> {
        let file_name = "foo";
        let reg = tremor_script::registry();
        let aggr_reg = tremor_script::aggr_registry();
        let module_path = tremor_script::path::load();
        let cus = vec![];
        let query = tremor_script::query::Query::parse(
            &module_path,
            &file_name,
            "select event from in into out;",
            cus,
            &reg,
            &aggr_reg,
        )
        .map_err(tremor_script::errors::CompilerError::error)?;

        let stmt_rental = tremor_script::query::StmtRental::try_new(Arc::new(query.clone()), |q| {
            q.suffix()
                .stmts
                .get(0)
                .cloned()
                .ok_or_else(|| Error::from("Invalid query"))
        })
        .map_err(|e| e.0)?;
        let stmt = tremor_script::query::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };
        Ok(stmt)
    }

    #[test]
    fn tumbling_window_on_time_emit() -> Result<()> {
        let stmt = stmt_rental()?;
        // interval = 10 seconds
        let mut window = TumblingWindowOnTime::from_stmt(
            10 * 1_000_000_000,
            WindowImpl::DEFAULT_EMIT_EMPTY_WINDOWS,
            WindowImpl::DEFAULT_MAX_GROUPS,
            None,
            None,
            &stmt,
        );
        assert_eq!(
            WindowEvent {
                include: false,
                opened: true,
                emit: false
            },
            window.on_event(&test_event(5))?
        );
        assert_eq!(
            WindowEvent {
                include: false,
                opened: false,
                emit: false
            },
            window.on_event(&test_event(10))?
        );
        assert_eq!(
            WindowEvent {
                include: false,
                opened: true,
                emit: true
            },
            window.on_event(&test_event(15))? // exactly on time
        );
        assert_eq!(
            WindowEvent {
                include: false,
                opened: true,
                emit: true
            },
            window.on_event(&test_event(26))? // exactly on time
        );
        Ok(())
    }

    fn json_event(ingest_ns: u64, payload: OwnedValue) -> Event {
        Event {
            id: (0, 0, ingest_ns).into(),
            ingest_ns,
            data: Value::from(payload).into(),
            ..Event::default()
        }
    }

    #[test]
    fn tumbling_window_on_time_from_script_emit() -> Result<()> {
        let stmt = stmt_rental()?;
        // create a WindowDecl with a custom script
        let reg = Registry::default();
        let aggr_reg = AggrRegistry::default();
        let module_path = ModulePath::load();
        let q = tremor_script::query::Query::parse(
            &module_path,
            "bar",
            r#"
            define tumbling window my_window
            with
                interval = 1000000000 # 1 second
            script
                event.timestamp
            end;"#,
            vec![],
            &reg,
            &aggr_reg,
        )
        .map_err(|ce| ce.error)?;
        let window_decl = match q.query.suffix().stmts.get(0) {
            Some(Stmt::WindowDecl(decl)) => decl.as_ref(),
            other => return Err(format!("Didnt get a window decl, got: {:?}", other).into()),
        };
        let mut params = halfbrown::HashMap::with_capacity(1);
        params.insert("size".to_string(), Value::Static(StaticNode::U64(3)));
        let interval = window_decl
            .params
            .get("interval")
            .and_then(Value::as_u64)
            .ok_or(Error::from("no interval found"))?;
        let mut window = TumblingWindowOnTime::from_stmt(
            interval,
            WindowImpl::DEFAULT_EMIT_EMPTY_WINDOWS,
            WindowImpl::DEFAULT_MAX_GROUPS,
            None,
            Some(&window_decl),
            &stmt,
        );
        let json1 = json!({
            "timestamp": 1_000_000_000
        });
        assert_eq!(
            WindowEvent {
                opened: true,
                include: false,
                emit: false
            },
            window.on_event(&json_event(1, json1))?
        );
        let json2 = json!({
            "timestamp": 1_999_999_999
        });
        assert_eq!(
            WindowEvent {
                opened: false,
                include: false,
                emit: false
            },
            window.on_event(&json_event(2, json2))?
        );
        let json3 = json!({
            "timestamp": 2_000_000_000
        });
        // ignoring on_tick as we have a script
        assert_eq!(
            WindowEvent {
                opened: false,
                include: false,
                emit: false
            },
            window.on_tick(2_000_000_000)?
        );
        assert_eq!(
            WindowEvent {
                opened: true,
                include: false,
                emit: true
            },
            window.on_event(&json_event(3, json3))?
        );
        Ok(())
    }

    #[test]
    fn tumbling_window_on_time_on_tick() -> Result<()> {
        let stmt = stmt_rental()?;
        let mut window = TumblingWindowOnTime::from_stmt(
            100,
            WindowImpl::DEFAULT_EMIT_EMPTY_WINDOWS,
            WindowImpl::DEFAULT_MAX_GROUPS,
            None,
            None,
            &stmt,
        );
        assert_eq!(
            WindowEvent {
                opened: true,
                include: false,
                emit: false
            },
            window.on_tick(0)?
        );
        assert_eq!(
            WindowEvent {
                opened: false,
                include: false,
                emit: false
            },
            window.on_tick(99)?
        );
        assert_eq!(
            WindowEvent {
                opened: true,
                include: false,
                emit: false // we dont emit if we had no event
            },
            window.on_tick(100)?
        );
        assert_eq!(
            WindowEvent {
                opened: false,
                include: false,
                emit: false
            },
            window.on_event(&json_event(101, json!({})))?
        );
        assert_eq!(
            WindowEvent {
                opened: false,
                include: false,
                emit: false
            },
            window.on_tick(102)?
        );
        assert_eq!(
            WindowEvent {
                opened: true,
                include: false,
                emit: true // we had an event yeah
            },
            window.on_tick(200)?
        );
        Ok(())
    }

    #[test]
    fn tumbling_window_on_time_emit_empty_windows() -> Result<()> {
        let stmt = stmt_rental()?;
        let mut window = TumblingWindowOnTime::from_stmt(
            100,
            true,
            WindowImpl::DEFAULT_MAX_GROUPS,
            None,
            None,
            &stmt,
        );
        assert_eq!(
            WindowEvent {
                opened: true,
                include: false,
                emit: false
            },
            window.on_tick(0)?
        );
        assert_eq!(
            WindowEvent {
                opened: false,
                include: false,
                emit: false
            },
            window.on_tick(99)?
        );
        assert_eq!(
            WindowEvent {
                opened: true,
                include: false,
                emit: true // we **DO** emit even if we had no event
            },
            window.on_tick(100)?
        );
        assert_eq!(
            WindowEvent {
                opened: false,
                include: false,
                emit: false
            },
            window.on_event(&json_event(101, json!({})))?
        );
        assert_eq!(
            WindowEvent {
                opened: false,
                include: false,
                emit: false
            },
            window.on_tick(102)?
        );
        assert_eq!(
            WindowEvent {
                opened: true,
                include: false,
                emit: true // we had an event yeah
            },
            window.on_tick(200)?
        );

        Ok(())
    }

    #[test]
    fn no_window_emit() -> Result<()> {
        let mut window = NoWindow::default();
        assert_eq!(
            WindowEvent {
                include: true,
                opened: true,
                emit: true
            },
            window.on_event(&test_event(0))?
        );
        assert_eq!(
            WindowEvent {
                include: false,
                opened: false,
                emit: false
            },
            window.on_tick(0)?
        );
        assert_eq!(
            WindowEvent {
                include: true,
                opened: true,
                emit: true
            },
            window.on_event(&test_event(1))?
        );
        assert_eq!(
            WindowEvent {
                include: false,
                opened: false,
                emit: false
            },
            window.on_tick(1)?
        );
        Ok(())
    }

    #[test]
    fn tumbling_window_on_number_emit() -> Result<()> {
        let stmt = stmt_rental()?;
        let mut window =
            TumblingWindowOnNumber::from_stmt(3, WindowImpl::DEFAULT_MAX_GROUPS, None, None, &stmt);
        // do not emit yet
        assert_eq!(
            WindowEvent {
                include: false,
                opened: false,
                emit: false
            },
            window.on_event(&test_event(0))?
        );
        assert_eq!(
            WindowEvent {
                include: false,
                opened: false,
                emit: false
            },
            window.on_tick(1_000_000_000)?
        );
        // do not emit yet
        assert_eq!(
            WindowEvent {
                include: false,
                opened: false,
                emit: false
            },
            window.on_event(&test_event(1))?
        );
        assert_eq!(
            WindowEvent {
                include: false,
                opened: false,
                emit: false
            },
            window.on_tick(2_000_000_000)?
        );
        // emit and open on the third event
        assert_eq!(
            WindowEvent {
                include: true,
                opened: true,
                emit: true
            },
            window.on_event(&test_event(2))?
        );
        // no emit here, next window
        assert_eq!(
            WindowEvent {
                include: false,
                opened: false,
                emit: false
            },
            window.on_event(&test_event(3))?
        );

        Ok(())
    }
}
