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

use crate::{
    errors::{Error, Result},
    op::prelude::max_groups_reached,
    EventId, EventIdGenerator,
};
use halfbrown::{HashMap, RawEntryMut};
use std::borrow::Cow as SCow;
use tremor_script::{
    self,
    ast::{Aggregates, InvokeAggrFn, NodeMetas, RunConsts, WindowDecl},
    interpreter::{Env, LocalStack},
    prelude::*,
    Value,
};

use super::select::NO_AGGRS;

#[derive(Debug, Clone)]
pub struct GroupData {
    pub(crate) group: Value<'static>,
    pub(crate) window: Impl,
    pub(crate) aggrs: Aggregates<'static>,
    pub(crate) id: EventId,
    pub(crate) transactional: bool,
}

impl GroupData {
    pub(crate) fn track_transactional(&mut self, transactional: bool) {
        self.transactional = self.transactional || transactional;
    }

    pub(crate) fn reset(&mut self) {
        for aggr in &mut self.aggrs {
            aggr.invocable.init();
        }
        self.transactional = false;
    }
}

pub(crate) type Groups = HashMap<String, GroupData>;

pub trait Trait: std::fmt::Debug {
    fn on_event(
        &mut self,
        data: &ValueAndMeta,
        ingest_ns: u64,
        origin_uri: &Option<EventOriginUri>,
    ) -> Result<WindowEvent>;
    /// handle a tick with the current time in nanoseconds as `ns` argument
    fn on_tick(&mut self, _ns: u64) -> Result<WindowEvent> {
        Ok(WindowEvent::all_false())
    }
    fn should_evict(&mut self, ns: u64) -> bool;
    /// maximum number of groups to keep around simultaneously
    /// a value of `u64::MAX` allows as much simultaneous groups as possible
    /// decreasing this value will guard against runwaway memory growth
    /// when faced with unexpected huge cardinalities for grouping dimensions
    fn max_groups(&self) -> u64;
}

#[derive(Debug)]
pub struct Window {
    pub(crate) window_impl: Impl,
    pub(crate) module: Vec<String>,
    pub(crate) name: String,
    pub(crate) dims: Groups,
    pub(crate) last_dims: Groups,
}

impl Window {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }
    pub(crate) fn module_path(fqwn: &str) -> Vec<String> {
        let mut segments: Vec<_> = fqwn.split("::").map(String::from).collect();
        segments.pop(); // Remove the last element
        segments
    }

    pub(crate) fn ident_name(fqwn: &str) -> &str {
        fqwn.split("::").last().map_or(fqwn, |last| last)
    }
    pub(crate) fn maybe_evict(&mut self, ns: u64) {
        if self.window_impl.should_evict(ns) {
            self.last_dims.clear();
            std::mem::swap(&mut self.dims, &mut self.last_dims);
        }
    }
}

#[derive(Debug, Clone)]
pub enum Impl {
    TumblingCountBased(TumblingOnNumber),
    TumblingTimeBased(TumblingOnTime),
}

impl Impl {
    // allow all the groups we can take by default
    // this preserves backward compatibility
    pub const DEFAULT_MAX_GROUPS: u64 = u64::MAX;

    // do not emit empty windows by default
    // this preserves backward compatibility
    pub const DEFAULT_EMIT_EMPTY_WINDOWS: bool = false;
}

impl Trait for Impl {
    fn on_event(
        &mut self,
        data: &ValueAndMeta,
        ingest_ns: u64,
        origin_uri: &Option<EventOriginUri>,
    ) -> Result<WindowEvent> {
        match self {
            Self::TumblingTimeBased(w) => w.on_event(data, ingest_ns, origin_uri),
            Self::TumblingCountBased(w) => w.on_event(data, ingest_ns, origin_uri),
        }
    }

    fn on_tick(&mut self, ns: u64) -> Result<WindowEvent> {
        match self {
            Self::TumblingTimeBased(w) => w.on_tick(ns),
            Self::TumblingCountBased(w) => w.on_tick(ns),
        }
    }

    fn should_evict(&mut self, ns: u64) -> bool {
        match self {
            Self::TumblingTimeBased(w) => w.should_evict(ns),
            Self::TumblingCountBased(w) => w.should_evict(ns),
        }
    }
    fn max_groups(&self) -> u64 {
        match self {
            Self::TumblingTimeBased(w) => w.max_groups(),
            Self::TumblingCountBased(w) => w.max_groups(),
        }
    }
}

impl From<TumblingOnNumber> for Impl {
    fn from(w: TumblingOnNumber) -> Self {
        Self::TumblingCountBased(w)
    }
}
impl From<TumblingOnTime> for Impl {
    fn from(w: TumblingOnTime) -> Self {
        Self::TumblingTimeBased(w)
    }
}

#[derive(Debug, PartialEq, Default)]
pub struct WindowEvent {
    /// New window has opened
    pub opened: bool,
    /// Include the current event in the window event to be emitted
    pub include: bool,
    /// Emit a window event
    pub emit: bool,
}

impl WindowEvent {
    pub(crate) fn all_true() -> Self {
        Self {
            opened: true,
            include: true,
            emit: true,
        }
    }
    pub(crate) fn all_false() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
pub struct No {
    open: bool,
}

impl Trait for No {
    fn should_evict(&mut self, _ns: u64) -> bool {
        false
    }
    fn on_event(
        &mut self,
        _data: &ValueAndMeta,
        _ingest_ns: u64,
        _origin_uri: &Option<EventOriginUri>,
    ) -> Result<WindowEvent> {
        self.open = true;
        Ok(WindowEvent::all_true())
    }
    fn max_groups(&self) -> u64 {
        u64::MAX
    }
}

#[derive(Default, Debug, Clone)]
pub struct TumblingOnTime {
    pub(crate) next_window: Option<u64>,
    pub(crate) emit_empty_windows: bool,
    pub(crate) max_groups: u64,
    pub(crate) events: u64,
    pub(crate) interval: u64,
    pub(crate) next_swap: u64,
    pub(crate) ttl: u64,
    pub(crate) script: Option<WindowDecl<'static>>,
}
impl TumblingOnTime {
    pub fn from_stmt(
        interval: u64,
        emit_empty_windows: bool,
        max_groups: u64,
        ttl: Option<u64>,
        script: Option<&WindowDecl>,
    ) -> Self {
        let script = script.cloned().map(WindowDecl::into_static);
        let ttl = ttl.unwrap_or(interval);
        Self {
            next_window: None,
            emit_empty_windows,
            max_groups,
            events: 0,
            interval,
            next_swap: ttl * 2,
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
            Some(_) => WindowEvent::all_false(),
        }
    }
}

impl Trait for TumblingOnTime {
    fn should_evict(&mut self, ns: u64) -> bool {
        if self.next_swap < ns {
            self.next_swap = ns + self.ttl * 2;
            true
        } else {
            false
        }
    }
    fn max_groups(&self) -> u64 {
        self.max_groups
    }
    fn on_event(
        &mut self,
        data: &ValueAndMeta,
        ingest_ns: u64,
        origin_uri: &Option<EventOriginUri>,
    ) -> Result<WindowEvent> {
        self.events += 1; // count events to check if we should emit, as we avoid to emit if we have no events
        let time = self
            .script
            .as_ref()
            .and_then(|script| script.script.as_ref())
            .map(|script| {
                // TODO avoid origin_uri clone here
                let context = EventContext::new(ingest_ns, origin_uri.clone());
                let (unwind_event, event_meta) = data.parts();
                let value = script.run_imut(
                    &context,
                    AggrType::Emit,
                    &unwind_event,  // event
                    &Value::null(), // state for the window
                    &event_meta,    // $
                )?;
                let data = match value {
                    Return::Emit { value, .. } => value.as_u64(),
                    Return::EmitEvent { .. } => unwind_event.as_u64(),
                    Return::Drop { .. } => None,
                };
                data.ok_or_else(|| Error::from("Data based window didn't provide a valid value"))
            })
            .unwrap_or(Ok(ingest_ns))?;
        Ok(self.get_window_event(time))
    }

    fn on_tick(&mut self, ns: u64) -> Result<WindowEvent> {
        if self.script.is_none() {
            Ok(self.get_window_event(ns))
        } else {
            // we basically ignore ticks when we have a script with a custom timestamp
            Ok(WindowEvent::all_false())
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct TumblingOnNumber {
    count: u64,
    max_groups: u64,
    size: u64,
    next_swap: u64,
    ttl: Option<u64>,
    script: Option<WindowDecl<'static>>,
}

impl TumblingOnNumber {
    pub fn from_stmt(
        size: u64,
        max_groups: u64,
        ttl: Option<u64>,
        script: Option<&WindowDecl>,
    ) -> Self {
        let script = script.cloned().map(WindowDecl::into_static);

        Self {
            max_groups,
            size,
            script,
            ttl,
            ..Default::default()
        }
    }
}
impl Trait for TumblingOnNumber {
    fn should_evict(&mut self, ns: u64) -> bool {
        if let Some(ttl) = self.ttl {
            if self.next_swap < ns {
                self.next_swap = ns + ttl * 2;
                true
            } else {
                false
            }
        } else {
            false
        }
    }
    fn max_groups(&self) -> u64 {
        self.max_groups
    }
    fn on_event(
        &mut self,
        data: &ValueAndMeta,
        ingest_ns: u64,
        origin_uri: &Option<EventOriginUri>,
    ) -> Result<WindowEvent> {
        let count = self
            .script
            .as_ref()
            .and_then(|script| script.script.as_ref())
            .map_or(Ok(1), |script| {
                // TODO avoid origin_uri clone here
                let context = EventContext::new(ingest_ns, origin_uri.clone());
                let (unwind_event, event_meta) = data.parts();
                let value = script.run_imut(
                    &context,
                    AggrType::Emit,
                    &unwind_event,  // event
                    &Value::null(), // state for the window
                    &event_meta,    // $
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
            // we can emit now, including this event
            Ok(WindowEvent::all_true())
        } else {
            self.count = new_count;
            Ok(WindowEvent::all_false())
        }
    }
}

/// accumulate the given `event` into the current `group`s aggregates
#[allow(clippy::too_many_arguments)] // this is the price for no transmutation
pub(crate) fn accumulate(
    context: &EventContext,
    consts: RunConsts,
    opts: ExecOpts,
    node_meta: &NodeMetas,
    local_stack: &LocalStack,
    state: &mut Value<'static>,
    group: &mut GroupData,
    data: &ValueAndMeta,
    id: &EventId,
    transactional: bool,
) -> Result<()> {
    let env = Env {
        context,
        consts,
        aggrs: &NO_AGGRS,
        meta: &node_meta,
        recursion_limit: tremor_script::recursion_limit(),
    };

    // we incorporate the event into this group below, so track it here
    group.id.track(&id);

    // track transactional state for the given event
    group.transactional = group.transactional || transactional;

    let (event_data, event_meta) = data.parts();
    for aggr in &mut group.aggrs {
        let invocable = &mut aggr.invocable;
        let mut argv: Vec<SCow<Value>> = Vec::with_capacity(aggr.args.len());
        let mut argv1: Vec<&Value> = Vec::with_capacity(aggr.args.len());
        for arg in &aggr.args {
            let result = arg.run(opts, &env, event_data, state, event_meta, &local_stack)?;
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
pub(crate) fn get_or_create_group<'window>(
    window: &'window mut Window,
    idgen: &mut EventIdGenerator,
    aggregates: &[InvokeAggrFn<'static>],
    group_str: &str,
    group_value: &Value,
) -> Result<&'window mut GroupData> {
    let this_groups = &mut window.dims;
    let groups_len = this_groups.len() as u64;
    let last_groups = &mut window.last_dims;
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
                    transactional: false,
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
