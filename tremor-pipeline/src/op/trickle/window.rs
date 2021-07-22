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

use crate::{Event, EventId, EventIdGenerator, OpMeta};
use beef::Cow;
use std::{borrow::Cow as SCow, mem};
use tremor_common::stry;
use tremor_script::{
    self,
    ast::{AggrSlice, Aggregates, Consts, NodeMetas, RunConsts, Select, WindowDecl},
    errors::Result,
    interpreter::{Env, LocalStack},
    prelude::*,
    Value,
};

use super::select::{execute_select_and_having, NO_AGGRS};

pub(crate) struct SelectCtx<'run, 'script, 'local> {
    pub(crate) select: &'run Select<'script>,
    pub(crate) local_stack: &'run LocalStack<'local>,
    pub(crate) node_meta: &'run NodeMetas,
    pub(crate) opts: ExecOpts,
    pub(crate) ctx: &'run EventContext<'run>,
    pub(crate) state: &'run mut Value<'static>,
    pub(crate) event_id: EventId,
    pub(crate) event_id_gen: &'run mut EventIdGenerator,
    pub(crate) ingest_ns: u64,
    pub(crate) op_meta: &'run OpMeta,
    pub(crate) origin_uri: &'run Option<EventOriginUri>,
    pub(crate) transactional: bool,
    pub(crate) recursion_limit: u32,
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug)]
pub struct GroupWindow {
    pub(crate) name: Value<'static>,
    pub(crate) window: Impl,
    pub(crate) aggrs: Aggregates<'static>,
    pub(crate) id: EventId,
    pub(crate) transactional: bool,
    pub(crate) next: Option<Box<GroupWindow>>,
}

impl GroupWindow {
    pub(crate) fn from_windows<'i, I>(
        aggrs: &AggrSlice<'static>,
        id: &EventId,
        mut iter: I,
    ) -> Option<Box<Self>>
    where
        I: std::iter::Iterator<Item = &'i Window>,
    {
        iter.next().map(|w| {
            Box::new(Self {
                window: w.window_impl.clone(),
                aggrs: aggrs.to_vec(),
                id: id.clone(),
                name: w.name.clone().into(),
                transactional: false,
                next: GroupWindow::from_windows(aggrs, id, iter),
            })
        })
    }
    pub(crate) fn reset(&mut self) {
        for aggr in &mut self.aggrs {
            aggr.invocable.init();
        }
        self.transactional = false;
    }

    pub(crate) fn accumulate(
        &mut self,
        ctx: &mut SelectCtx,
        consts: RunConsts,
        data: &ValueAndMeta,
    ) -> Result<()> {
        let mut consts = consts;
        consts.window = &self.name;

        let env = Env {
            context: &ctx.ctx,
            consts,
            aggrs: &NO_AGGRS,
            meta: &ctx.node_meta,
            recursion_limit: ctx.recursion_limit,
        };

        // we incorporate the event into this group below, so track it here
        self.id.track(&ctx.event_id);

        // track transactional state for the given event
        self.transactional |= ctx.transactional;

        let (event_data, event_meta) = data.parts();
        let SelectCtx {
            state,
            opts,
            node_meta,
            ..
        } = ctx;
        for aggr in &mut self.aggrs {
            let invocable = &mut aggr.invocable;
            let mut argv: Vec<SCow<Value>> = Vec::with_capacity(aggr.args.len());
            let mut argv1: Vec<&Value> = Vec::with_capacity(aggr.args.len());
            for arg in &aggr.args {
                let result =
                    stry!(arg.run(*opts, &env, event_data, state, event_meta, ctx.local_stack));
                argv.push(result);
            }
            for arg in &argv {
                argv1.push(arg);
            }

            stry!(invocable.accumulate(argv1.as_slice()).map_err(|e| {
                // TODO nice error
                let r: Option<&Registry> = None;
                e.into_err(aggr, aggr, r, node_meta)
            }));
        }
        Ok(())
    }

    fn merge(&mut self, ctx: &SelectCtx, prev: &AggrSlice<'static>) -> Result<()> {
        self.id.track(&ctx.event_id);
        self.transactional |= ctx.transactional;
        for (this, prev) in self.aggrs.iter_mut().zip(prev.iter()) {
            stry!(this.invocable.merge(&prev.invocable).map_err(|e| {
                let r: Option<&Registry> = None;
                e.into_err(prev, prev, r, ctx.node_meta)
            }));
        }
        Ok(())
    }

    pub(crate) fn on_event(
        &mut self,
        ctx: &mut SelectCtx,
        consts: RunConsts,
        data: &ValueAndMeta,
        events: &mut Vec<(Cow<'static, str>, Event)>,
        prev: Option<&Aggregates<'static>>,
        mut can_remove: bool,
    ) -> Result<bool> {
        let window_event = stry!(self.window.on_event(data, ctx.ingest_ns, ctx.origin_uri));

        if window_event.include {
            if let Some(prev) = prev {
                stry!(self.merge(ctx, prev));
            } else {
                stry!(self.accumulate(ctx, consts, data));
            }
        }

        if window_event.emit {
            // push
            let env = Env {
                context: &ctx.ctx,
                consts,
                aggrs: &self.aggrs,
                meta: &ctx.node_meta,
                recursion_limit: ctx.recursion_limit,
            };

            let mut outgoing_event_id = ctx.event_id_gen.next_id();
            mem::swap(&mut self.id, &mut outgoing_event_id);
            ctx.transactional = self.transactional;
            let mut consts = consts;
            consts.window = &self.name;
            std::mem::swap(&mut ctx.event_id, &mut outgoing_event_id);
            if let Some(port_and_event) = stry!(execute_select_and_having(ctx, &env, data)) {
                events.push(port_and_event);
            };
            // re-initialize aggr state for new window
            // reset transactional state for outgoing events

            if let Some(next) = &mut self.next {
                can_remove = can_remove
                    && stry!(next.on_event(
                        ctx,
                        consts,
                        data,
                        events,
                        Some(&self.aggrs),
                        can_remove
                    ));
            }
            std::mem::swap(&mut ctx.event_id, &mut outgoing_event_id);
            self.transactional = false;
            self.reset();
        }
        if window_event.include {
            Ok(can_remove)
        } else {
            if let Some(prev) = prev {
                stry!(self.merge(ctx, prev));
            } else {
                stry!(self.accumulate(ctx, consts, data));
            }
            Ok(false)
        }
    }
}

#[derive(Clone, Debug)]
pub struct Group {
    pub(crate) value: Value<'static>,
    pub(crate) windows: Option<Box<GroupWindow>>,
    pub(crate) aggrs: Aggregates<'static>,
}

impl Group {
    pub(crate) fn reset(&mut self) {
        let mut w = &mut self.windows;
        while let Some(g) = w {
            g.reset();
            g.window.reset();
            w = &mut g.next
        }
    }
    pub(crate) fn on_event(
        &mut self,
        mut ctx: SelectCtx,
        consts: &mut Consts,
        data: &ValueAndMeta,
        events: &mut Vec<(Cow<'static, str>, Event)>,
    ) -> Result<bool> {
        let mut run = consts.run();
        run.group = &self.value;
        if let Some(first) = &mut self.windows {
            first.on_event(&mut ctx, run, data, events, None, true)
        } else {
            let env = Env {
                context: ctx.ctx,
                consts: run,
                aggrs: &NO_AGGRS,
                meta: &ctx.node_meta,
                recursion_limit: ctx.recursion_limit,
            };
            if let Some(port_and_event) = stry!(execute_select_and_having(&ctx, &env, &data)) {
                events.push(port_and_event);
            };
            Ok(true)
        }
    }
}

pub trait Trait: std::fmt::Debug {
    fn on_event(
        &mut self,
        data: &ValueAndMeta,
        ingest_ns: u64,
        origin_uri: &Option<EventOriginUri>,
    ) -> Result<Actions>;
    /// handle a tick with the current time in nanoseconds as `ns` argument
    fn on_tick(&mut self, _ns: u64) -> Result<Actions> {
        Ok(Actions::all_false())
    }
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
}

impl Window {
    pub(crate) fn module_path(fqwn: &str) -> Vec<String> {
        let mut segments: Vec<_> = fqwn.split("::").map(String::from).collect();
        segments.pop(); // Remove the last element
        segments
    }

    pub(crate) fn ident_name(fqwn: &str) -> &str {
        fqwn.split("::").last().map_or(fqwn, |last| last)
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

    pub(crate) fn reset(&mut self) {
        match self {
            Self::TumblingTimeBased(w) => w.reset(),
            Self::TumblingCountBased(w) => w.reset(),
        }
    }
}

impl Trait for Impl {
    fn on_event(
        &mut self,
        data: &ValueAndMeta,
        ingest_ns: u64,
        origin_uri: &Option<EventOriginUri>,
    ) -> Result<Actions> {
        match self {
            Self::TumblingTimeBased(w) => w.on_event(data, ingest_ns, origin_uri),
            Self::TumblingCountBased(w) => w.on_event(data, ingest_ns, origin_uri),
        }
    }

    fn on_tick(&mut self, ns: u64) -> Result<Actions> {
        match self {
            Self::TumblingTimeBased(w) => w.on_tick(ns),
            Self::TumblingCountBased(w) => w.on_tick(ns),
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
pub struct Actions {
    /// New window has opened
    pub opened: bool,
    /// Include the current event in the window event to be emitted
    pub include: bool,
    /// Emit a window event
    pub emit: bool,
}

impl Actions {
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
pub struct No {}

impl Trait for No {
    fn on_event(
        &mut self,
        _data: &ValueAndMeta,
        _ingest_ns: u64,
        _origin_uri: &Option<EventOriginUri>,
    ) -> Result<Actions> {
        Ok(Actions::all_true())
    }
    fn max_groups(&self) -> u64 {
        u64::MAX
    }
}

#[derive(Default, Debug, Clone)]
pub struct TumblingOnTime {
    pub(crate) next_window: Option<u64>,
    pub(crate) max_groups: u64,
    /// How long a window lasts (how many ns we accumulate)
    pub(crate) interval: u64,
    pub(crate) script: Option<WindowDecl<'static>>,
}
impl TumblingOnTime {
    pub(crate) fn reset(&mut self) {
        self.next_window = None;
    }

    pub fn from_stmt(interval: u64, max_groups: u64, script: Option<&WindowDecl>) -> Self {
        let script = script.cloned().map(WindowDecl::into_static);
        Self {
            next_window: None,
            max_groups,
            interval,
            // assume 1st timestamp to be 0
            script,
        }
    }

    fn get_window_event(&mut self, time: u64) -> Actions {
        match self.next_window {
            None => {
                self.next_window = Some(time + self.interval);
                Actions {
                    opened: true,
                    include: false,
                    emit: false,
                }
            }
            Some(next_window) if next_window <= time => {
                self.next_window = Some(time + self.interval);
                Actions {
                    opened: true,   // this event has been put into the newly opened window
                    include: false, // event is beyond the current window, put it into the next
                    emit: true,     // only emit if we had any events in this interval
                }
            }
            Some(_) => Actions::all_false(),
        }
    }
}

impl Trait for TumblingOnTime {
    fn max_groups(&self) -> u64 {
        self.max_groups
    }
    fn on_event(
        &mut self,
        data: &ValueAndMeta,
        ingest_ns: u64,
        origin_uri: &Option<EventOriginUri>,
    ) -> Result<Actions> {
        let time = stry!(self
            .script
            .as_ref()
            .and_then(|script| script.script.as_ref())
            .map(|script| {
                let context = EventContext::new(ingest_ns, origin_uri.as_ref());
                let (unwind_event, event_meta) = data.parts();
                let value = stry!(script.run_imut(
                    &context,
                    AggrType::Emit,
                    &unwind_event,  // event
                    &Value::null(), // state for the window
                    &event_meta,    // $
                ));
                let data = match value {
                    Return::Emit { value, .. } => value.as_u64(),
                    Return::EmitEvent { .. } => unwind_event.as_u64(),
                    Return::Drop { .. } => None,
                };
                data.ok_or_else(|| "Data based window didn't provide a valid value".into())
            })
            .unwrap_or(Ok(ingest_ns)));
        Ok(self.get_window_event(time))
    }

    fn on_tick(&mut self, ns: u64) -> Result<Actions> {
        if self.script.is_none() {
            Ok(self.get_window_event(ns))
        } else {
            // we basically ignore ticks when we have a script with a custom timestamp
            Ok(Actions::all_false())
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct TumblingOnNumber {
    count: u64,
    max_groups: u64,
    size: u64,
    next_eviction: u64,
    script: Option<WindowDecl<'static>>,
}

impl TumblingOnNumber {
    pub(crate) fn reset(&mut self) {
        self.next_eviction = 0;
        self.count = 0;
    }
    pub fn from_stmt(size: u64, max_groups: u64, script: Option<&WindowDecl>) -> Self {
        let script = script.cloned().map(WindowDecl::into_static);

        Self {
            max_groups,
            size,
            script,
            ..TumblingOnNumber::default()
        }
    }
}
impl Trait for TumblingOnNumber {
    fn max_groups(&self) -> u64 {
        self.max_groups
    }
    fn on_event(
        &mut self,
        data: &ValueAndMeta,
        ingest_ns: u64,
        origin_uri: &Option<EventOriginUri>,
    ) -> Result<Actions> {
        let count = stry!(self
            .script
            .as_ref()
            .and_then(|script| script.script.as_ref())
            .map_or(Ok(1), |script| {
                let context = EventContext::new(ingest_ns, origin_uri.as_ref());
                let (unwind_event, event_meta) = data.parts();
                let value = stry!(script.run_imut(
                    &context,
                    AggrType::Emit,
                    &unwind_event,  // event
                    &Value::null(), // state for the window
                    &event_meta,    // $
                ));
                let data = match value {
                    Return::Emit { value, .. } => value.as_u64(),
                    Return::EmitEvent { .. } => unwind_event.as_u64(),
                    Return::Drop { .. } => None,
                };
                data.ok_or_else(|| "Data based window didn't provide a valid value".into())
            }));

        // If we're above count we emit and set the new count to 1
        // ( we emit on the ) previous event
        let new_count = self.count + count;
        if new_count >= self.size {
            self.count = new_count - self.size;
            // we can emit now, including this event
            Ok(Actions::all_true())
        } else {
            self.count = new_count;
            Ok(Actions::all_false())
        }
    }
}
