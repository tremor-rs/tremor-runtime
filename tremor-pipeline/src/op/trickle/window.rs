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

use super::select::execute_select_and_having;
use crate::{Event, EventId, EventIdGenerator, OpMeta};
use beef::Cow;
use std::{borrow::Cow as SCow, convert::TryFrom};
use tremor_common::{ids::OperatorId, stry};
use tremor_script::{
    self,
    ast::{AggrSlice, Aggregates, Consts, RunConsts, Script, Select, WindowDefinition},
    errors::Result,
    interpreter::{Env, LocalStack},
    prelude::*,
    Value, NO_AGGRS,
};

pub(crate) struct SelectCtx<'run, 'script, 'local> {
    pub(crate) select: &'run Select<'script>,
    pub(crate) local_stack: &'run LocalStack<'local>,
    pub(crate) opts: ExecOpts,
    pub(crate) ctx: &'run EventContext<'run>,
    pub(crate) event_id: EventId,
    pub(crate) ingest_ns: u64,
    pub(crate) op_meta: &'run OpMeta,
    pub(crate) origin_uri: &'run Option<EventOriginUri>,
    pub(crate) transactional: bool,
    pub(crate) recursion_limit: u32,
}

/// A singular tilt frame (window) inside a group
/// with a link to the next tilt frame and all required
/// information to handle data on this level.
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug)]
pub struct GroupWindow {
    /// Name of the window
    pub(crate) name: Value<'static>,
    /// The windowing implementation used
    pub(crate) window: Impl,
    /// The aggregates for this window
    pub(crate) aggrs: Aggregates<'static>,
    /// The event id(s) of all events that are tracked in this
    /// window
    pub(crate) id: EventId,
    /// For generating new ids for this window
    /// each window has a distinct stream id
    pub(crate) event_id_gen: EventIdGenerator,
    /// If the currently windowed data is considered transactional
    /// or not
    pub(crate) transactional: bool,
    /// The next (larger) tilt frame in a group
    pub(crate) next: Option<Box<GroupWindow>>,
    /// If the window holds any data
    pub(crate) holds_data: bool,
}

impl GroupWindow {
    /// Crate chain of tilt frames from a iterator of windows
    pub(crate) fn from_windows<'i, I>(
        aggrs: &AggrSlice<'static>,
        operator_id: OperatorId,
        mut iter: I,
    ) -> Option<Box<Self>>
    where
        I: std::iter::Iterator<Item = (u64, &'i Window)>,
    {
        iter.next().map(|(stream_id, w)| {
            let mut event_id_gen =
                EventIdGenerator::for_operator_with_stream(operator_id, stream_id);
            let id = event_id_gen.next_id();
            Box::new(Self {
                window: w.window_impl.clone(),
                aggrs: aggrs.to_vec(),
                id,
                event_id_gen,
                name: w.name.clone().into(),
                transactional: false,
                next: GroupWindow::from_windows(aggrs, operator_id, iter),
                holds_data: false,
            })
        })
    }
    /// Resets the aggregates and transactionality of this window
    pub(crate) fn reset(&mut self) {
        for aggr in &mut self.aggrs {
            aggr.invocable.init();
        }
        self.transactional = false;
        self.holds_data = false;
    }

    /// Accumultes data into the window
    pub(crate) fn accumulate(
        &mut self,
        ctx: &mut SelectCtx,
        consts: RunConsts,
        data: &ValueAndMeta,
    ) -> Result<()> {
        // track this event:
        //   - incorporate the event ids
        self.id.track(&ctx.event_id);
        //   - track transactional state
        self.transactional |= ctx.transactional;
        self.holds_data = true;

        // Ensure the `window` constant is set propery
        let mut consts = consts;
        consts.window = &self.name;

        // create an execution environment for the accumulation
        // note: we set aggrs to no_aggrs sice nested aggregation
        // is not supported and the `env` is used to evaluate
        // the function arguments for the aggregates not the
        // aggregates themsefls
        let env = Env {
            context: ctx.ctx,
            consts,
            aggrs: &NO_AGGRS,
            recursion_limit: ctx.recursion_limit,
        };

        let (event_data, event_meta) = data.parts();
        let SelectCtx { opts, .. } = ctx;
        for aggr in &mut self.aggrs {
            let invocable = &mut aggr.invocable;
            // We need two arrays to handle the we know the lenght so
            // we pre-allocate. We need this to minimize copying and allocations
            // the functions take a refference to a value and since we
            // might get owned data back in the `Cow` we don't know for
            // sure if we can reference it without keeping ownership.

            // the first one is the computed data in `Cow`s
            let mut argv: Vec<SCow<Value>> = Vec::with_capacity(aggr.args.len());
            // the second vector are refernces to the first vector
            let mut argv1: Vec<&Value> = Vec::with_capacity(aggr.args.len());

            // evaluate the arguments
            for arg in &aggr.args {
                let result =
                    stry!(arg.run(*opts, &env, event_data, &NULL, event_meta, ctx.local_stack));
                argv.push(result);
            }

            // collect references to them
            for arg in &argv {
                argv1.push(arg);
            }
            // now execute the fnctions
            stry!(invocable.accumulate(argv1.as_slice()).map_err(|e| {
                // TODO nice error
                let r: Option<&Registry> = None;
                e.into_err(aggr, aggr, r)
            }));
        }
        Ok(())
    }

    /// Merge data from the privious tilt frame / window into this one
    fn merge(&mut self, ctx: &SelectCtx, prev: &AggrSlice<'static>) -> Result<()> {
        // Track the parents id's and transactionality
        self.id.track(&ctx.event_id);
        self.transactional |= ctx.transactional;
        self.holds_data = true;
        // Ingest the data
        for (this, prev) in self.aggrs.iter_mut().zip(prev.iter()) {
            stry!(this.invocable.merge(&prev.invocable).map_err(|e| {
                let r: Option<&Registry> = None;
                e.into_err(prev, prev, r)
            }));
        }
        Ok(())
    }

    /// This window receives an event either as a root window
    /// or as a later tilt frame - the whole windowing magic
    /// happens here.
    ///
    /// # Returns
    ///
    /// true  - If this window and all following windows hold no
    ///         data since it was included in the emitted events.
    ///         The group can be safely removed.
    /// false - If this window or any of the following tilt frames
    ///         are holding on to data that wasn't mitted yet.
    ///         This group can **not** be removed.
    pub(crate) fn on_event(
        &mut self,
        ctx: &mut SelectCtx,
        consts: RunConsts,
        data: &mut ValueAndMeta,
        events: &mut Vec<(Cow<'static, str>, Event)>,
        prev: Option<(bool, &Aggregates<'static>)>,
        mut can_remove: bool,
    ) -> Result<bool> {
        // determin what to do with the event
        let window_event = stry!(self.window.on_event(data, ctx.ingest_ns, ctx.origin_uri));

        // if it should be included in the current window include it
        if window_event.include {
            if let Some((had_data, prev)) = prev {
                // We are not a top level window so we merge the previos
                // window data
                if had_data {
                    stry!(self.merge(ctx, prev));
                };
            } else {
                // We are a root level window so we accumulate the event data
                stry!(self.accumulate(ctx, consts, data));
            }
        }

        // if we should emit, do that
        if window_event.emit {
            // create a new event id for the next window recording

            // Move the recorded event ID into the context so it is
            // used for inclusion for the following windows.
            // and at the same time create a new event ID for the next window
            ctx.event_id = self.reset_event_id();

            // for the context the transactionality of any following window
            // is the transactionality of this window (since we propagate
            // the current data along the tilt frames)
            ctx.transactional = self.transactional;

            // Set the window name for emission

            if self.holds_data {
                let mut consts = consts;
                consts.window = &self.name;
                let env = Env {
                    context: ctx.ctx,
                    consts,
                    aggrs: &self.aggrs,
                    recursion_limit: ctx.recursion_limit,
                };

                // execute thw select body and apply the `having` to see if we publish an event
                if let Some(port_and_event) = stry!(execute_select_and_having(ctx, &env, data)) {
                    events.push(port_and_event);
                };
            }
            // if we have another tilt frame after that emit our aggregated data to it
            // this happens after emitting so we keep order of the events from the
            // smallest to the largest window
            if let Some(next) = &mut self.next {
                can_remove = can_remove
                    && stry!(next.on_event(
                        ctx,
                        consts,
                        data,
                        events,
                        Some((self.holds_data, &self.aggrs)),
                        can_remove
                    ));
            }
            // since we emitted we now can reset this window
            self.reset();
        }
        if window_event.include {
            // if include is set we recorded the event earlier, meaning that
            // from the point of view of this window we could remove the group
            Ok(can_remove)
        } else {
            // The event wasn't recorded earlier so we need to record it now
            // either by merging the pervious aggregates or accumulating the
            // event data
            if let Some((had_data, prev)) = prev {
                if had_data {
                    stry!(self.merge(ctx, prev));
                };
            } else {
                stry!(self.accumulate(ctx, consts, data));
            }
            // since we recorded new data we know we can't delete this group
            Ok(false)
        }
    }

    /// Return the current `event_id` and reset the internal one to a new one, ready for tracking the next events
    pub(crate) fn reset_event_id(&mut self) -> EventId {
        let mut new_id = self.event_id_gen.next_id();
        std::mem::swap(&mut self.id, &mut new_id);
        new_id
    }
}

/// A group wiht a number of none or more tilt frames
#[derive(Clone, Debug)]
pub struct Group {
    /// The caghed group value (this can be reused)
    pub(crate) value: Value<'static>,
    /// the first window in the group (or none)
    pub(crate) windows: Option<Box<GroupWindow>>,
}

impl Group {
    /// Resets the group and all it's sub windows this differs
    /// from `GroupWindow::reset` in that it not only resets
    /// the data but also sets to windo into a state of 'never
    /// having seen an element'.
    pub(crate) fn reset(&mut self) {
        let mut w = &mut self.windows;
        while let Some(g) = w {
            g.reset();
            g.window.reset();
            w = &mut g.next;
        }
    }

    /// The group receives an event we propagate it through
    /// the different windows.
    /// # Returns
    ///
    /// true  - If no window in the group holds on to any data
    ///         and the entire group can be safely removed.
    /// false - If at least one window holds on to some data
    ///         and this group can **not** be removed.
    pub(crate) fn on_event(
        &mut self,
        mut ctx: SelectCtx,
        consts: &mut Consts,
        data: &mut ValueAndMeta,
        events: &mut Vec<(Cow<'static, str>, Event)>,
    ) -> Result<bool> {
        // Set the group value for the exeuction
        let mut run = consts.run();
        run.group = &self.value;
        if let Some(first) = &mut self.windows {
            // If we have windows trigger `on_event` fo the first of them
            // with the assumption that this can be removed.
            first.on_event(&mut ctx, run, data, events, None, true)
        } else {
            // If we have no windows just execute the select statement
            // and mark this group as removable
            let env = Env {
                context: ctx.ctx,
                consts: run,
                aggrs: &NO_AGGRS,
                recursion_limit: ctx.recursion_limit,
            };
            if let Some(port_and_event) = stry!(execute_select_and_having(&ctx, &env, data)) {
                events.push(port_and_event);
            };
            Ok(true)
        }
    }
}

// Windowing implementaitons and traits

pub trait Trait: std::fmt::Debug {
    fn on_event(
        &mut self,
        data: &mut ValueAndMeta,
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
    fn max_groups(&self) -> usize;
}

#[derive(Debug)]
pub struct Window {
    pub(crate) window_impl: Impl,
    pub(crate) name: String,
}

impl Window {
    pub(crate) fn ident_name(fqwn: &str) -> &str {
        fqwn.split("::").last().map_or(fqwn, |last| last)
    }
}

#[derive(Debug, Clone)]
pub enum Impl {
    Count(TumblingOnNumber),
    Time(TumblingOnTime),
    State(TumblingOnState),
}

impl Impl {
    // allow all the groups we can take by default
    // this preserves backward compatibility
    pub const DEFAULT_MAX_GROUPS: usize = usize::MAX;

    pub(crate) fn reset(&mut self) {
        match self {
            Self::Time(w) => w.reset(),
            Self::Count(w) => w.reset(),
            Self::State(w) => w.reset(),
        }
    }
}

impl Trait for Impl {
    fn on_event(
        &mut self,
        data: &mut ValueAndMeta,
        ingest_ns: u64,
        origin_uri: &Option<EventOriginUri>,
    ) -> Result<Actions> {
        match self {
            Self::Time(w) => w.on_event(data, ingest_ns, origin_uri),
            Self::Count(w) => w.on_event(data, ingest_ns, origin_uri),
            Self::State(w) => w.on_event(data, ingest_ns, origin_uri),
        }
    }

    fn on_tick(&mut self, ns: u64) -> Result<Actions> {
        match self {
            Self::Time(w) => w.on_tick(ns),
            Self::Count(w) => w.on_tick(ns),
            Self::State(w) => w.on_tick(ns),
        }
    }

    fn max_groups(&self) -> usize {
        match self {
            Self::Time(w) => w.max_groups(),
            Self::Count(w) => w.max_groups(),
            Self::State(w) => w.max_groups(),
        }
    }
}

impl From<TumblingOnNumber> for Impl {
    fn from(w: TumblingOnNumber) -> Self {
        Self::Count(w)
    }
}
impl From<TumblingOnTime> for Impl {
    fn from(w: TumblingOnTime) -> Self {
        Self::Time(w)
    }
}

impl From<TumblingOnState> for Impl {
    fn from(w: TumblingOnState) -> Self {
        Self::State(w)
    }
}

#[derive(Debug, PartialEq, Default, Eq)]
pub struct Actions {
    /// Include the current event in the window event to be emitted
    pub include: bool,
    /// Emit a window event
    pub emit: bool,
}

impl Actions {
    pub(crate) fn all_true() -> Self {
        Self {
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
        _data: &mut ValueAndMeta,
        _ingest_ns: u64,
        _origin_uri: &Option<EventOriginUri>,
    ) -> Result<Actions> {
        Ok(Actions::all_true())
    }
    fn max_groups(&self) -> usize {
        usize::MAX
    }
}

impl<'v> TryFrom<&Value<'v>> for Actions {
    type Error = tremor_script::errors::Error;
    fn try_from(v: &Value<'v>) -> std::result::Result<Self, Self::Error> {
        if let Some(emit) = v.as_bool() {
            if emit {
                Ok(Actions::all_true())
            } else {
                Ok(Actions::all_false())
            }
        } else if let Some(r) = v
            .as_array()
            .and_then(|a| a.iter().map(Value::as_bool).collect::<Option<Vec<bool>>>())
        {
            if let [emit, include] = *r.as_slice() {
                Ok(Actions { include, emit })
            } else {
                Err("A array return needs to be a two element array of booleans with the form `[emit, include]`".into())
            }
        } else {
            Err(format!(
                "Window returned `{}` this can't be used as a window reply",
                v.encode()
            )
            .into())
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TumblingOnState {
    pub(crate) initial_state: Value<'static>,
    pub(crate) max_groups: usize,
    pub(crate) script: Script<'static>,
    pub(crate) tick_script: Option<Script<'static>>,
    pub(crate) state: Value<'static>,
}

impl TumblingOnState {
    pub(crate) fn reset(&mut self) {
        self.state = self.initial_state.clone();
    }
    pub fn from_stmt(
        state: Value<'static>,
        max_groups: usize,
        script: Script<'static>,
        tick_script: Option<Script<'static>>,
    ) -> Self {
        Self {
            initial_state: state.clone(),
            max_groups,
            script,
            state,
            tick_script,
        }
    }
}

impl Trait for TumblingOnState {
    fn max_groups(&self) -> usize {
        self.max_groups
    }
    fn on_event(
        &mut self,
        data: &mut ValueAndMeta,
        ingest_ns: u64,
        origin_uri: &Option<EventOriginUri>,
    ) -> Result<Actions> {
        let context = EventContext::new(ingest_ns, origin_uri.as_ref());
        let (unwind_event, event_meta) = data.parts_mut();
        let value = stry!(self.script.run(
            &context,
            AggrType::Emit,
            unwind_event,    // event
            &mut self.state, // state for the window
            event_meta,      // $
        ));
        match value {
            Return::Emit { value, .. } => Actions::try_from(&value),
            Return::EmitEvent { .. } => Actions::try_from(&*unwind_event),
            Return::Drop { .. } => Err("State based window didn't provide a boolean return".into()),
        }
    }

    fn on_tick(&mut self, ns: u64) -> Result<Actions> {
        let context = EventContext::new(ns, None);
        let mut unwind_event = Value::const_null();
        let mut event_meta = Value::const_null();
        // We protect against altering event by ensuring event and meta
        // arent accessed during 'compile' time
        if let Some(tick_script) = &self.tick_script {
            let value = tick_script.run(
                &context,
                AggrType::Emit,
                &mut unwind_event, // event
                &mut self.state,   // state for the window
                &mut event_meta,   // $
            )?;

            match value {
                Return::Emit { value, .. } => Actions::try_from(&value),
                Return::EmitEvent { .. } => Actions::try_from(&unwind_event),
                Return::Drop { .. } => {
                    Err("State based window didn't provide a boolean return".into())
                }
            }
        } else {
            Ok(Actions::all_false())
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct TumblingOnTime {
    pub(crate) next_window: Option<u64>,
    pub(crate) max_groups: usize,
    /// How long a window lasts (how many ns we accumulate)
    pub(crate) interval: u64,
    pub(crate) script: Option<WindowDefinition<'static>>,
}

impl TumblingOnTime {
    pub(crate) fn reset(&mut self) {
        self.next_window = None;
    }

    pub fn from_stmt(
        interval: u64,
        max_groups: usize,
        script: Option<&WindowDefinition<'static>>,
    ) -> Self {
        let script = script.cloned();
        Self {
            next_window: None,
            max_groups,
            interval,
            script,
        }
    }

    fn get_window_event(&mut self, time: u64) -> Actions {
        match self.next_window {
            None => {
                self.next_window = Some(time + self.interval);
                Actions::all_false()
            }
            Some(next_window) if next_window <= time => {
                self.next_window = Some(time + self.interval);
                Actions {
                    include: false, // event is beyond the current window, put it into the next
                    emit: true,     // only emit if we had any events in this interval
                }
            }
            Some(_) => Actions::all_false(),
        }
    }
}

impl Trait for TumblingOnTime {
    fn max_groups(&self) -> usize {
        self.max_groups
    }
    fn on_event(
        &mut self,
        data: &mut ValueAndMeta,
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
                    unwind_event,   // event
                    &Value::null(), // state for the window
                    event_meta,     // $
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
    max_groups: usize,
    size: u64,
    next_eviction: u64,
    script: Option<WindowDefinition<'static>>,
}

impl TumblingOnNumber {
    pub(crate) fn reset(&mut self) {
        self.next_eviction = 0;
        self.count = 0;
    }
    pub fn from_stmt(
        size: u64,
        max_groups: usize,
        script: Option<&WindowDefinition<'static>>,
    ) -> Self {
        let script = script.cloned();

        Self {
            max_groups,
            size,
            script,
            ..TumblingOnNumber::default()
        }
    }
}
impl Trait for TumblingOnNumber {
    fn max_groups(&self) -> usize {
        self.max_groups
    }
    fn on_event(
        &mut self,
        data: &mut ValueAndMeta,
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
                    unwind_event,   // event
                    &Value::null(), // state for the window
                    event_meta,     // $
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
