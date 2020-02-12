// Copyright 2018-2020, Wayfair GmbH
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

use crate::errors::*;
use crate::{Event, Operator};
use halfbrown::HashMap;
use simd_json::borrowed::Value;
use std::borrow::{Borrow, Cow};
use std::mem;
use std::sync::Arc;
use tremor_script::interpreter::Env;
use tremor_script::query::StmtRentalWrapper;
use tremor_script::utils::sorsorted_serialize;
use tremor_script::{
    self,
    ast::{InvokeAggrFn, SelectStmt, WindowDecl, ARGS_CONST_ID, GROUP_CONST_ID, WINDOW_CONST_ID},
    prelude::*,
    query::StmtRental,
};

pub type Aggrs<'script> = Vec<InvokeAggrFn<'script>>;

#[derive(Debug, Clone)]
pub struct GroupData<'groups> {
    group: Value<'static>,
    window: WindowImpl,
    aggrs: Aggrs<'groups>,
}
type Groups<'groups> = HashMap<String, GroupData<'groups>>;
rental! {
    pub mod rentals {
        use std::sync::Arc;
        use halfbrown::HashMap;
        use super::*;

        #[rental(covariant,debug,clone)]
        pub struct Dims {
            query: Arc<StmtRental>,
            groups: Groups<'query>,
        }

        #[rental(covariant,debug)]
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
pub use rentals::Dims as SelectDims;

impl SelectDims {
    pub fn from_query(stmt: Arc<StmtRental>) -> Self {
        Self::new(stmt, |_| HashMap::new())
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct TrickleSelect {
    pub id: String,
    pub select: rentals::Select,
    pub windows: Vec<Window>,
}

pub trait WindowTrait: std::fmt::Debug {
    fn on_event(&mut self, event: &Event) -> Result<WindowEvent>;
    fn eviction_ns(&self) -> Option<u64>;
}

#[derive(Debug)]
pub struct Window {
    window_impl: WindowImpl,
    name: String,
    dims: SelectDims,
    last_dims: SelectDims,
    next_swap: u64,
}

impl Window {}

// We allow this since No is barely ever used.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum WindowImpl {
    TumblingCountBased(TumblingWindowOnNumber),
    TumblingTimeBased(TumblingWindowOnTime),
    No(NoWindow),
}

impl std::default::Default for WindowImpl {
    fn default() -> Self {
        TumblingWindowOnTime {
            size: 15_000_000_000,
            next_window: None,
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
    fn eviction_ns(&self) -> Option<u64> {
        match self {
            Self::TumblingTimeBased(w) => w.eviction_ns(),
            Self::TumblingCountBased(w) => w.eviction_ns(),
            Self::No(w) => w.eviction_ns(),
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
pub enum Accumulate {
    Not,
    Before,
    After,
}

#[derive(Debug, PartialEq)]
pub struct WindowEvent {
    /// New window is opened,
    open: bool,
    /// Close the window before this event and opeen the next one
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
        if self.open {
            Ok(WindowEvent {
                open: false,
                emit: true,
            })
        } else {
            self.open = true;
            Ok(WindowEvent {
                open: true,
                emit: true,
            })
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct TumblingWindowOnTime {
    next_window: Option<u64>,
    size: u64,
    ttl: Option<u64>,
    script: Option<rentals::Window>,
}
impl TumblingWindowOnTime {
    pub fn from_stmt(
        size: u64,
        ttl: Option<u64>,
        script: Option<&WindowDecl>,
        stmt: &StmtRentalWrapper,
    ) -> Self {
        let script = script.map(|s| {
            rentals::Window::new(stmt.stmt.clone(), |_| unsafe { mem::transmute(s.clone()) })
        });
        Self {
            next_window: None,
            size,
            ttl,
            script,
        }
    }
}

impl WindowTrait for TumblingWindowOnTime {
    fn eviction_ns(&self) -> Option<u64> {
        self.ttl
    }
    fn on_event(&mut self, event: &Event) -> Result<WindowEvent> {
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
                    &mut unwind_event, // event
                    &mut event_meta,   // $
                )?;
                let data = match value {
                    Return::Emit { value, .. } => value.as_u64(),
                    Return::EmitEvent { .. } => unwind_event.as_u64(),
                    Return::Drop { .. } => None,
                };
                data.ok_or_else(|| Error::from("Data based window didn't provide a valid value"))
            })
            .unwrap_or(Ok(event.ingest_ns))?;
        match self.next_window {
            None => {
                self.next_window = Some(time + self.size);
                Ok(WindowEvent {
                    open: true,
                    emit: false,
                })
            }
            Some(next_window) if next_window <= time => {
                self.next_window = Some(time + self.size);
                Ok(WindowEvent {
                    open: true,
                    emit: true,
                })
            }
            Some(_) => Ok(WindowEvent {
                open: false,
                emit: false,
            }),
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct TumblingWindowOnNumber {
    count: u64,
    size: u64,
    ttl: Option<u64>,
    script: Option<rentals::Window>,
}

impl TumblingWindowOnNumber {
    pub fn from_stmt(
        size: u64,
        ttl: Option<u64>,
        script: Option<&WindowDecl>,
        stmt: &StmtRentalWrapper,
    ) -> Self {
        let script = script.map(|s| {
            rentals::Window::new(stmt.stmt.clone(), |_| unsafe { mem::transmute(s.clone()) })
        });
        Self {
            count: 0,
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
    fn on_event(&mut self, event: &Event) -> Result<WindowEvent> {
        let count = self
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
                    &mut unwind_event, // event
                    &mut event_meta,   // $
                )?;
                let data = match value {
                    Return::Emit { value, .. } => value.as_u64(),
                    Return::EmitEvent { .. } => unwind_event.as_u64(),
                    Return::Drop { .. } => None,
                };
                data.ok_or_else(|| Error::from("Data based window didn't provide a valid value"))
            })
            .unwrap_or(Ok(1))?;

        // If we're above count we emit and  set the new count to 1
        // ( we emit on the ) previous event
        if self.count >= self.size {
            self.count = count;
            Ok(WindowEvent {
                open: true,
                emit: true,
            })
        } else {
            self.count += count;
            Ok(WindowEvent {
                open: false,
                emit: false,
            })
        }
    }
}

const NO_AGGRS: [InvokeAggrFn<'static>; 0] = [];

impl TrickleSelect {
    pub fn with_stmt(
        id: String,
        dims: &SelectDims,
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
            .map(|(name, window_impl)| Window {
                dims: dims.clone(),
                last_dims: dims.clone(),
                name,
                window_impl,
                next_swap: 0,
            })
            .collect();
        Ok(Self {
            id,
            windows,
            select: rentals::Select::new(stmt_rentwrapped.stmt.clone(), move |_| unsafe {
                std::mem::transmute(select)
            }),
        })
    }
    fn opts() -> ExecOpts {
        ExecOpts {
            result_needed: true,
            aggr: AggrType::Emit,
        }
    }
}

impl Operator for TrickleSelect {
    #[allow(
        mutable_transmutes,
        clippy::transmute_ptr_to_ptr,
        clippy::too_many_lines
    )]
    fn on_event(&mut self, _port: &str, event: Event) -> Result<Vec<(Cow<'static, str>, Event)>> {
        let opts = Self::opts();
        // We guarantee at compile time that select in itself can't have locals, so this is safe

        // NOTE We are unwrapping our rental wrapped stmt
        let SelectStmt {
            stmt,
            aggregates,
            consts,
            locals,
            node_meta,
        }: &mut SelectStmt = unsafe { std::mem::transmute(self.select.suffix()) };
        let local_stack = tremor_script::interpreter::LocalStack::with_size(*locals);
        consts[WINDOW_CONST_ID] = Value::null();
        consts[GROUP_CONST_ID] = Value::null();
        consts[ARGS_CONST_ID] = Value::null();
        // TODO avoid origin_uri clone here
        let ctx = EventContext::new(event.ingest_ns, event.origin_uri.clone());

        //
        // Before any select processing, we filter by where clause
        //
        // FIXME: ?
        if let Some(guard) = &stmt.maybe_where {
            let (unwind_event, event_meta) = event.data.parts();
            let env = Env {
                context: &ctx,
                consts: &consts,
                aggrs: &NO_AGGRS,
                meta: &node_meta,
            };
            let test = guard.run(opts, &env, unwind_event, event_meta, &local_stack)?;
            if let Some(test) = test.as_bool() {
                if !test {
                    return Ok(vec![]);
                };
            } else {
                return tremor_script::errors::query_guard_not_bool(
                    &stmt, guard, &test, &node_meta,
                )?;
            };
        }

        let mut events = vec![];

        let mut group_values = {
            let data = event.data.suffix();
            let unwind_event: &Value<'_> = unsafe { std::mem::transmute(&data.value) };
            let event_meta: &Value<'_> = unsafe { std::mem::transmute(&data.meta) };
            if let Some(group_by) = &stmt.maybe_group_by {
                group_by.generate_groups(&ctx, &unwind_event, &node_meta, &event_meta)?
            } else {
                vec![]
            }
        };
        if self.windows.is_empty() && group_values.is_empty() {
            let group_value = Value::from(vec![()]);
            let group_str = sorsorted_serialize(&group_value)?;

            let data = event.data.suffix();
            #[allow(clippy::transmute_ptr_to_ptr)]
            let unwind_event: &mut Value<'_> = unsafe { std::mem::transmute(&data.value) };
            #[allow(clippy::transmute_ptr_to_ptr)]
            let event_meta: &Value<'_> = unsafe { std::mem::transmute(&data.meta) };
            consts[GROUP_CONST_ID] = group_value.clone_static();
            consts[GROUP_CONST_ID].push(group_str).ok();

            let env = Env {
                context: &ctx,
                consts: &consts,
                aggrs: &NO_AGGRS,
                meta: &node_meta,
            };
            let value = stmt
                .target
                .run(opts, &env, unwind_event, event_meta, &local_stack)?;

            let result = value.into_owned();
            if let Some(guard) = &stmt.maybe_having {
                let test = guard.run(opts, &env, &result, &NULL, &local_stack)?;
                if let Some(test) = test.as_bool() {
                    if !test {
                        return Ok(vec![]);
                    }
                } else {
                    return tremor_script::errors::query_guard_not_bool(
                        stmt.borrow(),
                        guard,
                        &test,
                        &node_meta,
                    )?;
                }
            }
            *unwind_event = result;
            return Ok(vec![("out".into(), event)]);
        }
        if group_values.is_empty() {
            group_values.push(vec![Value::null()])
        };

        // Handle eviction

        for window in &mut self.windows {
            if let Some(eviction_ns) = window.window_impl.eviction_ns() {
                if window.next_swap < event.ingest_ns {
                    window.next_swap = event.ingest_ns + eviction_ns;
                    let this_groups: &mut Groups =
                        unsafe { std::mem::transmute(window.dims.suffix()) };
                    let last_groups: &mut Groups =
                        unsafe { std::mem::transmute(window.last_dims.suffix()) };
                    last_groups.clear();
                    std::mem::swap(this_groups, last_groups);
                }
            }
        }

        let group_values: Vec<Value> = group_values.into_iter().map(Value::Array).collect();
        for group_value in group_values {
            let group_str = sorsorted_serialize(&group_value)?;
            let mut windows = self.windows.iter_mut().peekable();
            let mut emit_depth = 0;

            // We first iterate through the windows and emit as far as we would have to emit.
            while let Some(this) = windows.next() {
                let this_groups: &mut Groups = unsafe { std::mem::transmute(this.dims.suffix()) };
                let (_, this_group) = this_groups
                    .raw_entry_mut()
                    .from_key(&group_str)
                    .or_insert_with(|| {
                        let last_groups: &mut Groups =
                            unsafe { std::mem::transmute(this.last_dims.suffix()) };
                        (
                            group_str.clone(),
                            last_groups.remove(&group_str).unwrap_or_else(|| GroupData {
                                window: this.window_impl.clone(),
                                aggrs: aggregates.clone(),
                                group: group_value.clone_static(),
                            }),
                        )
                    });
                let window_event = this_group.window.on_event(&event)?;
                // FIXME
                // The issue with the windows is the following:
                // We emit on the first event of the next windows, this works well for the inital frame
                // on a second frame, we have the coordinate with the first we have a problem as we
                // merge the higher resolution data into the lower resolution data before we get a chance
                // to emit the lower res frame - causing us to have an element too much
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

                // If this window should emit
                if window_event.emit {
                    emit_depth += 1;
                    // See if we need to merge into the next tiltframe
                    // If so merge the aggregates
                    // Then emit the window itself
                    consts[WINDOW_CONST_ID] = Value::String(this.name.to_string().into());
                    let data = event.data.suffix();
                    #[allow(clippy::transmute_ptr_to_ptr)]
                    let unwind_event: &Value<'_> = unsafe { std::mem::transmute(&data.value) };
                    #[allow(clippy::transmute_ptr_to_ptr)]
                    let event_meta: &Value<'_> = unsafe { std::mem::transmute(&data.meta) };
                    consts[GROUP_CONST_ID] = group_value.clone_static();
                    consts[GROUP_CONST_ID].push(group_str.clone()).ok();

                    let env = Env {
                        context: &ctx,
                        consts: &consts,
                        aggrs: &this_group.aggrs,
                        meta: &node_meta,
                    };
                    let value =
                        stmt.target
                            .run(opts, &env, unwind_event, event_meta, &local_stack)?;

                    let result = value.into_owned();
                    if let Some(guard) = &stmt.maybe_having {
                        let test = guard.run(opts, &env, &result, &NULL, &local_stack)?;
                        if let Some(test) = test.as_bool() {
                            if !test {
                                continue;
                            }
                        } else {
                            return tremor_script::errors::query_guard_not_bool(
                                stmt.borrow(),
                                guard,
                                &test,
                                &node_meta,
                            )?;
                        }
                    }
                    events.push((
                        "out".into(),
                        Event {
                            id: event.id,
                            ingest_ns: event.ingest_ns,
                            // TODO avoid origin_uri clone here
                            origin_uri: event.origin_uri.clone(),
                            is_batch: event.is_batch,
                            kind: event.kind,
                            data: (result, event_meta.clone()).into(),
                        },
                    ));
                } else {
                    break;
                }
            }

            // Next we take care of propagating the data from narrower to wider
            // windows and clearning windows that we have already emitted (in this
            // order!).

            // calculate the count (depth + 1) since in the remaining code
            // we do not care about the index
            let emit_count = emit_depth;
            // Check if we are emitting all the windows, if not we need to take
            // special care in the code below
            let mut clear_all = emit_count == self.windows.len();
            let mut windows = self
                .windows
                .iter_mut()
                // We look one beyond the emit count this is required
                // since we go 'backwards' from the widest to the narrowest.
                // Since we do not want to clean the next not emitted
                // frame we calcualted emit_all above.
                .take(emit_count + 1)
                // We reverse the iterator since we want to start with
                // the widest window
                .rev()
                .peekable();

            while let Some(this) = windows.next() {
                // First start with getting our group
                let this_groups: &mut Groups = unsafe { std::mem::transmute(this.dims.suffix()) };
                let (_, this_group) = this_groups
                    .raw_entry_mut()
                    .from_key(&group_str)
                    .or_insert_with(|| {
                        let last_groups: &mut Groups =
                            unsafe { std::mem::transmute(this.last_dims.suffix()) };
                        (
                            group_str.clone(),
                            last_groups.remove(&group_str).unwrap_or_else(|| GroupData {
                                window: this.window_impl.clone(),
                                aggrs: aggregates.clone(),
                                group: group_value.clone_static(),
                            }),
                        )
                    });

                // Check if we want to clear all the following window
                // this is false for a non terminal widest window
                if clear_all {
                    for aggr in &this_group.aggrs {
                        let aggr_static: &mut InvokeAggrFn<'static> =
                            unsafe { std::mem::transmute(aggr) };
                        aggr_static.invocable.init();
                    }
                } else {
                    // If we skipped the widest window we can clear the rest
                    clear_all = true;
                }

                // Remember we iterate backwards so we the next element in the iterator
                // is the previous window.
                // We grab it and then merge the data from the previous window into this
                // This ensures that in the next iteration of the leep we can clear
                // the previous window since we already pulled all needed data out here.
                if let Some(prev) = windows.peek() {
                    let prev_groups: &mut Groups =
                        unsafe { std::mem::transmute(prev.dims.suffix()) };
                    let (_, prev_group) = prev_groups
                        .raw_entry_mut()
                        .from_key(&group_str)
                        .or_insert_with(|| {
                            let last_groups: &mut Groups =
                                unsafe { std::mem::transmute(prev.last_dims.suffix()) };
                            (
                                group_str.clone(),
                                last_groups.remove(&group_str).unwrap_or_else(|| GroupData {
                                    window: prev.window_impl.clone(),
                                    aggrs: aggregates.clone(),
                                    group: group_value.clone_static(),
                                }),
                            )
                        });
                    for (i, aggr) in prev_group.aggrs.iter().enumerate() {
                        let aggr_static: &InvokeAggrFn<'static> =
                            unsafe { std::mem::transmute(aggr) };
                        this_group.aggrs[i]
                            .invocable
                            .merge(&aggr_static.invocable)
                            .map_err(|e| {
                                // FIXME nice error
                                let r: Option<&Registry> = None;
                                e.into_err(aggr, aggr, r, &node_meta)
                            })?;
                    }
                }
            }

            // If we had at least one window ingest the event into this window
            if let Some(this) = self.windows.first() {
                let (unwind_event, event_meta) = event.data.parts();
                consts[WINDOW_CONST_ID] = Value::String(this.name.to_string().into());

                let this_groups: &mut Groups = unsafe { std::mem::transmute(this.dims.suffix()) };
                let (_, this_group) = this_groups
                    .raw_entry_mut()
                    .from_key(&group_str)
                    .or_insert_with(|| {
                        (
                            group_str.clone(),
                            GroupData {
                                window: this.window_impl.clone(),
                                aggrs: aggregates.clone(),
                                group: group_value.clone_static(),
                            },
                        )
                    });
                consts[GROUP_CONST_ID] = group_value.clone_static();
                consts[GROUP_CONST_ID].push(group_str.clone()).ok();

                let env = Env {
                    context: &ctx,
                    consts: &consts,
                    aggrs: &NO_AGGRS,
                    meta: &node_meta,
                };
                for aggr in &mut this_group.aggrs {
                    let invocable = &mut aggr.invocable;
                    let mut argv: Vec<Cow<Value>> = Vec::with_capacity(aggr.args.len());
                    let mut argv1: Vec<&Value> = Vec::with_capacity(aggr.args.len());
                    for arg in &aggr.args {
                        let result =
                            arg.run(opts, &env, unwind_event, &event_meta, &local_stack)?;
                        argv.push(result);
                    }
                    unsafe {
                        for i in 0..argv.len() {
                            argv1.push(argv.get_unchecked(i));
                        }
                    }
                    invocable.accumulate(argv1.as_slice()).map_err(|e| {
                        // FIXME nice error
                        let r: Option<&Registry> = None;
                        e.into_err(aggr, aggr, r, &node_meta)
                    })?;
                }
            } else {
                // otherwise we just pass it through the select portion of the statement
                let data = event.data.suffix();
                #[allow(clippy::transmute_ptr_to_ptr)]
                let unwind_event: &mut Value<'_> = unsafe { std::mem::transmute(&data.value) };
                #[allow(clippy::transmute_ptr_to_ptr)]
                let event_meta: &Value<'_> = unsafe { std::mem::transmute(&data.meta) };
                consts[GROUP_CONST_ID] = group_value.clone_static();
                consts[GROUP_CONST_ID].push(group_str.clone()).ok();

                let env = Env {
                    context: &ctx,
                    consts: &consts,
                    aggrs: &NO_AGGRS,
                    meta: &node_meta,
                };
                let value = stmt
                    .target
                    .run(opts, &env, unwind_event, event_meta, &local_stack)?;

                let result = value.into_owned();
                if let Some(guard) = &stmt.maybe_having {
                    let test = guard.run(opts, &env, &result, &NULL, &local_stack)?;
                    if let Some(test) = test.as_bool() {
                        if !test {
                            continue;
                        }
                    } else {
                        return tremor_script::errors::query_guard_not_bool(
                            stmt.borrow(),
                            guard,
                            &test,
                            &node_meta,
                        )?;
                    }
                }
                events.push((
                    "out".into(),
                    Event {
                        id: event.id,
                        ingest_ns: event.ingest_ns,
                        // TODO avoid origin_uri clone here
                        origin_uri: event.origin_uri.clone(),
                        is_batch: event.is_batch,
                        kind: event.kind,
                        data: (result.into_static(), event_meta.clone()).into(),
                    },
                ));
            }
        }
        Ok(events)
    }
}

#[cfg(test)]
mod test {
    #![allow(clippy::float_cmp)]
    use super::*;
    use simd_json::borrowed::Value;
    use simd_json::json;
    use tremor_script::ast::{self, Ident, ImutExpr, Literal};

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
            node_meta: ast::NodeMetas::default(),
        }
    }

    fn test_event(s: u64) -> Event {
        Event {
            origin_uri: None,
            is_batch: false,
            id: s,
            ingest_ns: s * 1_000_000_000,
            data: Value::from(json!({
               "h2g2" : 42,
            }))
            .into(),
            kind: None,
        }
    }

    use std::sync::Arc;

    fn test_select(stmt: tremor_script::query::StmtRentalWrapper) -> Result<TrickleSelect> {
        let groups = SelectDims::from_query(stmt.stmt.clone());
        let windows = vec![
            (
                "15s".into(),
                TumblingWindowOnTime {
                    ttl: None,
                    size: 15_000_000_000,
                    next_window: None,
                    script: None,
                }
                .into(),
            ),
            (
                "30s".into(),
                TumblingWindowOnTime {
                    ttl: None,
                    size: 30_000_000_000,
                    next_window: None,
                    script: None,
                }
                .into(),
            ),
        ];
        let id = "select".to_string();
        TrickleSelect::with_stmt(id, &groups, windows, &stmt)
    }

    fn try_enqueue(
        op: &mut TrickleSelect,
        event: Event,
    ) -> Result<Option<(Cow<'static, str>, Event)>> {
        let mut action = op.on_event("in", event)?;
        let first = action.pop();
        if action.is_empty() {
            Ok(first)
        } else {
            Ok(None)
        }
    }

    fn try_enqueue_two(
        op: &mut TrickleSelect,
        event: Event,
    ) -> Result<Option<[(Cow<'static, str>, Event); 2]>> {
        let mut action = op.on_event("in", event)?;
        let r = action
            .pop()
            .and_then(|second| Some([action.pop()?, second]));
        if action.is_empty() {
            Ok(r)
        } else {
            Ok(None)
        }
    }

    fn parse_query(query: &str) -> Result<crate::op::trickle::select::TrickleSelect> {
        let reg = tremor_script::registry();
        let aggr_reg = tremor_script::aggr_registry();
        let query = tremor_script::query::Query::parse(query, &reg, &aggr_reg)?;

        let stmt_rental = tremor_script::query::StmtRental::new(Arc::new(query.clone()), |q| {
            q.suffix().stmts[0].clone()
        });
        let stmt = tremor_script::query::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };
        Ok(test_select(stmt)?)
    }

    #[test]
    fn test_sum() -> Result<()> {
        let mut op = parse_query("select stats::sum(event.h2g2) from in into out;")?;
        assert!(try_enqueue(&mut op, test_event(0))?.is_none());
        assert!(try_enqueue(&mut op, test_event(1))?.is_none());
        let (out, event) =
            try_enqueue(&mut op, test_event(15))?.expect("no event emitted after aggregation");
        assert_eq!("out", out);

        assert_eq!(event.data.suffix().value, 84.0);
        Ok(())
    }

    #[test]
    fn test_count() -> Result<()> {
        let mut op = parse_query("select stats::count() from in into out;")?;
        assert!(try_enqueue(&mut op, test_event(0))?.is_none());
        assert!(try_enqueue(&mut op, test_event(1))?.is_none());
        let (out, event) = try_enqueue(&mut op, test_event(15))?.expect("no event");
        assert_eq!("out", out);

        assert_eq!(event.data.suffix().value, 2);
        Ok(())
    }

    #[test]
    fn count_tilt() -> Result<()> {
        // Windows are 15s and 30s
        let mut op = parse_query("select stats::count() from in into out;")?;
        // Insert two events prior to 15
        assert!(try_enqueue(&mut op, test_event(0))?.is_none());
        assert!(try_enqueue(&mut op, test_event(1))?.is_none());
        // Add one event at 15, this flushes the prior two and set this to one.
        // This is the first time the second frame sees an event so it's timer
        // starts at 15.
        let (out, event) = try_enqueue(&mut op, test_event(15))?.expect("no event 1");
        assert_eq!("out", out);
        assert_eq!(event.data.suffix().value, 2);
        // Add another event prior to 30
        assert!(try_enqueue(&mut op, test_event(16))?.is_none());
        // This emits only the initial event since the rollup
        // will only be emitted once it gets the first event
        // if the next window
        let (out, event) = try_enqueue(&mut op, test_event(30))?.expect("no event 2");
        assert_eq!("out", out);
        assert_eq!(event.data.suffix().value, 2);
        // Add another event prior to 45 to the first window
        assert!(try_enqueue(&mut op, test_event(31))?.is_none());

        // At 45 the 15s window emits the rollup with the previous data
        let [(out1, event1), (out2, event2)] =
            try_enqueue_two(&mut op, test_event(45))?.expect("no event 3");

        assert_eq!("out", out1);
        assert_eq!("out", out2);
        assert_eq!(event1.data.suffix().value, 2);
        assert_eq!(event2.data.suffix().value, 4);
        assert!(try_enqueue(&mut op, test_event(46))?.is_none());

        // Add 60 only the 15s window emits
        let (out, event) = try_enqueue(&mut op, test_event(60))?.expect("no event 4");
        assert_eq!("out", out);
        assert_eq!(event.data.suffix().value, 2);
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

        let mut op = test_select(stmt)?;
        assert!(try_enqueue(&mut op, test_event(0))?.is_none());
        assert!(try_enqueue(&mut op, test_event(1))?.is_none());
        let (out, event) = try_enqueue(&mut op, test_event(15))?.expect("no event");
        assert_eq!("out", out);

        assert_eq!(event.data.suffix().value, 42);
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

        let mut op = test_select(stmt)?;

        assert!(try_enqueue(&mut op, test_event(0))?.is_none());

        let (out, event) = try_enqueue(&mut op, test_event(15))?.expect("no event");
        assert_eq!("out", out);

        assert_eq!(event.data.suffix().value, 42);
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

        let mut op = test_select(stmt)?;
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
            value: Value::String("snot".into()),
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

        let mut op = test_select(stmt)?;

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

        let mut op = test_select(stmt)?;

        let event = test_event(0);
        assert!(try_enqueue(&mut op, event)?.is_none());

        let event = test_event(15);
        let (out, event) = try_enqueue(&mut op, event)?.expect("no event");
        assert_eq!("out", out);
        assert_eq!(event.data.suffix().value, 42);
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

        let mut op = test_select(stmt)?;
        let event = test_event(0);

        let next = try_enqueue(&mut op, event)?;

        assert_eq!(None, next);
        Ok(())
    }

    fn test_select_stmt(stmt: tremor_script::ast::Select) -> tremor_script::ast::Stmt {
        ast::Stmt::Select(SelectStmt {
            stmt: Box::new(stmt),
            aggregates: vec![],
            consts: vec![Value::null(), Value::null(), Value::null()],
            locals: 0,
            node_meta: ast::NodeMetas::default(),
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

        let mut op = test_select(stmt)?;
        let event = test_event(0);

        let next = try_enqueue(&mut op, event)?;

        // FIXME TODO - would be nicer to get error output in tests
        // syntax highlighted in capturable form for assertions
        assert_eq!(None, next);
        Ok(())
    }
}
