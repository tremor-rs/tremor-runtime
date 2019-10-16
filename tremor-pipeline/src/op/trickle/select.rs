// Copyright 2018-2019, Wayfair GmbH
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

use crate::errors::*;
use crate::{Event, Operator};
use halfbrown::HashMap;
use simd_json::borrowed::Value;
use std::borrow::{Borrow, Cow};
use std::mem;
use std::sync::Arc;
use tremor_script::query::StmtRentalWrapper;
use tremor_script::{
    self,
    ast::{InvokeAggrFn, SelectStmt, WindowDecl, ARGS_CONST_ID, GROUP_CONST_ID, WINDOW_CONST_ID},
    prelude::*,
    query::rentals::Stmt as StmtRental,
};

pub type Aggrs<'script> = Vec<InvokeAggrFn<'script>>;
type Groups<'groups> = HashMap<String, (Value<'static>, Aggrs<'groups>)>;
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
    fn on_event(&mut self, event: &Event) -> WindowEvent;
}

#[derive(Debug)]
pub struct Window {
    window_impl: WindowImpl,
    name: String,
    dims: SelectDims,
}

impl Window {}

impl WindowTrait for Window {
    fn on_event(&mut self, event: &Event) -> WindowEvent {
        self.window_impl.on_event(event)
    }
}

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
        }
        .into()
    }
}

impl WindowTrait for WindowImpl {
    fn on_event(&mut self, event: &Event) -> WindowEvent {
        match self {
            Self::TumblingTimeBased(w) => w.on_event(event),
            Self::TumblingCountBased(w) => w.on_event(event),
            Self::No(w) => w.on_event(event),
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
    pub open: bool,
    /// Close the window before this event and opeen the next one
    pub emit: bool,
}

#[derive(Default, Debug, Clone)]
pub struct NoWindow {
    open: bool,
}

impl WindowTrait for NoWindow {
    fn on_event(&mut self, _event: &Event) -> WindowEvent {
        if self.open {
            WindowEvent {
                open: false,
                emit: true,
            }
        } else {
            self.open = true;
            WindowEvent {
                open: true,
                emit: true,
            }
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct TumblingWindowOnTime {
    pub next_window: Option<u64>,
    pub size: u64,
    script: Option<rentals::Window>,
}
impl TumblingWindowOnTime {
    pub fn from_stmt(size: u64, script: Option<&WindowDecl>, stmt: &StmtRentalWrapper) -> Self {
        let script = script.map(|s| {
            rentals::Window::new(stmt.stmt.clone(), |_| unsafe { mem::transmute(s.clone()) })
        });
        Self {
            next_window: None,
            size,
            script,
        }
    }
}

impl WindowTrait for TumblingWindowOnTime {
    fn on_event(&mut self, event: &Event) -> WindowEvent {
        let time = if let Some(script) = self
            .script
            .as_ref()
            .and_then(|s| s.suffix().script.as_ref())
        {
            let context = EventContext::from_ingest_ns(event.ingest_ns);
            let (mut unwind_event, mut event_meta) = event.data.parts();
            let value = script.run(
                &context,
                AggrType::Emit,
                &mut unwind_event, // event
                &mut event_meta,   // $
            );
            let data = match value {
                Ok(Return::Emit { value, .. }) => value.as_u64(),
                _ => unimplemented!(),
            };
            if let Some(time) = data {
                time
            } else {
                unimplemented!();
            }
        } else {
            event.ingest_ns
        };
        match self.next_window {
            None => {
                self.next_window = Some(time + self.size);
                WindowEvent {
                    open: true,
                    emit: false,
                }
            }
            Some(next_window) if next_window <= time => {
                self.next_window = Some(time + self.size);
                WindowEvent {
                    open: true,
                    emit: true,
                }
            }
            Some(_) => WindowEvent {
                open: false,
                emit: false,
            },
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct TumblingWindowOnNumber {
    pub next_window: Option<u64>,
    pub size: u64,
    script: Option<rentals::Window>,
}

impl TumblingWindowOnNumber {
    pub fn from_stmt(size: u64, script: Option<&WindowDecl>, stmt: &StmtRentalWrapper) -> Self {
        let script = script.map(|s| {
            rentals::Window::new(stmt.stmt.clone(), |_| unsafe { mem::transmute(s.clone()) })
        });
        Self {
            next_window: None,
            size,
            script,
        }
    }
}
impl WindowTrait for TumblingWindowOnNumber {
    fn on_event(&mut self, event: &Event) -> WindowEvent {
        let count = if let Some(script) = self
            .script
            .as_ref()
            .and_then(|s| s.suffix().script.as_ref())
        {
            let context = EventContext::from_ingest_ns(event.ingest_ns);
            let (mut unwind_event, mut event_meta) = event.data.parts();
            let value = script.run(
                &context,
                AggrType::Emit,
                &mut unwind_event, // event
                &mut event_meta,   // $
            );
            let data = match value {
                Ok(Return::Emit { value, .. }) => value.as_u64(),
                _ => unimplemented!(),
            };
            if let Some(count) = data {
                count
            } else {
                unimplemented!();
            }
        } else {
            1
        };
        match self.next_window {
            None => {
                self.next_window = Some(0);
                WindowEvent {
                    open: true,
                    emit: false,
                }
            }
            Some(next_window) if next_window < (self.size - count) => {
                self.next_window = Some(next_window + count);
                WindowEvent {
                    open: false,
                    emit: false,
                }
            }
            Some(next_window) if next_window >= (self.size - count) => {
                self.next_window = Some(0);
                WindowEvent {
                    open: true,
                    emit: true,
                }
            }
            Some(_) => WindowEvent {
                // FIXME should never occur in practice
                open: false,
                emit: false,
            },
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
        let select = match stmt_rentwrapped.stmt.suffix() {
            tremor_script::ast::Stmt::Select(ref select) => select.clone(),
            _ => {
                return Err(ErrorKind::PipelineError(
                    "Trying to turn a non select into a select operator".into(),
                )
                .into())
            }
        };

        // FIXME - ensure that windows are sensilbe (i.e. they are ever growing multiples of each other  unwrap()
        let windows = windows
            .into_iter()
            .map(|(name, window_impl)| Window {
                dims: dims.clone(),
                name,
                window_impl,
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
    #[allow(clippy::transmute_ptr_to_ptr)]
    #[allow(mutable_transmutes)]
    fn on_event(&mut self, _port: &str, event: Event) -> Result<Vec<(String, Event)>> {
        let opts = Self::opts();
        // We guarantee at compile time that select in itself can't have locals, so this is safe
        let local_stack = tremor_script::interpreter::LocalStack::with_size(0);

        // NOTE We are unwrapping our rental wrapped stmt
        let SelectStmt {
            stmt,
            aggregates,
            consts,
        }: &mut SelectStmt = unsafe { std::mem::transmute(self.select.suffix()) };
        consts[WINDOW_CONST_ID] = Value::Null;
        consts[GROUP_CONST_ID] = Value::Null;
        consts[ARGS_CONST_ID] = Value::Null;
        let ctx = EventContext::from_ingest_ns(event.ingest_ns);

        //
        // Before any select processing, we filter by where clause
        //
        // FIXME: ?
        if let Some(guard) = &stmt.maybe_where {
            let (unwind_event, event_meta) = event.data.parts();
            let test = guard.run(
                opts,
                &ctx,
                &NO_AGGRS,
                unwind_event,
                event_meta,
                &local_stack,
                &consts,
            )?;
            match test.borrow() {
                Value::Bool(true) => (),
                Value::Bool(false) => {
                    return Ok(vec![]);
                }
                other => {
                    return tremor_script::errors::query_guard_not_bool(&stmt, guard, &other)?;
                }
            };
        }

        let mut events = vec![];

        let mut group_values = vec![];
        {
            let data = event.data.suffix();
            #[allow(clippy::transmute_ptr_to_ptr)]
            #[allow(mutable_transmutes)]
            let unwind_event: &Value<'_> = unsafe { std::mem::transmute(&data.value) };
            #[allow(clippy::transmute_ptr_to_ptr)]
            #[allow(mutable_transmutes)]
            let event_meta: &Value<'_> = unsafe { std::mem::transmute(&data.meta) };
            if let Some(group_by) = &stmt.maybe_group_by {
                group_by.generate_groups(&ctx, &unwind_event, &event_meta, &mut group_values)?
            };
        }
        if group_values.is_empty() {
            group_values.push(vec![Value::Null])
        };
        let mut itr = self.windows.iter_mut().peekable();
        while let Some(this) = itr.next() {
            let window_event = this.on_event(&event);
            let groups: &mut Groups = unsafe { std::mem::transmute(this.dims.suffix()) };
            if window_event.emit {
                // If we would emit first merge the data to the next window (if there is any)
                if let Some(next) = itr.peek() {
                    for (group, (group_val, aggrs)) in groups.iter() {
                        let next_groups: &mut Groups =
                            unsafe { std::mem::transmute(next.dims.suffix()) };
                        let (_, next_aggrs) = next_groups
                            .entry(group.clone())
                            .or_insert_with(|| (group_val.clone(), aggregates.clone()));
                        for (i, aggr) in aggrs.iter().enumerate() {
                            // I HATE YOU RUST WHY DO I HAVE TO TRANSMUTE HERE!?!?
                            let aggr_static: &InvokeAggrFn<'static> =
                                unsafe { std::mem::transmute(aggr) };
                            next_aggrs[i]
                                .invocable
                                .merge(&aggr_static.invocable)
                                .map_err(|e| {
                                    // FIXME nice error
                                    let r: Option<&Registry> = None;
                                    e.into_err(aggr, aggr, r)
                                })?;
                        }
                    }
                }
                consts[WINDOW_CONST_ID] = Value::String(this.name.to_string().into());

                for (_group, (group, aggrs)) in groups.iter() {
                    let data = event.data.suffix();
                    #[allow(clippy::transmute_ptr_to_ptr)]
                    let unwind_event: &Value<'_> = unsafe { std::mem::transmute(&data.value) };
                    #[allow(clippy::transmute_ptr_to_ptr)]
                    let event_meta: &Value<'_> = unsafe { std::mem::transmute(&data.meta) };
                    consts[GROUP_CONST_ID] = group.clone_static();

                    let value = stmt.target.run(
                        opts,
                        &ctx,
                        &aggrs,
                        unwind_event,
                        event_meta,
                        &local_stack,
                        &consts,
                    )?;

                    let result = value.into_owned();
                    if let Some(guard) = &stmt.maybe_having {
                        let test = guard.run(
                            opts,
                            &ctx,
                            &aggrs,
                            &result,
                            &Value::Null,
                            &local_stack,
                            &consts,
                        )?;
                        match test.borrow() {
                            Value::Bool(true) => (),
                            Value::Bool(false) => {
                                continue;
                            }
                            other => {
                                return tremor_script::errors::query_guard_not_bool(
                                    stmt.borrow(),
                                    guard,
                                    &other,
                                )?;
                            }
                        };
                    }
                    events.push((
                        "out".to_string(),
                        Event {
                            id: event.id,
                            ingest_ns: event.ingest_ns,
                            is_batch: event.is_batch,
                            kind: event.kind,
                            data: (result, event_meta.clone()).into(),
                        },
                    ));
                }
            }
            if window_event.open {
                // When we close the window we clear all the aggregates
                groups.clear()
            }
            if !window_event.emit {
                break;
            }
        }

        if let Some(this) = self.windows.first() {
            let groups: &mut Groups = unsafe { std::mem::transmute(this.dims.suffix()) };
            let data = event.data.suffix();
            #[allow(clippy::transmute_ptr_to_ptr)]
            #[allow(mutable_transmutes)]
            let unwind_event: &mut Value<'_> = unsafe { std::mem::transmute(&data.value) };
            #[allow(clippy::transmute_ptr_to_ptr)]
            #[allow(mutable_transmutes)]
            let event_meta: &mut Value<'_> = unsafe { std::mem::transmute(&data.meta) };
            consts[WINDOW_CONST_ID] = Value::String(this.name.to_string().into());
            for group in group_values {
                let group = Value::Array(group);
                let (_, aggrs) = groups
                    .entry(group.encode())
                    .or_insert_with(|| (group.clone_static(), aggregates.clone()));
                consts[GROUP_CONST_ID] = group.clone_static();
                for aggr in aggrs.iter_mut() {
                    let invocable = &mut aggr.invocable;
                    let mut argv: Vec<Cow<Value>> = Vec::with_capacity(aggr.args.len());
                    let mut argv1: Vec<&Value> = Vec::with_capacity(aggr.args.len());
                    for arg in &aggr.args {
                        let result = arg.run(
                            opts,
                            &ctx,
                            &NO_AGGRS,
                            unwind_event,
                            &event_meta,
                            &local_stack,
                            &consts,
                        )?;
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
                        e.into_err(aggr, aggr, r)
                    })?;
                }
            }
        } else {
            // After having has been applied to any emissions causal on this
            // event, we prepare the target expression synthetic event and
            // return it for downstream processing
            //
            // FIXME: This can be nicer, got to look at run for tremor script

            let (unwind_event, event_meta) = event.data.parts();

            let value = stmt.target.run(
                opts,
                &ctx,
                &NO_AGGRS,
                unwind_event,
                &event_meta,
                &local_stack,
                &consts,
            )?;

            *unwind_event = value.into_owned();
            if let Some(guard) = &stmt.maybe_having {
                let test = guard.run(
                    opts,
                    &ctx,
                    &NO_AGGRS,
                    unwind_event,
                    &Value::Null,
                    &local_stack,
                    &consts,
                )?;
                match test.into_owned() {
                    Value::Bool(true) => (),
                    Value::Bool(false) => {
                        return Ok(vec![]);
                    }
                    other => {
                        return tremor_script::errors::query_guard_not_bool(
                            stmt.borrow(),
                            guard,
                            &other,
                        )?;
                    }
                };
            }
            events.push(("out".to_string(), event));
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
    use tremor_script::ast;
    use tremor_script::pos::Location;

    fn test_target<'test>() -> ast::ImutExpr<'test> {
        let target: ast::ImutExpr<'test> = ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::I64(42),
        });
        target
    }

    fn test_stmt(target: ast::ImutExpr) -> ast::MutSelect {
        tremor_script::ast::MutSelect {
            start: Location::default(),
            end: Location::default(),
            from: (
                ast::Ident {
                    start: Location::default(),
                    end: Location::default(),
                    id: "in".into(),
                },
                ast::Ident {
                    start: Location::default(),
                    end: Location::default(),
                    id: "out".into(),
                },
            ),
            into: (
                ast::Ident {
                    start: Location::default(),
                    end: Location::default(),
                    id: "out".into(),
                },
                ast::Ident {
                    start: Location::default(),
                    end: Location::default(),
                    id: "in".into(),
                },
            ),
            target,
            maybe_where: Some(ast::ImutExpr::Literal(ast::Literal {
                start: Location::default(),
                end: Location::default(),
                value: Value::Bool(true),
            })),
            windows: vec![],
            maybe_group_by: None,
            maybe_having: None,
        }
    }

    fn test_query(stmt: ast::Stmt) -> ast::Query {
        ast::Query {
            stmts: vec![stmt.clone()],
        }
    }

    fn test_event(s: u64) -> Event {
        Event {
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
                    size: 15_000_000_000,
                    next_window: None,
                    script: None,
                }
                .into(),
            ),
            (
                "30s".into(),
                TumblingWindowOnTime {
                    size: 30_000_000_000,
                    next_window: None,
                    script: None,
                }
                .into(),
            ),
        ];
        let id = "select".to_string();
        TrickleSelect::with_stmt(id, groups, windows, stmt)
    }

    fn try_enqueue(op: &mut TrickleSelect, event: Event) -> Result<Option<(String, Event)>> {
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
    ) -> Result<Option<[(String, Event); 2]>> {
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

        let stmt_rental = tremor_script::query::rentals::Stmt::new(Arc::new(query.clone()), |q| {
            q.query.suffix().stmts[0].clone()
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
        // add one at thirty, this is the second event the second frame sees but it's
        // timer is at 15 only so it doens't emit yet. However the first window emits
        let (out, event) = try_enqueue(&mut op, test_event(30))?.expect("no event 2");
        assert_eq!("out", out);
        assert_eq!(event.data.suffix().value, 2);
        // Add another event prior to 45 to the first window
        assert!(try_enqueue(&mut op, test_event(31))?.is_none());
        // Emit at 45, this is 30 seconds into the second frame so we see
        // two emits here.
        let [(out1, event1), (out2, event2)] =
            try_enqueue_two(&mut op, test_event(45))?.expect("no event 2");
        assert_eq!("out", out1);
        assert_eq!("out", out2);
        assert_eq!(event1.data.suffix().value, 2);
        assert_eq!(event2.data.suffix().value, 4);
        Ok(())
    }

    #[test]
    fn select_nowin_nogrp_nowhr_nohav() -> Result<()> {
        let target = test_target();
        let stmt_ast = test_stmt(target);

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::query::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::query::Query {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::query::rentals::Stmt::new(Arc::new(query.clone()), |_| stmt_ast);

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

        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(true),
        }));

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::query::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::query::Query {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::query::rentals::Stmt::new(Arc::new(query.clone()), |_| stmt_ast);

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
        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(false),
        }));
        let stmt_ast = test_select_stmt(stmt_ast);

        let query_rental = Arc::new(tremor_script::query::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::query::Query {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::query::rentals::Stmt::new(Arc::new(query.clone()), |_| stmt_ast);

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
        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::String("snot".into()),
        }));

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::query::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::query::Query {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::query::rentals::Stmt::new(Arc::new(query.clone()), |_| stmt_ast);

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
        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(true),
        }));
        stmt_ast.maybe_having = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(true),
        }));

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::query::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::query::Query {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::query::rentals::Stmt::new(Arc::new(query.clone()), |_| stmt_ast);

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
        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(true),
        }));
        stmt_ast.maybe_having = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(false),
        }));

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::query::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::query::Query {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::query::rentals::Stmt::new(Arc::new(query.clone()), |_| stmt_ast);

        let stmt = tremor_script::query::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };

        let mut op = test_select(stmt)?;
        let event = test_event(0);

        let next = try_enqueue(&mut op, event)?;

        assert_eq!(None, next);
        Ok(())
    }

    fn test_select_stmt(stmt: tremor_script::ast::MutSelect) -> tremor_script::ast::Stmt {
        ast::Stmt::Select(SelectStmt {
            stmt: Box::new(stmt),
            aggregates: vec![],
            consts: vec![Value::Null, Value::Null, Value::Null],
        })
    }
    #[test]
    fn select_nowin_nogrp_whrt_havbad() -> Result<()> {
        use halfbrown::hashmap;
        let target = test_target();
        let mut stmt_ast = test_stmt(target);
        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(true),
        }));
        stmt_ast.maybe_having = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Object(hashmap! {
                "snot".into() => "badger".into(),
            }),
        }));

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::query::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::query::Query {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::query::rentals::Stmt::new(Arc::new(query.clone()), |_| stmt_ast);

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
