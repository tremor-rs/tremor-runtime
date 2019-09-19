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
use serde::Serialize;
use std::borrow::Cow;
use std::sync::Arc;

use tremor_script::{
    self,
    ast::InvokeAggrFn,
    interpreter::{AggrType, ExecOpts},
    script::rentals::Query,
    Context, EventContext,
};

pub type Aggrs<'script> = Vec<InvokeAggrFn<'script, EventContext>>;

rental! {
    pub mod rentals {
        use tremor_script::script::rentals::Query;
        use std::sync::Arc;
        use halfbrown::HashMap;
        use serde::Serialize;
        use super::*;

        #[rental(covariant,debug)]
        pub struct Dims<Ctx>
        where
            Ctx: Context + Serialize +'static {
            query: Arc<Query<Ctx>>,
            dimensions: HashMap<String, Window<'query>>,
        }
    }
}

impl<Ctx> rentals::Dims<Ctx>
where
    Ctx: Context + Serialize + 'static,
{
    pub fn from_query(query: Arc<Query<Ctx>>) -> Self {
        Self::new(query, |_| HashMap::new())
    }
}

#[derive(Debug)]
pub struct TrickleSelect {
    pub id: String,
    pub cnt: u64,
    pub stmt: tremor_script::StmtRentalWrapper,
    pub dimensions: rentals::Dims<EventContext>,
}

pub trait WindowTrait: std::fmt::Debug + Clone {
    fn on_event(&mut self, event: &Event) -> WindowEvent;
}

#[derive(Debug, Clone)]
pub struct Window<'query> {
    window_impl: WindowImpl,
    aggregates: Aggrs<'query>,
}

impl<'query> Window<'query> {
    fn from_aggregates(aggregates: Aggrs<'query>, window_impl: WindowImpl) -> Window<'query> {
        Window {
            aggregates,
            window_impl,
        }
    }
}

impl<'query> WindowTrait for Window<'query> {
    fn on_event(&mut self, event: &Event) -> WindowEvent {
        self.window_impl.on_event(event)
    }
}

#[derive(Debug, Clone)]
pub enum WindowImpl {
    Tumbling(TumblingWindowOnEventTime),
    No(NoWindow),
}

impl WindowTrait for WindowImpl {
    fn on_event(&mut self, event: &Event) -> WindowEvent {
        match self {
            WindowImpl::Tumbling(w) => w.on_event(event),
            WindowImpl::No(w) => w.on_event(event),
        }
    }
}

impl From<NoWindow> for WindowImpl {
    fn from(w: NoWindow) -> Self {
        WindowImpl::No(w)
    }
}

impl From<TumblingWindowOnEventTime> for WindowImpl {
    fn from(w: TumblingWindowOnEventTime) -> Self {
        WindowImpl::Tumbling(w)
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
pub struct TumblingWindowOnEventTime {
    next_window: Option<u64>,
    size: u64,
}

impl WindowTrait for TumblingWindowOnEventTime {
    fn on_event(&mut self, event: &Event) -> WindowEvent {
        match self.next_window {
            None => {
                self.next_window = Some(event.ingest_ns + self.size);
                WindowEvent {
                    open: true,

                    emit: false,
                }
            }
            Some(next_window) if next_window <= event.ingest_ns => {
                self.next_window = Some(event.ingest_ns + self.size);
                WindowEvent {
                    open: false,

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

impl Operator for TrickleSelect {
    #[allow(clippy::transmute_ptr_to_ptr)]
    #[allow(mutable_transmutes)]
    fn on_event(&mut self, _port: &str, event: Event) -> Result<Vec<(String, Event)>> {
        use simd_json::borrowed::Value; //, FN_REGISTRY};

        let opts = ExecOpts {
            result_needed: true,
            aggr: AggrType::Emit,
        };
        let unwind_event: &mut Value<'_> = unsafe { std::mem::transmute(event.value.suffix()) };

        let l = tremor_script::interpreter::LocalStack::with_size(0);
        let x = vec![];

        // NOTE We are unwrapping our rental wrapped stmt
        let stmt: &mut tremor_script::ast::Stmt<EventContext> =
            unsafe { std::mem::transmute(self.stmt.stmt.suffix()) };

        let ctx = EventContext::from_ingest_ns(event.ingest_ns);
        match stmt {
            tremor_script::ast::Stmt::SelectStmt {
                stmt,
                aggregates,
                consts,
            } => {
                //
                // Before any select processing, we filter by where clause
                //
                let no_aggrs = vec![];
                if let Some(guard) = &stmt.maybe_where {
                    let test =
                        guard.run(opts, &ctx, &no_aggrs, unwind_event, &Value::Null, &l, &x)?;
                    let _ = match test.into_owned() {
                        Value::Bool(true) => (),
                        Value::Bool(false) => {
                            return Ok(vec![]);
                        }
                        other => {
                            return tremor_script::errors::query_guard_not_bool(
                                &stmt, guard, &other,
                            )?;
                        }
                    };
                }
                let dimension = String::new();

                let dims: &mut HashMap<String, Window> =
                    unsafe { std::mem::transmute(self.dimensions.suffix()) };

                let w = dims.entry(dimension).or_insert_with(|| {
                    Window::from_aggregates(aggregates.clone(), NoWindow::default().into())
                });

                let window_event = w.on_event(&event);
                if window_event.open {
                    for aggr in w.aggregates.iter_mut() {
                        let invocable = &mut aggr.invocable;
                        invocable.init();
                    }
                }

                /*

                */
                // FIXME: ?
                let event_meta: simd_json::borrowed::Value =
                    simd_json::owned::Value::Object(event.meta.clone()).into();

                let res = if window_event.emit {
                    // After having has been applied to any emissions causal on this
                    // event, we prepare the target expression synthetic event and
                    // return it for downstream processing
                    //
                    // FIXME: This can be nicer, got to look at run for tremor script
                    let aggrs = vec![];
                    let value =
                        stmt.target
                            .run(opts, &ctx, &aggrs, unwind_event, &event_meta, &l, &x)?;
                    *unwind_event = value.into_owned();
                    // let o: simd_json::borrowed::Value = value.into_owned();
                    // let o: simd_json::owned::Value = o.into();
                    // let event = Event {
                    //     is_batch: false,
                    //     id: 1,
                    //     ingest_ns: 1,
                    //     meta: MetaMap::new(),
                    //     value: tremor_script::LineValue::new(Box::new(vec![]), |_| o.into()),
                    //     kind: None,
                    // };
                    if let Some(guard) = &stmt.maybe_having {
                        let test =
                            guard.run(opts, &ctx, &no_aggrs, unwind_event, &Value::Null, &l, &x)?;
                        let _ = match test.into_owned() {
                            Value::Bool(true) => (),
                            Value::Bool(false) => {
                                return Ok(vec![]);
                            }
                            other => {
                                return tremor_script::errors::query_guard_not_bool(
                                    &stmt, guard, &other,
                                )?;
                            }
                        };
                    }

                    Ok(vec![("out".to_string(), event)])
                } else {
                    Ok(vec![])
                };

                for aggr in w.aggregates.iter_mut() {
                    let l = tremor_script::interpreter::LocalStack::with_size(0);
                    let invocable = &mut aggr.invocable;
                    let mut argv: Vec<Cow<Value>> = Vec::with_capacity(aggr.args.len());
                    let mut argv1: Vec<&Value> = Vec::with_capacity(aggr.args.len());
                    for arg in aggr.args.iter() {
                        let result = arg.run(
                            opts,
                            &ctx,
                            &no_aggrs,
                            unwind_event,
                            &event_meta,
                            &l,
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
                        use tremor_script::Registry;
                        // FIXME nice error
                        let r: Option<&Registry<EventContext>> = None;
                        e.into_err(aggr, aggr, r)
                    })?;
                }

                res
            }
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::MetaMap; //, FN_REGISTRY};
    use serde_json::json;
    use simd_json::borrowed::Value;
    use simd_json::OwnedValue;
    use std::convert::TryInto;
    use tremor_script::ast;
    use tremor_script::pos::Location;

    fn test_target<'test>() -> ast::ImutExpr<'test, EventContext> {
        let target: ast::ImutExpr<'test, EventContext> =
            ast::ImutExpr::<EventContext>::Literal(ast::Literal {
                start: Location::default(),
                end: Location::default(),
                value: Value::I64(42),
            });
        target
    }

    fn test_stmt<'test>(
        target: ast::ImutExpr<'test, EventContext>,
    ) -> ast::MutSelect<'test, EventContext> {
        let stmt = tremor_script::ast::MutSelect {
            start: Location::default(),
            end: Location::default(),
            from: ast::Ident {
                start: Location::default(),
                end: Location::default(),
                id: "in".into(),
            },
            into: ast::Ident {
                start: Location::default(),
                end: Location::default(),
                id: "out".into(),
            },
            target,
            maybe_where: Some(ast::ImutExpr::Literal(ast::Literal {
                start: Location::default(),
                end: Location::default(),
                value: Value::Bool(true),
            })),
            maybe_having: None,
        };

        stmt
    }

    fn test_query<'test>(stmt: ast::Stmt<'test, EventContext>) -> ast::Query<'test, EventContext> {
        ast::Query {
            stmts: vec![stmt.clone()],
        }
    }

    fn test_event() -> Event {
        Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            meta: MetaMap::new(),
            value: sjv!(json!({
               "h2g2" : 42,
            })
            .try_into()
            .expect("bad event")),
            kind: None,
        }
    }

    use rentals::Dims;
    use std::sync::Arc;

    fn test_select<'test>(stmt: tremor_script::StmtRentalWrapper) -> TrickleSelect {
        TrickleSelect {
            id: "select".to_string(),
            stmt,
            cnt: 0,
            dimensions: Dims::from_query(stmt.stmt.query),
        }
    }

    fn try_enqueue<'test>(
        op: &'test mut TrickleSelect,
        event: Event,
    ) -> Result<Option<(String, Event)>> {
        let mut action = op.on_event("in", event)?;
        Ok(action.pop())
    }

    fn parse_query(query: &str) -> Result<crate::op::trickle::select::TrickleSelect> {
        let reg = tremor_script::registry();
        let aggr_reg = tremor_script::aggr_registry();
        let query = tremor_script::script::QueryRentalWrapper::parse(query, &reg, &aggr_reg)?;

        let stmt_rental = tremor_script::script::rentals::Stmt::new(query.query.clone(), |q| {
            q.suffix().stmts[0].clone()
        });
        let stmt = tremor_script::StmtRentalWrapper { stmt: Arc::new(stmt_rental) };
        Ok(test_select(stmt))
    }

    #[test]
    fn test_sum() -> Result<()> {
        let mut op = parse_query("select stats::sum(event.h2g2) from in into out;")?;
        let event = test_event();
        assert!(try_enqueue(&mut op, event)?.is_none());

        let event = test_event();
        let (out, event) = try_enqueue(&mut op, event)?.expect("bad event");
        assert_eq!("out", out);

        let j: OwnedValue = event.value.rent(|j| j.clone().into());
        assert_eq!(j, 84.0);
        Ok(())
    }

    #[test]
    fn test_count() -> Result<()> {
        let mut op = parse_query("select stats::count() from in into out;")?;
        let event = test_event();
        assert!(try_enqueue(&mut op, event)?.is_none());

        let event = test_event();
        let (out, event) = try_enqueue(&mut op, event)?.expect("bad event");
        assert_eq!("out", out);

        let j: OwnedValue = event.value.rent(|j| j.clone().into());
        assert_eq!(j, 2);
        Ok(())
    }

    #[test]
    fn select_nowin_nogrp_nowhr_nohav() -> Result<()> {
        let target = test_target();
        let stmt_ast = test_stmt(target);

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::script::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::QueryRentalWrapper {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::script::rentals::Stmt::new(query.query.clone(), |_| stmt_ast);

        let stmt = tremor_script::StmtRentalWrapper { stmt: Arc::new(stmt_rental) };

        let mut op = test_select(stmt);
        let event = test_event();
        assert!(try_enqueue(&mut op, event)?.is_none());
        let event = test_event();
        let (out, event) = try_enqueue(&mut op, event)?.expect("bad event");
        assert_eq!("out", out);

        let j: OwnedValue = event.value.rent(|j| j.clone().into());
        //        let jj: OwnedValue = json!({"snot": "badger", "a": 1}).try_into().expect("");
        assert_eq!(OwnedValue::I64(42), j);
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
        let query_rental = Arc::new(tremor_script::script::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::QueryRentalWrapper {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::script::rentals::Stmt::new(query.query.clone(), |_| stmt_ast);

        let stmt = tremor_script::StmtRentalWrapper { stmt: Arc::new(stmt_rental) };

        let mut op = test_select(stmt);

        let event = test_event();
        assert!(try_enqueue(&mut op, event)?.is_none());

        let event = test_event();
        let (out, event) = try_enqueue(&mut op, event)?.expect("bad event");
        assert_eq!("out", out);

        let j: OwnedValue = event.value.rent(|j| j.clone().into());
        assert_eq!(OwnedValue::I64(42), j);
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

        let query_rental = Arc::new(tremor_script::script::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::QueryRentalWrapper {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::script::rentals::Stmt::new(query.query.clone(), |_| stmt_ast);

        let stmt = tremor_script::StmtRentalWrapper { stmt: Arc::new(stmt_rental) };

        let mut op = test_select(stmt);
        let event = test_event();

        let next = try_enqueue(&mut op, event)?;
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
        let query_rental = Arc::new(tremor_script::script::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::QueryRentalWrapper {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::script::rentals::Stmt::new(query.query.clone(), |_| stmt_ast);

        let stmt = tremor_script::StmtRentalWrapper { stmt: Arc::new(stmt_rental) };

        let mut op = test_select(stmt);
        let event = test_event();

        let next = try_enqueue(&mut op, event)?;

        // FIXME TODO - would be nicer to get error output in tests
        // syntax highlighted in capturable form for assertions
        assert_eq!(None, next);
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
        let query_rental = Arc::new(tremor_script::script::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::QueryRentalWrapper {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::script::rentals::Stmt::new(query.query.clone(), |_| stmt_ast);

        let stmt = tremor_script::StmtRentalWrapper { stmt: Arc::new(stmt_rental) };

        let mut op = test_select(stmt);

        let event = test_event();
        assert!(try_enqueue(&mut op, event)?.is_none());

        let event = test_event();
        let (out, event) = try_enqueue(&mut op, event)?.expect("bad event");
        assert_eq!("out", out);
        let j: OwnedValue = event.value.rent(|j| j.clone().into());
        assert_eq!(OwnedValue::I64(42), j);
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
        let query_rental = Arc::new(tremor_script::script::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::QueryRentalWrapper {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::script::rentals::Stmt::new(query.query.clone(), |_| stmt_ast);

        let stmt = tremor_script::StmtRentalWrapper { stmt: Arc::new(stmt_rental) };

        let mut op = test_select(stmt);
        let event = test_event();

        let next = try_enqueue(&mut op, event)?;

        assert_eq!(None, next);
        Ok(())
    }

    fn test_select_stmt<'snot>(
        stmt: tremor_script::ast::MutSelect<'snot, EventContext>,
    ) -> tremor_script::ast::Stmt<'snot, EventContext> {
        ast::Stmt::SelectStmt {
            stmt: Box::new(stmt),
            aggregates: vec![],
            consts: vec![],
        }
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
        let query_rental = Arc::new(tremor_script::script::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::QueryRentalWrapper {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::script::rentals::Stmt::new(query.query.clone(), |_| stmt_ast);

        let stmt = tremor_script::StmtRentalWrapper { stmt: Arc::new(stmt_rental) };

        let mut op = test_select(stmt);
        let event = test_event();

        let next = try_enqueue(&mut op, event)?;

        // FIXME TODO - would be nicer to get error output in tests
        // syntax highlighted in capturable form for assertions
        assert_eq!(None, next);
        Ok(())
    }
}
