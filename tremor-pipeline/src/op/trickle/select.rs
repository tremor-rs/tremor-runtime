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
use std::borrow::Cow;
use std::marker::Send;
use tremor_script::{
    self,
    interpreter::{AggrType, ExecOpts},
    registry::{Context, Registry},
    EventContext,
}; // , ast::InvokeAggrFn};

#[derive(Debug)]
pub struct TrickleSelect {
    pub id: String,
    pub cnt: u64,
    pub fake_window_size: u64,
    pub stmt: tremor_script::StmtRentalWrapper<EventContext>,
}

unsafe impl Send for TrickleSelect {
    // NOTE - This is required as we are operating on a runtime where
    // rentals are assisting with lifetime handling. As a part of implementing
    // that we have elected to use reference counting ( Rc ) to track occurances of
    // statements originating from a single wrapped rented query instance
    //
    // Additionally, despite only ever being mutated during creation of a runtime
    // pipeline. As pipelines themselves are mult-threaded by their very nature supporting
    // queues for distribution, we are none-the-less required to assert that our
    // operators are thread-safe ( by implementing Send here ) and this in turn impacts
    // our rentals, which do not directly support threading
    //
    // In summary, we are left with no option but to implement send o a per
    // operator basis. This is also safe, because we guarantee in our engine that
    // we will only ever run an operator in the context of a single thread and all
    // messaging is managed by thread-safe collections.
}

impl Operator for TrickleSelect {
    #[allow(clippy::transmute_ptr_to_ptr)]
    #[allow(mutable_transmutes)]
    fn on_event(&mut self, _port: &str, event: Event) -> Result<Vec<(String, Event)>> {
        use crate::MetaMap;
        use simd_json::borrowed::Value; //, FN_REGISTRY};

        // NOTE We are unwrapping our rental wrapped stmt
        let stmt: &mut tremor_script::ast::Stmt<EventContext> =
            unsafe { std::mem::transmute(self.stmt.stmt.suffix()) };
        let opts = ExecOpts {
            result_needed: true,
            aggr: AggrType::Emit,
        };
        let ctx = EventContext::from_ingest_ns(event.ingest_ns);
        let unwind_event: &mut Value<'_> = unsafe { std::mem::transmute(event.value.suffix()) };
        let event_meta: simd_json::borrowed::Value =
            simd_json::owned::Value::Object(event.meta).into();

        let l = tremor_script::interpreter::LocalStack::with_size(0);
        let x = vec![];

        let (stmt, aggrs, _consts) = match stmt {
            tremor_script::ast::Stmt::SelectStmt {
                stmt,
                aggregates,
                consts,
            } => {
                if self.cnt == 0 {
                    for aggr in aggregates.iter_mut() {
                        let invocable = &mut aggr.invocable;
                        invocable.init();
                    }
                }
                let no_aggrs = vec![];
                for aggr in aggregates.iter_mut() {
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
                        // FIXME nice error
                        let r: Option<&Registry<EventContext>> = None;
                        e.into_err(aggr, aggr, r)
                    })?;
                }
                if self.cnt < self.fake_window_size {
                    self.cnt += 1;
                    return Ok(vec![]);
                } else {
                    self.cnt = 0;
                };
                (stmt, aggregates, consts)
            }
            _ => unreachable!(),
        };

        //
        // Before any select processing, we filter by where clause
        //

        if let Some(guard) = &stmt.maybe_where {
            let test = guard.run(opts, &ctx, &aggrs, unwind_event, &Value::Null, &l, &x)?;
            let _ = match test.into_owned() {
                Value::Bool(true) => (),
                Value::Bool(false) => {
                    return Ok(vec![]);
                }
                other => {
                    return tremor_script::errors::query_guard_not_bool(&stmt, guard, &other)?;
                }
            };
        }

        // After having has been applied to any emissions causal on this
        // event, we prepare the target expression synthetic event and
        // return it for downstream processing
        //

        if let Some(guard) = &stmt.maybe_having {
            let test = guard.run(opts, &ctx, &aggrs, unwind_event, &Value::Null, &l, &x)?;
            let _ = match test.into_owned() {
                Value::Bool(true) => (),
                Value::Bool(false) => {
                    return Ok(vec![]);
                }
                other => {
                    return tremor_script::errors::query_guard_not_bool(&stmt, guard, &other)?;
                }
            };
        }

        let value = stmt
            .target
            .run(opts, &ctx, &aggrs, unwind_event, &Value::Null, &l, &x)?;
        let o: simd_json::borrowed::Value = value.into_owned();
        let o: simd_json::owned::Value = o.into();
        let event = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            meta: MetaMap::new(),
            value: tremor_script::LineValue::new(Box::new(vec![]), |_| o.into()),
            kind: None,
        };
        Ok(vec![("out".to_string(), event)])
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

    use std::sync::Arc;

    fn test_select<'test>(stmt: tremor_script::StmtRentalWrapper<EventContext>) -> TrickleSelect {
        TrickleSelect {
            id: "select".to_string(),
            stmt,
            cnt: 0,
            fake_window_size: 1,
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
        let stmt = tremor_script::StmtRentalWrapper { stmt: stmt_rental };
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

        let stmt = tremor_script::StmtRentalWrapper { stmt: stmt_rental };

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

        let stmt = tremor_script::StmtRentalWrapper { stmt: stmt_rental };

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

        let stmt = tremor_script::StmtRentalWrapper { stmt: stmt_rental };

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

        let stmt = tremor_script::StmtRentalWrapper { stmt: stmt_rental };

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

        let stmt = tremor_script::StmtRentalWrapper { stmt: stmt_rental };

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

        let stmt = tremor_script::StmtRentalWrapper { stmt: stmt_rental };

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

        let stmt = tremor_script::StmtRentalWrapper { stmt: stmt_rental };

        let mut op = test_select(stmt);
        let event = test_event();

        let next = try_enqueue(&mut op, event)?;

        // FIXME TODO - would be nicer to get error output in tests
        // syntax highlighted in capturable form for assertions
        assert_eq!(None, next);
        Ok(())
    }
}
