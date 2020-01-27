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
use std::borrow::Cow;
use tremor_script::interpreter::Env;
use tremor_script::{
    self,
    ast::{InvokeAggrFn, SelectStmt},
    prelude::*,
    query::rentals::Stmt as StmtRental,
};

rental! {
    pub mod rentals {
        use std::sync::Arc;
        use halfbrown::HashMap;
        use super::*;

        #[rental(covariant,debug)]
        pub struct Select {
            stmt: Arc<StmtRental>,
            select: tremor_script::ast::SelectStmt<'stmt>,
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct TrickleSimpleSelect {
    pub id: String,
    pub select: rentals::Select,
}

const NO_AGGRS: [InvokeAggrFn<'static>; 0] = [];

impl TrickleSimpleSelect {
    pub fn with_stmt(
        id: String,
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

        Ok(Self {
            id,
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

impl Operator for TrickleSimpleSelect {
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
            locals,
            consts,
            node_meta,
            ..
        }: &mut SelectStmt = unsafe { std::mem::transmute(self.select.suffix()) };
        let local_stack = tremor_script::interpreter::LocalStack::with_size(*locals);
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

        if let Some(guard) = &stmt.maybe_having {
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

        Ok(vec![("out".into(), event)])
    }
}

#[cfg(test)]
mod test {
    #![allow(clippy::float_cmp)]
    use super::*;
    use simd_json::borrowed::Value;
    use simd_json::json;
    use tremor_script::ast;

    fn test_target<'test>() -> ast::ImutExpr<'test> {
        let target: ast::ImutExpr<'test> = ast::ImutExpr::Literal(ast::Literal {
            mid: 0,
            value: Value::from(42),
        });
        target
    }

    fn test_stmt(target: ast::ImutExpr) -> ast::Select {
        tremor_script::ast::Select {
            mid: 0,
            from: (
                ast::Ident {
                    mid: 0,
                    id: "in".into(),
                },
                ast::Ident {
                    mid: 0,
                    id: "out".into(),
                },
            ),
            into: (
                ast::Ident {
                    mid: 0,
                    id: "out".into(),
                },
                ast::Ident {
                    mid: 0,
                    id: "in".into(),
                },
            ),
            target,
            maybe_where: Some(ast::ImutExpr::Literal(ast::Literal {
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
            mid: 0,
            value: Value::from(true),
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
            mid: 0,
            value: Value::from(false),
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
            mid: 0,
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
            mid: 0,
            value: Value::from(true),
        }));
        stmt_ast.maybe_having = Some(ast::ImutExpr::Literal(ast::Literal {
            mid: 0,
            value: Value::from(true),
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
            mid: 0,
            value: Value::from(true),
        }));
        stmt_ast.maybe_having = Some(ast::ImutExpr::Literal(ast::Literal {
            mid: 0,
            value: Value::from(false),
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
        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal(ast::Literal {
            mid: 0,
            value: Value::from(true),
        }));
        stmt_ast.maybe_having = Some(ast::ImutExpr::Literal(ast::Literal {
            mid: 0,
            value: Value::from(hashmap! {
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
