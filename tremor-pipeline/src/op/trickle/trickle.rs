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
use tremor_script::{self, interpreter::{ExecOpts, AggrType}}; // , ast::InvokeAggrFn};
use std::marker::Send;

#[derive(Debug, Default, Clone, PartialEq, Serialize)]
pub struct EventContext {
    at: u64,
}
impl tremor_script::registry::Context for EventContext {
    fn ingest_ns(&self) -> u64 {
        self.at
    }

    fn from_ingest_ns(ingest_ns: u64) -> Self {
        EventContext { at: ingest_ns }
    }
}

#[derive(Debug)]
pub struct TrickleSelect<Ctx>
where
    Ctx: tremor_script::Context + serde::Serialize + 'static,
{
    pub id: String,
    pub stmt: tremor_script::StmtRentalWrapper<Ctx>,
}

unsafe impl<Ctx> Send for TrickleSelect<Ctx>
where
    Ctx: tremor_script::Context + serde::Serialize + 'static,
{
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

impl<Ctx> Operator for TrickleSelect<Ctx>
where
    Ctx: tremor_script::Context + serde::Serialize + 'static,
{
    fn on_event(&mut self, _port: &str, event: Event) -> Result<Vec<(String, Event)>> {
        use crate::MetaMap;
        use simd_json::borrowed::Value; //, FN_REGISTRY};

        // NOTE We are unwrapping our rental wrapped stmt
        let stmt: &tremor_script::ast::Stmt<Ctx> = self.stmt.stmt.suffix();
        let stmt = match stmt {
            tremor_script::ast::Stmt::SelectStmt(stmt) => stmt,
            _ => unreachable!(),
        };

        #[allow(clippy::transmute_ptr_to_ptr)]
        #[allow(mutable_transmutes)]
        let unwind_event: &mut Value<'_> = unsafe { std::mem::transmute(event.value.suffix()) };
        let mut _event_meta: simd_json::borrowed::Value =
            simd_json::owned::Value::Object(event.meta).into();
        let l = tremor_script::interpreter::LocalStack::with_size(0);
        let x = vec![];

        let ctx = Ctx::from_ingest_ns(event.ingest_ns);

	let opts = ExecOpts { result_needed: true, aggr: AggrType::Emit };
        let aggrs = vec![];
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

    fn test_select<'test>(
        stmt: tremor_script::StmtRentalWrapper<EventContext>,
    ) -> TrickleSelect<EventContext> {
        TrickleSelect {
            id: "select".to_string(),
            stmt,
        }
    }

    fn try_enqueue<'test>(
        op: &'test mut TrickleSelect<EventContext>,
        event: Event,
    ) -> Option<(String, Event)> {
        match op.on_event("in", event) {
            Ok(ref mut action) => action.pop(),
            Err(_e) => {
                None
                // FIXME Query::<Ctx>::format_error_from_script(&self.source, &mut h, &arse);
            }
        }
    }

    #[test]
    fn select_nowin_nogrp_nowhr_nohav() {
        let target = test_target();
        let stmt_ast = test_stmt(target);

        let stmt_ast = ast::Stmt::SelectStmt(Box::new(stmt_ast));
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

        let (out, event) = try_enqueue(&mut op, event).expect("bad event");
        assert_eq!("out", out);

        let j: OwnedValue = event.value.rent(|j| j.clone().into());
        //        let jj: OwnedValue = json!({"snot": "badger", "a": 1}).try_into().expect("");
        assert_eq!(OwnedValue::I64(42), j)
    }

    #[test]
    fn select_nowin_nogrp_whrt_nohav() {
        let target = test_target();
        let mut stmt_ast = test_stmt(target);

        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal( ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(true),
        }));

        let stmt_ast = ast::Stmt::SelectStmt(Box::new(stmt_ast));
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

        let (out, event) = try_enqueue(&mut op, event).expect("bad event");
        assert_eq!("out", out);

        let j: OwnedValue = event.value.rent(|j| j.clone().into());
        assert_eq!(OwnedValue::I64(42), j)
    }

    #[test]
    fn select_nowin_nogrp_whrf_nohav() {
        let target = test_target();
        let mut stmt_ast = test_stmt(target);

        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal( ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(false),
        }));
        let stmt_ast = ast::Stmt::SelectStmt(Box::new(stmt_ast));

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

        let next = try_enqueue(&mut op, event);
        assert_eq!(None, next);
    }

    #[test]
    fn select_nowin_nogrp_whrbad_nohav() {
        let target = test_target();
        let mut stmt_ast = test_stmt(target);
        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal( ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::String("snot".into()),
        }));

        let stmt_ast = ast::Stmt::SelectStmt(Box::new(stmt_ast));
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

        let next = try_enqueue(&mut op, event);

        // FIXME TODO - would be nicer to get error output in tests
        // syntax highlighted in capturable form for assertions
        assert_eq!(None, next);
    }

    #[test]
    fn select_nowin_nogrp_whrt_havt() {
        let target = test_target();
        let mut stmt_ast = test_stmt(target);
        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal( ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(true),
        }));
        stmt_ast.maybe_having = Some(ast::ImutExpr::Literal( ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(true),
        }));

        let stmt_ast = ast::Stmt::SelectStmt(Box::new(stmt_ast));
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

        let (out, event) = try_enqueue(&mut op, event).expect("bad event");
        assert_eq!("out", out);
        let j: OwnedValue = event.value.rent(|j| j.clone().into());
        assert_eq!(OwnedValue::I64(42), j)
    }

    #[test]
    fn select_nowin_nogrp_whrt_havf() {
        let target = test_target();
        let mut stmt_ast = test_stmt(target);
        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal( ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(true),
        }));
        stmt_ast.maybe_having = Some(ast::ImutExpr::Literal( ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(false),
        }));

        let stmt_ast = ast::Stmt::SelectStmt(Box::new(stmt_ast));
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

        let next = try_enqueue(&mut op, event);

        assert_eq!(None, next);
    }

    #[test]
    fn select_nowin_nogrp_whrt_havbad() {
        use halfbrown::hashmap;
        let target = test_target();
        let mut stmt_ast = test_stmt(target);
        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal( ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(true),
        }));
        stmt_ast.maybe_having = Some(ast::ImutExpr::Literal( ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Object( hashmap!{
                "snot".into() => "badger".into(),
            }),
        }));

        let stmt_ast = ast::Stmt::SelectStmt(Box::new(stmt_ast));
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

        let next = try_enqueue(&mut op, event);

        // FIXME TODO - would be nicer to get error output in tests
        // syntax highlighted in capturable form for assertions
        assert_eq!(None, next);
    }
}
