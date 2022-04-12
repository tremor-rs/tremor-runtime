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

// mod test { <- this is for safety.sh

#![allow(clippy::float_cmp)]
use crate::op::prelude::trickle::window::{Actions, Trait};
use crate::query::window_decl_to_impl;
use crate::EventId;

use super::*;

use tremor_script::ast::{visitors::ConstFolder, walkers::QueryWalker, Stmt, WindowDecl};
use tremor_script::{ast::Consts, Value};
use tremor_script::{
    ast::{self, Ident, Literal},
    path::ModulePath,
};
use tremor_value::literal;

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
    let mut consts = Consts::default();
    consts.args = literal!({"snot": "badger"});
    ast::Query {
        stmts: vec![stmt.clone()],
        node_meta: ast::NodeMetas::default(),
        windows: HashMap::new(),
        scripts: HashMap::new(),
        operators: HashMap::new(),
        config: HashMap::new(),
        consts,
    }
}

fn ingest_ns(s: u64) -> u64 {
    s * 1_000_000_000
}
fn test_event(s: u64) -> Event {
    Event {
        id: (0, 0, s).into(),
        ingest_ns: ingest_ns(s),
        data: literal!({
           "h2g2" : 42,
        })
        .into(),
        ..Event::default()
    }
}

fn test_event_tx(s: u64, transactional: bool, group: u64) -> Event {
    Event {
        id: (0, 0, s, s).into(),
        ingest_ns: s + 100,
        data: literal!({ "group": group, "s": s }).into(),
        transactional,
        ..Event::default()
    }
}

fn test_select(uid: u64, stmt: srs::Stmt) -> Result<Select> {
    let windows = vec![
        (
            "w15s".into(),
            window::TumblingOnTime {
                max_groups: window::Impl::DEFAULT_MAX_GROUPS,
                interval: 15_000_000_000,
                ..Default::default()
            }
            .into(),
        ),
        (
            "w30s".into(),
            window::TumblingOnTime {
                max_groups: window::Impl::DEFAULT_MAX_GROUPS,
                interval: 30_000_000_000,
                ..Default::default()
            }
            .into(),
        ),
    ];
    let id = "select".to_string();
    Select::with_stmt(uid, id, windows, &stmt)
}

fn try_enqueue(op: &mut Select, event: Event) -> Result<Option<(Cow<'static, str>, Event)>> {
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
    op: &mut Select,
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

fn parse_query(file_name: String, query: &str) -> Result<crate::op::trickle::select::Select> {
    let reg = tremor_script::registry();
    let aggr_reg = tremor_script::aggr_registry();
    let module_path = tremor_script::path::load();
    let cus = vec![];
    let query =
        tremor_script::query::Query::parse(&module_path, &file_name, query, cus, &reg, &aggr_reg)
            .map_err(tremor_script::errors::CompilerError::error)?;
    let stmt = srs::Stmt::try_new_from_query(&query.query, |q| {
        q.stmts
            .first()
            .cloned()
            .ok_or_else(|| Error::from("Invalid query"))
    })?;
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

fn select_stmt_from_query(query_str: &str) -> Result<Select> {
    let mut meta = NodeMetas::default();
    let reg = tremor_script::registry();
    let aggr_reg = tremor_script::aggr_registry();
    let module_path = tremor_script::path::load();
    let cus = vec![];
    let query =
        tremor_script::query::Query::parse(&module_path, "fake", query_str, cus, &reg, &aggr_reg)
            .map_err(tremor_script::errors::CompilerError::error)?;
    let mut window_decls: Vec<WindowDecl<'_>> = query
        .suffix()
        .stmts
        .iter()
        .filter_map(|stmt| match stmt {
            Stmt::WindowDecl(wd) => Some(wd.as_ref().clone()),
            _ => None,
        })
        .collect();
    let stmt = srs::Stmt::try_new_from_query(&query.query, |q| {
        q.stmts
            .iter()
            .find(|stmt| matches!(*stmt, Stmt::SelectStmt(_)))
            .cloned()
            .ok_or_else(|| Error::from("Invalid query, expected only 1 select statement"))
    })?;
    let windows: Vec<(String, window::Impl)> = window_decls
        .iter_mut()
        .enumerate()
        .map(|(i, window_decl)| {
            let fake_consts = Consts::default();
            let mut f = ConstFolder {
                meta: &mut meta,
                reg: &reg,
                consts: &fake_consts,
            };
            f.walk_window_decl(window_decl).unwrap();
            (
                i.to_string(),
                window_decl_to_impl(window_decl, &meta).unwrap(), // yes, indeed!
            )
        })
        .collect();

    let id = "select".to_string();
    Ok(Select::with_stmt(42, id, windows, &stmt)?)
}

fn test_tick(ns: u64) -> Event {
    Event {
        id: EventId::from_id(1, 1, ns),
        kind: Some(SignalKind::Tick),
        ingest_ns: ns,
        ..Event::default()
    }
}

#[test]
fn select_single_win_with_script_on_signal() -> Result<()> {
    let mut select = select_stmt_from_query(
        r#"
        define window window1 from tumbling
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
    let mut eis = select.on_signal(uid, &mut state, &mut tick1)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    let event = Event {
        id: (1, 1, 300).into(),
        ingest_ns: 1_000,
        data: literal!({
           "time" : 4,
           "g": "group"
        })
        .into(),
        ..Event::default()
    };
    eis = select.on_event(uid, "in", &mut state, event)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    // no emit on signal, although window interval would be passed
    let mut tick2 = test_tick(10);
    eis = select.on_signal(uid, &mut state, &mut tick2)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    // only emit on event
    let event = Event {
        id: (1, 1, 300).into(),
        ingest_ns: 3_000,
        data: literal!({
           "time" : 11,
           "g": "group"
        })
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
        define window window1 from tumbling
        with
            interval = 2
        end;
        select aggr::win::collect_flattened(event) from in[window1] group by event.g into out;
        "#,
    )?;
    let uid = 42;
    let mut state = Value::null();
    let mut tick1 = test_tick(1);
    let mut eis = select.on_signal(uid, &mut state, &mut tick1)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    let event = Event {
        id: (1, 1, 300).into(),
        ingest_ns: 2,
        data: literal!({
           "g": "group"
        })
        .into(),
        ..Event::default()
    };
    eis = select.on_event(uid, "IN", &mut state, event)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    // no emit yet
    let mut tick2 = test_tick(3);
    eis = select.on_signal(uid, &mut state, &mut tick2)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    // now emit
    let mut tick3 = test_tick(4);
    eis = select.on_signal(uid, &mut state, &mut tick3)?;
    assert!(eis.insights.is_empty());
    assert_eq!(1, eis.events.len());
    assert_eq!(
        r#"[{"g":"group"}]"#,
        sorted_serialize(eis.events[0].1.data.parts().0)?
    );
    assert_eq!(false, eis.events[0].1.transactional);
    Ok(())
}

#[test]
fn select_multiple_wins_on_signal() -> Result<()> {
    let mut select = select_stmt_from_query(
        r#"
        define window window1 from tumbling
        with
            interval = 100
        end;
        define window window2 from tumbling
        with
            size = 2
        end;
        select aggr::win::collect_flattened(event) from in[window1, window2] group by event.cat into out;
        "#,
    )?;
    let uid = 42;
    let mut state = Value::null();
    let mut tick1 = test_tick(1);
    let mut eis = select.on_signal(uid, &mut state, &mut tick1)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    let mut tick2 = test_tick(100);
    eis = select.on_signal(uid, &mut state, &mut tick2)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    // we have no groups, so no emit yet
    let mut tick3 = test_tick(201);
    eis = select.on_signal(uid, &mut state, &mut tick3)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    // insert first event
    let event = Event {
        id: (1, 1, 300).into(),
        ingest_ns: 300,
        data: literal!({
           "cat" : 42,
        })
        .into(),
        transactional: true,
        ..Event::default()
    };
    eis = select.on_event(uid, "in", &mut state, event)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    // finish the window with the next tick
    let mut tick4 = test_tick(401);
    eis = select.on_signal(uid, &mut state, &mut tick4)?;
    assert!(eis.insights.is_empty());
    assert_eq!(1, eis.events.len());
    let (_port, event) = eis.events.remove(0);
    assert_eq!(r#"[{"cat":42}]"#, sorted_serialize(event.data.parts().0)?);
    assert_eq!(true, event.transactional);

    let mut tick5 = test_tick(499);
    eis = select.on_signal(uid, &mut state, &mut tick5)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    Ok(())
}

#[test]
fn test_transactional_single_window() -> Result<()> {
    let mut op = select_stmt_from_query(
        r#"
            define window w2 from tumbling
            with
              size = 2
            end;
            select aggr::stats::count() from in[w2] into out;
        "#,
    )?;
    let mut state = Value::null();
    let event1 = test_event_tx(0, false, 0);
    let id1 = event1.id.clone();
    let res = op.on_event(0, "in", &mut state, event1)?;

    assert!(res.events.is_empty());

    let event2 = test_event_tx(1, true, 0);
    let id2 = event2.id.clone();
    let mut res = op.on_event(0, "in", &mut state, event2)?;
    assert_eq!(1, res.events.len());
    let (_, event) = res.events.pop().unwrap();
    assert_eq!(true, event.transactional);
    assert_eq!(true, event.id.is_tracking(&id1));
    assert_eq!(true, event.id.is_tracking(&id2));

    let event3 = test_event_tx(2, false, 0);
    let id3 = event3.id.clone();
    let res = op.on_event(0, "in", &mut state, event3)?;
    assert!(res.events.is_empty());

    let event4 = test_event_tx(3, false, 0);
    let id4 = event4.id.clone();
    let mut res = op.on_event(0, "in", &mut state, event4)?;
    assert_eq!(1, res.events.len());
    let (_, event) = res.events.pop().unwrap();
    assert_eq!(false, event.transactional);
    assert_eq!(true, event.id.is_tracking(&id3));
    assert_eq!(true, event.id.is_tracking(&id4));

    Ok(())
}

#[test]
fn test_transactional_multiple_windows() -> Result<()> {
    let mut op = select_stmt_from_query(
        r#"
            define window w2_1 from tumbling
            with
              size = 2
            end;
            define window w2_2 from tumbling
            with
              size = 2
            end;
            select aggr::win::collect_flattened(event) from in[w2_1, w2_2] group by set(event["group"]) into out;
        "#,
    )?;
    let mut state = Value::null();
    let event0 = test_event_tx(0, true, 0);
    let id0 = event0.id.clone();
    let res = op.on_event(0, "in", &mut state, event0)?;
    assert_eq!(0, res.len());

    let event1 = test_event_tx(1, false, 1);
    let id1 = event1.id.clone();
    let res = op.on_event(0, "in", &mut state, event1)?;
    assert_eq!(0, res.len());

    let event2 = test_event_tx(2, false, 0);
    let id2 = event2.id.clone();
    let mut res = op.on_event(0, "in", &mut state, event2)?;
    assert_eq!(1, res.len());
    let (_, event) = res.events.pop().unwrap();
    assert_eq!(true, event.transactional);
    assert_eq!(true, event.id.is_tracking(&id0));
    assert_eq!(true, event.id.is_tracking(&id2));

    let event3 = test_event_tx(3, false, 1);
    let id3 = event3.id.clone();
    let mut res = op.on_event(0, "in", &mut state, event3)?;
    assert_eq!(1, res.len());
    let (_, event) = res.events.remove(0);
    assert_eq!(false, event.transactional);
    assert_eq!(true, event.id.is_tracking(&id1));
    assert_eq!(true, event.id.is_tracking(&id3));

    let event4 = test_event_tx(4, false, 0);
    let id4 = event4.id.clone();
    let res = op.on_event(0, "in", &mut state, event4)?;
    assert_eq!(0, res.len());

    let event5 = test_event_tx(5, false, 0);
    let id5 = event5.id.clone();
    let mut res = op.on_event(0, "in", &mut state, event5)?;
    assert_eq!(2, res.len());

    // first event from event5 and event6 - none of the source events are transactional
    let (_, event_w1) = res.events.remove(0);
    assert!(!event_w1.transactional);
    assert!(event_w1.id.is_tracking(&id4));
    assert!(event_w1.id.is_tracking(&id5));

    let (_, event_w2) = res.events.remove(0);
    assert!(event_w2.transactional);
    assert!(event_w2.id.is_tracking(&id0));
    assert!(event_w2.id.is_tracking(&id2));
    assert!(event_w2.id.is_tracking(&id4));
    assert!(event_w2.id.is_tracking(&id5));

    Ok(())
}

#[test]
fn count_tilt() -> Result<()> {
    // Windows are 15s and 30s
    let mut op = select_stmt_from_query(
        r#"
        define window w15s from tumbling
        with
          interval = 15 * 1000000000
        end;
        define window w30s from tumbling
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
    let query = srs::Query::try_new::<Error, _>("target".into(), script, |_| {
        Ok(test_query(stmt_ast.clone()))
    })?;

    let stmt = srs::Stmt::try_new_from_query::<Error, _>(&query, |_| Ok(stmt_ast))?;

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
    let query = srs::Query::try_new::<Error, _>("target".into(), script, |_| {
        Ok(test_query(stmt_ast.clone()))
    })?;

    let stmt = srs::Stmt::try_new_from_query::<Error, _>(&query, |_| Ok(stmt_ast))?;

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
    stmt_ast.maybe_where = Some(ImutExpr::from(ast::Literal {
        mid: 0,
        value: Value::from(false),
    }));
    let stmt_ast = test_select_stmt(stmt_ast);
    let query = srs::Query::try_new::<Error, _>("target".into(), script, |_| {
        Ok(test_query(stmt_ast.clone()))
    })?;

    let stmt = srs::Stmt::try_new_from_query::<Error, _>(&query, |_| Ok(stmt_ast))?;

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
    let query = srs::Query::try_new::<Error, _>("target".into(), script, |_| {
        Ok(test_query(stmt_ast.clone()))
    })?;

    let stmt = srs::Stmt::try_new_from_query::<Error, _>(&query, |_| Ok(stmt_ast))?;

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
    let query = srs::Query::try_new::<Error, _>("target".into(), script, |_| {
        Ok(test_query(stmt_ast.clone()))
    })?;

    let stmt = srs::Stmt::try_new_from_query::<Error, _>(&query, |_| Ok(stmt_ast))?;

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
    let query = srs::Query::try_new::<Error, _>("target".into(), script, |_| {
        Ok(test_query(stmt_ast.clone()))
    })?;

    let stmt = srs::Stmt::try_new_from_query::<Error, _>(&query, |_| Ok(stmt_ast))?;

    let mut op = test_select(6, stmt)?;
    let event = test_event(0);

    let next = try_enqueue(&mut op, event)?;

    assert_eq!(None, next);
    Ok(())
}

fn test_select_stmt(stmt: tremor_script::ast::Select) -> tremor_script::ast::Stmt {
    let aggregates = vec![];
    ast::Stmt::SelectStmt(SelectStmt {
        stmt: Box::new(stmt),
        aggregates,
        consts: Consts::default(),
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
    let query = srs::Query::try_new::<Error, _>("target".into(), script, |_| {
        Ok(test_query(stmt_ast.clone()))
    })?;

    let stmt = srs::Stmt::try_new_from_query::<Error, _>(&query, |_| Ok(stmt_ast))?;

    let mut op = test_select(7, stmt)?;
    let event = test_event(0);

    let next = try_enqueue(&mut op, event)?;

    assert_eq!(None, next);
    Ok(())
}

#[test]
fn tumbling_window_on_time_emit() -> Result<()> {
    // interval = 10 seconds
    let mut window = window::TumblingOnTime::from_stmt(
        10 * 1_000_000_000,
        window::Impl::DEFAULT_MAX_GROUPS,
        None,
    );
    let vm = literal!({
       "h2g2" : 42,
    })
    .into();
    assert_eq!(
        Actions {
            include: false,
            emit: false
        },
        window.on_event(&vm, ingest_ns(5), &None)?
    );
    assert_eq!(
        Actions::all_false(),
        window.on_event(&vm, ingest_ns(10), &None)?
    );
    assert_eq!(
        Actions {
            include: false,
            emit: true
        },
        window.on_event(&vm, ingest_ns(15), &None)? // exactly on time
    );
    assert_eq!(
        Actions {
            include: false,
            emit: true
        },
        window.on_event(&vm, ingest_ns(26), &None)? // exactly on time
    );
    Ok(())
}

#[test]
fn tumbling_window_on_time_from_script_emit() -> Result<()> {
    // create a WindowDecl with a custom script
    let reg = Registry::default();
    let aggr_reg = AggrRegistry::default();
    let module_path = ModulePath::load();
    let q = tremor_script::query::Query::parse(
        &module_path,
        "bar",
        r#"
            define window my_window from tumbling
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
    let window_decl = match q.query.suffix().stmts.first() {
        Some(Stmt::WindowDecl(decl)) => decl.as_ref(),
        other => return Err(format!("Didnt get a window decl, got: {:?}", other).into()),
    };
    let mut params = halfbrown::HashMap::with_capacity(1);
    params.insert("size".to_string(), Value::from(3));
    let with = window_decl.params.render(&NodeMetas::default())?;
    let interval = with
        .get("interval")
        .and_then(Value::as_u64)
        .ok_or(Error::from("no interval found"))?;
    let mut window = window::TumblingOnTime::from_stmt(
        interval,
        window::Impl::DEFAULT_MAX_GROUPS,
        Some(&window_decl),
    );
    let json1 = literal!({
        "timestamp": 1_000_000_000
    })
    .into();
    assert_eq!(
        Actions {
            include: false,
            emit: false
        },
        window.on_event(&json1, 1, &None)?
    );
    let json2 = literal!({
        "timestamp": 1_999_999_999
    })
    .into();
    assert_eq!(Actions::all_false(), window.on_event(&json2, 2, &None)?);
    let json3 = literal!({
        "timestamp": 2_000_000_000
    })
    .into();
    // ignoring on_tick as we have a script
    assert_eq!(Actions::all_false(), window.on_tick(2_000_000_000));
    assert_eq!(
        Actions {
            include: false,
            emit: true
        },
        window.on_event(&json3, 3, &None)?
    );
    Ok(())
}

#[test]
fn tumbling_window_on_time_on_tick() -> Result<()> {
    let mut window = window::TumblingOnTime::from_stmt(100, window::Impl::DEFAULT_MAX_GROUPS, None);
    assert_eq!(
        Actions {
            include: false,
            emit: false
        },
        window.on_tick(0)
    );
    assert_eq!(Actions::all_false(), window.on_tick(99));
    assert_eq!(
        Actions {
            include: false,
            emit: true // we delete windows that do not have content so this is fine
        },
        window.on_tick(100)
    );
    assert_eq!(
        Actions::all_false(),
        window.on_event(&ValueAndMeta::default(), 101, &None)?
    );
    assert_eq!(Actions::all_false(), window.on_tick(102));
    assert_eq!(
        Actions {
            include: false,
            emit: true // we had an event yeah
        },
        window.on_tick(200)
    );
    Ok(())
}

#[test]
fn tumbling_window_on_time_emit_empty_windows() -> Result<()> {
    let mut window = window::TumblingOnTime::from_stmt(100, window::Impl::DEFAULT_MAX_GROUPS, None);
    assert_eq!(
        Actions {
            include: false,
            emit: false
        },
        window.on_tick(0)
    );
    assert_eq!(Actions::all_false(), window.on_tick(99));
    assert_eq!(
        Actions {
            include: false,
            emit: true // we **DO** emit even if we had no event
        },
        window.on_tick(100)
    );
    assert_eq!(
        Actions::all_false(),
        window.on_event(&ValueAndMeta::default(), 101, &None)?
    );
    assert_eq!(Actions::all_false(), window.on_tick(102));
    assert_eq!(
        Actions {
            include: false,
            emit: true // we had an event yeah
        },
        window.on_tick(200)
    );

    Ok(())
}

#[test]
fn no_window_emit() -> Result<()> {
    let mut window = window::No::default();

    let vm = literal!({
       "h2g2" : 42,
    })
    .into();

    assert_eq!(
        Actions::all_true(),
        window.on_event(&vm, ingest_ns(0), &None)?
    );
    assert_eq!(Actions::all_false(), window.on_tick(0));
    assert_eq!(
        Actions::all_true(),
        window.on_event(&vm, ingest_ns(1), &None)?
    );
    assert_eq!(Actions::all_false(), window.on_tick(1));
    Ok(())
}

#[test]
fn tumbling_window_on_number_emit() -> Result<()> {
    let mut window = window::TumblingOnNumber::from_stmt(3, window::Impl::DEFAULT_MAX_GROUPS, None);

    let vm = literal!({
       "h2g2" : 42,
    })
    .into();

    // do not emit yet
    assert_eq!(
        Actions::all_false(),
        window.on_event(&vm, ingest_ns(0), &None)?
    );
    assert_eq!(Actions::all_false(), window.on_tick(1_000_000_000));
    // do not emit yet
    assert_eq!(
        Actions::all_false(),
        window.on_event(&vm, ingest_ns(1), &None)?
    );
    assert_eq!(Actions::all_false(), window.on_tick(2_000_000_000));
    // emit and open on the third event
    assert_eq!(
        Actions::all_true(),
        window.on_event(&vm, ingest_ns(2), &None)?
    );
    // no emit here, next window
    assert_eq!(
        Actions::all_false(),
        window.on_event(&vm, ingest_ns(3), &None)?
    );

    Ok(())
}
