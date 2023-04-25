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
use crate::query::window_defn_to_impl;
use crate::EventId;

use super::*;

use tremor_common::uids::UId;
use tremor_script::ast::{self, Helper, Ident, Literal};
use tremor_script::{ast::Consts, NodeMeta};
use tremor_script::{
    ast::{visitors::ConstFolder, walkers::QueryWalker, Stmt},
    lexer::Location,
};
use tremor_value::{literal, Value};

fn test_uid() -> OperatorUId {
    OperatorUId::new(42)
}

fn test_select_stmt(stmt: tremor_script::ast::Select) -> SelectStmt {
    SelectStmt {
        stmt: Box::new(stmt),
        aggregates: vec![],
        consts: Consts::default(),
        locals: 0,
    }
}

fn mid() -> Box<NodeMeta> {
    Box::new(NodeMeta::new(Location::default(), Location::default()))
}

fn test_target<'test>() -> ast::ImutExpr<'test> {
    let target: ast::ImutExpr<'test> = ImutExpr::from(ast::Literal {
        mid: mid(),
        value: Value::from(42),
    });
    target
}

fn ident(id: &str) -> Ident {
    Ident::new(id.into(), mid())
}

fn test_stmt(target: ast::ImutExpr) -> ast::Select {
    tremor_script::ast::Select {
        mid: mid(),
        from: (ident("in"), ident("out")),
        into: (ident("out"), ident("in")),
        target,
        maybe_where: Some(ImutExpr::from(ast::Literal {
            mid: mid(),
            value: Value::from(true),
        })),
        windows: vec![],
        maybe_group_by: None,
        maybe_having: None,
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

fn test_select(uid: OperatorUId, stmt: &SelectStmt<'static>) -> Select {
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
    Select::from_stmt(uid, windows, stmt)
}

fn try_enqueue(op: &mut Select, event: Event) -> Result<Option<(Port<'static>, Event)>> {
    let mut state = Value::null();
    let mut action = op.on_event(0, test_uid(), &Port::In, &mut state, event)?;
    let first = action.events.pop();
    if action.events.is_empty() {
        Ok(first)
    } else {
        Ok(None)
    }
}

#[allow(clippy::type_complexity)]
fn try_enqueue_two(op: &mut Select, event: Event) -> Result<Option<[(Port<'static>, Event); 2]>> {
    let mut state = Value::null();
    let mut action = op.on_event(0, test_uid(), &Port::In, &mut state, event)?;
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

fn parse_query(query: &str) -> Result<crate::op::trickle::select::Select> {
    let reg = tremor_script::registry();
    let aggr_reg = tremor_script::aggr_registry();
    let query = tremor_script::query::Query::parse(&query, &reg, &aggr_reg)?;
    let stmt = query
        .query
        .stmts
        .into_iter()
        .find_map(|s| match s {
            Stmt::SelectStmt(s) => Some(s),
            _ => None,
        })
        .ok_or_else(|| Error::from("Invalid query"))?;
    Ok(test_select(test_uid(), &stmt))
}

#[test]
fn test_sum() -> Result<()> {
    let mut op = parse_query("select aggr::stats::sum(event.h2g2) from in[w15s, w30s] into out;")?;
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
    let mut op = parse_query("select aggr::stats::count() from in[w15s, w30s] into out;")?;
    assert!(try_enqueue(&mut op, test_event(0))?.is_none());
    assert!(try_enqueue(&mut op, test_event(1))?.is_none());
    let (out, event) = try_enqueue(&mut op, test_event(15))?.expect("no event");
    assert_eq!("out", out);

    assert_eq!(*event.data.suffix().value(), 2);
    Ok(())
}

fn as_select<'a, 'b>(stmt: &'a Stmt<'b>) -> Option<&'a SelectStmt<'b>> {
    match stmt {
        Stmt::SelectStmt(s) => Some(s),
        _ => None,
    }
}

fn select_stmt_from_query(query_str: &str) -> Result<Select> {
    let reg = tremor_script::registry();
    let aggr_reg = tremor_script::aggr_registry();
    let query = tremor_script::query::Query::parse(&query_str, &reg, &aggr_reg)?;
    let stmt = query
        .query
        .stmts
        .iter()
        .find_map(as_select)
        .cloned()
        .ok_or_else(|| Error::from("Invalid query, expected 1 select statement"))?;
    let h = Helper::new(&reg, &aggr_reg);
    let windows: Vec<(String, window::Impl)> = stmt
        .stmt
        .windows
        .iter()
        .map(|win_defn| {
            let mut window_defn = query
                .query
                .scope
                .content
                .windows
                .get(win_defn.id.id())
                .ok_or("no data")?
                .clone();
            let mut f = ConstFolder { helper: &h };
            f.walk_window_defn(&mut window_defn)?;
            Ok((window_defn.id.clone(), window_defn_to_impl(&window_defn)?))
        })
        .collect::<Result<_>>()?;

    Ok(Select::from_stmt(test_uid(), windows, &stmt))
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
    let uid = test_uid();
    let mut state = Value::null();
    let mut tick1 = test_tick(1);
    let mut eis = select.on_signal(0, uid, &mut state, &mut tick1)?;
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
    eis = select.on_event(0, uid, &Port::In, &mut state, event)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    // no emit on signal, although window interval would be passed
    let mut tick2 = test_tick(10);
    eis = select.on_signal(0, uid, &mut state, &mut tick2)?;
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
    eis = select.on_event(0, uid, &Port::In, &mut state, event)?;
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
    let uid = test_uid();
    let mut state = Value::null();
    let mut tick1 = test_tick(1);
    let mut eis = select.on_signal(0, uid, &mut state, &mut tick1)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    let event_id1: EventId = (1, 1, 300).into();
    let event = Event {
        id: event_id1.clone(),
        ingest_ns: 2,
        data: literal!({
           "g": "group"
        })
        .into(),
        ..Event::default()
    };
    eis = select.on_event(0, uid, &Port::In, &mut state, event)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    // no emit yet
    let mut tick2 = test_tick(3);
    eis = select.on_signal(0, uid, &mut state, &mut tick2)?;
    assert!(eis.insights.is_empty(), "{:?} is not empty", eis.insights);
    assert_eq!(0, eis.events.len());

    // now emit
    let mut tick3 = test_tick(4);
    eis = select.on_signal(0, uid, &mut state, &mut tick3)?;
    assert!(eis.insights.is_empty());
    assert_eq!(1, eis.events.len());
    let event_id = eis.events[0].1.id.clone();
    assert!(
        event_id.is_tracking(&event_id1),
        "select result event {:?} is not tracking input event {:?}",
        &event_id,
        &event_id1
    );
    assert_eq!(
        Some(0), // first event for the first window
        event_id.get_min_by_stream(uid.id(), 0)
    );
    assert_eq!(
        r#"[{"g":"group"}]"#,
        sorted_serialize(eis.events[0].1.data.parts().0)?
    );
    assert!(!eis.events[0].1.transactional);
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
    let uid = test_uid();
    let mut state = Value::null();
    let mut tick1 = test_tick(1);
    let mut eis = select.on_signal(0, uid, &mut state, &mut tick1)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    let mut tick2 = test_tick(100);
    eis = select.on_signal(0, uid, &mut state, &mut tick2)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    // we have no groups, so no emit yet
    let mut tick3 = test_tick(201);
    eis = select.on_signal(0, uid, &mut state, &mut tick3)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    // insert first event
    let event_id1: EventId = (1, 1, 300).into();
    let event = Event {
        id: event_id1.clone(),
        ingest_ns: 300,
        data: literal!({
           "cat" : 42,
        })
        .into(),
        transactional: true,
        ..Event::default()
    };
    eis = select.on_event(0, uid, &Port::In, &mut state, event)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    // finish the window with the next tick
    let mut tick4 = test_tick(401);
    eis = select.on_signal(0, uid, &mut state, &mut tick4)?;
    assert!(eis.insights.is_empty());
    assert_eq!(1, eis.events.len());
    let (_port, event) = eis.events.remove(0);
    assert_eq!(r#"[{"cat":42}]"#, sorted_serialize(event.data.parts().0)?);
    assert!(
        event.id.is_tracking(&event_id1),
        "Select result event {:?} is not tracking input event {:?}",
        &event.id,
        &event_id1
    );
    assert!(event.transactional);
    assert_eq!(
        Some(0), // first event in the first window
        event.id.get_min_by_stream(uid.id(), 0)
    );

    let mut tick5 = test_tick(499);
    eis = select.on_signal(0, uid, &mut state, &mut tick5)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());

    // add another event to emit from the first and second window
    let event_id2: EventId = (1, 1, 301).into();
    let event2 = Event {
        id: event_id2.clone(),
        ingest_ns: 500, // ensure that we don't emit on event, but only on signal
        data: literal!({
            "cat": 42 // needs to be in the same group
        })
        .into(),
        transactional: false,
        ..Event::default()
    };
    eis = select.on_event(0, uid, &Port::In, &mut state, event2)?;
    assert!(eis.insights.is_empty());
    assert_eq!(0, eis.events.len());
    let mut tick6 = test_tick(600);
    eis = select.on_signal(0, uid, &mut state, &mut tick6)?;
    assert!(eis.insights.is_empty());
    assert_eq!(2, eis.events.len());

    let (_, event1) = eis.events.remove(0);
    assert_eq!(event1.id.stream_id(), 0);
    assert!(event1.id.is_tracking(&event_id2));
    assert!(!event1.id.is_tracking(&event_id1));

    let (_, event2) = eis.events.remove(0);
    assert_eq!(event2.id.stream_id(), 1); // different window, different stream id
    assert!(event2.id.is_tracking(&event_id2));
    assert!(event2.id.is_tracking(&event1.id)); // second tilt is tracking window event from first window as well
    assert!(event2.id.is_tracking(&event_id1));

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
    let uid = test_uid();
    let mut state = Value::null();
    let event1 = test_event_tx(0, false, 0);
    let id1 = event1.id.clone();
    let res = op.on_event(0, uid, &Port::In, &mut state, event1)?;

    assert!(res.events.is_empty());

    let event2 = test_event_tx(1, true, 0);
    let id2 = event2.id.clone();
    let mut res = op.on_event(0, uid, &Port::In, &mut state, event2)?;
    assert_eq!(1, res.events.len());
    let (_, event) = res.events.pop().ok_or("no data")?;
    assert!(event.transactional);
    assert!(event.id.is_tracking(&id1));
    assert!(event.id.is_tracking(&id2));

    let event3 = test_event_tx(2, false, 0);
    let id3 = event3.id.clone();
    let res = op.on_event(0, uid, &Port::In, &mut state, event3)?;
    assert!(res.events.is_empty());

    let event4 = test_event_tx(3, false, 0);
    let id4 = event4.id.clone();
    let mut res = op.on_event(0, uid, &Port::In, &mut state, event4)?;
    assert_eq!(1, res.events.len());
    let (_, event) = res.events.pop().ok_or("no data")?;
    assert!(!event.transactional);
    assert!(event.id.is_tracking(&id3));
    assert!(event.id.is_tracking(&id4));

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

    let uid = test_uid();
    let mut state = Value::null();
    let event0 = test_event_tx(0, true, 0);
    let id0 = event0.id.clone();
    let res = op.on_event(0, uid, &Port::In, &mut state, event0)?;
    assert_eq!(0, res.len());

    let event1 = test_event_tx(1, false, 1);
    let id1 = event1.id.clone();
    let res = op.on_event(0, uid, &Port::In, &mut state, event1)?;
    assert_eq!(0, res.len());

    let event2 = test_event_tx(2, false, 0);
    let id2 = event2.id.clone();
    let mut res = op.on_event(0, uid, &Port::In, &mut state, event2)?;
    assert_eq!(1, res.len());
    let (_, event) = res.events.pop().ok_or("no data")?;
    assert!(event.transactional);
    assert!(event.id.is_tracking(&id0));
    assert!(event.id.is_tracking(&id2));

    let event3 = test_event_tx(3, false, 1);
    let id3 = event3.id.clone();
    let mut res = op.on_event(0, uid, &Port::In, &mut state, event3)?;
    assert_eq!(1, res.len());
    let (_, event) = res.events.remove(0);
    assert!(!event.transactional);
    assert!(event.id.is_tracking(&id1));
    assert!(event.id.is_tracking(&id3));

    let event4 = test_event_tx(4, false, 0);
    let id4 = event4.id.clone();
    let res = op.on_event(0, uid, &Port::In, &mut state, event4)?;
    assert_eq!(0, res.len());

    let event5 = test_event_tx(5, false, 0);
    let id5 = event5.id.clone();
    let mut res = op.on_event(0, uid, &Port::In, &mut state, event5)?;
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
          interval = 15 * 1_000_000_000
        end;
        define window w30s from tumbling
        with
          interval = 30 * 1_000_000_000
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

    let stmt = test_select_stmt(stmt_ast);

    let mut op = test_select(test_uid(), &stmt);
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
        mid: mid(),
        value: Value::from(true),
    }));

    let stmt = test_select_stmt(stmt_ast);

    let mut op = test_select(test_uid(), &stmt);

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

    stmt_ast.maybe_where = Some(ImutExpr::from(ast::Literal {
        mid: mid(),
        value: Value::from(false),
    }));
    let stmt = test_select_stmt(stmt_ast);

    let mut op = test_select(test_uid(), &stmt);
    let next = try_enqueue(&mut op, test_event(0))?;
    assert_eq!(None, next);
    Ok(())
}

#[test]
fn select_nowin_nogrp_whrbad_nohav() {
    let target = test_target();
    let mut stmt_ast = test_stmt(target);
    stmt_ast.maybe_where = Some(ImutExpr::from(ast::Literal {
        mid: mid(),
        value: Value::from("snot"),
    }));

    let stmt = test_select_stmt(stmt_ast);

    let mut op = test_select(test_uid(), &stmt);

    assert!(try_enqueue(&mut op, test_event(0)).is_err());
}

#[test]
fn select_nowin_nogrp_whrt_havt() -> Result<()> {
    let target = test_target();
    let mut stmt_ast = test_stmt(target);
    stmt_ast.maybe_where = Some(ImutExpr::from(ast::Literal {
        mid: mid(),
        value: Value::from(true),
    }));
    stmt_ast.maybe_having = Some(ImutExpr::from(ast::Literal {
        mid: mid(),
        value: Value::from(true),
    }));

    let stmt = test_select_stmt(stmt_ast);

    let mut op = test_select(test_uid(), &stmt);

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
        mid: mid(),
        value: Value::from(true),
    }));
    stmt_ast.maybe_having = Some(ImutExpr::from(ast::Literal {
        mid: mid(),
        value: Value::from(false),
    }));

    let stmt = test_select_stmt(stmt_ast);

    let mut op = test_select(test_uid(), &stmt);
    let event = test_event(0);

    let next = try_enqueue(&mut op, event)?;

    assert_eq!(None, next);
    Ok(())
}

#[test]
fn select_nowin_nogrp_whrt_havbad() -> Result<()> {
    use halfbrown::hashmap;
    let target = test_target();
    let mut stmt_ast = test_stmt(target);
    stmt_ast.maybe_where = Some(ImutExpr::from(ast::Literal {
        mid: mid(),
        value: Value::from(true),
    }));
    stmt_ast.maybe_having = Some(ImutExpr::from(Literal {
        mid: mid(),
        value: Value::from(hashmap! {
            "snot".into() => "badger".into(),
        }),
    }));

    let stmt = test_select_stmt(stmt_ast);

    let mut op = test_select(test_uid(), &stmt);
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
    let mut vm = literal!({
       "h2g2" : 42,
    })
    .into();
    assert_eq!(
        Actions {
            include: false,
            emit: false
        },
        window.on_event(0, &mut vm, ingest_ns(5), &None)?
    );
    assert_eq!(
        Actions::all_false(),
        window.on_event(0, &mut vm, ingest_ns(10), &None)?
    );
    assert_eq!(
        Actions {
            include: false,
            emit: true
        },
        window.on_event(0, &mut vm, ingest_ns(15), &None)? // exactly on time
    );
    assert_eq!(
        Actions {
            include: false,
            emit: true
        },
        window.on_event(0, &mut vm, ingest_ns(26), &None)? // exactly on time
    );
    Ok(())
}

#[test]
fn tumbling_window_on_time_from_script_emit() -> Result<()> {
    // create a WindowDecl with a custom script
    let reg = Registry::default();
    let aggr_reg = AggrRegistry::default();
    let q = tremor_script::query::Query::parse(
        &r#"
            define window my_window from tumbling
            with
                interval = 1000000000 # 1 second
            script
                event.timestamp
            end;"#,
        &reg,
        &aggr_reg,
    )?;
    let window_defn = match q.query.scope.content.windows.values().next() {
        Some(defn) => defn,
        other => return Err(format!("Didnt get a window defn, got: {other:?}").into()),
    };
    let mut params = halfbrown::HashMap::with_capacity(1);
    params.insert("size".to_string(), Value::from(3));
    let with = window_defn.params.render()?;
    let interval = with
        .get("interval")
        .and_then(Value::as_u64)
        .ok_or("no interval found")?;
    let mut window = window::TumblingOnTime::from_stmt(
        interval,
        window::Impl::DEFAULT_MAX_GROUPS,
        Some(window_defn),
    );
    let mut json1 = literal!({
        "timestamp": 1_000_000_000
    })
    .into();
    assert_eq!(
        Actions {
            include: false,
            emit: false
        },
        window.on_event(0, &mut json1, 1, &None)?
    );
    let mut json2 = literal!({
        "timestamp": 1_999_999_999
    })
    .into();
    assert_eq!(
        Actions::all_false(),
        window.on_event(0, &mut json2, 2, &None)?
    );
    let mut json3 = literal!({
        "timestamp": 2_000_000_000
    })
    .into();
    // ignoring on_tick as we have a script
    assert_eq!(Actions::all_false(), window.on_tick(0, 2_000_000_000)?);
    assert_eq!(
        Actions {
            include: false,
            emit: true
        },
        window.on_event(0, &mut json3, 3, &None)?
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
        window.on_tick(0, 0)?
    );
    assert_eq!(Actions::all_false(), window.on_tick(0, 99)?);
    assert_eq!(
        Actions {
            include: false,
            emit: true // we delete windows that do not have content so this is fine
        },
        window.on_tick(0, 100)?
    );
    let mut v = ValueAndMeta::default();
    assert_eq!(
        Actions::all_false(),
        window.on_event(0, &mut v, 101, &None)?
    );
    assert_eq!(Actions::all_false(), window.on_tick(0, 102)?);
    assert_eq!(
        Actions {
            include: false,
            emit: true // we had an event yeah
        },
        window.on_tick(0, 200)?
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
        window.on_tick(0, 0)?
    );
    assert_eq!(Actions::all_false(), window.on_tick(0, 99)?);
    assert_eq!(
        Actions {
            include: false,
            emit: true // we **DO** emit even if we had no event
        },
        window.on_tick(0, 100)?
    );
    let mut v = ValueAndMeta::default();
    assert_eq!(
        Actions::all_false(),
        window.on_event(0, &mut v, 101, &None)?
    );
    assert_eq!(Actions::all_false(), window.on_tick(0, 102)?);
    assert_eq!(
        Actions {
            include: false,
            emit: true // we had an event yeah
        },
        window.on_tick(0, 200)?
    );

    Ok(())
}

#[test]
fn no_window_emit() -> Result<()> {
    let mut window = window::No::default();

    let mut vm = literal!({
       "h2g2" : 42,
    })
    .into();

    assert_eq!(
        Actions::all_true(),
        window.on_event(0, &mut vm, ingest_ns(0), &None)?
    );
    assert_eq!(Actions::all_false(), window.on_tick(0, 0)?);
    assert_eq!(
        Actions::all_true(),
        window.on_event(0, &mut vm, ingest_ns(1), &None)?
    );
    assert_eq!(Actions::all_false(), window.on_tick(0, 1)?);
    Ok(())
}

#[test]
fn tumbling_window_on_number_emit() -> Result<()> {
    let mut window = window::TumblingOnNumber::from_stmt(3, window::Impl::DEFAULT_MAX_GROUPS, None);

    let mut vm = literal!({
       "h2g2" : 42,
    })
    .into();

    // do not emit yet
    assert_eq!(
        Actions::all_false(),
        window.on_event(0, &mut vm, ingest_ns(0), &None)?
    );
    assert_eq!(Actions::all_false(), window.on_tick(0, 1_000_000_000)?);
    // do not emit yet
    assert_eq!(
        Actions::all_false(),
        window.on_event(0, &mut vm, ingest_ns(1), &None)?
    );
    assert_eq!(Actions::all_false(), window.on_tick(0, 2_000_000_000)?);
    // emit and open on the third event
    assert_eq!(
        Actions::all_true(),
        window.on_event(0, &mut vm, ingest_ns(2), &None)?
    );
    // no emit here, next window
    assert_eq!(
        Actions::all_false(),
        window.on_event(0, &mut vm, ingest_ns(3), &None)?
    );

    Ok(())
}
