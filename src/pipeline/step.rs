// Copyright 2018, Wayfair GmbH
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

use super::messages::{Event, Return, Shutdown, Signal, Timeout};
use super::op::{Op, Opable};
use super::types::EventResult;
use crate::errors::*;
use actix;
use actix::prelude::*;
use std::time::Duration;
#[derive(Debug)]
pub struct Step {
    op: Op,
    uuid: String,
    timeout_handle: Option<SpawnHandle>,
    out: Vec<Option<Addr<Step>>>,
}

impl Step {
    pub fn actor(op: Op, out: Vec<Option<Addr<Step>>>) -> Addr<Self> {
        let uuid = op.spec.uuid.to_hyphenated().to_string();
        Step::create(|_ctx| Step {
            op,
            out,
            uuid,
            timeout_handle: None,
        })
    }
    fn handle_op_result(&mut self, result: EventResult, ctx: &mut Context<Self>) {
        match result {
            EventResult::Timeout {
                timeout_millis,
                result,
            } => {
                if timeout_millis == 0 {
                    self.timeout_handle = None;
                } else {
                    let handle =
                        ctx.notify_later(Timeout {}, Duration::from_millis(timeout_millis));
                    self.timeout_handle = Some(handle);
                }
                self.handle_op_result(*result, ctx)
            }
            EventResult::Error(mut event, err) => {
                if let Some(Some(ref err_actor)) = self.out.get(1) {
                    // Like with unix file descriptors we need to account for 1-based indexing verses 0-based offsets in get(...) above
                    // For example, get(1), is referring to the *second* element
                    //
                    if let Some(ref e) = err {
                        event.set_var(&"tremor-error", format!("{}", e));
                    };

                    err_actor.do_send(Event {
                        data: event.set_error(err).add_to_chain(ctx.address()),
                    });
                } else {
                    match err {
                        Some(err) => {
                            error!("{}", err);
                            event
                                .add_to_chain(ctx.address())
                                .make_return(Err(err))
                                .send()
                        }
                        None => {
                            error!("undefined error");
                            event
                                .add_to_chain(ctx.address())
                                .make_return(Err("unknown error".into()))
                                .send()
                        }
                    }
                }
            }
            EventResult::NextID(0, result) => {
                error!("Aborting pipeline as output -1 does not exist!");
                result
                    .add_to_chain(ctx.address())
                    .make_return(Err(ErrorKind::BadOutputid(0).into()))
                    .send()
            }
            EventResult::NextID(id, result) => {
                if let Some(Some(ref out)) = self.out.get(id - 1) {
                    out.do_send(Event {
                        data: result.add_to_chain(ctx.address()),
                    })
                } else {
                    warn!("No output output provided for id: {}", id);
                    result
                        .add_to_chain(ctx.address())
                        .make_return(Err(ErrorKind::BadOutputid(id).into()))
                        .send()
                }
            }

            EventResult::Return(ret) => {
                ret.send();
            }
            EventResult::StepError(e) => error!("Step related error: {:?}", e),
            EventResult::Done => (),
        }
    }
}

impl Actor for Step {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Context<Self>) {}
}

impl Handler<Shutdown> for Step {
    type Result = ();
    fn handle(&mut self, shutdown: Shutdown, ctx: &mut Context<Self>) -> Self::Result {
        self.op.shutdown();
        for n in &self.out {
            if let Some(n) = n {
                n.send(shutdown.clone())
                    .into_actor(self)
                    .then(|_, _, _| actix::fut::ok(()))
                    .wait(ctx);
            }
        }
    }
}

impl Handler<Signal> for Step {
    type Result = ();
    fn handle(&mut self, signal: Signal, _ctx: &mut Context<Self>) -> Self::Result {
        self.op.on_signal(&signal);
        for n in &self.out {
            if let Some(n) = n {
                n.do_send(signal.clone());
            }
        }
    }
}

impl Handler<Timeout> for Step {
    type Result = ();
    fn handle(&mut self, _timeout: Timeout, ctx: &mut Context<Self>) -> Self::Result {
        self.timeout_handle = None;
        let result = self.op.on_timeout();
        self.handle_op_result(result, ctx)
    }
}

impl Handler<Event> for Step {
    type Result = ();
    fn handle(&mut self, event: Event, ctx: &mut Context<Self>) -> Self::Result {
        let result = self.op.on_event(event.data);
        self.handle_op_result(result, ctx)
    }
}

impl Handler<Return> for Step {
    type Result = ();
    fn handle(&mut self, mut ret: Return, _ctx: &mut Context<Self>) -> Self::Result {
        let v1 = self.op.on_result(ret.v);
        ret.v = v1;
        match ret.chain.pop() {
            None => {
                if let Some(src) = ret.source.clone() {
                    src.do_send(ret)
                }
            }
            Some(prev) => prev.do_send(ret),
        }
    }
}
