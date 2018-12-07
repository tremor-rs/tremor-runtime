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

use super::messages::{Event, Return, Shutdown};
use super::op::{Op, Opable};
use super::types::EventResult;
use crate::error::TSError;
use actix;
use actix::prelude::*;
use futures::Future;
#[derive(Debug)]
pub struct Step {
    op: Op,
    uuid: String,
    out: Vec<Option<Addr<Step>>>,
}

impl Step {
    pub fn actor(op: Op, out: Vec<Option<Addr<Step>>>) -> Addr<Self> {
        let uuid = op.spec.uuid.to_hyphenated().to_string();
        Step::create(|_ctx| Step { op, out, uuid })
    }
}

impl Actor for Step {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Context<Self>) {}
}

impl Handler<Shutdown> for Step {
    type Result = ();
    fn handle(&mut self, _shutdown: Shutdown, _ctx: &mut Context<Self>) -> Self::Result {
        self.op.shutdown();
        for n in &self.out {
            if let Some(n) = n {
                let _ = n.send(Shutdown {}).wait();
            }
        }
    }
}

impl Handler<Event> for Step {
    type Result = ();
    fn handle(&mut self, event: Event, ctx: &mut Context<Self>) -> Self::Result {
        // Note: We index our outputs the same way file descriptors in unix are
        // used. This means the 'standard output' is 1 not 0!
        // Vectors (the way we store outputs) are indexed by 0, this means we have
        // to subtract 1 for every
        match self.op.exec(event.data) {
            EventResult::Next(result) => {
                if let Some(Some(ref out)) = self.out.get(0) {
                    out.do_send(Event {
                        data: result.add_to_chain(ctx.address()),
                    })
                } else {
                    warn!("Aborting pipeline as output 'standard' (1) does not exist!");
                    result
                        .add_to_chain(ctx.address())
                        .make_return(Err(TSError::new(&"No standard output provided")))
                        .send()
                }
            }
            EventResult::Error(mut event, err) => {
                if let Some(Some(ref err_actor)) = self.out.get(1) {
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
                                .set_error(Some(err.clone()))
                                .add_to_chain(ctx.address())
                                .make_return(Err(err))
                                .send()
                        }
                        None => {
                            error!("undefined error");
                            event
                                .add_to_chain(ctx.address())
                                .make_return(Err(TSError::new(&"unknown error")))
                                .send()
                        }
                    }
                }
            }
            EventResult::NextID(0, result) => {
                error!("Aborting pipeline as output -1 does not exist!");
                result
                    .add_to_chain(ctx.address())
                    .make_return(Err(TSError::new(
                        &"Aborting pipeline as output -1 does not exist!",
                    )))
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
                        .make_return(Err(TSError::new(&format!(
                            "No output output provided for id: {}",
                            id
                        ))))
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

impl Handler<Return> for Step {
    type Result = ();
    fn handle(&mut self, mut ret: Return, _ctx: &mut Context<Self>) -> Self::Result {
        let v1 = self.op.result(ret.v);
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
