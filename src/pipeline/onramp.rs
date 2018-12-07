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

use super::messages::{Event, OnData, Return, Shutdown};
use super::step::Step;
use super::types::EventData;
use actix;
use actix::prelude::*;
use futures::sync::mpsc;
use futures::Future;
use std::collections::HashMap;

pub type OnRampActorReplyChannel = mpsc::Sender<Return>;

#[derive(Debug)]
pub struct OnRampActor {
    pub pipeline: Addr<Step>,
    pub id: u64,
    pub replies: HashMap<u64, OnRampActorReplyChannel>,
}

impl Actor for OnRampActor {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Context<Self>) {}
}

impl Handler<OnData> for OnRampActor {
    type Result = ();
    fn handle(&mut self, data: OnData, ctx: &mut Context<Self>) -> Self::Result {
        let event = Event {
            data: EventData::new_with_vars(
                self.id,
                data.ingest_ns,
                Some(ctx.address()),
                data.data,
                data.vars,
            ),
        };
        if let Some(chan) = data.reply_channel {
            self.replies.insert(self.id, chan);
        }
        self.id += 1;
        self.pipeline.do_send(event);
    }
}
impl Handler<Shutdown> for OnRampActor {
    type Result = ();
    fn handle(&mut self, shutdown: Shutdown, _ctx: &mut Context<Self>) -> Self::Result {
        let _ = self.pipeline.send(shutdown).wait();
    }
}

impl Handler<Return> for OnRampActor {
    type Result = ();
    fn handle(&mut self, ret: Return, _ctx: &mut Context<Self>) -> Self::Result {
        for i in 0..ret.ids.len() {
            match self.replies.remove(&ret.ids[i]) {
                None => (),
                Some(mut chan) => {
                    if let Err(e) = chan.try_send(ret.clone()) {
                        error!("Pipeline return error: {}", e);
                    }
                }
            }
        }
    }
}
