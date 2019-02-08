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

use super::onramp::{OnRampActor, OnRampActorReplyChannel};
use super::step::Step;
use super::types::*;
use actix;
use actix::prelude::*;
use std::fmt;

#[derive(Clone)]
pub enum Signal {
    Default,
}

impl Message for Signal {
    type Result = ();
}

pub struct Event {
    pub data: EventData,
}

impl Message for Event {
    type Result = ();
}

//#[derive(Clone)]
pub struct OnData {
    pub data: EventValue,
    pub vars: VarMap,
    pub reply_channel: Option<OnRampActorReplyChannel>,
    pub ingest_ns: u64,
}

impl fmt::Debug for OnData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OnData({:?}, {:?})", self.data, self.vars)
    }
}

impl Message for OnData {
    type Result = ();
}

#[derive(Clone)]
pub struct Return {
    pub v: EventReturn,
    pub ids: Vec<u64>,
    pub chain: Vec<Addr<Step>>,
    pub source: Option<Addr<OnRampActor>>,
}

impl fmt::Debug for Return {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Return<{:?}>({:?})", self.ids, self.v)
    }
}

impl Message for Return {
    type Result = ();
}

impl Return {
    pub fn send(mut self) {
        match self.chain.pop() {
            None => {
                if let Some(src) = self.source.clone() {
                    src.do_send(self)
                }
            }
            Some(prev) => prev.do_send(self),
        }
    }
    pub fn with_value(mut self, v: EventReturn) -> Self {
        self.v = v;
        self
    }
}

#[derive(Clone)]
pub struct Shutdown;
impl Message for Shutdown {
    type Result = ();
}

pub struct Timeout {}
impl Message for Timeout {
    type Result = ();
}
