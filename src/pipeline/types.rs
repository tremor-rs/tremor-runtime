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

use super::messages::Return;
use super::onramp::OnRampActor;
use super::step::Step;
use crate::errors::*;
use actix;
use actix::prelude::*;
use mimir::registry::Context;
use serde_json;
use std::collections::HashMap;
use std::convert::AsMut;
use std::fmt::{self, Debug, Display};
use std::result;

//use futures::Future;
pub type EventReturn = Result<Option<f64>>;
pub use serde_yaml::Value as ConfValue;
pub type MetaValue = serde_json::Value;

#[derive(Debug)]
pub enum EventResult {
    /// Moves the event to a given step, or error if the step is not configured. (3 is the first custom output)
    NextID(usize, Box<EventData>),

    /// Moves the event to the error output (same as NextID(2))
    Error(Box<EventData>, Option<Error>),

    // Ends the pipeline and triggers a return event with the given value
    Return(Box<Return>),
    // Ends the pipeline without triggering a return event for async processing
    Done,
    // A error happened at step level that is not related to the event itself.
    StepError(Error),
    Timeout {
        timeout_millis: u64,
        result: Box<EventResult>,
    },
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ValueType {
    Raw,  // Raw data
    JSON, // utf8 JSON
    Any,  // Any type
    Same, // Same as the input
    None, // Things end here
}

impl Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MetaValueType {
    String,
    U64,
    Bool,
    VecS,
}

pub type VarMap = HashMap<String, MetaValue>;

#[derive(Clone)]
pub struct EventData {
    pub id: u64,
    pub vars: VarMap,
    pub value: EventValue,
    pub ingest_ns: u64,
    last_error: Option<Error>,
    chain: Vec<Addr<Step>>,
    source: Option<Addr<OnRampActor>>,
}
impl Debug for EventData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Event({}) {:?} {:?}", self.id, self.vars, self.value)
    }
}

impl EventData {
    pub fn replace_value<F: FnOnce(&EventValue) -> Result<EventValue>>(
        mut self,
        f: F,
    ) -> result::Result<Self, EventResult> {
        match f(&self.value) {
            Ok(v) => {
                self.value = v;
                Ok(self)
            }
            Err(e) => Err(error_result!(self, e)),
        }
        //Err(EventResult::Error(self, Some(e)))
    }

    pub fn mutate_value<F: FnOnce(&mut EventValue) -> Result<()>>(&mut self, f: F) -> Result<()> {
        match f(&mut self.value) {
            Ok(_v) => Ok(()),
            Err(e) => Err(e),
        }
        //Err(EventResult::Error(self, Some(e)))
    }

    pub fn maybe_extract<T, F: FnOnce(&EventValue) -> Result<(T, EventReturn)>>(
        self,
        f: F,
    ) -> result::Result<(T, Return), EventResult> {
        match f(&self.value) {
            Ok((val, ret)) => Ok((
                val,
                Return {
                    source: self.source,
                    ids: vec![self.id],
                    v: ret,
                    chain: self.chain,
                },
            )),
            Err(e) => Err(error_result!(self, e)),
        }
    }

    pub fn with_value(self, value: EventValue) -> Self {
        EventData {
            ingest_ns: self.ingest_ns,
            id: self.id,
            vars: self.vars,
            value,
            chain: self.chain,
            source: self.source,
            last_error: None,
        }
    }
    pub fn set_error(mut self, error: Option<Error>) -> Self {
        self.last_error = error;
        self
    }
    pub fn new(
        id: u64,
        ingest_ns: u64,
        source: Option<Addr<OnRampActor>>,
        value: EventValue,
    ) -> Self {
        EventData::new_with_vars(id, ingest_ns, source, value, HashMap::new())
    }
    pub fn new_with_vars(
        id: u64,
        ingest_ns: u64,
        source: Option<Addr<OnRampActor>>,
        value: EventValue,
        vars: HashMap<String, MetaValue>,
    ) -> Self {
        EventData {
            id,
            ingest_ns,
            vars,
            value,
            chain: Vec::new(),
            source,
            last_error: None,
        }
    }
    pub fn add_to_chain(mut self, actor: Addr<Step>) -> Self {
        self.chain.push(actor);
        self
    }
    pub fn make_return(self, r: EventReturn) -> Return {
        Return {
            source: self.source,
            ids: vec![self.id],
            v: r,
            chain: self.chain,
        }
    }
    pub fn make_return_and_value(self, r: EventReturn) -> (Return, EventValue) {
        (
            Return {
                source: self.source,
                ids: vec![self.id],
                v: r,
                chain: self.chain,
            },
            self.value,
        )
    }
    pub fn set_var<S1: ToString, S2: Into<MetaValue>>(&mut self, k: &S1, v: S2) {
        self.vars.insert(k.to_string(), v.into());
    }
    pub fn copy_var<S1: ToString, S2: ToString>(&mut self, from: &S1, to: &S2) {
        let r: Option<MetaValue> = match &self.vars.get(&from.to_string()) {
            Some(v) => Some(MetaValue::clone(v)),
            _ => None,
        };
        match r {
            Some(v) => self.vars.insert(to.to_string(), v),
            _ => None,
        };
    }
    pub fn var<T: ToString>(&self, k: &T) -> Option<&MetaValue> {
        self.vars.get(&k.to_string())
    }
    pub fn var_clone<T: ToString>(&self, k: &T) -> Option<MetaValue> {
        match self.vars.get(&k.to_string()) {
            Some(v) => Some(v.clone()),
            None => None,
        }
    }
    pub fn is_type(&self, t: ValueType) -> bool {
        self.value.t() == t
    }
}

impl Context for EventData {}

impl AsMut<EventData> for EventData {
    fn as_mut(&mut self) -> &mut EventData {
        self
    }
}

#[derive(Debug, Clone)]
pub enum EventValue {
    Raw(Vec<u8>),
    JSON(serde_json::Value),
}

impl EventValue {
    pub fn t(&self) -> ValueType {
        match self {
            EventValue::Raw(_) => ValueType::Raw,
            EventValue::JSON(_) => ValueType::JSON,
        }
    }
}

impl AsMut<EventValue> for EventValue {
    fn as_mut(&mut self) -> &mut EventValue {
        self
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ReturnDest {
    pub source: Option<Addr<OnRampActor>>,
    pub chain: Vec<Addr<Step>>,
}
