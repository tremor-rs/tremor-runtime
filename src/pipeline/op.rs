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

use super::messages::Signal;
use super::types::{ConfValue, EventData, EventResult, EventReturn, ValueType};
use crate::errors::*;
use crate::op::generic::Generic;
use crate::op::grouping::Grouper;
use crate::op::offramp::Offramp;
use crate::op::parser::{Parser, Renderer};
use crate::op::runtime::Runtime;
use std::collections::HashSet;
use std::fmt;
use uuid::Uuid;

pub trait Opable {
    fn on_event(&mut self, event: EventData) -> EventResult;
    fn on_result(&mut self, result: EventReturn) -> EventReturn {
        result
    }
    fn on_timeout(&mut self) -> EventResult {
        EventResult::Done
    }
    fn on_signal(&mut self, _signal: &Signal) {}
    fn input_type(&self) -> ValueType;
    fn output_type(&self) -> ValueType;
    fn shutdown(&mut self) {}
    fn input_vars(&self) -> HashSet<String> {
        HashSet::new()
    }
    fn output_vars(&self) -> HashSet<String> {
        HashSet::new()
    }
}

#[derive(Debug, Clone)]
pub enum OpType {
    Offramp,
    Op,
    Parse,
    Render,
    Grouper,
    Runtime,
}
impl fmt::Display for OpType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OpType::Offramp => write!(f, "offramp"),
            OpType::Op => write!(f, "op"),
            OpType::Parse => write!(f, "parse"),
            OpType::Render => write!(f, "render"),
            OpType::Grouper => write!(f, "grouper"),
            OpType::Runtime => write!(f, "runtime"),
        }
    }
}

pub struct StepConfig {
    pub namespace: String,
    pub name: String,
    pub config: ConfValue,
    pub uuid: Uuid,
}

#[derive(Debug, Clone)]
pub struct OpSpec {
    pub optype: OpType,
    pub name: String,
    pub opts: ConfValue,
    pub uuid: Uuid,
}

impl OpSpec {
    pub fn from_config(step: StepConfig) -> Result<Self> {
        match step.namespace.as_str() {
            "offramp" => Ok(OpSpec::new(
                OpType::Offramp,
                step.name,
                step.config,
                step.uuid,
            )),
            "op" => Ok(OpSpec::new(OpType::Op, step.name, step.config, step.uuid)),
            "parse" => Ok(OpSpec::new(
                OpType::Parse,
                step.name,
                step.config,
                step.uuid,
            )),
            "render" => Ok(OpSpec::new(
                OpType::Render,
                step.name,
                step.config,
                step.uuid,
            )),
            "grouper" => Ok(OpSpec::new(
                OpType::Grouper,
                step.name,
                step.config,
                step.uuid,
            )),
            "runtime" => Ok(OpSpec::new(
                OpType::Runtime,
                step.name,
                step.config,
                step.uuid,
            )),

            _ => Err(ErrorKind::UnknownNamespace(step.namespace).into()),
        }
    }

    pub fn new(optype: OpType, name: String, opts: ConfValue, uuid: Uuid) -> Self {
        Self {
            optype,
            name,
            opts,
            uuid,
        }
    }

    pub fn to_op(&self) -> Result<Op> {
        match self.optype {
            OpType::Offramp => Ok(Op {
                op: OpE::Offramp(Offramp::create(&self.name, &self.opts)?),
                spec: self.clone(),
            }),
            OpType::Op => Ok(Op {
                op: OpE::Op(Generic::create(&self.name, &self.opts)?),
                spec: self.clone(),
            }),
            OpType::Parse => Ok(Op {
                op: OpE::Parse(Parser::create(&self.name, &self.opts)?),
                spec: self.clone(),
            }),
            OpType::Render => Ok(Op {
                op: OpE::Render(Renderer::create(&self.name, &self.opts)?),
                spec: self.clone(),
            }),
            OpType::Grouper => Ok(Op {
                op: OpE::Grouper(Grouper::create(&self.name, &self.opts)?),
                spec: self.clone(),
            }),
            OpType::Runtime => Ok(Op {
                op: OpE::Runtime(Runtime::create(&self.name, &self.opts)?),
                spec: self.clone(),
            }),
        }
    }
}
#[derive(Debug)]
enum OpE {
    Offramp(Offramp),
    Op(Generic),
    Parse(Parser),
    Render(Renderer),
    Grouper(Grouper),
    Runtime(Runtime),
}

opable!(OpE, Op, Parse, Offramp, Render, Grouper, Runtime);

#[derive(Debug)]
pub struct Op {
    pub spec: OpSpec,
    op: OpE,
}

impl Opable for Op {
    fn on_timeout(&mut self) -> EventResult {
        self.op.on_timeout()
    }
    fn on_event(&mut self, event: EventData) -> EventResult {
        self.op.on_event(event)
    }
    fn on_signal(&mut self, signal: &Signal) {
        self.op.on_signal(signal)
    }
    fn on_result(&mut self, result: EventReturn) -> EventReturn {
        self.op.on_result(result)
    }
    fn input_type(&self) -> ValueType {
        self.op.input_type()
    }
    fn output_type(&self) -> ValueType {
        self.op.output_type()
    }
    fn shutdown(&mut self) {
        self.op.shutdown()
    }
    fn input_vars(&self) -> HashSet<String> {
        self.op.input_vars()
    }
    fn output_vars(&self) -> HashSet<String> {
        self.op.output_vars()
    }
}
