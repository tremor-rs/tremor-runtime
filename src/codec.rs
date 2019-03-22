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
use tremor_pipeline::EventValue;

pub mod influx;
pub mod json;
pub mod msgpack;

pub trait Codec: Send {
    fn decode(&self, data: EventValue) -> Result<EventValue>;
    fn encode(&self, data: EventValue) -> Result<EventValue>;
}

#[derive(Clone)]
struct Pass {}

impl Codec for Pass {
    fn decode(&self, data: EventValue) -> Result<EventValue> {
        Ok(data)
    }
    fn encode(&self, data: EventValue) -> Result<EventValue> {
        Ok(data)
    }
}

pub fn lookup(name: &str) -> Result<Box<dyn Codec>> {
    match name {
        "json" => Ok(Box::new(json::JSON {})),
        "msgpack" => Ok(Box::new(msgpack::MsgPack {})),
        "pass" => Ok(Box::new(Pass {})),
        "influx" => Ok(Box::new(influx::Influx {})),
        _ => Err(format!("Codec '{}' not found.", name).into()),
    }
}
