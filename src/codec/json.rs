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

use super::Codec;
use crate::errors::*;
use serde_json;
use tremor_pipeline::EventValue;

#[derive(Clone)]
pub struct JSON {}

impl Codec for JSON {
    fn decode(&self, data: EventValue) -> Result<EventValue> {
        match data {
            EventValue::JSON(_) => Ok(data),
            EventValue::Raw(data) => Ok(EventValue::JSON(serde_json::from_slice(&data)?)),
            EventValue::None => Ok(EventValue::JSON(serde_json::Value::Null)),
        }
    }
    fn encode(&self, data: EventValue) -> Result<EventValue> {
        match data {
            EventValue::JSON(data) => Ok(EventValue::Raw(serde_json::to_vec(&data)?)),
            _ => Err("Trying to encode non json data.".into()),
        }
    }
}
