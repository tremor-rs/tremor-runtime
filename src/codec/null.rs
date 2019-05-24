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
use tremor_script::{LineValue, Value};

#[derive(Clone)]
pub struct Null {}

impl Codec for Null {
    fn decode(&self, data: Vec<u8>) -> Result<LineValue> {
        Ok(LineValue::new(Box::new(data), |_| Value::Null))
    }
    fn encode(&self, _data: LineValue) -> Result<Vec<u8>> {
        Ok(vec![])
    }
}
