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
use simd_json::{self, BorrowedValue as Value};
use tremor_script::LineValue;

#[derive(Clone)]
pub struct String {}

impl Codec for String {
    fn decode(&mut self, data: Vec<u8>, _ingest_ns: u64) -> Result<Option<LineValue>> {
        LineValue::try_new(Box::new(vec![data]), |data| {
            Ok(Value::from(std::str::from_utf8(data[0].as_slice())?))
        })
        .map_err(|e| e.0)
        .map(Some)
    }

    fn encode(&self, data: &simd_json::BorrowedValue) -> Result<Vec<u8>> {
        match data {
            Value::String(s) => Ok(s.as_bytes().to_vec()),
            data => Ok(serde_json::to_vec(&data)?),
        }
    }
}
