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
use rmp_serde as rmps;
use tremor_script::LineValue;

#[derive(Clone)]
pub struct MsgPack {}

impl Codec for MsgPack {
    fn decode(&self, data: Vec<u8>) -> Result<LineValue> {
        let r = LineValue::try_new(Box::new(data), |data| rmps::from_slice(data));
        match r {
            Ok(v) => Ok(v),
            Err(e) => Err(e.0.into()),
        }
    }
    fn encode(&self, data: LineValue) -> Result<Vec<u8>> {
        Ok(rmps::to_vec(&data)?)
    }
}
