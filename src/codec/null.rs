// Copyright 2018-2020, Wayfair GmbH
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
use simd_json::ValueBuilder;
use tremor_script::{LineValue, Value};

#[derive(Clone)]
pub struct Null {}

impl Codec for Null {
    fn decode(&mut self, data: Vec<u8>, _ingest_ns: u64) -> Result<Option<LineValue>> {
        Ok(Some(LineValue::new(vec![data], |_| Value::null().into())))
    }
    fn encode(&self, _data: &simd_json::BorrowedValue) -> Result<Vec<u8>> {
        Ok(vec![])
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use simd_json::BorrowedValue;
    use simd_json::OwnedValue;

    #[test]
    fn test_null_codec() -> Result<()> {
        let seed: OwnedValue = OwnedValue::null();
        let seed: BorrowedValue = seed.into();

        let mut codec = Null {};
        let as_raw = codec.encode(&seed)?;
        let as_json = codec.decode(as_raw, 0);

        let _ = dbg!(as_json);

        Ok(())
    }
}
