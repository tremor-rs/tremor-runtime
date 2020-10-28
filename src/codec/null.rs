// Copyright 2020, The Tremor Team
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

use super::prelude::*;

#[derive(Clone)]
pub struct Null {}

impl Codec for Null {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "null"
    }

    fn decode<'input>(
        &self,
        _data: &'input mut [u8],
        _ingest_ns: u64,
    ) -> Result<Option<Value<'input>>> {
        Ok(Some(Value::null()))
    }
    fn encode(&self, _data: &simd_json::BorrowedValue) -> Result<Vec<u8>> {
        Ok(vec![])
    }

    #[cfg(not(tarpaulin_include))]
    fn boxed_clone(&self) -> Box<dyn Codec> {
        Box::new(self.clone())
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

        let codec = Null {};
        let mut as_raw = codec.encode(&seed)?;
        let as_json = codec.decode(as_raw.as_mut_slice(), 0);
        assert!(as_json.is_ok());
        as_json?;

        Ok(())
    }
}
