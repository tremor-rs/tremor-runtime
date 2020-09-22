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
pub struct JSON {}

impl Codec for JSON {
    fn name(&self) -> String {
        "json".to_string()
    }

    fn mime_types(&self) -> Vec<&str> {
        vec!["application/json"]
        // TODO: application/json-seq for one json doc per line?
    }

    fn decode<'input>(
        &self,
        data: &'input mut [u8],
        _ingest_ns: u64,
    ) -> Result<Option<Value<'input>>> {
        simd_json::to_borrowed_value(data)
            .map(Some)
            .map_err(|e| e.into())
    }
    fn encode(&self, data: &simd_json::BorrowedValue) -> Result<Vec<u8>> {
        let mut v = Vec::with_capacity(1024);
        data.write(&mut v)?;
        Ok(v)
    }
    fn encode_into(&self, data: &Value, dst: &mut Vec<u8>) -> Result<()> {
        data.write(dst)?;
        Ok(())
    }

    fn boxed_clone(&self) -> Box<dyn Codec> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use simd_json::json;
    use simd_json::BorrowedValue;
    use simd_json::OwnedValue;

    #[test]
    fn test_json_codec() -> Result<()> {
        let seed: OwnedValue = json!({ "snot": "badger" });
        let seed: BorrowedValue = seed.into();

        let codec = JSON {};
        let mut as_raw = codec.encode(&seed)?;
        let as_json = codec.decode(as_raw.as_mut_slice(), 0);

        let _ = dbg!(as_json);

        Ok(())
    }
}
