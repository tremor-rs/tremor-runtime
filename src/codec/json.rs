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

use std::cmp::max;

use super::prelude::*;
use simd_json::{prelude::*, AlignedBuf};

pub struct JSON {
    input_buffer: AlignedBuf,
    string_buffer: Vec<u8>,
}

impl Clone for JSON {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl Default for JSON {
    fn default() -> Self {
        Self {
            input_buffer: AlignedBuf::with_capacity(1024),
            string_buffer: Vec::with_capacity(1024),
        }
    }
}

impl Codec for JSON {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "json"
    }

    #[cfg(not(tarpaulin_include))]
    fn mime_types(&self) -> Vec<&str> {
        vec!["application/json"]
        // TODO: application/json-seq for one json doc per line?
    }

    fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        _ingest_ns: u64,
    ) -> Result<Option<Value<'input>>> {
        // The input buffer will be automatically grown if required
        if self.string_buffer.capacity() < data.len() {
            self.input_buffer
                .reserve(max(self.string_buffer.capacity(), data.len()) * 2);
        }
        simd_json::value::borrowed::to_value_with_buffers(
            data,
            &mut self.input_buffer,
            &mut self.string_buffer,
        )
        .map(Some)
        .map_err(|e| e.into())
    }
    fn encode(&self, data: &simd_json::BorrowedValue) -> Result<Vec<u8>> {
        let mut v = Vec::with_capacity(1024);
        self.encode_into(data, &mut v)?;
        Ok(v)
    }
    fn encode_into(&self, data: &Value, dst: &mut Vec<u8>) -> Result<()> {
        data.write(dst)?;
        Ok(())
    }

    #[cfg(not(tarpaulin_include))]
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
    fn decode() -> Result<()> {
        let mut codec = JSON {
            input_buffer: AlignedBuf::with_capacity(0),
            string_buffer: Vec::new(),
        };
        let expected: OwnedValue = json!({ "snot": "badger" });

        let mut data = br#"{ "snot": "badger" }"#.to_vec();
        let output = codec.decode(&mut data, 42)?.unwrap();
        assert_eq!(expected, output);

        let mut codec = codec.clone();

        let mut data = br#"{ "snot": "badger" }"#.to_vec();
        let output = codec.decode(&mut data, 42)?.unwrap();
        assert_eq!(expected, output);

        Ok(())
    }

    #[test]
    fn test_json_codec() -> Result<()> {
        let seed: OwnedValue = json!({ "snot": "badger" });
        let seed: BorrowedValue = seed.into();

        let mut codec = JSON::default();

        let mut as_raw = codec.encode(&seed)?;
        let as_json = codec.decode(as_raw.as_mut_slice(), 0);

        let _ = dbg!(as_json);

        Ok(())
    }
}
