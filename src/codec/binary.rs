// Copyright 2020-2021, The Tremor Team
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

//! The `binary` codec marshalls raw binary data data as a tremor `bytes` literal value.
//!
//! When data isn't already represented as `bytes` it will be encoded as JSON document.

use super::prelude::*;

#[derive(Clone)]
pub struct Binary {}

impl Codec for Binary {
    fn name(&self) -> &str {
        "bytes"
    }

    fn mime_types(&self) -> Vec<&'static str> {
        vec!["application/octet-stream"]
    }

    fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        _ingest_ns: u64,
    ) -> Result<Option<Value<'input>>> {
        let data: &'input [u8] = data;
        Ok(Some(Value::Bytes(data.into())))
    }

    fn encode(&mut self, data: &Value) -> Result<Vec<u8>> {
        if let Some(s) = data.as_str() {
            Ok(s.as_bytes().to_vec())
        } else if let Value::Bytes(b) = data {
            Ok(b.to_vec())
        } else {
            Ok(simd_json::to_vec(&data)?)
        }
    }

    fn boxed_clone(&self) -> Box<dyn Codec> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_binary_codec() -> Result<()> {
        let seed = Value::Bytes("snot badger".as_bytes().into());

        let mut codec = Binary {};
        let mut as_raw = codec.encode(&seed)?;
        assert_eq!(as_raw, b"snot badger");
        let as_value = codec.decode(as_raw.as_mut_slice(), 0)?.unwrap_or_default();
        assert_eq!(as_value, seed);

        Ok(())
    }
}
