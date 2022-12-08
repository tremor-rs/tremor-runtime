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

//! The `string` codec marshalls valid UTF-8 encoded data as a tremor string literal value.
//!
//! ## Considerations
//!
//! If the data is not **valid UTF-8** marshalling will fail.

use super::prelude::*;

#[derive(Clone)]
pub struct String {}

impl Codec for String {
    fn name(&self) -> &str {
        "string"
    }

    fn mime_types(&self) -> Vec<&'static str> {
        vec!["text/plain", "text/html"]
    }

    fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        _ingest_ns: u64,
    ) -> Result<Option<Value<'input>>> {
        std::str::from_utf8(data)
            .map(Value::from)
            .map(Some)
            .map_err(Error::from)
    }

    fn encode(&self, data: &Value) -> Result<Vec<u8>> {
        if let Some(s) = data.as_str() {
            Ok(s.as_bytes().to_vec())
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
    use tremor_value::literal;

    #[test]
    fn test_string_codec() -> Result<()> {
        let seed = literal!("snot badger");

        let mut codec = String {};
        let mut as_raw = codec.encode(&seed)?;
        let as_json = codec.decode(as_raw.as_mut_slice(), 0);
        assert!(as_json.is_ok());

        Ok(())
    }

    #[test]
    fn test_string_codec2() -> Result<()> {
        let seed = literal!(["snot badger"]);

        let codec = String {};
        let as_raw = codec.encode(&seed)?;
        assert_eq!(as_raw, b"[\"snot badger\"]");

        Ok(())
    }
}
