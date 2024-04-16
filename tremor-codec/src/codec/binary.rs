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

use crate::prelude::*;

#[derive(Clone)]
pub struct Binary {}

#[async_trait::async_trait]
impl Codec for Binary {
    fn name(&self) -> &str {
        "bytes"
    }

    fn mime_types(&self) -> Vec<&'static str> {
        vec!["application/octet-stream"]
    }

    async fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        _ingest_ns: u64,
        meta: Value<'input>,
    ) -> Result<Option<(Value<'input>, Value<'input>)>> {
        let data: &'input [u8] = data;
        Ok(Some((Value::Bytes(data.into()), meta)))
    }

    async fn encode(&mut self, data: &Value, _meta: &Value) -> Result<Vec<u8>> {
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
    use tremor_value::literal;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_binary_codec() -> Result<()> {
        let seed = Value::Bytes("snot badger".as_bytes().into());

        let mut codec = Binary {};
        assert_eq!("bytes", codec.name());
        assert_eq!("application/octet-stream", codec.mime_types()[0]);

        let mut as_raw = codec.encode(&seed, &Value::const_null()).await?;
        assert_eq!(as_raw, b"snot badger");
        let as_value = codec
            .decode(as_raw.as_mut_slice(), 0, Value::object())
            .await?
            .unwrap_or_default();
        assert_eq!(as_value.0, seed);

        let mut as_raw = codec
            .encode(&Value::from("snot badger"), &Value::const_null())
            .await?;
        let as_value = codec
            .decode(as_raw.as_mut_slice(), 0, Value::object())
            .await?
            .unwrap_or_default();
        assert_eq!(as_value.0, seed);

        let mut as_raw = codec
            .encode(&literal!([1, 2, 3]), &Value::const_null())
            .await?;
        let as_value = codec
            .decode(as_raw.as_mut_slice(), 0, Value::object())
            .await?
            .unwrap_or_default();
        assert_eq!(
            "[1,2,3]",
            &String::from_utf8_lossy(as_value.0.as_bytes().expect("bytes"))
        );

        let codec_clone = codec.boxed_clone();
        assert_eq!("bytes", codec_clone.name());

        Ok(())
    }
}
