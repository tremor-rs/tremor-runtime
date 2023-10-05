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

//! The `null` codec drops data.

use crate::prelude::*;

#[derive(Clone)]
pub struct Null {}

#[async_trait::async_trait]
impl Codec for Null {
    fn name(&self) -> &str {
        "null"
    }

    async fn decode<'input>(
        &mut self,
        _data: &'input mut [u8],
        _ingest_ns: u64,
        meta: Value<'input>,
    ) -> Result<Option<(Value<'input>, Value<'input>)>> {
        Ok(Some((Value::const_null(), meta)))
    }
    async fn encode(&mut self, _data: &Value, _meta: &Value) -> Result<Vec<u8>> {
        Ok(vec![])
    }

    fn boxed_clone(&self) -> Box<dyn Codec> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use simd_json::OwnedValue;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_null_codec() -> Result<()> {
        let seed: OwnedValue = OwnedValue::null();
        let seed: Value = seed.into();

        let mut codec = Null {};
        let mut as_raw = codec.encode(&seed, &Value::const_null()).await?;
        let as_json = codec
            .decode(as_raw.as_mut_slice(), 0, Value::object())
            .await;
        assert!(as_json.is_ok());
        as_json?;

        Ok(())
    }
}
