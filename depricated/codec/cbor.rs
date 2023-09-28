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

use super::prelude::*;

#[derive(Clone)]
pub(crate) struct Cbor {}

impl Codec for Cbor {
    fn name(&self) -> &str {
        "yaml"
    }

    fn mime_types(&self) -> Vec<&str> {
        vec!["application/cbor"]
    }

    fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        _ingest_ns: u64,
    ) -> Result<Option<Value<'input>>> {
        let data: &[u8] = data;
        Ok(ciborium::de::from_reader::<'_, Value, _>(data)
            .map(Some)
            .unwrap())
        //.map_err(|e| format!("{}", e).into())
    }

    fn encode(&mut self, data: &Value, _meta: &Value) -> Result<Vec<u8>> {
        let mut res = Vec::with_capacity(128);
        ciborium::ser::into_writer(data, &mut res).map_err(|e| Error::from(format!("{}", e)))?;
        Ok(res)
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
    fn test_cbor_codec() -> Result<()> {
        let seed = literal!({ "snot": "badger" });

        let mut codec = Cbor {};
        let mut as_raw = codec.encode(&seed, &Value::const_null())?;
        let as_json = codec.decode(as_raw.as_mut_slice(), 0)?;

        Ok(())
    }
}
