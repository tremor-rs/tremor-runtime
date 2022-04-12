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
use rmp_serde as rmps;

#[derive(Clone)]
pub struct MsgPack {}

impl Codec for MsgPack {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "msgpack"
    }

    #[cfg(not(tarpaulin_include))]
    fn mime_types(&self) -> Vec<&'static str> {
        vec![
            "application/msgpack",
            "application/x-msgpack",
            "application/vnd.msgpack",
        ]
    }

    fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        _ingest_ns: u64,
    ) -> Result<Option<Value<'input>>> {
        rmps::from_slice::<Value>(data)
            .map(Some)
            .map_err(Error::from)
    }

    fn encode(&self, data: &Value) -> Result<Vec<u8>> {
        Ok(rmps::to_vec(&data)?)
    }

    #[cfg(not(tarpaulin_include))]
    fn boxed_clone(&self) -> Box<dyn Codec> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tremor_value::literal;

    #[test]
    fn test_msgpack_codec() -> Result<()> {
        let seed = literal!({ "snot": "badger" });

        let mut codec = MsgPack {};
        let mut as_raw = codec.encode(&seed)?;
        let as_json = codec.decode(as_raw.as_mut_slice(), 0);

        let _ = dbg!(as_json);

        Ok(())
    }
}
