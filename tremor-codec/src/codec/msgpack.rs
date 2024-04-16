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

//! The `msgpack` codec supports the [Msgpack](https://msgpack.org) binary format that.
//!
//! The format is structurally compatible with JSON.
//!
//! Message pack is typically significantly more compute efficient to process and requires less memory
//! when compared to typical JSON processors.

use crate::prelude::*;
use rmp_serde as rmps;

#[derive(Clone)]
pub struct MsgPack {}

#[async_trait::async_trait]
impl Codec for MsgPack {
    fn name(&self) -> &str {
        "msgpack"
    }

    fn mime_types(&self) -> Vec<&'static str> {
        vec![
            "application/msgpack",
            "application/x-msgpack",
            "application/vnd.msgpack",
        ]
    }

    async fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        _ingest_ns: u64,
        meta: Value<'input>,
    ) -> Result<Option<(Value<'input>, Value<'input>)>> {
        rmps::from_slice::<Value>(data)
            .map(|v| Some((v, meta)))
            .map_err(Error::from)
    }

    async fn encode(&mut self, data: &Value, _meta: &Value) -> Result<Vec<u8>> {
        Ok(rmps::to_vec(&data)?)
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
    fn for_coverage_gods() {
        assert_eq!("msgpack", MsgPack {}.name());

        assert_eq!("application/msgpack", MsgPack {}.mime_types()[0]);
        assert_eq!("application/x-msgpack", MsgPack {}.mime_types()[1]);
        assert_eq!("application/vnd.msgpack", MsgPack {}.mime_types()[2]);

        let codec_clone: Box<dyn Codec> = MsgPack {}.boxed_clone();
        assert_eq!("msgpack", codec_clone.name());
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_msgpack_codec() -> Result<()> {
        let seed = literal!({ "snot": "badger" });

        let mut codec = MsgPack {};
        let mut as_raw = codec.encode(&seed, &Value::const_null()).await?;
        assert!(codec
            .decode(as_raw.as_mut_slice(), 0, Value::object())
            .await?
            .is_some());

        Ok(())
    }
}
