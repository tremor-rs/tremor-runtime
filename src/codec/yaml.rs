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
pub struct YAML {}

impl Codec for YAML {
    fn name(&self) -> String {
        "yaml".to_string()
    }

    fn decode(&mut self, data: Vec<u8>, _ingest_ns: u64) -> Result<Option<LineValue>> {
        LineValue::try_new(vec![data], |data| {
            serde_yaml::from_slice::<simd_json::OwnedValue>(&data[0])
                .map(Value::from)
                .map(ValueAndMeta::from)
        })
        .map(Some)
        .map_err(|e| e.0.into())
    }
    fn encode(&self, data: &simd_json::BorrowedValue) -> Result<Vec<u8>> {
        Ok(serde_yaml::to_vec(data)?)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use simd_json::json;
    use simd_json::BorrowedValue;
    use simd_json::OwnedValue;

    #[test]
    fn test_yaml_codec() -> Result<()> {
        let seed: OwnedValue = json!({ "snot": "badger" });
        let seed: BorrowedValue = seed.into();

        let mut codec = YAML {};
        let as_raw = codec.encode(&seed)?;
        let as_json = codec.decode(as_raw, 0);

        let _ = dbg!(as_json);

        Ok(())
    }
}
