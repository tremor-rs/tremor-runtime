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

//! The `yaml` codec supports marshalling the `YAML Ain't Markup Language` format.
//!
//! Specification: [YAML 1.2](https://yaml.org).

use crate::prelude::*;

#[derive(Clone)]
pub struct Yaml {}

#[async_trait::async_trait]
impl Codec for Yaml {
    fn name(&self) -> &str {
        "yaml"
    }

    fn mime_types(&self) -> Vec<&'static str> {
        vec!["application/yaml"]
    }

    async fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        _ingest_ns: u64,
        meta: Value<'input>,
    ) -> Result<Option<(Value<'input>, Value<'input>)>> {
        serde_yaml::from_slice::<simd_json::OwnedValue>(data)
            .map(Value::from)
            .map(|v| Some((v, meta)))
            .map_err(Error::from)
    }
    async fn encode(&mut self, data: &Value, _meta: &Value) -> Result<Vec<u8>> {
        Ok(serde_yaml::to_string(data)?.into_bytes())
    }

    fn boxed_clone(&self) -> Box<dyn Codec> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tremor_value::literal;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_yaml_codec() -> Result<()> {
        let seed = literal!({ "snot": "badger" });

        assert_eq!("yaml", Yaml {}.name());

        assert_eq!("application/yaml", Yaml {}.mime_types()[0]);

        let codec_clone = Yaml {}.boxed_clone();
        assert_eq!("yaml", codec_clone.name());

        let mut codec = Yaml {};
        let mut as_raw = codec.encode(&seed, &Value::const_null()).await?;
        let as_json = codec
            .decode(as_raw.as_mut_slice(), 0, Value::object())
            .await?
            .expect("no data");

        assert_eq!(seed, as_json.0);

        Ok(())
    }
}
