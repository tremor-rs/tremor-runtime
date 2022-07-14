// Copyright 2021, The Tremor Team
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

use crate::connectors::prelude::*;
use tremor_value::Value;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
struct Config {
    value: simd_json::OwnedValue,
}
impl tremor_config::Impl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "oneshot".into()
    }
    async fn build_cfg(
        &self,
        _: &alias::Connector,
        _: &ConnectorConfig,
        config: &Value,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(config)?;
        let value = tremor_value::to_value(&config.value)?;
        Ok(Box::new(Oneshot { value: Some(value) }))
    }
}

struct Oneshot {
    value: Option<Value<'static>>,
}

#[async_trait::async_trait]
impl Connector for Oneshot {
    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = OneshotSource {
            value: self.value.take(),
        };
        Ok(Some(builder.spawn(source, source_context)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }
}

struct OneshotSource {
    value: Option<Value<'static>>,
}

#[async_trait::async_trait]
impl Source for OneshotSource {
    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        if let Some(value) = self.value.take() {
            Ok(SourceReply::Structured {
                origin_uri: EventOriginUri {
                    scheme: "tremor-oneshot".to_string(),
                    host: "localhost".to_string(),
                    port: None,
                    path: vec![],
                },
                payload: (value, Value::object()).into(),
                stream: DEFAULT_STREAM_ID,
                port: None,
            })
        } else {
            Ok(SourceReply::Finished)
        }
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        false
    }
}
