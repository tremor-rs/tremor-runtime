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
use serde_yaml::Value as YamlValue;
use tremor_common::time::nanotime;

mod handler;

use handler::*;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Cron entries
    pub entries: Option<YamlValue>,
}

impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "crononome".into()
    }

    async fn from_config(
        &self,
        id: &str,
        raw_config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(raw) = raw_config {
            let raw = Config::new(raw)?;
            let payload = if let Some(yaml_entries) = raw.entries {
                let mut entries = simd_json::to_vec(&yaml_entries)?;
                let tremor_entries = tremor_value::parse_to_value(&mut entries)?;
                Some(tremor_entries.into_static())
            } else {
                None
            };

            let entries = if let Some(Value::Array(entries)) = &payload {
                entries
                    .iter()
                    .cloned()
                    .map(CronEntryInt::try_from)
                    .collect::<Result<Vec<CronEntryInt>>>()?
            } else {
                return Err(ErrorKind::InvalidConfiguration(
                    id.to_string(),
                    "missing `entries` array".to_string(),
                )
                .into());
            };

            Ok(Box::new(Crononome { entries }))
        } else {
            Err(ErrorKind::MissingConfiguration(id.to_string()).into())
        }
    }
}

#[derive(Clone, Debug)]
pub struct Crononome {
    entries: Vec<CronEntryInt>,
}

#[async_trait::async_trait()]
impl Connector for Crononome {
    fn is_structured(&self) -> bool {
        true
    }

    fn default_codec(&self) -> &str {
        "json"
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = CrononomeSource::new(self.entries.clone());
        builder.spawn(source, source_context).map(Some)
    }
}
struct CrononomeSource {
    entries: Vec<CronEntryInt>,
    cq: ChronomicQueue,
    origin_uri: EventOriginUri,
}

impl CrononomeSource {
    fn new(entries: Vec<CronEntryInt>) -> Self {
        Self {
            entries,
            cq: ChronomicQueue::default(),
            origin_uri: EventOriginUri {
                scheme: "tremor-crononome".to_string(),
                host: hostname(),
                port: None,
                path: vec![],
            },
        }
    }
}
#[async_trait::async_trait()]
impl Source for CrononomeSource {
    async fn connect(&mut self, _ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        self.cq = ChronomicQueue::default(); // create a new queue to avoid duplication in case of reconnect
        for entry in &self.entries {
            self.cq.enqueue(entry);
        }
        Ok(true)
    }
    async fn pull_data(&mut self, pull_id: &mut u64, ctx: &SourceContext) -> Result<SourceReply> {
        if !ctx.quiescence_beacon().continue_reading().await {
            return Ok(SourceReply::Empty(100));
        }
        if let Some(trigger) = self.cq.next() {
            let mut origin_uri = self.origin_uri.clone();
            origin_uri.path.push(trigger.0.clone());

            let mut tr: Value<'static> = Value::object_with_capacity(2);
            tr.try_insert("name", trigger.0.clone());
            if let Some(payload) = trigger.1 {
                tr.try_insert("payload", payload);
            }
            let data = literal!({
                "connector": "crononome",
                "ingest_ns": nanotime(),
                "id": *pull_id,
                "trigger": tr
            });
            let meta = ctx.meta(literal!({
                "trigger": trigger.0
            }));
            Ok(SourceReply::Structured {
                origin_uri,
                payload: (data, meta).into(),
                stream: DEFAULT_STREAM_ID,
                port: None,
            })
        } else {
            Ok(SourceReply::Empty(DEFAULT_POLL_INTERVAL))
        }
    }

    fn is_transactional(&self) -> bool {
        false
    }
}
