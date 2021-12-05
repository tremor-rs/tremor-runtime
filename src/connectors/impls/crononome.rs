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

#[derive(Clone, Debug)]
pub struct Crononome {
    config: Option<Value<'static>>,
    origin_uri: EventOriginUri,
    cq: ChronomicQueue,
    deploy_id: TremorUrl,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "crononome".into()
    }

    async fn from_config(
        &self,
        id: &TremorUrl,
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
            let origin_uri = EventOriginUri {
                scheme: "tremor-crononome".to_string(),
                host: hostname(),
                port: None,
                path: vec![],
            };

            // One time configuration of the cq with periodic cron
            // schedule
            let mut cq = ChronomicQueue::default();
            if let Some(Value::Array(entries)) = &payload {
                for entry in entries {
                    let entry = entry.clone();
                    let entry = CronEntryInt::try_from(entry)?;
                    cq.enqueue(&entry);
                }
            }

            Ok(Box::new(Crononome {
                origin_uri,
                config: payload,
                deploy_id: id.clone(),
                cq,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(String::from("crononome")).into())
        }
    }
}

#[async_trait::async_trait()]
impl Source for Crononome {
    async fn pull_data(&mut self, pull_id: u64, _ctx: &SourceContext) -> Result<SourceReply> {
        if let Some(trigger) = self.cq.next() {
            let mut origin_uri = self.origin_uri.clone();
            origin_uri.path.push(trigger.0.clone());

            let mut data: Value<'static> = Value::object_with_capacity(4);
            data.insert("onramp", "crononome")?; // TODO connector
            data.insert("ingest_ns", nanotime())?;
            data.insert("id", pull_id)?;
            let mut tr: Value<'static> = Value::object_with_capacity(2);
            tr.insert("name", trigger.0)?;
            if let Some(payload) = trigger.1 {
                tr.insert("payload", payload)?;
            }
            data.insert("trigger", tr)?;
            Ok(SourceReply::Structured {
                origin_uri,
                payload: data.into(),
                stream: DEFAULT_STREAM_ID,
                port: None,
            })
        } else {
            Ok(SourceReply::Empty(100))
        }
    }

    fn is_transactional(&self) -> bool {
        false
    }
}

#[async_trait::async_trait()]
impl Connector for Crononome {
    fn is_structured(&self) -> bool {
        true
    }

    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        if let Some(Value::Array(entries)) = &self.config {
            for entry in entries {
                let entry = entry.clone();
                let entry = CronEntryInt::try_from(entry)?;
                self.cq.enqueue(&entry);
            }
        }
        Ok(true)
    }

    fn default_codec(&self) -> &str {
        "json"
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        // TODO NOTE The spawn here uses `self.clone()` which is
        // very confusing as any initialized state will be lost
        // on the clone which - depending on the implementation of
        // the connector - is going to be very confusing for the
        // connector writer.
        //
        builder.spawn(self.clone(), source_context).map(Some)
    }
}
