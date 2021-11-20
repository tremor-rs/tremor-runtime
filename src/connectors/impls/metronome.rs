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
use tremor_common::time::nanotime;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Interval in milliseconds
    pub interval: u64,
}

impl ConfigImpl for Config {}

#[derive(Clone, Debug)]
pub struct Metronome {
    interval_ns: u64,
    next: u64,
    origin_uri: EventOriginUri,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "metronome".into()
    }

    async fn from_config(
        &self,
        _id: &TremorUrl,
        raw_config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(raw) = raw_config {
            let config = Config::new(raw)?;
            let origin_uri = EventOriginUri {
                scheme: "tremor-metronome".to_string(),
                host: hostname(),
                port: None,
                path: vec![config.interval.to_string()],
            };

            Ok(Box::new(Metronome {
                interval_ns: config.interval * 1_000_000_000,
                next: 0,
                origin_uri,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(String::from("metronome")).into())
        }
    }
}

#[async_trait::async_trait()]
impl Source for Metronome {
    async fn pull_data(&mut self, pull_id: u64, _ctx: &SourceContext) -> Result<SourceReply> {
        let now = nanotime();
        if self.next < now {
            self.next = now + self.interval_ns;
            let data = literal!({
                "onramp": "metronome",
                "ingest_ns": now,
                "id": pull_id
            });
            Ok(SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                payload: data.into(),
                stream: DEFAULT_STREAM_ID,
                port: None,
            })
        } else {
            Ok(SourceReply::Empty(self.next - now))
        }
    }

    fn is_transactional(&self) -> bool {
        false
    }
}
#[async_trait::async_trait()]
impl Connector for Metronome {
    fn is_structured(&self) -> bool {
        true
    }

    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
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
        builder.spawn(self.clone(), source_context).map(Some)
    }
}
