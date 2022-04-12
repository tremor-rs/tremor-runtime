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
    /// Interval in nanoseconds
    pub interval: u64,
}

impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "metronome".into()
    }

    async fn from_config(
        &self,
        _id: &str,
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
                interval: config.interval,
                origin_uri,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(String::from("metronome")).into())
        }
    }
}

#[derive(Clone, Debug)]
pub struct Metronome {
    interval: u64,
    origin_uri: EventOriginUri,
}

#[async_trait::async_trait()]
impl Connector for Metronome {
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
        let source = MetronomeSource::new(self.interval, self.origin_uri.clone());
        builder.spawn(source, source_context).map(Some)
    }
}

struct MetronomeSource {
    interval_ns: u64,
    next: u64,
    origin_uri: EventOriginUri,
    id: u64,
}

impl MetronomeSource {
    fn new(interval_ns: u64, origin_uri: EventOriginUri) -> Self {
        Self {
            interval_ns,
            next: nanotime() + interval_ns, // dummy placeholer
            origin_uri,
            id: 0,
        }
    }
}

#[async_trait::async_trait()]
impl Source for MetronomeSource {
    async fn connect(&mut self, _ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        self.next = nanotime() + self.interval_ns;
        Ok(true)
    }
    async fn pull_data(&mut self, pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        let now = nanotime();
        if self.next < now {
            self.next = now + self.interval_ns;
            *pull_id = self.id;
            self.id += 1;
            let data = literal!({
                "onramp": "metronome",
                "ingest_ns": now,
                "id": *pull_id
            });
            Ok(SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                payload: data.into(),
                stream: DEFAULT_STREAM_ID,
                port: None,
            })
        } else {
            let wait_ms = (self.next - now) / 1_000_000;
            Ok(SourceReply::Empty(wait_ms))
        }
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        false
    }
}
