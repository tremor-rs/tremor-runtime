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

use crate::source::prelude::*;
//NOTE: This is required for StreamHander's stream
use simd_json::BorrowedValue;
use std::time::Duration;
use tremor_common::time::nanotime;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// Interval in milliseconds
    pub interval: u64,
}

impl ConfigImpl for Config {}

#[derive(Clone, Debug)]
pub struct Metronome {
    pub config: Config,
    origin_uri: EventOriginUri,
    duration: Duration,
    onramp_id: TremorURL,
}

impl onramp::Impl for Metronome {
    fn from_config(id: &TremorURL, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let origin_uri = EventOriginUri {
                uid: 0,
                scheme: "tremor-metronome".to_string(),
                host: hostname(),
                port: None,
                path: vec![config.interval.to_string()],
            };
            let duration = Duration::from_millis(config.interval);
            Ok(Box::new(Self {
                config,
                origin_uri,
                duration,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Missing config for metronome onramp".into())
        }
    }
}

#[async_trait::async_trait()]
impl Source for Metronome {
    fn id(&self) -> &TremorURL {
        &self.onramp_id
    }

    async fn pull_event(&mut self, id: u64) -> Result<SourceReply> {
        task::sleep(self.duration).await;
        let mut data: BorrowedValue<'static> = BorrowedValue::object_with_capacity(3);
        data.insert("onramp", "metronome")?;
        data.insert("ingest_ns", nanotime())?;
        data.insert("id", id)?;
        Ok(SourceReply::Structured {
            origin_uri: self.origin_uri.clone(),
            data: data.into(),
        })
    }

    async fn init(&mut self) -> Result<SourceState> {
        Ok(SourceState::Connected)
    }
}

#[async_trait::async_trait]
impl Onramp for Metronome {
    async fn start(
        &mut self,
        onramp_uid: u64,
        codec: &str,
        codec_map: halfbrown::HashMap<String, String>,
        processors: Processors<'_>,
        metrics_reporter: RampReporter,
        _is_linked: bool,
    ) -> Result<onramp::Addr> {
        SourceManager::start(
            onramp_uid,
            self.clone(),
            codec,
            codec_map,
            processors,
            metrics_reporter,
        )
        .await
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
