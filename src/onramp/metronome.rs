// Copyright 2018-2020, Wayfair GmbH
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

use crate::onramp::prelude::*;
//NOTE: This is required for StreamHander's stream
use crate::utils::nanotime;
use serde_yaml::Value;
use simd_json::json;
use std::time::Duration;

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
    id: u64,
    onramp_id: String,
}

impl onramp::Impl for Metronome {
    fn from_config(id: &str, config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let origin_uri = EventOriginUri {
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
                id: 0,
                onramp_id: id.to_string(),
            }))
        } else {
            Err("Missing config for metronome onramp".into())
        }
    }
}

#[async_trait::async_trait()]
impl Source for Metronome {
    async fn read(&mut self) -> Result<SourceReply> {
        task::sleep(self.duration).await;
        let data = simd_json::to_vec(
            &json!({"onramp": "metronome", "ingest_ns": nanotime(), "id": self.id}),
        )?;
        self.id += 1;
        Ok(SourceReply::Data {
            origin_uri: self.origin_uri.clone(),
            data,
            stream: 0,
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
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        SourceManager::start(
            self.id(),
            self.clone(),
            codec,
            preprocessors,
            metrics_reporter,
        )
        .await
    }

    fn default_codec(&self) -> &str {
        "json"
    }
    fn id(&self) -> &str {
        &self.onramp_id
    }
}
