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

pub struct Metronome {
    pub config: Config,
}

impl onramp::Impl for Metronome {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self { config }))
        } else {
            Err("Missing config for metronome onramp".into())
        }
    }
}

fn onramp_loop(
    rx: &Receiver<onramp::Msg>,
    config: &Config,
    mut preprocessors: Preprocessors,
    mut codec: Box<dyn Codec>,
    mut metrics_reporter: RampReporter,
) -> Result<()> {
    let mut pipelines: Vec<(TremorURL, pipeline::Addr)> = Vec::new();
    let mut id = 0;

    let origin_uri = tremor_pipeline::EventOriginUri {
        scheme: "tremor-metronome".to_string(),
        host: hostname(),
        port: None,
        path: vec![config.interval.to_string()],
    };

    loop {
        match task::block_on(handle_pipelines(
            false,
            &rx,
            &mut pipelines,
            &mut metrics_reporter,
        ))? {
            PipeHandlerResult::Retry => continue,
            PipeHandlerResult::Terminate => return Ok(()),
            _ => (), // fixme .unwrap()
        }

        thread::sleep(Duration::from_millis(config.interval));
        let data =
            simd_json::to_vec(&json!({"onramp": "metronome", "ingest_ns": nanotime(), "id": id}));
        let mut ingest_ns = nanotime();
        if let Ok(data) = data {
            send_event(
                &pipelines,
                &mut preprocessors,
                &mut codec,
                &mut metrics_reporter,
                &mut ingest_ns,
                &origin_uri,
                id,
                data,
            );
        }
        id += 1;
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
        let config = self.config.clone();
        let (tx, rx) = channel(1);
        let codec = codec::lookup(codec)?;
        let preprocessors = make_preprocessors(&preprocessors)?;
        thread::Builder::new()
            .name(format!("onramp-metronome-{}", "???"))
            .spawn(move || onramp_loop(&rx, &config, preprocessors, codec, metrics_reporter))?;
        Ok(tx)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
