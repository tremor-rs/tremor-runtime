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

//! # Debug offramp reporting classification statistics.
//!
//! The debug offramp periodically reports on the number of events
//! per classification.
//!
//! ## Configuration
//!
//! This operator takes no configuration

use crate::sink::prelude::*;
use async_std::io;
use halfbrown::HashMap;

pub struct StdOut {
    postprocessors: Postprocessors,
    stdout: io::Stdout,
    config: Config,
}

#[derive(Clone, Debug, Deserialize, Default)]
struct Config {
    #[serde(default = "Default::default")]
    prefix: String,
}

impl ConfigImpl for Config {}

impl offramp::Impl for StdOut {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            Ok(SinkManager::new_box(Self {
                postprocessors: vec![],
                stdout: io::stdout(),
                config: Config::new(config)?,
            }))
        } else {
            Ok(SinkManager::new_box(Self {
                postprocessors: vec![],
                stdout: io::stdout(),
                config: Config::default(),
            }))
        }
    }
}
#[async_trait::async_trait]
impl Sink for StdOut {
    async fn on_event(
        &mut self,
        _input: &str,
        codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        for value in event.value_iter() {
            let raw = codec.encode(value)?;
            if let Ok(s) = std::str::from_utf8(&raw) {
                self.stdout.write_all(self.config.prefix.as_bytes()).await?;
                self.stdout.write_all(s.as_bytes()).await?;
                self.stdout.write_all(b"\n").await?;
            } else {
                self.stdout
                    .write_all(format!("{}{:?}\n", self.config.prefix, raw).as_bytes())
                    .await?
            }
        }
        self.stdout.flush().await?;
        Ok(None)
    }
    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        _sink_uid: u64,
        _sink_url: &TremorURL,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        _is_linked: bool,
        _reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        self.postprocessors = make_postprocessors(processors.post)?;
        Ok(())
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        Ok(None)
    }
    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        true
    }
}
