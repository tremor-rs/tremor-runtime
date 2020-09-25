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

pub struct StdErr {
    postprocessors: Postprocessors,
    stderr: io::Stderr,
}

impl offramp::Impl for StdErr {
    fn from_config(_config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        Ok(SinkManager::new_box(Self {
            postprocessors: vec![],
            stderr: io::stderr(),
        }))
    }
}
#[async_trait::async_trait]
impl Sink for StdErr {
    #[allow(clippy::used_underscore_binding)]
    async fn on_event(&mut self, _input: &str, codec: &dyn Codec, event: Event) -> ResultVec {
        for value in event.value_iter() {
            let raw = codec.encode(value)?;
            if let Ok(s) = std::str::from_utf8(&raw) {
                self.stderr.write_all(s.as_bytes()).await?
            } else {
                self.stderr
                    .write_all(format!("{:?}", raw).as_bytes())
                    .await?
            }
        }
        self.stderr.flush().await?;
        Ok(None)
    }
    async fn init(
        &mut self,
        postprocessors: &[String],
        _reply_channel: Sender<SinkReply>,
    ) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    #[allow(clippy::used_underscore_binding)]
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
