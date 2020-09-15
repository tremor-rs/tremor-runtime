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

//! # Exit offramp terminates runtime
//!
//! The exit offramp terminates the runtime with a system exit status
//!
//! ## Configuration
//!
//! This operator takes no configuration

use crate::sink::prelude::*;
use std::time::Duration;

pub struct Exit {}

impl offramp::Impl for Exit {
    fn from_config(_config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        Ok(SinkManager::new_box(Self {}))
    }
}

#[async_trait::async_trait]
impl Sink for Exit {
    #[allow(clippy::used_underscore_binding)]
    async fn on_event(&mut self, _input: &str, _codec: &dyn Codec, event: Event) -> ResultVec {
        for (value, _meta) in event.value_meta_iter() {
            if let Some(status) = value.get("exit").and_then(Value::as_i32) {
                if let Some(delay) = value.get("delay").and_then(Value::as_u64) {
                    task::sleep(Duration::from_millis(delay)).await;
                }
                // ALLOW: this is the supposed to exit
                std::process::exit(status);
            } else {
                return Err("Unexpected event received in exit offramp".into());
            }
        }
        Ok(None)
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    #[allow(clippy::used_underscore_binding)]
    async fn init(&mut self, _postprocessors: &[String]) -> Result<()> {
        Ok(())
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
