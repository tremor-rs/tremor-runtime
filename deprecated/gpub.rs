// Copyright 2020-2021, The Tremor Team
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

//! # Google `PubSub` Publication Offramp
//!
//! The `Gpub` offramp writes events to a Google `PubSub` topic.
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use crate::google::{pubsub_api, GpsHub};
use crate::offramp::prelude::*;
use google_pubsub1::{PublishRequest, PubsubMessage};
use hashbrown::HashMap;
use std::fmt;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Service Account secrets file ( json )
    pub service_account: String,
    /// topic to publish to
    pub topic: String,
}

impl ConfigImpl for Config {}

/// An offramp that write to GCS
pub struct GPub {
    config: Config,
    hub: GpsHub,
    pipelines: HashMap<TremorURL, pipeline::Addr>,
    postprocessors: Postprocessors,
}

impl fmt::Debug for GPub {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "(Gpub, topic: {})", self.config.topic)
    }
}

impl offramp::Impl for GPub {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let hub = pubsub_api(&config.service_account.to_string())?;
            Ok(Box::new(Self {
                config,
                hub,
                pipelines: HashMap::new(),
                postprocessors: vec![],
            }))
        } else {
            Err("Missing config for gpub offramp".into())
        }
    }
}

impl Offramp for GPub {
    fn start(&mut self, _codec: &dyn Codec, postprocessors: &[String]) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }

    fn add_pipeline(&mut self, id: TremorURL, addr: pipeline::Addr) {
        self.pipelines.insert(id, addr);
    }

    fn remove_pipeline(&mut self, id: TremorURL) -> bool {
        self.pipelines.remove(&id);
        self.pipelines.is_empty()
    }

    fn default_codec(&self) -> &str {
        "json"
    }

    fn on_event(&mut self, codec: &dyn Codec, _input: String, event: Event) -> Result<()> {
        let methods = self.hub.projects();
        let topic_name = self.config.topic.clone();

        for value in event.value_iter() {
            let raw = codec.encode(value)?;
            let message = PubsubMessage {
                data: Some(base64::encode(&raw)),
                ..PubsubMessage::default()
            };
            let request = PublishRequest {
                messages: Some(vec![message]),
            };
            methods
                .topics_publish(request.clone(), &topic_name)
                .doit()?;
        }
        Ok(())
    }
}
